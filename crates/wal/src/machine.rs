//! State machine for one WAL shard.
//!
//! Committed batches stay here until the uploader publishes them and a `Seal`
//! entry drops them. The state machine keeps each batch's header (offsets,
//! where it is in the log, its length), not its bytes: the bytes are already
//! in the Raft log entry they arrived in, and reads take them from there. A
//! batch is written to disk once.
//!
//! The log is purged only below its oldest entry that still holds an
//! unsealed batch ([`Machine::purge_floor`]), so this node's unsealed bytes
//! are always in its log. A snapshot carries those bytes too, for a follower
//! that installs it; that follower keeps them in the `DETACHED` table, since
//! its log will not hold their entries.
//!
//! Apply is on the produce ack path, so it changes only memory. The Raft
//! log, fsynced on a quorum, is what makes a batch durable. What apply
//! changed is recorded and [`Machine::flush`] writes just that, in one
//! durable redb commit: about once a second from the shard, before a
//! snapshot, and when the membership changes. After a crash openraft
//! re-applies the log from the last flush. Building a snapshot flushes, and
//! the log is purged only up to a snapshot, so the entries a restart needs
//! are still there.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftLogReader, RaftStateMachine, Snapshot};
use openraft::{
    AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Command, NodeId, Response, TypeConfig};

const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// Next offset per partition key.
const NEXT: TableDefinition<&str, i64> = TableDefinition::new("next");
/// Producer sequences per `{partition key}/{producer id}`, postcard.
const PRODUCERS: TableDefinition<&str, &[u8]> = TableDefinition::new("producers");
/// Open transaction spans, same key as a producer sequence, postcard.
const TXN_SPANS: TableDefinition<&str, &[u8]> = TableDefinition::new("txn_spans");
/// Most open transaction spans one shard keeps. A new producer past this
/// cap is refused instead of growing the map (ADR 0019).
const SPAN_CAP: usize = 65_536;
/// Unsealed batch headers by storage key, postcard.
const HEADS: TableDefinition<u64, &[u8]> = TableDefinition::new("heads");
/// Bytes of unsealed batches whose log entry a snapshot may purge.
const DETACHED: TableDefinition<u64, &[u8]> = TableDefinition::new("detached");
/// The latest snapshot, postcard.
const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");
const STATE: &str = "state";
const SNAPSHOT: &str = "current";

/// Largest client batch the shard accepts or re-seals. The broker applies the
/// topic's `max.message.bytes` before a batch reaches the shard.
const MAX_BATCH: usize = camus_protocol::batch::MAX_BATCH_CEILING;

/// Most batches one packed entry may hold; the storage key reserves 12 bits.
pub const MAX_PACKED: usize = 4096;

/// A committed, unsealed batch with its bytes, as the uploader and fetch see
/// it.
#[derive(Clone)]
pub struct Batch {
    pub topic: String,
    pub partition: i32,
    pub start: i64,
    pub end: i64,
    pub log_index: u64,
    pub topic_id: [u8; 16],
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    /// Client bytes with base offset and epoch filled in.
    pub bytes: Vec<u8>,
}

/// What the state machine keeps per unsealed batch.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Head {
    topic: String,
    partition: i32,
    start: i64,
    end: i64,
    log_index: u64,
    /// Position inside a packed entry. `0` for a single append.
    pos: u32,
    /// Offset of this batch inside the log record. `0` means the record was
    /// written before batches had their own range, so the read decodes the
    /// whole entry.
    #[serde(default)]
    at: u64,
    topic_id: [u8; 16],
    base_timestamp: i64,
    max_timestamp: i64,
    epoch: i32,
    len: u64,
}

impl Head {
    /// redb key: the log index with the position in its entry.
    fn key(&self) -> u64 {
        (self.log_index << 12) | u64::from(self.pos)
    }
}

/// Unsealed heads, indexed for how they are read: by partition and offset
/// for fetch and seal, by storage key (commit order) for the uploader.
#[derive(Default, Clone)]
struct Heads {
    by_part: BTreeMap<([u8; 16], i32), BTreeMap<i64, Head>>,
    by_key: BTreeMap<u64, ([u8; 16], i32, i64)>,
    /// Sum of every head's `len`.
    bytes: u64,
}

impl Heads {
    fn insert(&mut self, head: Head) {
        self.bytes += head.len;
        self.by_key
            .insert(head.key(), (head.topic_id, head.partition, head.start));
        self.by_part
            .entry((head.topic_id, head.partition))
            .or_default()
            .insert(head.start, head);
    }

    fn remove(&mut self, topic_id: [u8; 16], partition: i32, start: i64) -> Option<Head> {
        let part = self.by_part.get_mut(&(topic_id, partition))?;
        let head = part.remove(&start)?;
        if part.is_empty() {
            self.by_part.remove(&(topic_id, partition));
        }
        self.by_key.remove(&head.key());
        self.bytes -= head.len;
        Some(head)
    }

    /// Every head, in commit order.
    fn in_commit_order(&self) -> impl Iterator<Item = &Head> {
        self.by_key
            .values()
            .filter_map(|(topic_id, partition, start)| {
                self.by_part.get(&(*topic_id, *partition))?.get(start)
            })
    }

    /// One partition's heads at or after `sealed_end` that end after
    /// `offset`, in offset order.
    fn of_partition(
        &self,
        topic_id: [u8; 16],
        partition: i32,
        sealed_end: i64,
        offset: i64,
    ) -> impl Iterator<Item = &Head> {
        let part = self.by_part.get(&(topic_id, partition));
        // Start at the head holding `offset`, if one does.
        let holding = part
            .and_then(|part| part.range(..=offset).next_back())
            .filter(|(_, head)| head.end > offset)
            .map(|(start, _)| *start)
            .unwrap_or(offset);
        part.into_iter()
            .flat_map(move |part| part.range(holding.max(sealed_end)..))
            .map(|(_, head)| head)
    }
}

impl FromIterator<Head> for Heads {
    fn from_iter<I: IntoIterator<Item = Head>>(iter: I) -> Self {
        let mut heads = Heads::default();
        for head in iter {
            heads.insert(head);
        }
        heads
    }
}

/// What apply changed since the last flush.
#[derive(Default)]
struct Dirty {
    added: BTreeMap<u64, Head>,
    removed: BTreeSet<u64>,
    next: BTreeSet<String>,
    producers: BTreeSet<String>,
    spans: BTreeSet<String>,
    /// `last_applied` or the membership moved.
    meta: bool,
}

impl Dirty {
    fn is_empty(&self) -> bool {
        !self.meta
            && self.added.is_empty()
            && self.removed.is_empty()
            && self.next.is_empty()
            && self.producers.is_empty()
            && self.spans.is_empty()
    }

    fn add(&mut self, head: Head) {
        self.added.insert(head.key(), head);
    }

    fn remove(&mut self, key: u64) {
        // A head added and removed between flushes never reaches the tables.
        if self.added.remove(&key).is_none() {
            self.removed.insert(key);
        }
    }

    /// Put back what a failed flush took, under anything applied since.
    fn restore(&mut self, older: Dirty) {
        for (key, head) in older.added {
            if !self.removed.remove(&key) {
                self.added.entry(key).or_insert(head);
            }
        }
        for key in older.removed {
            if !self.added.contains_key(&key) {
                self.removed.insert(key);
            }
        }
        self.next.extend(older.next);
        self.producers.extend(older.producers);
        self.spans.extend(older.spans);
        self.meta |= older.meta;
    }
}

/// One producer's open transaction on a partition. The start stays at the
/// first batch; later batches move only the end.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpenSpan {
    start: i64,
    end: i64,
    /// `EndTxn` has frozen the span. Later batches from this producer are
    /// refused. `start < 0` means the producer had not written one.
    #[serde(default)]
    finished: bool,
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    last_applied: Option<LogId<NodeId>>,
    #[serde(default)]
    membership: Option<StoredMembership<NodeId, openraft::BasicNode>>,
    /// In the `NEXT` table. Read here only from state written before it.
    #[serde(default, skip_serializing)]
    next: BTreeMap<String, i64>,
    /// Producer sequences, keyed by `{topic id hex}/{partition}/{producer id}`.
    /// A leader change reads this with the log, so a retry is not appended twice.
    /// In the `PRODUCERS` table; read here only from state written before it.
    #[serde(default, skip_serializing)]
    producers: BTreeMap<String, camus_protocol::idempotence::ProducerSeq>,
    /// Open transaction spans, keyed like `producers`. In the `TXN_SPANS`
    /// table. One entry per producer per partition, dropped when the
    /// transaction ends, and never more than [`SPAN_CAP`].
    #[serde(default, skip_serializing)]
    txn_spans: BTreeMap<String, OpenSpan>,
    #[serde(skip)]
    heads: Heads,
    #[serde(skip)]
    dirty: Dirty,
}

/// What a snapshot carries besides openraft's meta.
#[derive(Serialize, Deserialize)]
struct SnapshotData {
    next: BTreeMap<String, i64>,
    heads: Vec<Head>,
    /// Sealed bytes of every head, by storage key.
    bytes: Vec<(u64, serde_bytes::ByteBuf)>,
    /// Producer sequences. Absent from snapshots written before idempotence.
    #[serde(default)]
    producers: BTreeMap<String, camus_protocol::idempotence::ProducerSeq>,
    /// Open transaction spans. Absent from snapshots written before ADR 0019.
    #[serde(default)]
    txn_spans: BTreeMap<String, OpenSpan>,
}

/// Snapshot body from before producer sequences were stored.
#[derive(Deserialize)]
struct SnapshotDataV1 {
    next: BTreeMap<String, i64>,
    heads: Vec<Head>,
    bytes: Vec<(u64, serde_bytes::ByteBuf)>,
}

fn decode_snapshot(data: &[u8]) -> Result<SnapshotData, &'static str> {
    if let Ok(parsed) = camus_raft::codec::decode(data) {
        return Ok(parsed);
    }
    let old: SnapshotDataV1 = camus_raft::codec::decode(data).map_err(|_| "snapshot")?;
    Ok(SnapshotData {
        next: old.next,
        heads: old.heads,
        bytes: old.bytes,
        producers: BTreeMap::new(),
        txn_spans: BTreeMap::new(),
    })
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

pub struct Machine {
    db: Database,
    log: camus_raft::SegmentLogStore<TypeConfig>,
    state: std::sync::Mutex<State>,
    /// Held across a flush or a snapshot install, so their writes do not
    /// interleave.
    flushing: std::sync::Mutex<()>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl Machine {
    /// `log` is the shard's own Raft log; unsealed bytes are read from it.
    pub fn open(
        path: &Path,
        log: camus_raft::SegmentLogStore<TypeConfig>,
    ) -> Result<Self, StorageError<NodeId>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        }
        let db = if path.exists() {
            Database::open(path)
        } else {
            Database::create(path)
        }
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let state = load_state(&db)?;
        let current = load_snapshot(&db)?;
        Ok(Self {
            db,
            log,
            state: std::sync::Mutex::new(state),
            flushing: std::sync::Mutex::new(()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(current),
        })
    }

    /// Inclusive start and exclusive end of this producer's open span.
    pub fn txn_span(
        &self,
        topic_id: &[u8; 16],
        partition: i32,
        producer_id: i64,
    ) -> Option<(i64, i64)> {
        let key = span_key(topic_id, partition, producer_id);
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        state.txn_spans.get(&key).and_then(|span| {
            if span.start < 0 {
                None
            } else {
                Some((span.start, span.end))
            }
        })
    }

    pub fn end_offset(&self, topic_id: &[u8; 16], partition: i32) -> i64 {
        let state = self.state.lock().expect("wal state");
        state
            .next
            .get(&part_key(topic_id, partition))
            .copied()
            .unwrap_or(0)
    }

    /// Make every applied change durable, writing only what changed since
    /// the last flush. Blocks on disk: call it off the async runtime.
    pub fn flush(&self) -> Result<(), StorageError<NodeId>> {
        let _gate = self.flushing.lock().expect("wal flush");
        let (meta, next, producers, spans, dirty) = {
            let mut state = self.state.lock().expect("wal state");
            if state.dirty.is_empty() {
                return Ok(());
            }
            let dirty = std::mem::take(&mut state.dirty);
            let meta = serde_json::to_string(&*state)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            let next: Vec<(String, Option<i64>)> = dirty
                .next
                .iter()
                .map(|key| (key.clone(), state.next.get(key).copied()))
                .collect();
            let producers = dirty
                .producers
                .iter()
                .map(|key| {
                    let raw = state
                        .producers
                        .get(key)
                        .map(|seq| {
                            camus_raft::codec::encode(seq)
                                .map_err(|err| io(ErrorSubject::StateMachine, &err))
                        })
                        .transpose()?;
                    Ok((key.clone(), raw))
                })
                .collect::<Result<Vec<_>, StorageError<NodeId>>>()?;
            let spans = dirty
                .spans
                .iter()
                .map(|key| {
                    let raw = state
                        .txn_spans
                        .get(key)
                        .map(|span| {
                            camus_raft::codec::encode(span)
                                .map_err(|err| io(ErrorSubject::StateMachine, &err))
                        })
                        .transpose()?;
                    Ok((key.clone(), raw))
                })
                .collect::<Result<Vec<_>, StorageError<NodeId>>>()?;
            (meta, next, producers, spans, dirty)
        };
        let written = write_changes(&self.db, &meta, &next, &producers, &spans, &dirty);
        if written.is_err() {
            self.state.lock().expect("wal state").dirty.restore(dirty);
        }
        written
    }

    pub fn unsealed_bytes(&self) -> u64 {
        self.state.lock().expect("wal state").heads.bytes
    }

    /// The oldest unsealed batches in commit order, with their bytes, up to
    /// `limit` bytes (at least one batch). Commit order keeps each
    /// partition's batches in offset order and lets no partition starve.
    pub async fn pending(&self, limit: u64) -> Result<Vec<Batch>, StorageError<NodeId>> {
        let heads: Vec<Head> = {
            let state = self.state.lock().expect("wal state");
            let mut size = 0u64;
            let mut taken = Vec::new();
            for head in state.heads.in_commit_order() {
                if !taken.is_empty() && size + head.len > limit {
                    break;
                }
                size += head.len;
                taken.push(head.clone());
            }
            taken
        };
        self.materialize(heads).await
    }

    /// Unsealed batches of one partition at or after `sealed_end` that end
    /// after `offset`, in offset order, stopping once `limit` bytes are read
    /// (the first batch is always returned whole).
    pub async fn read(
        &self,
        topic_id: &[u8; 16],
        partition: i32,
        sealed_end: i64,
        offset: i64,
        limit: usize,
    ) -> Result<Vec<u8>, StorageError<NodeId>> {
        let (heads, size) = {
            let state = self.state.lock().expect("wal state");
            let mut size = 0u64;
            let mut kept = Vec::new();
            for head in state
                .heads
                .of_partition(*topic_id, partition, sealed_end, offset)
            {
                if size != 0 && size + head.len > limit as u64 {
                    break;
                }
                size += head.len;
                kept.push(head.clone());
            }
            (kept, size)
        };
        let mut out = Vec::with_capacity(size as usize);
        for batch in self.materialize(heads).await? {
            out.extend_from_slice(&batch.bytes);
        }
        Ok(out)
    }

    /// Fetch each head's bytes: from `DETACHED` if a snapshot copied it, else
    /// from its log entry, re-sealed with the offset and epoch it was given.
    async fn materialize(&self, heads: Vec<Head>) -> Result<Vec<Batch>, StorageError<NodeId>> {
        let mut detached = BTreeMap::new();
        {
            let txn = self
                .db
                .begin_read()
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            match txn.open_table(DETACHED) {
                Ok(table) => {
                    for head in &heads {
                        if let Some(raw) = table
                            .get(head.key())
                            .map_err(|err| io(ErrorSubject::StateMachine, &err))?
                        {
                            detached.insert(head.key(), raw.value().to_vec());
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
            }
        }
        let wanted: BTreeSet<u64> = heads
            .iter()
            .filter(|head| head.at == 0 && !detached.contains_key(&head.key()))
            .map(|head| head.log_index)
            .collect();
        let mut entries: BTreeMap<u64, Command> = BTreeMap::new();
        // A head that knows its byte offset is read on its own. Older heads,
        // written before that offset was stored, still decode their entry.
        // Those entries are read one range per run of consecutive indexes.
        let mut log = self.log.clone();
        for (first, last) in runs(&wanted) {
            for entry in log.try_get_log_entries(first..=last).await? {
                if let EntryPayload::Normal(command) = entry.payload {
                    entries.insert(entry.log_id.index, command);
                }
            }
        }
        let mut out = Vec::with_capacity(heads.len());
        for head in heads {
            let bytes = if let Some(bytes) = detached.remove(&head.key()) {
                bytes
            } else if head.at > 0 {
                let raw = log
                    .read_slice(head.log_index, head.at, head.len as usize)
                    .await
                    .map_err(|err| io(ErrorSubject::Logs, &err))?
                    .ok_or_else(|| missing(&head))?;
                camus_protocol::batch::seal(&raw, head.start, head.epoch, MAX_BATCH)
                    .map_err(|_| missing(&head))?
                    .to_vec()
            } else {
                let raw = match entries.get(&head.log_index) {
                    Some(Command::Append(append)) if head.pos == 0 => &append.raw,
                    Some(Command::Appends(appends)) => match appends.get(head.pos as usize) {
                        Some(append) => &append.raw,
                        None => return Err(missing(&head)),
                    },
                    _ => return Err(missing(&head)),
                };
                camus_protocol::batch::seal(raw, head.start, head.epoch, MAX_BATCH)
                    .map_err(|_| missing(&head))?
                    .to_vec()
            };
            out.push(Batch {
                topic: head.topic,
                partition: head.partition,
                start: head.start,
                end: head.end,
                log_index: head.log_index,
                topic_id: head.topic_id,
                base_timestamp: head.base_timestamp,
                max_timestamp: head.max_timestamp,
                bytes,
            });
        }
        Ok(out)
    }

    /// Highest log index a purge may remove: below the oldest entry that
    /// still holds an unsealed batch, and no further than what is applied.
    /// `None` before anything is applied.
    pub fn purge_floor(&self) -> Option<u64> {
        let state = self.state.lock().expect("wal state");
        let applied = state.last_applied?.index;
        match state.heads.by_key.keys().next() {
            Some(oldest) => (oldest >> 12)
                .checked_sub(1)
                .map(|index| index.min(applied)),
            None => Some(applied),
        }
    }

    /// Every unsealed batch's bytes, for a snapshot's bytes section.
    async fn snapshot_bytes(
        &self,
    ) -> Result<Vec<(u64, serde_bytes::ByteBuf)>, StorageError<NodeId>> {
        let heads: Vec<Head> = self
            .state
            .lock()
            .expect("wal state")
            .heads
            .in_commit_order()
            .cloned()
            .collect();
        let keys: Vec<u64> = heads.iter().map(Head::key).collect();
        let batches = self.materialize(heads).await?;
        Ok(keys
            .into_iter()
            .zip(batches)
            .map(|(key, batch)| (key, serde_bytes::ByteBuf::from(batch.bytes)))
            .collect())
    }
}

/// Runs of consecutive indexes as inclusive `(first, last)` pairs.
fn runs(indexes: &BTreeSet<u64>) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &index in indexes {
        match out.last_mut() {
            Some((_, last)) if *last + 1 == index => *last = index,
            _ => out.push((index, index)),
        }
    }
    out
}

fn missing(head: &Head) -> StorageError<NodeId> {
    io(
        ErrorSubject::StateMachine,
        &std::io::Error::other(format!(
            "bytes for batch at log index {} position {} are neither in the log nor detached",
            head.log_index, head.pos
        )),
    )
}

/// Offsets are kept per topic id, so a topic deleted and created again under
/// the same name starts at 0 like its metadata does.
fn part_key(topic_id: &[u8; 16], partition: i32) -> String {
    let hex: String = topic_id.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{hex}/{partition}")
}

fn span_key(topic_id: &[u8; 16], partition: i32, producer_id: i64) -> String {
    format!("{}/{}", part_key(topic_id, partition), producer_id)
}

/// `true` when recording these transactional batches stays within [`SPAN_CAP`].
fn spans_fit(
    state: &State,
    partition: &str,
    identities: &[camus_protocol::batch::ProducerBatch],
    transactional: &[bool],
) -> bool {
    let mut extra = 0usize;
    for (identity, flag) in identities.iter().zip(transactional) {
        if !*flag || identity.producer_id < 0 {
            continue;
        }
        let key = format!("{partition}/{}", identity.producer_id);
        if state.txn_spans.contains_key(&key) {
            continue;
        }
        extra += 1;
    }
    state.txn_spans.len().saturating_add(extra) <= SPAN_CAP
}

fn remember_span(state: &mut State, key: String, start: i64, end: i64) {
    state.dirty.spans.insert(key.clone());
    match state.txn_spans.get_mut(&key) {
        Some(span) if span.finished => {}
        Some(span) => {
            if end > span.end {
                span.end = end;
            }
        }
        None => {
            state.txn_spans.insert(
                key,
                OpenSpan {
                    start,
                    end,
                    finished: false,
                },
            );
        }
    }
}

/// `true` when one of these transactional producers was frozen by `FinishTxn`.
fn span_finished(
    state: &State,
    partition: &str,
    identities: &[camus_protocol::batch::ProducerBatch],
    transactional: &[bool],
) -> bool {
    identities
        .iter()
        .zip(transactional)
        .any(|(identity, flag)| {
            *flag
                && identity.producer_id >= 0
                && state
                    .txn_spans
                    .get(&format!("{partition}/{}", identity.producer_id))
                    .is_some_and(|span| span.finished)
        })
}

fn load_state(db: &Database) -> Result<State, StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let txn = db.begin_read().map_err(|e| err(&e))?;
    let mut state = match txn.open_table(META) {
        Ok(table) => match table.get(STATE).map_err(|e| err(&e))? {
            Some(raw) => serde_json::from_str::<State>(raw.value()).map_err(|e| err(&e))?,
            None => State::default(),
        },
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(State::default()),
        Err(e) => return Err(err(&e)),
    };
    // State written before the per-key tables kept offsets and producer
    // sequences in the JSON above. Without the tables, those stay as read and
    // the next flush writes them out.
    match txn.open_table(NEXT) {
        Ok(table) => {
            state.next.clear();
            for row in table.iter().map_err(|e| err(&e))? {
                let (key, next) = row.map_err(|e| err(&e))?;
                state.next.insert(key.value().to_string(), next.value());
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {
            state.dirty.next = state.next.keys().cloned().collect();
        }
        Err(e) => return Err(err(&e)),
    }
    match txn.open_table(PRODUCERS) {
        Ok(table) => {
            state.producers.clear();
            for row in table.iter().map_err(|e| err(&e))? {
                let (key, raw) = row.map_err(|e| err(&e))?;
                let seq = camus_raft::codec::decode(raw.value()).map_err(|e| err(&e))?;
                state.producers.insert(key.value().to_string(), seq);
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {
            state.dirty.producers = state.producers.keys().cloned().collect();
        }
        Err(e) => return Err(err(&e)),
    }
    match txn.open_table(TXN_SPANS) {
        Ok(table) => {
            state.txn_spans.clear();
            for row in table.iter().map_err(|e| err(&e))? {
                let (key, raw) = row.map_err(|e| err(&e))?;
                let span = camus_raft::codec::decode(raw.value()).map_err(|e| err(&e))?;
                state.txn_spans.insert(key.value().to_string(), span);
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {
            state.dirty.spans = state.txn_spans.keys().cloned().collect();
        }
        Err(e) => return Err(err(&e)),
    }
    let heads = match txn.open_table(HEADS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(state),
        Err(e) => return Err(err(&e)),
    };
    for row in heads.iter().map_err(|e| err(&e))? {
        let (_, head) = row.map_err(|e| err(&e))?;
        let head: Head = camus_raft::codec::decode(head.value()).map_err(|e| err(&e))?;
        state.heads.insert(head);
    }
    Ok(state)
}

fn load_snapshot(db: &Database) -> Result<Option<StoredSnapshot>, StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let txn = db.begin_read().map_err(|e| err(&e))?;
    let table = match txn.open_table(SNAPSHOTS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(err(&e)),
    };
    let Some(raw) = table.get(SNAPSHOT).map_err(|e| err(&e))? else {
        return Ok(None);
    };
    Ok(Some(
        camus_raft::codec::decode(raw.value()).map_err(|e| err(&e))?,
    ))
}

/// Write what changed since the last flush, in one durable commit.
fn write_changes(
    db: &Database,
    meta: &str,
    next: &[(String, Option<i64>)],
    producers: &[(String, Option<Vec<u8>>)],
    spans: &[(String, Option<Vec<u8>>)],
    dirty: &Dirty,
) -> Result<(), StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let txn = db.begin_write().map_err(|e| err(&e))?;
    {
        let mut table = txn.open_table(META).map_err(|e| err(&e))?;
        table.insert(STATE, meta).map_err(|e| err(&e))?;
        let mut offsets = txn.open_table(NEXT).map_err(|e| err(&e))?;
        for (key, value) in next {
            match value {
                Some(value) => offsets.insert(key.as_str(), *value).map(|_| ()),
                None => offsets.remove(key.as_str()).map(|_| ()),
            }
            .map_err(|e| err(&e))?;
        }
        let mut sequences = txn.open_table(PRODUCERS).map_err(|e| err(&e))?;
        for (key, raw) in producers {
            match raw {
                Some(raw) => sequences.insert(key.as_str(), raw.as_slice()).map(|_| ()),
                None => sequences.remove(key.as_str()).map(|_| ()),
            }
            .map_err(|e| err(&e))?;
        }
        let mut open = txn.open_table(TXN_SPANS).map_err(|e| err(&e))?;
        for (key, raw) in spans {
            match raw {
                Some(raw) => open.insert(key.as_str(), raw.as_slice()).map(|_| ()),
                None => open.remove(key.as_str()).map(|_| ()),
            }
            .map_err(|e| err(&e))?;
        }
        let mut heads = txn.open_table(HEADS).map_err(|e| err(&e))?;
        let mut detached = txn.open_table(DETACHED).map_err(|e| err(&e))?;
        for key in &dirty.removed {
            heads.remove(*key).map_err(|e| err(&e))?;
            detached.remove(*key).map_err(|e| err(&e))?;
        }
        for (key, head) in &dirty.added {
            let raw = camus_raft::codec::encode(head).map_err(|e| err(&e))?;
            heads.insert(*key, raw.as_slice()).map_err(|e| err(&e))?;
        }
    }
    txn.commit().map_err(|e| err(&e))?;
    Ok(())
}

fn batch_offsets(entry: &openraft::Entry<TypeConfig>) -> Vec<u64> {
    let count = match &entry.payload {
        EntryPayload::Normal(Command::Append(_)) => 1,
        EntryPayload::Normal(Command::Appends(batches)) => batches.len(),
        _ => 0,
    };
    (0..count)
        .filter_map(|pos| crate::log_batch_at(entry, pos).ok().flatten())
        .collect()
}

fn apply_one(state: &mut State, command: Command, log_index: u64, offsets: &[u64]) -> Response {
    apply_at(state, command, log_index, 0, offsets)
}

fn apply_at(
    state: &mut State,
    command: Command,
    log_index: u64,
    pos: u32,
    offsets: &[u64],
) -> Response {
    match command {
        Command::Appends(batches) => {
            let each = batches
                .into_iter()
                .take(MAX_PACKED)
                .enumerate()
                .map(|(pos, batch)| {
                    apply_at(
                        state,
                        Command::Append(batch),
                        log_index,
                        pos as u32,
                        offsets,
                    )
                })
                .collect();
            Response {
                base_offset: -1,
                error_code: 0,
                each,
            }
        }
        Command::Append(batch) => {
            let key = part_key(&batch.topic_id, batch.partition);
            let start = state.next.get(&key).copied().unwrap_or(0).max(batch.floor);
            let identities = match camus_protocol::batch::producer_batches(&batch.raw, MAX_BATCH) {
                Ok(identities) => identities,
                Err(_) => {
                    return Response {
                        base_offset: -1,
                        error_code: 2,
                        each: Vec::new(),
                    };
                }
            };
            let mut known = std::collections::BTreeMap::new();
            for identity in &identities {
                if identity.producer_id < 0 {
                    continue;
                }
                let seq_key = format!("{key}/{}", identity.producer_id);
                if let Some(seq) = state.producers.get(&seq_key) {
                    known.insert(identity.producer_id, seq.clone());
                }
            }
            let class = match camus_protocol::idempotence::classify(&known, &identities) {
                Ok(class) => class,
                Err(code) => {
                    return Response {
                        base_offset: -1,
                        error_code: code,
                        each: Vec::new(),
                    };
                }
            };
            if let camus_protocol::idempotence::Class::Replay { base_offset } = class {
                return Response {
                    base_offset,
                    error_code: 0,
                    each: Vec::new(),
                };
            }
            // Validate in place. Sealing rewrites the base offset and epoch,
            // which the read path does when it builds the bytes a consumer sees.
            let infos = match camus_protocol::batch::inspect(&batch.raw, MAX_BATCH) {
                Ok(infos) => infos,
                Err(_) => {
                    return Response {
                        base_offset: -1,
                        error_code: 2,
                        each: Vec::new(),
                    };
                }
            };
            let transactional: Vec<bool> = infos.iter().map(|info| info.transactional).collect();
            if let camus_protocol::idempotence::Class::Fresh = class {
                if span_finished(state, &key, &identities, &transactional)
                    || !spans_fit(state, &key, &identities, &transactional)
                {
                    return Response {
                        base_offset: -1,
                        error_code: 48,
                        each: Vec::new(),
                    };
                }
                let mut at = start;
                for (identity, flag) in identities.iter().zip(&transactional) {
                    if identity.producer_id >= 0 {
                        let seq_key = format!("{key}/{}", identity.producer_id);
                        state.dirty.producers.insert(seq_key.clone());
                        let seq = state.producers.entry(seq_key.clone()).or_default();
                        camus_protocol::idempotence::record(
                            seq,
                            identity.producer_epoch,
                            identity.base_sequence,
                            identity.records,
                            at,
                        );
                        if *flag {
                            let end = at + i64::from(identity.records);
                            remember_span(state, seq_key, at, end);
                        }
                    }
                    at += i64::from(identity.records);
                }
            }
            let end = start + i64::from(batch.records);
            state.dirty.next.insert(key.clone());
            state.next.insert(key, end);
            let head = Head {
                topic: batch.topic,
                partition: batch.partition,
                start,
                end,
                log_index,
                pos,
                topic_id: batch.topic_id,
                base_timestamp: batch.base_timestamp,
                max_timestamp: batch.max_timestamp,
                epoch: batch.epoch,
                len: batch.raw.len() as u64,
                at: offsets.get(pos as usize).copied().unwrap_or(0),
            };
            state.dirty.add(head.clone());
            state.heads.insert(head);
            Response {
                base_offset: start,
                error_code: 0,
                each: Vec::new(),
            }
        }
        Command::Seal { drops } => {
            for drop in drops {
                if let Some(head) = state
                    .heads
                    .remove(drop.topic_id, drop.partition, drop.start)
                {
                    state.dirty.remove(head.key());
                }
            }
            crate::SEALED.notify_waiters();
            Response {
                base_offset: -1,
                error_code: 0,
                each: Vec::new(),
            }
        }
        Command::FinishTxn {
            topic_id,
            partition,
            producer_id,
        } => {
            let key = span_key(&topic_id, partition, producer_id);
            if let Some(span) = state.txn_spans.get_mut(&key) {
                span.finished = true;
                state.dirty.spans.insert(key);
            } else if state.txn_spans.len() < SPAN_CAP {
                state.dirty.spans.insert(key.clone());
                state.txn_spans.insert(
                    key,
                    OpenSpan {
                        start: -1,
                        end: -1,
                        finished: true,
                    },
                );
            }
            Response {
                base_offset: -1,
                error_code: 0,
                each: Vec::new(),
            }
        }
        Command::CloseTxn {
            topic_id,
            partition,
            producer_id,
        } => {
            let key = span_key(&topic_id, partition, producer_id);
            if state.txn_spans.remove(&key).is_some() {
                state.dirty.spans.insert(key);
            }
            Response {
                base_offset: -1,
                error_code: 0,
                each: Vec::new(),
            }
        }
    }
}

fn io(subject: ErrorSubject<NodeId>, err: &dyn std::error::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, ErrorVerb::Write, AnyError::error(err.to_string())),
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<Machine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // The log may be purged up to this snapshot, so the applied state
        // has to be on disk first. Unsealed bytes stay in the log
        // (`purge_floor`); the snapshot carries a copy for a follower.
        self.flush()?;
        let bytes = self.snapshot_bytes().await?;
        let (body, last_applied, membership) = {
            let state = self.state.lock().expect("wal state");
            (
                SnapshotData {
                    next: state.next.clone(),
                    heads: state.heads.in_commit_order().cloned().collect(),
                    bytes,
                    producers: state.producers.clone(),
                    txn_spans: state.txn_spans.clone(),
                },
                state.last_applied,
                state.membership.clone().unwrap_or_default(),
            )
        };
        let data =
            camus_raft::codec::encode(&body).map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match &last_applied {
            Some(last) => format!("{}-{}-{snapshot_idx}", last.leader_id, last.index),
            None => format!("--{snapshot_idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        store_snapshot(&self.db, &stored)?;
        *self.current_snapshot.write().await = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

fn store_snapshot(db: &Database, stored: &StoredSnapshot) -> Result<(), StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let raw = camus_raft::codec::encode(stored).map_err(|e| err(&e))?;
    let txn = db.begin_write().map_err(|e| err(&e))?;
    {
        let mut table = txn.open_table(SNAPSHOTS).map_err(|e| err(&e))?;
        table
            .insert(SNAPSHOT, raw.as_slice())
            .map_err(|e| err(&e))?;
    }
    txn.commit().map_err(|e| err(&e))?;
    Ok(())
}

impl RaftStateMachine<TypeConfig> for Arc<Machine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let state = self.state.lock().expect("wal state");
        Ok((
            state.last_applied,
            state.membership.clone().unwrap_or_default(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut state = self.state.lock().expect("wal state");
        let mut responses = Vec::new();
        let mut membership_changed = false;
        for entry in entries {
            let offsets = batch_offsets(&entry);
            state.last_applied = Some(entry.log_id);
            state.dirty.meta = true;
            let response = match entry.payload {
                EntryPayload::Blank => Response {
                    base_offset: -1,
                    error_code: 0,
                    each: Vec::new(),
                },
                EntryPayload::Normal(command) => {
                    apply_one(&mut state, command, entry.log_id.index, &offsets)
                }
                EntryPayload::Membership(membership) => {
                    membership_changed = true;
                    state.membership = Some(StoredMembership::new(Some(entry.log_id), membership));
                    Response {
                        base_offset: -1,
                        error_code: 0,
                        each: Vec::new(),
                    }
                }
            };
            responses.push(response);
        }
        drop(state);
        // Rare, and the shard's voters should not depend on a replay.
        if membership_changed {
            self.flush()?;
        }
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let subject = || ErrorSubject::Snapshot(Some(meta.signature()));
        let parsed =
            decode_snapshot(&data).map_err(|err| io(subject(), &std::io::Error::other(err)))?;
        let state = State {
            last_applied: meta.last_log_id,
            membership: Some(meta.last_membership.clone()),
            next: parsed.next,
            producers: parsed.producers,
            txn_spans: parsed.txn_spans,
            heads: parsed.heads.into_iter().collect(),
            dirty: Dirty::default(),
        };
        // Replace every table in one durable commit. The snapshot's bytes
        // become detached, since this node's log will not hold their entries.
        let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
        {
            let _gate = self.flushing.lock().expect("wal flush");
            let txn = self.db.begin_write().map_err(|e| err(&e))?;
            {
                let mut table = txn.open_table(META).map_err(|e| err(&e))?;
                let json = serde_json::to_string(&state).map_err(|e| err(&e))?;
                table.insert(STATE, json.as_str()).map_err(|e| err(&e))?;
                let mut offsets = txn.open_table(NEXT).map_err(|e| err(&e))?;
                offsets.retain(|_, _| false).map_err(|e| err(&e))?;
                for (key, next) in &state.next {
                    offsets.insert(key.as_str(), *next).map_err(|e| err(&e))?;
                }
                let mut sequences = txn.open_table(PRODUCERS).map_err(|e| err(&e))?;
                sequences.retain(|_, _| false).map_err(|e| err(&e))?;
                for (key, seq) in &state.producers {
                    let raw = camus_raft::codec::encode(seq).map_err(|e| err(&e))?;
                    sequences
                        .insert(key.as_str(), raw.as_slice())
                        .map_err(|e| err(&e))?;
                }
                let mut open = txn.open_table(TXN_SPANS).map_err(|e| err(&e))?;
                open.retain(|_, _| false).map_err(|e| err(&e))?;
                for (key, span) in &state.txn_spans {
                    let raw = camus_raft::codec::encode(span).map_err(|e| err(&e))?;
                    open.insert(key.as_str(), raw.as_slice())
                        .map_err(|e| err(&e))?;
                }
                let mut heads = txn.open_table(HEADS).map_err(|e| err(&e))?;
                heads.retain(|_, _| false).map_err(|e| err(&e))?;
                for head in state.heads.in_commit_order() {
                    let raw = camus_raft::codec::encode(head).map_err(|e| err(&e))?;
                    heads
                        .insert(head.key(), raw.as_slice())
                        .map_err(|e| err(&e))?;
                }
                let mut detached = txn.open_table(DETACHED).map_err(|e| err(&e))?;
                detached.retain(|_, _| false).map_err(|e| err(&e))?;
                for (key, bytes) in &parsed.bytes {
                    detached
                        .insert(*key, bytes.as_slice())
                        .map_err(|e| err(&e))?;
                }
            }
            txn.commit().map_err(|e| err(&e))?;
            *self.state.lock().expect("wal state") = state;
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        store_snapshot(&self.db, &stored)?;
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .as_ref()
            .map(|stored| Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(partition: i32, start: i64, end: i64, log_index: u64, pos: u32) -> Head {
        Head {
            topic: "events".into(),
            partition,
            start,
            end,
            log_index,
            pos,
            topic_id: [7; 16],
            base_timestamp: 0,
            max_timestamp: 0,
            epoch: 0,
            len: (end - start) as u64 * 10,
            at: 0,
        }
    }

    fn open(dir: &Path) -> Machine {
        let log = camus_raft::SegmentLogStore::open(dir.join("log")).unwrap();
        Machine::open(&dir.join("state.redb"), log).unwrap()
    }

    #[test]
    fn heads_are_found_by_partition_offset_and_commit_order() {
        let mut heads: Heads = [
            head(0, 0, 2, 1, 0),
            head(1, 0, 5, 1, 1),
            head(0, 2, 4, 2, 0),
            head(0, 4, 9, 3, 0),
        ]
        .into_iter()
        .collect();
        assert_eq!(heads.bytes, 20 + 50 + 20 + 50);
        let starts = |heads: &Heads, sealed_end, offset| -> Vec<i64> {
            heads
                .of_partition([7; 16], 0, sealed_end, offset)
                .map(|head| head.start)
                .collect()
        };
        assert_eq!(starts(&heads, 0, 0), vec![0, 2, 4]);
        assert_eq!(
            starts(&heads, 0, 3),
            vec![2, 4],
            "from the head holding offset 3"
        );
        assert_eq!(
            starts(&heads, 4, 0),
            vec![4],
            "nothing below the sealed end"
        );
        assert_eq!(starts(&heads, 0, 9), Vec::<i64>::new());
        let order: Vec<(i32, i64)> = heads
            .in_commit_order()
            .map(|head| (head.partition, head.start))
            .collect();
        assert_eq!(order, vec![(0, 0), (1, 0), (0, 2), (0, 4)]);

        assert!(heads.remove([7; 16], 0, 2).is_some());
        assert!(heads.remove([7; 16], 0, 2).is_none());
        assert_eq!(heads.bytes, 20 + 50 + 50);
        assert_eq!(starts(&heads, 0, 0), vec![0, 4]);
        heads.remove([7; 16], 1, 0);
        assert!(
            !heads.by_part.contains_key(&([7; 16], 1)),
            "an empty partition goes"
        );
    }

    #[test]
    fn a_head_added_and_sealed_between_flushes_is_never_written() {
        let mut dirty = Dirty::default();
        dirty.add(head(0, 0, 1, 1, 0));
        dirty.remove(head(0, 0, 1, 1, 0).key());
        assert!(dirty.added.is_empty() && dirty.removed.is_empty());
        dirty.remove(head(0, 1, 2, 2, 0).key());
        assert_eq!(
            dirty.removed.len(),
            1,
            "a flushed head's removal is written"
        );
    }

    #[test]
    fn a_failed_flush_gives_back_what_was_not_undone_since() {
        let mut taken = Dirty::default();
        taken.add(head(0, 0, 1, 1, 0));
        taken.add(head(0, 1, 2, 2, 0));
        taken.remove(head(0, 9, 10, 9, 0).key());
        taken.next.insert("a".into());
        let mut since = Dirty::default();
        // Sealed after the flush took it: it must not be written.
        since.remove(head(0, 0, 1, 1, 0).key());
        since.next.insert("b".into());
        since.restore(taken);
        assert_eq!(
            since.added.keys().copied().collect::<Vec<_>>(),
            vec![head(0, 1, 2, 2, 0).key()]
        );
        assert_eq!(
            since.removed.iter().copied().collect::<Vec<_>>(),
            vec![head(0, 9, 10, 9, 0).key()]
        );
        assert_eq!(since.next.len(), 2);
    }

    #[test]
    fn only_a_flush_writes_and_a_reopen_reads_it_back() {
        let dir = tempfile::tempdir().unwrap();
        {
            let machine = open(dir.path());
            {
                let mut state = machine.state.lock().unwrap();
                state.next.insert("p0".into(), 4);
                state.dirty.next.insert("p0".into());
                let seq = state.producers.entry("p0/9".into()).or_default();
                camus_protocol::idempotence::record(seq, 0, 0, 4, 0);
                state.dirty.producers.insert("p0/9".into());
                for head in [head(0, 0, 2, 1, 0), head(0, 2, 4, 2, 0)] {
                    state.dirty.add(head.clone());
                    state.heads.insert(head);
                }
            }
            drop(machine);
            let unflushed = open(dir.path());
            let state = unflushed.state.lock().unwrap();
            assert!(state.next.is_empty(), "apply's changes stay in memory");
            assert_eq!(state.heads.bytes, 0);
        }
        let machine = open(dir.path());
        {
            let mut state = machine.state.lock().unwrap();
            state.next.insert("p0".into(), 4);
            state.dirty.next.insert("p0".into());
            let seq = state.producers.entry("p0/9".into()).or_default();
            camus_protocol::idempotence::record(seq, 0, 0, 4, 0);
            state.dirty.producers.insert("p0/9".into());
            for head in [head(0, 0, 2, 1, 0), head(0, 2, 4, 2, 0)] {
                state.dirty.add(head.clone());
                state.heads.insert(head);
            }
        }
        machine.flush().unwrap();
        {
            let mut state = machine.state.lock().unwrap();
            assert!(state.dirty.is_empty());
            let sealed = state.heads.remove([7; 16], 0, 0).unwrap();
            state.dirty.remove(sealed.key());
        }
        machine.flush().unwrap();
        drop(machine);
        let reopened = open(dir.path());
        let state = reopened.state.lock().unwrap();
        assert_eq!(state.next.get("p0"), Some(&4));
        assert!(state.producers.contains_key("p0/9"));
        assert_eq!(
            state
                .heads
                .in_commit_order()
                .map(|head| head.start)
                .collect::<Vec<_>>(),
            vec![2],
            "the sealed head's row is gone"
        );
        assert!(state.dirty.is_empty());
    }

    #[test]
    fn state_from_before_the_key_tables_is_read_and_moved_on_the_next_flush() {
        let dir = tempfile::tempdir().unwrap();
        {
            // The old shape: offsets and producers inside the state JSON.
            let db = Database::create(dir.path().join("state.redb")).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(META).unwrap();
                let legacy = serde_json::json!({
                    "last_applied": null,
                    "membership": null,
                    "next": {"p0": 7},
                    "producers": {"p0/3": {"epoch": 0, "batches": []}},
                });
                table.insert(STATE, legacy.to_string().as_str()).unwrap();
            }
            txn.commit().unwrap();
        }
        let machine = open(dir.path());
        {
            let state = machine.state.lock().unwrap();
            assert_eq!(state.next.get("p0"), Some(&7));
            assert!(state.producers.contains_key("p0/3"));
            assert!(
                state.dirty.next.contains("p0"),
                "queued to move to its table"
            );
        }
        machine.flush().unwrap();
        drop(machine);
        let machine = open(dir.path());
        let state = machine.state.lock().unwrap();
        assert_eq!(state.next.get("p0"), Some(&7));
        assert!(state.producers.contains_key("p0/3"));
        assert!(state.dirty.is_empty(), "read from the tables now");
        let txn = machine.db.begin_read().unwrap();
        let json = txn.open_table(META).unwrap().get(STATE).unwrap().unwrap();
        assert!(!json.value().contains("\"next\""), "{}", json.value());
    }

    #[tokio::test]
    async fn a_tail_read_loads_one_batch() -> std::io::Result<()> {
        let raw0 = one_batch("alpha-batch")?;
        let raw1 = one_batch("beta-batch-is-longer")?;
        let topic_id = [9u8; 16];
        let entry = openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(Command::Appends(vec![
                produced(topic_id, 0, raw0.clone()),
                produced(topic_id, 1, raw1.clone()),
            ])),
        };
        let dir = tempfile::tempdir().map_err(|err| std::io::Error::other(err))?;
        let log = camus_raft::SegmentLogStore::open(dir.path().join("log"))
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        let mut encoded = Vec::new();
        crate::encode_log(&entry, &mut encoded)?;
        log.write_record(1, encoded)?;
        let mut machine = Arc::new(
            Machine::open(&dir.path().join("state.redb"), log)
                .map_err(|err| std::io::Error::other(err.to_string()))?,
        );
        openraft::storage::RaftStateMachine::apply(&mut machine, [entry])
            .await
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        let got = machine
            .read(&topic_id, 1, 0, 0, 1 << 20)
            .await
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        let read = machine
            .log
            .stats()
            .read_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        if read != raw1.len() as u64 {
            return Err(std::io::Error::other(format!(
                "read {read} bytes, the batch is {}",
                raw1.len()
            )));
        }
        if !got
            .windows(b"beta-batch-is-longer".len())
            .any(|window| window == b"beta-batch-is-longer")
        {
            return Err(std::io::Error::other(
                "the fetched batch is not the one asked for",
            ));
        }
        if got
            .windows(b"alpha-batch".len())
            .any(|window| window == b"alpha-batch")
        {
            return Err(std::io::Error::other("the fetch included the other batch"));
        }
        Ok(())
    }

    #[test]
    fn a_transaction_span_keeps_its_start_until_it_is_closed() -> Result<(), String> {
        let mut state = State::default();
        let topic_id = [7u8; 16];
        let first = apply_one(&mut state, txn_append(true, 9, 0, 1)?, 1, &[0]);
        if first.error_code != 0 || first.base_offset != 0 {
            return Err(format!("first {first:?}"));
        }
        let key = span_key(&topic_id, 0, 9);
        let Some(span) = state.txn_spans.get(&key) else {
            return Err("missing span".into());
        };
        if span.start != 0 || span.end != 1 {
            return Err(format!("opened {span:?}"));
        }
        let second = apply_one(&mut state, txn_append(true, 9, 1, 2)?, 2, &[0]);
        if second.error_code != 0 {
            return Err(format!("second {second:?}"));
        }
        let Some(span) = state.txn_spans.get(&key) else {
            return Err("span dropped".into());
        };
        if span.start != 0 || span.end != 3 {
            return Err(format!("extended {span:?}"));
        }
        let plain = apply_one(&mut state, txn_append(false, 4, 0, 1)?, 3, &[0]);
        if plain.error_code != 0 || state.txn_spans.contains_key(&span_key(&topic_id, 0, 4)) {
            return Err("a normal batch opened a span".into());
        }
        apply_one(
            &mut state,
            Command::FinishTxn {
                topic_id,
                partition: 0,
                producer_id: 9,
            },
            4,
            &[],
        );
        let Some(span) = state.txn_spans.get(&key) else {
            return Err("finish dropped the span".into());
        };
        if !span.finished || span.start != 0 || span.end != 3 {
            return Err(format!("frozen {span:?}"));
        }
        let late = apply_one(&mut state, txn_append(true, 9, 3, 1)?, 5, &[0]);
        if late.error_code != 48 {
            return Err(format!("late {late:?}"));
        }
        let Some(span) = state.txn_spans.get(&key) else {
            return Err("late batch dropped the span".into());
        };
        if span.end != 3 {
            return Err(format!("late batch moved the end to {}", span.end));
        }
        apply_one(
            &mut state,
            Command::CloseTxn {
                topic_id,
                partition: 0,
                producer_id: 9,
            },
            6,
            &[],
        );
        if state.txn_spans.contains_key(&key) {
            return Err("close left the span".into());
        }
        Ok(())
    }

    fn txn_append(
        transactional: bool,
        producer_id: i64,
        sequence: i32,
        records: i32,
    ) -> Result<Command, String> {
        use camus_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
            NO_PARTITION_LEADER_EPOCH,
        };
        let records_out = (0..records)
            .map(|index| Record {
                transactional,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id,
                producer_epoch: 0,
                sequence: sequence + index,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                timestamp: 9,
                key: None,
                value: Some(bytes::Bytes::from_static(b"v")),
                headers: Default::default(),
            })
            .collect::<Vec<_>>();
        let mut buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            &records_out,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .map_err(|err| err.to_string())?;
        Ok(Command::Append(crate::Append {
            topic: "orders".into(),
            partition: 0,
            topic_id: [7; 16],
            epoch: 1,
            raw: buf.freeze(),
            records,
            base_timestamp: 0,
            max_timestamp: 0,
            floor: 0,
        }))
    }

    fn one_batch(value: &str) -> std::io::Result<bytes::Bytes> {
        use camus_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
            NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
        };
        let records = vec![Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 9,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
            headers: Default::default(),
        }];
        let mut buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .map_err(|err| std::io::Error::other(err.to_string()))?;
        Ok(buf.freeze())
    }

    fn produced(topic_id: [u8; 16], partition: i32, raw: bytes::Bytes) -> crate::Append {
        crate::Append {
            topic: "events".into(),
            partition,
            topic_id,
            epoch: 1,
            records: 1,
            base_timestamp: 9,
            max_timestamp: 9,
            floor: 0,
            raw,
        }
    }
}
