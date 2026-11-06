//! State machine for one WAL shard.
//!
//! Committed batches stay here until the uploader publishes them and a `Seal`
//! entry drops them. The state machine keeps each batch's header (offsets,
//! where it is in the log, its length), not its bytes: the bytes are already
//! in the Raft log entry they arrived in, and reads take them from there. A
//! batch is written to disk once.
//!
//! openraft may purge log entries once a snapshot covers them. Before a
//! snapshot is handed out, every unsealed batch is copied into the `DETACHED`
//! table, so its bytes outlive the purge; the snapshot carries them too, for
//! a follower that installs it. Batches are normally sealed within the upload
//! interval, long before a snapshot, so this copy is the exception.
//!
//! Apply is on the produce ack path, so it writes only what changed in a redb
//! commit that does not fsync. After a crash redb rolls back to its last
//! durable commit and openraft re-applies the log from there.
//! [`Machine::flush`] makes the state durable before the log under it can go.

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
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Command, NodeId, Response, TypeConfig};

const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// Unsealed batch headers by storage key, postcard.
const HEADS: TableDefinition<u64, &[u8]> = TableDefinition::new("heads");
/// Bytes of unsealed batches whose log entry a snapshot may purge.
const DETACHED: TableDefinition<u64, &[u8]> = TableDefinition::new("detached");
/// The latest snapshot, postcard.
const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");
const STATE: &str = "state";
const SNAPSHOT: &str = "current";

/// Largest client batch the shard re-seals when it reads one back.
const MAX_BATCH: usize = 1024 * 1024;

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

#[derive(Serialize, Deserialize, Default, Clone)]
struct State {
    last_applied: Option<LogId<NodeId>>,
    #[serde(default)]
    membership: Option<StoredMembership<NodeId, openraft::BasicNode>>,
    #[serde(default)]
    next: BTreeMap<String, i64>,
    #[serde(skip)]
    heads: Vec<Head>,
}

/// What a snapshot carries besides openraft's meta.
#[derive(Serialize, Deserialize)]
struct SnapshotData {
    next: BTreeMap<String, i64>,
    heads: Vec<Head>,
    /// Sealed bytes of every head, by storage key.
    bytes: Vec<(u64, serde_bytes::ByteBuf)>,
}

/// What changed in one apply call.
#[derive(Default)]
struct Delta {
    added: Vec<Head>,
    removed: Vec<u64>,
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
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(current),
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

    /// Make every applied change durable. Call before purging the log.
    pub fn flush(&self) -> Result<(), StorageError<NodeId>> {
        let state = self.state.lock().expect("wal state");
        persist(&self.db, &state, &Delta::default(), true)
    }

    pub fn unsealed_bytes(&self) -> u64 {
        let state = self.state.lock().expect("wal state");
        state.heads.iter().map(|head| head.len).sum()
    }

    /// The oldest unsealed batches in commit order, with their bytes, up to
    /// `limit` bytes (at least one batch). Commit order keeps each
    /// partition's batches in offset order and lets no partition starve.
    pub async fn pending(&self, limit: u64) -> Result<Vec<Batch>, StorageError<NodeId>> {
        let mut heads = self.state.lock().expect("wal state").heads.clone();
        heads.sort_by_key(Head::key);
        let mut size = 0u64;
        let mut taken = 0;
        for head in &heads {
            if taken > 0 && size + head.len > limit {
                break;
            }
            size += head.len;
            taken += 1;
        }
        heads.truncate(taken);
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
        let mut heads: Vec<Head> = {
            let state = self.state.lock().expect("wal state");
            state
                .heads
                .iter()
                .filter(|head| {
                    &head.topic_id == topic_id
                        && head.partition == partition
                        && head.start >= sealed_end
                        && head.end > offset
                })
                .cloned()
                .collect()
        };
        heads.sort_by_key(|head| head.start);
        let mut size = 0u64;
        heads.retain(|head| {
            let keep = size == 0 || size + head.len <= limit as u64;
            if keep {
                size += head.len;
            }
            keep
        });
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
            .filter(|head| !detached.contains_key(&head.key()))
            .map(|head| head.log_index)
            .collect();
        let mut entries: BTreeMap<u64, Command> = BTreeMap::new();
        if let (Some(first), Some(last)) = (wanted.first(), wanted.last()) {
            let mut log = self.log.clone();
            for entry in log.try_get_log_entries(*first..=*last).await? {
                if !wanted.contains(&entry.log_id.index) {
                    continue;
                }
                if let EntryPayload::Normal(command) = entry.payload {
                    entries.insert(entry.log_id.index, command);
                }
            }
        }
        let mut out = Vec::with_capacity(heads.len());
        for head in heads {
            let bytes = if let Some(bytes) = detached.remove(&head.key()) {
                bytes
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

    /// Copy every unsealed batch's bytes into `DETACHED`, durably, so a log
    /// purge cannot take them. Returns the snapshot's bytes section.
    async fn detach_all(&self) -> Result<Vec<(u64, serde_bytes::ByteBuf)>, StorageError<NodeId>> {
        let heads = self.state.lock().expect("wal state").heads.clone();
        let batches = self.materialize(heads.clone()).await?;
        let txn = self
            .db
            .begin_write()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let mut out = Vec::with_capacity(heads.len());
        {
            let mut table = txn
                .open_table(DETACHED)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            for (head, batch) in heads.iter().zip(batches) {
                table
                    .insert(head.key(), batch.bytes.as_slice())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                out.push((head.key(), serde_bytes::ByteBuf::from(batch.bytes)));
            }
        }
        txn.commit()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        Ok(out)
    }
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
    let heads = match txn.open_table(HEADS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(state),
        Err(e) => return Err(err(&e)),
    };
    for row in heads.iter().map_err(|e| err(&e))? {
        let (_, head) = row.map_err(|e| err(&e))?;
        let head: Head = camus_raft::codec::decode(head.value()).map_err(|e| err(&e))?;
        state.heads.push(head);
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

/// Write one apply's changes. `durable` decides whether this commit fsyncs.
fn persist(
    db: &Database,
    state: &State,
    delta: &Delta,
    durable: bool,
) -> Result<(), StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let meta = serde_json::to_string(state).map_err(|e| err(&e))?;
    let mut txn = db.begin_write().map_err(|e| err(&e))?;
    txn.set_durability(if durable {
        redb::Durability::Immediate
    } else {
        redb::Durability::None
    });
    {
        let mut table = txn.open_table(META).map_err(|e| err(&e))?;
        table.insert(STATE, meta.as_str()).map_err(|e| err(&e))?;
        let mut heads = txn.open_table(HEADS).map_err(|e| err(&e))?;
        let mut detached = txn.open_table(DETACHED).map_err(|e| err(&e))?;
        for key in &delta.removed {
            heads.remove(*key).map_err(|e| err(&e))?;
            detached.remove(*key).map_err(|e| err(&e))?;
        }
        for head in &delta.added {
            let raw = camus_raft::codec::encode(head).map_err(|e| err(&e))?;
            heads
                .insert(head.key(), raw.as_slice())
                .map_err(|e| err(&e))?;
        }
    }
    txn.commit().map_err(|e| err(&e))?;
    Ok(())
}

fn apply_one(state: &mut State, command: Command, log_index: u64, delta: &mut Delta) -> Response {
    apply_at(state, command, log_index, 0, delta)
}

fn apply_at(
    state: &mut State,
    command: Command,
    log_index: u64,
    pos: u32,
    delta: &mut Delta,
) -> Response {
    match command {
        Command::Appends(batches) => {
            let each = batches
                .into_iter()
                .take(MAX_PACKED)
                .enumerate()
                .map(|(pos, batch)| {
                    apply_at(state, Command::Append(batch), log_index, pos as u32, delta)
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
            // Validate here, where every replica decides the same way. The
            // sealed bytes are rebuilt from the log entry when read.
            if camus_protocol::batch::seal(&batch.raw, start, batch.epoch, MAX_BATCH).is_err() {
                return Response {
                    base_offset: -1,
                    error_code: 2,
                    each: Vec::new(),
                };
            }
            let end = start + i64::from(batch.records);
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
            };
            delta.added.push(head.clone());
            state.heads.push(head);
            Response {
                base_offset: start,
                error_code: 0,
                each: Vec::new(),
            }
        }
        Command::Seal { drops } => {
            state.heads.retain(|head| {
                let sealed = drops.iter().any(|drop| {
                    drop.topic_id == head.topic_id
                        && drop.partition == head.partition
                        && drop.start == head.start
                });
                if sealed {
                    delta.removed.push(head.key());
                }
                !sealed
            });
            // A batch added and sealed in the same apply call never reaches
            // the tables.
            delta
                .added
                .retain(|head| !delta.removed.contains(&head.key()));
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
        // openraft may purge the log up to this snapshot. The applied state
        // has to be on disk, and every unsealed batch's bytes have to live
        // somewhere other than the log, first.
        self.flush()?;
        let bytes = self.detach_all().await?;
        let state = self.state.lock().expect("wal state").clone();
        let data = camus_raft::codec::encode(&SnapshotData {
            next: state.next.clone(),
            heads: state.heads.clone(),
            bytes,
        })
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match &state.last_applied {
            Some(last) => format!("{}-{}-{snapshot_idx}", last.leader_id, last.index),
            None => format!("--{snapshot_idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: state.last_applied,
            last_membership: state.membership.clone().unwrap_or_default(),
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
        let mut delta = Delta::default();
        let mut membership_changed = false;
        for entry in entries {
            state.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => Response {
                    base_offset: -1,
                    error_code: 0,
                    each: Vec::new(),
                },
                EntryPayload::Normal(command) => {
                    apply_one(&mut state, command, entry.log_id.index, &mut delta)
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
        persist(&self.db, &state, &delta, membership_changed)?;
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
        let parsed: SnapshotData =
            camus_raft::codec::decode(&data).map_err(|err| io(subject(), &err))?;
        let state = State {
            last_applied: meta.last_log_id,
            membership: Some(meta.last_membership.clone()),
            next: parsed.next,
            heads: parsed.heads,
        };
        // Replace every stored head; the snapshot's bytes become detached,
        // since this node's log will not hold their entries.
        let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
        {
            let txn = self.db.begin_write().map_err(|e| err(&e))?;
            {
                let mut heads = txn.open_table(HEADS).map_err(|e| err(&e))?;
                heads.retain(|_, _| false).map_err(|e| err(&e))?;
                let mut detached = txn.open_table(DETACHED).map_err(|e| err(&e))?;
                detached.retain(|_, _| false).map_err(|e| err(&e))?;
                for (key, bytes) in &parsed.bytes {
                    detached
                        .insert(*key, bytes.as_slice())
                        .map_err(|e| err(&e))?;
                }
            }
            txn.commit().map_err(|e| err(&e))?;
        }
        let delta = Delta {
            added: state.heads.clone(),
            removed: Vec::new(),
        };
        persist(&self.db, &state, &delta, true)?;
        *self.state.lock().expect("wal state") = state;
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
