//! State machine for one WAL shard.
//!
//! Committed batches live here until the uploader publishes them and a
//! `Seal` entry drops them. The Raft log is the recovery source until that
//! seal is purged.
//!
//! Apply is on the produce ack path, so it writes only what changed (the new
//! batch, or the sealed ones' keys) in a redb commit that does not fsync.
//! After a crash redb rolls back to its last durable commit and openraft
//! re-applies the log from there. [`Machine::flush`] makes the state durable
//! before the log under it is purged or a snapshot is built.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Command, NodeId, Response, TypeConfig};

const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// Unsealed batch headers by log index, as JSON with empty `bytes`.
const HEADS: TableDefinition<u64, &str> = TableDefinition::new("heads");
/// Unsealed batch bytes by log index.
const BYTES: TableDefinition<u64, &[u8]> = TableDefinition::new("bytes");
const STATE: &str = "state";
const SNAPSHOT: &str = "snapshot";

#[derive(Serialize, Deserialize, Clone)]
pub struct Batch {
    pub topic: String,
    pub partition: i32,
    pub start: i64,
    pub end: i64,
    pub log_index: u64,
    /// Position inside a packed entry. `0` for a single append.
    #[serde(default)]
    pub pos: u32,
    pub topic_id: [u8; 16],
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct State {
    last_applied: Option<LogId<NodeId>>,
    #[serde(default)]
    membership: Option<StoredMembership<NodeId, openraft::BasicNode>>,
    #[serde(default)]
    next: BTreeMap<String, i64>,
    #[serde(default)]
    batches: Vec<Batch>,
}

/// What changed in one apply call.
#[derive(Default)]
struct Delta {
    added: Vec<Batch>,
    removed: Vec<u64>,
}

/// `State` without the batches, which live in their own tables.
#[derive(Serialize)]
struct MetaRef<'a> {
    last_applied: &'a Option<LogId<NodeId>>,
    membership: &'a Option<StoredMembership<NodeId, openraft::BasicNode>>,
    next: &'a BTreeMap<String, i64>,
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

pub struct Machine {
    db: Database,
    state: std::sync::Mutex<State>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl Machine {
    pub fn open(path: &Path) -> Result<Self, StorageError<NodeId>> {
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
        Ok(Self {
            db,
            state: std::sync::Mutex::new(state),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
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
        state
            .batches
            .iter()
            .map(|batch| batch.bytes.len() as u64)
            .sum()
    }

    pub fn pending(&self) -> Vec<Batch> {
        let mut batches = self.state.lock().expect("wal state").batches.clone();
        batches.sort_by_key(|batch| (batch.topic.clone(), batch.partition, batch.start));
        batches
    }

    /// Batches at or after `sealed_end` that contain `offset`.
    pub fn read(
        &self,
        topic_id: &[u8; 16],
        partition: i32,
        sealed_end: i64,
        offset: i64,
    ) -> Vec<u8> {
        let state = self.state.lock().expect("wal state");
        let mut out = Vec::new();
        for batch in &state.batches {
            if &batch.topic_id != topic_id || batch.partition != partition {
                continue;
            }
            if batch.start < sealed_end || batch.end <= offset {
                continue;
            }
            out.extend_from_slice(&batch.bytes);
        }
        out
    }
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
    let bytes = txn.open_table(BYTES).map_err(|e| err(&e))?;
    for row in heads.iter().map_err(|e| err(&e))? {
        let (index, head) = row.map_err(|e| err(&e))?;
        let mut batch: Batch = serde_json::from_str(head.value()).map_err(|e| err(&e))?;
        if let Some(raw) = bytes.get(index.value()).map_err(|e| err(&e))? {
            batch.bytes = raw.value().to_vec();
        }
        state.batches.push(batch);
    }
    Ok(state)
}

/// Write one apply's changes. `durable` decides whether this commit fsyncs.
fn persist(
    db: &Database,
    state: &State,
    delta: &Delta,
    durable: bool,
) -> Result<(), StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let meta = serde_json::to_string(&MetaRef {
        last_applied: &state.last_applied,
        membership: &state.membership,
        next: &state.next,
    })
    .map_err(|e| err(&e))?;
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
        let mut bytes = txn.open_table(BYTES).map_err(|e| err(&e))?;
        for index in &delta.removed {
            heads.remove(*index).map_err(|e| err(&e))?;
            bytes.remove(*index).map_err(|e| err(&e))?;
        }
        for batch in &delta.added {
            let head = Batch {
                bytes: Vec::new(),
                ..batch.clone()
            };
            let json = serde_json::to_string(&head).map_err(|e| err(&e))?;
            heads
                .insert(batch.storage_key(), json.as_str())
                .map_err(|e| err(&e))?;
            bytes
                .insert(batch.storage_key(), batch.bytes.as_slice())
                .map_err(|e| err(&e))?;
        }
    }
    txn.commit().map_err(|e| err(&e))?;
    Ok(())
}

/// Replace every stored batch with `state`'s, durably. Used for snapshots.
fn persist_all(db: &Database, state: &State) -> Result<(), StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    {
        let txn = db.begin_write().map_err(|e| err(&e))?;
        {
            let mut heads = txn.open_table(HEADS).map_err(|e| err(&e))?;
            heads.retain(|_, _| false).map_err(|e| err(&e))?;
            let mut bytes = txn.open_table(BYTES).map_err(|e| err(&e))?;
            bytes.retain(|_, _| false).map_err(|e| err(&e))?;
        }
        txn.commit().map_err(|e| err(&e))?;
    }
    let delta = Delta {
        added: state.batches.clone(),
        removed: Vec::new(),
    };
    persist(db, state, &delta, true)
}

/// Most batches one packed entry may hold; the storage key reserves 12 bits.
pub const MAX_PACKED: usize = 4096;

impl Batch {
    /// redb key: the log index with the position in its entry.
    fn storage_key(&self) -> u64 {
        (self.log_index << 12) | u64::from(self.pos)
    }
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
            let sealed =
                match camus_protocol::batch::seal(&batch.raw, start, batch.epoch, 1024 * 1024) {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        return Response {
                            base_offset: -1,
                            error_code: 2,
                            each: Vec::new(),
                        }
                    }
                };
            let end = start + i64::from(batch.records);
            state.next.insert(key, end);
            let stored = Batch {
                topic: batch.topic,
                partition: batch.partition,
                start,
                end,
                log_index,
                pos,
                topic_id: batch.topic_id,
                base_timestamp: batch.base_timestamp,
                max_timestamp: batch.max_timestamp,
                bytes: sealed.to_vec(),
            };
            delta.added.push(stored.clone());
            state.batches.push(stored);
            Response {
                base_offset: start,
                error_code: 0,
                each: Vec::new(),
            }
        }
        Command::Seal { drops } => {
            state.batches.retain(|batch| {
                let sealed = drops.iter().any(|drop| {
                    drop.topic_id == batch.topic_id
                        && drop.partition == batch.partition
                        && drop.start == batch.start
                });
                if sealed {
                    delta.removed.push(batch.storage_key());
                }
                !sealed
            });
            // A batch added and sealed in the same apply call never reaches
            // the tables.
            delta
                .added
                .retain(|batch| !delta.removed.contains(&batch.storage_key()));
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
        // openraft may purge the log up to this snapshot, so the state it
        // covers has to be on disk first.
        self.flush()?;
        let state = self.state.lock().expect("wal state").clone();
        let bytes =
            serde_json::to_vec(&state).map_err(|err| io(ErrorSubject::StateMachine, &err))?;
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
            data: bytes.clone(),
        };
        {
            let txn = self
                .db
                .begin_write()
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            {
                let mut table = txn
                    .open_table(META)
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                let json = serde_json::to_string(&stored)
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                table
                    .insert(SNAPSHOT, json.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            }
            txn.commit()
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        }
        *self.current_snapshot.write().await = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
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
        let bytes = snapshot.into_inner();
        let mut state: State = serde_json::from_slice(&bytes)
            .map_err(|err| io(ErrorSubject::Snapshot(Some(meta.signature())), &err))?;
        state.last_applied = meta.last_log_id;
        state.membership = Some(meta.last_membership.clone());
        persist_all(&self.db, &state)?;
        *self.state.lock().expect("wal state") = state;
        *self.current_snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
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
