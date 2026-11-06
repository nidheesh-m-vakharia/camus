//! State machine for one WAL shard.
//!
//! Committed batches live here until the uploader publishes them and a
//! `Seal` entry drops them. The Raft log is the recovery source until that
//! seal is purged.

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
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Command, NodeId, Response, TypeConfig};

const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
const STATE: &str = "state";
const SNAPSHOT: &str = "snapshot";

#[derive(Serialize, Deserialize, Clone)]
pub struct Batch {
    pub topic: String,
    pub partition: i32,
    pub start: i64,
    pub end: i64,
    pub log_index: u64,
    pub topic_id: [u8; 16],
    pub base_timestamp: i64,
    pub max_timestamp: i64,
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

    pub fn end_offset(&self, topic: &str, partition: i32) -> i64 {
        let state = self.state.lock().expect("wal state");
        state
            .next
            .get(&part_key(topic, partition))
            .copied()
            .unwrap_or(0)
    }

    pub fn pending(&self) -> Vec<Batch> {
        let mut batches = self.state.lock().expect("wal state").batches.clone();
        batches.sort_by_key(|batch| (batch.topic.clone(), batch.partition, batch.start));
        batches
    }

    /// Batches at or after `sealed_end` that contain `offset`.
    pub fn read(&self, topic: &str, partition: i32, sealed_end: i64, offset: i64) -> Vec<u8> {
        let state = self.state.lock().expect("wal state");
        let mut out = Vec::new();
        for batch in &state.batches {
            if batch.topic != topic || batch.partition != partition {
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

fn part_key(topic: &str, partition: i32) -> String {
    format!("{topic}/{partition}")
}

fn load_state(db: &Database) -> Result<State, StorageError<NodeId>> {
    let txn = db
        .begin_read()
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    let table = match txn.open_table(META) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(State::default()),
        Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
    };
    let Some(raw) = table
        .get(STATE)
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?
    else {
        return Ok(State::default());
    };
    serde_json::from_str(raw.value()).map_err(|err| io(ErrorSubject::StateMachine, &err))
}

fn persist(db: &Database, state: &State) -> Result<(), StorageError<NodeId>> {
    let json = serde_json::to_string(state).map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    let txn = db
        .begin_write()
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    {
        let mut table = txn
            .open_table(META)
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        table
            .insert(STATE, json.as_str())
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    }
    txn.commit()
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    Ok(())
}

fn apply_one(state: &mut State, command: Command, log_index: u64) -> Response {
    match command {
        Command::Append(batch) => {
            let key = part_key(&batch.topic, batch.partition);
            let start = state.next.get(&key).copied().unwrap_or(0).max(batch.floor);
            let sealed =
                match camus_protocol::batch::seal(&batch.raw, start, batch.epoch, 1024 * 1024) {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        return Response {
                            base_offset: -1,
                            error_code: 2,
                        }
                    }
                };
            let end = start + i64::from(batch.records);
            state.next.insert(key, end);
            state.batches.push(Batch {
                topic: batch.topic,
                partition: batch.partition,
                start,
                end,
                log_index,
                topic_id: batch.topic_id,
                base_timestamp: batch.base_timestamp,
                max_timestamp: batch.max_timestamp,
                bytes: sealed.to_vec(),
            });
            Response {
                base_offset: start,
                error_code: 0,
            }
        }
        Command::Seal { drops } => {
            state.batches.retain(|batch| {
                !drops.iter().any(|drop| {
                    drop.topic == batch.topic
                        && drop.partition == batch.partition
                        && drop.start == batch.start
                })
            });
            Response {
                base_offset: -1,
                error_code: 0,
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
        for entry in entries {
            state.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => Response {
                    base_offset: -1,
                    error_code: 0,
                },
                EntryPayload::Normal(command) => apply_one(&mut state, command, entry.log_id.index),
                EntryPayload::Membership(membership) => {
                    state.membership = Some(StoredMembership::new(Some(entry.log_id), membership));
                    Response {
                        base_offset: -1,
                        error_code: 0,
                    }
                }
            };
            responses.push(response);
        }
        persist(&self.db, &state)?;
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
        persist(&self.db, &state)?;
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
