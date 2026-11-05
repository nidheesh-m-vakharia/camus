//! `redb` state machine for the metadata group.
//!
//! Applies are one write transaction: the key, the last applied log id, and
//! the membership commit together or not at all.

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

const KV: TableDefinition<&str, &str> = TableDefinition::new("kv");
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");

const LAST_APPLIED: &str = "last_applied";
const MEMBERSHIP: &str = "membership";
const SNAPSHOT: &str = "snapshot";

#[derive(Serialize, Deserialize, Default)]
struct SnapshotBody {
    data: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

/// On-disk metadata state machine.
pub struct RedbMachine {
    db: Database,
    snapshot_idx: AtomicU64,
    /// Latest snapshot, also stored in `redb` under [`SNAPSHOT`].
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl RedbMachine {
    /// Open or create the database file.
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
        let current = load_snapshot(&db)?;
        let snapshot_idx = current.as_ref().map(|_| 1).unwrap_or(0);
        Ok(Self {
            db,
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: RwLock::new(current),
        })
    }

    /// Local read of an applied key.
    pub fn get(&self, key: &str) -> Result<Option<String>, StorageError<NodeId>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let table = match txn.open_table(KV) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
        };
        let value = table
            .get(key)
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?
            .map(|guard| guard.value().to_owned());
        Ok(value)
    }
}

fn load_snapshot(db: &Database) -> Result<Option<StoredSnapshot>, StorageError<NodeId>> {
    let txn = db
        .begin_read()
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    let table = match txn.open_table(META) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
    };
    let Some(raw) = table
        .get(SNAPSHOT)
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?
    else {
        return Ok(None);
    };
    let stored =
        serde_json::from_str(raw.value()).map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    Ok(Some(stored))
}

fn read_applied(
    db: &Database,
) -> Result<
    (
        Option<LogId<NodeId>>,
        StoredMembership<NodeId, openraft::BasicNode>,
    ),
    StorageError<NodeId>,
> {
    let txn = db
        .begin_read()
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
    let table = match txn.open_table(META) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => {
            return Ok((None, StoredMembership::default()));
        }
        Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
    };
    let last_applied = match table
        .get(LAST_APPLIED)
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?
    {
        Some(raw) => Some(
            serde_json::from_str(raw.value())
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?,
        ),
        None => None,
    };
    let membership = match table
        .get(MEMBERSHIP)
        .map_err(|err| io(ErrorSubject::StateMachine, &err))?
    {
        Some(raw) => {
            serde_json::from_str(raw.value()).map_err(|err| io(ErrorSubject::StateMachine, &err))?
        }
        None => StoredMembership::default(),
    };
    Ok((last_applied, membership))
}

fn io(subject: ErrorSubject<NodeId>, err: &dyn std::error::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, ErrorVerb::Write, AnyError::error(err.to_string())),
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<RedbMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, membership) = read_applied(&self.db)?;
        let txn = self
            .db
            .begin_read()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let mut data = BTreeMap::new();
        if let Ok(table) = txn.open_table(KV) {
            for entry in table
                .iter()
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?
            {
                let (key, value) = entry.map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                data.insert(key.value().to_owned(), value.value().to_owned());
            }
        }
        let bytes = serde_json::to_vec(&SnapshotBody { data })
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match last_applied {
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

impl RaftStateMachine<TypeConfig> for Arc<RedbMachine> {
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
        read_applied(&self.db)
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut responses = Vec::with_capacity(entries.len());
        let txn = self
            .db
            .begin_write()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        {
            let mut kv = txn
                .open_table(KV)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            let mut meta = txn
                .open_table(META)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            for entry in entries {
                let applied = serde_json::to_string(&entry.log_id)
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                meta.insert(LAST_APPLIED, applied.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                match entry.payload {
                    EntryPayload::Blank => responses.push(Response { value: None }),
                    EntryPayload::Normal(Command::Put { key, value }) => {
                        let previous = kv
                            .insert(key.as_str(), value.as_str())
                            .map_err(|err| io(ErrorSubject::StateMachine, &err))?
                            .map(|old| old.value().to_owned());
                        responses.push(Response { value: previous });
                    }
                    EntryPayload::Membership(mem) => {
                        let membership = StoredMembership::new(Some(entry.log_id), mem);
                        let json = serde_json::to_string(&membership)
                            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                        meta.insert(MEMBERSHIP, json.as_str())
                            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                        responses.push(Response { value: None });
                    }
                }
            }
        }
        txn.commit()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
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
        let body: SnapshotBody = serde_json::from_slice(&bytes)
            .map_err(|err| io(ErrorSubject::Snapshot(Some(meta.signature())), &err))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        {
            let mut kv = txn
                .open_table(KV)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            let existing: Vec<String> = kv
                .iter()
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?
                .map(|row| row.map(|(key, _)| key.value().to_owned()))
                .collect::<Result<_, _>>()
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            for key in existing {
                kv.remove(key.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            }
            for (key, value) in &body.data {
                kv.insert(key.as_str(), value.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            }
            let mut meta_table = txn
                .open_table(META)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            if let Some(last) = &meta.last_log_id {
                let json = serde_json::to_string(last)
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                meta_table
                    .insert(LAST_APPLIED, json.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            }
            let membership = serde_json::to_string(&meta.last_membership)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            meta_table
                .insert(MEMBERSHIP, membership.as_str())
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            let stored = StoredSnapshot {
                meta: meta.clone(),
                data: bytes.clone(),
            };
            let json = serde_json::to_string(&stored)
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            meta_table
                .insert(SNAPSHOT, json.as_str())
                .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        }
        txn.commit()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
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
