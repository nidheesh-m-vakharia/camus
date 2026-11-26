//! `redb` state machine for the metadata group.
//!
//! Applies are one write transaction: the key, the last applied log id, and
//! the membership commit together or not at all.
//!
//! Apply is on every metadata write's path, so most applies commit without
//! fsync: the Raft log, fsynced on a quorum, is what makes a write durable.
//! After a crash redb rolls back to its last durable commit and openraft
//! re-applies the log from there. An apply is durable when it carries a
//! membership change or when [`DURABLE_EVERY`] has passed since the last
//! durable one, which bounds that replay. Storing a snapshot is a durable
//! commit too, so the state is on disk before openraft purges the log under
//! the snapshot.

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
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::acl;
use crate::admin;
use crate::catalog::{self, Outcome};
use crate::mirror;
use crate::txn;
use crate::{Command, NodeId, Response, TypeConfig};

const KV: TableDefinition<&str, &str> = TableDefinition::new("kv");
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");

/// The latest snapshot's body, as built or installed.
const SNAPSHOT_DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshot_data");

const LAST_APPLIED: &str = "last_applied";
const MEMBERSHIP: &str = "membership";
/// The latest snapshot's meta, JSON, in `META`; its body is in `SNAPSHOT_DATA`.
const SNAPSHOT_META: &str = "snapshot_meta";
const CURRENT: &str = "current";
/// The latest snapshot before `SNAPSHOT_DATA`: meta and body together, as
/// JSON, the body a JSON array of byte values. Read, never written.
const LEGACY_SNAPSHOT: &str = "snapshot";

/// First bytes of a snapshot body: the key-value rows follow, encoded as a
/// postcard `Vec<(String, String)>`. A body without them is the JSON
/// `SnapshotBody` written before.
const SNAPSHOT_MAGIC: &[u8; 8] = b"CAMUSKV2";

/// First and last bytes of a snapshot body with a block index (ADR 0018):
/// the rows exactly as after [`SNAPSHOT_MAGIC`], then the index, a postcard
/// [`IndexEntry`] per block, then a [`TRAILER_LEN`]-byte trailer.
/// `build_snapshot` writes it. Brokers read it from the release before, so a
/// rolling upgrade from that release installs every snapshot this one builds.
pub(crate) const INDEXED_MAGIC: &[u8; 8] = b"CAMUSKV3";

/// Index offset (u64 LE), index length (u64 LE), CRC32C of the index
/// (u32 LE), then [`INDEXED_MAGIC`].
pub(crate) const TRAILER_LEN: usize = 8 + 8 + 4 + 8;

/// One block of an indexed body: its first row's key, that row's byte offset
/// in the body, and the CRC32C of the block's bytes. A block runs from its
/// offset to the next block's, or to the index.
pub(crate) type IndexEntry = (String, u64, u32);

/// A row starts a new block once it is this far past the last block's start.
/// The index holds one entry per block, so it is about 1/65536 of the body.
const BLOCK_BYTES: u64 = 64 * 1024;

pub(crate) const CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// Longest time between durable applies.
const DURABLE_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

/// Snapshot body before [`SNAPSHOT_MAGIC`].
#[derive(Serialize, Deserialize, Default)]
struct SnapshotBody {
    data: BTreeMap<String, String>,
}

fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Encode every row of `kv` as an [`INDEXED_MAGIC`] snapshot body, straight
/// from the table.
fn encode_rows(kv: &impl ReadableTable<&'static str, &'static str>) -> Result<Vec<u8>, String> {
    let count = kv.len().map_err(|err| err.to_string())?;
    let mut body = IndexedBody::new(count);
    let mut written = 0u64;
    for row in kv.iter().map_err(|err| err.to_string())? {
        let (key, value) = row.map_err(|err| err.to_string())?;
        body.row(key.value(), value.value());
        written += 1;
    }
    if written != count {
        return Err(format!("snapshot read {written} rows of {count}"));
    }
    body.finish()
}

/// An [`INDEXED_MAGIC`] body being written, one row at a time in key order.
struct IndexedBody {
    out: Vec<u8>,
    /// Each block's first key and byte offset.
    firsts: Vec<(String, u64)>,
}

impl IndexedBody {
    /// A body that will hold `count` rows.
    fn new(count: u64) -> Self {
        let mut out = INDEXED_MAGIC.to_vec();
        push_varint(&mut out, count);
        Self {
            out,
            firsts: Vec::new(),
        }
    }

    fn row(&mut self, key: &str, value: &str) {
        let at = self.out.len() as u64;
        let starts_block = match self.firsts.last() {
            Some((_, start)) => at - start >= BLOCK_BYTES,
            None => true,
        };
        if starts_block {
            self.firsts.push((key.to_string(), at));
        }
        for text in [key, value] {
            push_varint(&mut self.out, text.len() as u64);
            self.out.extend_from_slice(text.as_bytes());
        }
    }

    /// Append the index, each block with its CRC32C, and the trailer.
    fn finish(self) -> Result<Vec<u8>, String> {
        let Self { mut out, firsts } = self;
        let index_at = out.len() as u64;
        let mut index: Vec<IndexEntry> = Vec::with_capacity(firsts.len());
        for (block, (key, start)) in firsts.iter().enumerate() {
            let end = firsts.get(block + 1).map_or(index_at, |(_, next)| *next);
            let bytes = out
                .get(*start as usize..end as usize)
                .ok_or("snapshot block out of bounds")?;
            index.push((key.clone(), *start, CRC32C.checksum(bytes)));
        }
        drop(firsts);
        let index = camus_raft::codec::encode(&index).map_err(|err| err.to_string())?;
        out.extend_from_slice(&index);
        out.extend_from_slice(&index_at.to_le_bytes());
        out.extend_from_slice(&(index.len() as u64).to_le_bytes());
        out.extend_from_slice(&CRC32C.checksum(&index).to_le_bytes());
        out.extend_from_slice(INDEXED_MAGIC);
        Ok(out)
    }
}

/// The rows of a snapshot body in either format.
/// Rows in a metadata snapshot body, the same bytes `build_snapshot` writes.
pub fn snapshot_rows(bytes: &[u8]) -> Result<Vec<(String, String)>, String> {
    decode_rows(bytes)
}

fn decode_rows(bytes: &[u8]) -> Result<Vec<(String, String)>, String> {
    if let Some(rows) = bytes.strip_prefix(SNAPSHOT_MAGIC) {
        return camus_raft::codec::decode(rows).map_err(|err| err.to_string());
    }
    if bytes.starts_with(INDEXED_MAGIC) {
        return decode_indexed(bytes);
    }
    serde_json::from_slice::<SnapshotBody>(bytes)
        .map(|body| body.data.into_iter().collect())
        .map_err(|err| err.to_string())
}

/// Rows of a [`INDEXED_MAGIC`] body. Install and restore read every row, so
/// they check every block: each index entry must start a row with its key,
/// and each block must match its CRC32C.
fn decode_indexed(bytes: &[u8]) -> Result<Vec<(String, String)>, String> {
    let (index_at, index) = indexed_parts(bytes)?;
    let rows_bytes = bytes
        .get(INDEXED_MAGIC.len()..index_at)
        .ok_or("snapshot index starts inside the magic")?;
    let rows: Vec<(String, String)> =
        camus_raft::codec::decode(rows_bytes).map_err(|err| err.to_string())?;
    let used = camus_raft::codec::encoded_len(&rows).map_err(|err| err.to_string())?;
    if used != rows_bytes.len() {
        return Err(format!(
            "snapshot rows end at byte {used} of {}",
            rows_bytes.len()
        ));
    }
    let entries: Vec<IndexEntry> =
        camus_raft::codec::decode(index).map_err(|err| err.to_string())?;
    check_blocks(bytes, index_at, &rows, &entries)?;
    Ok(rows)
}

/// Bytes postcard uses for the varint `value`.
fn varint_len(value: u64) -> usize {
    (u64::BITS - value.max(1).leading_zeros()).div_ceil(7) as usize
}

/// Check that `entries` start blocks on rows of `rows`, in order, from the
/// first row, and that each block's bytes match its CRC32C.
fn check_blocks(
    bytes: &[u8],
    index_at: usize,
    rows: &[(String, String)],
    entries: &[IndexEntry],
) -> Result<(), String> {
    let mut starts = Vec::with_capacity(entries.len());
    let mut at = INDEXED_MAGIC.len() + varint_len(rows.len() as u64);
    for (key, value) in rows {
        if let Some((first, offset, _)) = entries.get(starts.len()) {
            if *offset == at as u64 {
                if first != key {
                    return Err(format!(
                        "snapshot block {} names key {first:?}, its row is {key:?}",
                        starts.len()
                    ));
                }
                starts.push(at);
            }
        }
        if starts.is_empty() {
            return Err("snapshot index does not start at the first row".into());
        }
        for text in [key, value] {
            at += varint_len(text.len() as u64) + text.len();
        }
    }
    if starts.len() != entries.len() {
        return Err(format!(
            "snapshot block {} does not start on a row",
            starts.len()
        ));
    }
    for (block, ((_, _, crc), start)) in entries.iter().zip(&starts).enumerate() {
        let end = starts.get(block + 1).copied().unwrap_or(index_at);
        let bytes = bytes
            .get(*start..end)
            .ok_or("snapshot block is out of bounds")?;
        if CRC32C.checksum(bytes) != *crc {
            return Err(format!("snapshot block {block} fails its CRC32C"));
        }
    }
    Ok(())
}

/// Check an indexed body's trailer and the index's CRC32C, and return where
/// the rows end and the index's bytes.
fn indexed_parts(bytes: &[u8]) -> Result<(usize, &[u8]), String> {
    let trailer_at = bytes
        .len()
        .checked_sub(TRAILER_LEN)
        .filter(|at| *at >= INDEXED_MAGIC.len())
        .ok_or("snapshot is shorter than its trailer")?;
    let trailer = bytes
        .get(trailer_at..)
        .ok_or("snapshot trailer is missing")?;
    let (index_at, index_len, crc) = parse_trailer(trailer, bytes.len() as u64)?;
    let index_at = usize::try_from(index_at).map_err(|err| err.to_string())?;
    let index_len = usize::try_from(index_len).map_err(|err| err.to_string())?;
    let index = bytes
        .get(index_at..index_at + index_len)
        .ok_or("snapshot index is out of bounds")?;
    if CRC32C.checksum(index) != crc {
        return Err("snapshot index fails its CRC32C".into());
    }
    Ok((index_at, index))
}

/// The index offset, index length, and index CRC32C from an indexed body's
/// last [`TRAILER_LEN`] bytes, checked against the body's length: the index
/// must start after the magic and end where the trailer starts.
pub(crate) fn parse_trailer(trailer: &[u8], body_len: u64) -> Result<(u64, u64, u32), String> {
    if trailer.len() != TRAILER_LEN {
        return Err("snapshot trailer is truncated".into());
    }
    let le_u64 = |at: usize| -> Option<u64> {
        let field: [u8; 8] = trailer.get(at..at + 8)?.try_into().ok()?;
        Some(u64::from_le_bytes(field))
    };
    let (Some(index_at), Some(index_len)) = (le_u64(0), le_u64(8)) else {
        return Err("snapshot trailer is truncated".into());
    };
    let crc: [u8; 4] = trailer
        .get(16..20)
        .and_then(|field| field.try_into().ok())
        .ok_or("snapshot trailer is truncated")?;
    if trailer.get(20..) != Some(INDEXED_MAGIC.as_slice()) {
        return Err("snapshot trailer does not end in its magic".into());
    }
    let trailer_at = body_len.saturating_sub(TRAILER_LEN as u64);
    if index_at.checked_add(index_len) != Some(trailer_at) || index_at < INDEXED_MAGIC.len() as u64
    {
        return Err(format!(
            "snapshot index at {index_at}, {index_len} bytes, does not end at the trailer {trailer_at}"
        ));
    }
    Ok((index_at, index_len, u32::from_le_bytes(crc)))
}

/// Encode `rows`, in key order, as an [`INDEXED_MAGIC`] body.
#[cfg(test)]
pub(crate) fn encode_indexed(rows: &[(&str, &str)]) -> Result<Vec<u8>, String> {
    let mut body = IndexedBody::new(rows.len() as u64);
    for (key, value) in rows {
        body.row(key, value);
    }
    body.finish()
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

/// Store `stored` as the latest snapshot, in `txn`, and drop any legacy copy.
fn put_snapshot(txn: &redb::WriteTransaction, stored: &StoredSnapshot) -> Result<(), String> {
    let mut meta = txn.open_table(META).map_err(|err| err.to_string())?;
    let json = serde_json::to_string(&stored.meta).map_err(|err| err.to_string())?;
    meta.insert(SNAPSHOT_META, json.as_str())
        .map_err(|err| err.to_string())?;
    meta.remove(LEGACY_SNAPSHOT)
        .map_err(|err| err.to_string())?;
    let mut data = txn
        .open_table(SNAPSHOT_DATA)
        .map_err(|err| err.to_string())?;
    data.insert(CURRENT, stored.data.as_slice())
        .map_err(|err| err.to_string())?;
    Ok(())
}

/// On-disk metadata state machine.
pub struct RedbMachine {
    db: Database,
    snapshot_idx: AtomicU64,
    /// Latest snapshot, also stored in `redb` under [`SNAPSHOT`].
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    /// When an apply last committed with fsync.
    last_durable: std::sync::Mutex<std::time::Instant>,
    /// Bumped after each commit that may change the stored ACLs.
    acl_version: AtomicU64,
}

impl RedbMachine {
    /// The latest snapshot's log index and body, if one has been built.
    pub async fn snapshot_index_and_body(&self) -> Option<(u64, Vec<u8>)> {
        let stored = self.current_snapshot.read().await;
        let stored = stored.as_ref()?;
        let index = stored.meta.last_log_id?.index;
        Some((index, stored.data.clone()))
    }

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
            last_durable: std::sync::Mutex::new(std::time::Instant::now()),
            acl_version: AtomicU64::new(0),
        })
    }

    /// See [`crate::MetadataNode::acl_version`].
    pub fn acl_version(&self) -> u64 {
        self.acl_version.load(Ordering::Acquire)
    }

    /// Up to `limit` applied keys in `[from, to)`, in key order.
    pub fn scan_range(
        &self,
        from: &str,
        to: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, StorageError<NodeId>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let table = match txn.open_table(KV) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
        };
        scan_range(&table, from, to, limit)
            .map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))
    }

    /// Applied keys starting with `prefix`, in key order.
    pub fn scan(&self, prefix: &str) -> Result<Vec<(String, String)>, StorageError<NodeId>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        let table = match txn.open_table(KV) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(io(ErrorSubject::StateMachine, &err)),
        };
        let mut out = Vec::new();
        for row in table
            .range(prefix..)
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?
        {
            let (key, value) = row.map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            if !key.value().starts_with(prefix) {
                break;
            }
            out.push((key.value().to_string(), value.value().to_string()));
        }
        Ok(out)
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
    let fail = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    if let Some(raw) = table.get(SNAPSHOT_META).map_err(|e| fail(&e))? {
        let meta = serde_json::from_str(raw.value()).map_err(|e| fail(&e))?;
        let data = txn
            .open_table(SNAPSHOT_DATA)
            .map_err(|e| fail(&e))?
            .get(CURRENT)
            .map_err(|e| fail(&e))?
            .map(|raw| raw.value().to_vec())
            .unwrap_or_default();
        return Ok(Some(StoredSnapshot { meta, data }));
    }
    let Some(raw) = table.get(LEGACY_SNAPSHOT).map_err(|e| fail(&e))? else {
        return Ok(None);
    };
    let stored = serde_json::from_str(raw.value()).map_err(|e| fail(&e))?;
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

fn may_change_acls(command: &Command) -> bool {
    match command {
        Command::CreateAcls { .. } | Command::DeleteAcls { .. } => true,
        Command::Put { key, .. } | Command::Delete { key } => key == acl::KEY,
        Command::Batch(commands) => commands.iter().any(may_change_acls),
        _ => false,
    }
}

fn apply_command(
    kv: &mut redb::Table<&str, &str>,
    command: Command,
) -> Result<Response, StorageError<NodeId>> {
    if let Command::Batch(commands) = command {
        let mut each = Vec::with_capacity(commands.len());
        for command in commands {
            each.push(apply_command(kv, command)?);
        }
        return Ok(Response {
            value: None,
            error_code: 0,
            each,
        });
    }
    if let Command::SyncMirrorOffsets { group, offsets } = command {
        let read = |key: &str| -> Result<Option<String>, String> {
            match kv.get(key) {
                Ok(Some(guard)) => Ok(Some(guard.value().to_string())),
                Ok(None) => Ok(None),
                Err(err) => Err(err.to_string()),
            }
        };
        let ids = offsets.iter().map(|offset| offset.topic_id.as_str());
        let mirrors = mirror::all_mirrors(&read, ids)
            .map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))?;
        if !mirrors {
            return Ok(Response::reject(3));
        }
        return apply_group(
            kv,
            Command::CommitOffsets {
                group,
                generation: -1,
                member_id: String::new(),
                offsets,
            },
        );
    }
    match command {
        Command::SaveGroup { .. }
        | Command::CommitOffsets { .. }
        | Command::DeleteGroup { .. }
        | Command::ExpireOffsets { .. } => {
            return apply_group(kv, command);
        }
        Command::InitProducer {
            transactional,
            producer_id,
            producer_epoch,
        } => {
            return apply_init(kv, transactional, producer_id, producer_epoch);
        }
        _ => {}
    }
    if let Command::Put { key, value } = command {
        let previous = kv
            .insert(key.as_str(), value.as_str())
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?
            .map(|old| old.value().to_owned());
        return Ok(Response::ok(previous));
    }
    if let Command::Delete { key } = command {
        let previous = kv
            .remove(key.as_str())
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?
            .map(|old| old.value().to_owned());
        return Ok(Response::ok(previous));
    }
    let outcome = {
        let read = |key: &str| -> Result<Option<String>, String> {
            match kv.get(key) {
                Ok(Some(guard)) => Ok(Some(guard.value().to_string())),
                Ok(None) => Ok(None),
                Err(err) => Err(err.to_string()),
            }
        };
        let scan = |from: &str, to: &str, limit: usize| -> Result<Vec<(String, String)>, String> {
            scan_range(kv, from, to, limit)
        };
        match command {
            Command::Put { .. } | Command::Delete { .. } | Command::Batch(_) => {
                unreachable!("handled above")
            }
            Command::CreateTopic {
                name,
                id,
                partitions,
                durability,
                configs,
            } => catalog::create_topic(&read, &name, &id, partitions, &durability, &configs),
            Command::DeleteTopic { name } => catalog::delete_topic(&read, &scan, &name),
            Command::AddMirrorLink { link } => mirror::add_link(&read, &scan, &link),
            Command::RemoveMirrorLink { alias } => mirror::remove_link(&read, &alias),
            Command::CreateMirrorTopic {
                name,
                id,
                partitions,
                of,
            } => mirror::create_topic(&read, &name, &id, partitions, &of),
            Command::PublishMirror {
                parts,
                committed,
                aborted,
            } => {
                let committed: Vec<catalog::Span> = committed.iter().map(Into::into).collect();
                let aborted: Vec<catalog::Span> = aborted.iter().map(Into::into).collect();
                catalog::publish_mirror(&read, &parts, &committed, &aborted)
            }
            Command::PromoteMirror { topic } => mirror::promote(&read, &topic),
            Command::SyncMirrorOffsets { .. } => unreachable!("handled above"),
            Command::StampExtents { stamps } => catalog::stamp_extents(&read, &stamps),
            Command::SetMirrorGroups { alias, groups } => {
                mirror::set_groups(&read, &alias, &groups)
            }
            Command::TrimMirror { cuts } => {
                admin::trim_mirror(&read, &scan, &cuts).and_then(admin::outcome)
            }
            Command::ResetMirrorPartition { topic, partition } => {
                mirror::reset_partition(&read, &scan, &topic, partition)
            }
            Command::Publish { parts } => catalog::publish(&read, &parts, &[]),
            Command::PublishWithProducers { parts, producers } => {
                catalog::publish(&read, &parts, &producers)
            }
            Command::InitProducer { .. } => unreachable!("handled above"),
            Command::CreatePartitions { topics } => {
                admin::create_partitions(&read, &topics).and_then(admin::outcome)
            }
            Command::AlterConfigs { changes } => {
                admin::alter_configs(&read, &changes).and_then(admin::outcome)
            }
            Command::DeleteRecords { cuts } => {
                admin::delete_records(&read, &scan, &cuts).and_then(admin::outcome)
            }
            Command::OffsetDelete { forget } => {
                admin::offset_delete(&read, &forget).and_then(admin::outcome)
            }
            Command::InitTransaction {
                transactional_id,
                producer_id,
                producer_epoch,
            } => txn::outcome(txn::init(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                60_000,
                0,
            )),
            Command::AddTxnPartitions {
                transactional_id,
                producer_id,
                producer_epoch,
                partitions,
            } => txn::outcome(txn::add_partitions(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                &partitions,
                -1,
            )),
            Command::NoteTxn {
                transactional_id,
                producer_id,
                producer_epoch,
                span,
                skip_published,
            } => txn::outcome(txn::note(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                span,
                skip_published,
            )),
            Command::StageTxnOffsets {
                transactional_id,
                producer_id,
                producer_epoch,
                group,
                offsets,
            } => txn::outcome(txn::stage_offsets(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                &group,
                offsets,
            )),
            Command::CreateAcls { bindings } => acl::create(&read, &bindings),
            Command::DeleteAcls { filters } => acl::delete(&read, &filters),
            Command::InitTransactionTimed {
                transactional_id,
                producer_id,
                producer_epoch,
                timeout_ms,
                now_ms,
            } => txn::outcome(txn::init(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                if timeout_ms > 0 { timeout_ms } else { 60_000 },
                now_ms,
            )),
            Command::AbortTimedOutTxn {
                transactional_id,
                producer_id,
                producer_epoch,
                now_ms,
            } => txn::outcome(txn::abort_timed_out(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                now_ms,
            )),
            Command::ExpireTransactionalId {
                transactional_id,
                producer_id,
                producer_epoch,
                now_ms,
                expiration_ms,
            } => txn::expire_id(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                now_ms,
                expiration_ms,
            ),
            Command::ExpireProducers {
                keys,
                now_ms,
                expiration_ms,
            } => crate::producers::expire(&read, &scan, &keys, now_ms, expiration_ms),
            Command::AddTxnPartitionsAt {
                transactional_id,
                producer_id,
                producer_epoch,
                partitions,
                started_ms,
            } => txn::outcome(txn::add_partitions(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                &partitions,
                started_ms,
            )),
            Command::Compact {
                topic,
                partition,
                covered_start,
                covered_end,
                object_key,
                file_offset,
                length,
                base_timestamp,
                max_timestamp,
            } => catalog::compact_replace(
                &read,
                &scan,
                &topic,
                partition,
                covered_start,
                covered_end,
                &object_key,
                file_offset,
                length,
                base_timestamp,
                max_timestamp,
            ),
            Command::EndTransaction {
                transactional_id,
                producer_id,
                producer_epoch,
                committed,
                now_ms,
                ends,
            } => txn::outcome(txn::end(
                &read,
                &transactional_id,
                producer_id,
                producer_epoch,
                committed,
                now_ms,
                &ends,
            )),
            Command::SaveGroup { .. }
            | Command::CommitOffsets { .. }
            | Command::DeleteGroup { .. }
            | Command::ExpireOffsets { .. } => {
                unreachable!("handled above")
            }
        }
    };
    let outcome =
        outcome.map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))?;
    match outcome {
        Outcome::Reject(code) => Ok(Response::reject(code)),
        Outcome::Change { put, del, value } => {
            for (key, stored) in put {
                kv.insert(key.as_str(), stored.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            }
            for key in del {
                kv.remove(key.as_str())
                    .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
            }
            Ok(Response::ok(value))
        }
    }
}

/// Up to `limit` rows with keys in `[from, to)`, in key order.
fn scan_range(
    kv: &impl ReadableTable<&'static str, &'static str>,
    from: &str,
    to: &str,
    limit: usize,
) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    if from >= to {
        return Ok(out);
    }
    for row in kv.range(from..to).map_err(|err| err.to_string())? {
        if out.len() >= limit {
            break;
        }
        let (key, value) = row.map_err(|err| err.to_string())?;
        out.push((key.value().to_string(), value.value().to_string()));
    }
    Ok(out)
}

fn apply_init(
    kv: &mut redb::Table<&str, &str>,
    transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<Response, StorageError<NodeId>> {
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let read = |key: &str| -> Result<Option<String>, String> {
        match kv.get(key) {
            Ok(Some(guard)) => Ok(Some(guard.value().to_string())),
            Ok(None) => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    };
    match crate::producers::init_producer(&read, transactional, producer_id, producer_epoch)
        .map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))?
    {
        crate::producers::Issued::Reject(code) => Ok(Response::reject(code)),
        crate::producers::Issued::Applied { put, value } => {
            for (key, stored) in put {
                kv.insert(key.as_str(), stored.as_str())
                    .map_err(|error| err(&error))?;
            }
            Ok(Response::ok(Some(value)))
        }
    }
}

/// Group commands: they need a prefix scan, which the catalog's
/// get-only helpers do not offer.
fn apply_group(
    kv: &mut redb::Table<&str, &str>,
    command: Command,
) -> Result<Response, StorageError<NodeId>> {
    use crate::groups;
    let err = |err: &dyn std::error::Error| io(ErrorSubject::StateMachine, err);
    let read_group = |kv: &redb::Table<&str, &str>,
                      group: &str|
     -> Result<Option<groups::GroupRecord>, StorageError<NodeId>> {
        match kv
            .get(groups::group_key(group).as_str())
            .map_err(|e| err(&e))?
        {
            Some(raw) => Ok(Some(
                serde_json::from_str(raw.value()).map_err(|e| err(&e))?,
            )),
            None => Ok(None),
        }
    };
    match command {
        Command::SaveGroup { group, mut record } => {
            let stored = read_group(kv, &group)?;
            if let Some(stored) = &stored {
                if stored.generation > record.generation {
                    return Ok(Response::reject(groups::codes::ILLEGAL_GENERATION));
                }
            }
            if !record.members.is_empty() {
                record.empty_since_ms = -1;
            } else if let Some(stored) =
                stored.filter(|stored| stored.members.is_empty() && stored.empty_since_ms >= 0)
            {
                record.empty_since_ms = stored.empty_since_ms;
            }
            let raw = serde_json::to_string(&record).map_err(|e| err(&e))?;
            kv.insert(groups::group_key(&group).as_str(), raw.as_str())
                .map_err(|e| err(&e))?;
            Ok(Response::ok(None))
        }
        Command::CommitOffsets {
            group,
            generation,
            member_id,
            offsets,
        } => {
            let stored = read_group(kv, &group)?;
            if let Err(code) = groups::check_commit(stored.as_ref(), generation, &member_id) {
                return Ok(Response::reject(code));
            }
            if stored.is_none() {
                // A group that only has offsets still lists and describes.
                let raw =
                    serde_json::to_string(&groups::GroupRecord::default()).map_err(|e| err(&e))?;
                kv.insert(groups::group_key(&group).as_str(), raw.as_str())
                    .map_err(|e| err(&e))?;
            }
            for commit in offsets {
                let raw = serde_json::to_string(&commit.record).map_err(|e| err(&e))?;
                kv.insert(
                    groups::offset_key(&group, &commit.topic_id, commit.partition).as_str(),
                    raw.as_str(),
                )
                .map_err(|e| err(&e))?;
            }
            Ok(Response::ok(None))
        }
        Command::DeleteGroup { group } => {
            let stored = read_group(kv, &group)?;
            if stored
                .as_ref()
                .is_some_and(|record| !record.members.is_empty())
            {
                return Ok(Response::reject(groups::codes::NON_EMPTY_GROUP));
            }
            let prefix = groups::offset_prefix(&group);
            let mut doomed = Vec::new();
            for row in kv.range(prefix.as_str()..).map_err(|e| err(&e))? {
                let (key, _) = row.map_err(|e| err(&e))?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                doomed.push(key.value().to_string());
            }
            if stored.is_none() && doomed.is_empty() {
                return Ok(Response::reject(groups::codes::GROUP_ID_NOT_FOUND));
            }
            for key in doomed {
                kv.remove(key.as_str()).map_err(|e| err(&e))?;
            }
            kv.remove(groups::group_key(&group).as_str())
                .map_err(|e| err(&e))?;
            Ok(Response::ok(None))
        }
        Command::ExpireOffsets {
            group,
            now_ms,
            retention_ms,
        } => {
            let Some(mut stored) = read_group(kv, &group)? else {
                return Ok(Response::ok(Some("0".into())));
            };
            if !stored.members.is_empty() {
                return Ok(Response::ok(Some("0".into())));
            }
            let standalone = stored.protocol_type.is_none();
            if !standalone && stored.empty_since_ms < 0 {
                // Written before the time was recorded: start the clock now.
                stored.empty_since_ms = now_ms;
                let raw = serde_json::to_string(&stored).map_err(|e| err(&e))?;
                kv.insert(groups::group_key(&group).as_str(), raw.as_str())
                    .map_err(|e| err(&e))?;
                return Ok(Response::ok(Some("0".into())));
            }
            let prefix = groups::offset_prefix(&group);
            let mut expired = Vec::new();
            let mut kept = 0usize;
            for row in kv.range(prefix.as_str()..).map_err(|e| err(&e))? {
                let (key, value) = row.map_err(|e| err(&e))?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let record: groups::OffsetRecord =
                    serde_json::from_str(value.value()).map_err(|e| err(&e))?;
                let since = if standalone {
                    record.commit_timestamp
                } else {
                    stored.empty_since_ms
                };
                if now_ms.saturating_sub(since) >= retention_ms {
                    expired.push(key.value().to_string());
                } else {
                    kept += 1;
                }
            }
            let removed = expired.len();
            for key in expired {
                kv.remove(key.as_str()).map_err(|e| err(&e))?;
            }
            if kept == 0 {
                kv.remove(groups::group_key(&group).as_str())
                    .map_err(|e| err(&e))?;
            }
            Ok(Response::ok(Some(removed.to_string())))
        }
        _ => unreachable!("only group commands"),
    }
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
        let bytes = match txn.open_table(KV) {
            Ok(table) => encode_rows(&table),
            Err(redb::TableError::TableDoesNotExist(_)) => IndexedBody::new(0).finish(),
            Err(err) => Err(err.to_string()),
        }
        .map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))?;
        drop(txn);
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
            put_snapshot(&txn, &stored)
                .map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))?;
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
        let acls_change = entries.iter().any(|entry| match &entry.payload {
            EntryPayload::Normal(command) => may_change_acls(command),
            _ => false,
        });
        let durable = entries
            .iter()
            .any(|entry| matches!(entry.payload, EntryPayload::Membership(_)))
            || self.last_durable.lock().expect("last durable").elapsed() >= DURABLE_EVERY;
        let mut txn = self
            .db
            .begin_write()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        txn.set_durability(if durable {
            redb::Durability::Immediate
        } else {
            redb::Durability::None
        })
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
                    EntryPayload::Blank => responses.push(Response::ok(None)),
                    EntryPayload::Normal(command) => {
                        responses.push(apply_command(&mut kv, command)?);
                    }
                    EntryPayload::Membership(mem) => {
                        let membership = StoredMembership::new(Some(entry.log_id), mem);
                        let json = serde_json::to_string(&membership)
                            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                        meta.insert(MEMBERSHIP, json.as_str())
                            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
                        responses.push(Response::ok(None));
                    }
                }
            }
        }
        txn.commit()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        if durable {
            *self.last_durable.lock().expect("last durable") = std::time::Instant::now();
        }
        // After the commit, so whoever sees the new version reads the new ACLs.
        if acls_change {
            self.acl_version.fetch_add(1, Ordering::Release);
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
        let bytes = snapshot.into_inner();
        let rows = decode_rows(&bytes).map_err(|err| {
            io(
                ErrorSubject::Snapshot(Some(meta.signature())),
                &std::io::Error::other(err),
            )
        })?;
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
            for (key, value) in &rows {
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
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };
        put_snapshot(&txn, &stored)
            .map_err(|err| io(ErrorSubject::StateMachine, &std::io::Error::other(err)))?;
        txn.commit()
            .map_err(|err| io(ErrorSubject::StateMachine, &err))?;
        // After the commit, as in apply: the install may have changed the ACLs.
        self.acl_version.fetch_add(1, Ordering::Release);
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

    fn filled(dir: &Path, rows: &[(&str, &str)]) -> Arc<RedbMachine> {
        let machine = Arc::new(RedbMachine::open(&dir.join("state.redb")).unwrap());
        let txn = machine.db.begin_write().unwrap();
        {
            let mut kv = txn.open_table(KV).unwrap();
            for (key, value) in rows {
                kv.insert(*key, *value).unwrap();
            }
        }
        txn.commit().unwrap();
        machine
    }

    #[test]
    fn the_rows_are_what_postcard_writes() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let long = "x".repeat(300);
        let rows = [("a", "1"), ("b/longer", long.as_str()), ("c", "")];
        let machine = filled(dir.path(), &rows);
        let txn = machine.db.begin_read()?;
        let body = encode_rows(&txn.open_table(KV)?)?;
        assert_eq!(
            body,
            encode_indexed(&rows)?,
            "the table and the slice agree"
        );
        let expected: Vec<(String, String)> = rows
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let (index_at, _) = indexed_parts(&body)?;
        assert_eq!(
            body.get(INDEXED_MAGIC.len()..index_at),
            Some(camus_raft::codec::encode(&expected)?.as_slice())
        );
        assert_eq!(decode_rows(&body)?, expected);
        Ok(())
    }

    #[test]
    fn a_body_from_the_release_before_still_installs() -> Result<(), Box<dyn std::error::Error>> {
        let rows = vec![("k".to_string(), "v".to_string())];
        let mut body = SNAPSHOT_MAGIC.to_vec();
        body.extend_from_slice(&camus_raft::codec::encode(&rows)?);
        assert_eq!(decode_rows(&body)?, rows);
        Ok(())
    }

    #[test]
    fn a_snapshot_from_before_the_magic_still_installs() {
        let legacy = serde_json::to_vec(&SnapshotBody {
            data: [("k".to_string(), "v".to_string())].into_iter().collect(),
        })
        .unwrap();
        assert_eq!(
            decode_rows(&legacy).unwrap(),
            vec![("k".to_string(), "v".to_string())]
        );
    }

    #[tokio::test]
    async fn a_built_snapshot_installs_and_reloads_at_about_its_data_size() {
        let dir = tempfile::tempdir().unwrap();
        let value = "v".repeat(1_000);
        let rows: Vec<(String, String)> = (0..200)
            .map(|at| (format!("part/topic/{at}"), value.clone()))
            .collect();
        let borrowed: Vec<(&str, &str)> = rows
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let mut source = filled(dir.path(), &borrowed);
        let snapshot = source.build_snapshot().await.unwrap();
        let body = snapshot.snapshot.into_inner();
        let data_size: usize = rows
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        assert!(
            body.len() < data_size + data_size / 10,
            "{} bytes for {data_size} bytes of rows",
            body.len()
        );

        let other = tempfile::tempdir().unwrap();
        let mut target = Arc::new(RedbMachine::open(&other.path().join("state.redb")).unwrap());
        target
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(body.clone())))
            .await
            .unwrap();
        let mut expected = rows.clone();
        expected.sort();
        assert_eq!(target.scan("part/").unwrap(), expected);
        drop(target);
        let mut reopened = Arc::new(RedbMachine::open(&other.path().join("state.redb")).unwrap());
        let current = reopened.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.snapshot_id, snapshot.meta.snapshot_id);
        assert_eq!(current.snapshot.into_inner(), body);
        let txn = reopened.db.begin_read().unwrap();
        assert!(
            txn.open_table(META)
                .unwrap()
                .get(LEGACY_SNAPSHOT)
                .unwrap()
                .is_none(),
            "no JSON copy is written"
        );
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Rows in key order whose encoding spans several 64 KiB blocks.
    fn many_rows() -> Vec<(String, String)> {
        (0..3_000)
            .map(|at| (format!("ext/topic/{at:010}"), "v".repeat(100)))
            .collect()
    }

    fn borrowed(rows: &[(String, String)]) -> Vec<(&str, &str)> {
        rows.iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect()
    }

    /// The rows bytes (with the magic) and the decoded index of `body`.
    fn split(body: &[u8]) -> Result<(Vec<u8>, Vec<IndexEntry>), Box<dyn std::error::Error>> {
        let (index_at, index) = indexed_parts(body)?;
        let rows = body.get(..index_at).ok_or("rows out of bounds")?.to_vec();
        Ok((rows, camus_raft::codec::decode(index)?))
    }

    /// `rows` (with the magic), then `entries` as the index, then a trailer
    /// with `index_at` in place of the real offset.
    fn assemble(
        rows: &[u8],
        entries: &[IndexEntry],
        index_at: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let index = camus_raft::codec::encode(entries)?;
        let mut body = rows.to_vec();
        body.extend_from_slice(&index);
        body.extend_from_slice(&index_at.to_le_bytes());
        body.extend_from_slice(&(index.len() as u64).to_le_bytes());
        body.extend_from_slice(&CRC32C.checksum(&index).to_le_bytes());
        body.extend_from_slice(INDEXED_MAGIC);
        Ok(body)
    }

    fn refused(body: &[u8], expect: &str) -> TestResult {
        let err = decode_rows(body).err().ok_or("body decoded")?;
        assert!(err.contains(expect), "{err}");
        Ok(())
    }

    #[test]
    fn varint_len_matches_what_postcard_writes() -> TestResult {
        for value in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            assert_eq!(varint_len(value), camus_raft::codec::encoded_len(&value)?);
        }
        Ok(())
    }

    #[test]
    fn an_indexed_body_reads_the_same_rows() -> TestResult {
        let rows = many_rows();
        let body = encode_indexed(&borrowed(&rows))?;
        assert_eq!(decode_rows(&body)?, rows);
        assert_eq!(snapshot_rows(&body)?, rows);
        Ok(())
    }

    #[test]
    fn index_entries_start_blocks_on_row_boundaries() -> TestResult {
        let rows = many_rows();
        let body = encode_indexed(&borrowed(&rows))?;
        let (_, index) = split(&body)?;
        assert!(index.len() > 1, "{} blocks", index.len());
        assert_eq!(
            index.first().map(|(key, _, _)| key),
            rows.first().map(|(key, _)| key)
        );
        for pair in index.windows(2) {
            assert!(pair[1].1 - pair[0].1 >= BLOCK_BYTES);
            assert!(pair[0].0 < pair[1].0, "keys in order");
        }
        Ok(())
    }

    #[test]
    fn a_row_bigger_than_a_block_gets_a_block_of_its_own() -> TestResult {
        let big = "b".repeat(BLOCK_BYTES as usize * 2);
        let rows = [("a", "1"), ("b", big.as_str()), ("c", "3"), ("d", "4")];
        let body = encode_indexed(&rows)?;
        let (_, index) = split(&body)?;
        let firsts: Vec<&str> = index.iter().map(|(key, _, _)| key.as_str()).collect();
        assert_eq!(firsts, ["a", "c"], "the big row closes its block");
        assert_eq!(decode_rows(&body)?.len(), rows.len());
        Ok(())
    }

    #[test]
    fn a_row_exactly_one_block_on_starts_a_block() -> TestResult {
        // The first row starts at 9: the magic and a one-byte count. Size
        // the first value so the second row starts BLOCK_BYTES after it.
        // Row bytes: key length, key, value length (3 bytes for these
        // sizes), value.
        let value = "v".repeat(BLOCK_BYTES as usize - 1 - 1 - 3);
        let rows = [("a", value.as_str()), ("b", "1")];
        let body = encode_indexed(&rows)?;
        let (_, index) = split(&body)?;
        let offsets: Vec<u64> = index.iter().map(|(_, at, _)| *at).collect();
        assert_eq!(offsets, [9, 9 + BLOCK_BYTES]);
        assert_eq!(decode_rows(&body)?.len(), rows.len());
        Ok(())
    }

    #[test]
    fn an_empty_indexed_body_reads_no_rows() -> TestResult {
        let body = encode_indexed(&[])?;
        assert_eq!(decode_rows(&body)?, Vec::<(String, String)>::new());
        assert!(split(&body)?.1.is_empty());
        Ok(())
    }

    #[test]
    fn a_corrupt_index_is_refused() -> TestResult {
        let mut body = encode_indexed(&borrowed(&many_rows()))?;
        let (index_at, _) = indexed_parts(&body)?;
        let byte = body.get_mut(index_at).ok_or("no index byte")?;
        *byte ^= 0xff;
        refused(&body, "index fails its CRC32C")
    }

    #[test]
    fn a_flipped_byte_in_a_row_is_refused() -> TestResult {
        let mut body = encode_indexed(&borrowed(&many_rows()))?;
        // A `v` in a value near the middle: the rows still decode.
        let middle = body.len() / 2;
        let at = middle
            + body
                .get(middle..)
                .and_then(|rest| rest.iter().position(|byte| *byte == b'v'))
                .ok_or("no value byte")?;
        let byte = body.get_mut(at).ok_or("no row byte")?;
        *byte = b'w';
        refused(&body, "fails its CRC32C")
    }

    #[test]
    fn a_truncated_indexed_body_is_refused() -> TestResult {
        let body = encode_indexed(&borrowed(&many_rows()))?;
        for keep in [
            body.len() - 1,
            body.len() - TRAILER_LEN,
            INDEXED_MAGIC.len(),
        ] {
            let cut = body.get(..keep).ok_or("cut out of bounds")?;
            assert!(decode_rows(cut).is_err(), "{keep} bytes decoded");
        }
        Ok(())
    }

    #[test]
    fn a_trailer_that_misplaces_the_index_is_refused() -> TestResult {
        let body = encode_indexed(&[("a", "1")])?;
        let (rows, index) = split(&body)?;
        let real = rows.len() as u64;
        for index_at in [
            0,
            INDEXED_MAGIC.len() as u64 - 1,
            real - 1,
            real + 1,
            u64::MAX,
        ] {
            refused(
                &assemble(&rows, &index, index_at)?,
                "does not end at the trailer",
            )?;
        }
        Ok(())
    }

    #[test]
    fn rows_that_stop_short_of_the_index_are_refused() -> TestResult {
        let body = encode_indexed(&[("a", "1")])?;
        let (mut rows, index) = split(&body)?;
        rows.push(0);
        refused(&assemble(&rows, &index, rows.len() as u64)?, "rows end")
    }

    #[test]
    fn an_index_entry_off_a_row_start_is_refused() -> TestResult {
        let body = encode_indexed(&borrowed(&many_rows()))?;
        let (rows, mut index) = split(&body)?;
        let entry = index.get_mut(1).ok_or("one block")?;
        entry.1 += 1;
        refused(
            &assemble(&rows, &index, rows.len() as u64)?,
            "does not start on a row",
        )
    }

    #[test]
    fn an_index_entry_with_the_wrong_key_is_refused() -> TestResult {
        let body = encode_indexed(&borrowed(&many_rows()))?;
        let (rows, mut index) = split(&body)?;
        let entry = index.get_mut(1).ok_or("one block")?;
        entry.0.push('x');
        refused(&assemble(&rows, &index, rows.len() as u64)?, "names key")
    }

    #[test]
    fn an_index_that_skips_the_first_row_is_refused() -> TestResult {
        let body = encode_indexed(&borrowed(&many_rows()))?;
        let (rows, mut index) = split(&body)?;
        index.remove(0);
        refused(
            &assemble(&rows, &index, rows.len() as u64)?,
            "does not start at the first row",
        )
    }

    #[tokio::test]
    async fn an_indexed_snapshot_installs() -> TestResult {
        let rows = many_rows();
        let dir = tempfile::tempdir()?;
        let mut source = filled(dir.path(), &borrowed(&rows));
        let built = source.build_snapshot().await?;
        let body = encode_indexed(&borrowed(&rows))?;

        let other = tempfile::tempdir()?;
        let mut target = Arc::new(RedbMachine::open(&other.path().join("state.redb"))?);
        target
            .install_snapshot(&built.meta, Box::new(Cursor::new(body)))
            .await?;
        assert_eq!(target.scan("ext/")?, rows);
        Ok(())
    }

    #[tokio::test]
    async fn the_builder_writes_the_indexed_format() -> TestResult {
        let rows = many_rows();
        let dir = tempfile::tempdir()?;
        let mut machine = filled(dir.path(), &borrowed(&rows));
        let body = machine.build_snapshot().await?.snapshot.into_inner();
        assert!(body.starts_with(INDEXED_MAGIC));
        assert!(split(&body)?.1.len() > 1, "several blocks");
        assert_eq!(decode_rows(&body)?, rows);
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_machine_builds_an_indexed_snapshot() -> TestResult {
        let dir = tempfile::tempdir()?;
        let mut machine = Arc::new(RedbMachine::open(&dir.path().join("state.redb"))?);
        let body = machine.build_snapshot().await?.snapshot.into_inner();
        assert!(body.starts_with(INDEXED_MAGIC));
        assert!(decode_rows(&body)?.is_empty());
        Ok(())
    }
}
