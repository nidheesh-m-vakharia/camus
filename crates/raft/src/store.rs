//! [`RaftLogStorage`] backed by [`SegmentLog`].
//!
//! Append serializes entries, writes them, and `fsync`s once for the whole
//! batch before they are inserted into the readable index. `save_vote`
//! `fsync`s the meta file before it returns.

use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogId, LogState, RaftLogId, RaftLogReader, RaftTypeConfig, StorageError, Vote};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::segment::SegmentLog;

struct Inner<C>
where
    C: RaftTypeConfig,
{
    log: SegmentLog,
    dir: PathBuf,
    meta: RaftDiskMeta<C>,
}

#[derive(Serialize, Deserialize)]
struct RaftDiskMeta<C>
where
    C: RaftTypeConfig,
{
    vote: Option<Vote<C::NodeId>>,
    committed: Option<LogId<C::NodeId>>,
    last_purged: Option<LogId<C::NodeId>>,
}

impl<C> Default for RaftDiskMeta<C>
where
    C: RaftTypeConfig,
{
    fn default() -> Self {
        Self {
            vote: None,
            committed: None,
            last_purged: None,
        }
    }
}

/// On-disk Raft log. Clone shares the same files.
#[derive(Clone)]
pub struct SegmentLogStore<C>
where
    C: RaftTypeConfig,
{
    inner: Arc<Mutex<Inner<C>>>,
    stats: Arc<AppendStats>,
}

/// Counters for the append path. Clones of a store share them.
#[derive(Default, Debug)]
pub struct AppendStats {
    /// `append` calls, each one fsync.
    pub fsyncs: std::sync::atomic::AtomicU64,
    /// Entries written across those calls.
    pub entries: std::sync::atomic::AtomicU64,
    /// Encoded entry bytes written.
    pub bytes: std::sync::atomic::AtomicU64,
}

impl<C> SegmentLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Serialize + DeserializeOwned,
    C::NodeId: Serialize + DeserializeOwned,
{
    /// Create or reopen the log in `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StorageError<C::NodeId>> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|err| io_err::<C>(None, &err))?;
        let meta = read_meta::<C>(&dir)?;
        let log = SegmentLog::open(&dir).map_err(|err| io_err::<C>(None, &err))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner { log, dir, meta })),
            stats: Arc::new(AppendStats::default()),
        })
    }

    /// Append counters: fsyncs, entries, and bytes since open.
    pub fn stats(&self) -> Arc<AppendStats> {
        self.stats.clone()
    }
}

impl<C> RaftLogReader<C> for SegmentLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Serialize + DeserializeOwned + Clone,
    C::NodeId: Serialize + DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let start = bound_owned(range.start_bound());
        let end = bound_owned(range.end_bound());
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock(&inner)?;
            let records = guard
                .log
                .range(start, end)
                .map_err(|err| io_err::<C>(None, &err))?;
            records
                .into_iter()
                .map(|(_, bytes)| {
                    crate::codec::decode(&bytes).map_err(|err| io_err::<C>(None, &err))
                })
                .collect()
        })
        .await
        .map_err(|err| io_err::<C>(None, &err))?
    }
}

impl<C> RaftLogStorage<C> for SegmentLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Serialize + DeserializeOwned + Clone,
    C::NodeId: Serialize + DeserializeOwned,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock(&inner)?;
            let last = match guard.log.last_index() {
                Some(index) => {
                    let bytes = guard
                        .log
                        .get(index)
                        .map_err(|err| io_err::<C>(None, &err))?
                        .ok_or_else(|| {
                            io_err::<C>(None, &std::io::Error::other("missing last record"))
                        })?;
                    let entry: C::Entry =
                        crate::codec::decode(&bytes).map_err(|err| io_err::<C>(None, &err))?;
                    Some(entry.get_log_id().clone())
                }
                None => guard.meta.last_purged.clone(),
            };
            Ok(LogState {
                last_purged_log_id: guard.meta.last_purged.clone(),
                last_log_id: last,
            })
        })
        .await
        .map_err(|err| io_err::<C>(None, &err))?
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let vote = vote.clone();
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = lock(&inner)?;
            guard.meta.vote = Some(vote);
            write_meta(&guard.dir, &guard.meta)
        })
        .await
        .map_err(|err| io_err::<C>(None, &err))?
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || Ok(lock(&inner)?.meta.vote.clone()))
            .await
            .map_err(|err| io_err::<C>(None, &err))?
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        // Not fsynced on its own. openraft treats the committed id as a
        // hint: on restart the state machine's last applied id is the floor,
        // and the leader re-sends the commit index. It reaches disk with the
        // next vote or purge. Syncing it here cost two fsyncs per commit.
        lock(&self.inner)?.meta.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || Ok(lock(&inner)?.meta.committed.clone()))
            .await
            .map_err(|err| io_err::<C>(None, &err))?
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + Send,
        I::IntoIter: Send,
    {
        let mut payloads = Vec::new();
        for entry in entries {
            let index = entry.get_log_id().index;
            let bytes = crate::codec::encode(&entry)
                .map_err(|err| io_err::<C>(Some(entry.get_log_id().clone()), &err))?;
            payloads.push((index, bytes));
        }
        let entries = payloads.len() as u64;
        let bytes: u64 = payloads.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = lock(&inner)?;
            let borrowed: Vec<(u64, &[u8])> = payloads
                .iter()
                .map(|(index, bytes)| (*index, bytes.as_slice()))
                .collect();
            guard
                .log
                .append_batch(&borrowed)
                .map_err(|err| io_err::<C>(None, &err))
        })
        .await
        .map_err(|err| io_err::<C>(None, &err))??;
        use std::sync::atomic::Ordering::Relaxed;
        self.stats.fsyncs.fetch_add(1, Relaxed);
        self.stats.entries.fetch_add(entries, Relaxed);
        self.stats.bytes.fetch_add(bytes, Relaxed);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = lock(&inner)?;
            guard
                .log
                .truncate_from(log_id.index)
                .map_err(|err| io_err::<C>(Some(log_id), &err))
        })
        .await
        .map_err(|err| io_err::<C>(None, &err))?
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = lock(&inner)?;
            guard.meta.last_purged = Some(log_id.clone());
            write_meta(&guard.dir, &guard.meta)?;
            guard
                .log
                .purge_through(log_id.index)
                .map_err(|err| io_err::<C>(Some(log_id), &err))
        })
        .await
        .map_err(|err| io_err::<C>(None, &err))?
    }
}

fn lock<C>(
    inner: &Mutex<Inner<C>>,
) -> Result<std::sync::MutexGuard<'_, Inner<C>>, StorageError<C::NodeId>>
where
    C: RaftTypeConfig,
{
    inner
        .lock()
        .map_err(|err| io_err::<C>(None, &std::io::Error::other(err.to_string())))
}

fn io_err<C: RaftTypeConfig>(
    log_id: Option<LogId<C::NodeId>>,
    err: &dyn std::error::Error,
) -> StorageError<C::NodeId> {
    StorageError::IO {
        source: openraft::StorageIOError::new(
            match log_id {
                Some(id) => openraft::ErrorSubject::Log(id),
                None => openraft::ErrorSubject::Store,
            },
            openraft::ErrorVerb::Write,
            openraft::AnyError::error(err.to_string()),
        ),
    }
}

fn meta_path(dir: &Path) -> PathBuf {
    dir.join("raft-meta.json")
}

fn read_meta<C>(dir: &Path) -> Result<RaftDiskMeta<C>, StorageError<C::NodeId>>
where
    C: RaftTypeConfig,
    C::NodeId: Serialize + DeserializeOwned,
{
    let path = meta_path(dir);
    if !path.exists() {
        return Ok(RaftDiskMeta::default());
    }
    let text = std::fs::read_to_string(&path).map_err(|err| io_err::<C>(None, &err))?;
    serde_json::from_str(&text).map_err(|err| io_err::<C>(None, &err))
}

fn write_meta<C>(dir: &Path, meta: &RaftDiskMeta<C>) -> Result<(), StorageError<C::NodeId>>
where
    C: RaftTypeConfig,
    C::NodeId: Serialize + DeserializeOwned,
{
    let bytes = serde_json::to_vec(meta).map_err(|err| io_err::<C>(None, &err))?;
    let tmp = dir.join("raft-meta.json.tmp");
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|err| io_err::<C>(None, &err))?;
        file.write_all(&bytes)
            .map_err(|err| io_err::<C>(None, &err))?;
        file.sync_all().map_err(|err| io_err::<C>(None, &err))?;
    }
    std::fs::rename(&tmp, meta_path(dir)).map_err(|err| io_err::<C>(None, &err))?;
    std::fs::File::open(dir)
        .and_then(|dir_file| dir_file.sync_all())
        .map_err(|err| io_err::<C>(None, &err))?;
    Ok(())
}

fn bound_owned(bound: Bound<&u64>) -> Bound<u64> {
    match bound {
        Bound::Included(value) => Bound::Included(*value),
        Bound::Excluded(value) => Bound::Excluded(*value),
        Bound::Unbounded => Bound::Unbounded,
    }
}
