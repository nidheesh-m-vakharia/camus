//! One WAL shard.
//!
//! Produce waits for this Raft group to commit and fsync on a majority of
//! voters. The R2 upload happens after the ack. Brokers reach each other over
//! HTTP; a single broker is a one-voter shard, which is a quorum of one.

#![forbid(unsafe_code)]

mod machine;
mod network;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, EntryPayload, Raft};
use serde::{Deserialize, Serialize};

pub use machine::Batch;
use machine::Machine;
pub use network::Router;
use openraft::error::{ClientWriteError, Fatal, RaftError};
pub use openraft::BasicNode;
use std::sync::atomic::{AtomicBool, Ordering};

/// Time spent on AppendEntries RPCs that carry entries, in this process.
/// `sent` is measured by the leader around the whole request; `handled` by a
/// follower around its own `append_entries`. The difference is transport.
pub static RPC_TIMES: RpcTimes = RpcTimes::new();

/// Counters behind [`RPC_TIMES`].
pub struct RpcTimes {
    pub sent_count: std::sync::atomic::AtomicU64,
    pub sent_us: std::sync::atomic::AtomicU64,
    pub handled_count: std::sync::atomic::AtomicU64,
    pub handled_us: std::sync::atomic::AtomicU64,
}

impl RpcTimes {
    const fn new() -> Self {
        Self {
            sent_count: std::sync::atomic::AtomicU64::new(0),
            sent_us: std::sync::atomic::AtomicU64::new(0),
            handled_count: std::sync::atomic::AtomicU64::new(0),
            handled_us: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Record a follower's handling time for a request with entries.
    pub fn handled(&self, elapsed: Duration) {
        self.handled_count.fetch_add(1, Ordering::Relaxed);
        self.handled_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }
}

/// Voter id. Same numbering as the metadata group.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Raft types for a WAL shard.
    pub TypeConfig:
        D = Command,
        R = Response,
);

fn batch_slices(entry: &openraft::Entry<TypeConfig>) -> Vec<&[u8]> {
    match &entry.payload {
        EntryPayload::Normal(Command::Append(batch)) => vec![batch.raw.as_ref()],
        EntryPayload::Normal(Command::Appends(batches)) => {
            batches.iter().map(|batch| batch.raw.as_ref()).collect()
        }
        _ => Vec::new(),
    }
}

fn clear_batches(entry: &mut openraft::Entry<TypeConfig>) {
    match &mut entry.payload {
        EntryPayload::Normal(Command::Append(batch)) => batch.raw.clear(),
        EntryPayload::Normal(Command::Appends(batches)) => {
            for batch in batches {
                batch.raw.clear();
            }
        }
        _ => {}
    }
}

fn fill_batches(entry: &mut openraft::Entry<TypeConfig>, raws: Vec<Vec<u8>>) {
    let mut raws = raws.into_iter();
    match &mut entry.payload {
        EntryPayload::Normal(Command::Append(batch)) => {
            if let Some(raw) = raws.next() {
                batch.raw = bytes::Bytes::from(raw);
            }
        }
        EntryPayload::Normal(Command::Appends(batches)) => {
            for batch in batches {
                if let Some(raw) = raws.next() {
                    batch.raw = bytes::Bytes::from(raw);
                }
            }
        }
        _ => {}
    }
}

fn encode_log_len(entry: &openraft::Entry<TypeConfig>) -> std::io::Result<usize> {
    let slices = batch_slices(entry);
    let mut stripped = entry.clone();
    clear_batches(&mut stripped);
    let header = camus_raft::codec::encode(&stripped)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
    Ok(4 + 4
        + slices.len() * 4
        + 4
        + header.len()
        + slices.iter().map(|batch| batch.len()).sum::<usize>())
}

fn encode_log(
    entry: &openraft::Entry<TypeConfig>,
    out: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    let slices = batch_slices(entry);
    let mut stripped = entry.clone();
    clear_batches(&mut stripped);
    let header = camus_raft::codec::encode(&stripped)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
    camus_raft::layout::write_parts(&header, &slices, out)
}

fn decode_log(bytes: &[u8]) -> std::io::Result<openraft::Entry<TypeConfig>> {
    let Some((header, raws)) = camus_raft::layout::read_parts(bytes)? else {
        return camus_raft::codec::decode(bytes)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()));
    };
    let mut entry: openraft::Entry<TypeConfig> = camus_raft::codec::decode(header)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
    fill_batches(&mut entry, raws);
    Ok(entry)
}

fn log_batch_at(entry: &openraft::Entry<TypeConfig>, pos: usize) -> std::io::Result<Option<u64>> {
    let slices = batch_slices(entry);
    let mut stripped = entry.clone();
    clear_batches(&mut stripped);
    let header = camus_raft::codec::encode(&stripped)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
    let lens: Vec<usize> = slices.iter().map(|batch| batch.len()).collect();
    Ok(camus_raft::layout::batch_offset(header.len(), &lens, pos))
}

/// A committed command.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Append one sealed-later batch.
    Append(Append),
    /// Append several batches in one entry: group commit (ADR 0006). They
    /// take offsets in this order.
    Appends(Vec<Append>),
    /// Drop batches that metadata has published.
    Seal {
        /// Identity of each dropped batch.
        drops: Vec<SealDrop>,
    },
}

/// One produce batch, before offsets are assigned.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Append {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Topic id used when the uploader writes the object.
    pub topic_id: [u8; 16],
    /// Leader epoch written into the batch.
    pub epoch: i32,
    /// Client bytes, checked before they are proposed. A view of the
    /// request buffer, not a copy; encoded as one byte string.
    pub raw: bytes::Bytes,
    /// Records in `raw`.
    pub records: i32,
    /// First timestamp.
    pub base_timestamp: i64,
    /// Last timestamp.
    pub max_timestamp: i64,
    /// Lowest base offset this batch may take: the partition's sealed end in
    /// metadata. A shard rebuilt by `truncate-to-sealed` starts empty, and
    /// this keeps it from reusing offsets that R2 already holds.
    #[serde(default)]
    pub floor: i64,
}

/// One batch the uploader has published.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SealDrop {
    /// Topic name, for logs.
    pub topic: String,
    /// Topic id. Matching uses this, not the name.
    pub topic_id: [u8; 16],
    /// Partition.
    pub partition: i32,
    /// Base offset.
    pub start: i64,
}

/// Applied result.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// Assigned base offset. `-1` when the command is not an append.
    pub base_offset: i64,
    /// Kafka error code.
    pub error_code: i16,
    /// For [`Command::Appends`]: one result per batch, in order.
    #[serde(default)]
    pub each: Vec<Response>,
}

/// Why a proposal did not commit.
#[derive(Debug)]
pub enum WriteError {
    /// This node is not the shard leader. The entry was not appended here.
    NotLeader,
    /// The log or state machine could not write, for example a full disk.
    /// openraft stops the shard on the first such error; every later write
    /// reports this too.
    Storage,
    /// Anything else: shutdown, storage, or a lost quorum.
    Other(anyhow::Error),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader => f.write_str("not the wal shard leader"),
            Self::Storage => f.write_str("wal storage failed"),
            Self::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for WriteError {}

/// A running shard.
pub struct Shard {
    /// Raft handle.
    pub raft: Raft<TypeConfig>,
    machine: Arc<Machine>,
    node_id: NodeId,
    storage_failed: Arc<AtomicBool>,
    log_stats: Arc<camus_raft::AppendStats>,
    queue: tokio::sync::mpsc::UnboundedSender<Vec<Queued>>,
    /// [`QUEUE_BYTES`] of permits, one per byte.
    budget: Arc<tokio::sync::Semaphore>,
    /// The shard's background loop stops once this is dropped.
    _alive: Arc<()>,
}

/// One produce batch waiting for the committer. A request's batches for one
/// shard are sent together, so the committer sees all of them at once.
type Queued = (
    Append,
    tokio::sync::oneshot::Sender<Result<Response, Failure>>,
    Budget,
);

/// A share of a shard's queue budget, released when every batch it covers
/// has its result.
type Budget = Arc<tokio::sync::OwnedSemaphorePermit>;

/// Bytes of batches a shard holds between `submit` and their result: queued,
/// packed or committing. `submit` waits for room, in arrival order, so a
/// producer that does not wait for acks cannot grow memory without bound.
const QUEUE_BYTES: usize = 64 * 1024 * 1024;

/// Woken whenever any shard applies a seal, which frees unsealed bytes.
static SEALED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// A future that completes at the next seal applied on any shard. Enable it
/// before checking the condition it waits for, so no seal is missed.
pub fn sealed() -> tokio::sync::futures::Notified<'static> {
    SEALED.notified()
}

/// A batch queued by [`Shard::submit`].
pub struct Pending(tokio::sync::oneshot::Receiver<Result<Response, Failure>>);

impl Pending {
    /// Wait until the batch commits on a majority and is applied here.
    pub async fn wait(self) -> Result<Response, WriteError> {
        match self.0.await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(failure)) => Err(failure.into()),
            Err(_) => Err(WriteError::Other(anyhow::anyhow!("wal committer stopped"))),
        }
    }
}

/// A [`WriteError`] that can be handed to every batch in a failed group.
#[derive(Clone, Debug)]
enum Failure {
    NotLeader,
    Storage,
    Other(String),
}

impl From<Failure> for WriteError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::NotLeader => WriteError::NotLeader,
            Failure::Storage => WriteError::Storage,
            Failure::Other(text) => WriteError::Other(anyhow::anyhow!(text)),
        }
    }
}

/// Bytes one packed entry may carry before the committer starts another.
const GROUP_BYTES: usize = 1024 * 1024;

/// Packed entries that may be committing at once. A second one starts only
/// when a full group is already waiting, so its disk flush overlaps the
/// previous quorum. Starting it sooner halves the group: on 2026-11-10, 32
/// producers did 0.12 fsyncs per ack against 0.06 with one slot
/// ([#129](https://github.com/nidheesh-m-vakharia/camus/issues/129)). Four
/// slots, taken before the pack, measured 10.4 MiB/s and 0.18 fsyncs per ack
/// against 14.9 MiB/s and 0.07 (`broker/tests/throughput.rs`, 32 producers).
const IN_FLIGHT: usize = 2;

/// Group commit (ADR 0006). Batches that arrive while entries are being
/// committed wait here and go into the next entry together, so concurrent
/// producers share one quorum fsync. Entries are submitted to Raft in arrival
/// order, which is the order offsets are assigned.
fn ack_delay() -> Option<std::time::Duration> {
    let Ok(raw) = std::env::var("CAMUS_ACK_DELAY_MS") else {
        return None;
    };
    let Ok(ms) = raw.parse::<u64>() else {
        return None;
    };
    (ms > 0).then_some(std::time::Duration::from_millis(ms))
}

async fn commit_loop(
    raft: Raft<TypeConfig>,
    storage_failed: Arc<AtomicBool>,
    mut queue: tokio::sync::mpsc::UnboundedReceiver<Vec<Queued>>,
) {
    // Test hook: the entry is already committed. Holding the ack lets a
    // produce time out and still leave the record in the log.
    let ack_delay = ack_delay();
    let slots = Arc::new(tokio::sync::Semaphore::new(IN_FLIGHT));
    // Batches received but not yet packed, in arrival order. A request's
    // batches arrive as one chunk; a chunk larger than a group spills into
    // the next one.
    let mut ready = std::collections::VecDeque::new();
    loop {
        if ready.is_empty() {
            let Some(chunk) = queue.recv().await else {
                return;
            };
            ready.extend(chunk);
        }
        // While one commit is in flight, leave a short group queued. The
        // slot that just freed would otherwise flush it immediately, and the
        // queue never fills during the quorum wait.
        loop {
            while let Ok(chunk) = queue.try_recv() {
                ready.extend(chunk);
            }
            let waiting: usize = ready.iter().map(|(append, _, _)| append.raw.len()).sum();
            let idle = slots.available_permits() == IN_FLIGHT;
            if waiting >= GROUP_BYTES || ready.len() >= machine::MAX_PACKED || idle {
                break;
            }
            let idle_slots = slots.clone();
            tokio::select! {
                biased;
                chunk = queue.recv() => {
                    let Some(chunk) = chunk else {
                        return;
                    };
                    ready.extend(chunk);
                }
                _ = wait_until_idle(idle_slots) => {}
            }
        }
        let mut size = 0;
        let mut group = Vec::new();
        pack(&mut queue, &mut ready, &mut group, &mut size);
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        pack(&mut queue, &mut ready, &mut group, &mut size);
        let mut appends = Vec::with_capacity(group.len());
        let mut waiters = Vec::with_capacity(group.len());
        let mut budgets = Vec::with_capacity(group.len());
        for (append, waiter, budget) in group {
            appends.push(append);
            waiters.push(waiter);
            budgets.push(budget);
        }
        let command = if appends.len() == 1 {
            Command::Append(appends.into_iter().next().expect("one append"))
        } else {
            Command::Appends(appends)
        };
        let submitted = raft.client_write_ff(command).await;
        let raft = raft.clone();
        let flag = storage_failed.clone();
        let ack_delay = ack_delay;
        tokio::spawn(async move {
            let _slot = slot;
            // Dropped once this task has sent every result.
            let _budgets = budgets;
            let failure = |failure: Failure, waiters: Vec<_>| {
                for waiter in waiters {
                    let _: Result<(), _> =
                        tokio::sync::oneshot::Sender::send(waiter, Err(failure.clone()));
                }
            };
            let rx = match submitted {
                Ok(rx) => rx,
                Err(fatal) => {
                    let code = match classify(&raft, &flag, RaftError::Fatal(fatal)) {
                        WriteError::Storage => Failure::Storage,
                        other => Failure::Other(other.to_string()),
                    };
                    return failure(code, waiters);
                }
            };
            let written = match rx.await {
                Ok(Ok(written)) => written,
                Ok(Err(err)) => {
                    let code = match classify(&raft, &flag, RaftError::APIError(err)) {
                        WriteError::NotLeader => Failure::NotLeader,
                        WriteError::Storage => Failure::Storage,
                        WriteError::Other(err) => Failure::Other(err.to_string()),
                    };
                    return failure(code, waiters);
                }
                Err(_) => {
                    let code = if storage_has_failed(&raft, &flag) {
                        Failure::Storage
                    } else {
                        Failure::Other("raft stopped".into())
                    };
                    return failure(code, waiters);
                }
            };
            if let Some(delay) = ack_delay {
                tokio::time::sleep(delay).await;
            }
            if written.data.each.is_empty() {
                if let Some(waiter) = waiters.into_iter().next() {
                    let _ = waiter.send(Ok(written.data));
                }
                return;
            }
            let mut each = written.data.each.into_iter();
            for waiter in waiters {
                let result = each
                    .next()
                    .ok_or_else(|| Failure::Other("no result for packed batch".into()));
                let _ = waiter.send(result);
            }
        });
    }
}

/// How often the shard writes what apply changed. Apply itself touches only
/// memory; this bounds how much of the log a restart re-applies.
const FLUSH_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

/// Every [`FLUSH_EVERY`] until the shard is dropped: flush the state machine
/// off the async runtime, then purge what the log no longer needs. A failed
/// flush keeps its changes for the next one. Every voter purges here, not
/// only the shard leader that uploads.
///
/// The machine is held weakly between ticks, so a closed shard's database
/// can be opened again.
async fn flush_loop(
    alive: std::sync::Weak<()>,
    raft: Raft<TypeConfig>,
    machine: std::sync::Weak<Machine>,
) {
    let mut tick = tokio::time::interval(FLUSH_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let (Some(_alive), Some(machine)) = (alive.upgrade(), machine.upgrade()) else {
            return;
        };
        let flushing = machine.clone();
        match tokio::task::spawn_blocking(move || flushing.flush()).await {
            Ok(Ok(())) => purge_to(&raft, &machine, u64::MAX).await,
            Ok(Err(err)) => tracing::warn!(%err, "wal state flush failed; retrying"),
            Err(_) => return,
        }
    }
}

/// Purge the log up to `upto`, but not into an entry that still holds an
/// unsealed batch and not past the latest snapshot (openraft's own bound).
async fn purge_to(raft: &Raft<TypeConfig>, machine: &Machine, upto: u64) {
    let Some(floor) = machine.purge_floor() else {
        return;
    };
    let (snapshot, purged) = {
        let metrics = raft.metrics().borrow().clone();
        (
            metrics.snapshot.map(|id| id.index),
            metrics.purged.map(|id| id.index),
        )
    };
    let Some(snapshot) = snapshot else {
        return;
    };
    let upto = upto.min(floor).min(snapshot);
    if purged.is_some_and(|purged| upto <= purged) {
        return;
    }
    if let Err(err) = raft.trigger().purge_log(upto).await {
        tracing::warn!(%err, "wal log purge failed");
    }
}

/// Wait until every commit slot is free, then release them. Dropping this
/// future releases any slot it has already taken.
async fn wait_until_idle(slots: Arc<tokio::sync::Semaphore>) {
    let mut held = Vec::with_capacity(IN_FLIGHT);
    for _ in 0..IN_FLIGHT {
        let Ok(permit) = slots.clone().acquire_owned().await else {
            return;
        };
        held.push(permit);
    }
}

/// Move batches from `ready`, then from the queue, into `group` until it
/// reaches `GROUP_BYTES` or `MAX_PACKED`.
fn pack(
    queue: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<Queued>>,
    ready: &mut std::collections::VecDeque<Queued>,
    group: &mut Vec<Queued>,
    size: &mut usize,
) {
    while *size < GROUP_BYTES && group.len() < machine::MAX_PACKED {
        if ready.is_empty() {
            let Ok(chunk) = queue.try_recv() else { break };
            ready.extend(chunk);
            continue;
        }
        let next = ready.pop_front().expect("ready batch");
        *size += next.0.raw.len();
        group.push(next);
    }
}

fn storage_has_failed(raft: &Raft<TypeConfig>, flag: &AtomicBool) -> bool {
    if flag.load(Ordering::SeqCst) {
        return true;
    }
    // A writer waiting on the entry may only see `Stopped`; the cause is in
    // the metrics.
    let failed = matches!(
        raft.metrics().borrow().running_state,
        Err(Fatal::StorageError(_))
    );
    if failed {
        flag.store(true, Ordering::SeqCst);
    }
    failed
}

fn classify(
    raft: &Raft<TypeConfig>,
    flag: &AtomicBool,
    err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
) -> WriteError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => WriteError::NotLeader,
        RaftError::Fatal(Fatal::StorageError(_)) => {
            flag.store(true, Ordering::SeqCst);
            WriteError::Storage
        }
        RaftError::Fatal(_) if storage_has_failed(raft, flag) => WriteError::Storage,
        other => WriteError::Other(other.into()),
    }
}

impl Shard {
    /// Open the log and state machine with HTTP RPC between brokers. A fresh
    /// shard waits for [`Shard::initialize`]; a restarted one rejoins from
    /// its stored membership.
    pub async fn open(node_id: NodeId, dir: &Path) -> anyhow::Result<Self> {
        Self::open_shard(node_id, dir, 0).await
    }

    /// Open shard `shard` with HTTP RPC under its own path (ADR 0014).
    pub async fn open_shard(node_id: NodeId, dir: &Path, shard: u32) -> anyhow::Result<Self> {
        Self::start(node_id, dir, network::HttpFactory::for_shard(shard)).await
    }

    /// Open a shard whose RPC stays in this process, for tests.
    pub async fn open_in_process(
        node_id: NodeId,
        dir: &Path,
        router: Arc<Router>,
    ) -> anyhow::Result<Self> {
        let shard = Self::start(
            node_id,
            dir,
            network::Factory {
                router: router.clone(),
            },
        )
        .await?;
        router.insert(node_id, shard.raft.clone());
        Ok(shard)
    }

    async fn start<N>(node_id: NodeId, dir: &Path, network: N) -> anyhow::Result<Self>
    where
        N: openraft::RaftNetworkFactory<TypeConfig>,
    {
        let (heartbeat_interval, election_timeout_min, election_timeout_max) =
            camus_raft::quic::raft_timeouts();
        let config = Config {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
            // An entry is a packed group of up to `GROUP_BYTES` (plus one
            // client batch). Eight per AppendEntries keeps an RPC far below
            // the broker's HTTP body limit even with JSON encoding.
            max_payload_entries: 8,
            // openraft would purge everything but the last 1000 entries after
            // each snapshot, unsealed batches' entries too. Every purge goes
            // through `purge_to` instead, which stops below the oldest one.
            max_in_snapshot_log_to_keep: u64::MAX,
            ..Default::default()
        };
        let config = Arc::new(config.validate()?);
        let mut log = camus_raft::SegmentLogStore::open(dir.join("log"))?;
        log.set_codec(encode_log_len, encode_log, decode_log);
        let log_stats = log.stats();
        let machine = Arc::new(Machine::open(&dir.join("state.redb"), log.clone())?);
        let raft = Raft::new(node_id, config, network, log, machine.clone()).await?;
        let alive = Arc::new(());
        tokio::spawn(flush_loop(
            Arc::downgrade(&alive),
            raft.clone(),
            Arc::downgrade(&machine),
        ));
        let storage_failed = Arc::new(AtomicBool::new(false));
        let (queue, waiting) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(commit_loop(raft.clone(), storage_failed.clone(), waiting));
        Ok(Self {
            raft,
            machine,
            node_id,
            storage_failed,
            log_stats,
            queue,
            budget: Arc::new(tokio::sync::Semaphore::new(QUEUE_BYTES)),
            _alive: alive,
        })
    }

    /// Create the shard with these voters. Call once, on one voter. Fails when
    /// this log already has a membership.
    pub async fn initialize(&self, nodes: BTreeMap<NodeId, BasicNode>) -> anyhow::Result<()> {
        self.raft.initialize(nodes).await?;
        Ok(())
    }

    /// Current shard leader, if this node knows one.
    pub fn leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Whether this node leads and a quorum acknowledged it within `within`.
    /// A restarted node can report itself leader from its stored vote before
    /// any peer has confirmed it; this does not.
    pub fn leads_quorum(&self, within: Duration) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
            && metrics
                .millis_since_quorum_ack
                .is_some_and(|ms| u128::from(ms) < within.as_millis())
    }

    /// Add `id` as a learner, wait until it has this shard's log, then make
    /// it a voter. The caller is the leader. `node.addr` is that broker's
    /// HTTP address, the same form `initialize` stores.
    pub async fn add_voter(&self, id: NodeId, node: BasicNode) -> anyhow::Result<()> {
        self.raft
            .add_learner(id, node, true)
            .await
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        let mut ids = std::collections::BTreeSet::new();
        ids.insert(id);
        self.raft
            .change_membership(openraft::ChangeMembers::AddVoterIds(ids), false)
            .await
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        Ok(())
    }

    /// Voter ids in the latest membership this node has seen.
    pub fn voters(&self) -> Vec<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    /// Initialize a one-voter group when this log has no membership yet, then
    /// wait until this node is leader.
    pub async fn ensure_voter(&self) -> anyhow::Result<()> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.node_id,
            BasicNode {
                addr: format!("wal-{}", self.node_id),
            },
        );
        match self.raft.initialize(nodes).await {
            Ok(()) => {}
            Err(err) => {
                let text = err.to_string();
                if !text.to_ascii_lowercase().contains("not allowed")
                    && !text.to_ascii_lowercase().contains("initialize")
                {
                    return Err(err.into());
                }
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if self.is_leader() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("wal shard {} did not become leader", self.node_id);
    }

    /// Whether this process is the shard leader.
    /// Campaign for leadership. openraft 0.9 has no leadership transfer, so
    /// a preferred leader takes the shard back by winning an election
    /// (ADR 0014).
    pub async fn campaign(&self) -> anyhow::Result<()> {
        self.raft.trigger().elect().await?;
        Ok(())
    }

    /// On the leader: `follower` has every entry the leader has, so it can
    /// win an election without losing anything.
    pub fn follower_caught_up(&self, follower: NodeId) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        let Some(replication) = metrics.replication else {
            return false;
        };
        let matched = replication
            .get(&follower)
            .copied()
            .flatten()
            .map(|log| log.index);
        matched.is_some() && matched == metrics.last_log_index
    }

    /// The HTTP address of a voter, from the shard's membership.
    pub fn voter_addr(&self, node: NodeId) -> Option<String> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_node(&node)
            .map(|node| node.addr.clone())
    }

    /// The shard's current Raft term.
    pub fn term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node_id)
    }

    /// Propose a batch and wait until a majority has it on disk and this
    /// leader has applied it. Concurrent calls share one entry and one
    /// quorum fsync.
    pub async fn append(&self, batch: Append) -> Result<Response, WriteError> {
        let mut pending = self.submit(vec![batch]).await?;
        pending.pop().expect("one pending").wait().await
    }

    /// Queue these batches, in order, without waiting for them to commit.
    /// They reach the committer together, so batches that fit in one group
    /// share one entry and one quorum fsync. Each [`Pending`] resolves like
    /// [`Shard::append`]; dropping one does not withdraw its batch. Waits
    /// first while the shard holds [`QUEUE_BYTES`] of batches.
    pub async fn submit(&self, batches: Vec<Append>) -> Result<Vec<Pending>, WriteError> {
        if !self.is_leader() {
            return Err(WriteError::NotLeader);
        }
        let bytes: usize = batches.iter().map(|batch| batch.raw.len()).sum();
        // One batch larger than the budget takes all of it.
        let permits = u32::try_from(bytes.min(QUEUE_BYTES)).unwrap_or(u32::MAX);
        let budget: Budget = Arc::new(
            self.budget
                .clone()
                .acquire_many_owned(permits)
                .await
                .map_err(|_| WriteError::Other(anyhow::anyhow!("wal committer stopped")))?,
        );
        let (chunk, pending): (Vec<_>, Vec<_>) = batches
            .into_iter()
            .map(|batch| {
                let (done, wait) = tokio::sync::oneshot::channel();
                ((batch, done, budget.clone()), Pending(wait))
            })
            .unzip();
        self.queue
            .send(chunk)
            .map_err(|_| WriteError::Other(anyhow::anyhow!("wal committer stopped")))?;
        Ok(pending)
    }

    /// Whether a log or state-machine write has failed on this shard.
    pub fn storage_failed(&self) -> bool {
        storage_has_failed(&self.raft, &self.storage_failed)
    }

    fn classify(&self, err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> WriteError {
        classify(&self.raft, &self.storage_failed, err)
    }

    /// Drop published batches and return the log index of that commit.
    pub async fn seal(&self, drops: Vec<SealDrop>) -> Result<u64, WriteError> {
        let response = self
            .raft
            .client_write(Command::Seal { drops })
            .await
            .map_err(|err| self.classify(err))?;
        Ok(response.log_id.index)
    }

    /// Purge the shard log through `index`, inclusive.
    pub async fn purge(&self, index: u64) -> anyhow::Result<()> {
        // openraft purges only entries a snapshot covers, and building a
        // snapshot makes the applied state durable first, so no flush here.
        // One here cost an fsync per upload pass. `purge_to` keeps every
        // entry an unsealed batch is still in.
        purge_to(&self.raft, &self.machine, index).await;
        Ok(())
    }

    /// Next offset for a partition. `0` when the shard has not appended.
    pub fn end_offset(&self, topic_id: &[u8; 16], partition: i32) -> i64 {
        self.machine.end_offset(topic_id, partition)
    }

    /// Log append counters for this shard: `(fsyncs, entries, bytes)`.
    pub fn log_stats(&self) -> (u64, u64, u64) {
        (
            self.log_stats.fsyncs.load(Ordering::Relaxed),
            self.log_stats.entries.load(Ordering::Relaxed),
            self.log_stats.bytes.load(Ordering::Relaxed),
        )
    }

    /// Bytes of committed batches not yet sealed. The disk limit is checked
    /// against this.
    pub fn unsealed_bytes(&self) -> u64 {
        self.machine.unsealed_bytes()
    }

    /// The oldest committed batches not yet sealed, up to about `limit`
    /// bytes, with their bytes read from the log.
    pub async fn pending(&self, limit: u64) -> anyhow::Result<Vec<Batch>> {
        Ok(self.machine.pending(limit).await?)
    }

    /// Unsealed batches of one partition from `offset`, up to about `limit`
    /// bytes (at least one batch), read from the log.
    pub async fn read(
        &self,
        topic_id: &[u8; 16],
        partition: i32,
        sealed_end: i64,
        offset: i64,
        limit: usize,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(self
            .machine
            .read(topic_id, partition, sealed_end, offset, limit)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn corrupt_append_returns_corrupt_message() {
        let dir = tempfile::tempdir().unwrap();
        let shard = Shard::open(1, dir.path()).await.unwrap();
        shard.ensure_voter().await.unwrap();
        let response = shard
            .append(Append {
                topic: "events".into(),
                partition: 0,
                topic_id: [1; 16],
                epoch: 0,
                raw: vec![0; 8].into(),
                records: 1,
                base_timestamp: 1,
                max_timestamp: 1,
                floor: 0,
            })
            .await
            .unwrap();
        assert_eq!(response.error_code, 2, "a short buffer is not a batch");
        drop(shard);
    }

    fn record_batch(value: &[u8]) -> Vec<u8> {
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
            timestamp: 1,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(value)),
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
        .unwrap();
        buf.to_vec()
    }

    fn append_of(value: &[u8]) -> Append {
        Append {
            topic: "events".into(),
            partition: 0,
            topic_id: [1; 16],
            epoch: 0,
            raw: record_batch(value).into(),
            records: 1,
            base_timestamp: 1,
            max_timestamp: 1,
            floor: 0,
        }
    }

    async fn wait_leader(shards: &[&Shard]) -> NodeId {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            for shard in shards {
                if shard.is_leader() {
                    return shard.node_id;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no wal leader");
    }

    /// Append on whichever shard leads, as a client would. Right after a
    /// failover a new leader can step down before the write reaches it, so
    /// `NotLeader` is retried until the deadline.
    async fn append_on_leader(shards: &[&Shard], batch: Append) -> (NodeId, Response) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leader = wait_leader(shards).await;
            let Some(shard) = shards.iter().find(|shard| shard.node_id == leader) else {
                panic!("leader {leader} is not a live shard");
            };
            match shard.append(batch.clone()).await {
                Ok(response) => return (leader, response),
                Err(WriteError::NotLeader) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("append on {leader} failed: {err}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_voters_keep_acked_batches_after_losing_the_leader() {
        let dir = tempfile::tempdir().unwrap();
        let router = Arc::new(Router::default());
        let mut shards = Vec::new();
        for id in 1..=3 {
            let path = dir.path().join(format!("n{id}"));
            shards.push(Some(
                Shard::open_in_process(id, &path, router.clone())
                    .await
                    .unwrap(),
            ));
        }
        let nodes: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    BasicNode {
                        addr: format!("wal-{id}"),
                    },
                )
            })
            .collect();
        shards[0].as_ref().unwrap().initialize(nodes).await.unwrap();
        let live: Vec<&Shard> = shards.iter().flatten().collect();
        let leader = wait_leader(&live).await;
        let first = live.iter().find(|shard| shard.node_id == leader).unwrap();
        assert_eq!(first.voters(), vec![1, 2, 3]);
        let follower = live.iter().find(|shard| shard.node_id != leader).unwrap();
        assert!(matches!(
            follower.append(append_of(b"x")).await,
            Err(WriteError::NotLeader)
        ));
        for (offset, value) in [b"a", b"b", b"c"].iter().enumerate() {
            let response = first.append(append_of(*value)).await.unwrap();
            assert_eq!(response.base_offset, offset as i64);
        }

        let gone = shards[(leader - 1) as usize].take().unwrap();
        gone.raft.shutdown().await.unwrap();
        drop(gone);
        let live: Vec<&Shard> = shards.iter().flatten().collect();
        let (next, response) = append_on_leader(&live, append_of(b"d")).await;
        assert_ne!(next, leader);
        let Some(survivor) = live.iter().find(|shard| shard.node_id == next) else {
            panic!("no survivor {next}");
        };
        assert_eq!(response.base_offset, 3, "acked offsets 0..3 survive");
        assert_eq!(survivor.end_offset(&[1; 16], 0), 4);
        let tail = survivor.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap();
        let info = camus_protocol::batch::inspect(&tail, 1024 * 1024).unwrap();
        assert_eq!(info.len(), 4);
    }

    fn idempotent(value: &[u8], producer: i64, epoch: i16, sequence: i32) -> Append {
        use camus_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
            NO_PARTITION_LEADER_EPOCH,
        };
        let records = vec![Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: producer,
            producer_epoch: epoch,
            sequence,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(value)),
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
        .unwrap();
        Append {
            topic: "events".into(),
            partition: 0,
            topic_id: [1; 16],
            epoch: 0,
            raw: buf.freeze(),
            records: 1,
            base_timestamp: 1,
            max_timestamp: 1,
            floor: 0,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_idempotent_retry_is_not_appended_again_after_a_leader_change() {
        let dir = tempfile::tempdir().unwrap();
        let router = Arc::new(Router::default());
        let mut shards = Vec::new();
        for id in 1..=3 {
            let path = dir.path().join(format!("n{id}"));
            shards.push(Some(
                Shard::open_in_process(id, &path, router.clone())
                    .await
                    .unwrap(),
            ));
        }
        let nodes: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    BasicNode {
                        addr: format!("wal-{id}"),
                    },
                )
            })
            .collect();
        shards[0].as_ref().unwrap().initialize(nodes).await.unwrap();
        let live: Vec<&Shard> = shards.iter().flatten().collect();
        let leader = wait_leader(&live).await;
        let first = live.iter().find(|shard| shard.node_id == leader).unwrap();
        let batch = idempotent(b"once", 7, 0, 0);
        let first_ack = first.append(batch.clone()).await.unwrap();
        assert_eq!(first_ack.error_code, 0);
        assert_eq!(first_ack.base_offset, 0);
        let again = first.append(batch.clone()).await.unwrap();
        assert_eq!(again.base_offset, 0);
        assert_eq!(
            first.end_offset(&[1; 16], 0),
            1,
            "the retry is not a second record"
        );

        let gone = shards[(leader - 1) as usize].take().unwrap();
        gone.raft.shutdown().await.unwrap();
        drop(gone);
        let live: Vec<&Shard> = shards.iter().flatten().collect();
        let (next, after) = append_on_leader(&live, batch).await;
        assert_ne!(next, leader);
        assert_eq!(after.error_code, 0);
        assert_eq!(
            after.base_offset, 0,
            "the new leader still has the sequence"
        );
        let (next, followed) = append_on_leader(&live, idempotent(b"next", 7, 0, 1)).await;
        assert_eq!(followed.base_offset, 1);
        let Some(survivor) = live.iter().find(|shard| shard.node_id == next) else {
            panic!("no survivor {next}");
        };
        assert_eq!(survivor.end_offset(&[1; 16], 0), 2);
        let tail = survivor.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap();
        assert_eq!(
            camus_protocol::batch::inspect(&tail, 1024 * 1024)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn a_fresh_shard_starts_at_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let shard = Shard::open(1, dir.path()).await.unwrap();
        shard.ensure_voter().await.unwrap();
        let mut append = append_of(b"after-truncate");
        append.floor = 7;
        assert_eq!(shard.append(append).await.unwrap().base_offset, 7);
        let mut lower = append_of(b"next");
        lower.floor = 2;
        assert_eq!(
            shard.append(lower).await.unwrap().base_offset,
            8,
            "the floor never moves the end backward"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopened_shard_recovers_batches_from_the_log() {
        let dir = tempfile::tempdir().unwrap();
        {
            let shard = Shard::open(1, dir.path()).await.unwrap();
            shard.ensure_voter().await.unwrap();
            for value in [b"a", b"b", b"c"] {
                shard.append(append_of(value)).await.unwrap();
            }
            shard.raft.shutdown().await.unwrap();
        }
        let shard = Shard::open(1, dir.path()).await.unwrap();
        shard.ensure_voter().await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while shard.end_offset(&[1; 16], 0) < 3 {
            assert!(std::time::Instant::now() < deadline, "log not re-applied");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let tail = shard.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap();
        assert_eq!(
            camus_protocol::batch::inspect(&tail, 1 << 20)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(shard.append(append_of(b"d")).await.unwrap().base_offset, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_share_fsyncs_and_keep_offsets_unique() {
        let dir = tempfile::tempdir().unwrap();
        let shard = Arc::new(Shard::open(1, dir.path()).await.unwrap());
        shard.ensure_voter().await.unwrap();
        let (fsyncs_before, _, _) = shard.log_stats();
        let tasks: Vec<_> = (0..64)
            .map(|index| {
                let shard = shard.clone();
                tokio::spawn(async move {
                    let value = format!("v{index}");
                    shard.append(append_of(value.as_bytes())).await.unwrap()
                })
            })
            .collect();
        let mut offsets = Vec::new();
        for task in tasks {
            let response = task.await.unwrap();
            assert_eq!(response.error_code, 0);
            offsets.push(response.base_offset);
        }
        offsets.sort();
        assert_eq!(offsets, (0..64).collect::<Vec<i64>>());
        let (fsyncs, _, _) = shard.log_stats();
        assert!(
            fsyncs - fsyncs_before < 64,
            "64 concurrent appends took {} fsyncs",
            fsyncs - fsyncs_before
        );
        let tail = shard.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap();
        assert_eq!(
            camus_protocol::batch::inspect(&tail, 1 << 20)
                .unwrap()
                .len(),
            64
        );
    }

    async fn wait_purged(shard: &Shard, index: u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let purged = shard.raft.metrics().borrow().purged.map(|log| log.index);
            if purged >= Some(index) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "log never purged to {index}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_snapshot(shard: &Shard, index: u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while shard.raft.metrics().borrow().snapshot.map(|log| log.index) < Some(index) {
            assert!(
                std::time::Instant::now() < deadline,
                "no snapshot at {index}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn drops_of(batches: &[Batch]) -> Vec<SealDrop> {
        batches
            .iter()
            .map(|batch| SealDrop {
                topic: batch.topic.clone(),
                topic_id: batch.topic_id,
                partition: batch.partition,
                start: batch.start,
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_purge_keeps_unsealed_batches_in_the_log_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let last = {
            let shard = Shard::open(1, dir.path()).await.unwrap();
            shard.ensure_voter().await.unwrap();
            for value in [b"a", b"b", b"c"] {
                shard.append(append_of(value)).await.unwrap();
            }
            let last = shard.raft.metrics().borrow().last_applied.unwrap().index;
            let oldest = shard.pending(u64::MAX).await.unwrap()[0].log_index;
            shard.raft.trigger().snapshot().await.unwrap();
            wait_snapshot(&shard, last).await;
            shard.purge(last).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let purged = shard.raft.metrics().borrow().purged.map(|log| log.index);
            assert!(
                purged < Some(oldest),
                "purged to {purged:?}, into the unsealed entry at {oldest}"
            );
            shard.raft.shutdown().await.unwrap();
            last
        };
        let shard = Shard::open(1, dir.path()).await.unwrap();
        shard.ensure_voter().await.unwrap();
        let tail = shard.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap();
        assert_eq!(
            camus_protocol::batch::inspect(&tail, 1 << 20)
                .unwrap()
                .len(),
            3
        );
        let pending = shard.pending(u64::MAX).await.unwrap();
        assert_eq!(
            pending.iter().map(|batch| batch.start).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        // Once sealed, nothing holds the log back but the snapshot.
        let sealed = shard.seal(drops_of(&pending)).await.unwrap();
        assert_eq!(shard.unsealed_bytes(), 0);
        shard.purge(sealed).await.unwrap();
        wait_purged(&shard, last).await;
        assert!(shard
            .read(&[1; 16], 0, 0, 0, usize::MAX)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_follower_behind_a_purge_gets_unsealed_bytes_by_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let router = Arc::new(Router::default());
        let path = |id: u64| dir.path().join(format!("n{id}"));
        let mut shards = Vec::new();
        for id in 1..=3 {
            shards.push(Some(
                Shard::open_in_process(id, &path(id), router.clone())
                    .await
                    .unwrap(),
            ));
        }
        let nodes: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    BasicNode {
                        addr: format!("wal-{id}"),
                    },
                )
            })
            .collect();
        shards[0].as_ref().unwrap().initialize(nodes).await.unwrap();
        let live: Vec<&Shard> = shards.iter().flatten().collect();
        let leader = wait_leader(&live).await;
        let lagging = (1..=3).find(|id| *id != leader).unwrap();

        // Take one follower down; the other two still commit.
        let down = shards[(lagging - 1) as usize].take().unwrap();
        down.raft.shutdown().await.unwrap();
        drop(down);
        let lead = shards[(leader - 1) as usize].as_ref().unwrap();
        for value in [b"a", b"b", b"c"] {
            lead.append(append_of(value)).await.unwrap();
        }
        // Seal the first two, so the log can be purged up to the third.
        let pending = lead.pending(u64::MAX).await.unwrap();
        let kept = pending[2].log_index;
        lead.seal(drops_of(&pending[..2])).await.unwrap();
        let last = lead.raft.metrics().borrow().last_applied.unwrap().index;
        lead.raft.trigger().snapshot().await.unwrap();
        wait_snapshot(lead, last).await;
        lead.purge(last).await.unwrap();
        wait_purged(lead, kept - 1).await;
        assert!(
            lead.raft.metrics().borrow().purged.map(|log| log.index) < Some(kept),
            "the unsealed batch's entry stays"
        );

        // The follower comes back with an empty log past its old end. The
        // leader no longer has those entries, so it sends the snapshot.
        let back = Shard::open_in_process(lagging, &path(lagging), router.clone())
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while back.end_offset(&[1; 16], 0) < 3 {
            assert!(
                std::time::Instant::now() < deadline,
                "follower never caught up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        wait_snapshot(&back, last).await;
        let tail = back.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap();
        assert_eq!(
            camus_protocol::batch::inspect(&tail, 1 << 20)
                .unwrap()
                .len(),
            1,
            "only the unsealed batch"
        );
        assert_eq!(
            tail,
            lead.read(&[1; 16], 0, 0, 0, usize::MAX).await.unwrap(),
            "the follower serves the same bytes"
        );
    }

    #[tokio::test]
    async fn pending_takes_the_oldest_batches_up_to_the_limit() {
        let dir = tempfile::tempdir().unwrap();
        let shard = Shard::open(1, dir.path()).await.unwrap();
        shard.ensure_voter().await.unwrap();
        for value in [b"a", b"b", b"c", b"d"] {
            shard.append(append_of(value)).await.unwrap();
        }
        let one = shard.pending(u64::MAX).await.unwrap()[0].bytes.len() as u64;
        let starts = |batches: Vec<Batch>| batches.iter().map(|b| b.start).collect::<Vec<_>>();
        assert_eq!(starts(shard.pending(0).await.unwrap()), [0], "at least one");
        assert_eq!(starts(shard.pending(2 * one).await.unwrap()), [0, 1]);
        assert_eq!(starts(shard.pending(2 * one + 1).await.unwrap()), [0, 1]);
        assert_eq!(starts(shard.pending(u64::MAX).await.unwrap()), [0, 1, 2, 3]);
    }
}
