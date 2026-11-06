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

use openraft::{Config, Raft};
use serde::{Deserialize, Serialize};

pub use machine::Batch;
use machine::Machine;
pub use network::Router;
use openraft::error::{ClientWriteError, RaftError};
pub use openraft::BasicNode;

/// Voter id. Same numbering as the metadata group.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Raft types for a WAL shard.
    pub TypeConfig:
        D = Command,
        R = Response,
);

/// A committed command.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Append one sealed-later batch.
    Append(Append),
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
    /// Client bytes, checked before they are proposed.
    pub raw: Vec<u8>,
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
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Base offset.
    pub start: i64,
}

/// Applied result.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// Assigned base offset. `-1` when the command is not an append.
    pub base_offset: i64,
    /// Kafka error code.
    pub error_code: i16,
}

/// Why a proposal did not commit.
#[derive(Debug)]
pub enum WriteError {
    /// This node is not the shard leader. The entry was not appended here.
    NotLeader,
    /// Anything else: shutdown, storage, or a lost quorum.
    Other(anyhow::Error),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader => f.write_str("not the wal shard leader"),
            Self::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>> for WriteError {
    fn from(err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Self {
        match err {
            RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => Self::NotLeader,
            other => Self::Other(other.into()),
        }
    }
}

/// A running shard.
pub struct Shard {
    /// Raft handle.
    pub raft: Raft<TypeConfig>,
    machine: Arc<Machine>,
    node_id: NodeId,
}

impl Shard {
    /// Open the log and state machine with HTTP RPC between brokers. A fresh
    /// shard waits for [`Shard::initialize`]; a restarted one rejoins from
    /// its stored membership.
    pub async fn open(node_id: NodeId, dir: &Path) -> anyhow::Result<Self> {
        Self::start(node_id, dir, network::HttpFactory).await
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
        let config = Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        };
        let config = Arc::new(config.validate()?);
        let log = camus_raft::SegmentLogStore::open(dir.join("log"))?;
        let machine = Arc::new(Machine::open(&dir.join("state.redb"))?);
        let raft = Raft::new(node_id, config, network, log, machine.clone()).await?;
        Ok(Self {
            raft,
            machine,
            node_id,
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
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node_id)
    }

    /// Propose a batch and wait until a majority has it on disk and this
    /// leader has applied it.
    pub async fn append(&self, batch: Append) -> Result<Response, WriteError> {
        let response = self.raft.client_write(Command::Append(batch)).await?;
        Ok(response.data)
    }

    /// Drop published batches and return the log index of that commit.
    pub async fn seal(&self, drops: Vec<SealDrop>) -> Result<u64, WriteError> {
        let response = self.raft.client_write(Command::Seal { drops }).await?;
        Ok(response.log_id.index)
    }

    /// Purge the shard log through `index`, inclusive.
    pub async fn purge(&self, index: u64) -> anyhow::Result<()> {
        self.raft.trigger().purge_log(index).await?;
        Ok(())
    }

    /// Next offset for a partition. `0` when the shard has not appended.
    pub fn end_offset(&self, topic: &str, partition: i32) -> i64 {
        self.machine.end_offset(topic, partition)
    }

    /// Committed batches not yet sealed.
    pub fn pending(&self) -> Vec<Batch> {
        self.machine.pending()
    }

    /// Unsealed batches for a fetch.
    pub fn read(&self, topic: &str, partition: i32, sealed_end: i64, offset: i64) -> Vec<u8> {
        self.machine.read(topic, partition, sealed_end, offset)
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
                raw: vec![0; 8],
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
            raw: record_batch(value),
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
        let next = wait_leader(&live).await;
        assert_ne!(next, leader);
        let survivor = live.iter().find(|shard| shard.node_id == next).unwrap();
        let response = survivor.append(append_of(b"d")).await.unwrap();
        assert_eq!(response.base_offset, 3, "acked offsets 0..3 survive");
        assert_eq!(survivor.end_offset("events", 0), 4);
        let tail = survivor.read("events", 0, 0, 0);
        let info = camus_protocol::batch::inspect(&tail, 1024 * 1024).unwrap();
        assert_eq!(info.len(), 4);
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
}
