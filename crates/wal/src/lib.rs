//! One WAL shard.
//!
//! Produce waits for this Raft group to commit and fsync. The R2 upload
//! happens after the ack. A single broker initializes a one-voter shard,
//! which is a quorum of one.

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
use openraft::BasicNode;

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

/// A running shard.
pub struct Shard {
    /// Raft handle.
    pub raft: Raft<TypeConfig>,
    machine: Arc<Machine>,
    node_id: NodeId,
}

impl Shard {
    /// Open the log and state machine. Call [`Shard::ensure_voter`] once.
    pub async fn open(node_id: NodeId, dir: &Path) -> anyhow::Result<Self> {
        let config = Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        };
        let config = Arc::new(config.validate()?);
        let log = camus_raft::SegmentLogStore::open(dir.join("log"))?;
        let machine = Arc::new(Machine::open(&dir.join("state.redb"))?);
        let router = Arc::new(Router::default());
        let raft = Raft::new(
            node_id,
            config,
            network::Factory {
                router: router.clone(),
            },
            log,
            machine.clone(),
        )
        .await?;
        router.insert(node_id, raft.clone());
        Ok(Self {
            raft,
            machine,
            node_id,
        })
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

    /// Propose a batch and wait for the commit.
    pub async fn append(&self, batch: Append) -> anyhow::Result<Response> {
        let response = self.raft.client_write(Command::Append(batch)).await?;
        Ok(response.data)
    }

    /// Drop published batches and return the log index of that commit.
    pub async fn seal(&self, drops: Vec<SealDrop>) -> anyhow::Result<u64> {
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
            })
            .await
            .unwrap();
        assert_eq!(response.error_code, 2, "a short buffer is not a batch");
        drop(shard);
    }
}
