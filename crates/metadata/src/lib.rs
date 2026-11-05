//! The metadata Raft group.
//!
//! Phase 0 replicates a string map so the group can elect, commit, and
//! restart. Topic records, consumer groups, and producer ids land in later
//! phases as more variants of [`Command`]. This crate does not pretend those
//! exist yet.

#![forbid(unsafe_code)]

mod machine;
mod network;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use openraft::{Config, Raft};
use serde::{Deserialize, Serialize};

pub use machine::RedbMachine;
pub use network::SharedRouter;
pub use openraft::BasicNode;
pub use openraft::RaftMetrics;

/// Node id in the metadata group.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Raft types for the metadata group.
    pub TypeConfig:
        D = Command,
        R = Response,
);

/// A command committed through the metadata group.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Insert or replace one key.
    Put { key: String, value: String },
}

/// State-machine response for [`Command`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// Previous value, when the command replaced one.
    pub value: Option<String>,
}

/// One broker's metadata Raft node.
pub struct MetadataNode {
    /// The openraft handle. Routes and tests call this directly.
    pub raft: Raft<TypeConfig>,
    machine: Arc<RedbMachine>,
}

impl MetadataNode {
    /// Open the on-disk log and state machine and start Raft.
    ///
    /// `http_addr` is the address other brokers use for Raft RPC. The HTTP
    /// server itself is started by `camus-broker`.
    pub async fn open(node_id: NodeId, dir: &Path, http_addr: &str) -> anyhow::Result<Self> {
        let _ = http_addr;
        Self::start(node_id, dir, network::HttpFactory).await
    }

    /// Open a node whose Raft RPC stays in-process, routed by `group`.
    pub async fn open_in_process(
        node_id: NodeId,
        dir: &Path,
        group: &str,
        router: Arc<SharedRouter>,
    ) -> anyhow::Result<Self> {
        let factory = network::InProcFactory {
            group: group.to_string(),
            router,
        };
        Self::start(node_id, dir, factory).await
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
        let machine = Arc::new(RedbMachine::open(&dir.join("state.redb"))?);
        let raft = Raft::new(node_id, config, network, log, machine.clone()).await?;
        Ok(Self { raft, machine })
    }

    /// Register this in-process node so peers can reach it.
    pub fn register_in_process(&self, group: &str, node_id: NodeId, router: &SharedRouter) {
        router.insert(group, node_id, self.raft.clone());
    }

    /// Create the cluster. Call this once, on one voter, listing every voter.
    pub async fn initialize(&self, nodes: BTreeMap<NodeId, BasicNode>) -> anyhow::Result<()> {
        self.raft.initialize(nodes).await?;
        Ok(())
    }

    /// Commit a put. Fails when this node is not the leader.
    pub async fn write(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> anyhow::Result<Response> {
        let response = self
            .raft
            .client_write(Command::Put {
                key: key.into(),
                value: value.into(),
            })
            .await?;
        Ok(response.data)
    }

    /// Read the latest applied value on this node. This is not a linearizable read.
    pub fn read_local(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self.machine.get(key)?)
    }

    /// Stop Raft tasks. The files stay on disk.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.raft.shutdown().await?;
        Ok(())
    }
}
