//! Raft RPC for one WAL shard. Brokers use HTTP. Tests use an in-process router.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{NodeId, TypeConfig};

type RpcError<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

/// Routes RPC to the shard nodes in this process.
#[derive(Clone, Default)]
pub struct Router {
    inner: Arc<Mutex<HashMap<NodeId, openraft::Raft<TypeConfig>>>>,
}

impl Router {
    pub fn insert(&self, node_id: NodeId, raft: openraft::Raft<TypeConfig>) {
        self.inner.lock().expect("wal router").insert(node_id, raft);
    }
}

#[derive(Clone)]
pub struct Factory {
    pub router: Arc<Router>,
}

pub struct Conn {
    target: NodeId,
    router: Arc<Router>,
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Conn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Conn {
            target,
            router: self.router.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        let raft = self.raft()?;
        raft.append_entries(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        let raft = self.raft().map_err(|err| match err {
            RPCError::Unreachable(inner) => RPCError::Unreachable(inner),
            other => {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(other.to_string())))
            }
        })?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        let raft = self.raft()?;
        raft.vote(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}

impl Conn {
    fn raft(&self) -> Result<openraft::Raft<TypeConfig>, RpcError> {
        self.router
            .inner
            .lock()
            .expect("wal router")
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other("missing wal node")))
            })
    }
}

/// HTTP RPC factory. `BasicNode.addr` is the broker HTTP `host:port`; shard
/// RPC lives under `/wal/` so it does not collide with the metadata group.
#[derive(Clone, Default)]
pub struct HttpFactory;

pub struct HttpConn {
    target: NodeId,
    addr: String,
    client: reqwest::Client,
}

impl RaftNetworkFactory<TypeConfig> for HttpFactory {
    type Network = HttpConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        HttpConn {
            target,
            addr: node.addr.clone(),
            client: reqwest::Client::new(),
        }
    }
}

impl RaftNetwork<TypeConfig> for HttpConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        self.send("raft-append", rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        self.send("raft-snapshot", rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        self.send("raft-vote", rpc, option).await
    }
}

impl HttpConn {
    async fn send<Req, Resp, E>(
        &self,
        path: &str,
        req: Req,
        option: RPCOption,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let url = format!("http://{}/wal/{path}", self.addr);
        let response = self
            .client
            .post(&url)
            .timeout(option.hard_ttl())
            .json(&req)
            .send()
            .await
            .map_err(|err| {
                if err.is_connect() {
                    RPCError::Unreachable(Unreachable::new(&err))
                } else {
                    RPCError::Network(NetworkError::new(&err))
                }
            })?;
        let body: Result<Resp, RaftError<NodeId, E>> = response
            .json()
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        body.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}
