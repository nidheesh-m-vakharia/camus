//! Raft RPC. Brokers use HTTP. Tests use an in-process router so two groups
//! can share a process without sharing a log.

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

pub type RpcError<E = openraft::error::Infallible> =
    RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

/// Routes in-process Raft RPC by `(group, node)`.
#[derive(Clone, Default)]
pub struct SharedRouter {
    inner: Arc<Mutex<HashMap<(String, NodeId), openraft::Raft<TypeConfig>>>>,
}

impl SharedRouter {
    /// Publish a node after `Raft::new` has returned.
    pub fn insert(&self, group: &str, node_id: NodeId, raft: openraft::Raft<TypeConfig>) {
        self.inner
            .lock()
            .expect("router lock")
            .insert((group.to_string(), node_id), raft);
    }
}

#[derive(Clone)]
pub struct InProcFactory {
    pub group: String,
    pub router: Arc<SharedRouter>,
}

pub struct InProcConn {
    group: String,
    target: NodeId,
    router: Arc<SharedRouter>,
}

impl RaftNetworkFactory<TypeConfig> for InProcFactory {
    type Network = InProcConn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InProcConn {
            group: self.group.clone(),
            target,
            router: self.router.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for InProcConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        let raft = {
            let guard = self.router.inner.lock().expect("router lock");
            guard
                .get(&(self.group.clone(), self.target))
                .cloned()
                .ok_or_else(|| {
                    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("missing node")))
                })?
        };
        raft.append_entries(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        let raft = {
            let guard = self.router.inner.lock().expect("router lock");
            guard
                .get(&(self.group.clone(), self.target))
                .cloned()
                .ok_or_else(|| {
                    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("missing node")))
                })?
        };
        raft.install_snapshot(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        let raft = {
            let guard = self.router.inner.lock().expect("router lock");
            guard
                .get(&(self.group.clone(), self.target))
                .cloned()
                .ok_or_else(|| {
                    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("missing node")))
                })?
        };
        raft.vote(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}

/// HTTP RPC factory. `BasicNode.addr` is `host:port` with no scheme.
#[derive(Clone, Default)]
pub struct HttpFactory;

pub struct HttpConn {
    target: NodeId,
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for HttpFactory {
    type Network = HttpConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        HttpConn {
            target,
            addr: node.addr.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for HttpConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        send(self.target, &self.addr, "raft-append", rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        send(self.target, &self.addr, "raft-snapshot", rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        send(self.target, &self.addr, "raft-vote", rpc).await
    }
    /// Retry an unreachable peer every 100ms. openraft's default is 500ms,
    /// which leaves a restarted broker's replica stale for up to a second.
    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(std::time::Duration::from_millis(100)))
    }
}

async fn send<Req, Resp, E>(
    target: NodeId,
    addr: &str,
    path: &str,
    req: Req,
) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
where
    Req: Serialize,
    Resp: DeserializeOwned,
    E: std::error::Error + DeserializeOwned,
{
    let url = format!("http://{addr}/{path}");
    let client = reqwest::Client::new();
    let body = camus_raft::codec::encode(&req)
        .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
    let response = client
        .post(&url)
        .header(
            reqwest::header::CONTENT_TYPE,
            camus_raft::codec::CONTENT_TYPE,
        )
        .body(body)
        .send()
        .await
        .map_err(|err| {
            if err.is_connect() {
                RPCError::Unreachable(Unreachable::new(&err))
            } else {
                RPCError::Network(NetworkError::new(&err))
            }
        })?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
    if !status.is_success() {
        let text = String::from_utf8_lossy(&bytes).into_owned();
        return Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other(format!("{status}: {text}")),
        )));
    }
    let body: Result<Resp, RaftError<NodeId, E>> = camus_raft::codec::decode(&bytes)
        .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
    body.map_err(|err| RPCError::RemoteError(RemoteError::new(target, err)))
}
