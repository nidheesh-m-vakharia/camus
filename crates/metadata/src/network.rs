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
    /// Kept for the connection's life, so RPCs reuse pooled keep-alive
    /// connections instead of a new TCP handshake each.
    client: reqwest::Client,
    /// After a QUIC failure, HTTP is used until this instant (ADR 0016).
    quic_after: std::time::Instant,
}

impl RaftNetworkFactory<TypeConfig> for HttpFactory {
    type Network = HttpConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        HttpConn {
            target,
            addr: node.addr.clone(),
            client: reqwest::Client::new(),
            quic_after: std::time::Instant::now(),
        }
    }
}

impl RaftNetwork<TypeConfig> for HttpConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        if rpc.entries.is_empty() {
            if let Some(result) = self
                .try_quic(camus_raft::quic::KIND_HEARTBEAT, &rpc, &option)
                .await
            {
                return result;
            }
        }
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
        if let Some(result) = self
            .try_quic(camus_raft::quic::KIND_VOTE, &rpc, &option)
            .await
        {
            return result;
        }
        self.send("raft-vote", rpc, option).await
    }
    /// Retry an unreachable peer every 100ms. openraft's default is 500ms,
    /// which leaves a restarted broker's replica stale for up to a second.
    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(std::time::Duration::from_millis(100)))
    }
}

impl HttpConn {
    /// QUIC for a vote or an empty heartbeat. `None` means use HTTP.
    async fn try_quic<Req, Resp, E>(
        &mut self,
        kind: u8,
        req: &Req,
        option: &RPCOption,
    ) -> Option<Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        if !camus_raft::quic::enabled() || std::time::Instant::now() < self.quic_after {
            return None;
        }
        let Some(endpoint) = camus_raft::quic::current() else {
            return None;
        };
        let body = camus_raft::codec::encode(req).ok()?;
        let frame = camus_raft::quic::Frame {
            route: camus_raft::quic::ROUTE_METADATA,
            shard: 0,
            kind,
            body,
        };
        let peer = camus_raft::quic::ControlEndpoint::peer_socket(&self.addr).ok()?;
        if !endpoint.live(peer).await {
            endpoint.warm(peer);
            return None;
        }
        // Half the budget. openraft cancels the whole RPC at `hard_ttl`, so a
        // QUIC attempt that uses all of it never reaches the HTTP fallback.
        let quic_budget = option.hard_ttl() / 2;
        if quic_budget.is_zero() {
            return None;
        }
        let outcome = tokio::time::timeout(quic_budget, endpoint.call(peer, &frame)).await;
        let bytes = match outcome {
            Ok(Ok(bytes)) => bytes,
            _ => {
                self.quic_after = std::time::Instant::now() + std::time::Duration::from_secs(1);
                return None;
            }
        };
        let decoded: Result<Resp, RaftError<NodeId, E>> = match camus_raft::codec::decode(&bytes) {
            Ok(decoded) => decoded,
            Err(_) => {
                self.quic_after = std::time::Instant::now() + std::time::Duration::from_secs(1);
                return None;
            }
        };
        Some(decoded.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err))))
    }

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
        let url = format!("http://{}/{path}", self.addr);
        let body = camus_raft::codec::encode(&req)
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let response = self
            .client
            .post(&url)
            .timeout(option.hard_ttl())
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
        body.map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}
