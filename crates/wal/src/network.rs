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
/// Shard 0 uses `/wal/raft-*`, as before shards had ids; shard `n` uses
/// `/wal/<n>/raft-*` (ADR 0014).
#[derive(Clone, Default)]
pub struct HttpFactory {
    /// Path under the broker's HTTP root: `wal` or `wal/<n>`.
    pub prefix: String,
    shard: u32,
}

impl HttpFactory {
    /// The factory for shard `shard`.
    pub fn for_shard(shard: u32) -> Self {
        Self {
            prefix: if shard == 0 {
                "wal".into()
            } else {
                format!("wal/{shard}")
            },
            shard,
        }
    }
}

pub struct HttpConn {
    target: NodeId,
    addr: String,
    prefix: String,
    shard: u32,
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
            prefix: self.prefix.clone(),
            shard: self.shard,
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
        let carries = !rpc.entries.is_empty();
        if !carries {
            if let Some(result) = self
                .try_quic(camus_raft::quic::KIND_HEARTBEAT, &rpc, &option)
                .await
            {
                return result;
            }
        }
        let started = std::time::Instant::now();
        let result = self.send("raft-append", rpc, option).await;
        if carries {
            use std::sync::atomic::Ordering::Relaxed;
            crate::RPC_TIMES.sent_count.fetch_add(1, Relaxed);
            crate::RPC_TIMES
                .sent_us
                .fetch_add(started.elapsed().as_micros() as u64, Relaxed);
        }
        result
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
            route: camus_raft::quic::ROUTE_WAL,
            shard: self.shard,
            kind,
            body,
        };
        let peer = camus_raft::quic::ControlEndpoint::peer_socket(&self.addr).ok()?;
        let outcome = tokio::time::timeout(option.hard_ttl(), endpoint.call(peer, &frame)).await;
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
        let url = format!("http://{}/{}/{path}", self.addr, self.prefix);
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
