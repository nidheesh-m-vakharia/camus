//! Camus broker. Phase 0 serves the metadata Raft group and Prometheus.
//! Kafka produce and fetch are not accepted here.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use camus_metadata::{BasicNode, MetadataNode, NodeId};
use clap::Parser;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde::Deserialize;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "camus-broker")]
struct Args {
    /// Voter id. Must be unique in the metadata group.
    #[arg(long)]
    node_id: NodeId,
    /// `host:port` to bind. Use port 0 in tests; the bound address is written
    /// to `{data_dir}/listen-addr`.
    #[arg(long)]
    http_addr: String,
    /// Directory for the Raft log and the redb state machine.
    #[arg(long)]
    data_dir: PathBuf,
}

#[derive(Deserialize)]
struct Peer {
    id: NodeId,
    addr: String,
}

#[derive(Deserialize)]
struct Put {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct ReadQuery {
    key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    let node = Arc::new(MetadataNode::open(args.node_id, &args.data_dir, &args.http_addr).await?);

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(prometheus))
        .route("/admin/metrics", get(admin_metrics))
        .route("/admin/init", post(init))
        .route("/admin/write", post(write))
        .route("/admin/read", get(read))
        .route("/raft-vote", post(vote))
        .route("/raft-append", post(append))
        .route("/raft-snapshot", post(snapshot))
        .with_state(node);

    let listener = tokio::net::TcpListener::bind(&args.http_addr).await?;
    let bound = listener.local_addr()?;
    std::fs::write(args.data_dir.join("listen-addr"), bound.to_string())?;
    info!(node_id = args.node_id, %bound, "metadata broker listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn prometheus(State(node): State<Arc<MetadataNode>>) -> String {
    let metrics = node.raft.metrics().borrow().clone();
    let leader = metrics
        .current_leader
        .map(|id| id.to_string())
        .unwrap_or_else(|| "NaN".into());
    format!(
        "# HELP camus_up Broker process is running.\n\
         # TYPE camus_up gauge\n\
         camus_up 1\n\
         # HELP camus_raft_term Current metadata Raft term.\n\
         # TYPE camus_raft_term gauge\n\
         camus_raft_term {}\n\
         # HELP camus_raft_leader 1 when this node is the current leader.\n\
         # TYPE camus_raft_leader gauge\n\
         camus_raft_leader {}\n\
         # HELP camus_raft_current_leader Node id of the current leader, or NaN.\n\
         # TYPE camus_raft_current_leader gauge\n\
         camus_raft_current_leader {leader}\n",
        metrics.current_term,
        if metrics.current_leader == Some(metrics.id) {
            1
        } else {
            0
        },
    )
}

async fn admin_metrics(State(node): State<Arc<MetadataNode>>) -> Json<serde_json::Value> {
    let metrics = node.raft.metrics().borrow().clone();
    Json(serde_json::json!({
        "id": metrics.id,
        "current_term": metrics.current_term,
        "current_leader": metrics.current_leader,
        "state": format!("{:?}", metrics.state),
    }))
}

async fn init(
    State(node): State<Arc<MetadataNode>>,
    Json(peers): Json<Vec<Peer>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut nodes = BTreeMap::new();
    for peer in peers {
        nodes.insert(peer.id, BasicNode { addr: peer.addr });
    }
    node.initialize(nodes)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|err| (StatusCode::CONFLICT, err.to_string()))
}

async fn write(
    State(node): State<Arc<MetadataNode>>,
    Json(body): Json<Put>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let response = node
        .write(body.key, body.value)
        .await
        .map_err(|err| (StatusCode::SERVICE_UNAVAILABLE, err.to_string()))?;
    Ok(Json(serde_json::json!({ "previous": response.value })))
}

async fn read(
    State(node): State<Arc<MetadataNode>>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let value = node
        .read_local(&query.key)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(Json(serde_json::json!({ "value": value })))
}

async fn vote(
    State(node): State<Arc<MetadataNode>>,
    Json(req): Json<VoteRequest<NodeId>>,
) -> Json<Result<openraft::raft::VoteResponse<NodeId>, openraft::error::RaftError<NodeId>>> {
    Json(node.raft.vote(req).await)
}

async fn append(
    State(node): State<Arc<MetadataNode>>,
    Json(req): Json<AppendEntriesRequest<camus_metadata::TypeConfig>>,
) -> Json<Result<openraft::raft::AppendEntriesResponse<NodeId>, openraft::error::RaftError<NodeId>>>
{
    Json(node.raft.append_entries(req).await)
}

async fn snapshot(
    State(node): State<Arc<MetadataNode>>,
    Json(req): Json<InstallSnapshotRequest<camus_metadata::TypeConfig>>,
) -> Json<
    Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
    >,
> {
    Json(node.raft.install_snapshot(req).await)
}
