//! Camus broker. The HTTP port is the metadata Raft group. `--kafka-addr`
//! accepts the Kafka protocol in `r2` mode.

#![forbid(unsafe_code)]

mod kafka;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use camus_metadata::{BasicNode, MetadataNode, NodeId};
use camus_storage::{Bucket, BucketConfig};
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
    /// Kafka listener. PLAINTEXT needs `--dev-plaintext`. SASL_SSL needs
    /// `--tls-cert` and `--tls-key`.
    #[arg(long)]
    kafka_addr: Option<String>,
    /// PEM certificate for the SASL_SSL listener.
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    /// PEM private key for the SASL_SSL listener.
    #[arg(long)]
    tls_key: Option<PathBuf>,
    /// Allow a PLAINTEXT Kafka listener. Refused for a non-loopback bind unless
    /// `--allow-non-loopback` is also set.
    #[arg(long, default_value_t = false)]
    dev_plaintext: bool,
    /// Allow the PLAINTEXT listener to bind outside the loopback interface.
    #[arg(long, default_value_t = false)]
    allow_non_loopback: bool,
    /// Require SASL/PLAIN before any API other than ApiVersions and the SASL pair.
    #[arg(long, default_value_t = false)]
    require_sasl: bool,
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
        .with_state(Arc::clone(&node));

    if let Some(kafka_addr) = &args.kafka_addr {
        start_kafka(node.clone(), args.node_id, kafka_addr, &args).await?;
    }

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

async fn start_kafka(
    node: Arc<MetadataNode>,
    node_id: NodeId,
    kafka_addr: &str,
    args: &Args,
) -> anyhow::Result<()> {
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(load_tls(cert, key)?),
        (None, None) => None,
        _ => anyhow::bail!("--tls-cert and --tls-key are set together"),
    };
    if tls.is_none() && !args.dev_plaintext {
        anyhow::bail!("Kafka listener requires --dev-plaintext or a TLS certificate");
    }
    let bind: SocketAddr = kafka_addr.parse()?;
    if tls.is_none() && !bind.ip().is_loopback() && !args.allow_non_loopback {
        anyhow::bail!("non-loopback PLAINTEXT requires --allow-non-loopback");
    }
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let bound = listener.local_addr()?;
    std::fs::write(args.data_dir.join("kafka-addr"), bound.to_string())?;
    let host = if bind.ip().is_unspecified() {
        "127.0.0.1".to_string()
    } else {
        bound.ip().to_string()
    };
    let bucket = Bucket::connect(&BucketConfig {
        endpoint: std::env::var("CAMUS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9100".into()),
        bucket: std::env::var("CAMUS_S3_BUCKET").unwrap_or_else(|_| "camus".into()),
        region: "us-east-1".into(),
        access_key: std::env::var("CAMUS_S3_ACCESS_KEY").unwrap_or_else(|_| "camus".into()),
        secret_key: std::env::var("CAMUS_S3_SECRET_KEY")
            .unwrap_or_else(|_| "camus-dev-secret".into()),
    })?;
    let user = std::env::var("CAMUS_SASL_USER").unwrap_or_default();
    let password = std::env::var("CAMUS_SASL_PASSWORD").unwrap_or_default();
    let require_sasl = args.require_sasl || tls.is_some();
    if require_sasl && (user.is_empty() || password.is_empty()) {
        anyhow::bail!("SASL requires CAMUS_SASL_USER and CAMUS_SASL_PASSWORD");
    }
    let kafka = Arc::new(kafka::Kafka::new(
        node,
        bucket,
        i32::try_from(node_id)?,
        host,
        bound.port() as i32,
        require_sasl,
        user,
        password,
        tls,
    ));
    info!(%bound, sasl = require_sasl, "kafka listener");
    tokio::spawn(async move { kafka.run(listener).await });
    Ok(())
}

fn load_tls(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_file = std::fs::File::open(cert_path)?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader).collect::<Result<_, _>>()?;
    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow::anyhow!("no private key in {key_path:?}"))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
}
