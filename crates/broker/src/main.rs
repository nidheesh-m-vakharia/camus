//! Camus broker. The HTTP port carries the metadata Raft group and, under
//! `/wal/`, the WAL shard. `--kafka-addr` accepts the Kafka protocol.

#![forbid(unsafe_code)]

mod kafka;
mod slot;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::serve::ListenerExt;
use axum::{Json, Router};
use camus_metadata::{BasicNode, Command, MetadataNode, NodeId, ProducerNote, PublishPart};
use camus_storage::{Bucket, BucketConfig};
use camus_wal::Shard;
use clap::Parser;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde::Deserialize;
use slot::WalSlot;
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
    /// WAL shard directory. Defaults to `{data_dir}/wal`. Put it on its own
    /// volume to keep a full WAL disk from stopping metadata.
    #[arg(long)]
    wal_dir: Option<PathBuf>,
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

/// Largest Raft RPC body the HTTP routes accept.
const RAFT_BODY_LIMIT: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
struct Peer {
    id: NodeId,
    addr: String,
    /// Whether this broker is a WAL shard voter. Defaults to yes. Every
    /// broker is a metadata voter.
    #[serde(default = "yes")]
    wal: bool,
}

fn yes() -> bool {
    true
}

#[derive(Deserialize)]
struct Truncate {
    #[serde(default)]
    i_accept_data_loss: bool,
}

/// State for routes that touch both groups.
struct Admin {
    node: Arc<MetadataNode>,
    wal: Arc<WalSlot>,
    node_id: NodeId,
    wal_dir: PathBuf,
    http: reqwest::Client,
    truncating: tokio::sync::Mutex<()>,
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
    let wal_dir = args
        .wal_dir
        .clone()
        .unwrap_or_else(|| args.data_dir.join("wal"));
    let wal = Arc::new(WalSlot::new(Shard::open(args.node_id, &wal_dir).await?));
    let admin = Arc::new(Admin {
        node: Arc::clone(&node),
        wal: Arc::clone(&wal),
        node_id: args.node_id,
        wal_dir,
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?,
        truncating: tokio::sync::Mutex::new(()),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(prometheus))
        .route("/admin/write", post(write))
        .route("/admin/publish", post(publish))
        .route("/admin/apply", post(apply_command))
        .route("/admin/read", get(read))
        .route("/raft-vote", post(vote))
        .route("/raft-append", post(append))
        .route("/raft-snapshot", post(snapshot))
        .with_state(Arc::clone(&node))
        .merge(
            Router::new()
                .route("/admin/metrics", get(admin_metrics))
                .route("/admin/init", post(init))
                .route("/admin/truncate-to-sealed", post(truncate_to_sealed))
                .with_state(Arc::clone(&admin)),
        )
        .merge(
            Router::new()
                .route("/wal/init", post(wal_init))
                .route("/wal/raft-vote", post(wal_vote))
                .route("/wal/raft-append", post(wal_append))
                .route("/wal/raft-snapshot", post(wal_snapshot))
                .with_state(Arc::clone(&wal)),
        );

    if let Some(kafka_addr) = &args.kafka_addr {
        start_kafka(node.clone(), wal, args.node_id, kafka_addr, &args).await?;
    }

    let listener = tokio::net::TcpListener::bind(&args.http_addr).await?;
    let bound = listener.local_addr()?;
    std::fs::write(args.data_dir.join("listen-addr"), bound.to_string())?;
    info!(node_id = args.node_id, %bound, "metadata broker listening");
    // AppendEntries carries produce batches. axum's default 2 MB body limit
    // rejected a single 1 MiB client batch once JSON-encoded, and replication
    // stalled until followers elected. The shard caps an RPC well under this.
    let app = app.layer(axum::extract::DefaultBodyLimit::max(RAFT_BODY_LIMIT));
    // Raft RPCs are small request/response pairs; Nagle plus delayed ACK
    // would add tens of milliseconds to every quorum round trip.
    let listener = listener.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
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

async fn admin_metrics(State(admin): State<Arc<Admin>>) -> Json<serde_json::Value> {
    let metrics = admin.node.raft.metrics().borrow().clone();
    let wal = admin.wal.get();
    Json(serde_json::json!({
        "id": metrics.id,
        "current_term": metrics.current_term,
        "current_leader": metrics.current_leader,
        "state": format!("{:?}", metrics.state),
        "wal_leader": wal.leader(),
        "wal_voters": wal.voters(),
        "wal_unsealed_bytes": wal.unsealed_bytes(),
        "wal_log_fsyncs": wal.log_stats().0,
        "wal_log_entries": wal.log_stats().1,
        "wal_log_bytes": wal.log_stats().2,
        "wal_rpc_sent_count": camus_wal::RPC_TIMES.sent_count.load(std::sync::atomic::Ordering::Relaxed),
        "wal_rpc_sent_us": camus_wal::RPC_TIMES.sent_us.load(std::sync::atomic::Ordering::Relaxed),
        "wal_rpc_handled_count": camus_wal::RPC_TIMES.handled_count.load(std::sync::atomic::Ordering::Relaxed),
        "wal_rpc_handled_us": camus_wal::RPC_TIMES.handled_us.load(std::sync::atomic::Ordering::Relaxed),
        "wal_storage_failed": wal.storage_failed(),
    }))
}

/// Create the metadata group from every peer and the WAL shard from the
/// peers marked `wal`. Each `addr` is that broker's HTTP address; both
/// groups' RPC goes there. The shard is initialized on its first voter.
async fn init(
    State(admin): State<Arc<Admin>>,
    Json(peers): Json<Vec<Peer>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut nodes = BTreeMap::new();
    let mut shard = BTreeMap::new();
    for peer in peers {
        let node = BasicNode { addr: peer.addr };
        if peer.wal {
            shard.insert(peer.id, node.clone());
        }
        nodes.insert(peer.id, node);
    }
    let Some(first) = shard.values().next().cloned() else {
        return Err((StatusCode::BAD_REQUEST, "no wal voter".into()));
    };
    admin
        .node
        .initialize(nodes)
        .await
        .map_err(|err| (StatusCode::CONFLICT, err.to_string()))?;
    let response = admin
        .http
        .post(format!("http://{}/wal/init", first.addr))
        .json(&shard)
        .send()
        .await
        .map_err(|err| (StatusCode::BAD_GATEWAY, err.to_string()))?;
    if !response.status().is_success() {
        let status =
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        return Err((status, response.text().await.unwrap_or_default()));
    }
    Ok(StatusCode::OK)
}

async fn wal_init(
    State(wal): State<Arc<WalSlot>>,
    Json(nodes): Json<BTreeMap<NodeId, BasicNode>>,
) -> Result<StatusCode, (StatusCode, String)> {
    wal.get()
        .initialize(nodes)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|err| (StatusCode::CONFLICT, err.to_string()))
}

/// ADR 0008 majority loss: replace this survivor's shard with a fresh
/// one-voter shard. Acknowledged entries that metadata had not sealed are
/// gone; each partition resumes at its sealed end. The old WAL directory is
/// moved aside, not deleted.
///
/// Refused without `i_accept_data_loss`, and refused while the shard still
/// has a leader that can reach a quorum, because then nothing is lost and a
/// second shard would split the partitions.
async fn truncate_to_sealed(
    State(admin): State<Arc<Admin>>,
    Json(body): Json<Truncate>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !body.i_accept_data_loss {
        return Err((
            StatusCode::BAD_REQUEST,
            "truncate-to-sealed drops acknowledged offsets that are not sealed; \
             pass i_accept_data_loss to run it"
                .into(),
        ));
    }
    let _one = admin.truncating.lock().await;
    let me = admin.node_id;
    let old = admin.wal.get();
    if !old.voters().contains(&me) {
        return Err((
            StatusCode::CONFLICT,
            format!("node {me} is not a wal voter"),
        ));
    }
    let leader = old.leader();
    let healthy = if leader == Some(me) {
        old.leads_quorum(std::time::Duration::from_secs(2))
    } else {
        leader.is_some()
    };
    if healthy {
        return Err((
            StatusCode::CONFLICT,
            "the wal shard has a working leader; restore a lost voter instead".into(),
        ));
    }
    let addr = admin
        .node
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .get_node(&me)
        .map(|node| node.addr.clone())
        .ok_or_else(|| (StatusCode::CONFLICT, format!("node {me} has no address")))?;
    let internal = |err: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    let _ = old.raft.shutdown().await;
    let dir = admin.wal_dir.clone();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default();
    let name = dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "wal".into());
    let aside = dir.with_file_name(format!("{name}.truncated-{stamp}"));
    std::fs::rename(&dir, &aside).map_err(|err| internal(err.into()))?;
    let fresh = Shard::open(me, &dir).await.map_err(internal)?;
    fresh
        .initialize(BTreeMap::from([(me, BasicNode { addr })]))
        .await
        .map_err(internal)?;
    admin.wal.replace(fresh);
    tracing::warn!(node_id = me, aside = %aside.display(), "wal shard truncated to sealed");
    Ok(Json(serde_json::json!({
        "voters": [me],
        "moved_aside": aside.display().to_string(),
    })))
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

#[derive(Deserialize)]
struct PublishBody {
    parts: Vec<PublishPart>,
    #[serde(default)]
    producers: Vec<ProducerNote>,
}

/// Commit a metadata command for a broker that does not lead the group.
async fn apply_command(
    State(node): State<Arc<MetadataNode>>,
    Json(command): Json<Command>,
) -> Result<Json<camus_metadata::Response>, (StatusCode, String)> {
    node.apply(command)
        .await
        .map(Json)
        .map_err(|err| (StatusCode::SERVICE_UNAVAILABLE, err.to_string()))
}

/// Commit extents on behalf of a WAL shard leader that does not lead metadata.
async fn publish(
    State(node): State<Arc<MetadataNode>>,
    Json(body): Json<PublishBody>,
) -> Result<Json<camus_metadata::Response>, (StatusCode, String)> {
    node.publish(body.parts, body.producers)
        .await
        .map(Json)
        .map_err(|err| (StatusCode::SERVICE_UNAVAILABLE, err.to_string()))
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

/// Decode a postcard Raft RPC body, or answer 400.
fn rpc_body<T: serde::de::DeserializeOwned>(
    body: &axum::body::Bytes,
) -> Result<T, axum::response::Response> {
    camus_raft::codec::decode(body).map_err(|err| {
        axum::response::IntoResponse::into_response((StatusCode::BAD_REQUEST, err.to_string()))
    })
}

/// Encode a Raft RPC result as postcard.
fn rpc_reply<T: serde::Serialize>(value: &T) -> axum::response::Response {
    match camus_raft::codec::encode(value) {
        Ok(bytes) => axum::response::IntoResponse::into_response((
            [(
                axum::http::header::CONTENT_TYPE,
                camus_raft::codec::CONTENT_TYPE,
            )],
            bytes,
        )),
        Err(err) => axum::response::IntoResponse::into_response((
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )),
    }
}

async fn vote(
    State(node): State<Arc<MetadataNode>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let req: VoteRequest<NodeId> = match rpc_body(&body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&node.raft.vote(req).await)
}

async fn append(
    State(node): State<Arc<MetadataNode>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let req: AppendEntriesRequest<camus_metadata::TypeConfig> = match rpc_body(&body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&node.raft.append_entries(req).await)
}

async fn snapshot(
    State(node): State<Arc<MetadataNode>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let req: InstallSnapshotRequest<camus_metadata::TypeConfig> = match rpc_body(&body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&node.raft.install_snapshot(req).await)
}

async fn wal_vote(
    State(wal): State<Arc<WalSlot>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let req: VoteRequest<NodeId> = match rpc_body(&body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&wal.get().raft.vote(req).await)
}

async fn wal_append(
    State(wal): State<Arc<WalSlot>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let req: AppendEntriesRequest<camus_wal::TypeConfig> = match rpc_body(&body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    let carries = !req.entries.is_empty();
    let started = std::time::Instant::now();
    let result = wal.get().raft.append_entries(req).await;
    if carries {
        camus_wal::RPC_TIMES.handled(started.elapsed());
    }
    rpc_reply(&result)
}

async fn wal_snapshot(
    State(wal): State<Arc<WalSlot>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let req: InstallSnapshotRequest<camus_wal::TypeConfig> = match rpc_body(&body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&wal.get().raft.install_snapshot(req).await)
}

async fn start_kafka(
    node: Arc<MetadataNode>,
    wal: Arc<WalSlot>,
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
        wal,
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
