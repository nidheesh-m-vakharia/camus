//! Camus broker. The HTTP port carries the metadata Raft group and, under
//! `/wal/`, the WAL shard. `--kafka-addr` accepts the Kafka protocol.

#![forbid(unsafe_code)]

mod kafka;
mod recover;
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
    /// `host:port` clients should connect to, as Kafka's
    /// `advertised.listeners`. Defaults to the bound address, or 127.0.0.1
    /// when bound to every interface. Needed behind NAT, in containers, and
    /// whenever clients reach the broker by another name.
    #[arg(long)]
    advertised_addr: Option<String>,
    /// PEM certificate for the SASL_SSL listener.
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    /// PEM private key for the SASL_SSL listener.
    #[arg(long)]
    tls_key: Option<PathBuf>,
    /// PEM CA bundle for client certificates. With it the TLS listener
    /// requires a certificate from that CA and, as Kafka's SSL listener, takes
    /// the principal from its subject; SASL is then off unless
    /// `--require-sasl` is set.
    #[arg(long)]
    tls_client_ca: Option<PathBuf>,
    /// Allow a PLAINTEXT Kafka listener. Refused for a non-loopback bind unless
    /// `--allow-non-loopback` is also set.
    #[arg(long, default_value_t = false)]
    dev_plaintext: bool,
    /// Allow the PLAINTEXT listener to bind outside the loopback interface.
    #[arg(long, default_value_t = false)]
    allow_non_loopback: bool,
    /// Require SASL (PLAIN or SCRAM) before any API other than ApiVersions and the SASL pair.
    #[arg(long, default_value_t = false)]
    require_sasl: bool,
}

/// Largest Raft RPC body the HTTP routes accept.
const RAFT_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// `/admin/init` body: the peers, as before shards had ids, or the peers and
/// a shard count (ADR 0014). Without a count the count follows the cores of
/// this process, rounded up so each voter leads the same number of shards.
/// `CAMUS_WAL_SHARDS=voters` keeps one shard per voter. A positive integer
/// in that variable is the count.
#[derive(Deserialize)]
#[serde(untagged)]
enum InitBody {
    Peers(Vec<Peer>),
    Layout {
        peers: Vec<Peer>,
        #[serde(default)]
        shards: Option<u32>,
    },
}

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
    /// The WAL shard to truncate. Shard 0, the only one, when absent.
    #[serde(default)]
    shard: u32,
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

#[derive(Deserialize)]
struct ScanQuery {
    prefix: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    let listener = tokio::net::TcpListener::bind(&args.http_addr).await?;
    let http_bound = listener.local_addr()?;
    if camus_raft::quic::enabled() {
        let identity = quic_identity(&args, http_bound)?;
        let mut quic_addr = http_bound;
        if let Some(port) = camus_raft::quic::port_override()? {
            quic_addr.set_port(port);
        }
        camus_raft::quic::ControlEndpoint::bind(quic_addr, identity)?.install()?;
    }
    let node = Arc::new(MetadataNode::open(args.node_id, &args.data_dir, &args.http_addr).await?);
    let wal_dir = resolve_wal_dir(args.wal_dir.clone(), &args.data_dir);
    std::fs::create_dir_all(&wal_dir)?;
    let wal = Arc::new(WalSlot::new(Shard::open(args.node_id, &wal_dir).await?));
    warn_if_rotating_wal(&wal_dir);
    if let Some(endpoint) = camus_raft::quic::current() {
        let node_q = Arc::clone(&node);
        let wal_q = Arc::clone(&wal);
        tokio::spawn(async move {
            endpoint
                .serve(move |frame| {
                    let node_q = Arc::clone(&node_q);
                    let wal_q = Arc::clone(&wal_q);
                    async move { quic_dispatch(node_q, wal_q, frame).await }
                })
                .await;
        });
    }
    tokio::spawn(open_shards(
        Arc::clone(&node),
        Arc::clone(&wal),
        args.node_id,
        wal_dir.clone(),
    ));
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
        .route("/admin/write", post(write))
        .route("/admin/publish", post(publish))
        .route("/admin/apply", post(apply_command))
        .route("/admin/read", get(read))
        .route("/admin/scan", get(scan))
        .route("/raft-vote", post(vote))
        .route("/raft-append", post(append))
        .route("/raft-snapshot", post(snapshot))
        .with_state(Arc::clone(&node))
        .merge(
            Router::new()
                .route("/metrics", get(prometheus))
                .route("/admin/metrics", get(admin_metrics))
                .route("/admin/init", post(init))
                .route("/admin/truncate-to-sealed", post(truncate_to_sealed))
                .route("/admin/join-wal", post(join_wal))
                .route("/admin/add-wal-voter", post(add_wal_voter))
                .route("/admin/restore-cluster", post(restore_cluster))
                .route("/wal/{shard}/hand-over", post(shard_hand_over))
                .route("/wal/{shard}/end-offset", post(shard_end_offset))
                .with_state(Arc::clone(&admin)),
        )
        .merge(
            Router::new()
                .route("/wal/init", post(wal_init))
                .route("/wal/raft-vote", post(wal_vote))
                .route("/wal/raft-append", post(wal_append))
                .route("/wal/raft-snapshot", post(wal_snapshot))
                .route("/wal/{shard}/init", post(shard_init))
                .route("/wal/{shard}/campaign", post(shard_campaign))
                .route("/wal/{shard}/raft-vote", post(shard_vote))
                .route("/wal/{shard}/raft-append", post(shard_append))
                .route("/wal/{shard}/raft-snapshot", post(shard_snapshot))
                .with_state(Arc::clone(&wal)),
        );

    if let Some(kafka_addr) = &args.kafka_addr {
        start_kafka(node.clone(), wal, args.node_id, kafka_addr, &args).await?;
    }

    let bound = http_bound;
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

fn quic_identity(args: &Args, addr: SocketAddr) -> anyhow::Result<camus_raft::quic::Identity> {
    if let (Some(cert), Some(key), Some(ca)) = (&args.tls_cert, &args.tls_key, &args.tls_client_ca)
    {
        let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert)?);
        let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
        let mut key_reader = std::io::BufReader::new(std::fs::File::open(key)?);
        let key = rustls_pemfile::private_key(&mut key_reader)?
            .ok_or_else(|| anyhow::anyhow!("no private key in {key:?}"))?;
        let mut ca_reader = std::io::BufReader::new(std::fs::File::open(ca)?);
        let trusted = rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;
        return Ok(camus_raft::quic::Identity::new(certs, key, trusted)?);
    }
    if args.dev_plaintext && (addr.ip().is_loopback() || addr.ip().is_unspecified()) {
        return Ok(camus_raft::quic::Identity::loopback_dev()?);
    }
    anyhow::bail!("QUIC off loopback needs --tls-cert, --tls-key, and --tls-client-ca")
}

fn qerr(err: impl std::fmt::Display) -> camus_raft::quic::QuicError {
    std::io::Error::other(err.to_string()).into()
}

async fn quic_dispatch(
    node: Arc<MetadataNode>,
    wal: Arc<WalSlot>,
    frame: camus_raft::quic::Frame,
) -> Result<Vec<u8>, camus_raft::quic::QuicError> {
    use camus_raft::quic::{KIND_HEARTBEAT, KIND_VOTE, ROUTE_METADATA, ROUTE_WAL};
    match (frame.route, frame.kind) {
        (ROUTE_METADATA, KIND_VOTE) => {
            let req: VoteRequest<NodeId> = camus_raft::codec::decode(&frame.body).map_err(qerr)?;
            camus_raft::codec::encode(&node.raft.vote(req).await).map_err(qerr)
        }
        (ROUTE_METADATA, KIND_HEARTBEAT) => {
            let req: AppendEntriesRequest<camus_metadata::TypeConfig> =
                camus_raft::codec::decode(&frame.body).map_err(qerr)?;
            camus_raft::codec::encode(&node.raft.append_entries(req).await).map_err(qerr)
        }
        (ROUTE_WAL, KIND_VOTE) => {
            let shard = wal
                .shard(frame.shard)
                .ok_or_else(|| qerr(format!("no wal shard {}", frame.shard)))?;
            let req: VoteRequest<NodeId> = camus_raft::codec::decode(&frame.body).map_err(qerr)?;
            camus_raft::codec::encode(&shard.raft.vote(req).await).map_err(qerr)
        }
        (ROUTE_WAL, KIND_HEARTBEAT) => {
            let shard = wal
                .shard(frame.shard)
                .ok_or_else(|| qerr(format!("no wal shard {}", frame.shard)))?;
            let req: AppendEntriesRequest<camus_wal::TypeConfig> =
                camus_raft::codec::decode(&frame.body).map_err(qerr)?;
            camus_raft::codec::encode(&shard.raft.append_entries(req).await).map_err(qerr)
        }
        _ => Err(qerr("unknown quic frame")),
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn prometheus(State(admin): State<Arc<Admin>>) -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let node = &admin.node;
    let stats = &camus_compactor::STATS;
    let metrics = node.raft.metrics().borrow().clone();
    let leader = metrics
        .current_leader
        .map(|id| id.to_string())
        .unwrap_or_else(|| "NaN".into());
    let mut text = format!(
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
         camus_raft_current_leader {leader}\n\
         # HELP camus_retention_cuts_total Partitions whose log start retention moved.\n\
         # TYPE camus_retention_cuts_total counter\n\
         camus_retention_cuts_total {}\n\
         # HELP camus_sweeps_total Completed orphan object sweeps.\n\
         # TYPE camus_sweeps_total counter\n\
         camus_sweeps_total {}\n\
         # HELP camus_sweep_objects_deleted_total Objects the orphan sweep deleted.\n\
         # TYPE camus_sweep_objects_deleted_total counter\n\
         camus_sweep_objects_deleted_total {}\n\
         # HELP camus_sweep_bytes_deleted_total Bytes in the objects the orphan sweep deleted.\n\
         # TYPE camus_sweep_bytes_deleted_total counter\n\
         camus_sweep_bytes_deleted_total {}\n\
         # HELP camus_produce_hold_ms_total Milliseconds produce requests were held at the WAL disk limit.\n\
         # TYPE camus_produce_hold_ms_total counter\n\
         camus_produce_hold_ms_total {}\n\
         # HELP camus_meta_snapshot_index Log index of the newest metadata snapshot in object storage.\n\
         # TYPE camus_meta_snapshot_index gauge\n\
         camus_meta_snapshot_index {}\n\
         # HELP camus_meta_snapshot_unix_ms Unix milliseconds of that snapshot, or 0.\n\
         # TYPE camus_meta_snapshot_unix_ms gauge\n\
         camus_meta_snapshot_unix_ms {}\n",
        metrics.current_term,
        if metrics.current_leader == Some(metrics.id) {
            1
        } else {
            0
        },
        stats.retention_cuts.load(Relaxed),
        stats.sweeps.load(Relaxed),
        stats.objects_deleted.load(Relaxed),
        stats.bytes_deleted.load(Relaxed),
        kafka::PRODUCE_HOLD_MS.load(Relaxed),
        kafka::META_SNAPSHOT_INDEX.load(Relaxed),
        kafka::META_SNAPSHOT_AT_MS.load(Relaxed),
    );
    // One series per open shard. The shard count is fixed at init.
    let shards = admin.wal.all();
    let families: [(&str, &str, &str, fn(&camus_wal::Shard, NodeId) -> u64); 3] = [
        (
            "camus_wal_unsealed_bytes",
            "Committed WAL bytes not yet sealed into object storage.",
            "gauge",
            |shard, _| shard.unsealed_bytes(),
        ),
        (
            "camus_wal_leader",
            "1 when this broker leads the WAL shard.",
            "gauge",
            |shard, me| u64::from(shard.leader() == Some(me)),
        ),
        (
            "camus_wal_storage_failed",
            "1 when a WAL write failed and the shard stopped.",
            "gauge",
            |shard, _| u64::from(shard.storage_failed()),
        ),
    ];
    for (name, help, kind, value) in families {
        text.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
        for (id, shard) in &shards {
            let value = value(shard, admin.node_id);
            text.push_str(&format!("{name}{{shard=\"{id}\"}} {value}\n"));
        }
    }
    text
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
        "retention_cuts": camus_compactor::STATS.retention_cuts.load(std::sync::atomic::Ordering::Relaxed),
        "sweeps": camus_compactor::STATS.sweeps.load(std::sync::atomic::Ordering::Relaxed),
        "sweep_objects_deleted": camus_compactor::STATS.objects_deleted.load(std::sync::atomic::Ordering::Relaxed),
        "sweep_bytes_deleted": camus_compactor::STATS.bytes_deleted.load(std::sync::atomic::Ordering::Relaxed),
        "produce_hold_ms": kafka::PRODUCE_HOLD_MS.load(std::sync::atomic::Ordering::Relaxed),
        "meta_snapshot_index": kafka::META_SNAPSHOT_INDEX.load(std::sync::atomic::Ordering::Relaxed),
        "meta_snapshot_unix_ms": kafka::META_SNAPSHOT_AT_MS.load(std::sync::atomic::Ordering::Relaxed),
        "wal_shards": admin
            .wal
            .all()
            .into_iter()
            .map(|(id, shard)| serde_json::json!({
                "id": id,
                "leader": shard.leader(),
                "voters": shard.voters(),
                "unsealed_bytes": shard.unsealed_bytes(),
            }))
            .collect::<Vec<_>>(),
    }))
}

/// Shard count when init omits one. One shard per core of this process,
/// rounded up to a multiple of the voter count so each voter is the preferred
/// leader of the same number of shards, and capped at eight shards per voter.
fn default_shard_count(voters: usize) -> u32 {
    if let Ok(raw) = std::env::var("CAMUS_WAL_SHARDS") {
        if raw == "voters" {
            return u32::try_from(voters).unwrap_or(1).max(1);
        }
        if let Ok(count) = raw.parse::<u32>() {
            if count >= 1 {
                return count;
            }
        }
    }
    shard_count_for(
        voters,
        std::thread::available_parallelism()
            .map(|cores| u32::try_from(cores.get()).unwrap_or(1))
            .unwrap_or(1),
    )
}

/// [`default_shard_count`] without reading the environment.
fn shard_count_for(voters: usize, cores: u32) -> u32 {
    let voters = u32::try_from(voters).unwrap_or(1).max(1);
    let rounded = cores.max(voters).div_ceil(voters) * voters;
    rounded.min(voters.saturating_mul(8)).max(1)
}

/// The local disk tier of the read cache (ADR 0010): at most
/// `CAMUS_READ_DISK_CACHE_BYTES` (4 GiB) in `CAMUS_READ_DISK_CACHE_ENTRIES`
/// (65536) files. A size of 0 turns it off, and a cache that cannot be opened
/// is left off with a warning.
fn open_disk_cache(dir: &std::path::Path) -> Option<camus_storage::DiskCache> {
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    };
    let bytes = env("CAMUS_READ_DISK_CACHE_BYTES").unwrap_or(4 * 1024 * 1024 * 1024);
    let entries = env("CAMUS_READ_DISK_CACHE_ENTRIES").unwrap_or(65_536);
    if bytes == 0 || entries == 0 {
        return None;
    }
    let entries = usize::try_from(entries).unwrap_or(usize::MAX);
    match camus_storage::DiskCache::open(dir, bytes, entries) {
        Ok(cache) => Some(cache),
        Err(err) => {
            tracing::warn!(path = %dir.display(), %err, "read disk cache is off");
            None
        }
    }
}

/// `{data_dir}/wal`, or a directory on local NVMe when that path is a
/// rotating disk and an NVMe filesystem is mounted. An explicit `--wal-dir`
/// is used as given.
fn resolve_wal_dir(explicit: Option<PathBuf>, data_dir: &std::path::Path) -> PathBuf {
    if let Some(dir) = explicit {
        return dir;
    }
    let fallback = data_dir.join("wal");
    #[cfg(target_os = "linux")]
    if device_rotates(data_dir) == Some(true) {
        if let Some(mount) = largest_nvme_mount() {
            let dir = mount.join("camus-wal");
            tracing::info!(
                path = %dir.display(),
                mount = %mount.display(),
                "WAL directory is on local NVMe"
            );
            return dir;
        }
    }
    fallback
}

/// Log when the WAL directory sits on a rotating disk. The produce ack waits
/// for that disk.
fn warn_if_rotating_wal(path: &std::path::Path) {
    if device_rotates(path) == Some(true) {
        tracing::warn!(
            path = %path.display(),
            "WAL directory is on a rotating disk; put it on local NVMe"
        );
    }
}

/// `Some(true)` when `path` (or the nearest existing parent) is on a rotating
/// disk. `None` when the device cannot be read, including off Linux.
fn device_rotates(path: &std::path::Path) -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        let mut cursor = path.to_path_buf();
        let meta = loop {
            if let Ok(meta) = std::fs::metadata(&cursor) {
                break meta;
            }
            if !cursor.pop() {
                return None;
            }
        };
        use std::os::unix::fs::MetadataExt;
        let dev = meta.dev();
        // glibc's encoding of a device id. Partitions expose `queue` on the
        // parent disk, so both paths are tried.
        let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
        let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
        let queue = format!("/sys/dev/block/{major}:{minor}/queue/rotational");
        let parent = format!("/sys/dev/block/{major}:{minor}/../queue/rotational");
        let text = std::fs::read_to_string(&queue).or_else(|_| std::fs::read_to_string(&parent));
        return text.ok().map(|text| text.trim() == "1");
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        None
    }
}

/// Mount point of the largest non-rotating NVMe filesystem, skipping the
/// root and boot filesystems.
#[cfg(target_os = "linux")]
fn largest_nvme_mount() -> Option<PathBuf> {
    let table = std::fs::read_to_string("/proc/self/mounts").ok()?;
    let mut best: Option<(u64, PathBuf)> = None;
    for line in table.lines() {
        let mut fields = line.split_whitespace();
        let Some(spec) = fields.next() else { continue };
        let Some(mount) = fields.next() else { continue };
        let mount = mount.replace("\\040", " ");
        let fstype = fields.next().unwrap_or("");
        if !spec.starts_with("/dev/nvme") {
            continue;
        }
        if matches!(
            fstype,
            "tmpfs" | "devtmpfs" | "overlay" | "squashfs" | "autofs"
        ) {
            continue;
        }
        if mount == "/" || mount.starts_with("/boot") || mount.starts_with("/snap") {
            continue;
        }
        let path = PathBuf::from(&mount);
        if device_rotates(&path) != Some(false) {
            continue;
        }
        let bytes = nvme_disk_bytes(spec);
        if best.as_ref().is_none_or(|(have, _)| bytes > *have) {
            best = Some((bytes, path));
        }
    }
    best.map(|(_, path)| path)
}

/// Whole-disk size in bytes for `/dev/nvme0n1` or `/dev/nvme0n1p2`.
#[cfg(target_os = "linux")]
fn nvme_disk_bytes(spec: &str) -> u64 {
    let name = spec.trim_start_matches("/dev/");
    let disk = name.split('p').next().unwrap_or(name);
    let sectors = std::fs::read_to_string(format!("/sys/block/{disk}/size")).unwrap_or_default();
    sectors
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
        .saturating_mul(512)
}

/// Where shard `shard` keeps its log: shard 0 in `wal_dir`, as before shards
/// had ids, and shard `n` beside it in `<wal_dir>-shards/<n>` (ADR 0014).
fn shard_dir(wal_dir: &std::path::Path, shard: u32) -> PathBuf {
    if shard == 0 {
        return wal_dir.to_path_buf();
    }
    let name = wal_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "wal".into());
    wal_dir
        .with_file_name(format!("{name}-shards"))
        .join(shard.to_string())
}

/// Open every shard in the stored layout that this broker has not opened
/// yet. A shard this broker is not a voter of stays empty.
async fn open_shards(
    node: Arc<MetadataNode>,
    wal: Arc<WalSlot>,
    node_id: NodeId,
    wal_dir: PathBuf,
) {
    loop {
        let count = node
            .read_local(camus_metadata::SHARDS)
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .and_then(|layout| layout["count"].as_u64())
            .unwrap_or(1);
        for shard in 1..u32::try_from(count).unwrap_or(1) {
            if wal.shard(shard).is_some() {
                continue;
            }
            let dir = shard_dir(&wal_dir, shard);
            match Shard::open_shard(node_id, &dir, shard).await {
                Ok(opened) => {
                    wal.replace(shard, opened);
                    tracing::info!(shard, "opened wal shard");
                }
                Err(err) => tracing::warn!(shard, %err, "opening wal shard"),
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Each shard's voters: every WAL voter for a one-shard cluster, as before
/// shards had ids; otherwise `min(3, voters)` of them, starting at a
/// different voter for each shard so each is the first, preferred, voter of
/// some shard (ADR 0014).
fn shard_voters(voters: &[NodeId], shards: u32) -> Vec<Vec<NodeId>> {
    if shards <= 1 {
        return vec![voters.to_vec()];
    }
    let each = voters.len().min(3);
    (0..shards as usize)
        .map(|shard| {
            (0..each)
                .map(|offset| voters[(shard + offset) % voters.len()])
                .collect()
        })
        .collect()
}

/// Create the metadata group from every peer and the WAL shards from the
/// peers marked `wal`. Each `addr` is that broker's HTTP address; both
/// groups' RPC goes there. Each shard is initialized on its first voter.
async fn init(
    State(admin): State<Arc<Admin>>,
    Json(body): Json<InitBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    let (peers, shards) = match body {
        InitBody::Peers(peers) => (peers, None),
        InitBody::Layout { peers, shards } => (peers, shards),
    };
    let mut nodes = BTreeMap::new();
    let mut voters = Vec::new();
    for peer in peers {
        let node = BasicNode { addr: peer.addr };
        if peer.wal {
            voters.push(peer.id);
        }
        nodes.insert(peer.id, node);
    }
    if voters.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "no wal voter".into()));
    }
    voters.sort_unstable();
    let count = shards.unwrap_or_else(|| default_shard_count(voters.len()));
    let layout = shard_voters(&voters, count);
    admin
        .node
        .initialize(nodes.clone())
        .await
        .map_err(|err| (StatusCode::CONFLICT, err.to_string()))?;
    if count > 1 {
        let raw = serde_json::json!({ "count": count, "voters": layout }).to_string();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while let Err(err) = metadata_write(&admin, camus_metadata::SHARDS, &raw).await {
            if std::time::Instant::now() > deadline {
                return Err((StatusCode::SERVICE_UNAVAILABLE, err.to_string()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    // Initialize each shard on every voter, with the same membership, so
    // any majority of them can elect even if one dies before it first
    // leads. A broker opens a new shard once it sees the layout, so retry
    // until it has; a voter that is already initialized answers 409. Then
    // the first voter campaigns, so it usually leads the shard.
    for (shard, members) in layout.iter().enumerate() {
        let map: BTreeMap<NodeId, BasicNode> =
            members.iter().map(|id| (*id, nodes[id].clone())).collect();
        let path = if shard == 0 {
            "wal/init".to_string()
        } else {
            format!("wal/{shard}/init")
        };
        for member in members {
            let addr = &nodes[member].addr;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let sent = admin
                    .http
                    .post(format!("http://{addr}/{path}"))
                    .json(&map)
                    .send()
                    .await;
                match sent {
                    Ok(response)
                        if response.status().is_success()
                            || response.status() == StatusCode::CONFLICT =>
                    {
                        break
                    }
                    Ok(response)
                        if response.status() != StatusCode::NOT_FOUND
                            || std::time::Instant::now() > deadline =>
                    {
                        let status = StatusCode::from_u16(response.status().as_u16())
                            .unwrap_or(StatusCode::BAD_GATEWAY);
                        return Err((status, response.text().await.unwrap_or_default()));
                    }
                    Err(err) if std::time::Instant::now() > deadline => {
                        return Err((StatusCode::BAD_GATEWAY, err.to_string()));
                    }
                    _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                }
            }
        }
        if layout.len() > 1 {
            let first = &nodes[&members[0]].addr;
            let _ = admin
                .http
                .post(format!("http://{first}/wal/{shard}/campaign"))
                .send()
                .await;
        }
    }
    // A voter that granted a vote before its own initialize refuses it and
    // learns the membership from the leader instead. Return only once every
    // voter of every shard has it, so any majority can elect even if the
    // first leader dies at once.
    for (shard, members) in layout.iter().enumerate() {
        for member in members {
            let addr = &nodes[member].addr;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let formed = admin
                    .http
                    .get(format!("http://{addr}/admin/metrics"))
                    .send()
                    .await
                    .ok();
                let formed = match formed {
                    Some(response) => response
                        .json::<serde_json::Value>()
                        .await
                        .ok()
                        .and_then(|metrics| {
                            metrics["wal_shards"].as_array().and_then(|shards| {
                                shards
                                    .iter()
                                    .find(|row| row["id"].as_u64() == Some(shard as u64))
                                    .map(|row| {
                                        row["voters"].as_array().map_or(0, Vec::len)
                                            == members.len()
                                    })
                            })
                        })
                        .unwrap_or(false),
                    None => false,
                };
                if formed {
                    break;
                }
                if std::time::Instant::now() > deadline {
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("wal shard {shard} has no membership on broker {member}"),
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    Ok(StatusCode::OK)
}

async fn wal_init(
    State(wal): State<Arc<WalSlot>>,
    Json(nodes): Json<BTreeMap<NodeId, BasicNode>>,
) -> Result<StatusCode, (StatusCode, String)> {
    initialize_shard(&wal, 0, nodes).await
}

async fn shard_init(
    State(wal): State<Arc<WalSlot>>,
    axum::extract::Path(shard): axum::extract::Path<u32>,
    Json(nodes): Json<BTreeMap<NodeId, BasicNode>>,
) -> Result<StatusCode, (StatusCode, String)> {
    initialize_shard(&wal, shard, nodes).await
}

/// Make this broker campaign for a shard. Only the shard's leader asks,
/// after checking this broker has every entry (ADR 0014).
async fn shard_campaign(
    State(wal): State<Arc<WalSlot>>,
    axum::extract::Path(shard): axum::extract::Path<u32>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(opened) = wal.shard(shard) else {
        return Err((StatusCode::NOT_FOUND, format!("no wal shard {shard}")));
    };
    opened
        .campaign()
        .await
        .map(|()| StatusCode::OK)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

#[derive(Deserialize)]
struct HandOver {
    to: NodeId,
}

/// On a shard's leader: hand the shard to voter `to` when it has caught up,
/// by asking it to campaign. `409` when this broker does not lead the shard
/// or `to` is behind.
async fn shard_hand_over(
    State(admin): State<Arc<Admin>>,
    axum::extract::Path(shard): axum::extract::Path<u32>,
    Json(body): Json<HandOver>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(opened) = admin.wal.shard(shard) else {
        return Err((StatusCode::NOT_FOUND, format!("no wal shard {shard}")));
    };
    if body.to == admin.node_id {
        return Ok(StatusCode::OK);
    }
    // Replication metrics lag the moment leadership is won. Give the target
    // a moment to match the log before refusing.
    let caught_up = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if !opened.is_leader() {
            return Err((StatusCode::CONFLICT, "not the shard leader".into()));
        }
        if opened.follower_caught_up(body.to) {
            break;
        }
        if tokio::time::Instant::now() >= caught_up {
            return Err((StatusCode::CONFLICT, format!("voter {} is behind", body.to)));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let addr = opened
        .voter_addr(body.to)
        .ok_or_else(|| (StatusCode::CONFLICT, format!("{} is not a voter", body.to)))?;
    // openraft 0.9 has no leadership transfer, and followers that heard from
    // a leader within its lease refuse votes, so one campaign can end in an
    // election someone else wins. Campaign again, past the lease, until `to`
    // leads.
    for _ in 0..6 {
        if opened.leader() == Some(body.to) {
            return Ok(StatusCode::OK);
        }
        let _ = admin
            .http
            .post(format!("http://{addr}/wal/{shard}/campaign"))
            .send()
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    if opened.leader() == Some(body.to) {
        Ok(StatusCode::OK)
    } else {
        Err((
            StatusCode::GATEWAY_TIMEOUT,
            format!("voter {} did not take shard {shard}", body.to),
        ))
    }
}

#[derive(Deserialize)]
struct EndOffset {
    topic: String,
    partition: i32,
}

/// On a shard's leader: the next offset of one partition, which `EndTxn`
/// uses as the exclusive end of that partition's span (ADR 0015).
async fn shard_end_offset(
    State(admin): State<Arc<Admin>>,
    axum::extract::Path(shard): axum::extract::Path<u32>,
    Json(body): Json<EndOffset>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let Some(opened) = admin.wal.shard(shard) else {
        return Err((StatusCode::NOT_FOUND, format!("no wal shard {shard}")));
    };
    if !opened.is_leader() {
        return Err((StatusCode::CONFLICT, "not the shard leader".into()));
    }
    let Some(raw) = admin
        .node
        .read_local(&camus_metadata::topic_storage_key(&body.topic))
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
    else {
        return Err((StatusCode::NOT_FOUND, format!("no topic {}", body.topic)));
    };
    let topic = camus_metadata::parse_topic(&raw)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;
    let id = uuid::Uuid::parse_str(&topic.id)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    let end = opened.end_offset(id.as_bytes(), body.partition);
    Ok(Json(serde_json::json!({ "end": end })))
}

async fn initialize_shard(
    wal: &WalSlot,
    shard: u32,
    nodes: BTreeMap<NodeId, BasicNode>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(opened) = wal.shard(shard) else {
        return Err((StatusCode::NOT_FOUND, format!("no wal shard {shard} yet")));
    };
    opened
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
    let Some(old) = admin.wal.shard(body.shard) else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("node {me} has no wal shard {}", body.shard),
        ));
    };
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
    // The fresh shard starts at term 1. Raise the epoch base past every epoch
    // the old shard used first, so clients see the truncation as a new epoch
    // and OffsetForLeaderEpoch can tell them where their history ended.
    let base: i64 = admin
        .node
        .read_local(&kafka::epoch_base_key(body.shard))
        .ok()
        .flatten()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(0);
    let raised = base.saturating_add(old.term() as i64);
    metadata_write(
        &admin,
        &kafka::epoch_base_key(body.shard),
        &raised.to_string(),
    )
    .await
    .map_err(|err| (StatusCode::SERVICE_UNAVAILABLE, err.to_string()))?;
    let _ = old.raft.shutdown().await;
    let dir = shard_dir(&admin.wal_dir, body.shard);
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
    let fresh = Shard::open_shard(me, &dir, body.shard)
        .await
        .map_err(internal)?;
    fresh
        .initialize(BTreeMap::from([(me, BasicNode { addr })]))
        .await
        .map_err(internal)?;
    admin.wal.replace(body.shard, fresh);
    tracing::warn!(node_id = me, aside = %aside.display(), "wal shard truncated to sealed");
    Ok(Json(serde_json::json!({
        "voters": [me],
        "moved_aside": aside.display().to_string(),
    })))
}

#[derive(Deserialize)]
struct JoinWal {
    /// The old shard directory is moved aside only when this is set. A fresh
    /// node waits to be added; it does not initialize a membership of its own.
    #[serde(default)]
    replace: bool,
    #[serde(default)]
    shard: u32,
}

/// Drop this broker's copy of a shard and open an empty one that can be
/// added to a membership. Used after `truncate-to-sealed`, when this broker
/// still holds the old membership.
async fn join_wal(
    State(admin): State<Arc<Admin>>,
    Json(body): Json<JoinWal>,
) -> Result<StatusCode, (StatusCode, String)> {
    if !body.replace {
        return Err((
            StatusCode::BAD_REQUEST,
            "join-wal replaces this broker's shard; pass replace".into(),
        ));
    }
    let _held = admin.truncating.lock().await;
    let Some(old) = admin.wal.shard(body.shard) else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("no wal shard {}", body.shard),
        ));
    };
    let internal = |err: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    let _ = old.raft.shutdown().await;
    let dir = shard_dir(&admin.wal_dir, body.shard);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default();
    let name = dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "wal".into());
    let aside = dir.with_file_name(format!("{name}.joined-{stamp}"));
    std::fs::rename(&dir, &aside).map_err(|err| internal(err.into()))?;
    let fresh = Shard::open_shard(admin.node_id, &dir, body.shard)
        .await
        .map_err(internal)?;
    admin.wal.replace(body.shard, fresh);
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct AddWalVoter {
    id: NodeId,
    /// `host:port` of the broker being added, as stored in a membership.
    addr: String,
    #[serde(default)]
    shard: u32,
}

/// On the shard leader: wipe `id`'s old shard, replicate this shard's log to
/// it, and make it a voter.
async fn add_wal_voter(
    State(admin): State<Arc<Admin>>,
    Json(body): Json<AddWalVoter>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if body.id == admin.node_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "a broker does not add itself".into(),
        ));
    }
    let Some(opened) = admin.wal.shard(body.shard) else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("no wal shard {}", body.shard),
        ));
    };
    if !opened.is_leader() {
        return Err((StatusCode::CONFLICT, "not the shard leader".into()));
    }
    let join = admin
        .http
        .post(format!("http://{}/admin/join-wal", body.addr))
        .json(&serde_json::json!({ "replace": true, "shard": body.shard }))
        .send()
        .await
        .map_err(|err| (StatusCode::BAD_GATEWAY, err.to_string()))?;
    if !join.status().is_success() {
        let status =
            StatusCode::from_u16(join.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        return Err((status, join.text().await.unwrap_or_default()));
    }
    // The fresh shard is listening before join returns, and the first
    // append can still arrive before it is ready to take a learner.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut last = String::new();
    loop {
        if opened.voters().contains(&body.id) {
            break;
        }
        match opened
            .add_voter(
                body.id,
                BasicNode {
                    addr: body.addr.clone(),
                },
            )
            .await
        {
            Ok(()) => break,
            Err(err) if std::time::Instant::now() < deadline => {
                last = err.to_string();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(err) => return Err((StatusCode::BAD_GATEWAY, err.to_string())),
        }
    }
    if !opened.voters().contains(&body.id) {
        return Err((StatusCode::BAD_GATEWAY, last));
    }
    Ok(Json(serde_json::json!({ "voters": opened.voters() })))
}

/// Write one metadata key here when this node leads the group, otherwise
/// through the leader's `/admin/write`.
async fn metadata_write(admin: &Admin, key: &str, value: &str) -> anyhow::Result<()> {
    let metrics = admin.node.raft.metrics().borrow().clone();
    let leader = metrics
        .current_leader
        .ok_or_else(|| anyhow::anyhow!("no metadata leader"))?;
    if leader == metrics.id {
        admin.node.write(key, value).await?;
        return Ok(());
    }
    let addr = metrics
        .membership_config
        .membership()
        .get_node(&leader)
        .map(|node| node.addr.clone())
        .ok_or_else(|| anyhow::anyhow!("metadata leader has no address"))?;
    let response = admin
        .http
        .post(format!("http://{addr}/admin/write"))
        .json(&serde_json::json!({ "key": key, "value": value }))
        .send()
        .await?;
    anyhow::ensure!(response.status().is_success(), "metadata write failed");
    Ok(())
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

/// Every locally applied key under `prefix`, as `[[key, value], ...]`.
async fn scan(
    State(node): State<Arc<MetadataNode>>,
    Query(query): Query<ScanQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = node
        .scan_local(&query.prefix)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(Json(serde_json::json!(rows)))
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

fn raft_http_blocked() -> bool {
    matches!(
        std::env::var("CAMUS_BLOCK_RAFT_HTTP").as_deref(),
        Ok("1") | Ok("on") | Ok("true")
    )
}

async fn vote(
    State(node): State<Arc<MetadataNode>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    if raft_http_blocked() {
        return axum::response::IntoResponse::into_response((
            StatusCode::SERVICE_UNAVAILABLE,
            "raft http blocked",
        ));
    }
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
    if raft_http_blocked() && req.entries.is_empty() {
        return axum::response::IntoResponse::into_response((
            StatusCode::SERVICE_UNAVAILABLE,
            "raft http blocked",
        ));
    }
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

/// Shard `id`, or a 404 when this broker does not have it open.
fn open_shard(wal: &WalSlot, id: u32) -> Result<Arc<Shard>, axum::response::Response> {
    use axum::response::IntoResponse;
    wal.shard(id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("no wal shard {id}")).into_response())
}

async fn vote_on(wal: &WalSlot, id: u32, body: &axum::body::Bytes) -> axum::response::Response {
    if raft_http_blocked() {
        return axum::response::IntoResponse::into_response((
            StatusCode::SERVICE_UNAVAILABLE,
            "raft http blocked",
        ));
    }
    let shard = match open_shard(wal, id) {
        Ok(shard) => shard,
        Err(reply) => return reply,
    };
    let req: VoteRequest<NodeId> = match rpc_body(body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&shard.raft.vote(req).await)
}

async fn append_on(wal: &WalSlot, id: u32, body: &axum::body::Bytes) -> axum::response::Response {
    let shard = match open_shard(wal, id) {
        Ok(shard) => shard,
        Err(reply) => return reply,
    };
    let req: AppendEntriesRequest<camus_wal::TypeConfig> = match rpc_body(body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    if raft_http_blocked() && req.entries.is_empty() {
        return axum::response::IntoResponse::into_response((
            StatusCode::SERVICE_UNAVAILABLE,
            "raft http blocked",
        ));
    }
    let carries = !req.entries.is_empty();
    let started = std::time::Instant::now();
    let result = shard.raft.append_entries(req).await;
    if carries {
        camus_wal::RPC_TIMES.handled(started.elapsed());
    }
    rpc_reply(&result)
}

async fn snapshot_on(wal: &WalSlot, id: u32, body: &axum::body::Bytes) -> axum::response::Response {
    let shard = match open_shard(wal, id) {
        Ok(shard) => shard,
        Err(reply) => return reply,
    };
    let req: InstallSnapshotRequest<camus_wal::TypeConfig> = match rpc_body(body) {
        Ok(req) => req,
        Err(reply) => return reply,
    };
    rpc_reply(&shard.raft.install_snapshot(req).await)
}

/// Shard 0's routes from before shards had ids. They stay so brokers of
/// both versions can replicate during a rolling upgrade (ADR 0014).
async fn wal_vote(
    State(wal): State<Arc<WalSlot>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    vote_on(&wal, 0, &body).await
}

async fn wal_append(
    State(wal): State<Arc<WalSlot>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    append_on(&wal, 0, &body).await
}

async fn wal_snapshot(
    State(wal): State<Arc<WalSlot>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    snapshot_on(&wal, 0, &body).await
}

async fn shard_vote(
    State(wal): State<Arc<WalSlot>>,
    axum::extract::Path(id): axum::extract::Path<u32>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    vote_on(&wal, id, &body).await
}

async fn shard_append(
    State(wal): State<Arc<WalSlot>>,
    axum::extract::Path(id): axum::extract::Path<u32>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    append_on(&wal, id, &body).await
}

async fn shard_snapshot(
    State(wal): State<Arc<WalSlot>>,
    axum::extract::Path(id): axum::extract::Path<u32>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    snapshot_on(&wal, id, &body).await
}

async fn start_kafka(
    node: Arc<MetadataNode>,
    wal: Arc<WalSlot>,
    node_id: NodeId,
    kafka_addr: &str,
    args: &Args,
) -> anyhow::Result<()> {
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(load_tls(cert, key, args.tls_client_ca.as_deref())?),
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
    let (host, port) = match &args.advertised_addr {
        Some(advertised) => {
            let (host, port) = advertised
                .rsplit_once(':')
                .ok_or_else(|| anyhow::anyhow!("--advertised-addr must be host:port"))?;
            (host.to_string(), port.parse()?)
        }
        None => advertised_kafka(bind, bound.port()),
    };
    // The MinIO fallbacks are for development. A broker serving TLS must be
    // told where its log lives instead of silently using a local stand-in.
    if !args.dev_plaintext {
        let missing: Vec<&str> = [
            "CAMUS_S3_ENDPOINT",
            "CAMUS_S3_BUCKET",
            "CAMUS_S3_ACCESS_KEY",
            "CAMUS_S3_SECRET_KEY",
        ]
        .into_iter()
        .filter(|name| std::env::var(name).map_or(true, |value| value.is_empty()))
        .collect();
        if !missing.is_empty() {
            anyhow::bail!(
                "object storage is not configured; set {} (the MinIO defaults apply only with --dev-plaintext)",
                missing.join(", ")
            );
        }
    }
    let bucket = open_bucket()?;
    let user = std::env::var("CAMUS_SASL_USER").unwrap_or_default();
    let password = std::env::var("CAMUS_SASL_PASSWORD").unwrap_or_default();
    // TLS alone means SASL_SSL. With a client CA it is Kafka's SSL listener,
    // where the certificate is the identity.
    let require_sasl = args.require_sasl || (tls.is_some() && args.tls_client_ca.is_none());
    if require_sasl && (user.is_empty() || password.is_empty()) {
        anyhow::bail!("SASL requires CAMUS_SASL_USER and CAMUS_SASL_PASSWORD");
    }
    let kafka = kafka::Kafka::new(
        node,
        bucket,
        i32::try_from(node_id)?,
        host,
        port,
        require_sasl,
        user,
        password,
        tls,
        wal,
    );
    let kafka = Arc::new(match open_disk_cache(&args.data_dir.join("read-cache")) {
        Some(cache) => kafka.with_disk_cache(cache),
        None => kafka,
    });
    info!(%bound, sasl = require_sasl, "kafka listener");
    tokio::spawn(async move { kafka.run(listener).await });
    Ok(())
}

fn open_bucket() -> anyhow::Result<Bucket> {
    Bucket::connect(&BucketConfig {
        endpoint: std::env::var("CAMUS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9100".into()),
        bucket: std::env::var("CAMUS_S3_BUCKET").unwrap_or_else(|_| "camus".into()),
        region: std::env::var("CAMUS_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key: std::env::var("CAMUS_S3_ACCESS_KEY").unwrap_or_else(|_| "camus".into()),
        secret_key: std::env::var("CAMUS_S3_SECRET_KEY")
            .unwrap_or_else(|_| "camus-dev-secret".into()),
        prefix: std::env::var("CAMUS_S3_PREFIX").unwrap_or_default(),
        virtual_hosted: env_flag("CAMUS_S3_VIRTUAL_HOST"),
    })
}

async fn restore_cluster(
    State(admin): State<Arc<Admin>>,
    Json(body): Json<Truncate>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !body.i_accept_data_loss {
        return Err((
            StatusCode::BAD_REQUEST,
            "restore-cluster drops the unsealed WAL and metadata newer than the snapshot. \
             Pass i_accept_data_loss."
                .into(),
        ));
    }
    let bucket =
        open_bucket().map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    match recover::restore(&admin.node, &bucket).await {
        Ok(body) => Ok(Json(body)),
        Err(err) => {
            let text = err.to_string();
            let status = if text.contains("does not lead") || text.contains("already has topics") {
                StatusCode::CONFLICT
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            Err((status, text))
        }
    }
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref().map(str::trim),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

/// Host and port clients should dial. `CAMUS_ADVERTISED_HOST` wins. On
/// Railway, the public TCP proxy is used when that pair is unset.
fn advertised_kafka(bind: SocketAddr, bound_port: u16) -> (String, i32) {
    let host = std::env::var("CAMUS_ADVERTISED_HOST")
        .ok()
        .or_else(|| std::env::var("RAILWAY_TCP_PROXY_DOMAIN").ok());
    let port = std::env::var("CAMUS_ADVERTISED_PORT")
        .ok()
        .or_else(|| std::env::var("RAILWAY_TCP_PROXY_PORT").ok());
    advertised_endpoint(bind, bound_port, host.as_deref(), port.as_deref())
}

fn advertised_endpoint(
    bind: SocketAddr,
    bound_port: u16,
    host: Option<&str>,
    port: Option<&str>,
) -> (String, i32) {
    let host = host.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    });
    let port = port
        .and_then(|value| value.trim().parse().ok())
        .filter(|port: &i32| *port > 0);
    if let Some(host) = host {
        return (host.to_string(), port.unwrap_or(i32::from(bound_port)));
    }
    let host = if bind.ip().is_unspecified() {
        "127.0.0.1".to_string()
    } else {
        bind.ip().to_string()
    };
    (host, i32::from(bound_port))
}

fn load_tls(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    client_ca: Option<&std::path::Path>,
) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_file = std::fs::File::open(cert_path)?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader).collect::<Result<_, _>>()?;
    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow::anyhow!("no private key in {key_path:?}"))?;
    let builder = rustls::ServerConfig::builder();
    let config = match client_ca {
        None => builder.with_no_client_auth().with_single_cert(certs, key)?,
        Some(path) => {
            let mut roots = rustls::RootCertStore::empty();
            let mut reader = std::io::BufReader::new(std::fs::File::open(path)?);
            for cert in rustls_pemfile::certs(&mut reader) {
                roots.add(cert?)?;
            }
            anyhow::ensure!(!roots.is_empty(), "no CA certificate in {path:?}");
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
                    .build()?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        }
    };
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::{advertised_endpoint, shard_count_for};

    #[test]
    fn shard_count_follows_cores_and_shares_them_across_voters() {
        assert_eq!(shard_count_for(3, 8), 9);
        assert_eq!(shard_count_for(3, 1), 3);
        assert_eq!(shard_count_for(1, 10), 8);
        assert_eq!(shard_count_for(3, 100), 24);
    }

    #[test]
    fn unspecified_bind_advertises_loopback() {
        let bind: SocketAddr = "0.0.0.0:9092".parse().unwrap();
        assert_eq!(
            advertised_endpoint(bind, 9092, None, None),
            ("127.0.0.1".into(), 9092)
        );
    }

    #[test]
    fn railway_proxy_replaces_the_bind_address() {
        let bind: SocketAddr = "0.0.0.0:9092".parse().unwrap();
        assert_eq!(
            advertised_endpoint(bind, 9092, Some("roundhouse.proxy.rlwy.net"), Some("15140")),
            ("roundhouse.proxy.rlwy.net".into(), 15140)
        );
    }
}
