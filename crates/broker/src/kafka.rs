//! Kafka TCP listener.
//!
//! `r2` produce buffers batches in memory. A flush writes one object, then
//! commits the extent in the metadata group. The produce response is sent
//! only after that commit. `wal` produce waits for the shard Raft commit,
//! which fsyncs, and the upload to object storage happens after the ack.
//! `acks=0` does not wait and gets no response.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_metadata::{
    sequence_key, Command, MetadataNode, NodeId, ProducerNote, PublishPart, Topic, TxnSpan,
};
use camus_protocol::batch::{self, BatchError};
use camus_protocol::error::ResponseError;
use camus_protocol::frame::{self, encode_response};
use camus_protocol::idempotence::{self, Class, ProducerSeq};
use camus_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use camus_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use camus_protocol::messages::delete_topics_response::{
    DeletableTopicResult, DeleteTopicsResponse,
};
use camus_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use camus_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use camus_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use camus_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use camus_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use camus_protocol::messages::{
    ApiKey, ApiVersionsRequest, BrokerId, CreateTopicsRequest, DeleteTopicsRequest,
    DescribeConfigsRequest, FetchRequest, FetchResponse, InitProducerIdRequest,
    InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    ProduceRequest, ProduceResponse, RequestHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use camus_protocol::protocol::{decode_request_header_from_buffer, Decodable, StrBytes};
use camus_storage::Bucket;
use camus_uploader::{self, Piece};
use camus_wal::{Append, SealDrop, Shard, WriteError};

use crate::slot::WalSlot;

mod acl;
mod admin;
mod groups;
mod txn;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

const MAX_FRAME: usize = 100 * 1024 * 1024;
const MAX_BATCH: usize = batch::MAX_BATCH_CEILING;
const FLUSH_BYTES: usize = 8 * 1024 * 1024;
/// Most unsealed bytes one upload pass takes: up to four objects of
/// `FLUSH_BYTES`, put in parallel, then one publish and one seal for all.
const UPLOAD_PASS_BYTES: usize = 4 * FLUSH_BYTES;
const BUFFER_LIMIT: usize = 256 * 1024 * 1024;
const WAL_DISK_LIMIT: u64 = 8 * 1024 * 1024 * 1024;

const CORRUPT: i16 = 2;
const UNKNOWN_TOPIC: i16 = 3;
const LEADER_NOT_AVAILABLE: i16 = 5;
const NOT_LEADER: i16 = 6;
const REQUEST_TIMED_OUT: i16 = 7;
const TOO_LARGE: i16 = 10;
const INVALID_RECORD: i16 = 87;
const INVALID_PARTITIONS: i16 = 37;
const INVALID_TOPIC: i16 = 17;
const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
const INVALID_REPLICATION_FACTOR: i16 = 38;
const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// `DescribeConfigs` sources: set on the topic, set on the broker, default.
const CONFIG_SOURCE_TOPIC: i8 = 1;
const CONFIG_SOURCE_BROKER: i8 = 4;
const CONFIG_SOURCE_DEFAULT: i8 = 5;
const SASL_MECHANISM: i16 = 33;
const ILLEGAL_SASL: i16 = 34;
const INVALID_CONFIG: i16 = 40;
const INVALID_REQUEST: i16 = 42;
const TXN: i16 = 48;
const STORAGE: i16 = 56;
const SASL_AUTH_FAILED: i16 = 58;

struct Item {
    topic: String,
    partition: i32,
    raw: Bytes,
    records: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    ack: Option<oneshot::Sender<Result<i64, i16>>>,
    /// Set when every batch in `raw` belongs to this transaction.
    txn_id: Option<String>,
    producer_id: i64,
    producer_epoch: i16,
}

enum Reply {
    Frame(Bytes),
    Silent,
    Close,
}

enum Socket {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Socket {
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(socket) => socket.read_exact(buf).await.map(|_| ()),
            Self::Tls(socket) => socket.read_exact(buf).await.map(|_| ()),
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(socket) => socket.write_all(buf).await,
            Self::Tls(socket) => socket.write_all(buf).await,
        }
    }
}

/// Metadata key for shard 0's leader and voters. Phase 2 runs one shard.
const SHARD_KEY: &str = "wal/0";

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
struct ShardView {
    leader: Option<NodeId>,
    voters: Vec<NodeId>,
}

/// One broker's Kafka endpoint.
pub struct Kafka {
    node: Arc<MetadataNode>,
    bucket: Bucket,
    node_id: i32,
    host: String,
    port: i32,
    flush_ms: u64,
    /// How often to compact sealed extents of `cleanup.policy=compact` topics.
    /// `0` turns compaction off. Kafka's cleaner is on by default, and so is
    /// this.
    compact_ms: u64,
    /// `wal.disk.limit`: unsealed bytes past which produce is held.
    wal_limit: u64,
    /// Kafka's `log.retention.check.interval.ms`: how often the metadata
    /// leader applies `retention.ms` and `retention.bytes`.
    retention_check_ms: u64,
    /// How often the metadata leader looks for orphaned objects.
    sweep_ms: u64,
    /// An object younger than this is never swept, so an upload waiting
    /// for its metadata commit survives (ADR 0007).
    sweep_min_age_ms: i64,
    /// Kafka's `file.delete.delay.ms`: how long an object stays after it is
    /// first seen unreferenced.
    sweep_delay_ms: i64,
    sweeper: Mutex<camus_compactor::sweep::Sweeper>,
    /// Kafka's `transaction.max.timeout.ms`.
    txn_max_timeout_ms: i64,
    /// Kafka's `transaction.abort.timed.out.transaction.cleanup.interval.ms`.
    txn_abort_interval_ms: u64,
    /// Kafka's `transactional.id.expiration.ms`.
    txn_id_expiration_ms: i64,
    /// Kafka's `transaction.remove.expired.transaction.cleanup.interval.ms`.
    txn_cleanup_interval_ms: u64,
    /// Kafka's `producer.id.expiration.ms`.
    producer_id_expiration_ms: i64,
    /// Kafka's `producer.id.expiration.check.interval.ms`.
    producer_id_check_ms: u64,
    /// Kafka's `offsets.retention.minutes`, in milliseconds.
    offsets_retention_ms: i64,
    /// Kafka's `offsets.retention.check.interval.ms`.
    offsets_check_ms: u64,
    /// `durability.mode` for a topic created without one. `wal` per ADR 0005.
    default_durability: String,
    /// Kafka's `auto.create.topics.enable`.
    auto_create_topics: bool,
    /// Kafka's `num.partitions`, for auto-created topics and CreateTopics
    /// with `-1`.
    default_partitions: i32,
    /// `message.max.bytes`: the batch limit for a topic without
    /// `max.message.bytes`. Kafka's default, 1048588.
    message_max_bytes: usize,
    require_sasl: bool,
    /// Kafka's `super.users`. The SASL user from the environment is always one,
    /// so a new cluster has someone who may create the first ACLs.
    super_users: std::collections::BTreeSet<String>,
    /// Kafka's `allow.everyone.if.no.acl.found`.
    allow_if_no_acl: bool,
    sasl_user: String,
    sasl_pass: String,
    tls: Option<TlsAcceptor>,
    wal: Arc<WalSlot>,
    http: reqwest::Client,
    /// Classic consumer groups, coordinated while this broker leads metadata.
    coordinator: Arc<camus_coordinator::Coordinator>,
    /// The metadata term the coordinator's groups were loaded in.
    coordinator_term: Mutex<Option<u64>>,
    /// Topic ids seen in the WAL with no topic in metadata, and since when.
    unknown: std::sync::Mutex<BTreeMap<[u8; 16], Instant>>,
    buf: Mutex<Vec<Item>>,
    flush_lock: Mutex<()>,
}

impl Kafka {
    /// `host` and `port` are what clients should dial, not necessarily the bind address.
    pub fn new(
        node: Arc<MetadataNode>,
        bucket: Bucket,
        node_id: i32,
        host: String,
        port: i32,
        require_sasl: bool,
        sasl_user: String,
        sasl_pass: String,
        tls: Option<TlsAcceptor>,
        wal: Arc<WalSlot>,
    ) -> Self {
        let wal_limit = std::env::var("CAMUS_WAL_DISK_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(WAL_DISK_LIMIT);
        let flush_ms = std::env::var("CAMUS_FLUSH_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(250);
        let env_ms = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(default)
        };
        let retention_check_ms = env_ms("CAMUS_RETENTION_CHECK_MS", 300_000);
        let sweep_ms = env_ms("CAMUS_SWEEP_MS", 300_000);
        let sweep_min_age_ms = env_ms("CAMUS_SWEEP_MIN_AGE_MS", 3_600_000) as i64;
        let sweep_delay_ms = env_ms("CAMUS_FILE_DELETE_DELAY_MS", 60_000) as i64;
        // Kafka's `super.users` is a semicolon-separated principal list.
        let mut super_users: std::collections::BTreeSet<String> =
            std::env::var("CAMUS_SUPER_USERS")
                .unwrap_or_default()
                .split(';')
                .map(str::trim)
                .filter(|principal| !principal.is_empty())
                .map(str::to_string)
                .collect();
        if require_sasl && !sasl_user.is_empty() {
            super_users.insert(format!("User:{sasl_user}"));
        }
        // Without SASL every client is `User:ANONYMOUS`, as on a Kafka
        // listener with no authorizer, so a resource no ACL names stays open.
        let allow_if_no_acl = match std::env::var("CAMUS_ACL_ALLOW_IF_NO_ACL").ok().as_deref() {
            Some("true") => true,
            Some("false") => false,
            _ => !require_sasl,
        };
        let txn_max_timeout_ms = env_ms("CAMUS_TXN_MAX_TIMEOUT_MS", 900_000) as i64;
        let txn_abort_interval_ms = env_ms("CAMUS_TXN_ABORT_INTERVAL_MS", 10_000);
        let txn_id_expiration_ms =
            env_ms("CAMUS_TRANSACTIONAL_ID_EXPIRATION_MS", 604_800_000) as i64;
        let txn_cleanup_interval_ms = env_ms("CAMUS_TXN_CLEANUP_INTERVAL_MS", 3_600_000);
        let producer_id_expiration_ms =
            env_ms("CAMUS_PRODUCER_ID_EXPIRATION_MS", 86_400_000) as i64;
        let producer_id_check_ms = env_ms("CAMUS_PRODUCER_ID_CHECK_INTERVAL_MS", 600_000);
        // Kafka's setting is in minutes; `CAMUS_OFFSETS_RETENTION_MS`
        // overrides it for tests that cannot wait a minute.
        let offsets_retention_ms = std::env::var("CAMUS_OFFSETS_RETENTION_MS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or_else(|| {
                env_ms("CAMUS_OFFSETS_RETENTION_MINUTES", 10_080).saturating_mul(60_000) as i64
            });
        let offsets_check_ms = env_ms("CAMUS_OFFSETS_RETENTION_CHECK_INTERVAL_MS", 600_000);
        let auto_create_topics = std::env::var("CAMUS_AUTO_CREATE_TOPICS")
            .map(|value| value != "false")
            .unwrap_or(true);
        let default_partitions = std::env::var("CAMUS_NUM_PARTITIONS")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|count| *count > 0)
            .unwrap_or(1);
        let message_max_bytes = std::env::var("CAMUS_MESSAGE_MAX_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(camus_metadata::config::DEFAULT_MESSAGE_MAX_BYTES as usize)
            .min(batch::MAX_BATCH_CEILING);
        let compact_ms = std::env::var("CAMUS_COMPACT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300_000);
        let store = Arc::new(groups::MetadataStore {
            node: Arc::clone(&node),
        });
        Self {
            node,
            bucket,
            node_id,
            host,
            port,
            flush_ms,
            compact_ms,
            auto_create_topics,
            default_partitions,
            message_max_bytes,
            retention_check_ms,
            sweep_ms,
            sweep_min_age_ms,
            sweep_delay_ms,
            sweeper: Mutex::new(camus_compactor::sweep::Sweeper::default()),
            txn_max_timeout_ms,
            txn_abort_interval_ms,
            txn_id_expiration_ms,
            txn_cleanup_interval_ms,
            producer_id_expiration_ms,
            producer_id_check_ms,
            offsets_retention_ms,
            offsets_check_ms,
            wal_limit,
            default_durability: std::env::var("CAMUS_DEFAULT_DURABILITY")
                .unwrap_or_else(|_| "wal".into()),
            require_sasl,
            super_users,
            allow_if_no_acl,
            sasl_user,
            sasl_pass,
            tls,
            wal,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("http client"),
            unknown: std::sync::Mutex::new(BTreeMap::new()),
            coordinator: camus_coordinator::Coordinator::new(store, groups::coordinator_config()),
            coordinator_term: Mutex::new(None),
            buf: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
        }
    }

    fn leader(&self) -> bool {
        self.node.raft.metrics().borrow().current_leader == Some(self.node_id as u64)
    }

    /// The shard serving requests now.
    fn wal(&self) -> Arc<Shard> {
        self.wal.get()
    }

    fn gone_long_enough(&self, id: [u8; 16]) -> bool {
        let mut unknown = self.unknown.lock().expect("unknown topics");
        let since = *unknown.entry(id).or_insert_with(Instant::now);
        since.elapsed() >= Duration::from_secs(30)
    }

    /// HTTP address of the metadata leader, when another broker leads.
    fn leader_http(&self) -> Option<String> {
        let metrics = self.node.raft.metrics().borrow().clone();
        let leader = metrics.current_leader?;
        if leader == self.node_id as u64 {
            return None;
        }
        metrics
            .membership_config
            .membership()
            .get_node(&leader)
            .map(|node| node.addr.clone())
    }

    /// Commit a put here when this broker leads metadata, else on the leader.
    async fn meta_write(&self, key: String, value: String) -> anyhow::Result<()> {
        if self.leader() {
            self.node.write(key, value).await?;
            return Ok(());
        }
        let addr = self
            .leader_http()
            .ok_or_else(|| anyhow::anyhow!("no metadata leader"))?;
        let response = self
            .http
            .post(format!("http://{addr}/admin/write"))
            .json(&serde_json::json!({ "key": key, "value": value }))
            .send()
            .await?;
        anyhow::ensure!(response.status().is_success(), "forwarded write failed");
        Ok(())
    }

    /// Commit extents here when this broker leads metadata, else on the leader.
    async fn meta_publish(
        &self,
        parts: Vec<PublishPart>,
        producers: Vec<ProducerNote>,
    ) -> anyhow::Result<camus_metadata::Response> {
        if self.leader() {
            return self.node.publish(parts, producers).await;
        }
        let addr = self
            .leader_http()
            .ok_or_else(|| anyhow::anyhow!("no metadata leader"))?;
        let response = self
            .http
            .post(format!("http://{addr}/admin/publish"))
            .json(&serde_json::json!({ "parts": parts, "producers": producers }))
            .send()
            .await?;
        anyhow::ensure!(response.status().is_success(), "forwarded publish failed");
        Ok(response.json().await?)
    }

    /// Commit a metadata command here, or on the metadata leader.
    async fn meta_apply(
        &self,
        command: camus_metadata::Command,
    ) -> anyhow::Result<camus_metadata::Response> {
        if self.leader() {
            return self.node.apply(command).await;
        }
        let addr = self
            .leader_http()
            .ok_or_else(|| anyhow::anyhow!("no metadata leader"))?;
        let response = self
            .http
            .post(format!("http://{addr}/admin/apply"))
            .json(&command)
            .send()
            .await?;
        anyhow::ensure!(response.status().is_success(), "forwarded apply failed");
        Ok(response.json().await?)
    }

    /// Shard leader and voters as clients should see them. The shard leader
    /// records itself in metadata so brokers outside the shard can answer
    /// Metadata; until it has, this broker's own view is used.
    fn shard_view(&self) -> ShardView {
        self.node
            .read_local(SHARD_KEY)
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_else(|| self.local_shard_view())
    }

    fn local_shard_view(&self) -> ShardView {
        let wal = self.wal();
        ShardView {
            leader: wal.leader(),
            voters: wal.voters(),
        }
    }

    /// Record this broker as shard leader in metadata when it leads and the
    /// stored view differs.
    async fn announce_shard(&self) {
        if !self.wal().leads_quorum(Duration::from_millis(500)) {
            return;
        }
        let mine = self.local_shard_view();
        let stored: Option<ShardView> = self
            .node
            .read_local(SHARD_KEY)
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str(&raw).ok());
        if stored.as_ref() == Some(&mine) {
            return;
        }
        if let Ok(raw) = serde_json::to_string(&mine) {
            let _ = self.meta_write(SHARD_KEY.to_string(), raw).await;
        }
    }

    fn topic(&self, name: &str) -> Option<Topic> {
        let raw = self
            .node
            .read_local(&camus_metadata::topic_storage_key(name))
            .ok()??;
        camus_metadata::parse_topic(&raw).ok()
    }

    fn partition_state(&self, topic: &Topic, partition: i32) -> Option<camus_metadata::Partition> {
        let raw = self
            .node
            .read_local(&camus_metadata::partition_storage_key(&topic.id, partition))
            .ok()??;
        camus_metadata::parse_partition(&raw).ok()
    }

    /// `Some` when every batch is one ongoing transaction on this partition.
    fn accept_txn(
        &self,
        txn_id: Option<&str>,
        info: &[camus_protocol::batch::BatchInfo],
        identities: &[camus_protocol::batch::ProducerBatch],
        topic: &str,
        partition: i32,
    ) -> Result<Option<(String, i64, i16)>, i16> {
        let any = info.iter().any(|batch| batch.transactional);
        let all = !info.is_empty() && info.iter().all(|batch| batch.transactional);
        let Some(id) = txn_id else {
            return if any { Err(TXN) } else { Ok(None) };
        };
        if !all {
            return Err(INVALID_REQUEST);
        }
        let Some(first) = identities.first() else {
            return Err(CORRUPT);
        };
        if identities.iter().any(|batch| {
            batch.producer_id != first.producer_id || batch.producer_epoch != first.producer_epoch
        }) {
            return Err(INVALID_REQUEST);
        }
        let get = |key: &str| self.node.read_local(key).map_err(|err| err.to_string());
        camus_metadata::txn_allows(
            &get,
            id,
            first.producer_id,
            first.producer_epoch,
            topic,
            partition,
        )?;
        Ok(Some((
            id.to_string(),
            first.producer_id,
            first.producer_epoch,
        )))
    }

    /// The partition's high watermark: the sealed end, or the WAL end for a
    /// `wal` topic when that is further.
    fn high_watermark(&self, topic: &Topic, partition: i32) -> Option<i64> {
        let sealed = self.partition_state(topic, partition)?.next_offset;
        if topic.durability == "wal" {
            Some(sealed.max(self.wal().end_offset(&topic_id(topic), partition)))
        } else {
            Some(sealed)
        }
    }

    /// The largest batch `topic` accepts: its `max.message.bytes`, or the
    /// broker's `message.max.bytes`.
    fn batch_limit(&self, topic: &Topic) -> usize {
        if topic.is_set("max.message.bytes") {
            usize::try_from(topic.config_i64("max.message.bytes")).unwrap_or(0)
        } else {
            self.message_max_bytes
        }
    }

    /// A compacting topic rejects a record with no key, with `INVALID_RECORD`
    /// as in Kafka.
    fn reject_null_key(&self, topic: &Topic, raw: &[u8]) -> Result<(), i16> {
        if !camus_protocol::compact::compacts(&topic.cleanup_policy) {
            return Ok(());
        }
        let mut bytes = bytes::Bytes::copy_from_slice(raw);
        let sets = camus_protocol::records::RecordBatchDecoder::decode_all(&mut bytes)
            .map_err(|_| CORRUPT)?;
        if sets
            .iter()
            .flat_map(|set| &set.records)
            .any(|record| record.key.is_none())
        {
            return Err(INVALID_RECORD);
        }
        Ok(())
    }

    /// Move each partition's log start past the sealed extents that
    /// `retention.ms` or `retention.bytes` no longer keeps.
    async fn enforce_retention(&self) {
        if !self.leader() {
            return;
        }
        let now = txn::now_ms();
        let mut cuts = Vec::new();
        for name in self.topic_names() {
            let Some(topic) = self.topic(&name) else {
                continue;
            };
            if !topic
                .cleanup_policy
                .split(',')
                .any(|policy| policy == "delete")
            {
                continue;
            }
            for partition in 0..topic.partitions {
                let Some(state) = self.partition_state(&topic, partition) else {
                    continue;
                };
                if let Some(offset) = camus_compactor::retention::cut(
                    &state.extents,
                    state.log_start,
                    topic.retention_ms,
                    topic.retention_bytes,
                    now,
                ) {
                    cuts.push(camus_metadata::RecordCut {
                        topic: name.clone(),
                        partition,
                        offset,
                        high_watermark: -1,
                    });
                }
            }
        }
        if cuts.is_empty() {
            return;
        }
        let count = cuts.len() as u64;
        match self.meta_apply(Command::DeleteRecords { cuts }).await {
            Ok(_) => {
                camus_compactor::STATS
                    .retention_cuts
                    .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
                tracing::info!(partitions = count, "retention moved log starts");
            }
            Err(err) => tracing::warn!(%err, "retention commit failed"),
        }
    }

    /// Abort transactions that have been ongoing longer than their timeout.
    async fn abort_timed_out_transactions(&self) {
        if !self.leader() {
            return;
        }
        let Ok(rows) = self.node.scan_local("txn/") else {
            return;
        };
        let now = txn::now_ms();
        for (key, raw) in rows {
            let Ok(clock) = camus_metadata::txn_clock(&raw) else {
                continue;
            };
            if !clock.ongoing
                || clock.started_ms < 0
                || now.saturating_sub(clock.started_ms) <= i64::from(clock.timeout_ms)
            {
                continue;
            }
            let id = key.trim_start_matches("txn/").to_string();
            let applied = self
                .meta_apply(Command::AbortTimedOutTxn {
                    transactional_id: id.clone(),
                    producer_id: clock.producer_id,
                    producer_epoch: clock.epoch,
                    now_ms: now,
                })
                .await;
            if let Ok(response) = applied {
                if response.value.as_deref() == Some("1") {
                    tracing::info!(transactional_id = %id, "aborted a transaction past its timeout");
                }
            }
        }
    }

    /// Remove transactional ids idle for `transactional.id.expiration.ms`.
    async fn expire_transactional_ids(&self) {
        if !self.leader() {
            return;
        }
        let Ok(rows) = self.node.scan_local("txn/") else {
            return;
        };
        let now = txn::now_ms();
        for (key, raw) in rows {
            let Ok(clock) = camus_metadata::txn_clock(&raw) else {
                continue;
            };
            let idle = clock.updated_ms < 0
                || now.saturating_sub(clock.updated_ms) >= self.txn_id_expiration_ms;
            if clock.ongoing || !idle {
                continue;
            }
            let _ = self
                .meta_apply(Command::ExpireTransactionalId {
                    transactional_id: key.trim_start_matches("txn/").to_string(),
                    producer_id: clock.producer_id,
                    producer_epoch: clock.epoch,
                    now_ms: now,
                    expiration_ms: self.txn_id_expiration_ms,
                })
                .await;
        }
    }

    /// Remove committed offsets of empty groups past `offsets.retention.minutes`.
    async fn expire_offsets(&self) {
        if !self.leader() {
            return;
        }
        let Ok(rows) = self.node.scan_local(camus_metadata::groups::group_prefix()) else {
            return;
        };
        let now = txn::now_ms();
        for (key, _) in rows {
            let Some(group) = camus_metadata::groups::group_from_key(&key) else {
                continue;
            };
            let applied = self
                .node
                .group_write(Command::ExpireOffsets {
                    group: group.clone(),
                    now_ms: now,
                    retention_ms: self.offsets_retention_ms,
                })
                .await;
            let gone = applied.is_ok()
                && self
                    .node
                    .read_local(&key)
                    .is_ok_and(|stored| stored.is_none());
            if gone {
                self.coordinator.forget(&group);
            }
        }
    }

    /// Remove idempotent producer state Kafka would expire. Metadata decides
    /// per key; this only names the candidates.
    async fn expire_producer_state(&self) {
        if !self.leader() {
            return;
        }
        let Ok(rows) = self.node.scan_local("seq/") else {
            return;
        };
        let keys: Vec<(String, i32, i64)> = rows
            .iter()
            .filter_map(|(key, _)| {
                let rest = key.strip_prefix("seq/")?;
                let (rest, producer) = rest.rsplit_once('/')?;
                let (topic, partition) = rest.rsplit_once('/')?;
                Some((
                    topic.to_string(),
                    partition.parse().ok()?,
                    producer.parse().ok()?,
                ))
            })
            .collect();
        let now = txn::now_ms();
        for chunk in keys.chunks(500) {
            let _ = self
                .meta_apply(Command::ExpireProducers {
                    keys: chunk.to_vec(),
                    now_ms: now,
                    expiration_ms: self.producer_id_expiration_ms,
                })
                .await;
        }
    }

    /// Every object key an extent points into. Any read or parse failure is
    /// an error, because a missing reference would delete live data.
    fn referenced_objects(&self) -> anyhow::Result<std::collections::BTreeSet<String>> {
        let mut keys = std::collections::BTreeSet::new();
        let names: Vec<String> = match self.node.read_local(camus_metadata::topic_index_key())? {
            Some(raw) => serde_json::from_str(&raw)?,
            None => Vec::new(),
        };
        for name in names {
            let Some(raw) = self
                .node
                .read_local(&camus_metadata::topic_storage_key(&name))?
            else {
                continue;
            };
            let topic = camus_metadata::parse_topic(&raw).map_err(anyhow::Error::msg)?;
            for partition in 0..topic.partitions {
                let raw = self
                    .node
                    .read_local(&camus_metadata::partition_storage_key(&topic.id, partition))?
                    .ok_or_else(|| anyhow::anyhow!("{name}-{partition} has no partition row"))?;
                let state = camus_metadata::parse_partition(&raw).map_err(anyhow::Error::msg)?;
                keys.extend(state.extents.into_iter().map(|extent| extent.key));
            }
        }
        Ok(keys)
    }

    /// Delete bundle objects that no extent has referenced for the delete
    /// delay. Runs on the metadata leader only.
    async fn sweep_objects(&self) -> anyhow::Result<()> {
        if !self.leader() {
            return Ok(());
        }
        // List before reading references: an object published in between is
        // then either referenced or too young to delete.
        let listed: Vec<camus_compactor::sweep::Listed> = self
            .bucket
            .list("bundle/")
            .await?
            .into_iter()
            .map(|(key, size, modified_ms)| camus_compactor::sweep::Listed {
                key,
                size,
                modified_ms,
            })
            .collect();
        let referenced = self.referenced_objects()?;
        let plan = self.sweeper.lock().await.plan(
            &listed,
            &referenced,
            txn::now_ms(),
            self.sweep_min_age_ms,
            self.sweep_delay_ms,
        );
        let stats = &camus_compactor::STATS;
        for object in plan {
            if !self.leader() {
                break;
            }
            match self.bucket.delete(&object.key).await {
                Ok(()) => {
                    stats
                        .objects_deleted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    stats
                        .bytes_deleted
                        .fetch_add(object.size, std::sync::atomic::Ordering::Relaxed);
                }
                Err(err) => tracing::warn!(key = %object.key, %err, "orphan delete failed"),
            }
        }
        stats
            .sweeps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Rewrite sealed extents so each key keeps its newest record.
    async fn compact_sealed(&self) {
        if !self.leader() {
            return;
        }
        for name in self.topic_names() {
            let Some(topic) = self.topic(&name) else {
                continue;
            };
            if !camus_protocol::compact::compacts(&topic.cleanup_policy) {
                continue;
            }
            for partition in 0..topic.partitions {
                self.compact_partition(&topic, partition).await;
            }
        }
    }

    async fn compact_partition(&self, topic: &Topic, partition: i32) {
        let Some(state) = self.partition_state(topic, partition) else {
            return;
        };
        // `min.compaction.lag.ms`: only the leading extents whose newest
        // record is at least that old are compacted.
        let now = txn::now_ms();
        let lag = topic.config_i64("min.compaction.lag.ms");
        let eligible: Vec<&camus_metadata::Extent> = state
            .extents
            .iter()
            .take_while(|extent| now.saturating_sub(extent.max_timestamp) >= lag)
            .collect();
        let Some(last) = eligible.last() else {
            return;
        };
        let covered_end = last.end;
        let tombstones_before = now.saturating_sub(topic.config_i64("delete.retention.ms"));
        let mut records = Vec::new();
        let mut base_timestamp = i64::MAX;
        let mut max_timestamp = i64::MIN;
        for extent in eligible {
            if extent.length == 0 {
                continue;
            }
            let end = extent.file_offset + u64::from(extent.length);
            let Ok(bytes) = self
                .bucket
                .get_range(&extent.key, extent.file_offset..end)
                .await
            else {
                return;
            };
            let mut cursor = bytes::Bytes::from(bytes);
            let Ok(sets) = camus_protocol::records::RecordBatchDecoder::decode_all(&mut cursor)
            else {
                return;
            };
            for set in sets {
                for record in set.records {
                    let covers = |spans: &[camus_metadata::Span]| {
                        spans
                            .iter()
                            .any(|span| span.start <= record.offset && record.offset < span.end)
                    };
                    if record.transactional
                        && !covers(&state.txn_committed)
                        && !covers(&state.txn_aborted)
                    {
                        return;
                    }
                    if covers(&state.txn_aborted) {
                        continue;
                    }
                    base_timestamp = base_timestamp.min(record.timestamp);
                    max_timestamp = max_timestamp.max(record.timestamp);
                    records.push(record);
                }
            }
        }
        let Some(kept) = camus_protocol::compact::plan_expiring(&records, tombstones_before) else {
            return;
        };
        // When nothing survives, the prefix becomes an extent with no
        // object and no bytes. The log start stays where it is, as Kafka's
        // cleaner leaves it, and a fetch there moves on to the next record.
        let (object_key, length) = if kept.is_empty() {
            (String::new(), 0)
        } else {
            let Ok(body) = camus_protocol::compact::rebuild(&kept) else {
                return;
            };
            let Ok(length) = u32::try_from(body.len()) else {
                return;
            };
            let object_key = camus_uploader::object_key();
            if self.bucket.put_new(&object_key, body).await.is_err() {
                return;
            }
            (object_key, length)
        };
        if base_timestamp == i64::MAX {
            base_timestamp = 0;
            max_timestamp = 0;
        }
        let _ = self
            .node
            .apply(Command::Compact {
                topic: topic.name.clone(),
                partition,
                covered_end,
                object_key,
                file_offset: 0,
                length,
                base_timestamp,
                max_timestamp,
            })
            .await;
    }

    /// Accept connections until the listener closes.
    pub async fn run(self: Arc<Self>, listener: TcpListener) {
        let flush = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(flush.flush_ms)).await;
                flush.flush().await;
            }
        });
        if self.compact_ms > 0 {
            let compact = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(compact.compact_ms)).await;
                    compact.compact_sealed().await;
                }
            });
        }
        if self.retention_check_ms > 0 {
            let retention = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(retention.retention_check_ms)).await;
                    retention.enforce_retention().await;
                }
            });
        }
        if self.txn_abort_interval_ms > 0 {
            let txns = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(txns.txn_abort_interval_ms)).await;
                    txns.abort_timed_out_transactions().await;
                }
            });
        }
        if self.txn_cleanup_interval_ms > 0 {
            let txns = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(txns.txn_cleanup_interval_ms)).await;
                    txns.expire_transactional_ids().await;
                }
            });
        }
        if self.offsets_check_ms > 0 {
            let offsets = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(offsets.offsets_check_ms)).await;
                    offsets.expire_offsets().await;
                }
            });
        }
        if self.producer_id_check_ms > 0 {
            let producers = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(producers.producer_id_check_ms)).await;
                    producers.expire_producer_state().await;
                }
            });
        }
        if self.sweep_ms > 0 {
            let sweep = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(sweep.sweep_ms)).await;
                    if let Err(err) = sweep.sweep_objects().await {
                        tracing::warn!(%err, "object sweep skipped");
                    }
                }
            });
        }
        let upload = Arc::clone(&self);
        tokio::spawn(async move {
            let mut failures = 0u32;
            let mut backlog = false;
            loop {
                // A pass that filled its object leaves more behind; start the
                // next one at once so a backlog drains at R2's pace.
                let delay = if failures > 0 {
                    retry_delay(failures)
                } else if backlog {
                    Duration::ZERO
                } else {
                    Duration::from_millis(upload.flush_ms)
                };
                tokio::time::sleep(delay).await;
                match upload.upload_wal().await {
                    Upload::Failed => {
                        failures = failures.saturating_add(1);
                        backlog = false;
                        tracing::warn!(failures, "wal upload failed; retrying");
                    }
                    pass => {
                        failures = 0;
                        backlog = pass == Upload::Full;
                    }
                }
            }
        });
        tokio::spawn(groups::tick(
            Arc::clone(&self),
            Arc::clone(&self.coordinator),
        ));
        let register = Arc::clone(&self);
        tokio::spawn(async move {
            let key = format!("broker/{}", register.node_id);
            let addr = format!("{}:{}", register.host, register.port);
            loop {
                let stored = register.node.read_local(&key).ok().flatten();
                if stored.as_deref() != Some(addr.as_str()) {
                    let _ = register.meta_write(key.clone(), addr.clone()).await;
                }
                register.announce_shard().await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        loop {
            let Ok((tcp, peer)) = listener.accept().await else {
                break;
            };
            let _ = tcp.set_nodelay(true);
            let kafka = Arc::clone(&self);
            tokio::spawn(async move {
                let socket = if let Some(acceptor) = &kafka.tls {
                    match acceptor.accept(tcp).await {
                        Ok(stream) => Socket::Tls(stream),
                        Err(err) => {
                            tracing::debug!(%err, "tls handshake failed");
                            return;
                        }
                    }
                } else {
                    Socket::Plain(tcp)
                };
                let _ = kafka.connection(socket, peer.ip().to_string()).await;
            });
        }
    }

    async fn connection(&self, mut socket: Socket, host: String) -> anyhow::Result<()> {
        let mut session = Session {
            authed: !self.require_sasl,
            host,
        };
        loop {
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_err() {
                break;
            }
            let size = i32::from_be_bytes(size_buf);
            if size <= 0 || size as usize > MAX_FRAME {
                break;
            }
            let mut body = vec![0u8; size as usize];
            if socket.read_exact(&mut body).await.is_err() {
                break;
            }
            match self.handle(&body, &mut session).await {
                Reply::Frame(frame) => {
                    if socket.write_all(&frame).await.is_err() {
                        break;
                    }
                }
                Reply::Silent => {}
                Reply::Close => break,
            }
        }
        Ok(())
    }

    async fn handle(&self, body: &[u8], session: &mut Session) -> Reply {
        let mut buf = Bytes::copy_from_slice(body);
        let header = match decode_request_header_from_buffer(&mut buf) {
            Ok(header) => header,
            Err(_) => return Reply::Close,
        };
        let Ok(key) = ApiKey::try_from(header.request_api_key) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        tracing::debug!(
            api = header.request_api_key,
            version,
            correlation = header.correlation_id,
            "kafka request"
        );
        let allowed_before_auth = matches!(
            key,
            ApiKey::ApiVersions | ApiKey::SaslHandshake | ApiKey::SaslAuthenticate
        );
        if self.require_sasl && !session.authed && !allowed_before_auth {
            return self.illegal_sasl(key, version, header.correlation_id);
        }
        if !frame::is_implemented(key, version) {
            return unsupported(key, version, header.correlation_id);
        }
        let caller = Caller {
            principal: self.principal(session.authed),
            host: session.host.clone(),
        };
        match key {
            ApiKey::ApiVersions => self.api_versions(&header, &mut buf),
            ApiKey::Metadata => self.metadata(&header, &mut buf, &caller).await,
            ApiKey::CreateTopics => self.create_topics(&header, &mut buf, &caller).await,
            ApiKey::DeleteTopics => self.delete_topics(&header, &mut buf, &caller).await,
            ApiKey::DescribeConfigs => self.describe_configs(&header, &mut buf, &caller),
            ApiKey::Produce => self.produce(&header, &mut buf, &caller).await,
            ApiKey::Fetch => self.fetch(&header, &mut buf, &caller).await,
            ApiKey::ListOffsets => self.list_offsets(&header, &mut buf, &caller).await,
            ApiKey::SaslHandshake => self.sasl_handshake(&header, &mut buf),
            ApiKey::SaslAuthenticate => {
                self.sasl_authenticate(&header, &mut buf, &mut session.authed)
            }
            ApiKey::FindCoordinator => self.find_coordinator(&header, &mut buf, &caller).await,
            ApiKey::JoinGroup => self.join_group(&header, &mut buf, &caller).await,
            ApiKey::SyncGroup => self.sync_group(&header, &mut buf, &caller).await,
            ApiKey::Heartbeat => self.heartbeat(&header, &mut buf, &caller).await,
            ApiKey::LeaveGroup => self.leave_group(&header, &mut buf, &caller).await,
            ApiKey::OffsetCommit => self.offset_commit(&header, &mut buf, &caller).await,
            ApiKey::OffsetFetch => self.offset_fetch(&header, &mut buf, &caller).await,
            ApiKey::ListGroups => self.list_groups(&header, &mut buf, &caller).await,
            ApiKey::DescribeGroups => self.describe_groups(&header, &mut buf, &caller).await,
            ApiKey::DeleteGroups => self.delete_groups(&header, &mut buf, &caller).await,
            ApiKey::InitProducerId => self.init_producer_id(&header, &mut buf, &caller).await,
            ApiKey::CreatePartitions => self.create_partitions(&header, &mut buf, &caller).await,
            ApiKey::AlterConfigs => self.alter_configs(&header, &mut buf, &caller).await,
            ApiKey::IncrementalAlterConfigs => {
                self.incremental_alter_configs(&header, &mut buf, &caller)
                    .await
            }
            ApiKey::DeleteRecords => self.delete_records(&header, &mut buf, &caller).await,
            ApiKey::OffsetDelete => self.offset_delete(&header, &mut buf, &caller).await,
            ApiKey::DescribeCluster => self.describe_cluster(&header, &mut buf, &caller),
            ApiKey::AddPartitionsToTxn => {
                self.add_partitions_to_txn(&header, &mut buf, &caller).await
            }
            ApiKey::AddOffsetsToTxn => self.add_offsets_to_txn(&header, &mut buf, &caller).await,
            ApiKey::TxnOffsetCommit => self.txn_offset_commit(&header, &mut buf, &caller).await,
            ApiKey::EndTxn => self.end_txn(&header, &mut buf, &caller).await,
            ApiKey::DescribeTransactions => {
                self.describe_transactions(&header, &mut buf, &caller).await
            }
            ApiKey::ListTransactions => self.list_transactions(&header, &mut buf, &caller).await,
            ApiKey::DescribeAcls => self.describe_acls(&header, &mut buf, &caller).await,
            ApiKey::CreateAcls => self.create_acls(&header, &mut buf, &caller).await,
            ApiKey::DeleteAcls => self.delete_acls(&header, &mut buf, &caller).await,
            ApiKey::ConsumerGroupHeartbeat => {
                self.consumer_group_heartbeat(&header, &mut buf, &caller)
                    .await
            }
            _ => unsupported(key, version, header.correlation_id),
        }
    }

    fn illegal_sasl(&self, key: ApiKey, version: i16, correlation: i32) -> Reply {
        if key == ApiKey::Produce && frame::is_implemented(key, version) {
            let mut response = ProduceResponse::default();
            let mut topic_response = TopicProduceResponse::default();
            topic_response.name = topic_name("");
            topic_response.partition_responses = vec![partition_error(0, ILLEGAL_SASL)];
            response.responses.push(topic_response);
            return frame_of(version, correlation, &response);
        }
        error_frame(key, version, correlation, ILLEGAL_SASL)
    }

    fn api_versions(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        if ApiVersionsRequest::decode(buf, header.request_api_version).is_err() {
            return Reply::Close;
        }
        let mut response = ApiVersionsResponse::default();
        response.api_keys = frame::implemented_apis()
            .iter()
            .map(|api| {
                let mut version = ApiVersion::default();
                version.api_key = api.key as i16;
                version.min_version = api.min;
                version.max_version = api.max;
                version
            })
            .collect();
        response.finalized_features_epoch = -1;
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn metadata(&self, header: &RequestHeader, buf: &mut Bytes, caller: &Caller) -> Reply {
        let Ok(request) = MetadataRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        // Kafka leaves out topics the caller may not describe when it lists
        // every topic, and reports them as unauthorized when they are named.
        let listing = request.topics.is_none();
        let auto_create = self.auto_create_topics
            && (header.request_api_version < 4 || request.allow_auto_topic_creation);
        let names = match request.topics {
            None => self.topic_names(),
            Some(topics) => topics
                .into_iter()
                .filter_map(|topic| {
                    let named = topic
                        .name
                        .as_ref()
                        .map(|name| name.0.to_string())
                        .filter(|name| !name.is_empty());
                    named.or_else(|| self.topic_name_by_id(&topic.topic_id))
                })
                .collect(),
        };
        let mut response = MetadataResponse::default();
        response.cluster_id = Some(StrBytes::from_static_str("camus"));
        let leader = self
            .node
            .raft
            .metrics()
            .borrow()
            .current_leader
            .map(|id| id as i32);
        response.controller_id = BrokerId(leader.unwrap_or(-1));
        response.brokers = self.broker_list();
        for name in names {
            if !listing && auto_create && self.topic(&name).is_none() {
                response.topics.push(self.auto_create(&name, caller).await);
                continue;
            }
            if self.allows(caller, auth::TOPIC, &name, auth::DESCRIBE) {
                response.topics.push(self.metadata_topic(&name, leader));
            } else if !listing {
                let mut denied = MetadataResponseTopic::default();
                denied.name = Some(topic_name(&name));
                denied.error_code = auth::TOPIC_FAILED;
                response.topics.push(denied);
            }
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    /// Create a topic a Metadata request named, as Kafka's
    /// `auto.create.topics.enable` does: with `CREATE` on the cluster or the
    /// topic, the broker's partition count, and the default durability. The
    /// first answer is `LEADER_NOT_AVAILABLE`, so the client asks again.
    async fn auto_create(&self, name: &str, caller: &Caller) -> MetadataResponseTopic {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(topic_name(name));
        let allowed = self.allows(caller, auth::CLUSTER, auth::CLUSTER_NAME, auth::CREATE)
            || self.allows(caller, auth::TOPIC, name, auth::CREATE);
        if !allowed {
            topic.error_code = auth::TOPIC_FAILED;
            return topic;
        }
        if self.check_topic_name(name).is_err() {
            topic.error_code = INVALID_TOPIC;
            return topic;
        }
        let created = self
            .meta_apply(Command::CreateTopic {
                name: name.to_string(),
                id: Uuid::new_v4().to_string(),
                partitions: self.default_partitions,
                durability: self.default_durability.clone(),
                configs: Vec::new(),
            })
            .await;
        topic.error_code = match created {
            Ok(response) if response.error_code == 0 || response.error_code == 36 => {
                LEADER_NOT_AVAILABLE
            }
            Ok(response) => response.error_code,
            Err(_) => LEADER_NOT_AVAILABLE,
        };
        topic
    }

    fn topic_names(&self) -> Vec<String> {
        let Ok(Some(raw)) = self.node.read_local(camus_metadata::topic_index_key()) else {
            return Vec::new();
        };
        serde_json::from_str(&raw).unwrap_or_default()
    }

    fn broker_list(&self) -> Vec<MetadataResponseBroker> {
        let ids: Vec<NodeId> = self
            .node
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        ids.into_iter()
            .filter_map(|id| {
                let id = i32::try_from(id).ok()?;
                let raw = self.node.read_local(&format!("broker/{id}")).ok()??;
                let (host, port) = raw.rsplit_once(':')?;
                let port = port.parse().ok()?;
                let mut broker = MetadataResponseBroker::default();
                broker.node_id = BrokerId(id);
                broker.host = StrBytes::from_string(host.to_string());
                broker.port = port;
                Some(broker)
            })
            .collect()
    }

    fn metadata_topic(&self, name: &str, leader: Option<i32>) -> MetadataResponseTopic {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(topic_name(name));
        let Some(stored) = self.topic(name) else {
            topic.error_code = UNKNOWN_TOPIC;
            topic.name = Some(topic_name(name));
            return topic;
        };
        topic.topic_id = Uuid::parse_str(&stored.id).unwrap_or_else(|_| Uuid::nil());
        // A `wal` partition is led by its shard leader (ADR 0006), not by the
        // metadata leader.
        let (leader, replicas) = if stored.durability == "wal" {
            let view = self.shard_view();
            let replicas: Vec<BrokerId> = view
                .voters
                .into_iter()
                .filter_map(|id| i32::try_from(id).ok().map(BrokerId))
                .collect();
            (view.leader.map(|id| id as i32), replicas)
        } else {
            (leader, leader.map(BrokerId).into_iter().collect())
        };
        let leader_id = leader.unwrap_or(-1);
        let error = if leader.is_some() {
            0
        } else {
            LEADER_NOT_AVAILABLE
        };
        for index in 0..stored.partitions {
            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = index;
            partition.error_code = error;
            partition.leader_id = BrokerId(leader_id);
            partition.leader_epoch = stored.leader_epoch;
            if leader.is_some() {
                partition.replica_nodes = replicas.clone();
                partition.isr_nodes = replicas.clone();
            }
            topic.partitions.push(partition);
        }
        topic
    }

    fn principal(&self, authed: bool) -> String {
        if authed && self.require_sasl && !self.sasl_user.is_empty() {
            format!("User:{}", self.sasl_user)
        } else {
            "User:ANONYMOUS".into()
        }
    }

    /// Kafka's authorizer: super users may do anything; otherwise the ACLs
    /// decide, and a resource no ACL names follows
    /// `allow.everyone.if.no.acl.found`.
    fn allows(&self, caller: &Caller, resource_type: i8, name: &str, operation: i8) -> bool {
        if self.super_users.contains(&caller.principal) {
            return true;
        }
        let raw = self.node.read_local("acls").ok().flatten();
        camus_metadata::acl_allows(
            raw.as_deref(),
            &caller.principal,
            &caller.host,
            resource_type,
            name,
            operation,
            self.allow_if_no_acl,
        )
    }

    async fn create_topics(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = CreateTopicsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if !self.leader() {
            return self.create_error(&request, header, NOT_LEADER);
        }
        let cluster_create = self.allows(caller, auth::CLUSTER, auth::CLUSTER_NAME, auth::CREATE);
        let mut response = CreateTopicsResponse::default();
        let version = header.request_api_version;
        for topic in request.topics {
            let name = topic.name.0.to_string();
            let configs: Vec<(String, Option<String>)> = topic
                .configs
                .iter()
                .map(|config| {
                    (
                        config.name.to_string(),
                        config.value.as_ref().map(|value| value.to_string()),
                    )
                })
                .collect();
            let durability = configs
                .iter()
                .find(|(config, _)| config == "durability.mode")
                .map(|(_, value)| value.clone().unwrap_or_default())
                .unwrap_or_else(|| self.default_durability.clone());
            let partitions = if topic.num_partitions == -1 {
                self.default_partitions
            } else {
                topic.num_partitions
            };
            let mut result = CreatableTopicResult::default();
            result.name = topic_name(&name);
            let mut checked =
                if cluster_create || self.allows(caller, auth::TOPIC, &name, auth::CREATE) {
                    self.check_new_topic(&name, &topic, partitions, &configs)
                } else {
                    Err((auth::TOPIC_FAILED, None))
                };
            if checked.is_ok() && !request.validate_only {
                checked = match self
                    .node
                    .create_topic(&name, partitions, durability.clone(), configs.clone())
                    .await
                {
                    Ok(created) if created.error_code == 0 => Ok(()),
                    Ok(created) => Err((created.error_code, None)),
                    Err(_) => Err((NOT_LEADER, None)),
                };
            }
            match checked {
                Ok(()) => {
                    let stored = if request.validate_only {
                        let mut draft = Topic::new(&name, "", partitions, &durability);
                        for (config, value) in &configs {
                            let _ = camus_metadata::config::edit(
                                &mut draft,
                                config,
                                camus_metadata::config::Op::Set,
                                value.as_deref(),
                                true,
                            );
                        }
                        Some(draft)
                    } else {
                        self.topic(&name)
                    };
                    result.num_partitions = partitions;
                    result.replication_factor = self.replication_factor(&durability);
                    if let Some(stored) = stored {
                        result.topic_id =
                            Uuid::parse_str(&stored.id).unwrap_or_else(|_| Uuid::nil());
                        if version >= 5 {
                            result.configs = Some(
                                self.topic_configs(&stored)
                                    .into_iter()
                                    .map(|row| {
                                        let mut out = camus_protocol::messages::create_topics_response::CreatableTopicConfigs::default();
                                        out.name = StrBytes::from_static_str(row.spec.name);
                                        out.value = Some(StrBytes::from_string(row.value));
                                        out.read_only = row.spec.create_only;
                                        out.config_source = row.source;
                                        out
                                    })
                                    .collect(),
                            );
                        }
                    }
                }
                Err((code, message)) => {
                    result.error_code = code;
                    result.error_message = message.map(StrBytes::from_string).or_else(|| {
                        (code == 36).then(|| {
                            StrBytes::from_string(format!("Topic '{name}' already exists."))
                        })
                    });
                    result.num_partitions = -1;
                    result.replication_factor = -1;
                }
            }
            response.topics.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn create_error(
        &self,
        request: &CreateTopicsRequest,
        header: &RequestHeader,
        code: i16,
    ) -> Reply {
        let mut response = CreateTopicsResponse::default();
        for topic in &request.topics {
            let mut result = CreatableTopicResult::default();
            result.name = topic_name(topic.name.0.as_str());
            result.error_code = code;
            response.topics.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    /// Check a CreateTopics entry the way Kafka does, with its message.
    fn check_new_topic(
        &self,
        name: &str,
        topic: &camus_protocol::messages::create_topics_request::CreatableTopic,
        partitions: i32,
        configs: &[(String, Option<String>)],
    ) -> Result<(), (i16, Option<String>)> {
        if self.topic(name).is_some() {
            return Err((36, None));
        }
        self.check_topic_name(name)?;
        if !topic.assignments.is_empty() {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                Some("Camus places replicas itself; replica assignments are not accepted.".into()),
            ));
        }
        if partitions < 1 {
            return Err((
                INVALID_PARTITIONS,
                Some(format!(
                    "Number of partitions must be larger than 0, or -1 for the default; got {}.",
                    topic.num_partitions
                )),
            ));
        }
        let brokers = self.broker_list().len().max(1) as i16;
        if topic.replication_factor != -1
            && (topic.replication_factor < 1 || topic.replication_factor > brokers)
        {
            return Err((
                INVALID_REPLICATION_FACTOR,
                Some(format!(
                    "Replication factor must be -1 or between 1 and the {brokers} available brokers; got {}.",
                    topic.replication_factor
                )),
            ));
        }
        camus_metadata::config::check_create(configs)
            .map_err(|rejected| (rejected.code, Some(rejected.message)))
    }

    /// Kafka's topic name rules: 1 to 249 characters from `[a-zA-Z0-9._-]`,
    /// not `.` or `..`, and no collision with an existing name once `.` and
    /// `_` are treated as the same, since metric names cannot tell them apart.
    fn check_topic_name(&self, name: &str) -> Result<(), (i16, Option<String>)> {
        let invalid = |why: String| Err((INVALID_TOPIC, Some(why)));
        if name.is_empty() || name == "." || name == ".." {
            return invalid(format!("Topic name '{name}' is illegal."));
        }
        if name.len() > 249 {
            return invalid(format!(
                "Topic name '{name}' is longer than 249 characters."
            ));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return invalid(format!(
                "Topic name '{name}' contains a character other than ASCII alphanumerics, '.', '_', and '-'."
            ));
        }
        let folded = name.replace('.', "_");
        if let Some(existing) = self
            .topic_names()
            .into_iter()
            .find(|existing| existing != name && existing.replace('.', "_") == folded)
        {
            return invalid(format!(
                "Topic '{name}' collides with existing topic: {existing}"
            ));
        }
        Ok(())
    }

    /// The replica count Metadata reports for a topic of this durability.
    fn replication_factor(&self, durability: &str) -> i16 {
        if durability == "wal" {
            i16::try_from(self.shard_view().voters.len().max(1)).unwrap_or(i16::MAX)
        } else {
            1
        }
    }

    /// Every config of `topic` with its effective value and Kafka source.
    fn topic_configs(&self, topic: &Topic) -> Vec<ConfigView> {
        camus_metadata::config::SPECS
            .iter()
            .map(|spec| {
                let broker_value = match spec.name {
                    "max.message.bytes" => Some(self.message_max_bytes.to_string()),
                    "durability.mode" => Some(self.default_durability.clone()),
                    _ => None,
                };
                let (value, source) = if topic.is_set(spec.name) {
                    (topic.config(spec.name), CONFIG_SOURCE_TOPIC)
                } else if let Some(value) = broker_value.filter(|value| value != spec.default) {
                    (value, CONFIG_SOURCE_BROKER)
                } else if spec.name == "durability.mode" {
                    (topic.durability.clone(), CONFIG_SOURCE_DEFAULT)
                } else {
                    (spec.default.to_string(), CONFIG_SOURCE_DEFAULT)
                };
                ConfigView {
                    spec,
                    value,
                    source,
                }
            })
            .collect()
    }

    async fn delete_topics(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DeleteTopicsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut names: Vec<String> = request
            .topic_names
            .iter()
            .map(|name| name.0.to_string())
            .collect();
        for topic in &request.topics {
            if let Some(name) = &topic.name {
                names.push(name.0.to_string());
            } else if let Some(name) = self.topic_name_by_id(&topic.topic_id) {
                names.push(name);
            }
        }
        let mut response = DeleteTopicsResponse::default();
        for name in names {
            let code = if !self.allows(caller, auth::TOPIC, &name, auth::DELETE) {
                29
            } else if !self.leader() {
                NOT_LEADER
            } else {
                match self.node.delete_topic(&name).await {
                    Ok(result) => result.error_code,
                    Err(_) => NOT_LEADER,
                }
            };
            let mut result = DeletableTopicResult::default();
            result.name = Some(topic_name(&name));
            result.error_code = code;
            response.responses.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn resolve_topic(&self, name: &str, id: &Uuid, by_id: bool) -> String {
        if by_id || name.is_empty() {
            return self.topic_name_by_id(id).unwrap_or_default();
        }
        name.to_string()
    }

    fn topic_name_by_id(&self, id: &Uuid) -> Option<String> {
        self.node.read_local(&format!("id/{id}")).ok().flatten()
    }

    fn describe_configs(&self, header: &RequestHeader, buf: &mut Bytes, caller: &Caller) -> Reply {
        let Ok(request) = DescribeConfigsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = DescribeConfigsResponse::default();
        for resource in request.resources {
            let mut result = DescribeConfigsResult::default();
            result.resource_type = resource.resource_type;
            result.resource_name = resource.resource_name.clone();
            if resource.resource_type != 2 {
                result.error_code = INVALID_REQUEST;
            } else if !self.allows(
                caller,
                auth::TOPIC,
                resource.resource_name.as_str(),
                auth::DESCRIBE_CONFIGS,
            ) {
                result.error_code = auth::TOPIC_FAILED;
            } else if let Some(topic) = self.topic(resource.resource_name.as_str()) {
                // Null asks for every config. An empty list is treated the
                // same way, since asking for no configs has no use.
                let wanted = resource
                    .configuration_keys
                    .as_ref()
                    .filter(|keys| !keys.is_empty());
                result.configs = self
                    .topic_configs(&topic)
                    .into_iter()
                    .filter(|row| {
                        wanted.is_none_or(|keys| keys.iter().any(|key| key.as_str() == row.spec.name))
                    })
                    .map(|row| {
                        let mut out = DescribeConfigsResourceResult::default();
                        out.name = StrBytes::from_static_str(row.spec.name);
                        out.value = Some(StrBytes::from_string(row.value.clone()));
                        out.read_only = row.spec.create_only;
                        out.config_source = row.source;
                        out.config_type = row.spec.config_type();
                        if request.include_synonyms {
                            let mut synonym = camus_protocol::messages::describe_configs_response::DescribeConfigsSynonym::default();
                            synonym.name = StrBytes::from_static_str(row.spec.name);
                            synonym.value = Some(StrBytes::from_string(row.value));
                            synonym.source = row.source;
                            out.synonyms.push(synonym);
                        }
                        out
                    })
                    .collect();
            } else {
                result.error_code = UNKNOWN_TOPIC;
            }
            response.results.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn init_producer_id(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = InitProducerIdRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = InitProducerIdResponse::default();
        response.producer_epoch = -1;
        if !self.leader() {
            response.error_code = NOT_LEADER;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let transactional = request
            .transactional_id
            .as_ref()
            .filter(|id| !id.0.is_empty())
            .map(|id| id.0.to_string());
        // A transactional id needs WRITE on it. An idempotent producer needs
        // IDEMPOTENT_WRITE on the cluster or WRITE on some topic (KIP-679).
        let authorized = match &transactional {
            Some(id) => self.allows(caller, auth::TXN_ID, id, auth::WRITE),
            None => {
                self.allows(
                    caller,
                    auth::CLUSTER,
                    auth::CLUSTER_NAME,
                    auth::IDEMPOTENT_WRITE,
                ) || self
                    .topic_names()
                    .iter()
                    .any(|name| self.allows(caller, auth::TOPIC, name, auth::WRITE))
            }
        };
        if !authorized {
            response.error_code = if transactional.is_some() {
                auth::TXN_ID_FAILED
            } else {
                auth::CLUSTER_FAILED
            };
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        if transactional.is_some()
            && (request.transaction_timeout_ms <= 0
                || i64::from(request.transaction_timeout_ms) > self.txn_max_timeout_ms)
        {
            response.error_code = INVALID_TRANSACTION_TIMEOUT;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let applied = match &transactional {
            Some(id) => {
                self.node
                    .apply(Command::InitTransactionTimed {
                        transactional_id: id.clone(),
                        producer_id: request.producer_id.0,
                        producer_epoch: request.producer_epoch,
                        timeout_ms: request.transaction_timeout_ms,
                        now_ms: txn::now_ms(),
                    })
                    .await
            }
            None => {
                self.node
                    .init_producer(false, request.producer_id.0, request.producer_epoch)
                    .await
            }
        };
        match applied {
            Ok(result) if result.error_code == 0 => {
                let Some(value) = result.value else {
                    response.error_code = STORAGE;
                    return frame_of(header.request_api_version, header.correlation_id, &response);
                };
                let Some((id, epoch)) = value.split_once(',') else {
                    response.error_code = STORAGE;
                    return frame_of(header.request_api_version, header.correlation_id, &response);
                };
                response.producer_id = id.parse::<i64>().unwrap_or(-1).into();
                response.producer_epoch = epoch.parse::<i16>().unwrap_or(-1);
            }
            Ok(result) => response.error_code = result.error_code,
            Err(_) => response.error_code = NOT_LEADER,
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn produce(&self, header: &RequestHeader, buf: &mut Bytes, caller: &Caller) -> Reply {
        let Ok(request) = ProduceRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if request.acks != 0 && request.acks != 1 && request.acks != -1 {
            return self.produce_all_error(&request, header, INVALID_REQUEST);
        }
        let txn_id = request
            .transactional_id
            .as_ref()
            .filter(|id| !id.0.is_empty())
            .map(|id| id.0.to_string());
        if let Some(id) = &txn_id {
            if !self.allows(caller, auth::TXN_ID, id, auth::WRITE) {
                return self.produce_all_error(&request, header, auth::TXN_ID_FAILED);
            }
        }
        let mut response = ProduceResponse::default();
        let mut waiters = Vec::new();
        let mut held = Duration::ZERO;
        for topic in request.topic_data {
            let name = self.resolve_topic(
                topic.name.0.as_str(),
                &topic.topic_id,
                header.request_api_version >= 13,
            );
            let topic_index = response.responses.len();
            let mut topic_response = TopicProduceResponse::default();
            topic_response.name = topic_name(&name);
            topic_response.topic_id = topic.topic_id;
            let stored = self.topic(&name);
            if !self.allows(caller, auth::TOPIC, &name, auth::WRITE) {
                for partition in topic.partition_data {
                    if request.acks != 0 {
                        topic_response
                            .partition_responses
                            .push(partition_error(partition.index, 29));
                    }
                }
                response.responses.push(topic_response);
                continue;
            }
            for partition in topic.partition_data {
                match self
                    .enqueue_partition(
                        &name,
                        stored.as_ref(),
                        &partition.records,
                        partition.index,
                        request.acks,
                        request.timeout_ms,
                        txn_id.as_deref(),
                        &mut held,
                    )
                    .await
                {
                    Ok(Some(rx)) => {
                        let partition_index = topic_response.partition_responses.len();
                        topic_response
                            .partition_responses
                            .push(partition_error(partition.index, 0));
                        waiters.push((topic_index, partition_index, rx));
                    }
                    Ok(None) => {
                        if request.acks != 0 {
                            topic_response
                                .partition_responses
                                .push(partition_error(partition.index, 0));
                        }
                    }
                    Err(code) => {
                        if request.acks != 0 {
                            topic_response
                                .partition_responses
                                .push(partition_error(partition.index, code));
                        }
                    }
                }
            }
            if request.acks != 0 {
                response.responses.push(topic_response);
            }
        }
        if request.acks == 0 {
            return Reply::Silent;
        }
        for (topic_index, partition_index, rx) in waiters {
            let settled = rx.await.unwrap_or(Err(NOT_LEADER));
            let row = &mut response.responses[topic_index].partition_responses[partition_index];
            match settled {
                Ok(offset) => row.base_offset = offset,
                Err(code) => {
                    row.error_code = code;
                    row.error_message = produce_message(code);
                }
            }
        }
        // The hold already slowed this producer. Reporting it as
        // `throttle_time_ms` too would make a client that honors the throttle
        // wait a second time, so the time goes to a metric instead.
        PRODUCE_HOLD_MS.fetch_add(
            u64::try_from(held.as_millis()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        response.throttle_time_ms = 0;
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn produce_all_error(
        &self,
        request: &ProduceRequest,
        header: &RequestHeader,
        code: i16,
    ) -> Reply {
        let mut response = ProduceResponse::default();
        for topic in &request.topic_data {
            let mut topic_response = TopicProduceResponse::default();
            topic_response.name = topic_name(topic.name.0.as_str());
            topic_response.topic_id = topic.topic_id;
            for partition in &topic.partition_data {
                topic_response
                    .partition_responses
                    .push(partition_error(partition.index, code));
            }
            response.responses.push(topic_response);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn enqueue_partition(
        &self,
        name: &str,
        topic: Option<&Topic>,
        records: &Option<Bytes>,
        partition: i32,
        acks: i16,
        timeout_ms: i32,
        txn_id: Option<&str>,
        held: &mut Duration,
    ) -> Result<Option<oneshot::Receiver<Result<i64, i16>>>, i16> {
        let Some(topic) = topic else {
            return Err(UNKNOWN_TOPIC);
        };
        if topic.durability == "wal" {
            return self
                .write_wal(
                    name, topic, records, partition, acks, timeout_ms, txn_id, held,
                )
                .await;
        }
        if topic.durability != "r2" {
            return Err(INVALID_CONFIG);
        }
        if !self.leader() {
            return Err(NOT_LEADER);
        }
        if partition < 0 || partition >= topic.partitions {
            return Err(UNKNOWN_TOPIC);
        }
        let Some(raw) = records else {
            return Ok(None);
        };
        if raw.is_empty() {
            return Ok(None);
        }
        let info = match batch::inspect(raw, self.batch_limit(topic)) {
            Ok(info) => info,
            Err(BatchError::TooLarge) => return Err(TOO_LARGE),
            Err(BatchError::Corrupt) => return Err(CORRUPT),
            Err(BatchError::Transactional) => return Err(TXN),
        };
        let identities = batch::producer_batches(raw, MAX_BATCH).map_err(|_| CORRUPT)?;
        self.reject_null_key(topic, raw)?;
        let txn = self.accept_txn(txn_id, &info, &identities, name, partition)?;
        let (tx, rx) = oneshot::channel();
        let waited = self
            .enqueue(Item {
                topic: name.to_string(),
                partition,
                raw: raw.clone(),
                records: info.iter().map(|batch| batch.records).sum(),
                base_timestamp: info.first().map(|batch| batch.base_timestamp).unwrap_or(-1),
                max_timestamp: info.last().map(|batch| batch.max_timestamp).unwrap_or(-1),
                ack: if acks == 0 { None } else { Some(tx) },
                txn_id: txn.as_ref().map(|(id, _, _)| id.clone()),
                producer_id: txn.as_ref().map(|(_, id, _)| *id).unwrap_or(-1),
                producer_epoch: txn.as_ref().map(|(_, _, epoch)| *epoch).unwrap_or(-1),
            })
            .await;
        // A full `r2.buffer.limit` holds the produce in `enqueue`; report it.
        *held = (*held).max(waited);
        if acks == 0 {
            Ok(None)
        } else {
            Ok(Some(rx))
        }
    }

    async fn write_wal(
        &self,
        name: &str,
        topic: &Topic,
        records: &Option<Bytes>,
        partition: i32,
        acks: i16,
        timeout_ms: i32,
        txn_id: Option<&str>,
        held: &mut Duration,
    ) -> Result<Option<oneshot::Receiver<Result<i64, i16>>>, i16> {
        let wal = self.wal();
        if wal.storage_failed() {
            return Err(STORAGE);
        }
        if !wal.is_leader() {
            return Err(NOT_LEADER);
        }
        if partition < 0 || partition >= topic.partitions {
            return Err(UNKNOWN_TOPIC);
        }
        let Some(state) = self.partition_state(topic, partition) else {
            return Err(UNKNOWN_TOPIC);
        };
        let Some(raw) = records else {
            return Ok(None);
        };
        if raw.is_empty() {
            return Ok(None);
        }
        let info = match batch::inspect(raw, self.batch_limit(topic)) {
            Ok(info) => info,
            Err(BatchError::TooLarge) => return Err(TOO_LARGE),
            Err(BatchError::Corrupt) => return Err(CORRUPT),
            Err(BatchError::Transactional) => return Err(TXN),
        };
        let identities = batch::producer_batches(raw, MAX_BATCH).map_err(|_| CORRUPT)?;
        self.reject_null_key(topic, raw)?;
        let txn = self.accept_txn(txn_id, &info, &identities, name, partition)?;
        if txn.is_some() && acks == 0 {
            return Err(INVALID_REQUEST);
        }
        let append = Append {
            topic: name.to_string(),
            partition,
            topic_id: topic_id(topic),
            epoch: topic.leader_epoch,
            raw: raw.clone(),
            records: info.iter().map(|batch| batch.records).sum(),
            base_timestamp: info.first().map(|batch| batch.base_timestamp).unwrap_or(-1),
            max_timestamp: info.last().map(|batch| batch.max_timestamp).unwrap_or(-1),
            floor: state.next_offset,
        };
        if acks == 0 {
            tokio::spawn(async move {
                let _ = wal.append(append).await;
            });
            return Ok(None);
        }
        // ADR 0005 backpressure: past the disk limit, hold the produce until
        // an upload seals enough, and report the hold as throttle time. The
        // batch is not appended while held, so a timeout loses nothing that
        // was acknowledged.
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
        let records = append.records;
        let size = append.raw.len() as u64;
        let hold_start = Instant::now();
        loop {
            let unsealed = wal.unsealed_bytes();
            if unsealed == 0 || unsealed + size <= self.wal_limit {
                break;
            }
            if Instant::now() >= deadline {
                *held = (*held).max(hold_start.elapsed());
                return Err(REQUEST_TIMED_OUT);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        *held = (*held).max(hold_start.elapsed());
        // Without a quorum the commit never comes. Kafka answers a produce
        // that outlives `timeout_ms` with REQUEST_TIMED_OUT; the entry may
        // still commit later, as in Kafka.
        let wait = deadline.saturating_duration_since(Instant::now());
        let response = match tokio::time::timeout(wait, wal.append(append)).await {
            Err(_) => return Err(REQUEST_TIMED_OUT),
            Ok(Err(WriteError::NotLeader)) => return Err(NOT_LEADER),
            Ok(Err(WriteError::Storage)) => return Err(STORAGE),
            Ok(Err(_)) if wal.storage_failed() => return Err(STORAGE),
            Ok(Err(WriteError::Other(_))) if !wal.is_leader() => return Err(NOT_LEADER),
            Ok(Err(WriteError::Other(_))) => return Err(STORAGE),
            Ok(Ok(response)) => response,
        };
        if let Some((id, producer, epoch)) = txn {
            if response.error_code == 0 {
                let noted = self
                    .meta_apply(Command::NoteTxn {
                        transactional_id: id,
                        producer_id: producer,
                        producer_epoch: epoch,
                        span: TxnSpan {
                            topic: name.to_string(),
                            partition,
                            start: response.base_offset,
                            end: response.base_offset + i64::from(records),
                        },
                    })
                    .await;
                if let Some(code) = noted_error(&noted) {
                    return Err(code);
                }
            }
        }
        let (tx, rx) = oneshot::channel();
        let settled = if response.error_code == 0 {
            Ok(response.base_offset)
        } else {
            Err(response.error_code)
        };
        let _ = tx.send(settled);
        Ok(Some(rx))
    }

    /// Split `upload` (commit order) into objects of at most `FLUSH_BYTES`,
    /// put them in parallel, and return the publish parts and seals for the
    /// objects that were stored, in order, up to the first failure. That
    /// prefix keeps every partition's ranges contiguous. The third value is
    /// `false` when any put failed; a stored object past the failure is left
    /// for the orphan sweep (ADR 0007).
    async fn put_objects(
        &self,
        upload: Vec<camus_wal::Batch>,
    ) -> (Vec<PublishPart>, Vec<ProducerNote>, Vec<SealDrop>, bool) {
        let mut chunks: Vec<Vec<camus_wal::Batch>> = Vec::new();
        let mut size = 0usize;
        for batch in upload {
            if chunks.is_empty() || size + batch.bytes.len() > FLUSH_BYTES {
                chunks.push(Vec::new());
                size = 0;
            }
            size += batch.bytes.len();
            chunks.last_mut().expect("chunk").push(batch);
        }
        let mut puts = tokio::task::JoinSet::new();
        for (slot, chunk) in chunks.into_iter().enumerate() {
            let bucket = self.bucket.clone();
            puts.spawn(async move {
                let mut heads = Vec::with_capacity(chunk.len());
                let mut notes = Vec::new();
                let mut pieces = Vec::with_capacity(chunk.len());
                for batch in chunk {
                    if let Ok(extra) =
                        producer_notes(&batch.topic, batch.partition, &batch.bytes, batch.start)
                    {
                        notes.extend(extra);
                    }
                    heads.push((
                        batch.topic,
                        batch.topic_id,
                        batch.partition,
                        batch.start,
                        batch.end,
                        batch.base_timestamp,
                        batch.max_timestamp,
                    ));
                    pieces.push(Piece {
                        topic_id: batch.topic_id,
                        partition: batch.partition,
                        start: batch.start,
                        end: batch.end,
                        base_timestamp: batch.base_timestamp,
                        max_timestamp: batch.max_timestamp,
                        bytes: Bytes::from(batch.bytes),
                    });
                }
                let (index, body) = camus_uploader::encode(&pieces);
                drop(pieces);
                let key = camus_uploader::object_key();
                let stored = bucket.put_new(&key, body).await.is_ok();
                (slot, key, heads, notes, index, stored)
            });
        }
        let mut done = Vec::new();
        while let Some(joined) = puts.join_next().await {
            if let Ok(result) = joined {
                done.push(result);
            }
        }
        done.sort_by_key(|(slot, ..)| *slot);
        let mut parts = Vec::new();
        let mut producers = Vec::new();
        let mut seals = Vec::new();
        let mut all_put = true;
        for (expected, (slot, key, heads, notes, index, stored)) in done.into_iter().enumerate() {
            if !stored || slot != expected || !all_put {
                all_put = false;
                continue;
            }
            producers.extend(notes);
            for ((topic, topic_id, partition, start, end, base, max), (file_offset, length)) in
                heads.into_iter().zip(index)
            {
                parts.push(PublishPart {
                    topic: topic.clone(),
                    partition,
                    start,
                    end,
                    object_key: key.clone(),
                    file_offset,
                    length,
                    base_timestamp: base,
                    max_timestamp: max,
                });
                seals.push(SealDrop {
                    topic,
                    topic_id,
                    partition,
                    start,
                });
            }
        }
        (parts, producers, seals, all_put)
    }

    /// One upload pass: at most `upload.flush.bytes` (8 MiB, ADR 0006) of
    /// the oldest unsealed batches go into one object.
    async fn upload_wal(&self) -> Upload {
        // One shard for the whole pass, so a truncate mid-upload cannot seal
        // into the shard that replaced it.
        let wal = self.wal();
        if !wal.is_leader() {
            return Upload::Done;
        }
        let pending = match wal.pending(UPLOAD_PASS_BYTES as u64).await {
            Ok(pending) => pending,
            Err(err) => {
                tracing::error!(%err, "reading unsealed wal batches");
                return Upload::Failed;
            }
        };
        if pending.is_empty() {
            return Upload::Done;
        }
        let taken: usize = pending.iter().map(|batch| batch.bytes.len()).sum();
        let full = taken >= UPLOAD_PASS_BYTES;
        let mut ok = true;
        let mut drops = Vec::new();
        let mut upload = Vec::new();
        let mut cursor: BTreeMap<(String, i32), i64> = BTreeMap::new();
        for batch in pending {
            let topic = self
                .topic_name_by_id(&Uuid::from_bytes(batch.topic_id))
                .and_then(|name| self.topic(&name))
                .filter(|topic| topic_id(topic) == batch.topic_id);
            let Some(topic) = topic else {
                // A deleted topic's batches are dropped once the id has been
                // unknown long enough that a lagging replica is not the cause.
                if self.gone_long_enough(batch.topic_id) {
                    drops.push(SealDrop {
                        topic: batch.topic,
                        topic_id: batch.topic_id,
                        partition: batch.partition,
                        start: batch.start,
                    });
                }
                continue;
            };
            let Some(state) = self.partition_state(&topic, batch.partition) else {
                continue;
            };
            if batch.end <= state.next_offset {
                drops.push(SealDrop {
                    topic: batch.topic,
                    topic_id: batch.topic_id,
                    partition: batch.partition,
                    start: batch.start,
                });
                continue;
            }
            let key = (batch.topic.clone(), batch.partition);
            let next = cursor.get(&key).copied().unwrap_or(state.next_offset);
            if batch.start != next {
                continue;
            }
            cursor.insert(key, batch.end);
            upload.push(batch);
        }
        if !upload.is_empty() {
            let (parts, producers, seals, all_put) = self.put_objects(upload).await;
            if !all_put {
                ok = false;
            }
            if !parts.is_empty() {
                let published = self.meta_publish(parts, producers).await;
                let accepted = published.is_ok_and(|response| response.error_code == 0);
                if !accepted {
                    ok = false;
                }
                if accepted && std::env::var("CAMUS_FAILPOINT").as_deref() == Ok("after-publish") {
                    // Test hook: die between the metadata commit and the WAL
                    // seal, the window ADR 0006 says a restart must survive
                    // without publishing an extent twice.
                    tracing::warn!("failpoint after-publish: aborting");
                    std::process::abort();
                }
                if accepted {
                    drops.extend(seals);
                }
            }
        }
        let done = if !ok {
            Upload::Failed
        } else if full {
            Upload::Full
        } else {
            Upload::Done
        };
        if drops.is_empty() {
            return done;
        }
        if let Ok(index) = wal.seal(drops).await {
            let _ = wal.purge(index).await;
        }
        done
    }

    /// Buffer one `r2` batch. Returns how long it waited for buffer space.
    async fn enqueue(&self, item: Item) -> Duration {
        let started = Instant::now();
        loop {
            let held = self
                .buf
                .lock()
                .await
                .iter()
                .map(|queued| queued.raw.len())
                .sum::<usize>();
            if held == 0 || held + item.raw.len() <= BUFFER_LIMIT {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let waited = started.elapsed();
        let mut buf = self.buf.lock().await;
        let size = buf.iter().map(|queued| queued.raw.len()).sum::<usize>() + item.raw.len();
        buf.push(item);
        drop(buf);
        if size >= FLUSH_BYTES {
            self.flush().await;
        }
        waited
    }

    async fn flush(&self) {
        let _gate = self.flush_lock.lock().await;
        let items = {
            let mut buf = self.buf.lock().await;
            std::mem::take(&mut *buf)
        };
        if items.is_empty() {
            return;
        }
        let fail = |items: Vec<Item>, code: i16| {
            for item in items {
                if let Some(ack) = item.ack {
                    let _ = ack.send(Err(code));
                }
            }
        };
        if !self.leader() {
            return fail(items, NOT_LEADER);
        }
        let mut next: BTreeMap<(String, i32), i64> = BTreeMap::new();
        let mut epochs: BTreeMap<(String, i32), i32> = BTreeMap::new();
        let mut ids: BTreeMap<String, [u8; 16]> = BTreeMap::new();
        for item in &items {
            let key = (item.topic.clone(), item.partition);
            if next.contains_key(&key) {
                continue;
            }
            let Some(topic) = self.topic(&item.topic) else {
                return fail(items, UNKNOWN_TOPIC);
            };
            let Some(state) = self.partition_state(&topic, item.partition) else {
                return fail(items, UNKNOWN_TOPIC);
            };
            next.insert(key.clone(), state.next_offset);
            epochs.insert(key, state.leader_epoch);
            let id = Uuid::parse_str(&topic.id).unwrap_or_else(|_| Uuid::nil());
            ids.insert(item.topic.clone(), *id.as_bytes());
        }
        let mut sequences: BTreeMap<(String, i32), BTreeMap<i64, ProducerSeq>> = BTreeMap::new();
        // One result per item. Appends that share the object are filled in after publish.
        enum Plan {
            Replay(i64),
            Error(i16),
            Append,
        }
        let mut plans = Vec::with_capacity(items.len());
        let mut pieces = Vec::new();
        let mut notes = Vec::new();
        for item in &items {
            let identities = match batch::producer_batches(&item.raw, MAX_BATCH) {
                Ok(identities) => identities,
                Err(_) => {
                    plans.push(Plan::Error(CORRUPT));
                    continue;
                }
            };
            let key = (item.topic.clone(), item.partition);
            let known = sequences.entry(key).or_default();
            for identity in &identities {
                if identity.producer_id < 0 || known.contains_key(&identity.producer_id) {
                    continue;
                }
                if let Some(raw) = self
                    .node
                    .read_local(&sequence_key(
                        &item.topic,
                        item.partition,
                        identity.producer_id,
                    ))
                    .ok()
                    .flatten()
                {
                    if let Ok(seq) = serde_json::from_str::<ProducerSeq>(&raw) {
                        known.insert(identity.producer_id, seq);
                    }
                }
            }
            let class = match idempotence::classify(known, &identities) {
                Ok(class) => class,
                Err(code) => {
                    plans.push(Plan::Error(code));
                    continue;
                }
            };
            if let Class::Replay { base_offset } = class {
                plans.push(Plan::Replay(base_offset));
                continue;
            }
            let start = *next
                .get(&(item.topic.clone(), item.partition))
                .expect("offset");
            let epoch = *epochs
                .get(&(item.topic.clone(), item.partition))
                .expect("epoch");
            let sealed = match batch::seal(&item.raw, start, epoch, MAX_BATCH) {
                Ok(bytes) => bytes,
                Err(_) => {
                    plans.push(Plan::Error(CORRUPT));
                    continue;
                }
            };
            let end = start + i64::from(item.records);
            *next
                .get_mut(&(item.topic.clone(), item.partition))
                .expect("offset") = end;
            if let Class::Fresh = class {
                let mut at = start;
                for identity in &identities {
                    if identity.producer_id >= 0 {
                        notes.push(ProducerNote {
                            topic: item.topic.clone(),
                            partition: item.partition,
                            producer_id: identity.producer_id,
                            producer_epoch: identity.producer_epoch,
                            base_sequence: identity.base_sequence,
                            records: identity.records,
                            base_offset: at,
                        });
                        idempotence::record(
                            known.entry(identity.producer_id).or_default(),
                            identity.producer_epoch,
                            identity.base_sequence,
                            identity.records,
                            at,
                        );
                    }
                    at += i64::from(identity.records);
                }
            }
            plans.push(Plan::Append);
            pieces.push(Piece {
                topic_id: *ids.get(&item.topic).expect("id"),
                partition: item.partition,
                start,
                end,
                base_timestamp: item.base_timestamp,
                max_timestamp: item.max_timestamp,
                bytes: sealed,
            });
        }
        let mut parts = Vec::new();
        if !pieces.is_empty() {
            let (index, body) = camus_uploader::encode(&pieces);
            let object_key = camus_uploader::object_key();
            if self.bucket.put_new(&object_key, body).await.is_err() {
                return fail(items, STORAGE);
            }
            let appended = items.iter().zip(&plans).filter_map(|(item, plan)| {
                if matches!(plan, Plan::Append) {
                    Some(item)
                } else {
                    None
                }
            });
            for (item, (file_offset, length)) in appended.zip(index) {
                let start = pieces[parts.len()].start;
                parts.push(PublishPart {
                    topic: item.topic.clone(),
                    partition: item.partition,
                    start,
                    end: start + i64::from(item.records),
                    object_key: object_key.clone(),
                    file_offset,
                    length,
                    base_timestamp: item.base_timestamp,
                    max_timestamp: item.max_timestamp,
                });
            }
            let published = self.node.publish(parts, notes).await;
            let code = match published {
                Ok(response) if response.error_code == 0 => None,
                Ok(response) => Some(response.error_code),
                Err(_) => Some(NOT_LEADER),
            };
            if let Some(code) = code {
                return fail(items, code);
            }
        }
        let mut noted = pieces.iter();
        for (item, plan) in items.iter().zip(&plans) {
            let Some(id) = item.txn_id.clone() else {
                if matches!(plan, Plan::Append) {
                    let _ = noted.next();
                }
                continue;
            };
            let (start, end) = match plan {
                Plan::Replay(offset) => (*offset, offset + i64::from(item.records)),
                Plan::Append => {
                    let piece = noted.next().expect("piece");
                    (piece.start, piece.end)
                }
                Plan::Error(_) => continue,
            };
            let applied = self
                .meta_apply(Command::NoteTxn {
                    transactional_id: id,
                    producer_id: item.producer_id,
                    producer_epoch: item.producer_epoch,
                    span: TxnSpan {
                        topic: item.topic.clone(),
                        partition: item.partition,
                        start,
                        end,
                    },
                })
                .await;
            if let Some(code) = noted_error(&applied) {
                return fail(items, code);
            }
        }
        let mut appends = pieces.iter();
        for (item, plan) in items.into_iter().zip(plans) {
            let settled = match plan {
                Plan::Replay(offset) => Ok(offset),
                Plan::Error(code) => Err(code),
                Plan::Append => Ok(appends.next().expect("piece").start),
            };
            if let Some(ack) = item.ack {
                let _ = ack.send(settled);
            }
        }
    }

    async fn fetch(&self, header: &RequestHeader, buf: &mut Bytes, caller: &Caller) -> Reply {
        let Ok(request) = FetchRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = FetchResponse::default();
        let deadline = Instant::now() + Duration::from_millis(request.max_wait_ms.max(0) as u64);
        loop {
            response.responses.clear();
            let mut any = false;
            for topic in &request.topics {
                let name = self.resolve_topic(
                    topic.topic.0.as_str(),
                    &topic.topic_id,
                    header.request_api_version >= 13,
                );
                let mut topic_response = FetchableTopicResponse::default();
                topic_response.topic = topic_name(&name);
                topic_response.topic_id = topic.topic_id;
                let stored = self.topic(&name);
                if !self.allows(caller, auth::TOPIC, &name, auth::READ) {
                    for partition in &topic.partitions {
                        let mut row =
                            camus_protocol::messages::fetch_response::PartitionData::default();
                        row.partition_index = partition.partition;
                        row.error_code = 29;
                        topic_response.partitions.push(row);
                    }
                    response.responses.push(topic_response);
                    continue;
                }
                for partition in &topic.partitions {
                    let row = self
                        .fetch_partition(
                            stored.as_ref(),
                            partition.partition,
                            partition.fetch_offset,
                            partition.partition_max_bytes,
                            request.isolation_level,
                        )
                        .await;
                    if row.records.as_ref().is_some_and(|bytes| !bytes.is_empty()) {
                        any = true;
                    }
                    topic_response.partitions.push(row);
                }
                response.responses.push(topic_response);
            }
            if any || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn fetch_partition(
        &self,
        topic: Option<&Topic>,
        partition: i32,
        offset: i64,
        max_bytes: i32,
        isolation: i8,
    ) -> PartitionData {
        let mut row = PartitionData::default();
        row.partition_index = partition;
        let Some(topic) = topic else {
            row.error_code = UNKNOWN_TOPIC;
            return row;
        };
        if topic.durability == "wal" && !self.wal().is_leader() {
            row.error_code = NOT_LEADER;
            return row;
        }
        let limit = max_bytes.max(0) as usize;
        // Publish seals the WAL after the metadata write. A fetch that reads
        // metadata first can miss batches sealed before the WAL read. Retry
        // when that prefix moves.
        for _ in 0..8 {
            let Some(state) = self.partition_state(topic, partition) else {
                row.error_code = UNKNOWN_TOPIC;
                return row;
            };
            let sealed = state.next_offset;
            let wal_end = self.wal().end_offset(&topic_id(topic), partition);
            let high_watermark = sealed.max(wal_end);
            let mut stable = high_watermark;
            row.high_watermark = high_watermark;
            row.last_stable_offset = high_watermark;
            row.log_start_offset = state.log_start;
            row.error_code = 0;
            row.records = None;
            if offset < state.log_start || offset > high_watermark {
                row.error_code = 1;
                return row;
            }
            let mut out = BytesMut::new();
            let mut stopped = false;
            'extents: for extent in &state.extents {
                // A compacted range whose records all expired has no bytes.
                if extent.end <= offset || extent.length == 0 {
                    continue;
                }
                let end = extent.file_offset + u64::from(extent.length);
                let Ok(bytes) = self
                    .bucket
                    .get_range(&extent.key, extent.file_offset..end)
                    .await
                else {
                    row.error_code = STORAGE;
                    return row;
                };
                let Ok(info) = batch::inspect(&bytes, MAX_FRAME) else {
                    row.error_code = CORRUPT;
                    return row;
                };
                let mut cursor = 0;
                for batch in info {
                    let next = batch.last_offset + 1;
                    if isolation == 1 {
                        match txn_batch(batch.base_offset, batch.transactional, &state) {
                            TxnView::Stop => {
                                stable = stable.min(batch.base_offset);
                                stopped = true;
                                break 'extents;
                            }
                            TxnView::Skip => {
                                cursor += batch.len;
                                continue;
                            }
                            TxnView::Take => {}
                        }
                    }
                    if next > offset && batch.base_offset < stable {
                        if !out.is_empty() && out.len() + batch.len > limit {
                            stopped = true;
                            break 'extents;
                        }
                        out.extend_from_slice(&bytes[cursor..cursor + batch.len]);
                    }
                    cursor += batch.len;
                }
            }
            if !stopped && offset < stable {
                let remaining = limit.saturating_sub(out.len());
                let extra = match self
                    .wal()
                    .read(&topic_id(topic), partition, sealed, offset, remaining)
                    .await
                {
                    Ok(extra) => extra,
                    Err(_) => {
                        row.error_code = STORAGE;
                        return row;
                    }
                };
                let extra = if isolation == 1 {
                    filter_txn(&extra, &state, &mut stable)
                } else {
                    extra
                };
                if !extra.is_empty() && (out.is_empty() || out.len() + extra.len() <= limit) {
                    out.extend_from_slice(&extra);
                }
            }
            let moved = self
                .partition_state(topic, partition)
                .is_some_and(|later| later.next_offset != sealed);
            if isolation == 1 {
                row.high_watermark = stable;
                row.last_stable_offset = stable;
            }
            if !out.is_empty() {
                row.records = Some(out.freeze());
            }
            if !moved {
                return row;
            }
        }
        row
    }

    async fn list_offsets(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = ListOffsetsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = ListOffsetsResponse::default();
        for topic in request.topics {
            let name = topic.name.0.to_string();
            let mut topic_response = ListOffsetsTopicResponse::default();
            topic_response.name = topic_name(&name);
            let stored = self.topic(&name);
            let allowed = self.allows(caller, auth::TOPIC, &name, auth::DESCRIBE);
            for partition in topic.partitions {
                if !allowed {
                    let mut row = ListOffsetsPartitionResponse::default();
                    row.partition_index = partition.partition_index;
                    row.error_code = auth::TOPIC_FAILED;
                    row.timestamp = -1;
                    row.offset = -1;
                    topic_response.partitions.push(row);
                    continue;
                }
                topic_response.partitions.push(
                    self.list_one(
                        stored.as_ref(),
                        partition.partition_index,
                        partition.timestamp,
                    )
                    .await,
                );
            }
            response.topics.push(topic_response);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn list_one(
        &self,
        topic: Option<&Topic>,
        partition: i32,
        timestamp: i64,
    ) -> ListOffsetsPartitionResponse {
        let mut row = ListOffsetsPartitionResponse::default();
        row.partition_index = partition;
        row.timestamp = -1;
        let Some(topic) = topic else {
            row.error_code = UNKNOWN_TOPIC;
            row.offset = -1;
            return row;
        };
        if topic.durability == "wal" && !self.wal().is_leader() {
            row.error_code = NOT_LEADER;
            row.offset = -1;
            return row;
        }
        let Some(state) = self.partition_state(topic, partition) else {
            row.error_code = UNKNOWN_TOPIC;
            row.offset = -1;
            return row;
        };
        row.leader_epoch = state.leader_epoch;
        if timestamp == -2 {
            row.offset = state.log_start;
            return row;
        }
        if timestamp == -1 {
            row.offset = state
                .next_offset
                .max(self.wal().end_offset(&topic_id(topic), partition));
            return row;
        }
        for extent in &state.extents {
            if extent.max_timestamp < timestamp || extent.length == 0 {
                continue;
            }
            let end = extent.file_offset + u64::from(extent.length);
            let Ok(bytes) = self
                .bucket
                .get_range(&extent.key, extent.file_offset..end)
                .await
            else {
                row.error_code = STORAGE;
                row.offset = -1;
                return row;
            };
            let Ok(info) = batch::inspect(&bytes, MAX_FRAME) else {
                row.error_code = CORRUPT;
                row.offset = -1;
                return row;
            };
            let mut batch_offset = extent.start;
            for batch in info {
                if batch.max_timestamp >= timestamp {
                    row.offset = batch_offset;
                    row.timestamp = batch.max_timestamp;
                    return row;
                }
                batch_offset += i64::from(batch.records);
            }
        }
        row.offset = -1;
        row
    }

    fn sasl_handshake(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = SaslHandshakeRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = SaslHandshakeResponse::default();
        response.mechanisms = vec![StrBytes::from_static_str("PLAIN")];
        if request.mechanism.as_str() != "PLAIN" {
            response.error_code = SASL_MECHANISM;
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn sasl_authenticate(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        authed: &mut bool,
    ) -> Reply {
        let Ok(request) = SaslAuthenticateRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = SaslAuthenticateResponse::default();
        let bytes = request.auth_bytes.as_ref();
        let mut parts = bytes.split(|byte| *byte == 0);
        let _authzid = parts.next();
        let user = parts.next().unwrap_or_default();
        let pass = parts.next().unwrap_or_default();
        if constant_eq(user, self.sasl_user.as_bytes())
            && constant_eq(pass, self.sasl_pass.as_bytes())
            && !self.sasl_user.is_empty()
        {
            *authed = true;
        } else {
            response.error_code = SASL_AUTH_FAILED;
            response.error_message = Some(StrBytes::from_static_str("authentication failed"));
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

/// Milliseconds produce requests spent held at `wal.disk.limit`, in total.
pub static PRODUCE_HOLD_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Kafka ACL resource types, operations, and authorization errors.
pub(crate) mod auth {
    pub(crate) const TOPIC: i8 = 2;
    pub(crate) const GROUP: i8 = 3;
    pub(crate) const CLUSTER: i8 = 4;
    pub(crate) const TXN_ID: i8 = 5;
    pub(crate) const READ: i8 = 3;
    pub(crate) const WRITE: i8 = 4;
    pub(crate) const CREATE: i8 = 5;
    pub(crate) const DELETE: i8 = 6;
    pub(crate) const ALTER: i8 = 7;
    pub(crate) const DESCRIBE: i8 = 8;
    pub(crate) const DESCRIBE_CONFIGS: i8 = 10;
    pub(crate) const ALTER_CONFIGS: i8 = 11;
    pub(crate) const IDEMPOTENT_WRITE: i8 = 12;
    pub(crate) const CLUSTER_NAME: &str = "kafka-cluster";
    pub(crate) const TOPIC_FAILED: i16 = 29;
    pub(crate) const GROUP_FAILED: i16 = 30;
    pub(crate) const CLUSTER_FAILED: i16 = 31;
    pub(crate) const TXN_ID_FAILED: i16 = 53;
}

/// Who sent a request: the principal and the client's IP address.
pub(crate) struct Caller {
    pub(crate) principal: String,
    pub(crate) host: String,
}

/// What a connection knows about its client.
struct Session {
    authed: bool,
    host: String,
}

/// How an upload pass ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Upload {
    /// Nothing left, or everything taken fit in one object.
    Done,
    /// The object reached the size cap; more is waiting.
    Full,
    /// The put or the publish failed; back off.
    Failed,
}

/// Exponential backoff from 100ms to 5s, with up to half again as jitter so
/// several brokers do not retry R2 in step.
fn retry_delay(failures: u32) -> Duration {
    let base = 100u64.saturating_mul(1 << failures.min(6)).min(5_000);
    let noise = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::from(elapsed.subsec_nanos()))
        .unwrap_or(0)
        % (base / 2 + 1);
    Duration::from_millis(base + noise)
}

fn producer_notes(
    topic: &str,
    partition: i32,
    bytes: &[u8],
    start: i64,
) -> Result<Vec<ProducerNote>, BatchError> {
    let identities = batch::producer_batches(bytes, MAX_BATCH)?;
    let mut notes = Vec::new();
    let mut at = start;
    for identity in identities {
        if identity.producer_id >= 0 {
            notes.push(ProducerNote {
                topic: topic.to_string(),
                partition,
                producer_id: identity.producer_id,
                producer_epoch: identity.producer_epoch,
                base_sequence: identity.base_sequence,
                records: identity.records,
                base_offset: at,
            });
        }
        at += i64::from(identity.records);
    }
    Ok(notes)
}

fn topic_id(topic: &Topic) -> [u8; 16] {
    Uuid::parse_str(&topic.id)
        .map(|id| *id.as_bytes())
        .unwrap_or([0; 16])
}

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

fn partition_error(index: i32, code: i16) -> PartitionProduceResponse {
    let mut row = PartitionProduceResponse::default();
    row.index = index;
    row.error_code = code;
    row.error_message = produce_message(code);
    row.base_offset = -1;
    row.log_append_time_ms = -1;
    row.log_start_offset = 0;
    row
}

/// The message Kafka sends with a produce error, where it sends one.
fn produce_message(code: i16) -> Option<StrBytes> {
    let text = match code {
        INVALID_RECORD => "Compacted topic cannot accept message without key.",
        TOO_LARGE => "The record batch is larger than the topic's max.message.bytes.",
        _ => return None,
    };
    Some(StrBytes::from_static_str(text))
}

/// One topic config as CreateTopics and DescribeConfigs report it.
struct ConfigView {
    spec: &'static camus_metadata::config::Spec,
    value: String,
    source: i8,
}

/// `NoteTxn` applies the abort and reports the Kafka code in `value` when the
/// batch cannot join the transaction. A normal success leaves `value` empty.
fn noted_error(applied: &anyhow::Result<camus_metadata::Response>) -> Option<i16> {
    match applied {
        Ok(response) if response.error_code != 0 => Some(response.error_code),
        Ok(response) => match response.value.as_deref() {
            Some(text) if !text.is_empty() => text.parse().ok(),
            _ => None,
        },
        Err(_) => Some(NOT_LEADER),
    }
}

enum TxnView {
    Take,
    Skip,
    Stop,
}

fn txn_batch(offset: i64, transactional: bool, state: &camus_metadata::Partition) -> TxnView {
    if !transactional {
        return TxnView::Take;
    }
    if state
        .txn_aborted
        .iter()
        .any(|span| span.start <= offset && offset < span.end)
    {
        return TxnView::Skip;
    }
    if state
        .txn_committed
        .iter()
        .any(|span| span.start <= offset && offset < span.end)
    {
        TxnView::Take
    } else {
        TxnView::Stop
    }
}

fn filter_txn(bytes: &[u8], state: &camus_metadata::Partition, stable: &mut i64) -> Vec<u8> {
    let Ok(info) = batch::inspect(bytes, MAX_FRAME) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = 0;
    for batch in info {
        match txn_batch(batch.base_offset, batch.transactional, state) {
            TxnView::Stop => {
                *stable = (*stable).min(batch.base_offset);
                break;
            }
            TxnView::Skip => {}
            TxnView::Take if batch.base_offset < *stable => {
                out.extend_from_slice(&bytes[cursor..cursor + batch.len]);
            }
            TxnView::Take => {}
        }
        cursor += batch.len;
    }
    out
}

fn frame_of<M>(version: i16, correlation: i32, message: &M) -> Reply
where
    M: camus_protocol::protocol::Encodable + camus_protocol::protocol::HeaderVersion,
{
    match encode_response(version, correlation, message) {
        Ok(frame) => Reply::Frame(frame),
        Err(err) => {
            tracing::warn!(%err, version, "response encode failed");
            Reply::Close
        }
    }
}

fn unsupported(key: ApiKey, version: i16, correlation: i32) -> Reply {
    error_frame(
        key,
        version,
        correlation,
        ResponseError::UnsupportedVersion.code(),
    )
}

fn error_frame(key: ApiKey, version: i16, correlation: i32, code: i16) -> Reply {
    match camus_protocol::unsupported::error_response(key, version, correlation, code) {
        Some(frame) => Reply::Frame(frame),
        None => Reply::Close,
    }
}

fn constant_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right) {
        diff |= a ^ b;
    }
    diff == 0
}
