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
use camus_metadata::{MetadataNode, PublishPart, Topic};
use camus_protocol::batch::{self, BatchError};
use camus_protocol::error::ResponseError;
use camus_protocol::frame::{self, encode_response};
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
    DescribeConfigsRequest, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, ProduceRequest, ProduceResponse, RequestHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use camus_protocol::protocol::{decode_request_header_from_buffer, Decodable, StrBytes};
use camus_storage::Bucket;
use camus_uploader::{self, Piece};
use camus_wal::{Append, SealDrop, Shard};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

const MAX_FRAME: usize = 100 * 1024 * 1024;
const MAX_BATCH: usize = 1024 * 1024;
const FLUSH_BYTES: usize = 8 * 1024 * 1024;
const BUFFER_LIMIT: usize = 256 * 1024 * 1024;

const CORRUPT: i16 = 2;
const UNKNOWN_TOPIC: i16 = 3;
const LEADER_NOT_AVAILABLE: i16 = 5;
const NOT_LEADER: i16 = 6;
const TOO_LARGE: i16 = 10;
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

/// One broker's Kafka endpoint.
pub struct Kafka {
    node: Arc<MetadataNode>,
    bucket: Bucket,
    node_id: i32,
    host: String,
    port: i32,
    flush_ms: u64,
    require_sasl: bool,
    sasl_user: String,
    sasl_pass: String,
    tls: Option<TlsAcceptor>,
    wal: Arc<Shard>,
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
        wal: Arc<Shard>,
    ) -> Self {
        let flush_ms = std::env::var("CAMUS_FLUSH_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(250);
        Self {
            node,
            bucket,
            node_id,
            host,
            port,
            flush_ms,
            require_sasl,
            sasl_user,
            sasl_pass,
            tls,
            wal,
            buf: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
        }
    }

    fn leader(&self) -> bool {
        self.node.raft.metrics().borrow().current_leader == Some(self.node_id as u64)
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

    /// Accept connections until the listener closes.
    pub async fn run(self: Arc<Self>, listener: TcpListener) {
        let flush = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(flush.flush_ms)).await;
                flush.flush().await;
            }
        });
        let upload = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(upload.flush_ms)).await;
                upload.upload_wal().await;
            }
        });
        let register = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                if register.leader() {
                    let addr = format!("{}:{}", register.host, register.port);
                    let _ = register
                        .node
                        .write(format!("broker/{}", register.node_id), &addr)
                        .await;
                    let _ = register
                        .node
                        .write(
                            "brokers",
                            serde_json::to_string(&vec![register.node_id])
                                .unwrap_or_else(|_| "[]".into()),
                        )
                        .await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                break;
            };
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
                let _ = kafka.connection(socket).await;
            });
        }
    }

    async fn connection(&self, mut socket: Socket) -> anyhow::Result<()> {
        let mut authed = !self.require_sasl;
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
            match self.handle(&body, &mut authed).await {
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

    async fn handle(&self, body: &[u8], authed: &mut bool) -> Reply {
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
        if self.require_sasl && !(*authed) && !allowed_before_auth {
            return self.illegal_sasl(key, version, header.correlation_id);
        }
        if !frame::is_implemented(key, version) {
            return unsupported(key, version, header.correlation_id);
        }
        match key {
            ApiKey::ApiVersions => self.api_versions(&header, &mut buf),
            ApiKey::Metadata => self.metadata(&header, &mut buf).await,
            ApiKey::CreateTopics => self.create_topics(&header, &mut buf).await,
            ApiKey::DeleteTopics => self.delete_topics(&header, &mut buf).await,
            ApiKey::DescribeConfigs => self.describe_configs(&header, &mut buf),
            ApiKey::Produce => self.produce(&header, &mut buf).await,
            ApiKey::Fetch => self.fetch(&header, &mut buf).await,
            ApiKey::ListOffsets => self.list_offsets(&header, &mut buf).await,
            ApiKey::SaslHandshake => self.sasl_handshake(&header, &mut buf),
            ApiKey::SaslAuthenticate => self.sasl_authenticate(&header, &mut buf, authed),
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

    async fn metadata(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = MetadataRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
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
            response.topics.push(self.metadata_topic(&name, leader));
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn topic_names(&self) -> Vec<String> {
        let Ok(Some(raw)) = self.node.read_local(camus_metadata::topic_index_key()) else {
            return Vec::new();
        };
        serde_json::from_str(&raw).unwrap_or_default()
    }

    fn broker_list(&self) -> Vec<MetadataResponseBroker> {
        let Ok(Some(raw)) = self.node.read_local("brokers") else {
            return Vec::new();
        };
        let ids: Vec<i32> = serde_json::from_str(&raw).unwrap_or_default();
        ids.into_iter()
            .filter_map(|id| {
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
                partition.replica_nodes = vec![BrokerId(leader_id)];
                partition.isr_nodes = vec![BrokerId(leader_id)];
            }
            topic.partitions.push(partition);
        }
        topic
    }

    async fn create_topics(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = CreateTopicsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if !self.leader() {
            return self.create_error(&request, header, NOT_LEADER);
        }
        let mut response = CreateTopicsResponse::default();
        for topic in request.topics {
            let name = topic.name.0.to_string();
            let partitions = if topic.num_partitions < 1 {
                1
            } else {
                topic.num_partitions
            };
            let durability = topic
                .configs
                .iter()
                .find(|config| config.name.as_str() == "durability.mode")
                .map(|config| {
                    config
                        .value
                        .as_ref()
                        .map(|value| value.to_string())
                        .unwrap_or_default()
                })
                .unwrap_or_else(|| "r2".into());
            let code = if request.validate_only {
                if self.topic(&name).is_some() {
                    36
                } else if durability != "r2" && durability != "wal" {
                    INVALID_CONFIG
                } else {
                    0
                }
            } else {
                match self.node.create_topic(&name, partitions, durability).await {
                    Ok(result) => result.error_code,
                    Err(_) => NOT_LEADER,
                }
            };
            let mut result = CreatableTopicResult::default();
            result.name = topic_name(&name);
            result.error_code = code;
            result.num_partitions = partitions;
            result.replication_factor = 1;
            if code == 0 {
                if let Some(stored) = self.topic(&name) {
                    result.topic_id = Uuid::parse_str(&stored.id).unwrap_or_else(|_| Uuid::nil());
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

    async fn delete_topics(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
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
            let code = if !self.leader() {
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

    fn describe_configs(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
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
            } else if let Some(topic) = self.topic(resource.resource_name.as_str()) {
                result.configs = vec![
                    config_row("durability.mode", &topic.durability),
                    config_row("cleanup.policy", "delete"),
                ];
            } else {
                result.error_code = UNKNOWN_TOPIC;
            }
            response.results.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn produce(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = ProduceRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if request.acks != 0 && request.acks != 1 && request.acks != -1 {
            return self.produce_all_error(&request, header, INVALID_REQUEST);
        }
        if request.transactional_id.is_some() {
            return self.produce_all_error(&request, header, TXN);
        }
        if !self.leader() {
            return self.produce_all_error(&request, header, NOT_LEADER);
        }
        let mut response = ProduceResponse::default();
        let mut waiters = Vec::new();
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
            for partition in topic.partition_data {
                match self
                    .enqueue_partition(
                        &name,
                        stored.as_ref(),
                        &partition.records,
                        partition.index,
                        request.acks,
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
                Err(code) => row.error_code = code,
            }
        }
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
    ) -> Result<Option<oneshot::Receiver<Result<i64, i16>>>, i16> {
        let Some(topic) = topic else {
            return Err(UNKNOWN_TOPIC);
        };
        if topic.durability == "wal" {
            return self.write_wal(name, topic, records, partition, acks).await;
        }
        if topic.durability != "r2" {
            return Err(INVALID_CONFIG);
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
        let info = match batch::inspect(raw, MAX_BATCH) {
            Ok(info) => info,
            Err(BatchError::TooLarge) => return Err(TOO_LARGE),
            Err(BatchError::Transactional) => return Err(TXN),
            Err(BatchError::Corrupt) => return Err(CORRUPT),
        };
        let (tx, rx) = oneshot::channel();
        self.enqueue(Item {
            topic: name.to_string(),
            partition,
            raw: raw.clone(),
            records: info.iter().map(|batch| batch.records).sum(),
            base_timestamp: info.first().map(|batch| batch.base_timestamp).unwrap_or(-1),
            max_timestamp: info.last().map(|batch| batch.max_timestamp).unwrap_or(-1),
            ack: if acks == 0 { None } else { Some(tx) },
        })
        .await;
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
    ) -> Result<Option<oneshot::Receiver<Result<i64, i16>>>, i16> {
        if !self.wal.is_leader() {
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
        let info = match batch::inspect(raw, MAX_BATCH) {
            Ok(info) => info,
            Err(BatchError::TooLarge) => return Err(TOO_LARGE),
            Err(BatchError::Transactional) => return Err(TXN),
            Err(BatchError::Corrupt) => return Err(CORRUPT),
        };
        let topic_id = Uuid::parse_str(&topic.id)
            .map(|id| *id.as_bytes())
            .unwrap_or([0; 16]);
        let append = Append {
            topic: name.to_string(),
            partition,
            topic_id,
            epoch: topic.leader_epoch,
            raw: raw.to_vec(),
            records: info.iter().map(|batch| batch.records).sum(),
            base_timestamp: info.first().map(|batch| batch.base_timestamp).unwrap_or(-1),
            max_timestamp: info.last().map(|batch| batch.max_timestamp).unwrap_or(-1),
        };
        if acks == 0 {
            let wal = Arc::clone(&self.wal);
            tokio::spawn(async move {
                let _ = wal.append(append).await;
            });
            return Ok(None);
        }
        let response = self.wal.append(append).await.map_err(|_| STORAGE)?;
        let (tx, rx) = oneshot::channel();
        let settled = if response.error_code == 0 {
            Ok(response.base_offset)
        } else {
            Err(response.error_code)
        };
        let _ = tx.send(settled);
        Ok(Some(rx))
    }

    async fn upload_wal(&self) {
        if !self.leader() || !self.wal.is_leader() {
            return;
        }
        let pending = self.wal.pending();
        if pending.is_empty() {
            return;
        }
        let mut drops = Vec::new();
        let mut upload = Vec::new();
        let mut cursor: BTreeMap<(String, i32), i64> = BTreeMap::new();
        for batch in pending {
            let Some(topic) = self.topic(&batch.topic) else {
                continue;
            };
            let Some(state) = self.partition_state(&topic, batch.partition) else {
                continue;
            };
            if batch.end <= state.next_offset {
                drops.push(SealDrop {
                    topic: batch.topic,
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
            let pieces: Vec<Piece> = upload
                .iter()
                .map(|batch| Piece {
                    topic_id: batch.topic_id,
                    partition: batch.partition,
                    start: batch.start,
                    end: batch.end,
                    base_timestamp: batch.base_timestamp,
                    max_timestamp: batch.max_timestamp,
                    bytes: Bytes::from(batch.bytes.clone()),
                })
                .collect();
            let (index, body) = camus_uploader::encode(&pieces);
            let key = camus_uploader::object_key();
            if self.bucket.put_new(&key, body).await.is_ok() {
                let parts = upload
                    .iter()
                    .zip(&index)
                    .map(
                        |(batch, (file_offset, length))| camus_metadata::PublishPart {
                            topic: batch.topic.clone(),
                            partition: batch.partition,
                            start: batch.start,
                            end: batch.end,
                            object_key: key.clone(),
                            file_offset: *file_offset,
                            length: *length,
                            base_timestamp: batch.base_timestamp,
                            max_timestamp: batch.max_timestamp,
                        },
                    )
                    .collect();
                if self.node.publish(parts).await.is_ok() {
                    drops.extend(upload.into_iter().map(|batch| SealDrop {
                        topic: batch.topic,
                        partition: batch.partition,
                        start: batch.start,
                    }));
                }
            }
        }
        if drops.is_empty() {
            return;
        }
        if let Ok(index) = self.wal.seal(drops).await {
            let _ = self.wal.purge(index).await;
        }
    }

    async fn enqueue(&self, item: Item) {
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
        let mut buf = self.buf.lock().await;
        let size = buf.iter().map(|queued| queued.raw.len()).sum::<usize>() + item.raw.len();
        buf.push(item);
        drop(buf);
        if size >= FLUSH_BYTES {
            self.flush().await;
        }
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
        let mut pieces = Vec::new();
        let mut parts = Vec::new();
        let mut assigned = Vec::new();
        for item in &items {
            let key = (item.topic.clone(), item.partition);
            let start = *next.get(&key).expect("offset");
            let epoch = *epochs.get(&key).expect("epoch");
            let sealed = match batch::seal(&item.raw, start, epoch, MAX_BATCH) {
                Ok(bytes) => bytes,
                Err(_) => return fail(items, CORRUPT),
            };
            let end = start + i64::from(item.records);
            *next.get_mut(&key).expect("offset") = end;
            assigned.push(start);
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
        let (index, body) = camus_uploader::encode(&pieces);
        let object_key = camus_uploader::object_key();
        if self.bucket.put_new(&object_key, body).await.is_err() {
            return fail(items, STORAGE);
        }
        for (item, (file_offset, length)) in items.iter().zip(index) {
            let start = assigned[parts.len()];
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
        let published = self.node.publish(parts).await;
        let code = match published {
            Ok(response) if response.error_code == 0 => None,
            Ok(response) => Some(response.error_code),
            Err(_) => Some(NOT_LEADER),
        };
        if let Some(code) = code {
            return fail(items, code);
        }
        for (item, start) in items.into_iter().zip(assigned) {
            if let Some(ack) = item.ack {
                let _ = ack.send(Ok(start));
            }
        }
    }

    async fn fetch(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
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
                for partition in &topic.partitions {
                    let row = self
                        .fetch_partition(
                            stored.as_ref(),
                            partition.partition,
                            partition.fetch_offset,
                            partition.partition_max_bytes,
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
    ) -> PartitionData {
        let mut row = PartitionData::default();
        row.partition_index = partition;
        let Some(topic) = topic else {
            row.error_code = UNKNOWN_TOPIC;
            return row;
        };
        let Some(state) = self.partition_state(topic, partition) else {
            row.error_code = UNKNOWN_TOPIC;
            return row;
        };
        let wal_end = self.wal.end_offset(&topic.name, partition);
        let high_watermark = state.next_offset.max(wal_end);
        row.high_watermark = high_watermark;
        row.last_stable_offset = high_watermark;
        row.log_start_offset = state.log_start;
        if offset < state.log_start || offset > high_watermark {
            row.error_code = 1;
            return row;
        }
        let mut out = BytesMut::new();
        let limit = max_bytes.max(0) as usize;
        for extent in &state.extents {
            if extent.end <= offset {
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
            let mut batch_offset = extent.start;
            for batch in info {
                let next = batch_offset + i64::from(batch.records);
                if next > offset {
                    if !out.is_empty() && out.len() + batch.len > limit {
                        break;
                    }
                    out.extend_from_slice(&bytes[cursor..cursor + batch.len]);
                }
                cursor += batch.len;
                batch_offset = next;
            }
        }
        if offset < high_watermark {
            let extra = self
                .wal
                .read(&topic.name, partition, state.next_offset, offset);
            if !extra.is_empty() && (out.is_empty() || out.len() + extra.len() <= limit) {
                out.extend_from_slice(&extra);
            }
        }
        if !out.is_empty() {
            row.records = Some(out.freeze());
        }
        row
    }

    async fn list_offsets(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = ListOffsetsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = ListOffsetsResponse::default();
        for topic in request.topics {
            let name = topic.name.0.to_string();
            let mut topic_response = ListOffsetsTopicResponse::default();
            topic_response.name = topic_name(&name);
            let stored = self.topic(&name);
            for partition in topic.partitions {
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
                .max(self.wal.end_offset(&topic.name, partition));
            return row;
        }
        for extent in &state.extents {
            if extent.max_timestamp < timestamp {
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

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

fn partition_error(index: i32, code: i16) -> PartitionProduceResponse {
    let mut row = PartitionProduceResponse::default();
    row.index = index;
    row.error_code = code;
    row.base_offset = -1;
    row.log_append_time_ms = -1;
    row.log_start_offset = 0;
    row
}

fn config_row(name: &str, value: &str) -> DescribeConfigsResourceResult {
    let mut row = DescribeConfigsResourceResult::default();
    row.name = StrBytes::from_string(name.to_string());
    row.value = Some(StrBytes::from_string(value.to_string()));
    row.config_source = 1;
    row.config_type = 2;
    row
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
