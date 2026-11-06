//! Helpers shared by the multi-broker `wal` tests.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::frame;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    ApiKey, CreateTopicsRequest, FetchRequest, MetadataRequest, ProduceRequest, RequestHeader,
    TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

pub const TOPIC: &str = "orders";

pub struct Node {
    pub id: i32,
    pub child: Child,
    pub dir: PathBuf,
    pub http: String,
    s3: Option<String>,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    pub fn spawn(id: i32, dir: &Path) -> Self {
        Self::spawn_at(id, dir, "127.0.0.1:0", "127.0.0.1:0", None)
    }

    /// Spawn with object storage reached through `s3` (an `http://` URL).
    pub fn spawn_via(id: i32, dir: &Path, s3: &str) -> Self {
        Self::spawn_at(id, dir, "127.0.0.1:0", "127.0.0.1:0", Some(s3.to_string()))
    }

    /// Restart on the addresses the node had: its peers know it by them.
    pub fn restart(&mut self) {
        let http = self.http.clone();
        let kafka = kafka_addr(self);
        let dir = self.dir.clone();
        let s3 = self.s3.clone();
        *self = Self::spawn_at(self.id, &dir, &http, &kafka, s3);
    }

    fn spawn_at(id: i32, dir: &Path, http: &str, kafka: &str, s3: Option<String>) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let _ = std::fs::remove_file(dir.join("listen-addr"));
        let _ = std::fs::remove_file(dir.join("kafka-addr"));
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("broker.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_camus-broker"));
        if let Some(s3) = &s3 {
            command.env("CAMUS_S3_ENDPOINT", s3);
        }
        let child = command
            .args([
                "--node-id",
                &id.to_string(),
                "--http-addr",
                http,
                "--data-dir",
                dir.to_str().unwrap(),
                "--kafka-addr",
                kafka,
                "--dev-plaintext",
            ])
            .env("CAMUS_FLUSH_MS", "40")
            .env("RUST_LOG", "info")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let http = wait_file(&dir.join("listen-addr"));
        wait_file(&dir.join("kafka-addr"));
        Self {
            id,
            child,
            dir: dir.to_path_buf(),
            http,
            s3,
        }
    }

    pub fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

pub fn wait_file(path: &Path) -> String {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if let Ok(text) = std::fs::read_to_string(path) {
            if !text.is_empty() {
                return text;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {}", path.display());
}

pub fn admin(http: &str) -> Option<serde_json::Value> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .get(format!("http://{http}/admin/metrics"))
        .send()
        .ok()?
        .json()
        .ok()
}

/// Wait until every node agrees on the metadata leader and every shard voter
/// agrees on the shard leader. Returns the metadata leader's id.
pub fn wait_settled(nodes: &[&Node]) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let views: Vec<_> = nodes.iter().filter_map(|node| admin(&node.http)).collect();
        let meta: BTreeSet<_> = views
            .iter()
            .map(|view| view["current_leader"].as_i64())
            .collect();
        let wal: BTreeSet<_> = views
            .iter()
            .filter(|view| {
                view["wal_voters"]
                    .as_array()
                    .is_some_and(|voters| !voters.is_empty())
            })
            .map(|view| view["wal_leader"].as_i64())
            .collect();
        if views.len() == nodes.len()
            && meta.len() == 1
            && wal.len() == 1
            && !meta.contains(&None)
            && !wal.contains(&None)
        {
            return meta.into_iter().next().unwrap().unwrap() as i32;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("nodes did not agree on leaders");
}

pub fn connect(addr: &str) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
}

pub fn send<M: Encodable>(stream: &mut TcpStream, key: ApiKey, version: i16, message: &M) {
    let header = RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("camus-wal-cluster")));
    let frame = frame::encode_request(&header, message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

pub fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> M {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(size_buf) as usize];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

pub fn kafka_addr(node: &Node) -> String {
    wait_file(&node.dir.join("kafka-addr"))
}

/// Ask `via` who leads the partition, as a client would after an error.
/// Returns `host:port` of that broker once it is listed and not `exclude`.
pub fn partition_leader(via: &str, exclude: Option<i32>) -> (i32, String) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let mut stream = connect(via);
        let mut topic = MetadataRequestTopic::default();
        topic.name = Some(TopicName(StrBytes::from_static_str(TOPIC)));
        send(
            &mut stream,
            ApiKey::Metadata,
            12,
            &MetadataRequest::default().with_topics(Some(vec![topic])),
        );
        let metadata = recv::<MetadataResponse>(&mut stream, 12);
        // A node that has not applied the topic yet reports it unknown.
        let Some(partition) = metadata.topics[0].partitions.first() else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let leader = partition.leader_id.0;
        if partition.error_code == 0 && leader >= 0 && Some(leader) != exclude {
            // A restarted broker's new port reaches metadata a moment after
            // it listens; a client reconnects after a refresh, so retry.
            if let Some(broker) = metadata.brokers.iter().find(|b| b.node_id.0 == leader) {
                let addr = format!("{}:{}", broker.host.as_str(), broker.port);
                if TcpStream::connect(&addr).is_ok() {
                    return (leader, addr);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("no partition leader reported by {via}");
}

pub fn batch(value: &str) -> Bytes {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        sequence: 0,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        timestamp: 9,
        key: None,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: Default::default(),
    }];
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    buf.freeze()
}

/// Produce one batch with `acks=all`. Returns the partition error and offset.
pub fn produce(addr: &str, value: &str) -> (i16, i64) {
    let mut stream = connect(addr);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch(value)))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    let produced = recv::<ProduceResponse>(&mut stream, 9);
    let part = &produced.responses[0].partition_responses[0];
    (part.error_code, part.base_offset)
}

/// Produce as a client does: on a retriable error, refresh metadata through
/// `via` and send again. Returns the broker that accepted and the offset.
pub fn produce_retrying(via: &str, value: &str) -> (i32, i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (leader, addr) = partition_leader(via, None);
        let (code, offset) = produce(&addr, value);
        match code {
            0 => return (leader, offset),
            // LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, REQUEST_TIMED_OUT
            5..=7 if Instant::now() < deadline => {
                eprintln!("produce to {leader} got {code}; refreshing metadata");
                std::thread::sleep(Duration::from_millis(100));
            }
            other => panic!("produce {value} failed with {other}"),
        }
    }
}

/// Read the partition from offset 0 up to `end` and return each record value
/// in offset order. Single-record uncompressed batches only.
pub fn read_all(addr: &str, end: i64) -> Vec<String> {
    let mut stream = connect(addr);
    let mut values = Vec::new();
    let mut offset = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while offset < end {
        assert!(
            Instant::now() < deadline,
            "stuck at offset {offset} of {end}"
        );
        let mut part = FetchPartition::default();
        part.partition = 0;
        part.fetch_offset = offset;
        part.partition_max_bytes = 1024 * 1024;
        let mut topic = FetchTopic::default();
        topic.topic = TopicName(StrBytes::from_static_str(TOPIC));
        topic.partitions = vec![part];
        send(
            &mut stream,
            ApiKey::Fetch,
            11,
            &FetchRequest::default()
                .with_max_wait_ms(200)
                .with_topics(vec![topic]),
        );
        let fetched = recv::<FetchResponse>(&mut stream, 11);
        let row = &fetched.responses[0].partitions[0];
        assert_eq!(row.error_code, 0, "{row:?}");
        let Some(bytes) = row.records.clone() else {
            continue;
        };
        let mut cursor = 0;
        for info in camus_protocol::batch::inspect(&bytes, 1024 * 1024).unwrap() {
            let raw = &bytes[cursor..cursor + info.len];
            let base = i64::from_be_bytes(raw[0..8].try_into().unwrap());
            cursor += info.len;
            if base < offset {
                continue;
            }
            assert_eq!(base, offset, "gap or overlap in fetched batches");
            let mut buf = Bytes::copy_from_slice(raw);
            let set = camus_protocol::records::RecordBatchDecoder::decode(&mut buf).unwrap();
            for record in set.records {
                values.push(String::from_utf8(record.value.unwrap().to_vec()).unwrap());
            }
            offset += i64::from(info.records);
        }
    }
    values
}

/// Create `TOPIC` with `durability.mode=wal` through the controller.
pub fn create_wal_topic(controller: &str) {
    let mut stream = connect(controller);
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str(TOPIC));
    created.num_partitions = 1;
    created.replication_factor = 3;
    let mut durability = CreatableTopicConfig::default();
    durability.name = StrBytes::from_static_str("durability.mode");
    durability.value = Some(StrBytes::from_static_str("wal"));
    created.configs = vec![durability];
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");
}

/// Initialize the cluster through `nodes[0]`. `wal` names the shard voters.
pub fn init(nodes: &[Node], wal: &[i32]) {
    let peers: Vec<_> = nodes
        .iter()
        .map(|node| {
            serde_json::json!({"id": node.id, "addr": node.http, "wal": wal.contains(&node.id)})
        })
        .collect();
    let init = reqwest::blocking::Client::new()
        .post(format!("http://{}/admin/init", nodes[0].http))
        .json(&peers)
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
}
