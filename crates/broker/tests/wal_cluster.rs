//! Three brokers share one WAL shard. Killing the shard leader, and later one
//! follower, loses no acknowledged offset, and clients find the new leader
//! through Metadata (ADR 0006, ADR 0013 Phase 2).
//!
//! Object storage is optional here. With MinIO up, the uploader moves the
//! committed prefix into extents and the fetches stitch extents and WAL; with
//! it down, every byte is served from the shard.

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

const TOPIC: &str = "orders";

struct Node {
    id: i32,
    child: Child,
    dir: PathBuf,
    http: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    fn spawn(id: i32, dir: &Path) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let _ = std::fs::remove_file(dir.join("listen-addr"));
        let _ = std::fs::remove_file(dir.join("kafka-addr"));
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("broker.log"))
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
            .args([
                "--node-id",
                &id.to_string(),
                "--http-addr",
                "127.0.0.1:0",
                "--data-dir",
                dir.to_str().unwrap(),
                "--kafka-addr",
                "127.0.0.1:0",
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
        }
    }

    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

fn wait_file(path: &Path) -> String {
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

fn admin(http: &str) -> Option<serde_json::Value> {
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

/// Wait until every node agrees on a metadata leader and a shard leader.
/// Returns the metadata leader's id.
fn wait_settled(nodes: &[&Node]) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let views: Vec<_> = nodes.iter().filter_map(|node| admin(&node.http)).collect();
        let meta: BTreeSet<_> = views
            .iter()
            .map(|view| view["current_leader"].as_i64())
            .collect();
        let wal: BTreeSet<_> = views
            .iter()
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

fn connect(addr: &str) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
}

fn send<M: Encodable>(stream: &mut TcpStream, key: ApiKey, version: i16, message: &M) {
    let header = RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("camus-wal-cluster")));
    let frame = frame::encode_request(&header, message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> M {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(size_buf) as usize];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

fn kafka_addr(node: &Node) -> String {
    wait_file(&node.dir.join("kafka-addr"))
}

/// Ask `via` who leads the partition, as a client would after an error.
/// Returns `host:port` of that broker once it is listed and not `exclude`.
fn partition_leader(via: &str, exclude: Option<i32>) -> (i32, String) {
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
            assert_eq!(partition.replica_nodes.len(), 3, "{partition:?}");
            if let Some(broker) = metadata.brokers.iter().find(|b| b.node_id.0 == leader) {
                return (leader, format!("{}:{}", broker.host.as_str(), broker.port));
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("no partition leader reported by {via}");
}

fn batch(value: &str) -> Bytes {
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
fn produce(addr: &str, value: &str) -> (i16, i64) {
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

/// Read the partition from offset 0 up to `end` and return each record value
/// in offset order. Single-record uncompressed batches only.
fn read_all(addr: &str, end: i64) -> Vec<String> {
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

#[test]
fn wal_shard_survives_losing_the_leader_then_a_follower() {
    let root = tempfile::tempdir().unwrap();
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::spawn(id, &root.path().join(format!("node-{id}"))))
        .collect();
    let peers: Vec<_> = nodes
        .iter()
        .map(|node| serde_json::json!({"id": node.id, "addr": node.http}))
        .collect();
    let init = reqwest::blocking::Client::new()
        .post(format!("http://{}/admin/init", nodes[0].http))
        .json(&peers)
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());

    // CreateTopics goes to the controller, which is the metadata leader.
    let controller = nodes.iter().find(|node| node.id == meta_leader).unwrap();
    let mut stream = connect(&kafka_addr(controller));
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

    let (leader, leader_addr) = partition_leader(&kafka_addr(&nodes[0]), None);
    let follower = nodes.iter().find(|node| node.id != leader).unwrap();
    // Wait until the follower has applied the topic, so the refusal below is
    // about leadership and not a metadata lag.
    partition_leader(&kafka_addr(follower), None);
    assert_eq!(
        produce(&kafka_addr(follower), "refused").0,
        6,
        "a follower answers NOT_LEADER_OR_FOLLOWER"
    );
    let mut acked = Vec::new();
    for index in 0..5 {
        let value = format!("v{index}");
        let (code, offset) = produce(&leader_addr, &value);
        assert_eq!(code, 0);
        assert_eq!(offset, index);
        acked.push(value);
    }

    // Kill the shard leader. The survivors elect one of themselves.
    let gone = nodes.iter().position(|node| node.id == leader).unwrap();
    nodes[gone].kill();
    let survivor = nodes.iter().find(|node| node.id != leader).unwrap();
    let (next, next_addr) = partition_leader(&kafka_addr(survivor), Some(leader));
    assert_eq!(read_all(&next_addr, 5), acked, "acked batches survive");
    let (code, offset) = produce(&next_addr, "v5");
    assert_eq!((code, offset), (0, 5));
    acked.push("v5".into());

    // The old leader rejoins from its own disk and catches up.
    let dir = nodes[gone].dir.clone();
    nodes[gone] = Node::spawn(leader, &dir);
    wait_settled(&nodes.iter().collect::<Vec<_>>());

    // Kill one follower. Two of three voters still commit.
    let (now, now_addr) = partition_leader(&kafka_addr(&nodes[gone]), None);
    let victim = nodes.iter().position(|node| node.id != now).unwrap();
    nodes[victim].kill();
    let (code, offset) = produce(&now_addr, "v6");
    assert_eq!(
        (code, offset),
        (0, 6),
        "a follower loss does not block acks"
    );
    acked.push("v6".into());
    assert_eq!(read_all(&now_addr, 7), acked);
    assert_ne!(next, -1);
}
