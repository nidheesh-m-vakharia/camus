//! `wal` produce acks after the shard commit, then the same bytes are readable
//! after the process is killed.

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
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    ApiKey, CreateTopicsRequest, FetchRequest, ProduceRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

struct Node {
    child: Child,
    dir: PathBuf,
    http: String,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    fn spawn(dir: &Path) -> Self {
        let dir = dir.to_path_buf();
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join("listen-addr"));
        let _ = std::fs::remove_file(dir.join("kafka-addr"));
        let log = std::fs::File::create(dir.join("broker.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
            .args([
                "--node-id",
                "1",
                "--http-addr",
                "127.0.0.1:0",
                "--data-dir",
                dir.to_str().unwrap(),
                "--kafka-addr",
                "127.0.0.1:0",
                "--dev-plaintext",
            ])
            .env("CAMUS_FLUSH_MS", "40")
            .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let http = wait_file(&dir.join("listen-addr"));
        let kafka = wait_file(&dir.join("kafka-addr"));
        Self {
            child,
            dir,
            http,
            kafka,
        }
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

fn wait_leader(http: &str) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client.get(format!("http://{http}/admin/metrics")).send() {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1) {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("node 1 did not become leader");
}

fn batch(value: &[u8]) -> Bytes {
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
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::copy_from_slice(value)),
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

fn header(key: ApiKey, version: i16, correlation: i32) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("camus-wal")))
}

fn send<M: Encodable>(
    stream: &mut TcpStream,
    key: ApiKey,
    version: i16,
    correlation: i32,
    message: &M,
) {
    let frame =
        frame::encode_request(&header(key, version, correlation), message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> M {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

fn fetch_value(stream: &mut TcpStream, offset: i64) -> Bytes {
    let mut fetch_part = FetchPartition::default();
    fetch_part.partition = 0;
    fetch_part.fetch_offset = offset;
    fetch_part.partition_max_bytes = 1024 * 1024;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_static_str("events"));
    fetch_topic.partitions = vec![fetch_part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        3,
        &FetchRequest::default()
            .with_max_wait_ms(200)
            .with_topics(vec![fetch_topic]),
    );
    let fetched = recv::<FetchResponse>(stream, 11);
    fetched.responses[0].partitions[0].records.clone().unwrap()
}

#[test]
fn wal_produce_survives_restart() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping wal produce test; set CAMUS_MINIO=1");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let mut node = Node::spawn(root.path());
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);

    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str("events"));
    created.num_partitions = 1;
    created.replication_factor = 1;
    let mut durability = CreatableTopicConfig::default();
    durability.name = StrBytes::from_static_str("durability.mode");
    durability.value = Some(StrBytes::from_static_str("wal"));
    created.configs = vec![durability];
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");

    let raw = batch(b"wal");
    let covered = raw[21..].to_vec();
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("events"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(raw))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        2,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    let produced = recv::<ProduceResponse>(&mut stream, 9);
    let part = &produced.responses[0].partition_responses[0];
    assert_eq!(part.error_code, 0, "{produced:?}");
    assert_eq!(part.base_offset, 0);
    let fetched = fetch_value(&mut stream, 0);
    assert_eq!(&fetched[21..21 + covered.len()], covered.as_slice());

    node.child.kill().unwrap();
    node.child.wait().unwrap();
    node = Node::spawn(&node.dir);
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(
        init.status().is_success() || init.status().as_u16() == 409,
        "{}",
        init.text().unwrap()
    );
    wait_leader(&node.http);
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let fetched = fetch_value(&mut stream, 0);
    assert_eq!(&fetched[21..21 + covered.len()], covered.as_slice());
}
