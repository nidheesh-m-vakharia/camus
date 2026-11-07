//! Produce and fetch against MinIO. Skipped unless `CAMUS_MINIO=1`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::messages::create_topics_request::CreatableTopic;
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, FetchRequest,
    ProduceRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use camus_protocol::{frame, ResponseError};

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
        let bin = env!("CARGO_BIN_EXE_camus-broker");
        let child = Command::new(bin)
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
            .env("RUST_LOG", "info")
            .env("CAMUS_FLUSH_MS", "40")
            .env("CAMUS_DEFAULT_DURABILITY", "r2")
            .stdout(log.try_clone().unwrap())
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

fn batch(value: &[u8], compression: Compression, transactional: bool) -> Bytes {
    let records = vec![Record {
        transactional,
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
            compression,
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
        .with_client_id(Some(StrBytes::from_static_str("camus-test")))
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

fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> (i32, M) {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let response_header =
        camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
            .unwrap();
    let message = M::decode(&mut buf, version).unwrap();
    (response_header.correlation_id, message)
}

#[test]
fn produce_fetch_and_restart_keep_acked_bytes() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping r2 produce test; set CAMUS_MINIO=1");
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
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let (_, created) = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");

    for (compression, value) in [
        (Compression::None, &b"plain"[..]),
        (Compression::Gzip, &b"gzip"[..]),
        (Compression::Snappy, &b"snappy"[..]),
        (Compression::Lz4, &b"lz4"[..]),
        (Compression::Zstd, &b"zstd"[..]),
    ] {
        let raw = batch(value, compression, false);
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
                .with_timeout_ms(5000)
                .with_topic_data(vec![topic]),
        );
        let (_, produced) = recv::<ProduceResponse>(&mut stream, 9);
        let row = &produced.responses[0].partition_responses[0];
        assert_eq!(row.error_code, 0, "{produced:?}");
        let base = row.base_offset;
        let mut fetch_topic = FetchTopic::default();
        fetch_topic.topic = TopicName(StrBytes::from_static_str("events"));
        fetch_topic.partitions = vec![FetchPartition::default()
            .with_partition(0)
            .with_fetch_offset(base)
            .with_partition_max_bytes(1_000_000)];
        send(
            &mut stream,
            ApiKey::Fetch,
            11,
            3,
            &FetchRequest::default()
                .with_max_wait_ms(500)
                .with_topics(vec![fetch_topic]),
        );
        let (_, fetched) = recv::<FetchResponse>(&mut stream, 11);
        let records = fetched.responses[0].partitions[0]
            .records
            .clone()
            .expect("records");
        assert_eq!(&records[21..21 + covered.len()], covered.as_slice());
        assert_eq!(i64::from_be_bytes(records[0..8].try_into().unwrap()), base);
    }

    let raw = batch(b"zero", Compression::None, false);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("events"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(raw))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        4,
        &ProduceRequest::default()
            .with_acks(0)
            .with_topic_data(vec![topic]),
    );
    send(
        &mut stream,
        ApiKey::ApiVersions,
        3,
        5,
        &ApiVersionsRequest::default(),
    );
    let (correlation, _) = recv::<ApiVersionsResponse>(&mut stream, 3);
    assert_eq!(correlation, 5);

    let mut bad = batch(b"bad", Compression::None, false).to_vec();
    bad[21] ^= 0xff;
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("events"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(Bytes::from(bad)))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        6,
        &ProduceRequest::default()
            .with_acks(1)
            .with_topic_data(vec![topic]),
    );
    let (_, produced) = recv::<ProduceResponse>(&mut stream, 9);
    assert_eq!(
        produced.responses[0].partition_responses[0].error_code,
        ResponseError::CorruptMessage.code()
    );

    let txn = batch(b"txn", Compression::None, true);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("events"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(txn))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        7,
        &ProduceRequest::default()
            .with_acks(1)
            .with_topic_data(vec![topic]),
    );
    let (_, produced) = recv::<ProduceResponse>(&mut stream, 9);
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 48);

    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        8,
        &camus_protocol::messages::InitProducerIdRequest::default()
            .with_transactional_id(None),
    );
    let (_, init) = recv::<camus_protocol::messages::InitProducerIdResponse>(&mut stream, 4);
    assert_eq!(init.error_code, 0);
    assert!(init.producer_id.0 > 0);
    assert_eq!(init.producer_epoch, 0);
    drop(stream);

    node.child.kill().unwrap();
    node.child.wait().unwrap();
    let dir = node.dir.clone();
    drop(node);
    let node = Node::spawn(&dir);
    wait_leader(&node.http);
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_static_str("events"));
    fetch_topic.partitions = vec![FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(0)
        .with_partition_max_bytes(1_000_000)];
    send(
        &mut stream,
        ApiKey::Fetch,
        11,
        9,
        &FetchRequest::default().with_topics(vec![fetch_topic]),
    );
    let (_, fetched) = recv::<FetchResponse>(&mut stream, 11);
    let records = fetched.responses[0].partitions[0]
        .records
        .clone()
        .expect("restarted records");
    assert!(records.len() > 21);
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
