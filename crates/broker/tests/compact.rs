//! `cleanup.policy=compact` keeps the latest record for each key at the
//! offset it was produced at. A record with no key is corrupt.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::frame;
use camus_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    AlterConfigsRequest, ApiKey, CreateTopicsRequest, FetchRequest, ProduceRequest, RequestHeader,
    TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

struct Node {
    child: Child,
    http: String,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(compact_ms: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.keep();
    std::fs::create_dir_all(&path).unwrap();
    let log = std::fs::File::create(path.join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            path.to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .env("CAMUS_FLUSH_MS", "40")
        .env("CAMUS_COMPACT_MS", compact_ms)
        .stdout(std::fs::File::create(path.join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let http = wait_file(&path.join("listen-addr"));
    let kafka = wait_file(&path.join("kafka-addr"));
    let node = Node { child, http, kafka };
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    while Instant::now() < deadline {
        if let Ok(body) = client
            .get(format!("http://{}/admin/metrics", node.http))
            .send()
        {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1)
                    && json.get("wal_leader").and_then(|value| value.as_u64()) == Some(1)
                {
                    return node;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("node 1 did not become leader");
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

fn header(key: ApiKey, version: i16, correlation: i32) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("camus-compact")))
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

fn batch(key: Option<&[u8]>, value: &[u8]) -> Bytes {
    batch_of(key, Some(value))
}

fn tombstone(key: &[u8]) -> Bytes {
    batch_of(Some(key), None)
}

fn batch_of(key: Option<&[u8]>, value: Option<&[u8]>) -> Bytes {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: 0,
        timestamp: 9,
        key: key.map(Bytes::copy_from_slice),
        value: value.map(Bytes::copy_from_slice),
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

fn topic(stream: &mut TcpStream, durability: &'static str) {
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str("orders"));
    created.num_partitions = 1;
    created.replication_factor = 1;
    let mut mode = CreatableTopicConfig::default();
    mode.name = StrBytes::from_static_str("durability.mode");
    mode.value = Some(StrBytes::from_static_str(durability));
    created.configs = vec![mode];
    send(
        stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");
    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("orders");
    resource.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        stream,
        ApiKey::AlterConfigs,
        1,
        2,
        &AlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let altered = recv::<AlterConfigsResponse>(stream, 1);
    assert_eq!(altered.responses[0].error_code, 0, "{altered:?}");
}

fn produce(stream: &mut TcpStream, correlation: i32, raw: Bytes) -> i16 {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("orders"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(raw))];
    send(
        stream,
        ApiKey::Produce,
        9,
        correlation,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    let produced = recv::<ProduceResponse>(stream, 9);
    produced.responses[0].partition_responses[0].error_code
}

fn fetched(stream: &mut TcpStream) -> Vec<Record> {
    let mut fetch_part = FetchPartition::default();
    fetch_part.partition = 0;
    fetch_part.fetch_offset = 0;
    fetch_part.partition_max_bytes = 1024 * 1024;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_static_str("orders"));
    fetch_topic.partitions = vec![fetch_part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        8,
        &FetchRequest::default()
            .with_max_wait_ms(50)
            .with_topics(vec![fetch_topic]),
    );
    let row = recv::<FetchResponse>(stream, 11)
        .responses
        .remove(0)
        .partitions
        .remove(0);
    assert_eq!(row.error_code, 0, "{row:?}");
    let Some(mut bytes) = row.records else {
        return Vec::new();
    };
    if bytes.is_empty() {
        return Vec::new();
    }
    RecordBatchDecoder::decode_all(&mut bytes)
        .unwrap()
        .into_iter()
        .flat_map(|set| set.records)
        .collect()
}

#[test]
fn compaction_keeps_the_latest_value_at_its_offset() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping compaction test; set CAMUS_MINIO=1");
        return;
    }
    let node = spawn("100");
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    topic(&mut stream, "r2");
    assert_eq!(produce(&mut stream, 3, batch(Some(b"a"), b"1")), 0);
    assert_eq!(produce(&mut stream, 4, batch(Some(b"b"), b"1")), 0);
    assert_eq!(produce(&mut stream, 5, batch(Some(b"a"), b"2")), 0);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = Vec::new();
    while Instant::now() < deadline {
        last = fetched(&mut stream);
        if last.len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(last.len(), 2, "{last:?}");
    assert_eq!(last[0].offset, 1);
    assert_eq!(last[0].key.as_deref(), Some(b"b".as_slice()));
    assert_eq!(last[1].offset, 2);
    assert_eq!(last[1].value.as_deref(), Some(b"2".as_slice()));
}

/// Replace the topic's configs, as legacy AlterConfigs does.
fn configure(stream: &mut TcpStream, configs: &[(&'static str, &'static str)]) {
    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("orders");
    resource.configs = configs
        .iter()
        .map(|(name, value)| {
            AlterableConfig::default()
                .with_name(StrBytes::from_static_str(name))
                .with_value(Some(StrBytes::from_static_str(value)))
        })
        .collect();
    send(
        stream,
        ApiKey::AlterConfigs,
        1,
        2,
        &AlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let altered = recv::<AlterConfigsResponse>(stream, 1);
    assert_eq!(altered.responses[0].error_code, 0, "{altered:?}");
}

#[test]
fn an_expired_tombstone_is_removed_with_its_key() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping compaction test; set CAMUS_MINIO=1");
        return;
    }
    let node = spawn("100");
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    topic(&mut stream, "r2");
    // The test batches carry timestamp 9, so the default one-day
    // delete.retention.ms has long passed for this tombstone.
    assert_eq!(produce(&mut stream, 3, batch(Some(b"a"), b"1")), 0);
    assert_eq!(produce(&mut stream, 4, tombstone(b"a")), 0);
    assert_eq!(produce(&mut stream, 5, batch(Some(b"b"), b"1")), 0);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = Vec::new();
    while Instant::now() < deadline {
        last = fetched(&mut stream);
        if last.len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(last.len(), 1, "{last:?}");
    assert_eq!(last[0].key.as_deref(), Some(b"b".as_slice()));
    assert_eq!(last[0].offset, 2);
}

#[test]
fn min_compaction_lag_keeps_recent_records() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping compaction test; set CAMUS_MINIO=1");
        return;
    }
    let node = spawn("100");
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    topic(&mut stream, "r2");
    configure(
        &mut stream,
        &[
            ("cleanup.policy", "compact"),
            ("min.compaction.lag.ms", "9223372036854775807"),
        ],
    );
    assert_eq!(produce(&mut stream, 3, batch(Some(b"a"), b"1")), 0);
    assert_eq!(produce(&mut stream, 4, batch(Some(b"a"), b"2")), 0);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(fetched(&mut stream).len(), 2);
}
