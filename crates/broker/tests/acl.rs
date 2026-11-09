//! An ACL denial is an authorization error. A resource with no ACL stays open.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::frame;
use camus_protocol::messages::create_acls_request::AclCreation;
use camus_protocol::messages::create_acls_response::CreateAclsResponse;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::describe_acls_response::DescribeAclsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    ApiKey, CreateAclsRequest, CreateTopicsRequest, DescribeAclsRequest, FetchRequest,
    ProduceRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

struct Node {
    child: Child,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn() -> Node {
    let dir = tempfile::tempdir().unwrap().keep();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .env("CAMUS_WAL_SHARDS", "voters")
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
        .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(std::fs::File::create(dir.join("broker.log")).unwrap())
        .spawn()
        .unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{http}/admin/init"))
        .json(&serde_json::json!([{"id": 1, "addr": http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client.get(format!("http://{http}/admin/metrics")).send() {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1)
                    && json.get("wal_leader").and_then(|value| value.as_u64()) == Some(1)
                {
                    return Node { child, kafka };
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
        .with_client_id(Some(StrBytes::from_static_str("camus-acl")))
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

fn batch() -> Bytes {
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
        timestamp: 1,
        key: Some(Bytes::from_static(b"a")),
        value: Some(Bytes::from_static(b"1")),
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

fn allow(operation: i8, permission: i8) -> AclCreation {
    AclCreation::default()
        .with_resource_type(2)
        .with_resource_name(StrBytes::from_static_str("orders"))
        .with_resource_pattern_type(3)
        .with_principal(StrBytes::from_static_str("User:ANONYMOUS"))
        .with_host(StrBytes::from_static_str("*"))
        .with_operation(operation)
        .with_permission_type(permission)
}

fn produce(stream: &mut TcpStream, correlation: i32) -> i16 {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("orders"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch()))];
    send(
        stream,
        ApiKey::Produce,
        9,
        correlation,
        &ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    recv::<ProduceResponse>(stream, 9).responses[0].partition_responses[0].error_code
}

fn fetch_code(stream: &mut TcpStream, correlation: i32) -> i16 {
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = 0;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("orders"));
    topic.partitions = vec![part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        correlation,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    recv::<FetchResponse>(stream, 11).responses[0].partitions[0].error_code
}

#[test]
fn a_denied_produce_is_an_authorization_error() {
    let node = spawn();
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str("orders"));
    created.num_partitions = 1;
    created.replication_factor = 1;
    let mut mode = CreatableTopicConfig::default();
    mode.name = StrBytes::from_static_str("durability.mode");
    mode.value = Some(StrBytes::from_static_str("wal"));
    created.configs = vec![mode];
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    assert_eq!(
        recv::<CreateTopicsResponse>(&mut stream, 7).topics[0].error_code,
        0
    );
    assert_eq!(produce(&mut stream, 2), 0);

    send(
        &mut stream,
        ApiKey::CreateAcls,
        1,
        3,
        &CreateAclsRequest::default().with_creations(vec![allow(4, 3)]),
    );
    let created = recv::<CreateAclsResponse>(&mut stream, 1);
    assert_eq!(created.results[0].error_code, 0, "{created:?}");
    assert_eq!(produce(&mut stream, 4), 0);
    assert_eq!(fetch_code(&mut stream, 5), 29);

    send(
        &mut stream,
        ApiKey::DescribeAcls,
        1,
        6,
        &DescribeAclsRequest::default()
            .with_resource_type_filter(1)
            .with_resource_name_filter(None)
            .with_pattern_type_filter(1)
            .with_principal_filter(None)
            .with_host_filter(None)
            .with_operation(1)
            .with_permission_type(1),
    );
    let described = recv::<DescribeAclsResponse>(&mut stream, 1);
    assert_eq!(described.error_code, 0, "{described:?}");
    assert_eq!(described.resources.len(), 1);
    assert_eq!(described.resources[0].acls[0].operation, 4);

    send(
        &mut stream,
        ApiKey::CreateAcls,
        1,
        7,
        &CreateAclsRequest::default().with_creations(vec![allow(3, 3), allow(4, 2)]),
    );
    assert_eq!(
        recv::<CreateAclsResponse>(&mut stream, 1).results[0].error_code,
        0
    );
    assert_eq!(fetch_code(&mut stream, 8), 0);
    assert_eq!(produce(&mut stream, 9), 29);
}
