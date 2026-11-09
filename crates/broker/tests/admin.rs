//! Phase 4 admin APIs on one broker: describe the cluster, grow a topic,
//! change retention, trim the log at an object boundary, and delete offsets.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::Bytes;
use camus_protocol::frame;
use camus_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse;
use camus_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use camus_protocol::messages::create_partitions_response::CreatePartitionsResponse;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use camus_protocol::messages::delete_records_response::DeleteRecordsResponse;
use camus_protocol::messages::describe_cluster_response::DescribeClusterResponse;
use camus_protocol::messages::describe_configs_request::DescribeConfigsResource;
use camus_protocol::messages::describe_configs_response::DescribeConfigsResponse;
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::offset_delete_request::{
    OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
};
use camus_protocol::messages::offset_delete_response::OffsetDeleteResponse;
use camus_protocol::messages::{
    AlterConfigsRequest, ApiKey, CreatePartitionsRequest, CreateTopicsRequest,
    DeleteRecordsRequest, DescribeClusterRequest, DescribeConfigsRequest, GroupId, MetadataRequest,
    OffsetDeleteRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

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
        .with_client_id(Some(StrBytes::from_static_str("camus-admin")))
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

#[test]
fn admin_apis_change_the_topic_and_refuse_a_past_the_end_delete() {
    let dir = tempfile::tempdir().unwrap();
    let log = std::fs::File::create(dir.path().join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .env("CAMUS_WAL_SHARDS", "voters")
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .stdout(std::fs::File::create(dir.path().join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let http = wait_file(&dir.path().join("listen-addr"));
    let kafka = wait_file(&dir.path().join("kafka-addr"));
    let node = Node { child, http, kafka };
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    let start = Instant::now();
    loop {
        let body = client
            .get(format!("http://{}/admin/metrics", node.http))
            .send()
            .unwrap();
        let json = body.json::<serde_json::Value>().unwrap_or_default();
        if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1)
            && json.get("wal_leader").and_then(|value| value.as_u64()) == Some(1)
        {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(10), "no leader");
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    send(
        &mut stream,
        ApiKey::DescribeCluster,
        0,
        1,
        &DescribeClusterRequest::default(),
    );
    let cluster = recv::<DescribeClusterResponse>(&mut stream, 0);
    assert_eq!(cluster.error_code, 0);
    assert_eq!(cluster.cluster_id.as_str(), "camus");
    assert_eq!(cluster.controller_id.0, 1);

    let mut topic = CreatableTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("events"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    topic.configs = vec![CreatableTopicConfig::default()
        .with_name(StrBytes::from_static_str("durability.mode"))
        .with_value(Some(StrBytes::from_static_str("r2")))];
    send(
        &mut stream,
        ApiKey::CreateTopics,
        2,
        2,
        &CreateTopicsRequest::default().with_topics(vec![topic]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 2);
    assert_eq!(created.topics[0].error_code, 0);

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("events");
    resource.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_value(Some(StrBytes::from_static_str("1000")))];
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        1,
        3,
        &AlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let altered = recv::<AlterConfigsResponse>(&mut stream, 1);
    assert_eq!(altered.responses[0].error_code, 0);

    let mut described = DescribeConfigsResource::default();
    described.resource_type = 2;
    described.resource_name = StrBytes::from_static_str("events");
    send(
        &mut stream,
        ApiKey::DescribeConfigs,
        1,
        4,
        &DescribeConfigsRequest::default().with_resources(vec![described]),
    );
    let configs = recv::<DescribeConfigsResponse>(&mut stream, 1);
    let retention = configs.results[0]
        .configs
        .iter()
        .find(|row| row.name.as_str() == "retention.ms")
        .unwrap();
    assert_eq!(retention.value.as_ref().unwrap().as_str(), "1000");

    let mut compact = AlterConfigsResource::default();
    compact.resource_type = 2;
    compact.resource_name = StrBytes::from_static_str("events");
    compact.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        1,
        5,
        &AlterConfigsRequest::default().with_resources(vec![compact]),
    );
    let accepted = recv::<AlterConfigsResponse>(&mut stream, 1);
    assert_eq!(accepted.responses[0].error_code, 0);

    send(
        &mut stream,
        ApiKey::CreatePartitions,
        0,
        6,
        &CreatePartitionsRequest::default().with_topics(vec![CreatePartitionsTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("events")))
            .with_count(3)]),
    );
    let grown = recv::<CreatePartitionsResponse>(&mut stream, 0);
    assert_eq!(grown.results[0].error_code, 0);

    send(
        &mut stream,
        ApiKey::Metadata,
        1,
        7,
        &MetadataRequest::default().with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_static_str("events"))))])),
    );
    let metadata = recv::<MetadataResponse>(&mut stream, 1);
    let events = metadata
        .topics
        .iter()
        .find(|topic| {
            topic
                .name
                .as_ref()
                .is_some_and(|name| name.0.as_str() == "events")
        })
        .unwrap();
    assert_eq!(events.partitions.len(), 3);

    let mut cut = DeleteRecordsTopic::default();
    cut.name = TopicName(StrBytes::from_static_str("events"));
    cut.partitions = vec![DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(0)];
    send(
        &mut stream,
        ApiKey::DeleteRecords,
        1,
        8,
        &DeleteRecordsRequest::default().with_topics(vec![cut]),
    );
    let deleted = recv::<DeleteRecordsResponse>(&mut stream, 1);
    assert_eq!(deleted.topics[0].partitions[0].error_code, 0);
    assert_eq!(deleted.topics[0].partitions[0].low_watermark, 0);

    let mut past = DeleteRecordsTopic::default();
    past.name = TopicName(StrBytes::from_static_str("events"));
    past.partitions = vec![DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(1)];
    send(
        &mut stream,
        ApiKey::DeleteRecords,
        1,
        9,
        &DeleteRecordsRequest::default().with_topics(vec![past]),
    );
    let missed = recv::<DeleteRecordsResponse>(&mut stream, 1);
    assert_eq!(missed.topics[0].partitions[0].error_code, 1);

    send(
        &mut stream,
        ApiKey::OffsetDelete,
        0,
        10,
        &OffsetDeleteRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("missing")))
            .with_topics(vec![OffsetDeleteRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("events")))
                .with_partitions(vec![
                    OffsetDeleteRequestPartition::default().with_partition_index(0)
                ])]),
    );
    let offsets = recv::<OffsetDeleteResponse>(&mut stream, 0);
    assert_eq!(offsets.error_code, 69);
}
