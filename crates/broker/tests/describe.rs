//! The describe APIs operations tools call: ConsumerGroupDescribe,
//! DescribeTopicPartitions, DescribeLogDirs, and DescribeProducers.

mod support;

use std::time::{Duration, Instant};

use camus_protocol::messages::consumer_group_describe_response::ConsumerGroupDescribeResponse;
use camus_protocol::messages::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use camus_protocol::messages::create_topics_request::CreatableTopic;
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::describe_log_dirs_response::DescribeLogDirsResponse;
use camus_protocol::messages::describe_producers_request::TopicRequest as ProducersTopic;
use camus_protocol::messages::describe_producers_response::DescribeProducersResponse;
use camus_protocol::messages::describe_topic_partitions_request::{Cursor, TopicRequest};
use camus_protocol::messages::describe_topic_partitions_response::DescribeTopicPartitionsResponse;
use camus_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    ApiKey, ConsumerGroupDescribeRequest, ConsumerGroupHeartbeatRequest, CreateTopicsRequest,
    DescribeLogDirsRequest, DescribeProducersRequest, DescribeTopicPartitionsRequest, GroupId,
    InitProducerIdRequest, ProduceRequest, TopicName,
};
use camus_protocol::protocol::StrBytes;
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use support::{connect, kafka_addr, recv, send, single, wait_sealed, Opts, TOPIC};

fn name(value: &'static str) -> TopicName {
    TopicName(StrBytes::from_static_str(value))
}

#[test]
fn consumer_group_describe_reports_a_kip_848_member() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::ConsumerGroupHeartbeat,
        1,
        &ConsumerGroupHeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("readers")))
            .with_member_id(StrBytes::from_static_str("member-a"))
            .with_member_epoch(0)
            .with_rebalance_timeout_ms(30_000)
            .with_subscribed_topic_names(Some(vec![name(TOPIC)])),
    );
    let joined = recv::<ConsumerGroupHeartbeatResponse>(&mut stream, 1);
    assert_eq!(joined.error_code, 0, "{joined:?}");

    send(
        &mut stream,
        ApiKey::ConsumerGroupDescribe,
        1,
        &ConsumerGroupDescribeRequest::default().with_group_ids(vec![
            GroupId(StrBytes::from_static_str("readers")),
            GroupId(StrBytes::from_static_str("nobody")),
        ]),
    );
    let described = recv::<ConsumerGroupDescribeResponse>(&mut stream, 1);
    let group = &described.groups[0];
    assert_eq!(group.error_code, 0, "{group:?}");
    assert_eq!(group.assignor_name.as_str(), "uniform");
    assert!(group.group_epoch >= 1);
    let member = &group.members[0];
    assert_eq!(member.member_id.as_str(), "member-a");
    assert_eq!(member.client_host.as_str(), "/127.0.0.1");
    assert_eq!(member.subscribed_topic_names[0].0.as_str(), TOPIC);
    let target = &member.target_assignment.topic_partitions[0];
    assert_eq!(target.topic_name.0.as_str(), TOPIC);
    assert_eq!(target.partitions, vec![0]);
    assert_eq!(described.groups[1].error_code, 69);
}

#[test]
fn describe_topic_partitions_pages_with_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    let mut wide = CreatableTopic::default();
    wide.name = name("wide");
    wide.num_partitions = 5;
    wide.replication_factor = -1;
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        &CreateTopicsRequest::default().with_topics(vec![wide]),
    );
    assert_eq!(
        recv::<CreateTopicsResponse>(&mut stream, 7).topics[0].error_code,
        0
    );

    let page = |cursor: Option<Cursor>| {
        let mut stream = connect(&addr);
        send(
            &mut stream,
            ApiKey::DescribeTopicPartitions,
            0,
            &DescribeTopicPartitionsRequest::default()
                .with_topics(vec![TopicRequest::default().with_name(name("wide"))])
                .with_response_partition_limit(3)
                .with_cursor(cursor),
        );
        recv::<DescribeTopicPartitionsResponse>(&mut stream, 0)
    };
    let first = page(None);
    let indexes: Vec<i32> = first.topics[0]
        .partitions
        .iter()
        .map(|partition| partition.partition_index)
        .collect();
    assert_eq!(indexes, vec![0, 1, 2]);
    let cursor = first.next_cursor.expect("more partitions");
    assert_eq!(cursor.partition_index, 3);
    let second = page(Some(
        Cursor::default()
            .with_topic_name(cursor.topic_name.clone())
            .with_partition_index(cursor.partition_index),
    ));
    let indexes: Vec<i32> = second.topics[0]
        .partitions
        .iter()
        .map(|partition| partition.partition_index)
        .collect();
    assert_eq!(indexes, vec![3, 4]);
    assert!(second.next_cursor.is_none());
}

#[test]
fn describe_log_dirs_names_the_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::DescribeLogDirs,
        4,
        &DescribeLogDirsRequest::default().with_topics(None),
    );
    let response = recv::<DescribeLogDirsResponse>(&mut stream, 4);
    assert_eq!(response.error_code, 0);
    let result = &response.results[0];
    assert!(result.log_dir.as_str().starts_with("s3://"), "{result:?}");
    assert!(result
        .topics
        .iter()
        .any(|topic| topic.name.0.as_str() == TOPIC));
}

fn idempotent_batch(producer_id: i64) -> bytes::Bytes {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id,
        producer_epoch: 0,
        sequence: 0,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        timestamp: 9,
        key: None,
        value: Some(bytes::Bytes::from_static(b"v")),
        headers: Default::default(),
    }];
    let mut buf = bytes::BytesMut::new();
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

#[test]
fn describe_producers_lists_a_sealed_idempotent_producer() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping DescribeProducers test; set CAMUS_MINIO=1");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        &InitProducerIdRequest::default().with_transactional_id(None),
    );
    let producer = recv::<InitProducerIdResponse>(&mut stream, 4);
    assert_eq!(producer.error_code, 0);
    let id = producer.producer_id.0;
    let mut topic = TopicProduceData::default();
    topic.name = name(TOPIC);
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(idempotent_batch(id)))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    assert_eq!(
        recv::<ProduceResponse>(&mut stream, 9).responses[0].partition_responses[0].error_code,
        0
    );
    wait_sealed(&node.http, 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        send(
            &mut stream,
            ApiKey::DescribeProducers,
            0,
            &DescribeProducersRequest::default().with_topics(vec![ProducersTopic::default()
                .with_name(name(TOPIC))
                .with_partition_indexes(vec![0])]),
        );
        let response = recv::<DescribeProducersResponse>(&mut stream, 0);
        let partition = &response.topics[0].partitions[0];
        assert_eq!(partition.error_code, 0);
        if let Some(found) = partition.active_producers.first() {
            assert_eq!(found.producer_id.0, id);
            assert_eq!(found.last_sequence, 0);
            assert_eq!(found.current_txn_start_offset, -1);
            break;
        }
        assert!(Instant::now() < deadline, "the producer never appeared");
        std::thread::sleep(Duration::from_millis(100));
    }
}
