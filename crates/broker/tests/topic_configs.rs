//! Topic configs on one broker: CreateTopics applies them, DescribeConfigs
//! reports every one with its source, the two AlterConfigs flavors keep
//! Kafka's semantics, produce honors `max.message.bytes`, and a stored
//! `min.insync.replicas` does not change the acknowledgement.

mod support;

use bytes::{Bytes, BytesMut};
use camus_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::describe_configs_request::DescribeConfigsResource;
use camus_protocol::messages::describe_configs_response::DescribeConfigsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
};
use camus_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    AlterConfigsRequest, ApiKey, CreateTopicsRequest, DescribeConfigsRequest, FetchRequest,
    IncrementalAlterConfigsRequest, MetadataRequest, ProduceRequest, TopicName,
};
use camus_protocol::protocol::StrBytes;
use camus_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use support::{connect, kafka_addr, recv, send, single, Opts};

fn topic(name: &'static str, configs: &[(&'static str, &'static str)]) -> CreatableTopic {
    let mut topic = CreatableTopic::default();
    topic.name = TopicName(StrBytes::from_static_str(name));
    topic.num_partitions = -1;
    topic.replication_factor = -1;
    topic.configs = configs
        .iter()
        .map(|(name, value)| {
            CreatableTopicConfig::default()
                .with_name(StrBytes::from_static_str(name))
                .with_value(Some(StrBytes::from_static_str(value)))
        })
        .collect();
    topic
}

fn create(addr: &str, topics: Vec<CreatableTopic>, validate_only: bool) -> CreateTopicsResponse {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        &CreateTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(5_000)
            .with_validate_only(validate_only),
    );
    recv::<CreateTopicsResponse>(&mut stream, 7)
}

/// `(name, value, source)` for every config of `name`.
fn describe(addr: &str, name: &'static str) -> Vec<(String, String, i8)> {
    let mut stream = connect(addr);
    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str(name);
    send(
        &mut stream,
        ApiKey::DescribeConfigs,
        4,
        &DescribeConfigsRequest::default().with_resources(vec![resource]),
    );
    let response = recv::<DescribeConfigsResponse>(&mut stream, 4);
    assert_eq!(response.results[0].error_code, 0);
    response.results[0]
        .configs
        .iter()
        .map(|row| {
            (
                row.name.to_string(),
                row.value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                row.config_source,
            )
        })
        .collect()
}

fn config(rows: &[(String, String, i8)], name: &str) -> (String, i8) {
    let row = rows
        .iter()
        .find(|row| row.0 == name)
        .unwrap_or_else(|| panic!("{name} missing from {rows:?}"));
    (row.1.clone(), row.2)
}

fn batch_of(size: usize, key: Option<&[u8]>) -> Bytes {
    let encode = |value_len: usize| {
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
            key: key.map(Bytes::copy_from_slice),
            value: Some(Bytes::from(vec![b'x'; value_len])),
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
    };
    // Varint lengths make the size step unevenly; search for an exact fit.
    let overhead = encode(0).len();
    let mut value_len = size.saturating_sub(overhead);
    loop {
        let batch = encode(value_len);
        if batch.len() == size {
            return batch;
        }
        assert!(
            batch.len() > size || value_len < size,
            "no exact fit for {size}"
        );
        if batch.len() > size {
            value_len -= 1;
        } else {
            value_len += 1;
        }
    }
}

fn produce(addr: &str, name: &'static str, batch: Bytes) -> (i16, Option<String>) {
    let mut stream = connect(addr);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(name));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    let response = recv::<ProduceResponse>(&mut stream, 9);
    let row = &response.responses[0].partition_responses[0];
    (
        row.error_code,
        row.error_message.as_ref().map(|m| m.to_string()),
    )
}

#[test]
fn create_topics_applies_configs_and_describe_reports_them() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);

    let created = create(
        &addr,
        vec![topic(
            "compacted",
            &[
                ("cleanup.policy", "compact"),
                ("retention.ms", "60000"),
                ("segment.bytes", "1048576"),
            ],
        )],
        false,
    );
    let result = &created.topics[0];
    assert_eq!(result.error_code, 0, "{result:?}");
    assert_eq!(result.num_partitions, 1);
    assert_eq!(result.replication_factor, 1);
    let returned = result.configs.as_ref().expect("configs in v5+");
    let policy = returned
        .iter()
        .find(|row| row.name.as_str() == "cleanup.policy")
        .unwrap();
    assert_eq!(
        policy.value.as_ref().map(|v| v.to_string()).as_deref(),
        Some("compact")
    );
    assert_eq!(policy.config_source, 1);

    let rows = describe(&addr, "compacted");
    assert_eq!(config(&rows, "cleanup.policy"), ("compact".into(), 1));
    assert_eq!(config(&rows, "retention.ms"), ("60000".into(), 1));
    assert_eq!(config(&rows, "segment.bytes"), ("1048576".into(), 1));
    assert_eq!(config(&rows, "retention.bytes"), ("-1".into(), 5));
    assert_eq!(config(&rows, "max.message.bytes"), ("1048588".into(), 5));
    assert_eq!(config(&rows, "durability.mode"), ("wal".into(), 5));
    assert_eq!(rows.len(), camus_metadata::config::SPECS.len());
}

#[test]
fn create_topics_rejects_what_kafka_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);

    let mut zero = topic("zero", &[]);
    zero.num_partitions = 0;
    let mut wide = topic("wide", &[]);
    wide.replication_factor = 5;
    let response = create(
        &addr,
        vec![
            topic("unknown", &[("no.such.config", "1")]),
            topic("logappend", &[("message.timestamp.type", "LogAppendTime")]),
            zero,
            wide,
            topic(support::TOPIC, &[]),
        ],
        false,
    );
    let codes: Vec<i16> = response.topics.iter().map(|t| t.error_code).collect();
    assert_eq!(codes, vec![40, 40, 37, 38, 36], "{response:?}");
    for result in &response.topics {
        assert!(result.error_message.is_some(), "{result:?}");
    }
    let message = response.topics[0].error_message.as_ref().unwrap();
    assert!(message.contains("no.such.config"), "{message}");

    let checked = create(
        &addr,
        vec![topic("dry", &[("cleanup.policy", "compact")])],
        true,
    );
    assert_eq!(checked.topics[0].error_code, 0);
    let again = create(&addr, vec![topic("dry", &[])], false);
    assert_eq!(
        again.topics[0].error_code, 0,
        "validate_only must not create"
    );
}

#[test]
fn legacy_alter_replaces_and_incremental_edits() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let created = create(
        &addr,
        vec![topic(
            "altered",
            &[("retention.ms", "60000"), ("segment.bytes", "1048576")],
        )],
        false,
    );
    assert_eq!(created.topics[0].error_code, 0);

    let mut stream = connect(&addr);
    let mut resource = IncrementalResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("altered");
    resource.configs = vec![IncrementalConfig::default()
        .with_name(StrBytes::from_static_str("max.message.bytes"))
        .with_config_operation(0)
        .with_value(Some(StrBytes::from_static_str("2000")))];
    send(
        &mut stream,
        ApiKey::IncrementalAlterConfigs,
        1,
        &IncrementalAlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let response = recv::<IncrementalAlterConfigsResponse>(&mut stream, 1);
    assert_eq!(response.responses[0].error_code, 0);
    let rows = describe(&addr, "altered");
    assert_eq!(config(&rows, "retention.ms"), ("60000".into(), 1));
    assert_eq!(config(&rows, "max.message.bytes"), ("2000".into(), 1));

    let mut resource = IncrementalResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("altered");
    resource.configs = vec![IncrementalConfig::default()
        .with_name(StrBytes::from_static_str("durability.mode"))
        .with_config_operation(0)
        .with_value(Some(StrBytes::from_static_str("r2")))];
    send(
        &mut stream,
        ApiKey::IncrementalAlterConfigs,
        1,
        &IncrementalAlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let response = recv::<IncrementalAlterConfigsResponse>(&mut stream, 1);
    assert_eq!(response.responses[0].error_code, 40);
    assert!(response.responses[0].error_message.is_some());

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("altered");
    resource.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        2,
        &AlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let response = recv::<AlterConfigsResponse>(&mut stream, 2);
    assert_eq!(response.responses[0].error_code, 0);
    let rows = describe(&addr, "altered");
    assert_eq!(config(&rows, "cleanup.policy"), ("compact".into(), 1));
    assert_eq!(config(&rows, "retention.ms"), ("604800000".into(), 5));
    assert_eq!(config(&rows, "segment.bytes"), ("1073741824".into(), 5));
    assert_eq!(config(&rows, "max.message.bytes"), ("1048588".into(), 5));
}

#[test]
fn produce_honors_max_message_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let created = create(
        &addr,
        vec![
            topic("small", &[("max.message.bytes", "1000")]),
            topic("default", &[]),
        ],
        false,
    );
    assert!(
        created.topics.iter().all(|t| t.error_code == 0),
        "{created:?}"
    );

    assert_eq!(produce(&addr, "small", batch_of(1000, None)).0, 0);
    let (code, message) = produce(&addr, "small", batch_of(1001, None));
    assert_eq!(code, 10);
    assert!(message.is_some());

    assert_eq!(produce(&addr, "default", batch_of(1_048_588, None)).0, 0);
    assert_eq!(produce(&addr, "default", batch_of(1_048_589, None)).0, 10);
}

/// Kafka would refuse this produce: one replica cannot satisfy
/// `min.insync.replicas=2`. Camus stores the value and still acks after the
/// WAL majority (a single voter, here).
#[test]
fn min_insync_replicas_does_not_change_the_ack() -> std::io::Result<()> {
    let dir = tempfile::tempdir().map_err(std::io::Error::other)?;
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let created = create(
        &addr,
        vec![topic("strict", &[("min.insync.replicas", "2")])],
        false,
    );
    let result = &created.topics[0];
    if result.error_code != 0 {
        return Err(std::io::Error::other(format!("create failed: {result:?}")));
    }
    let (value, note) = described(&addr, "strict", "min.insync.replicas")?;
    if value != "2" {
        return Err(std::io::Error::other(format!(
            "stored min.insync.replicas was {value}"
        )));
    }
    let note = note.unwrap_or_default();
    if !note.contains("WAL majority") {
        return Err(std::io::Error::other(format!(
            "documentation does not name the ack barrier: {note}"
        )));
    }
    let batch = batch_of(200, Some(b"k"));
    for acks in [1_i16, -1] {
        let (code, offset, message) = produce_acks(&addr, "strict", batch.clone(), acks);
        if code != 0 || offset < 0 {
            return Err(std::io::Error::other(format!(
                "acks={acks} was refused ({code}): {message:?}"
            )));
        }
    }
    let values = fetch_values(&addr, "strict")?;
    if values.len() != 2 {
        return Err(std::io::Error::other(format!(
            "expected both acks to land, got {values:?}"
        )));
    }
    Ok(())
}

fn described(
    addr: &str,
    topic_name: &'static str,
    name: &str,
) -> std::io::Result<(String, Option<String>)> {
    let mut stream = connect(addr);
    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str(topic_name);
    send(
        &mut stream,
        ApiKey::DescribeConfigs,
        4,
        &DescribeConfigsRequest::default()
            .with_resources(vec![resource])
            .with_include_documentation(true),
    );
    let response = recv::<DescribeConfigsResponse>(&mut stream, 4);
    let result = &response.results[0];
    if result.error_code != 0 {
        return Err(std::io::Error::other(format!(
            "describe failed: {result:?}"
        )));
    }
    let row = result
        .configs
        .iter()
        .find(|row| row.name.as_str() == name)
        .ok_or_else(|| std::io::Error::other(format!("missing {name}")))?;
    Ok((
        row.value
            .as_ref()
            .map(|value| value.to_string())
            .unwrap_or_default(),
        row.documentation.as_ref().map(|note| note.to_string()),
    ))
}

fn produce_acks(
    addr: &str,
    name: &'static str,
    batch: Bytes,
    acks: i16,
) -> (i16, i64, Option<String>) {
    let mut stream = connect(addr);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(name));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(acks)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    let response = recv::<ProduceResponse>(&mut stream, 9);
    let row = &response.responses[0].partition_responses[0];
    (
        row.error_code,
        row.base_offset,
        row.error_message
            .as_ref()
            .map(|message| message.to_string()),
    )
}

fn fetch_values(addr: &str, name: &'static str) -> std::io::Result<Vec<String>> {
    let mut stream = connect(addr);
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = 0;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str(name));
    topic.partitions = vec![part];
    send(
        &mut stream,
        ApiKey::Fetch,
        11,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    let fetched = recv::<FetchResponse>(&mut stream, 11);
    let row = &fetched.responses[0].partitions[0];
    if row.error_code != 0 {
        return Err(std::io::Error::other(format!(
            "fetch failed with {}",
            row.error_code
        )));
    }
    let Some(bytes) = row.records.clone() else {
        return Ok(Vec::new());
    };
    let info = camus_protocol::batch::inspect(&bytes, 1024 * 1024)
        .map_err(|err| std::io::Error::other(format!("{err:?}")))?;
    let mut values = Vec::new();
    let mut cursor = 0;
    for batch in info {
        let raw = &bytes[cursor..cursor + batch.len];
        cursor += batch.len;
        let mut buf = Bytes::copy_from_slice(raw);
        let set = RecordBatchDecoder::decode(&mut buf)
            .map_err(|err| std::io::Error::other(format!("{err:?}")))?;
        for record in set.records {
            let Some(value) = record.value else {
                continue;
            };
            let text =
                String::from_utf8(value.to_vec()).map_err(|err| std::io::Error::other(err))?;
            values.push(text);
        }
    }
    Ok(values)
}

#[test]
fn a_null_key_on_a_compacted_topic_is_an_invalid_record() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let created = create(
        &addr,
        vec![topic("keyed", &[("cleanup.policy", "compact")])],
        false,
    );
    assert_eq!(created.topics[0].error_code, 0);
    let (code, message) = produce(&addr, "keyed", batch_of(100, None));
    assert_eq!(code, 87);
    assert!(message.unwrap().contains("without key"));
    assert_eq!(produce(&addr, "keyed", batch_of(100, Some(b"k"))).0, 0);
}

#[test]
fn topic_names_follow_kafka_rules() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    assert_eq!(
        create(&addr, vec![topic("a.b", &[])], false).topics[0].error_code,
        0
    );
    let long: &'static str = Box::leak("x".repeat(250).into_boxed_str());
    let response = create(
        &addr,
        vec![
            topic("bad name", &[]),
            topic("..", &[]),
            topic(long, &[]),
            topic("a_b", &[]),
        ],
        false,
    );
    let codes: Vec<i16> = response.topics.iter().map(|t| t.error_code).collect();
    assert_eq!(codes, vec![17, 17, 17, 17], "{response:?}");
    let collision = response.topics[3].error_message.as_ref().unwrap();
    assert!(collision.contains("a.b"), "{collision}");
}

fn metadata_for(addr: &str, name: &'static str, auto: bool) -> (i16, usize) {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::Metadata,
        12,
        &MetadataRequest::default()
            .with_topics(Some(vec![MetadataRequestTopic::default()
                .with_name(Some(TopicName(StrBytes::from_static_str(name))))]))
            .with_allow_auto_topic_creation(auto),
    );
    let response = recv::<MetadataResponse>(&mut stream, 12);
    (
        response.topics[0].error_code,
        response.topics[0].partitions.len(),
    )
}

#[test]
fn metadata_creates_a_missing_topic_when_asked() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default().env("CAMUS_NUM_PARTITIONS", "3"));
    let addr = kafka_addr(&node);
    assert_eq!(metadata_for(&addr, "untouched", false), (3, 0));
    assert_eq!(
        metadata_for(&addr, "fresh", true).0,
        5,
        "LEADER_NOT_AVAILABLE first"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let (code, partitions) = metadata_for(&addr, "fresh", true);
        if code == 0 {
            assert_eq!(partitions, 3);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "never created: {code}"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(metadata_for(&addr, "bad name", true).0, 17);
}

#[test]
fn auto_create_can_be_turned_off() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(
        dir.path(),
        Opts::default().env("CAMUS_AUTO_CREATE_TOPICS", "false"),
    );
    let addr = kafka_addr(&node);
    assert_eq!(metadata_for(&addr, "fresh", true), (3, 0));
}

#[test]
fn describe_configs_reports_the_broker() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    let resource = |name: &'static str| {
        let mut resource = DescribeConfigsResource::default();
        resource.resource_type = 4;
        resource.resource_name = StrBytes::from_static_str(name);
        resource
    };
    send(
        &mut stream,
        ApiKey::DescribeConfigs,
        4,
        &DescribeConfigsRequest::default().with_resources(vec![resource("1"), resource("2")]),
    );
    let response = recv::<DescribeConfigsResponse>(&mut stream, 4);
    let mine = &response.results[0];
    assert_eq!(mine.error_code, 0);
    let max = mine
        .configs
        .iter()
        .find(|row| row.name.as_str() == "message.max.bytes")
        .expect("message.max.bytes");
    assert_eq!(
        max.value.as_ref().map(|v| v.to_string()).as_deref(),
        Some("1048588")
    );
    assert!(max.read_only);
    assert!(mine
        .configs
        .iter()
        .any(|row| row.name.as_str() == "auto.create.topics.enable"));
    assert_eq!(response.results[1].error_code, 42, "another broker's id");
}
