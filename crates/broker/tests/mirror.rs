//! Mirror links and mirror topics on one broker (ADR 0018, phase 1): links
//! are added, listed, and removed over the admin HTTP routes, and a mirror
//! topic refuses produce and every admin change but retention.

mod support;

use camus_metadata::mirror::{MirrorLink, MirrorOf};
use camus_metadata::Command;
use camus_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse;
use camus_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use camus_protocol::messages::create_partitions_response::CreatePartitionsResponse;
use camus_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use camus_protocol::messages::delete_records_response::DeleteRecordsResponse;
use camus_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
};
use camus_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    AlterConfigsRequest, ApiKey, CreatePartitionsRequest, DeleteRecordsRequest,
    IncrementalAlterConfigsRequest, ProduceRequest, TopicName,
};
use camus_protocol::protocol::StrBytes;
use reqwest::StatusCode;
use support::{batch, connect, kafka_addr, produce, recv, send, single, Opts};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MIRROR: &str = "mirrored";
const INVALID_TOPIC: i16 = 17;
const POLICY_VIOLATION: i16 = 44;

fn link(bucket: &str) -> MirrorLink {
    MirrorLink {
        alias: "us-east".into(),
        endpoint: "https://source.example".into(),
        region: "auto".into(),
        bucket: bucket.into(),
        prefix: String::new(),
        virtual_host: false,
    }
}

fn add(http: &str, link: &MirrorLink) -> Result<StatusCode, reqwest::Error> {
    Ok(reqwest::blocking::Client::new()
        .post(format!("http://{http}/admin/mirror/links"))
        .json(link)
        .send()?
        .status())
}

/// `(error code, has a message)` for one produce to the mirror.
fn produce_to_mirror(addr: &str) -> (i16, bool) {
    let mut stream = connect(addr);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(MIRROR));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch("refused")))];
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
    let part = &response.responses[0].partition_responses[0];
    (part.error_code, part.error_message.is_some())
}

#[test]
fn a_mirror_topic_is_read_only_and_holds_its_link() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let client = reqwest::blocking::Client::new();

    assert_eq!(add(&node.http, &link("logs"))?, StatusCode::OK);
    assert_eq!(add(&node.http, &link("logs"))?, StatusCode::OK, "a retry");
    assert_eq!(add(&node.http, &link("other"))?, StatusCode::CONFLICT);
    let mut bad = link("logs");
    bad.alias = "US_EAST".into();
    assert_eq!(add(&node.http, &bad)?, StatusCode::BAD_REQUEST);
    let listed: Vec<MirrorLink> = client
        .get(format!("http://{}/admin/mirror/links", node.http))
        .send()?
        .json()?;
    assert_eq!(listed, vec![link("logs")]);

    let created: camus_metadata::Response = client
        .post(format!("http://{}/admin/apply", node.http))
        .json(&Command::CreateMirrorTopic {
            name: MIRROR.into(),
            id: "00000000-0000-4000-8000-000000000018".into(),
            partitions: 2,
            of: MirrorOf {
                alias: "us-east".into(),
                topic_id: "source-topic-id".into(),
            },
        })
        .send()?
        .json()?;
    assert_eq!(created.error_code, 0);

    assert_eq!(produce_to_mirror(&addr), (INVALID_TOPIC, true));
    assert_eq!(produce(&addr, "plain topics still take produce").0, 0);

    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::CreatePartitions,
        0,
        &CreatePartitionsRequest::default()
            .with_topics(vec![CreatePartitionsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(MIRROR)))
                .with_count(3)])
            .with_timeout_ms(5_000),
    );
    let grown = recv::<CreatePartitionsResponse>(&mut stream, 0);
    assert_eq!(grown.results[0].error_code, POLICY_VIOLATION);
    assert!(grown.results[0].error_message.is_some());

    let mut retention = AlterConfigsResource::default();
    retention.resource_type = 2;
    retention.resource_name = StrBytes::from_static_str(MIRROR);
    retention.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_value(Some(StrBytes::from_static_str("1000")))];
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        2,
        &AlterConfigsRequest::default().with_resources(vec![retention]),
    );
    let altered = recv::<AlterConfigsResponse>(&mut stream, 2);
    assert_eq!(
        altered.responses[0].error_code, 0,
        "retention is the mirror's own"
    );

    let mut compact = IncrementalResource::default();
    compact.resource_type = 2;
    compact.resource_name = StrBytes::from_static_str(MIRROR);
    compact.configs = vec![IncrementalConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_config_operation(0)
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        &mut stream,
        ApiKey::IncrementalAlterConfigs,
        1,
        &IncrementalAlterConfigsRequest::default().with_resources(vec![compact]),
    );
    let refused = recv::<IncrementalAlterConfigsResponse>(&mut stream, 1);
    assert_eq!(refused.responses[0].error_code, POLICY_VIOLATION);
    assert!(refused.responses[0].error_message.is_some());

    let mut cut = DeleteRecordsTopic::default();
    cut.name = TopicName(StrBytes::from_static_str(MIRROR));
    cut.partitions = vec![DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(0)];
    send(
        &mut stream,
        ApiKey::DeleteRecords,
        2,
        &DeleteRecordsRequest::default()
            .with_topics(vec![cut])
            .with_timeout_ms(5_000),
    );
    let deleted = recv::<DeleteRecordsResponse>(&mut stream, 2);
    assert_eq!(deleted.topics[0].partitions[0].error_code, POLICY_VIOLATION);

    let removed = client
        .delete(format!("http://{}/admin/mirror/links/us-east", node.http))
        .send()?
        .status();
    assert_eq!(
        removed,
        StatusCode::CONFLICT,
        "a mirror still names the link"
    );
    Ok(())
}
