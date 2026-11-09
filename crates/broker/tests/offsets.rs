//! Committed offsets expire as Kafka's `offsets.retention.minutes` does. A
//! group that only commits (no members, no protocol) counts from each commit.

mod support;

use std::time::{Duration, Instant};

use camus_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use camus_protocol::messages::offset_commit_response::OffsetCommitResponse;
use camus_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use camus_protocol::messages::offset_fetch_response::OffsetFetchResponse;
use camus_protocol::messages::{
    ApiKey, GroupId, OffsetCommitRequest, OffsetFetchRequest, TopicName,
};
use camus_protocol::protocol::StrBytes;
use support::{connect, kafka_addr, recv, send, single, Opts, TOPIC};

fn committed(addr: &str) -> i64 {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::OffsetFetch,
        7,
        &OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("loner")))
            .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_indexes(vec![0])])),
    );
    let response = recv::<OffsetFetchResponse>(&mut stream, 7);
    response.topics[0].partitions[0].committed_offset
}

#[test]
fn a_standalone_commit_expires_after_the_retention() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(
        dir.path(),
        Opts::default()
            .env("CAMUS_OFFSETS_RETENTION_MS", "500")
            .env("CAMUS_OFFSETS_RETENTION_CHECK_INTERVAL_MS", "100"),
    );
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::OffsetCommit,
        8,
        &OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("loner")))
            .with_generation_id_or_member_epoch(-1)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(1)])]),
    );
    let response = recv::<OffsetCommitResponse>(&mut stream, 8);
    assert_eq!(response.topics[0].partitions[0].error_code, 0);
    assert_eq!(committed(&addr), 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    while committed(&addr) != -1 {
        assert!(Instant::now() < deadline, "the commit never expired");
        std::thread::sleep(Duration::from_millis(50));
    }
}
