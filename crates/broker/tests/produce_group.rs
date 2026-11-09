//! A produce request's partitions on one shard commit as one group: one WAL
//! entry and one quorum fsync, not one per partition (#95).

mod support;

use std::time::Duration;

use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{ApiKey, ProduceRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

const PARTITIONS: i32 = 8;

fn counter(http: &str, name: &str) -> u64 {
    admin(http).unwrap()[name].as_u64().unwrap()
}

/// Send one `acks=all` request carrying one batch for every partition.
fn produce_all(addr: &str, value: &str) -> ProduceResponse {
    let mut stream = connect(addr);
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = (0..PARTITIONS)
        .map(|index| {
            PartitionProduceData::default()
                .with_index(index)
                .with_records(Some(batch(&format!("{value}-{index}"))))
        })
        .collect();
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    recv::<ProduceResponse>(&mut stream, 9)
}

#[test]
fn a_request_for_many_partitions_on_one_shard_commits_as_one_entry() {
    let dir = tempfile::tempdir().unwrap();
    // No upload during the test, so no seal adds a WAL entry.
    let node = Node::with(
        1,
        dir.path(),
        Opts::default().env("CAMUS_FLUSH_MS", "600000"),
    );
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    let addr = kafka_addr(&node);
    create_topic_with(&addr, "wal", PARTITIONS);
    partition_leader(&addr, None);

    // The first request also waits out shard leadership settling.
    let warm = produce_all(&addr, "warm");
    for part in &warm.responses[0].partition_responses {
        assert_eq!(part.error_code, 0, "{warm:?}");
    }

    let before = counter(&node.http, "wal_log_entries");
    let produced = produce_all(&addr, "grouped");
    let after = counter(&node.http, "wal_log_entries");

    let parts = &produced.responses[0].partition_responses;
    assert_eq!(parts.len(), PARTITIONS as usize);
    for (index, part) in parts.iter().enumerate() {
        assert_eq!(part.index, index as i32);
        assert_eq!(part.error_code, 0, "{produced:?}");
        assert_eq!(
            part.base_offset, 1,
            "partition {index} after one warm batch"
        );
    }
    assert_eq!(
        after - before,
        1,
        "{PARTITIONS} partitions on one shard should share one WAL entry"
    );
}
