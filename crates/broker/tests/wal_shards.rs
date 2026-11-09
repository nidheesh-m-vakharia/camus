//! ADR 0014 step 2: three brokers make three WAL shards, one led by each,
//! with a topic's partitions spread over them. Losing a broker moves only
//! its shard; every partition stays writable and readable.

mod support;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use bytes::Bytes;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{ApiKey, FetchRequest, MetadataRequest, ProduceRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

const PARTITIONS: i32 = 3;

/// `(leader id, leader address)` of each partition, from `via`'s Metadata,
/// once every partition has a reachable leader that is not `excluded`.
fn leaders(via: &str, excluded: Option<i32>) -> Vec<(i32, String)> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut stream = connect(via);
        send(
            &mut stream,
            ApiKey::Metadata,
            12,
            &MetadataRequest::default().with_topics(Some(vec![MetadataRequestTopic::default()
                .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC))))])),
        );
        let metadata = recv::<MetadataResponse>(&mut stream, 12);
        let found: Vec<(i32, String)> = metadata.topics[0]
            .partitions
            .iter()
            .filter(|partition| partition.error_code == 0)
            .filter_map(|partition| {
                let id = partition.leader_id.0;
                if Some(id) == excluded {
                    return None;
                }
                let broker = metadata.brokers.iter().find(|b| b.node_id.0 == id)?;
                Some((id, format!("{}:{}", broker.host.as_str(), broker.port)))
            })
            .collect();
        if found.len() == PARTITIONS as usize {
            return found;
        }
        assert!(Instant::now() < deadline, "partitions without a leader");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn produce_to(addr: &str, partition: i32, value: &str) -> i16 {
    let mut stream = connect(addr);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(batch(value)))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    recv::<ProduceResponse>(&mut stream, 9).responses[0].partition_responses[0].error_code
}

fn has_records(addr: &str, partition: i32) -> bool {
    let mut stream = connect(addr);
    let mut part = FetchPartition::default();
    part.partition = partition;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partitions = vec![part];
    send(
        &mut stream,
        ApiKey::Fetch,
        11,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    let response = recv::<FetchResponse>(&mut stream, 11);
    let row = &response.responses[0].partitions[0];
    row.error_code == 0
        && row
            .records
            .as_ref()
            .is_some_and(|bytes: &Bytes| !bytes.is_empty())
}

/// Produce to every partition through its leader, retrying while
/// leadership settles, then read each one back.
fn write_and_read_everywhere(via: &str, excluded: Option<i32>, value: &str) {
    for partition in 0..PARTITIONS {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (_, addr) = leaders(via, excluded)[partition as usize].clone();
            if produce_to(&addr, partition, value) == 0 && has_records(&addr, partition) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "partition {partition} never accepted {value}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[test]
fn three_brokers_lead_three_shards_and_survive_losing_one() {
    let root = tempfile::tempdir().unwrap();
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::spawn(id, &root.path().join(format!("node-{id}"))))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let controller = kafka_addr(nodes.iter().find(|node| node.id == meta_leader).unwrap());
    create_topic_with(&controller, "wal", PARTITIONS);

    // One partition per shard, and each shard first led by its preferred
    // voter: a different broker each.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let distinct: BTreeSet<i32> = leaders(&controller, None)
            .iter()
            .map(|(id, _)| *id)
            .collect();
        if distinct.len() == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "leaders never spread: {distinct:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    write_and_read_everywhere(&controller, None, "before");

    // Each shard's leader uploads its own partitions (one object per shard).
    if std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1") {
        let http = nodes[0].http.clone();
        let topic: serde_json::Value =
            serde_json::from_str(&read_key(&http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
        let id = topic["id"].as_str().unwrap().to_string();
        let deadline = Instant::now() + Duration::from_secs(20);
        for partition in 0..PARTITIONS {
            loop {
                let row: serde_json::Value = serde_json::from_str(
                    &read_key(&http, &format!("part/{id}/{partition}")).unwrap(),
                )
                .unwrap();
                assert_eq!(row["shard"].as_i64(), Some(i64::from(partition)));
                if row["next_offset"].as_i64().unwrap_or(0) >= 1 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "partition {partition} never sealed"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    // Kill a broker that is not the metadata leader. Its shard elects among
    // the two survivors; the other shards keep their leaders.
    let victim = nodes
        .iter()
        .map(|node| node.id)
        .find(|id| *id != meta_leader)
        .unwrap();
    let survivors_before: Vec<(i32, String)> = leaders(&controller, None);
    nodes[(victim - 1) as usize].kill();
    write_and_read_everywhere(&controller, Some(victim), "after");
    let survivors_after = leaders(&controller, Some(victim));
    for (partition, (before, _)) in survivors_before.iter().enumerate() {
        if *before != victim {
            assert_eq!(
                survivors_after[partition].0, *before,
                "partition {partition}'s shard should keep its leader"
            );
        }
    }
}
