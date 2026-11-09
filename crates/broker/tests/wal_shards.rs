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
        let seen: Vec<(i16, i32)> = metadata.topics[0]
            .partitions
            .iter()
            .map(|partition| (partition.error_code, partition.leader_id.0))
            .collect();
        assert!(
            Instant::now() < deadline,
            "partitions without a leader (error, leader): {seen:?}, excluding {excluded:?}"
        );
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
    // A short balancer interval: if a preferred voter loses the first
    // election, the balancer hands it its shard within a second.
    let (mut nodes, meta_leader, controller) = cluster(
        root.path(),
        Opts::default().env("CAMUS_LEADER_IMBALANCE_CHECK_MS", "500"),
    );

    // One partition per shard, each led by its preferred voter: a different
    // broker each.
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

/// Three brokers, three shards, `TOPIC` with one partition per shard.
/// Partition `p` is on shard `p`, whose preferred leader is broker `p + 1`.
fn cluster(root: &std::path::Path, opts: Opts) -> (Vec<Node>, i32, String) {
    let nodes: Vec<Node> = (1..=3)
        .map(|id| Node::with(id, &root.join(format!("node-{id}")), opts.clone()))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let controller = kafka_addr(nodes.iter().find(|node| node.id == meta_leader).unwrap());
    create_topic_with(&controller, "wal", PARTITIONS);
    (nodes, meta_leader, controller)
}

/// Kill a broker that is not the metadata leader, wait until its shard is
/// led elsewhere, and bring it back. Returns its id.
fn bounce(nodes: &mut [Node], meta_leader: i32, controller: &str) -> i32 {
    let victim = nodes
        .iter()
        .map(|node| node.id)
        .find(|id| *id != meta_leader)
        .unwrap();
    nodes[(victim - 1) as usize].kill();
    let moved = (victim - 1) as usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let alive: Vec<serde_json::Value> = nodes
            .iter()
            .filter(|node| node.id != victim)
            .map(|node| admin(&node.http).unwrap_or_default()["wal_shards"].clone())
            .collect();
        let led = alive.iter().any(|shards| {
            shards[moved]["leader"]
                .as_u64()
                .is_some_and(|leader| leader != victim as u64)
        });
        if led {
            break;
        }
        assert!(Instant::now() < deadline, "shard {moved} never moved off {victim}: {alive:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
    nodes[(victim - 1) as usize].restart();
    victim
}

#[test]
fn a_returning_broker_takes_its_shard_back() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, meta_leader, controller) = cluster(
        root.path(),
        Opts::default().env("CAMUS_LEADER_IMBALANCE_CHECK_MS", "500"),
    );
    let victim = bounce(&mut nodes, meta_leader, &controller);
    let deadline = Instant::now() + Duration::from_secs(30);
    while leaders(&controller, None)[(victim - 1) as usize].0 != victim {
        assert!(Instant::now() < deadline, "broker {victim} never took its shard back");
        std::thread::sleep(Duration::from_millis(200));
    }
    write_and_read_everywhere(&controller, None, "balanced");
}

fn elect(addr: &str, election_type: i8) -> Vec<i16> {
    use camus_protocol::messages::elect_leaders_request::TopicPartitions;
    use camus_protocol::messages::elect_leaders_response::ElectLeadersResponse;
    use camus_protocol::messages::ElectLeadersRequest;
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::ElectLeaders,
        2,
        &ElectLeadersRequest::default()
            .with_election_type(election_type)
            .with_timeout_ms(5_000)
            .with_topic_partitions(Some(vec![TopicPartitions::default()
                .with_topic(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions((0..PARTITIONS).collect())])),
    );
    let response = recv::<ElectLeadersResponse>(&mut stream, 2);
    assert_eq!(response.error_code, 0);
    response.replica_election_results[0]
        .partition_result
        .iter()
        .map(|row| row.error_code)
        .collect()
}

#[test]
fn elect_leaders_moves_a_shard_back_to_its_preferred_broker() {
    let root = tempfile::tempdir().unwrap();
    let (nodes, _, controller) = cluster(
        root.path(),
        Opts::default().env("CAMUS_AUTO_LEADER_REBALANCE", "false"),
    );
    // Partition 1 is on shard 1, preferred by broker 2. Wait for broker 2
    // to lead it, then hand it to broker 3 through the leader.
    let deadline = Instant::now() + Duration::from_secs(30);
    while leaders(&controller, None)[1].0 != 2 {
        assert!(Instant::now() < deadline, "broker 2 never led shard 1");
        std::thread::sleep(Duration::from_millis(100));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let handed = reqwest::blocking::Client::new()
            .post(format!("http://{}/wal/1/hand-over", nodes[1].http))
            .json(&serde_json::json!({ "to": 3 }))
            .send()
            .unwrap();
        if handed.status().is_success() {
            break;
        }
        assert!(Instant::now() < deadline, "hand-over refused: {}", handed.status());
        std::thread::sleep(Duration::from_millis(100));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while leaders(&controller, None)[1].0 != 3 {
        assert!(Instant::now() < deadline, "shard 1 never moved to broker 3");
        std::thread::sleep(Duration::from_millis(100));
    }

    assert_eq!(elect(&controller, 1), vec![83; PARTITIONS as usize], "UNCLEAN");
    // Just after a move the new leader may not yet see broker 2 caught up,
    // and answers PREFERRED_LEADER_NOT_AVAILABLE (80) until it does.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let codes = elect(&controller, 0);
        if codes == vec![84, 0, 84] {
            break;
        }
        assert!(
            codes == vec![84, 80, 84] && Instant::now() < deadline,
            "PREFERRED answered {codes:?}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while leaders(&controller, None)[1].0 != 2 {
        assert!(Instant::now() < deadline, "the election did not move shard 1 back");
        std::thread::sleep(Duration::from_millis(100));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while elect(&controller, 0) != vec![84; PARTITIONS as usize] {
        assert!(Instant::now() < deadline, "metadata never showed the move");
        std::thread::sleep(Duration::from_millis(200));
    }
}
