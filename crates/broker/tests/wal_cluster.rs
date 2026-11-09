//! Three brokers share one WAL shard. Killing the shard leader, and later one
//! follower, loses no acknowledged offset, and clients find the new leader
//! through Metadata (ADR 0006, ADR 0013 Phase 2).
//!
//! Object storage is optional here. With MinIO up, the uploader moves the
//! committed prefix into extents and the fetches stitch extents and WAL; with
//! it down, every byte is served from the shard.

mod support;

use support::*;

#[test]
fn wal_shard_survives_losing_the_leader_then_a_follower() {
    let root = tempfile::tempdir().unwrap();
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::spawn(id, &root.path().join(format!("node-{id}"))))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());

    // CreateTopics goes to the controller, which is the metadata leader.
    let controller = nodes.iter().find(|node| node.id == meta_leader).unwrap();
    create_wal_topic(&kafka_addr(controller));

    let (leader, leader_addr) = partition_leader(&kafka_addr(&nodes[0]), None);
    let replicas = admin(&nodes[0].http).unwrap()["wal_voters"].clone();
    assert_eq!(replicas, serde_json::json!([1, 2, 3]));
    let follower = nodes.iter().find(|node| node.id != leader).unwrap();
    // Wait until the follower has applied the topic, so the refusal below is
    // about leadership and not a metadata lag.
    partition_leader(&kafka_addr(follower), None);
    assert_eq!(
        produce(&kafka_addr(follower), "refused").0,
        6,
        "a follower answers NOT_LEADER_OR_FOLLOWER"
    );
    // And until the leader has, or its first produce finds no topic.
    partition_leader(&leader_addr, None);
    let mut acked = Vec::new();
    for index in 0..5 {
        let value = format!("v{index}");
        let (code, offset) = produce(&leader_addr, &value);
        assert_eq!(code, 0);
        assert_eq!(offset, index);
        acked.push(value);
    }

    // Kill the shard leader. The survivors elect one of themselves.
    let gone = nodes.iter().position(|node| node.id == leader).unwrap();
    nodes[gone].kill();
    let survivor = nodes.iter().find(|node| node.id != leader).unwrap();
    let (next, next_addr) = partition_leader(&kafka_addr(survivor), Some(leader));
    assert_eq!(read_all(&next_addr, 5), acked, "acked batches survive");
    let (code, offset) = produce(&next_addr, "v5");
    assert_eq!((code, offset), (0, 5));
    acked.push("v5".into());

    // The old leader rejoins from its own disk and catches up.
    nodes[gone].restart();
    wait_settled(&nodes.iter().collect::<Vec<_>>());

    // Kill one follower. Two of three voters still commit.
    let (now, _) = partition_leader(&kafka_addr(&nodes[gone]), None);
    let victim = nodes.iter().position(|node| node.id != now).unwrap();
    nodes[victim].kill();
    let via = kafka_addr(nodes.iter().find(|node| node.id == now).unwrap());
    let (now, offset) = produce_retrying(&via, "v6");
    let now_addr = kafka_addr(nodes.iter().find(|node| node.id == now).unwrap());
    assert_eq!(offset, 6, "a follower loss does not block acks");
    acked.push("v6".into());
    assert_eq!(read_all(&now_addr, 7), acked);
    assert_ne!(next, -1);
}
