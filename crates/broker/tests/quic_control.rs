//! Raft votes and empty heartbeats over QUIC, with HTTP refused (ADR 0016).

mod support;

use support::*;

#[test]
fn an_election_completes_when_http_raft_control_is_blocked() -> Result<(), String> {
    let root = tempfile::tempdir().map_err(|err| err.to_string())?;
    let opts = Opts::default()
        .env("CAMUS_QUIC", "on")
        .env("CAMUS_BLOCK_RAFT_HTTP", "1");
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::with(id, &root.path().join(format!("node-{id}")), opts.clone()))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let controller = nodes
        .iter()
        .find(|node| node.id == meta)
        .ok_or("no metadata leader")?;
    create_wal_topic(&kafka_addr(controller));
    let (leader, leader_addr) = partition_leader(&kafka_addr(&nodes[0]), None);
    let (code, offset) = produce(&leader_addr, "v0");
    if (code, offset) != (0, 0) {
        return Err(format!("first produce {code} {offset}"));
    }
    let gone = nodes
        .iter()
        .position(|node| node.id == leader)
        .ok_or("leader")?;
    nodes[gone].kill();
    let survivor = nodes
        .iter()
        .find(|node| node.id != leader)
        .ok_or("survivor")?;
    let (next, next_addr) = partition_leader(&kafka_addr(survivor), Some(leader));
    let (code, offset) = produce(&next_addr, "v1");
    if (code, offset) != (0, 1) {
        return Err(format!("produce after election {code} {offset}"));
    }
    if next == leader {
        return Err("leader did not move".into());
    }
    Ok(())
}
