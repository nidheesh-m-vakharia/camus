//! ADR 0014 step 1: one shard, with shard ids in place. Partitions record
//! their shard, and WAL RPC has a route per shard beside shard 0's old one.

mod support;

use support::{read_key, single, Opts, TOPIC};

#[test]
fn a_single_shard_cluster_places_partitions_on_shard_0_and_routes_by_id() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(&node.http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let id = topic["id"].as_str().unwrap();
    let partition: serde_json::Value =
        serde_json::from_str(&read_key(&node.http, &format!("part/{id}/0")).unwrap()).unwrap();
    assert_eq!(partition["shard"].as_u64(), Some(0));
    assert!(read_key(&node.http, "wal/next-shard").is_none());

    let http = reqwest::blocking::Client::new();
    let post = |path: &str| {
        http.post(format!("http://{}{path}", node.http))
            .body("not a raft message")
            .send()
            .unwrap()
            .status()
            .as_u16()
    };
    assert_eq!(post("/wal/0/raft-vote"), 400, "shard 0 is open");
    assert_eq!(
        post("/wal/raft-vote"),
        400,
        "the old route still reaches shard 0"
    );
    assert_eq!(post("/wal/7/raft-vote"), 404, "no such shard");
}

#[test]
fn init_with_a_shard_count_opens_every_shard() {
    let dir = tempfile::tempdir().unwrap();
    let node = support::Node::with(1, dir.path(), Opts::default());
    let init = reqwest::blocking::Client::new()
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!({
            "peers": [{"id": 1, "addr": node.http, "wal": true}],
            "shards": 2,
        }))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    support::wait_settled(&[&node]);
    let addr = support::kafka_addr(&node);
    support::create_topic_with(&addr, "wal", 2);
    let layout: serde_json::Value =
        serde_json::from_str(&read_key(&node.http, "wal/shards").unwrap()).unwrap();
    assert_eq!(layout["count"].as_u64(), Some(2));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let shards = support::admin(&node.http).unwrap()["wal_shards"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|shard| shard["leader"].as_u64() == Some(1))
            .count();
        if shards == 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "shard 1 never led");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
