//! One shard's seal does not stop the others, and an `r2` produce with no
//! metadata quorum is `REQUEST_TIMED_OUT` at `timeout_ms`.

mod support;

use std::time::{Duration, Instant};

use support::*;

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        support::skipped("upload isolation test; set CAMUS_MINIO=1");
    }
    on
}

fn next_offset(http: &str, partition: i32) -> i64 {
    let Some(raw) = read_key(http, &format!("topic/{TOPIC}")) else {
        return -1;
    };
    let Ok(topic) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return -1;
    };
    let Some(id) = topic["id"].as_str() else {
        return -1;
    };
    let Some(raw) = read_key(http, &format!("part/{id}/{partition}")) else {
        return -1;
    };
    let Ok(part) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return -1;
    };
    part["next_offset"].as_i64().unwrap_or(-1)
}

#[test]
fn a_stuck_shard_does_not_stop_the_other_from_sealing() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let node = Node::with(
        1,
        root.path(),
        Opts::default()
            .env("CAMUS_WAL_SHARDS", "2")
            .env("CAMUS_FAILPOINT", "slow-seal:0"),
    );
    init_shards(std::slice::from_ref(&node), &[1], 2);
    wait_settled(&[&node]);
    let kafka = kafka_addr(&node);
    create_topic_with(&kafka, "wal", 2);
    assert_eq!(produce_partition(&kafka, 0, "stuck").0, 0);
    assert_eq!(produce_partition(&kafka, 1, "moving").0, 0);

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if next_offset(&node.http, 1) == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "shard 1 never sealed while shard 0's seal was held"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        next_offset(&node.http, 0),
        0,
        "shard 0 sealed while its seal was held"
    );
    Ok(())
}

#[test]
fn r2_produce_without_metadata_quorum_times_out() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::with(id, &root.path().join(format!("node-{id}")), Opts::default()))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let Some(survivor) = nodes.iter().find(|node| node.id == leader) else {
        panic!("metadata leader {leader} is not in the cluster");
    };
    let kafka = kafka_addr(survivor);
    create_topic(&kafka, "r2");
    for node in nodes.iter_mut() {
        if node.id != leader {
            node.kill();
        }
    }
    let started = Instant::now();
    let (code, _, _) = produce_full(&kafka, "held", 800);
    let elapsed = started.elapsed();
    assert_eq!(code, 7, "no metadata quorum is REQUEST_TIMED_OUT");
    assert!(
        elapsed >= Duration::from_millis(700),
        "answered before timeout_ms: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "waited past timeout_ms: {elapsed:?}"
    );
    Ok(())
}
