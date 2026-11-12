//! A compaction pass runs off the request threads. Produce still acks while
//! that pass is held.

mod support;

use std::time::{Duration, Instant};

use camus_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse;
use camus_protocol::messages::{AlterConfigsRequest, ApiKey};
use camus_protocol::protocol::StrBytes;

use support::*;

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        support::skipped("compaction produce test; set CAMUS_MINIO=1");
    }
    on
}

fn next_offset(http: &str) -> i64 {
    let Some(raw) = read_key(http, &format!("topic/{TOPIC}")) else {
        return -1;
    };
    let Ok(topic) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return -1;
    };
    let Some(id) = topic["id"].as_str() else {
        return -1;
    };
    let Some(raw) = read_key(http, &format!("part/{id}/0")) else {
        return -1;
    };
    let Ok(part) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return -1;
    };
    part["next_offset"].as_i64().unwrap_or(-1)
}

fn compact_policy(addr: &str) {
    let mut stream = connect(addr);
    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str(TOPIC);
    resource.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        1,
        &AlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let altered = recv::<AlterConfigsResponse>(&mut stream, 1);
    assert_eq!(altered.responses[0].error_code, 0, "{altered:?}");
}

#[test]
fn compaction_pass_does_not_block_produce() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let marker = root.path().join("compact-held");
    let node = Node::with(
        1,
        root.path(),
        Opts::default()
            .env("CAMUS_COMPACT_MS", "100")
            .env("CAMUS_FAILPOINT", "slow-compact")
            .env("CAMUS_FAILPOINT_MARKER", &marker.to_string_lossy()),
    );
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    let kafka = kafka_addr(&node);
    create_topic(&kafka, "wal");
    compact_policy(&kafka);
    assert_eq!(produce_batch(&kafka, keyed_batch("a", "1")).0, 0);
    assert_eq!(produce_batch(&kafka, keyed_batch("a", "2")).0, 0);
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if next_offset(&node.http) >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "records never sealed before compaction"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let held = Instant::now() + Duration::from_secs(8);
    while !marker.exists() {
        assert!(
            Instant::now() < held,
            "compaction never reached the held pass"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let started = Instant::now();
    let (code, _) = produce_batch(&kafka, keyed_batch("b", "3"));
    let elapsed = started.elapsed();
    assert_eq!(code, 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "produce waited on the held compaction pass: {elapsed:?}"
    );
    Ok(())
}
