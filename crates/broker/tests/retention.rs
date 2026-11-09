//! `retention.ms` moves the log start past expired sealed extents, and the
//! orphan sweep then deletes the objects nothing references. Needs MinIO
//! (`CAMUS_MINIO=1`). Each test uses its own key prefix, so the sweep cannot
//! touch objects from tests running beside it.

mod support;

use std::time::{Duration, Instant};

use camus_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use camus_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use camus_protocol::messages::{ApiKey, IncrementalAlterConfigsRequest};
use camus_protocol::protocol::StrBytes;
use support::{
    connect, fetch_code, kafka_addr, produce, read_key, recv, send, single, wait_sealed, Node,
    Opts, TOPIC,
};

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        eprintln!("skipping retention test; set CAMUS_MINIO=1");
    }
    on
}

fn start(dir: &std::path::Path) -> Node {
    let prefix = format!(
        "retention-{}-{}",
        std::process::id(),
        dir.file_name().unwrap().to_string_lossy()
    );
    single(
        dir,
        Opts::default()
            .env("CAMUS_S3_PREFIX", &prefix)
            .env("CAMUS_RETENTION_CHECK_MS", "100")
            .env("CAMUS_SWEEP_MS", "100")
            // No minimum age, so the test need not wait an hour. The delete
            // delay then has to outlast an upload's metadata commit, or a
            // fresh object would be swept before it is referenced.
            .env("CAMUS_SWEEP_MIN_AGE_MS", "0")
            .env("CAMUS_FILE_DELETE_DELAY_MS", "2000"),
    )
}

fn set_retention(addr: &str, value: &'static str) {
    let mut stream = connect(addr);
    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str(TOPIC);
    resource.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_config_operation(0)
        .with_value(Some(StrBytes::from_static_str(value)))];
    send(
        &mut stream,
        ApiKey::IncrementalAlterConfigs,
        1,
        &IncrementalAlterConfigsRequest::default().with_resources(vec![resource]),
    );
    let response = recv::<IncrementalAlterConfigsResponse>(&mut stream, 1);
    assert_eq!(response.responses[0].error_code, 0);
}

fn log_start(http: &str) -> i64 {
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let id = topic["id"].as_str().unwrap();
    let part: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("part/{id}/0")).unwrap()).unwrap();
    part["log_start"].as_i64().unwrap()
}

fn metric(http: &str, name: &str) -> u64 {
    support::admin(http)
        .and_then(|json| json[name].as_u64())
        .unwrap_or(0)
}

#[test]
fn expired_extents_are_cut_and_their_objects_deleted() {
    if !minio() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path());
    let addr = kafka_addr(&node);
    // Keep everything until the batches are sealed, then expire them. The
    // test batches carry timestamp 9, so any finite retention covers them.
    set_retention(&addr, "-1");
    for value in ["a", "b", "c"] {
        assert_eq!(produce(&addr, value).0, 0);
    }
    wait_sealed(&node.http, 3);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(log_start(&node.http), 0, "retention.ms=-1 must keep data");
    assert_eq!(fetch_code(&addr, 0), 0);

    set_retention(&addr, "60000");
    let deadline = Instant::now() + Duration::from_secs(15);
    while log_start(&node.http) < 3 {
        assert!(
            Instant::now() < deadline,
            "retention never moved the log start"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(fetch_code(&addr, 0), 1, "an expired offset is out of range");
    assert!(metric(&node.http, "retention_cuts") >= 1);

    let deadline = Instant::now() + Duration::from_secs(15);
    while metric(&node.http, "sweep_objects_deleted") == 0 {
        assert!(
            Instant::now() < deadline,
            "the sweep never deleted the orphan"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // New data after the cut is still produced and read.
    assert_eq!(produce(&addr, "d").0, 0);
    assert_eq!(fetch_code(&addr, 3), 0);
}

#[test]
fn a_compact_only_topic_is_not_cut_by_retention() {
    if !minio() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path());
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str(TOPIC);
    resource.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_config_operation(0)
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        &mut stream,
        ApiKey::IncrementalAlterConfigs,
        1,
        &IncrementalAlterConfigsRequest::default().with_resources(vec![resource]),
    );
    assert_eq!(
        recv::<IncrementalAlterConfigsResponse>(&mut stream, 1).responses[0].error_code,
        0
    );
    set_retention(&addr, "1");
    let keyed = support::keyed_batch("k", "v");
    assert_eq!(support::produce_batch(&addr, keyed).0, 0);
    wait_sealed(&node.http, 1);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(log_start(&node.http), 0);
    assert_eq!(fetch_code(&addr, 0), 0);
}
