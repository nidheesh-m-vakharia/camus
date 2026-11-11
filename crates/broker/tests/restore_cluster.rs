//! Wipe the broker disk and rebuild sealed data plus snapshot metadata from
//! object storage.

mod support;

use std::time::Duration;

use support::{
    create_wal_topic, init, kafka_addr, produce, read_all, read_key, wait_sealed, Node, Opts,
};

#[test]
fn a_wiped_disk_restores_sealed_data_and_snapshot_metadata() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("cluster restore test; set CAMUS_MINIO=1");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let prefix = format!("restore-{}/", std::process::id());
    let opts = Opts::default()
        .env("CAMUS_META_SNAPSHOT_MS", "200")
        .env("CAMUS_S3_PREFIX", &prefix);
    let node = Node::with(1, root.path(), opts.clone());
    init(std::slice::from_ref(&node), &[1]);
    let client = reqwest::blocking::Client::new();
    let written = client
        .post(format!("http://{}/admin/write", node.http))
        .json(&serde_json::json!({"key": "snap-probe", "value": "kept"}))
        .send()
        .unwrap();
    assert!(written.status().is_success(), "{}", written.text().unwrap());
    create_wal_topic(&kafka_addr(&node));
    let (code, offset) = produce(&kafka_addr(&node), "sealed");
    assert_eq!(code, 0, "produce failed");
    assert_eq!(offset, 0);
    wait_sealed(&node.http, 1);
    std::thread::sleep(Duration::from_secs(2));

    let dir = node.dir.clone();
    drop(node);
    std::fs::remove_dir_all(&dir).unwrap();
    let node = Node::with(1, &dir, opts);
    init(std::slice::from_ref(&node), &[1]);

    let restored = client
        .post(format!("http://{}/admin/restore-cluster", node.http))
        .json(&serde_json::json!({"i_accept_data_loss": true}))
        .send()
        .unwrap();
    assert!(
        restored.status().is_success(),
        "{}",
        restored.text().unwrap()
    );
    assert_eq!(read_key(&node.http, "snap-probe").as_deref(), Some("kept"));
    assert_eq!(read_all(&kafka_addr(&node), 1), vec!["sealed".to_string()]);
}
