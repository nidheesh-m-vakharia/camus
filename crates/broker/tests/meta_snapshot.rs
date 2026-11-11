//! The metadata leader writes a snapshot to object storage and the body
//! decodes back into the same keys.

mod support;

use std::time::{Duration, Instant};

use camus_metadata::snapshot_rows;
use camus_storage::{Bucket, BucketConfig};
use support::{Node, Opts};

#[test]
fn metadata_snapshot_lands_in_object_storage() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping metadata snapshot test; set CAMUS_MINIO=1");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let prefix = format!("meta-snap-{}/", std::process::id());
    let node = Node::with(
        1,
        root.path(),
        Opts::default()
            .env("CAMUS_META_SNAPSHOT_MS", "200")
            .env("CAMUS_S3_PREFIX", &prefix),
    );
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());

    let written = client
        .post(format!("http://{}/admin/write", node.http))
        .json(&serde_json::json!({"key": "snap-probe", "value": "kept"}))
        .send()
        .unwrap();
    assert!(written.status().is_success(), "{}", written.text().unwrap());

    let bucket = Bucket::connect(&BucketConfig {
        endpoint: "http://127.0.0.1:9100".into(),
        bucket: "camus".into(),
        region: "us-east-1".into(),
        access_key: "camus".into(),
        secret_key: "camus-dev-secret".into(),
        prefix: prefix.clone(),
        virtual_hosted: false,
    })
    .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let rows = runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            let Ok(objects) = bucket.list("meta/snap/").await else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let Some((_, current_size, _)) = objects
                .iter()
                .find(|(name, _, _)| name == "meta/snap/CURRENT")
            else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let Ok(pointer) = bucket
                .get_range("meta/snap/CURRENT", 0..*current_size)
                .await
            else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let key = String::from_utf8(pointer.to_vec()).unwrap();
            let Some((_, size, _)) = objects.iter().find(|(name, _, _)| name == &key) else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let body = bucket.get_range(&key, 0..*size).await.unwrap();
            let rows = snapshot_rows(&body).unwrap();
            if rows
                .iter()
                .any(|(key, value)| key == "snap-probe" && value == "kept")
            {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("snapshot did not contain snap-probe");
    });
    assert!(rows
        .iter()
        .any(|(key, value)| key == "snap-probe" && value == "kept"));
}
