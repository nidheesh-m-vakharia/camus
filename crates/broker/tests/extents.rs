//! Extents live in metadata keys of their own, one per object and partition,
//! so the partition row stays a fixed size (#109). A cluster written before
//! that keeps serving its old rows and moves them to keys on the next publish.

mod support;

use support::*;

const KV: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("kv");

fn topic_id(http: &str) -> String {
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    topic["id"].as_str().unwrap().to_string()
}

/// Sealing uploads to object storage, which these tests need.
fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        eprintln!("skipping extent test; set CAMUS_MINIO=1");
    }
    on
}

#[test]
fn batches_sealed_together_are_one_extent() {
    if !minio() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    // A long upload interval, so all five batches are sealed in one pass.
    let node = single(dir.path(), Opts::default().env("CAMUS_FLUSH_MS", "2000"));
    let addr = kafka_addr(&node);
    for index in 0..5 {
        assert_eq!(produce(&addr, &format!("v{index}")), (0, index));
    }
    wait_sealed(&node.http, 5);
    let ranges = extents(&node.http);
    assert_eq!(ranges.first().map(|range| range.0), Some(0));
    assert_eq!(ranges.last().map(|range| range.1), Some(5));
    assert!(
        ranges.len() <= 2,
        "five batches in one or two uploads, not one extent each: {ranges:?}"
    );
    let row = read_key(&node.http, &format!("part/{}/0", topic_id(&node.http))).unwrap();
    assert!(!row.contains("extents"), "{row}");
    assert_eq!(read_all(&addr, 5), vec!["v0", "v1", "v2", "v3", "v4"]);
}

#[test]
fn a_row_from_before_extent_keys_is_served_and_then_migrated() {
    if !minio() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    for index in 0..3 {
        assert_eq!(produce(&addr, &format!("v{index}")), (0, index));
        // One upload each, so the old row holds several extents.
        wait_sealed(&node.http, index + 1);
    }
    let id = topic_id(&node.http);
    // Metadata applies fsync at most once a second; the rest come back by
    // replaying the log, which would re-add the keys this test removes. A
    // write a second after the last one is durable and takes every earlier
    // apply with it.
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    create_topic_named(&addr, "durable", "wal", 1);
    let _ = node.child.kill();
    let _ = node.child.wait();
    move_extents_into_the_row(dir.path(), &id);

    node.restart();
    wait_settled(&[&node]);
    let addr = kafka_addr(&node);
    assert_eq!(extents(&node.http), vec![(0, 1), (1, 2), (2, 3)]);
    assert!(
        scan_keys(&node.http, "ext/").is_empty(),
        "still the old shape"
    );
    assert_eq!(read_all(&addr, 3), vec!["v0", "v1", "v2"]);

    let (code, _) = produce(&addr, "v3");
    assert_eq!(code, 0);
    wait_sealed(&node.http, 4);
    let row = read_key(&node.http, &format!("part/{id}/0")).unwrap();
    assert!(!row.contains("extents"), "migrated: {row}");
    let ranges = extents(&node.http);
    assert_eq!(&ranges[..3], &[(0, 1), (1, 2), (2, 3)]);
    assert_eq!(ranges.last().map(|range| range.1), Some(4));
    assert_eq!(read_all(&addr, 4), vec!["v0", "v1", "v2", "v3"]);
}

/// Rewrite partition 0 as a build before extent keys stored it: its extents
/// in the row, and no `ext/` keys.
fn move_extents_into_the_row(dir: &std::path::Path, id: &str) {
    let db = redb::Database::open(dir.join("state.redb")).unwrap();
    let txn = db.begin_write().unwrap();
    {
        use redb::ReadableTable;
        let mut table = txn.open_table(KV).unwrap();
        let prefix = format!("ext/{id}/");
        let keys: Vec<(String, String)> = table
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value().to_string(), value.value().to_string())
            })
            .filter(|(key, _)| key.starts_with(&prefix))
            .collect();
        assert!(!keys.is_empty(), "no extent keys to move");
        let extents: Vec<serde_json::Value> = keys
            .iter()
            .map(|(_, raw)| serde_json::from_str(raw).unwrap())
            .collect();
        for (key, _) in &keys {
            table.remove(key.as_str()).unwrap();
        }
        let part_key = format!("part/{id}/0");
        let mut row: serde_json::Value =
            serde_json::from_str(table.get(part_key.as_str()).unwrap().unwrap().value()).unwrap();
        row["extents"] = serde_json::Value::Array(extents);
        table
            .insert(part_key.as_str(), row.to_string().as_str())
            .unwrap();
    }
    txn.commit().unwrap();
}
