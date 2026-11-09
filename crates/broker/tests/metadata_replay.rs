//! Metadata applies commit without fsync, at most one durable commit a
//! second. A broker killed right after a metadata write gets it back by
//! replaying the Raft log on restart (#106).

mod support;

use support::*;

#[test]
fn a_metadata_write_survives_a_kill_before_its_apply_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = single(dir.path(), Opts::default());
    let (code, offset) = produce(&kafka_addr(&node), "kept");
    assert_eq!(code, 0, "produce");
    // The first write is a second past the last durable commit, so it
    // fsyncs; the next one, straight after, does not.
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    create_topic_named(&kafka_addr(&node), "durable", "wal", 1);
    create_topic_named(&kafka_addr(&node), "late", "wal", 3);
    let _ = node.child.kill();
    let _ = node.child.wait();
    assert!(on_disk(dir.path(), "topic/durable"), "the durable write");
    assert!(
        !on_disk(dir.path(), "topic/late"),
        "the second write should not have reached disk before the kill"
    );

    node.restart();
    wait_settled(&[&node]);
    let addr = kafka_addr(&node);
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(&node.http, "topic/late").expect("replayed topic")).unwrap();
    assert_eq!(topic["partitions"].as_i64(), Some(3), "{topic}");
    assert_eq!(read_all(&addr, offset + 1), vec!["kept".to_string()]);
    let (code, _) = produce(&addr, "after");
    assert_eq!(code, 0, "produce after restart");
}

/// Whether `key` is in the metadata state on disk.
fn on_disk(dir: &std::path::Path, key: &str) -> bool {
    const KV: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("kv");
    let db = redb::Database::open(dir.join("state.redb")).unwrap();
    let txn = redb::ReadableDatabase::begin_read(&db).unwrap();
    let table = txn.open_table(KV).unwrap();
    let found = redb::ReadableTable::get(&table, key).unwrap().is_some();
    found
}
