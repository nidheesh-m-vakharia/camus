//! ADR 0014: one shard, with shard ids in place, and a cluster written
//! before those ids still restarts on shard 0. Partitions record their
//! shard, and WAL RPC has a route per shard beside shard 0's old one.

mod support;

use support::{
    create_topic_named, kafka_addr, produce, read_all, read_key, single, wait_settled, Opts, TOPIC,
};

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

/// ADR 0014 step 4. A single-shard cluster's partition rows lose their
/// `shard` field, which is how a build from before shard ids stored them.
/// Restarting reads the same records from the original `wal` directory.
#[test]
fn a_pre_shard_cluster_restarts_with_its_data_on_shard_0() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = single(dir.path(), Opts::default());
    let (code, offset) = produce(&kafka_addr(&node), "kept");
    assert_eq!(code, 0, "produce");
    // Metadata applies fsync at most once a second; the rest come back by
    // replaying the log. This test edits the redb file directly, so make the
    // rows durable first: a write a second after the last one commits with
    // fsync, and takes every earlier apply with it.
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    create_topic_named(&kafka_addr(&node), "durable", "wal", 1);
    let _ = node.child.kill();
    let _ = node.child.wait();
    drop_shard_fields(dir.path());
    assert!(dir.path().join("wal").is_dir(), "shard 0 log stays put");
    assert!(
        !dir.path().join("wal-shards").exists(),
        "a single shard has no side directory"
    );

    node.restart();
    wait_settled(&[&node]);
    let addr = kafka_addr(&node);
    assert_eq!(read_all(&addr, offset + 1), vec!["kept".to_string()]);
    assert!(read_key(&node.http, "wal/shards").is_none());
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(&node.http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let id = topic["id"].as_str().unwrap();
    let raw = read_key(&node.http, &format!("part/{id}/0")).unwrap();
    assert!(
        !raw.contains("\"shard\""),
        "the stored row stays in the old shape"
    );
    let partition: camus_metadata::Partition = serde_json::from_str(&raw).unwrap();
    assert_eq!(partition.shard, 0);
    let (code, _) = produce(&addr, "still");
    assert_eq!(code, 0, "produce after restart");
}

/// Rewrite partition rows, and any snapshot of them, as an older build did:
/// no `shard` field.
fn drop_shard_fields(dir: &std::path::Path) {
    const KV: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("kv");
    const META: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("meta");
    let db = redb::Database::open(dir.join("state.redb")).unwrap();
    let txn = db.begin_write().unwrap();
    {
        use redb::ReadableTable;
        let mut table = txn.open_table(KV).unwrap();
        let rows: Vec<(String, String)> = table
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value().to_string(), value.value().to_string())
            })
            .collect();
        let mut stripped = 0;
        for (key, value) in rows {
            if !key.starts_with("part/") {
                continue;
            }
            let mut json: serde_json::Value = serde_json::from_str(&value).unwrap();
            assert!(
                json.get("shard").is_some(),
                "{key} has no shard field to drop"
            );
            json.as_object_mut().unwrap().remove("shard");
            table
                .insert(key.as_str(), json.to_string().as_str())
                .unwrap();
            stripped += 1;
        }
        assert!(stripped >= 1, "no partition row");
    }
    let raw = {
        use redb::ReadableTable;
        let meta = txn.open_table(META).unwrap();
        meta.get("snapshot")
            .unwrap()
            .map(|guard| guard.value().to_string())
    };
    let Some(raw) = raw else {
        txn.commit().unwrap();
        return;
    };
    {
        let mut stored: SnapshotFile = serde_json::from_str(&raw).unwrap();
        let mut body: SnapshotBody = serde_json::from_slice(&stored.data).unwrap();
        for (key, value) in body.data.iter_mut() {
            if !key.starts_with("part/") {
                continue;
            }
            let mut json: serde_json::Value = serde_json::from_str(value).unwrap();
            json.as_object_mut().unwrap().remove("shard");
            *value = json.to_string();
        }
        stored.data = serde_json::to_vec(&body).unwrap();
        let mut meta = txn.open_table(META).unwrap();
        meta.insert("snapshot", serde_json::to_string(&stored).unwrap().as_str())
            .unwrap();
    }
    txn.commit().unwrap();
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotFile {
    meta: serde_json::Value,
    data: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotBody {
    data: std::collections::BTreeMap<String, String>,
}
