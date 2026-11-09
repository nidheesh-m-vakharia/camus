//! ADR 0008 majority loss. Five brokers form the metadata group; brokers 1–3
//! are the WAL shard. With R2 cut off, two batches are acknowledged but not
//! sealed. Killing two shard voters takes the partition offline. Only
//! `truncate-to-sealed`, with its data-loss flag, brings it back, and only
//! then are the unsealed offsets gone: the partition resumes at the sealed
//! end, and the next produce reuses those offsets.
//!
//! Needs MinIO (`CAMUS_MINIO=1`). The brokers reach it through a proxy the
//! test can cut.

mod support;

use std::time::{Duration, Instant};

use support::*;

fn truncate(http: &str, accept: bool) -> reqwest::blocking::Response {
    reqwest::blocking::Client::new()
        .post(format!("http://{http}/admin/truncate-to-sealed"))
        .json(&serde_json::json!({ "i_accept_data_loss": accept }))
        .send()
        .unwrap()
}

#[test]
fn majority_loss_needs_truncate_to_sealed() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping majority-loss test; set CAMUS_MINIO=1");
        return;
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let s3 = format!("http://{}", proxy.addr);
    let root = tempfile::tempdir().unwrap();
    let mut nodes: Vec<Node> = (1..=5)
        .map(|id| Node::spawn_via(id, &root.path().join(format!("node-{id}")), &s3))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    create_wal_topic(&kafka_addr(
        nodes.iter().find(|node| node.id == meta_leader).unwrap(),
    ));

    // Broker 4 is not in the shard; it learns the shard leader from metadata.
    let outsider = kafka_addr(&nodes[3]);
    assert_eq!(produce_retrying(&outsider, "v0").1, 0);
    assert_eq!(produce_retrying(&outsider, "v1").1, 1);
    wait_sealed(&nodes[3].http, 2);

    // R2 is unreachable: these two are acknowledged by the shard quorum but
    // never sealed.
    proxy.cut();
    assert_eq!(produce_retrying(&outsider, "v2").1, 2);
    assert_eq!(produce_retrying(&outsider, "v3").1, 3);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(sealed_end(&nodes[3].http), 2, "R2 is cut off");

    // Lose the shard leader and one more voter: no quorum left.
    let (leader, _) = partition_leader(&outsider, None);
    let before = partition_epoch(&outsider);
    assert!(
        before >= 1,
        "wal partitions report the shard term as their epoch"
    );
    let other = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let survivor = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader && *id != other)
        .unwrap();
    for id in [leader, other] {
        nodes[(id - 1) as usize].kill();
    }
    let survivor_node = &nodes[(survivor - 1) as usize];
    let survivor_kafka = kafka_addr(survivor_node);

    // Offline: the survivor neither accepts produce nor serves the tail, and
    // stays that way. It does not elect itself.
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        assert_ne!(produce(&survivor_kafka, "lost").0, 0);
        assert_eq!(fetch_code(&survivor_kafka, 0), 6);
        std::thread::sleep(Duration::from_millis(200));
    }
    let view = admin(&survivor_node.http).unwrap();
    assert_ne!(view["wal_leader"].as_i64(), Some(i64::from(survivor)));

    // The command is refused without the flag and on a broker outside the shard.
    assert_eq!(truncate(&survivor_node.http, false).status().as_u16(), 400);
    assert_eq!(truncate(&nodes[3].http, true).status().as_u16(), 409);
    proxy.restore();
    let done = truncate(&survivor_node.http, true);
    assert!(done.status().is_success(), "{}", done.text().unwrap());

    // The survivor leads a one-voter shard that starts at the sealed end.
    // Offsets 2 and 3 were acknowledged and are gone.
    let (now, now_addr) = partition_leader(&outsider, Some(leader));
    assert_eq!(now, survivor);
    assert_eq!(
        fetch_code(&now_addr, 3),
        1,
        "OFFSET_OUT_OF_RANGE past the sealed end"
    );
    // The truncation is a new leader epoch. A consumer that read offsets 2
    // and 3 in the old one learns its epoch ended at 2 and resets.
    let deadline = Instant::now() + Duration::from_secs(10);
    while partition_epoch(&outsider) <= before {
        assert!(
            Instant::now() < deadline,
            "the epoch never moved past {before}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let after = partition_epoch(&outsider);
    assert_eq!(epoch_end(&now_addr, after, before).2, 2);
    let (_, offset) = produce_retrying(&outsider, "v4");
    assert_eq!(offset, 2, "the next produce appends at the sealed end");
    assert_eq!(read_all(&now_addr, 3), vec!["v0", "v1", "v4"]);
    wait_sealed(&nodes[3].http, 3);
}
