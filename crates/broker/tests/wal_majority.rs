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
        support::skipped("majority-loss test; set CAMUS_MINIO=1");
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

/// Lose two of three voters, truncate, add the two back, then lose one more.
/// The remaining two are a majority of the new membership, so produce still
/// commits.
#[test]
fn fresh_voters_restore_a_quorum() -> std::io::Result<()> {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("fresh-voter test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let s3 = format!("http://{}", proxy.addr);
    let root = tempfile::tempdir().map_err(|err| std::io::Error::other(err))?;
    let mut nodes: Vec<Node> = (1..=5)
        .map(|id| Node::spawn_via(id, &root.path().join(format!("node-{id}")), &s3))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    create_wal_topic(&kafka_addr(node_by(&nodes, meta_leader)?));
    let outsider = kafka_addr(node_by(&nodes, 4)?);
    let (_, offset) = produce_retrying(&outsider, "v0");
    if offset != 0 {
        return Err(std::io::Error::other(format!("first offset was {offset}")));
    }
    wait_sealed(&node_by(&nodes, 4)?.http, 1);

    let (leader, _) = partition_leader(&outsider, None);
    let other = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader)
        .ok_or_else(|| std::io::Error::other("no second voter"))?;
    let survivor = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader && *id != other)
        .ok_or_else(|| std::io::Error::other("no survivor"))?;
    for id in [leader, other] {
        nodes[(id - 1) as usize].kill();
    }
    let survivor_http = node_by(&nodes, survivor)?.http.clone();
    // The dead leader stays in metrics until the election times out. Truncate
    // refuses while that id is still there.
    let quiet = Instant::now() + Duration::from_secs(5);
    loop {
        let view = admin(&survivor_http).ok_or_else(|| std::io::Error::other("no wal metrics"))?;
        if view["wal_leader"].is_null() {
            break;
        }
        if Instant::now() > quiet {
            return Err(std::io::Error::other(format!(
                "the shard still reports a leader: {view}"
            )));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let until = Instant::now() + Duration::from_secs(10);
    let truncated = loop {
        let response = post_json(
            &format!("http://{survivor_http}/admin/truncate-to-sealed"),
            &serde_json::json!({ "i_accept_data_loss": true }),
        )?;
        if response.status().is_success()
            || response.status().as_u16() != 503
            || Instant::now() > until
        {
            break response;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    if !truncated.status().is_success() {
        let status = truncated.status();
        let body = truncated
            .text()
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        return Err(std::io::Error::other(format!("truncate {status}: {body}")));
    }
    for id in [leader, other] {
        nodes[(id - 1) as usize].restart();
    }
    // Election can take the full 400–800 ms, and a restarted voter can win
    // it. Add through whoever leads, and try again while the answer is
    // "not the shard leader".
    let deadline = Instant::now() + Duration::from_secs(10);
    for id in [leader, other] {
        let addr = node_by(&nodes, id)?.http.clone();
        loop {
            let view =
                admin(&survivor_http).ok_or_else(|| std::io::Error::other("no wal metrics"))?;
            // A restarted voter can win the election before this loop asks
            // for it. It is already in the membership, and the leader
            // refuses to add itself.
            if view["wal_leader"].as_i64() == Some(i64::from(id)) {
                break;
            }
            let target = view["wal_leader"]
                .as_i64()
                .and_then(|led| nodes.iter().find(|node| node.id == led as i32))
                .map(|node| node.http.clone())
                .unwrap_or_else(|| survivor_http.clone());
            let added = post_json(
                &format!("http://{target}/admin/add-wal-voter"),
                &serde_json::json!({ "id": id, "addr": addr }),
            )?;
            if added.status().is_success() {
                break;
            }
            let status = added.status();
            let body = added
                .text()
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            if status.as_u16() == 409
                && body.contains("not the shard leader")
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            return Err(std::io::Error::other(format!(
                "add voter {id} {status}: {body}"
            )));
        }
    }
    wait_voters(&survivor_http, 3)?;
    let (_, offset) = produce_retrying(&kafka_addr(node_by(&nodes, leader)?), "v1");
    if offset != 1 {
        return Err(std::io::Error::other(format!(
            "produce after the new voters joined landed at {offset}"
        )));
    }
    nodes[(survivor - 1) as usize].kill();
    let (_, offset) = produce_retrying(&kafka_addr(node_by(&nodes, other)?), "v2");
    if offset != 2 {
        return Err(std::io::Error::other(format!(
            "produce after losing one more voter landed at {offset}"
        )));
    }
    Ok(())
}

fn node_by(nodes: &[Node], id: i32) -> std::io::Result<&Node> {
    nodes
        .iter()
        .find(|node| node.id == id)
        .ok_or_else(|| std::io::Error::other(format!("no node {id}")))
}

fn post_json(url: &str, body: &serde_json::Value) -> std::io::Result<reqwest::blocking::Response> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|err| std::io::Error::other(err.to_string()))?
        .post(url)
        .json(body)
        .send()
        .map_err(|err| std::io::Error::other(err.to_string()))
}

fn wait_voters(http: &str, count: usize) -> std::io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let body = reqwest::blocking::get(format!("http://{http}/admin/metrics"))
            .map_err(|err| std::io::Error::other(err.to_string()))?
            .text()
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        let value: serde_json::Value =
            serde_json::from_str(&body).map_err(|err| std::io::Error::other(err.to_string()))?;
        let voters = value["wal_voters"]
            .as_array()
            .map(|rows| rows.len())
            .unwrap_or(0);
        if voters == count {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(std::io::Error::other(format!(
                "shard has {voters} voters, wanted {count}: {body}"
            )));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
