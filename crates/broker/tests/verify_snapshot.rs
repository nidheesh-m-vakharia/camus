//! Restore drill: rebuild the newest snapshot and the object footers in a
//! scratch directory and check the result matches the live cluster, which
//! the drill must leave as it was.

mod support;

use std::time::{Duration, Instant};

use support::{
    create_topic_with, init, kafka_addr, produce_partition, scan_keys, wait_settled, Node, Opts,
    TOPIC,
};

/// `(partition, log_start, next_offset)` for every partition the live
/// metadata has.
fn live_partitions(http: &str) -> anyhow::Result<Vec<(i64, i64, i64)>> {
    let mut rows = Vec::new();
    for (key, raw) in scan_keys(http, "part/") {
        let partition = key
            .rsplit('/')
            .next()
            .ok_or_else(|| anyhow::anyhow!("partition key {key}"))?
            .parse()?;
        let row: serde_json::Value = serde_json::from_str(&raw)?;
        let field = |name: &str| {
            row[name]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("{name} missing from {key}"))
        };
        rows.push((partition, field("log_start")?, field("next_offset")?));
    }
    rows.sort_unstable();
    Ok(rows)
}

/// `(partition, log_start, end)` from a verify-snapshot report.
fn drilled_partitions(report: &serde_json::Value) -> anyhow::Result<Vec<(i64, i64, i64)>> {
    let listed = report["listed"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("no partition list in {report}"))?;
    let mut rows = Vec::new();
    for row in listed {
        anyhow::ensure!(row["topic"] == TOPIC, "unexpected topic in {row}");
        anyhow::ensure!(row["offline"] == false, "offline partition {row}");
        let field = |name: &str| {
            row[name]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("{name} missing from {row}"))
        };
        rows.push((field("partition")?, field("log_start")?, field("end")?));
    }
    rows.sort_unstable();
    Ok(rows)
}

#[test]
fn a_restore_drill_matches_the_live_cluster_and_changes_nothing() -> anyhow::Result<()> {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("restore drill test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let prefix = format!("verify-{}/", std::process::id());
    let opts = Opts::default()
        .env("CAMUS_META_SNAPSHOT_MS", "200")
        .env("CAMUS_S3_PREFIX", &prefix);
    let node = Node::with(1, root.path(), opts);
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    let kafka = kafka_addr(&node);
    create_topic_with(&kafka, "wal", 3);
    let counts = [3, 2, 0];
    for (partition, count) in counts.iter().enumerate() {
        for record in 0..*count {
            let (code, _) = produce_partition(&kafka, partition as i32, &format!("r{record}"));
            anyhow::ensure!(
                code == 0,
                "produce to partition {partition} failed ({code})"
            );
        }
    }

    let expected: Vec<i64> = counts.iter().map(|count| *count as i64).collect();
    let deadline = Instant::now() + Duration::from_secs(20);
    let live = loop {
        let live = live_partitions(&node.http)?;
        let ends: Vec<i64> = live.iter().map(|(_, _, end)| *end).collect();
        if ends == expected {
            break live;
        }
        anyhow::ensure!(Instant::now() < deadline, "never sealed: {live:?}");
        std::thread::sleep(Duration::from_millis(100));
    };

    // The newest snapshot may predate the last seals; the drill rebuilds
    // those extents from the footers. Retry only until a snapshot that has
    // the topic exists.
    let client = reqwest::blocking::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    let report = loop {
        let response = client
            .get(format!("http://{}/admin/verify-snapshot", node.http))
            .send()?;
        if response.status().is_success() {
            let report: serde_json::Value = response.json()?;
            if report["partitions"] == 3 {
                break report;
            }
        }
        anyhow::ensure!(Instant::now() < deadline, "no snapshot with the topic");
        std::thread::sleep(Duration::from_millis(200));
    };

    assert_eq!(report["topics"], 1, "{report}");
    assert_eq!(report["offline"], 0, "{report}");
    assert_eq!(drilled_partitions(&report)?, live, "{report}");
    let age = report["snapshot_age_ms"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("no snapshot age in {report}"))?;
    assert!((0..60_000).contains(&age), "snapshot age {age} ms");
    assert_eq!(
        live_partitions(&node.http)?,
        live,
        "the drill changed the cluster"
    );
    Ok(())
}
