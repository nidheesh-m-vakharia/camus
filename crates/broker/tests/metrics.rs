//! `/metrics` exports consumer lag, WAL upload lag, object-store calls by
//! operation, and produce and fetch latency (issue 171).

mod support;

use std::time::{Duration, Instant};

use camus_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use camus_protocol::messages::offset_commit_response::OffsetCommitResponse;
use camus_protocol::messages::{ApiKey, GroupId, OffsetCommitRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::{connect, kafka_addr, produce, read_all, recv, send, single, Opts, TOPIC};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn scrape(http: &str) -> Result<String> {
    Ok(reqwest::blocking::get(format!("http://{http}/metrics"))?.text()?)
}

/// The value of the first line that starts with `series`.
fn value(text: &str, series: &str) -> Result<f64> {
    let line = text
        .lines()
        .find(|line| line.starts_with(series))
        .ok_or_else(|| format!("no {series} in\n{text}"))?;
    let raw = line.rsplit(' ').next().ok_or("empty line")?;
    Ok(raw.parse()?)
}

fn commit(addr: &str, group: &'static str, offset: i64) -> Result<()> {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::OffsetCommit,
        8,
        &OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str(group)))
            .with_generation_id_or_member_epoch(-1)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(offset)])]),
    );
    let response = recv::<OffsetCommitResponse>(&mut stream, 8);
    let code = response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .map(|part| part.error_code)
        .ok_or("empty OffsetCommit response")?;
    if code != 0 {
        return Err(format!("OffsetCommit error {code}").into());
    }
    Ok(())
}

#[test]
fn metrics_export_lag_upload_lag_and_request_latency() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    for value in ["a", "b", "c"] {
        let (code, _) = produce(&addr, value);
        if code != 0 {
            return Err(format!("produce error {code}").into());
        }
    }
    read_all(&addr, 3);
    commit(&addr, "lag \"one\"", 1)?;

    let labels = "group=\"lag \\\"one\\\"\",topic=\"orders\",partition=\"0\"";
    let deadline = Instant::now() + Duration::from_secs(10);
    let text = loop {
        let text = scrape(&node.http)?;
        if text.contains(&format!("camus_consumer_group_lag{{{labels}}} 2\n")) {
            break text;
        }
        if Instant::now() > deadline {
            return Err(format!("no lag of 2 for the group in\n{text}").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let committed = format!("camus_consumer_group_committed_offset{{{labels}}}");
    if value(&text, &committed)? != 1.0 {
        return Err(format!("committed offset is not 1 in\n{text}").into());
    }
    let end = format!("camus_consumer_group_log_end_offset{{{labels}}}");
    if value(&text, &end)? != 3.0 {
        return Err(format!("log end offset is not 3 in\n{text}").into());
    }
    if value(&text, "camus_consumer_group_lag_series_dropped")? != 0.0 {
        return Err("lag series were dropped under the cap".into());
    }
    if value(&text, "camus_produce_request_seconds_count")? < 3.0 {
        return Err(format!("fewer than 3 produces timed in\n{text}").into());
    }
    if value(&text, "camus_produce_request_seconds_bucket{le=\"+Inf\"}")? < 3.0 {
        return Err("the +Inf bucket misses produces".into());
    }
    if value(&text, "camus_fetch_request_seconds_count")? < 1.0 {
        return Err(format!("no fetch timed in\n{text}").into());
    }
    for series in [
        "camus_wal_oldest_unsealed_age_seconds{shard=\"0\"}",
        "camus_wal_upload_seconds_count{shard=\"0\"}",
        "camus_wal_seal_seconds_count{shard=\"0\"}",
        "camus_object_store_errors_total{op=\"put\"}",
        "camus_object_store_errors_total{op=\"list\"}",
        "camus_object_store_listed_keys_total",
        "# TYPE camus_wal_upload_seconds histogram",
    ] {
        if !text.contains(series) {
            return Err(format!("no {series} in\n{text}").into());
        }
    }

    // Uploads need a bucket. With one, the pass is timed and the WAL drains.
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("upload and seal timings; set CAMUS_MINIO=1");
        return Ok(());
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let text = scrape(&node.http)?;
        let uploads = value(&text, "camus_wal_upload_seconds_count{shard=\"0\"}")?;
        let seals = value(&text, "camus_wal_seal_seconds_count{shard=\"0\"}")?;
        let unsealed = value(&text, "camus_wal_unsealed_bytes{shard=\"0\"}")?;
        let age = value(&text, "camus_wal_oldest_unsealed_age_seconds{shard=\"0\"}")?;
        if uploads >= 1.0 && seals >= 1.0 && unsealed == 0.0 && age == 0.0 {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("the upload was never timed in\n{text}").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
