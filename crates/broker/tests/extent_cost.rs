//! Benchmark: what produce and fetch cost on a partition with a long sealed
//! history (#109). Uploads run every millisecond, so each acked batch is its
//! own extent; then a producer and a fetcher time requests against it.
//!
//! ```text
//! CAMUS_MINIO=1 cargo test --release -p camus-broker --test extent_cost -- --ignored --nocapture
//! ```
//!
//! `CAMUS_BENCH_EXTENTS` (default 3000) sets how many extents to build first.

mod support;

use std::time::{Duration, Instant};

use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::{ApiKey, FetchRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index].as_secs_f64() * 1000.0
}

fn fetch_at(stream: &mut std::net::TcpStream, offset: i64) {
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = offset;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partitions = vec![part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    let fetched = recv::<FetchResponse>(stream, 11);
    let row = &fetched.responses[0].partitions[0];
    assert_eq!(row.error_code, 0, "{row:?}");
    assert!(row.records.as_ref().is_some_and(|bytes| !bytes.is_empty()));
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn produce_and_fetch_cost_with_many_extents() {
    assert_eq!(
        std::env::var("CAMUS_MINIO").ok().as_deref(),
        Some("1"),
        "needs MinIO"
    );
    let built: i64 = std::env::var("CAMUS_BENCH_EXTENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3_000);
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default().env("CAMUS_FLUSH_MS", "1"));
    let addr = kafka_addr(&node);
    let mut stream = connect(&addr);
    stream.set_nodelay(true).unwrap();
    let started = Instant::now();
    for index in 0..built {
        assert_eq!(produce(&addr, &format!("v{index}")).0, 0);
    }
    wait_sealed(&node.http, built);
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(&node.http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let row = read_key(
        &node.http,
        &format!("part/{}/0", topic["id"].as_str().unwrap()),
    )
    .unwrap();
    println!(
        "built {built} batches in {:.1}s; partition row is {} bytes",
        started.elapsed().as_secs_f64(),
        row.len()
    );

    let samples = 300;
    let mut produced = Vec::with_capacity(samples);
    for index in 0..samples {
        let at = Instant::now();
        assert_eq!(produce(&addr, &format!("p{index}")).0, 0);
        produced.push(at.elapsed());
    }
    // The newest sealed batch: found through the extents, read from R2.
    let mut fetched = Vec::with_capacity(samples);
    for _ in 0..samples {
        let at = Instant::now();
        fetch_at(&mut stream, built - 1);
        fetched.push(at.elapsed());
    }
    produced.sort();
    fetched.sort();
    println!("| Request | Samples | p50 ms | p99 ms |");
    println!("| --- | ---: | ---: | ---: |");
    for (name, sorted) in [("produce", &produced), ("fetch newest sealed", &fetched)] {
        println!(
            "| {name} | {} | {:.2} | {:.2} |",
            sorted.len(),
            pct(sorted, 0.50),
            pct(sorted, 0.99)
        );
    }
}
