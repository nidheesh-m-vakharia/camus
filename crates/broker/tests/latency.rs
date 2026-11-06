//! Phase 2 latency benchmark: `wal` versus `r2` produce on the same machine.
//!
//! Each mode gets its own three-broker cluster. One producer sends 1 KiB
//! `acks=all` batches one at a time over one connection, so each sample is
//! one full ack round trip. Both use the production upload interval (250ms).
//!
//! ```text
//! CAMUS_MINIO=1 cargo test --release -p camus-broker --test latency -- --ignored --nocapture
//! ```
//!
//! `CAMUS_BENCH_WAL` and `CAMUS_BENCH_R2` set the sample counts.

mod support;

use std::net::TcpStream;
use std::time::{Duration, Instant};

use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{ApiKey, ProduceRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

fn count(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn produce_on(stream: &mut TcpStream, value: &str) -> i16 {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch(value)))];
    send(
        stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(30_000)
            .with_topic_data(vec![topic]),
    );
    recv::<ProduceResponse>(stream, 9).responses[0].partition_responses[0].error_code
}

fn run(mode: &'static str, warmup: usize, samples: usize, opts: Opts) -> Vec<Duration> {
    let root = tempfile::tempdir().unwrap();
    let brokers = count("CAMUS_BENCH_BROKERS", 3) as i32;
    let nodes: Vec<Node> = (1..=brokers)
        .map(|id| Node::with(id, &root.path().join(format!("node-{id}")), opts.clone()))
        .collect();
    init(&nodes, &(1..=brokers).collect::<Vec<_>>());
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let controller = nodes.iter().find(|node| node.id == meta_leader).unwrap();
    create_topic(&kafka_addr(controller), mode);
    let (_, leader) = partition_leader(&kafka_addr(&nodes[0]), None);
    let mut stream = connect(&leader);
    stream.set_nodelay(true).unwrap();
    let value = "x".repeat(1024);
    for _ in 0..warmup {
        assert_eq!(produce_on(&mut stream, &value), 0);
    }
    let mut out = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        assert_eq!(produce_on(&mut stream, &value), 0);
        out.push(started.elapsed());
    }
    out.sort();
    out
}

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index].as_secs_f64() * 1000.0
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn wal_versus_r2_produce_latency() {
    assert_eq!(
        std::env::var("CAMUS_MINIO").ok().as_deref(),
        Some("1"),
        "needs MinIO"
    );
    // Both modes run with the production upload interval.
    let wal = run(
        "wal",
        100,
        count("CAMUS_BENCH_WAL", 2_000),
        Opts::default().env("CAMUS_FLUSH_MS", "250"),
    );
    let r2 = run(
        "r2",
        5,
        count("CAMUS_BENCH_R2", 100),
        Opts::default().env("CAMUS_FLUSH_MS", "250"),
    );
    println!("| Mode | Samples | p50 ms | p99 ms | max ms |");
    println!("| --- | --- | --- | --- | --- |");
    for (mode, sorted) in [("wal", &wal), ("r2", &r2)] {
        println!(
            "| `{mode}` | {} | {:.2} | {:.2} | {:.2} |",
            sorted.len(),
            pct(sorted, 0.50),
            pct(sorted, 0.99),
            sorted.last().unwrap().as_secs_f64() * 1000.0,
        );
    }
}
