//! Write-path throughput benchmark (PLAN, "Write path").
//!
//! Three brokers on one machine. `P` producers, each on its own connection,
//! send `acks=all` batches of 16 × 1 KiB records to `P` partitions for a
//! fixed time. Reports acked MB/s, ack latency, and from the shard leader's
//! log counters: fsyncs per ack (does group commit happen?) and log bytes
//! per payload byte (how much the entry encoding adds).
//!
//! ```text
//! CAMUS_MINIO=1 cargo test --release -p camus-broker --test throughput -- --ignored --nocapture
//! ```
//!
//! `CAMUS_BENCH_SECONDS` (default 10), `CAMUS_BENCH_PRODUCERS` (a comma
//! list, default `1,8,32`), and `CAMUS_BENCH_BROKERS` (default 3) adjust the
//! run.

mod support;

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{ApiKey, ProduceRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use support::*;

const RECORDS: usize = 16;
const RECORD_BYTES: usize = 1024;

fn records_batch() -> Bytes {
    let records: Vec<Record> = (0..RECORDS)
        .map(|index| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: index as i64,
            timestamp: 9,
            key: None,
            value: Some(Bytes::from(vec![b'x'; RECORD_BYTES])),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    buf.freeze()
}

fn produce_on(stream: &mut TcpStream, partition: i32, batch: &Bytes) -> i16 {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(batch.clone()))];
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

fn counter(http: &str, name: &str) -> u64 {
    admin(http).unwrap()[name].as_u64().unwrap()
}

struct Row {
    producers: usize,
    acks: usize,
    mb_per_s: f64,
    p50: f64,
    p99: f64,
    fsyncs_per_ack: f64,
    log_bytes_per_payload_byte: f64,
}

fn run(producers: usize, seconds: u64) -> Row {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().to_path_buf();
    if std::env::var("CAMUS_BENCH_KEEP").is_ok() {
        // Leave the broker logs for inspection.
        eprintln!("keeping {}", base.display());
        std::mem::forget(root);
    }
    let brokers: i32 = std::env::var("CAMUS_BENCH_BROKERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);
    let nodes: Vec<Node> = (1..=brokers)
        .map(|id| {
            Node::with(
                id,
                &base.join(format!("node-{id}")),
                Opts::default().env("CAMUS_FLUSH_MS", "250"),
            )
        })
        .collect();
    init(&nodes, &(1..=brokers).collect::<Vec<_>>());
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let controller = nodes.iter().find(|node| node.id == meta_leader).unwrap();
    create_topic_with(&kafka_addr(controller), "wal", producers as i32);
    let (leader, leader_addr) = partition_leader(&kafka_addr(&nodes[0]), None);
    let leader_http = nodes
        .iter()
        .find(|node| node.id == leader)
        .unwrap()
        .http
        .clone();
    let batch = records_batch();

    // Warm up every connection, then measure.
    let mut streams: Vec<TcpStream> = (0..producers)
        .map(|partition| {
            let mut stream = connect(&leader_addr);
            stream.set_nodelay(true).unwrap();
            assert_eq!(produce_on(&mut stream, partition as i32, &batch), 0);
            stream
        })
        .collect();
    let fsyncs_before = counter(&leader_http, "wal_log_fsyncs");
    let bytes_before = counter(&leader_http, "wal_log_bytes");
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let workers: Vec<_> = streams
        .drain(..)
        .enumerate()
        .map(|(partition, mut stream)| {
            let stop = stop.clone();
            let batch = batch.clone();
            std::thread::spawn(move || {
                let mut latencies = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let sent = Instant::now();
                    assert_eq!(produce_on(&mut stream, partition as i32, &batch), 0);
                    latencies.push(sent.elapsed());
                }
                latencies
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(seconds));
    stop.store(true, Ordering::Relaxed);
    let mut latencies: Vec<Duration> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    let elapsed = started.elapsed().as_secs_f64();
    let fsyncs = counter(&leader_http, "wal_log_fsyncs") - fsyncs_before;
    let sent = counter(&leader_http, "wal_rpc_sent_us") as f64
        / counter(&leader_http, "wal_rpc_sent_count").max(1) as f64;
    let followers: Vec<&Node> = nodes.iter().filter(|node| node.id != leader).collect();
    let handled = followers
        .iter()
        .map(|node| counter(&node.http, "wal_rpc_handled_us"))
        .sum::<u64>() as f64
        / followers
            .iter()
            .map(|node| counter(&node.http, "wal_rpc_handled_count"))
            .sum::<u64>()
            .max(1) as f64;
    if !followers.is_empty() {
        eprintln!(
            "producers={producers}: append RPC {:.2} ms on the leader, {:.2} ms handling on a follower",
            sent / 1000.0,
            handled / 1000.0
        );
    }
    let log_bytes = counter(&leader_http, "wal_log_bytes") - bytes_before;
    latencies.sort();
    let acks = latencies.len();
    let payload = (acks * RECORDS * RECORD_BYTES) as f64;
    let pct = |p: f64| latencies[((acks as f64 - 1.0) * p).round() as usize].as_secs_f64() * 1000.0;
    Row {
        producers,
        acks,
        mb_per_s: payload / elapsed / (1024.0 * 1024.0),
        p50: pct(0.50),
        p99: pct(0.99),
        fsyncs_per_ack: fsyncs as f64 / acks as f64,
        log_bytes_per_payload_byte: log_bytes as f64 / payload,
    }
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn wal_produce_throughput() {
    assert_eq!(
        std::env::var("CAMUS_MINIO").ok().as_deref(),
        Some("1"),
        "needs MinIO"
    );
    let seconds = std::env::var("CAMUS_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let producers: Vec<usize> = std::env::var("CAMUS_BENCH_PRODUCERS")
        .unwrap_or_else(|_| "1,8,32".into())
        .split(',')
        .map(|value| value.trim().parse().unwrap())
        .collect();
    println!(
        "| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |"
    );
    println!("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for count in producers {
        let row = run(count, seconds);
        println!(
            "| {} | {} | {:.1} | {:.2} | {:.2} | {:.2} | {:.2} |",
            row.producers,
            row.acks,
            row.mb_per_s,
            row.p50,
            row.p99,
            row.fsyncs_per_ack,
            row.log_bytes_per_payload_byte,
        );
    }
}
