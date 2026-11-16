//! Write-path throughput benchmark (PLAN, "Write path").
//!
//! Three brokers on one machine. `P` producers, each on its own connection,
//! send `acks=all` batches of 16 × 1 KiB records to `P` partitions for a
//! fixed time. Reports acked MB/s, ack latency, and from the shard leader's
//! log counters: fsyncs per ack (does group commit happen?) and log bytes
//! per payload byte. A non-compacted batch is written once, into the buffer
//! the durable write submits, so that ratio is framing and the entry around
//! the batch, not a second copy of the batch.
//!
//! ```text
//! CAMUS_MINIO=1 cargo test --release -p camus-broker --test throughput -- --ignored --nocapture
//! ```
//!
//! `CAMUS_BENCH_SECONDS` (default 10), `CAMUS_BENCH_PRODUCERS` (a comma
//! list, default `1,8,32`), and `CAMUS_BENCH_BROKERS` (default 3) adjust the
//! run. `CAMUS_BENCH_RECORDS` (default 16) and `CAMUS_BENCH_RECORD_BYTES`
//! (default 1024) set the batch; 250 × 1000 is Iggy's `pinned_producer`
//! workload. `CAMUS_BENCH_CLUSTER` and `CAMUS_BENCH_RESET` run it against
//! brokers on other machines (see `BenchCluster` in `support`); memory is
//! then not sampled.

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

fn knob(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn records_batch(records: usize, record_bytes: usize) -> Bytes {
    let records: Vec<Record> = (0..records)
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
            value: Some(Bytes::from(vec![b'x'; record_bytes])),
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
    p999: f64,
    fsyncs_per_ack: f64,
    log_bytes_per_payload_byte: f64,
    /// `None` when the brokers run on other machines.
    memory: Option<Memory>,
    leader: i32,
}

fn run(producers: usize, seconds: u64) -> Result<Row, Box<dyn std::error::Error>> {
    let brokers = knob("CAMUS_BENCH_BROKERS", 3) as i32;
    // One shard, so every partition is on the leader the producers dial.
    let cluster = BenchCluster::start(
        brokers,
        Opts::default().env("CAMUS_FLUSH_MS", "250"),
        Some(1),
    )?;
    create_topic_with(
        &cluster.node(cluster.controller)?.kafka,
        "wal",
        producers as i32,
    );
    let first = cluster.nodes.first().ok_or("no brokers")?;
    let (leader, leader_addr) = partition_leader(&first.kafka, None);
    let leader_http = cluster.node(leader)?.http.clone();
    let records = knob("CAMUS_BENCH_RECORDS", 16);
    let record_bytes = knob("CAMUS_BENCH_RECORD_BYTES", 1024);
    let batch = records_batch(records, record_bytes);
    // Memory is sampled only when every broker runs on this machine.
    let pids = cluster.pids();
    // Empty: the cluster and topic exist, nothing has been produced.
    let idle = pids
        .as_ref()
        .map(|pids| steady_rss(pids, Duration::from_secs(3)));
    let sampler = pids.clone().map(MemorySampler::start);

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
    let pool_wait_before = counter(&leader_http, "wal_log_pool_wait_us");
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
    let pool_wait = counter(&leader_http, "wal_log_pool_wait_us") - pool_wait_before;
    eprintln!(
        "producers={producers}: leader log append waits {:.3} ms for a blocking-pool thread on average, {:.2} ms at most",
        pool_wait as f64 / fsyncs.max(1) as f64 / 1000.0,
        counter(&leader_http, "wal_log_pool_wait_max_us") as f64 / 1000.0
    );
    let sent = counter(&leader_http, "wal_rpc_sent_us") as f64
        / counter(&leader_http, "wal_rpc_sent_count").max(1) as f64;
    let followers: Vec<&BenchNode> = cluster
        .nodes
        .iter()
        .filter(|node| node.id != leader)
        .collect();
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
    let memory = match (pids, idle, sampler) {
        (Some(pids), Some(idle), Some(sampler)) => {
            let peak = sampler.finish();
            std::thread::sleep(Duration::from_secs(3));
            let after = steady_rss(&pids, Duration::from_secs(2));
            Some(Memory {
                ids: cluster.nodes.iter().map(|node| node.id).collect(),
                idle,
                peak,
                after,
            })
        }
        _ => None,
    };
    latencies.sort();
    let acks = latencies.len();
    let payload = (acks * records * record_bytes) as f64;
    let pct = |p: f64| latencies[((acks as f64 - 1.0) * p).round() as usize].as_secs_f64() * 1000.0;
    Ok(Row {
        producers,
        acks,
        mb_per_s: payload / elapsed / (1024.0 * 1024.0),
        p50: pct(0.50),
        p99: pct(0.99),
        p999: pct(0.999),
        fsyncs_per_ack: fsyncs as f64 / acks as f64,
        log_bytes_per_payload_byte: log_bytes as f64 / payload,
        memory,
        leader,
    })
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn wal_produce_throughput() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        std::env::var("CAMUS_MINIO").ok().as_deref(),
        Some("1"),
        "needs MinIO"
    );
    let seconds = std::env::var("CAMUS_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let producers = std::env::var("CAMUS_BENCH_PRODUCERS")
        .unwrap_or_else(|_| "1,8,32".into())
        .split(',')
        .map(|value| value.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "| Producers | Acks | MiB/s | p50 ms | p99 ms | p99.9 ms | fsyncs/ack | log bytes/payload byte |"
    );
    println!("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    let mut memory = Vec::new();
    for count in producers {
        let row = run(count, seconds)?;
        println!(
            "| {} | {} | {:.1} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
            row.producers,
            row.acks,
            row.mb_per_s,
            row.p50,
            row.p99,
            row.p999,
            row.fsyncs_per_ack,
            row.log_bytes_per_payload_byte,
        );
        if let Some(sampled) = &row.memory {
            memory.extend(sampled.rows(&format!("{} producers", row.producers), Some(row.leader)));
        }
    }
    if memory.is_empty() {
        // Remote brokers: their memory is not sampled from here.
        return Ok(());
    }
    println!();
    println!("| Run | Broker | Idle MiB | Peak MiB | After MiB |");
    println!("| --- | --- | ---: | ---: | ---: |");
    for line in memory {
        println!("{line}");
    }
    Ok(())
}
