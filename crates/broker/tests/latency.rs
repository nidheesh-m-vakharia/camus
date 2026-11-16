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
//! `CAMUS_BENCH_WAL` (default 20,000) and `CAMUS_BENCH_R2` (default 100) set
//! the sample counts. p99.9 needs 1,000 samples and p99.99 needs 10,000;
//! with fewer, the table shows `-`. `CAMUS_BENCH_FLUSH_MS` sets the upload
//! interval (default 250, the production value). Two more tables split a
//! `wal` ack into stages from the brokers' admin counters: mean time per
//! stage, and how many events in each stage took 50 ms or more.
//! `CAMUS_BENCH_PARTITIONS` sends one batch to each of that many partitions
//! in every request, as a client batching for one broker does. Every
//! partition must be led by the broker the producer dials, so use it with
//! `CAMUS_BENCH_BROKERS=1`.

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

fn produce_on(stream: &mut TcpStream, value: &str, partitions: i32) -> i16 {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = (0..partitions)
        .map(|index| {
            PartitionProduceData::default()
                .with_index(index)
                .with_records(Some(batch(value)))
        })
        .collect();
    send(
        stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(30_000)
            .with_topic_data(vec![topic]),
    );
    recv::<ProduceResponse>(stream, 9).responses[0]
        .partition_responses
        .iter()
        .map(|part| part.error_code)
        .find(|code| *code != 0)
        .unwrap_or(0)
}

/// Counters a broker's admin endpoint reports for the stages of a `wal` ack.
const STAGE_COUNTERS: [&str; 26] = [
    "produce_count",
    "produce_us",
    "wal_queued_count",
    "wal_queued_us",
    "wal_queued_slow",
    "wal_queued_max_us",
    "wal_committed_count",
    "wal_committed_us",
    "wal_committed_slow",
    "wal_committed_max_us",
    "wal_log_fsyncs",
    "wal_log_append_us",
    "wal_log_pool_wait_us",
    "wal_log_pool_wait_slow",
    "wal_log_pool_wait_max_us",
    "wal_log_io_us",
    "wal_log_io_slow",
    "wal_log_io_max_us",
    "wal_rpc_sent_count",
    "wal_rpc_sent_us",
    "wal_rpc_sent_slow",
    "wal_rpc_sent_max_us",
    "wal_rpc_handled_count",
    "wal_rpc_handled_us",
    "wal_rpc_handled_slow",
    "wal_rpc_handled_max_us",
];

/// A slow event, as the brokers count it (`camus_wal::SLOW_US`).
const SLOW: Duration = Duration::from_millis(50);

/// Each of [`STAGE_COUNTERS`] on `http`, or 0 when it is missing.
fn stage_counters(http: &str) -> Vec<u64> {
    let values = admin(http).unwrap_or_default();
    STAGE_COUNTERS
        .iter()
        .map(|name| values[*name].as_u64().unwrap_or(0))
        .collect()
}

/// How much each counter grew between two readings, by name.
struct Stages(std::collections::HashMap<&'static str, u64>);

impl Stages {
    fn between(before: &[u64], after: &[u64]) -> Self {
        Self(
            STAGE_COUNTERS
                .iter()
                .zip(before.iter().zip(after))
                .map(|(name, (before, after))| {
                    // A maximum is the broker's since it started, not a count.
                    let grew = if name.ends_with("_max_us") {
                        *after
                    } else {
                        after.saturating_sub(*before)
                    };
                    (*name, grew)
                })
                .collect(),
        )
    }

    fn get(&self, name: &str) -> u64 {
        self.0.get(name).copied().unwrap_or(0)
    }

    /// Mean milliseconds per event: `us` over `count`.
    fn mean_ms(&self, us: &str, count: &str) -> f64 {
        self.get(us) as f64 / self.get(count).max(1) as f64 / 1000.0
    }

    fn add(&mut self, other: &Stages) {
        for (name, value) in &other.0 {
            let mine = self.0.entry(name).or_insert(0);
            if name.ends_with("_max_us") {
                *mine = (*mine).max(*value);
            } else {
                *mine += value;
            }
        }
    }

    /// A stage's slow count and its longest time in milliseconds.
    fn slowest(&self, stage: &str) -> (u64, f64) {
        (
            self.get(&format!("{stage}_slow")),
            self.get(&format!("{stage}_max_us")) as f64 / 1000.0,
        )
    }
}

/// One mode's samples, sorted, and what the brokers counted while they ran.
struct Run {
    sorted: Vec<Duration>,
    memory: Memory,
    leader: i32,
    /// The shard leader's counters.
    on_leader: Stages,
    /// The followers' counters, summed.
    on_followers: Stages,
}

fn run(mode: &'static str, warmup: usize, samples: usize, opts: Opts) -> Run {
    let root = tempfile::tempdir().unwrap();
    let brokers = count("CAMUS_BENCH_BROKERS", 3) as i32;
    let nodes: Vec<Node> = (1..=brokers)
        .map(|id| Node::with(id, &root.path().join(format!("node-{id}")), opts.clone()))
        .collect();
    init(&nodes, &(1..=brokers).collect::<Vec<_>>());
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    let controller = nodes.iter().find(|node| node.id == meta_leader).unwrap();
    let partitions = count("CAMUS_BENCH_PARTITIONS", 1) as i32;
    create_topic_with(&kafka_addr(controller), mode, partitions);
    let (leader_id, leader) = partition_leader(&kafka_addr(&nodes[0]), None);
    let pids: Vec<u32> = nodes.iter().map(|node| node.child.id()).collect();
    // Empty: the cluster and topic exist, nothing has been produced.
    let idle = steady_rss(&pids, Duration::from_secs(3));
    let sampler = MemorySampler::start(pids.clone());
    let mut stream = connect(&leader);
    stream.set_nodelay(true).unwrap();
    let value = "x".repeat(1024);
    for _ in 0..warmup {
        assert_eq!(produce_on(&mut stream, &value, partitions), 0);
    }
    let before: Vec<Vec<u64>> = nodes
        .iter()
        .map(|node| stage_counters(&node.http))
        .collect();
    let mut out = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        assert_eq!(produce_on(&mut stream, &value, partitions), 0);
        out.push(started.elapsed());
    }
    let mut on_leader = Stages::between(&[], &[]);
    let mut on_followers = Stages::between(&[], &[]);
    for (node, before) in nodes.iter().zip(&before) {
        let grew = Stages::between(before, &stage_counters(&node.http));
        if node.id == leader_id {
            on_leader.add(&grew);
        } else {
            on_followers.add(&grew);
        }
    }
    let peak = sampler.finish();
    std::thread::sleep(Duration::from_secs(3));
    let after = steady_rss(&pids, Duration::from_secs(2));
    out.sort();
    let memory = Memory {
        ids: nodes.iter().map(|node| node.id).collect(),
        idle,
        peak,
        after,
    };
    Run {
        sorted: out,
        memory,
        leader: leader_id,
        on_leader,
        on_followers,
    }
}

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index].as_secs_f64() * 1000.0
}

/// The `p` percentile in milliseconds, or `-` with fewer than `needed`
/// samples, where it would only repeat the maximum.
fn pct_or_dash(sorted: &[Duration], p: f64, needed: usize) -> String {
    if sorted.len() < needed {
        "-".into()
    } else {
        format!("{:.2}", pct(sorted, p))
    }
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn wal_versus_r2_produce_latency() {
    assert_eq!(
        std::env::var("CAMUS_MINIO").ok().as_deref(),
        Some("1"),
        "needs MinIO"
    );
    // Both modes run with the production upload interval unless overridden.
    let flush = count("CAMUS_BENCH_FLUSH_MS", 250).to_string();
    let wal = run(
        "wal",
        100,
        count("CAMUS_BENCH_WAL", 20_000),
        Opts::default().env("CAMUS_FLUSH_MS", &flush),
    );
    let r2 = run(
        "r2",
        5,
        count("CAMUS_BENCH_R2", 100),
        Opts::default().env("CAMUS_FLUSH_MS", &flush),
    );
    println!("| Mode | Samples | p50 ms | p99 ms | p99.9 ms | p99.99 ms | max ms |");
    println!("| --- | --- | --- | --- | --- | --- | --- |");
    for (mode, run) in [("wal", &wal), ("r2", &r2)] {
        let sorted = &run.sorted;
        let max = sorted.last().map_or(0.0, |max| max.as_secs_f64() * 1000.0);
        println!(
            "| `{mode}` | {} | {:.2} | {:.2} | {} | {} | {max:.2} |",
            sorted.len(),
            pct(sorted, 0.50),
            pct(sorted, 0.99),
            pct_or_dash(sorted, 0.999, 1_000),
            pct_or_dash(sorted, 0.9999, 10_000),
        );
    }
    println!();
    // Means, so the stages of one ack can be compared. Every stage below
    // the produce handler is part of the one above it, except the follower
    // row, which runs during the append RPC.
    let leader = &wal.on_leader;
    let followers = &wal.on_followers;
    let acks = wal.sorted.len().max(1) as f64;
    let client = wal.sorted.iter().map(Duration::as_secs_f64).sum::<f64>() / acks * 1000.0;
    println!("| `wal` stage | Mean ms | Per |");
    println!("| --- | ---: | --- |");
    println!("| Client round trip | {client:.2} | ack |");
    println!(
        "| Produce handler on the leader | {:.2} | request |",
        leader.mean_ms("produce_us", "produce_count")
    );
    println!(
        "| Queued before Raft | {:.2} | batch |",
        leader.mean_ms("wal_queued_us", "wal_queued_count")
    );
    println!(
        "| Raft commit and apply | {:.2} | group |",
        leader.mean_ms("wal_committed_us", "wal_committed_count")
    );
    println!(
        "| Leader log append | {:.2} | append |",
        leader.mean_ms("wal_log_append_us", "wal_log_fsyncs")
    );
    println!(
        "| of which waiting for a blocking-pool thread | {:.2} | append |",
        leader.mean_ms("wal_log_pool_wait_us", "wal_log_fsyncs")
    );
    println!(
        "| of which write and fsync | {:.2} | append |",
        leader.mean_ms("wal_log_io_us", "wal_log_fsyncs")
    );
    println!(
        "| Append RPC from the leader | {:.2} | RPC |",
        leader.mean_ms("wal_rpc_sent_us", "wal_rpc_sent_count")
    );
    println!(
        "| Append handling on a follower | {:.2} | RPC |",
        followers.mean_ms("wal_rpc_handled_us", "wal_rpc_handled_count")
    );
    println!(
        "| Leader log appends per ack, one blocking-pool hop each | {:.2} | ack |",
        leader.get("wal_log_fsyncs") as f64 / acks
    );
    println!();
    // A stage's slow events can explain slow acks only where the counts
    // line up. "Longest" is since the broker started, warmup included.
    let slow_acks = wal.sorted.iter().filter(|ack| **ack >= SLOW).count();
    let longest = wal
        .sorted
        .last()
        .map_or(0.0, |max| max.as_secs_f64() * 1000.0);
    println!(
        "| `wal` stage | Events of {} ms or more | Longest ms |",
        SLOW.as_millis()
    );
    println!("| --- | ---: | ---: |");
    println!("| Client round trip | {slow_acks} | {longest:.2} |");
    for (label, stats, stage) in [
        ("Queued before Raft", leader, "wal_queued"),
        ("Raft commit and apply", leader, "wal_committed"),
        (
            "Leader log: waiting for a blocking-pool thread",
            leader,
            "wal_log_pool_wait",
        ),
        ("Leader log: write and fsync", leader, "wal_log_io"),
        ("Append RPC from the leader", leader, "wal_rpc_sent"),
        (
            "Follower: waiting for a blocking-pool thread",
            followers,
            "wal_log_pool_wait",
        ),
        ("Follower: write and fsync", followers, "wal_log_io"),
        (
            "Append handling on a follower",
            followers,
            "wal_rpc_handled",
        ),
    ] {
        let (slow, longest) = stats.slowest(stage);
        println!("| {label} | {slow} | {longest:.2} |");
    }
    println!();
    println!("| Run | Broker | Idle MiB | Peak MiB | After MiB |");
    println!("| --- | --- | ---: | ---: | ---: |");
    for (mode, run) in [("wal", &wal), ("r2", &r2)] {
        for row in run
            .memory
            .rows(&format!("latency `{mode}`"), Some(run.leader))
        {
            println!("{row}");
        }
    }
}
