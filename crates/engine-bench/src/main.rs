//! Fsync benchmark for ADR 0011.
//!
//! Defaults match the ADR: 10s warmup, 60s measured, one thread then eight,
//! four payload shapes. `CAMUS_BENCH_SECONDS` and `CAMUS_BENCH_WARMUP` shorten
//! a laptop run. The engines are the segment file (what the broker uses),
//! `redb`, and `fjall` (measured, not linked into the broker).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use camus_raft::SegmentLog;
use redb::TableDefinition;

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log");

fn main() {
    let seconds: u64 = std::env::var("CAMUS_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(60);
    let warmup: u64 = std::env::var("CAMUS_BENCH_WARMUP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let threads = if std::env::var("CAMUS_BENCH_THREADS").ok().as_deref() == Some("all") {
        vec![1, 8]
    } else {
        vec![1]
    };
    let measure = Duration::from_secs(seconds);
    let warmup = Duration::from_secs(warmup);
    println!(
        "seconds={seconds} warmup_s={} threads={threads:?}",
        warmup.as_secs()
    );

    let workloads: &[(&str, usize, usize)] = &[
        ("1KiB", 1024, 1),
        ("64KiB", 64 * 1024, 1),
        ("1MiB", 1024 * 1024, 1),
        ("group-32KiBx16", 32 * 1024, 16),
    ];

    for &nthreads in &threads {
        for &(name, size, batch) in workloads {
            let payload = vec![7u8; size];
            report(
                "segment",
                name,
                nthreads,
                run_segment(&payload, batch, nthreads, warmup, measure),
            );
            report(
                "redb",
                name,
                nthreads,
                run_redb(&payload, batch, nthreads, warmup, measure),
            );
            report(
                "fjall",
                name,
                nthreads,
                run_fjall(&payload, batch, nthreads, warmup, measure),
            );
        }
    }
}

struct Stats {
    p50_us: u128,
    p99_us: u128,
    mib_s: f64,
}

fn report(engine: &str, workload: &str, threads: usize, stats: Stats) {
    println!(
        "{engine}\t{workload}\tthreads={threads}\tp50_us={}\tp99_us={}\tmib_s={:.2}",
        stats.p50_us, stats.p99_us, stats.mib_s
    );
}

fn run_segment(
    payload: &[u8],
    batch: usize,
    threads: usize,
    warmup: Duration,
    measure: Duration,
) -> Stats {
    let owned = payload.to_vec();
    let dir = tempfile::tempdir().unwrap();
    let next = AtomicU64::new(0);
    parallel(threads, warmup, measure, owned.len() * batch, || {
        let payload = owned.clone();
        let id = next.fetch_add(1, Ordering::Relaxed);
        let log_dir = dir.path().join(id.to_string());
        let mut log = SegmentLog::open(&log_dir).unwrap();
        let mut index = 1u64;
        move || {
            let records: Vec<(u64, &[u8])> = (0..batch)
                .map(|offset| {
                    let index = index + offset as u64;
                    (index, payload.as_slice())
                })
                .collect();
            // Indexes must be unique per log. Each thread has its own log.
            index += batch as u64;
            log.append_batch(&records).unwrap();
        }
    })
}

fn run_redb(
    payload: &[u8],
    batch: usize,
    threads: usize,
    warmup: Duration,
    measure: Duration,
) -> Stats {
    let owned = payload.to_vec();
    let dir = tempfile::tempdir().unwrap();
    let next = AtomicU64::new(0);
    parallel(threads, warmup, measure, owned.len() * batch, || {
        let payload = owned.clone();
        let id = next.fetch_add(1, Ordering::Relaxed);
        let path = dir.path().join(format!("{id}.redb"));
        let db = redb::Database::create(&path).unwrap();
        let mut index = 1u64;
        move || {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(TABLE).unwrap();
                for offset in 0..batch {
                    table
                        .insert(index + offset as u64, payload.as_slice())
                        .unwrap();
                }
            }
            txn.commit().unwrap();
            index += batch as u64;
        }
    })
}

fn run_fjall(
    payload: &[u8],
    batch: usize,
    threads: usize,
    warmup: Duration,
    measure: Duration,
) -> Stats {
    let owned = payload.to_vec();
    let dir = tempfile::tempdir().unwrap();
    let next = AtomicU64::new(0);
    parallel(threads, warmup, measure, owned.len() * batch, || {
        let payload = owned.clone();
        let id = next.fetch_add(1, Ordering::Relaxed);
        let path = dir.path().join(id.to_string());
        let keyspace = fjall::Config::new(&path).open().unwrap();
        let partition = keyspace.open_partition("log", Default::default()).unwrap();
        let mut index = 1u64;
        move || {
            for offset in 0..batch {
                let key = (index + offset as u64).to_be_bytes();
                partition.insert(key, payload.as_slice()).unwrap();
            }
            keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
            index += batch as u64;
        }
    })
}

fn parallel<F, G>(
    threads: usize,
    warmup: Duration,
    measure: Duration,
    bytes_per_op: usize,
    mut make: G,
) -> Stats
where
    G: FnMut() -> F,
    F: FnMut() + Send + 'static,
{
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..threads {
        let mut op = make();
        handles.push(std::thread::spawn(move || {
            let mut samples = Vec::new();
            while start.elapsed() < warmup {
                op();
            }
            let begin = Instant::now();
            let mut ops = 0u64;
            while begin.elapsed() < measure {
                let tick = Instant::now();
                op();
                samples.push(tick.elapsed().as_micros());
                ops += 1;
            }
            (samples, ops, begin.elapsed())
        }));
    }
    let mut samples = Vec::new();
    let mut ops = 0u64;
    let mut elapsed = Duration::ZERO;
    for handle in handles {
        let (mut local, local_ops, local_elapsed) = handle.join().unwrap();
        samples.append(&mut local);
        ops += local_ops;
        elapsed = elapsed.max(local_elapsed);
    }
    samples.sort_unstable();
    let p50 = percentile(&samples, 50);
    let p99 = percentile(&samples, 99);
    let seconds = elapsed.as_secs_f64().max(0.001);
    let mib_s = (ops as f64 * bytes_per_op as f64) / seconds / (1024.0 * 1024.0);
    Stats {
        p50_us: p50,
        p99_us: p99,
        mib_s,
    }
}

fn percentile(samples: &[u128], pct: usize) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let index = (samples.len().saturating_sub(1) * pct) / 100;
    samples[index]
}

#[allow(dead_code)]
fn _touch(path: &Path) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .unwrap();
    file.write_all(b"x").unwrap();
}
