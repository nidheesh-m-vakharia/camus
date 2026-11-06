# Benchmarks

2026-11-05. Apple M4 Pro, 24 GiB, macOS 26.6, APFS on the internal SSD.
Release build, commit `97cd9f3` plus the memory sampling in this change.
MinIO in Docker on the same machine. All brokers share one SSD, and macOS
`fsync` is a full device flush, so multi-broker numbers are pessimistic.

`memory.html` charts the memory results. `raw/` has the benchmark output.

## How to run

```text
CAMUS_MINIO=1 cargo test --release -p camus-broker --test latency -- --ignored --nocapture
CAMUS_MINIO=1 cargo test --release -p camus-broker --test throughput -- --ignored --nocapture
CAMUS_BENCH_BROKERS=1 CAMUS_MINIO=1 cargo test --release -p camus-broker --test throughput -- --ignored --nocapture
```

Memory is resident set size (RSS) from `ps`, per broker process.

- **Idle**: cluster formed and topic created, nothing produced. Mean over 3s.
- **Peak**: highest reading during the run, sampled every 20ms. A spike
  shorter than 20ms can be missed.
- **After**: mean over 2s, starting 3s after the load stopped.

## Latency, 3 brokers, one producer, 1 KiB `acks=all`

| Mode | Samples | p50 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| `wal` | 2000 | 11.06 | 31.06 | 38.03 |
| `r2` | 100 | 278.40 | 284.75 | 285.28 |

## Throughput, 16 × 1 KiB records per batch, `acks=all`, 10s

| Brokers | Producers | MiB/s | p50 ms | p99 ms | fsyncs/ack |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 1 | 1.3 | 11.06 | 32.21 | 1.03 |
| 3 | 8 | 4.8 | 24.06 | 51.47 | 0.26 |
| 3 | 32 | 15.3 | 30.05 | 60.23 | 0.06 |
| 1 | 1 | 2.6 | 5.65 | 12.53 | 1.02 |
| 1 | 8 | 12.8 | 9.07 | 20.84 | 0.25 |
| 1 | 32 | 42.2 | 11.09 | 22.60 | 0.06 |

## Memory, MiB

The broker serving produce (the shard leader). `raw/` has every broker.

| Run | Idle | Peak | After |
| --- | ---: | ---: | ---: |
| latency `wal`, 3 brokers | 20.5 | 30.8 | 30.9 |
| latency `r2`, 3 brokers | 20.5 | 22.6 | 22.6 |
| 3 brokers, 1 producer | 20.6 | 29.9 | 27.5 |
| 3 brokers, 8 producers | 21.4 | 47.0 | 47.1 |
| 3 brokers, 32 producers | 20.7 | 232.1 | 159.5 |
| 1 broker, 1 producer | 19.4 | 34.5 | 34.5 |
| 1 broker, 8 producers | 19.4 | 202.4 | 174.2 |
| 1 broker, 32 producers | 20.2 | 652.0 | 144.4 |

Followers peaked at 52–58 MiB in the heaviest runs.

## Findings

- **Idle is about 20 MiB per broker**, in every run.
- **Peak grows with throughput on the broker serving produce.** The worst
  was 652 MiB on one broker at 42 MiB/s.
- **It is not a leak.** After a 1-broker, 32-producer run the process held
  546 MiB resident, but `heap` counted 71 MiB of live allocations. `vmmap`
  showed the rest as freed memory macOS malloc keeps resident: 322 MiB in
  large blocks and 97 MiB in small ones.
- **Likely cause, not yet proven**: the uploader packs the whole unsealed
  backlog into one object, with no cap. ADR 0006 says 8 MiB. During the run
  the backlog reached 60 MiB, and each pass allocates it more than once.

## Next

- Enforce the 8 MiB upload cap, then rerun the memory numbers.
- Try mimalloc or jemalloc, which return freed memory sooner.
- Repeat on Linux with one disk per broker.
