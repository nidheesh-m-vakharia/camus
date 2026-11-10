# Benchmarks

The newest results are first. The 2026-11-05 baseline is kept below,
unchanged. `raw/` has the benchmark output for both.

## 2026-11-10, commit `56f0c3a`

Apple M4 Pro, macOS, APFS on the internal SSD. Release build, commit
`56f0c3a`. MinIO in Docker on the same machine, machine otherwise idle,
each benchmark run once. Output in `raw/2026-11-10-macos.md`.

### Latency, 3 brokers, one producer, 1 KiB `acks=all`

| Mode | Samples | p50 ms | p99 ms | max ms | Baseline p50 / p99 / max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `wal` | 2000 | 11.11 | 27.07 | 42.96 | 11.06 / 31.06 / 38.03 |
| `r2` | 100 | 265.82 | 280.20 | 284.94 | 278.40 / 284.75 / 285.28 |

### Throughput, 16 × 1 KiB records per batch, `acks=all`, 10s

3-broker runs use one WAL shard.

| Brokers | Producers | MiB/s | p50 ms | p99 ms | fsyncs/ack | Baseline MiB/s | Baseline p50 / p99 | Baseline fsyncs/ack |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 1 | 1.3 | 11.13 | 28.96 | 1.03 | 1.3 | 11.06 / 32.21 | 1.03 |
| 3 | 8 | 4.1 | 29.17 | 49.03 | 0.39 | 4.8 | 24.06 / 51.47 | 0.26 |
| 3 | 32 | 15.6 | 30.92 | 51.96 | 0.10 | 15.3 | 30.05 / 60.23 | 0.06 |
| 1 | 1 | 3.8 | 4.00 | 10.08 | 1.01 | 2.6 | 5.65 / 12.53 | 1.02 |
| 1 | 8 | 9.2 | 14.06 | 24.06 | 0.43 | 12.8 | 9.07 / 20.84 | 0.25 |
| 1 | 32 | 27.7 | 17.98 | 28.23 | 0.12 | 42.2 | 11.09 / 22.60 | 0.06 |

**Throughput regression, not caused by #117–#128.** The 1-broker 8- and
32-producer runs and the 3-broker 8-producer run are slower than the
baseline. Main just before #117 (commit `9687151`) measured the same:
8.9 and 9.4 MiB/s at 8 producers, 27.2 and 28.6 MiB/s at 32, on 1 broker,
with the same fsyncs/ack. The drop came in between the baseline and #93.
It is being bisected in [#129](https://github.com/nidheesh-m-vakharia/camus/issues/129).

### Memory, MiB, broker serving produce

| Run | Peak | After | Baseline peak |
| --- | ---: | ---: | ---: |
| latency `wal`, 3 brokers | 29.3 | 29.3 | 30.8 |
| latency `r2`, 3 brokers | 22.4 | | 22.6 |
| 3 brokers, 1 producer | 37.2 | | 29.9 |
| 3 brokers, 8 producers | 41.6 | | 47.0 |
| 3 brokers, 32 producers | 177.7 | 152.8 | 232.1 |
| 1 broker, 1 producer | 45.2 | | 34.5 |
| 1 broker, 8 producers | 148.2 | 151.5 | 202.4 |
| 1 broker, 32 producers | 273.6 | 263.2 | 652.0 |

Idle was 20.7 MiB in the `wal` latency run. Followers peaked at
36.7–38.4 MiB in the heaviest 3-broker run (baseline 52–58 MiB).

### New benchmarks, #117–#128

Each "before" is from that PR's own run, on a different day. Treat the
comparison as indicative.

| Benchmark | Now | Before |
| --- | --- | --- |
| Produce to 8 partitions in one request, 1 broker, p50 / p99 | 4.04 / 10.99 ms | 34.26 / 49.95 ms (#117) |
| Ack to a waiting consumer, 200 samples, p50 / p90 / p99 / max | 0.18 / 0.31 / 1.56 / 4.00 ms | 11.35 / 19.68 / 22.34 / 22.60 ms (#122) |
| One connection, 5 produces in flight | 572 acks/s | about 225 acks/s (#123) |
| Metadata writes, 64 concurrent writers × 200 | 5349 writes/s | about 266 writes/s (#127) |
| 3000 sealed extents: partition row | 130 bytes | 413 KB (#119) |
| 3000 sealed extents: fetch newest sealed, p50 / p99 | 0.15 / 0.35 ms | 57.09 / 72.76 ms (#119) |
| 3000 sealed extents: produce, p50 / p99 | 4.98 / 12.38 ms | |
| 3000 sealed extents: catch-up read of all 3000 batches | 103 ms cold, 2 ms from the cache | 259 ms cold, 261 ms again (#125) |

- **8 partitions in one request**: `latency.rs` with
  `CAMUS_BENCH_PARTITIONS=8 CAMUS_BENCH_BROKERS=1`. One produce request
  carries a batch for each of 8 partitions.
- **Ack to a waiting consumer**: `tail_latency.rs`. A consumer long-polls
  at the high watermark while a producer sends one batch every 50 ms. Each
  sample is the time from the produce's ack to the fetch returning that
  batch.
- **5 produces in flight**: `pipelined_throughput.rs`. One connection
  keeps five `acks=all` produces in flight, as a Kafka client does by
  default (`max.in.flight.requests.per.connection`).
- **Metadata writes**: `write_throughput.rs` in `camus-metadata`. 64
  concurrent writers, 200 writes each, as consumer groups committing
  offsets make them.
- **3000 sealed extents**: `extent_cost.rs`. Uploads run every
  millisecond, so each acked batch is its own extent. Then it measures the
  partition's metadata row, a fetch of the newest sealed extent, produce
  latency, and a catch-up read of all 3000 batches, cold and again.

Linux results for some of these are in the descriptions of #118 and #120.
They were run in an OrbStack VM and are relative comparisons only, so they
are not in this table.

## How to run

```text
CAMUS_MINIO=1 cargo test --release -p camus-broker --test latency -- --ignored --nocapture
CAMUS_MINIO=1 cargo test --release -p camus-broker --test throughput -- --ignored --nocapture
CAMUS_BENCH_BROKERS=1 CAMUS_MINIO=1 cargo test --release -p camus-broker --test throughput -- --ignored --nocapture
CAMUS_MINIO=1 CAMUS_BENCH_BROKERS=1 CAMUS_BENCH_PARTITIONS=8 cargo test --release -p camus-broker --test latency -- --ignored --nocapture
CAMUS_MINIO=1 cargo test --release -p camus-broker --test extent_cost -- --ignored --nocapture
cargo test --release -p camus-broker --test tail_latency -- --ignored --nocapture
cargo test --release -p camus-broker --test pipelined_throughput -- --ignored --nocapture
cargo test --release -p camus-metadata --test write_throughput -- --ignored --nocapture
```

Memory is resident set size (RSS) from `ps`, per broker process.

- **Idle**: cluster formed and topic created, nothing produced. Mean over 3s.
- **Peak**: highest reading during the run, sampled every 20ms. A spike
  shorter than 20ms can be missed.
- **After**: mean over 2s, starting 3s after the load stopped.

## Baseline: 2026-11-05, commit `97cd9f3`

Apple M4 Pro, 24 GiB, macOS 26.6, APFS on the internal SSD.
Release build, commit `97cd9f3` plus the memory sampling in this change.
MinIO in Docker on the same machine. All brokers share one SSD, and macOS
`fsync` is a full device flush, so multi-broker numbers are pessimistic.

`memory.html` charts the memory results. `raw/` has the benchmark output.

### Latency, 3 brokers, one producer, 1 KiB `acks=all`

| Mode | Samples | p50 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| `wal` | 2000 | 11.06 | 31.06 | 38.03 |
| `r2` | 100 | 278.40 | 284.75 | 285.28 |

### Throughput, 16 × 1 KiB records per batch, `acks=all`, 10s

| Brokers | Producers | MiB/s | p50 ms | p99 ms | fsyncs/ack |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 1 | 1.3 | 11.06 | 32.21 | 1.03 |
| 3 | 8 | 4.8 | 24.06 | 51.47 | 0.26 |
| 3 | 32 | 15.3 | 30.05 | 60.23 | 0.06 |
| 1 | 1 | 2.6 | 5.65 | 12.53 | 1.02 |
| 1 | 8 | 12.8 | 9.07 | 20.84 | 0.25 |
| 1 | 32 | 42.2 | 11.09 | 22.60 | 0.06 |

### Memory, MiB

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

### Findings

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

### After the upload changes

The uploader now takes at most 32 MiB per pass, in commit order, as up to
four objects of at most 8 MiB put in parallel, then one publish and one
seal. Same machine, 32 producers:

| | Before | Single 8 MiB object | Parallel objects |
| --- | ---: | ---: | ---: |
| 1 broker, peak MiB | 652.0 | 277.0 | 320.0 |
| 1 broker, upload backlog | up to ~60 MiB | 160 MiB and growing | 10–22 MiB, steady |
| 1 broker, MiB/s | 42.2 | 35.4 | 35.0 |
| 3-broker leader, peak MiB | 232.1 | 185.0 | 206.2 |
| 3 brokers, MiB/s | 15.3 | 13.8 | 11.9 |

The earlier throughput was higher only because uploads fell behind and the
work was deferred. With uploads keeping pace, MinIO's writes share the one
SSD with every WAL `fsync`, and the follower's AppendEntries went from 8.5
to 9.9 ms. The new numbers are the sustained rate on this machine.

### Allocator: mimalloc

Tried behind a build feature and removed. On macOS it changed peaks by
-8 to +30 MiB and roughly doubled follower peaks. On Linux idle rose from
25 to 50–77 MiB and every peak was higher. It is also C, and the default
build is meant to be pure Rust.

### Linux

`raw/linux-orbstack.md`. Linux 7.0 (aarch64) in an OrbStack VM on the same
Mac, 12 CPUs, 12 GiB, broker data on a Docker volume, MinIO on the Mac
reached over host networking, system allocator, same commit. The VM's
virtual disk may not pass every `fsync` through to the SSD, so these
latencies can be optimistic.

| Run | p50 ms | p99 ms | MiB/s |
| --- | ---: | ---: | ---: |
| latency `wal`, 3 brokers | 7.10 | 16.55 | |
| latency `r2`, 3 brokers | 293.81 | 576.16 | |
| 3 brokers, 1 producer | 9.27 | 22.08 | 1.6 |
| 3 brokers, 8 producers | 21.41 | 53.58 | 5.5 |
| 3 brokers, 32 producers | 34.13 | 130.03 | 10.7 |
| 1 broker, 1 producer | 4.08 | 14.93 | 3.5 |
| 1 broker, 8 producers | 8.77 | 42.28 | 12.6 |
| 1 broker, 32 producers | 15.32 | 136.03 | 24.7 |

The `wal` latency run had one 211 ms outlier in 2000.

| Run (broker serving produce) | Idle MiB | Peak MiB | After MiB |
| --- | ---: | ---: | ---: |
| 3 brokers, 32 producers, leader | 42.3 | 123.6 | 116.2 |
| 3 brokers, 32 producers, followers | 24.7 | 44.6 | 44.1 |
| 1 broker, 32 producers | 25.0 | 200.3 | 188.8 |

Followers idle at about 25 MiB on Linux, the leader at about 40 MiB.

### Next

- Repeat on Linux machines with one disk per broker and R2 instead of a
  local MinIO.
- `fdatasync` with preallocated segments, measured on Linux; on macOS both
  calls do a full device flush.
