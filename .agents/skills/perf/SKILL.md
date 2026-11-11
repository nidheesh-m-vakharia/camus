---
name: perf
description: Performance work on Camus. Run the benchmark suites, compare against the baseline, keep within the memory budget, and record results in Benchmarks/.
---

# Performance

## Rules

- Measure before and after on the same machine with a release build.
  Report p50, p99, and max, not only the mean.
- Memory is a fixed budget. A new cache, map, or queue needs a cap in bytes
  or entries, set by a `CAMUS_*` variable if operators may tune it. If it
  can't be capped, change the on-disk layout or go zero-copy instead.
- Never move the ack earlier than the durability barrier (ADR 0005).

## Benchmarks

The benchmarks are `#[ignore]` tests. Start MinIO first (`run-cluster`).

```
CAMUS_MINIO=1 cargo test --release -p camus-broker --test <name> -- --ignored --nocapture
```

| Test | Measures |
| --- | --- |
| `latency` | ack latency, one producer, `wal` and `r2` |
| `throughput` | MiB/s and fsyncs per ack with 1, 8, and 32 producers |
| `pipelined_throughput` | many in-flight requests per connection |
| `tail_latency` | p99 and max under load |
| `extent_cost` | object-store requests and bytes |
| `camus-metadata --test write_throughput` | metadata group commit |
| `cargo run --release -p camus-engine-bench` | fsync cost by storage engine (ADR 0011) |

Knobs: `CAMUS_BENCH_SECONDS`, `CAMUS_BENCH_PRODUCERS` (for example `1,8,32`),
`CAMUS_BENCH_BROKERS`. Each test's header comment has the rest.

## Recording results

For a published run, add a dated section at the top of
`Benchmarks/README.md`. Give the machine, OS, filesystem, commit, and that
MinIO ran in local Docker. Use the existing tables, with the baseline columns
next to the new numbers. Put the raw output in `Benchmarks/raw/`. Keep the
older sections unchanged.
