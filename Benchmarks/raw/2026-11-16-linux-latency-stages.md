```text
Linux 7.2.12-orbstack-00394-gbecdd6130068 aarch64, 12 CPUs, overlay filesystem
Container rust:1-bookworm, --net host, MinIO in Docker on the host
Base commit 527abcc plus the stage counters in this change
CAMUS_MINIO=1 cargo test --release --locked -p camus-broker --test latency -- --ignored --nocapture
```

| Mode | Samples | p50 ms | p99 ms | p99.9 ms | p99.99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| `wal` | 20000 | 3.43 | 13.96 | 27.14 | 202.89 | 280.17 |
| `r2` | 100 | 270.24 | 349.86 | - | - | 389.92 |
| `wal` stage | Mean ms | Per |
| --- | ---: | --- |
| Client round trip | 3.93 | ack |
| Produce handler on the leader | 3.88 | request |
| Queued before Raft | 0.00 | batch |
| Raft commit and apply | 3.85 | group |
| Leader log append | 1.89 | append |
| of which waiting for a blocking-pool thread | 0.01 | append |
| of which write and fsync | 1.87 | append |
| Append RPC from the leader | 2.10 | RPC |
| Append handling on a follower | 2.02 | RPC |
| Leader log appends per ack, one blocking-pool hop each | 1.01 | ack |
| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| latency `wal` | 1 (leader) | 26.0 | 33.0 | 31.7 |
| latency `wal` | 2 | 25.3 | 27.9 | 27.9 |
| latency `wal` | 3 | 25.4 | 28.3 | 28.2 |
| latency `r2` | 1 (leader) | 25.2 | 27.1 | 27.1 |
| latency `r2` | 2 | 25.1 | 25.4 | 25.4 |
| latency `r2` | 3 | 24.7 | 25.2 | 25.2 |

--- slow-ack attribution, same VM, CAMUS_BENCH_R2=20, run after the baseline above

```text
CAMUS_BENCH_FLUSH_MS=$f cargo test --release --locked -p camus-broker --test latency -- --ignored --nocapture   # f = 250, then 2000
```

=== flush 250
| Mode | Samples | p50 ms | p99 ms | p99.9 ms | p99.99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| `wal` | 20000 | 3.39 | 76.38 | 355.69 | 681.16 | 858.01 |
| `r2` | 20 | 270.19 | 279.35 | - | - | 279.35 |
| `wal` stage | Mean ms | Per |
| --- | ---: | --- |
| Client round trip | 8.40 | ack |
| Produce handler on the leader | 8.34 | request |
| Queued before Raft | 0.00 | batch |
| Raft commit and apply | 8.30 | group |
| Leader log append | 4.16 | append |
| of which waiting for a blocking-pool thread | 0.01 | append |
| of which write and fsync | 4.13 | append |
| Append RPC from the leader | 4.57 | RPC |
| Append handling on a follower | 4.11 | RPC |
| Leader log appends per ack, one blocking-pool hop each | 1.01 | ack |
| `wal` stage | Events of 50 ms or more | Longest ms |
| --- | ---: | ---: |
| Client round trip | 376 | 858.01 |
| Queued before Raft | 0 | 0.54 |
| Raft commit and apply | 371 | 857.66 |
| Leader log: waiting for a blocking-pool thread | 0 | 1.46 |
| Leader log: write and fsync | 149 | 603.11 |
| Append RPC from the leader | 354 | 381.81 |
| Follower: waiting for a blocking-pool thread | 0 | 4.22 |
| Follower: write and fsync | 309 | 749.03 |
| Append handling on a follower | 282 | 200.30 |
| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| latency `wal` | 1 (leader) | 26.7 | 31.9 | 25.5 |
| latency `wal` | 2 | 25.4 | 27.8 | 22.0 |
| latency `wal` | 3 | 25.4 | 27.8 | 21.6 |
| latency `r2` | 1 (leader) | 25.8 | 26.8 | 26.8 |
| latency `r2` | 2 | 23.4 | 23.7 | 23.7 |
| latency `r2` | 3 | 24.8 | 25.3 | 25.3 |
=== flush 2000
| Mode | Samples | p50 ms | p99 ms | p99.9 ms | p99.99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| `wal` | 20000 | 2.73 | 17.37 | 104.33 | 551.61 | 1484.23 |
| `r2` | 20 | 2027.38 | 2040.69 | - | - | 2040.69 |
| `wal` stage | Mean ms | Per |
| --- | ---: | --- |
| Client round trip | 3.81 | ack |
| Produce handler on the leader | 3.77 | request |
| Queued before Raft | 0.00 | batch |
| Raft commit and apply | 3.75 | group |
| Leader log append | 1.89 | append |
| of which waiting for a blocking-pool thread | 0.01 | append |
| of which write and fsync | 1.87 | append |
| Append RPC from the leader | 2.00 | RPC |
| Append handling on a follower | 1.86 | RPC |
| Leader log appends per ack, one blocking-pool hop each | 1.00 | ack |
| `wal` stage | Events of 50 ms or more | Longest ms |
| --- | ---: | ---: |
| Client round trip | 106 | 1484.23 |
| Queued before Raft | 0 | 0.09 |
| Raft commit and apply | 106 | 1483.96 |
| Leader log: waiting for a blocking-pool thread | 0 | 3.49 |
| Leader log: write and fsync | 20 | 1473.94 |
| Append RPC from the leader | 49 | 204.23 |
| Follower: waiting for a blocking-pool thread | 0 | 2.78 |
| Follower: write and fsync | 40 | 1480.86 |
| Append handling on a follower | 33 | 186.36 |
| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| latency `wal` | 1 (leader) | 26.3 | 49.0 | 49.0 |
| latency `wal` | 2 | 24.6 | 33.3 | 33.3 |
| latency `wal` | 3 | 25.1 | 34.0 | 34.0 |
| latency `r2` | 1 (leader) | 26.2 | 27.6 | 27.6 |
| latency `r2` | 2 | 24.6 | 25.1 | 25.2 |
| latency `r2` | 3 | 25.1 | 25.4 | 25.4 |

--- throughput, same VM

```text
cargo test --release --locked -p camus-broker --test throughput -- --ignored --nocapture
```

| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
producers=1: leader log append waits 0.011 ms for a blocking-pool thread on average, 0.90 ms at most
producers=1: append RPC 1.67 ms on the leader, 1.61 ms handling on a follower
| 1 | 3202 | 5.0 | 2.98 | 6.81 | 1.01 | 1.07 |
producers=8: leader log append waits 0.025 ms for a blocking-pool thread on average, 0.73 ms at most
producers=8: append RPC 2.23 ms on the leader, 2.14 ms handling on a follower
| 8 | 10050 | 15.7 | 6.99 | 23.28 | 0.25 | 1.07 |
producers=32: leader log append waits 0.045 ms for a blocking-pool thread on average, 1.23 ms at most
producers=32: append RPC 4.34 ms on the leader, 4.14 ms handling on a follower
| 32 | 20508 | 32.0 | 14.37 | 40.32 | 0.06 | 1.07 |
| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 | 23.2 | 25.2 | 25.2 |
| 1 producers | 2 (leader) | 21.3 | 47.4 | 47.4 |
| 1 producers | 3 | 19.2 | 21.7 | 21.7 |
| 8 producers | 1 | 23.3 | 29.2 | 29.2 |
| 8 producers | 2 (leader) | 21.3 | 106.4 | 106.4 |
| 8 producers | 3 | 19.3 | 25.1 | 25.2 |
| 32 producers | 1 | 23.3 | 37.4 | 37.2 |
| 32 producers | 2 (leader) | 20.9 | 174.9 | 174.9 |
| 32 producers | 3 | 19.3 | 33.1 | 31.2 |
