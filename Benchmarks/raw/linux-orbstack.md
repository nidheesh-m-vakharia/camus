Linux 7.0.14-orbstack-00380-ga7e0a2dc9535 aarch64
12
               total        used        free      shared  buff/cache   available
Mem:           12016         859        9469          22        1928       11156
/dev/vdb1        32G   29G  3.5G  90% /data
=== allocator: system

| Mode | Samples | p50 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- |
| `wal` | 2000 | 7.10 | 16.55 | 211.06 |
| `r2` | 100 | 293.81 | 576.16 | 593.43 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| latency `wal` | 1 (leader) | 40.6 | 75.3 | 75.7 |
| latency `wal` | 2 | 24.5 | 54.9 | 55.1 |
| latency `wal` | 3 | 24.5 | 54.9 | 56.2 |
| latency `r2` | 1 (leader) | 40.5 | 43.3 | 43.4 |
| latency `r2` | 2 | 24.6 | 25.3 | 25.3 |
| latency `r2` | 3 | 24.5 | 25.2 | 25.2 |

--- throughput brokers=3

| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
producers=1: append RPC 5.39 ms on the leader, 5.21 ms handling on a follower
| 1 | 1013 | 1.6 | 9.27 | 22.08 | 1.03 | 1.07 |
producers=8: append RPC 6.79 ms on the leader, 6.51 ms handling on a follower
| 8 | 3502 | 5.5 | 21.41 | 53.58 | 0.25 | 1.07 |
producers=32: append RPC 14.44 ms on the leader, 10.56 ms handling on a follower
| 32 | 6853 | 10.7 | 34.13 | 130.03 | 0.06 | 1.07 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 (leader) | 40.4 | 61.3 | 62.0 |
| 1 producers | 2 | 24.6 | 40.0 | 40.3 |
| 1 producers | 3 | 24.7 | 38.1 | 38.7 |
| 8 producers | 1 (leader) | 42.4 | 104.4 | 104.4 |
| 8 producers | 2 | 24.7 | 41.2 | 41.2 |
| 8 producers | 3 | 24.6 | 40.6 | 40.6 |
| 32 producers | 1 (leader) | 42.3 | 123.6 | 116.2 |
| 32 producers | 2 | 24.7 | 44.6 | 43.0 |
| 32 producers | 3 | 24.6 | 44.1 | 44.1 |

--- throughput brokers=1

| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2229 | 3.5 | 4.08 | 14.93 | 1.01 | 1.07 |
| 8 | 8090 | 12.6 | 8.77 | 42.28 | 0.24 | 1.07 |
| 32 | 16026 | 24.7 | 15.32 | 136.03 | 0.06 | 1.07 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 (leader) | 25.0 | 63.6 | 65.6 |
| 8 producers | 1 (leader) | 25.0 | 148.7 | 137.8 |
| 32 producers | 1 (leader) | 25.0 | 200.3 | 188.8 |

=== allocator: --features mimalloc

thread 'wal_versus_r2_produce_latency' (17635) panicked at crates/broker/tests/latency.rs:78:9:

--- throughput brokers=3

| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
producers=1: append RPC 7.50 ms on the leader, 7.23 ms handling on a follower
| 1 | 752 | 1.2 | 12.42 | 29.84 | 1.04 | 1.07 |
producers=8: append RPC 8.35 ms on the leader, 8.01 ms handling on a follower
| 8 | 2853 | 4.5 | 24.20 | 112.53 | 0.26 | 1.07 |
producers=32: append RPC 13.79 ms on the leader, 11.84 ms handling on a follower
| 32 | 6742 | 10.5 | 37.48 | 176.25 | 0.06 | 1.07 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 (leader) | 71.9 | 136.1 | 125.6 |
| 1 producers | 2 | 51.3 | 86.8 | 71.5 |
| 1 producers | 3 | 49.3 | 85.0 | 78.5 |
| 8 producers | 1 (leader) | 73.4 | 199.3 | 166.9 |
| 8 producers | 2 | 49.3 | 145.2 | 126.1 |
| 8 producers | 3 | 49.3 | 134.8 | 120.5 |
| 32 producers | 1 (leader) | 76.8 | 283.9 | 212.9 |
| 32 producers | 2 | 49.3 | 163.5 | 135.3 |
| 32 producers | 3 | 50.8 | 172.6 | 142.9 |

--- throughput brokers=1

| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1944 | 3.0 | 2.94 | 28.36 | 1.01 | 1.07 |
| 8 | 9516 | 14.9 | 6.90 | 46.48 | 0.24 | 1.07 |
| 32 | 14497 | 22.6 | 16.24 | 171.61 | 0.06 | 1.07 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 (leader) | 49.7 | 136.4 | 138.5 |
| 8 producers | 1 (leader) | 49.7 | 264.4 | 187.3 |
| 32 producers | 1 (leader) | 49.7 | 325.1 | 220.4 |
