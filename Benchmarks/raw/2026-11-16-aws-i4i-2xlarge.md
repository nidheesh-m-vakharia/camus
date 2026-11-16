```text
Brokers: 3 x i4i.2xlarge, us-east-1a: Intel Xeon Platinum 8375C @ 2.90GHz, 8 vCPU, 64 GiB,
  1.7 TB local NVMe instance storage (ext4, noatime) at /data; Ubuntu 24.04, kernel 7.0.0-1014-aws
Client: 1 x c6i.2xlarge in the same zone. Object store: S3 us-east-1, virtual-hosted.
Remote mode: CAMUS_BENCH_CLUSTER, CAMUS_BENCH_RESET=scripts/bench-reset-ssh.sh
Base commit 7d92263 plus the remote-mode harness in this change
```

=== latency flush 250, 100000 samples
| Mode | Samples | p50 ms | p99 ms | p99.9 ms | p99.99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| `wal` | 100000 | 0.93 | 1.03 | 1.94 | 2.47 | 37.34 |
| `r2` | 100 | 280.13 | 313.27 | - | - | 340.66 |
| `wal` stage | Mean ms | Per |
| --- | ---: | --- |
| Client round trip | 0.93 | ack |
| Produce handler on the leader | 0.79 | request |
| Queued before Raft | 0.01 | batch |
| Raft commit and apply | 0.75 | group |
| Leader log append | 0.20 | append |
| of which waiting for a blocking-pool thread | 0.01 | append |
| of which write and fsync | 0.17 | append |
| Append RPC from the leader | 0.48 | RPC |
| Append handling on a follower | 0.27 | RPC |
| Leader log appends per ack, one blocking-pool hop each | 1.00 | ack |
| `wal` stage | Events of 50 ms or more | Longest ms |
| --- | ---: | ---: |
| Client round trip | 0 | 37.34 |
| Queued before Raft | 0 | 0.04 |
| Raft commit and apply | 0 | 37.13 |
| Leader log: waiting for a blocking-pool thread | 0 | 0.21 |
| Leader log: write and fsync | 0 | 0.75 |
| Append RPC from the leader | 0 | 36.70 |
| Follower: waiting for a blocking-pool thread | 0 | 3.73 |
| Follower: write and fsync | 0 | 6.76 |
| Append handling on a follower | 0 | 6.81 |
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 126.55s
=== latency flush 2000
| Mode | Samples | p50 ms | p99 ms | p99.9 ms | p99.99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| `wal` | 20000 | 0.93 | 1.02 | 1.45 | 5.16 | 6.51 |
| `r2` | 100 | 2030.28 | 2050.39 | - | - | 2079.01 |
| `wal` stage | Mean ms | Per |
| --- | ---: | --- |
| Client round trip | 0.93 | ack |
| Produce handler on the leader | 0.79 | request |
| Queued before Raft | 0.00 | batch |
| Raft commit and apply | 0.75 | group |
| Leader log append | 0.20 | append |
| of which waiting for a blocking-pool thread | 0.01 | append |
| of which write and fsync | 0.17 | append |
| Append RPC from the leader | 0.47 | RPC |
| Append handling on a follower | 0.27 | RPC |
| Leader log appends per ack, one blocking-pool hop each | 1.00 | ack |
| `wal` stage | Events of 50 ms or more | Longest ms |
| --- | ---: | ---: |
| Client round trip | 0 | 6.51 |
| Queued before Raft | 0 | 0.03 |
| Raft commit and apply | 0 | 6.33 |
| Leader log: waiting for a blocking-pool thread | 0 | 0.20 |
| Leader log: write and fsync | 0 | 1.02 |
| Append RPC from the leader | 0 | 5.48 |
| Follower: waiting for a blocking-pool thread | 0 | 4.52 |
| Follower: write and fsync | 0 | 3.10 |
| Append handling on a follower | 0 | 5.38 |
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 233.80s
=== throughput 1,8,32 x 16 x 1024
| Producers | Acks | MiB/s | p50 ms | p99 ms | p99.9 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
producers=1: leader log append waits 0.011 ms for a blocking-pool thread on average, 0.16 ms at most
producers=1: append RPC 0.50 ms on the leader, 0.27 ms handling on a follower
| 1 | 9036 | 14.1 | 1.10 | 1.26 | 2.03 | 1.00 | 1.07 |
producers=8: leader log append waits 0.012 ms for a blocking-pool thread on average, 0.08 ms at most
producers=8: append RPC 0.75 ms on the leader, 0.44 ms handling on a follower
| 8 | 27720 | 43.3 | 2.84 | 3.84 | 4.69 | 0.25 | 1.07 |
producers=32: leader log append waits 0.014 ms for a blocking-pool thread on average, 0.30 ms at most
producers=32: append RPC 1.56 ms on the leader, 0.99 ms handling on a follower
| 32 | 44377 | 69.3 | 7.09 | 10.05 | 12.31 | 0.06 | 1.07 |
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 32.22s
=== iggy-style: 1 producer, 250 x 1000, 30s
| Producers | Acks | MiB/s | p50 ms | p99 ms | p99.9 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
producers=1: leader log append waits 0.013 ms for a blocking-pool thread on average, 0.25 ms at most
producers=1: append RPC 1.53 ms on the leader, 1.00 ms handling on a follower
| 1 | 8952 | 71.1 | 3.26 | 4.85 | 17.71 | 1.01 | 1.07 |
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 30.82s
