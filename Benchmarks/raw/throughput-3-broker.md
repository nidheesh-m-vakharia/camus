
| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
producers=1: append RPC 5.96 ms on the leader, 5.75 ms handling on a follower
| 1 | 839 | 1.3 | 11.06 | 32.21 | 1.03 | 1.07 |
producers=8: append RPC 6.61 ms on the leader, 6.41 ms handling on a follower
| 8 | 3105 | 4.8 | 24.06 | 51.47 | 0.26 | 1.07 |
producers=32: append RPC 8.48 ms on the leader, 8.18 ms handling on a follower
| 32 | 9825 | 15.3 | 30.05 | 60.23 | 0.06 | 1.07 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 (leader) | 20.6 | 29.9 | 27.5 |
| 1 producers | 2 | 19.1 | 33.4 | 29.4 |
| 1 producers | 3 | 19.1 | 33.9 | 29.9 |
| 8 producers | 1 (leader) | 21.4 | 47.0 | 47.1 |
| 8 producers | 2 | 19.0 | 36.4 | 36.4 |
| 8 producers | 3 | 19.8 | 37.0 | 37.0 |
| 32 producers | 1 (leader) | 20.7 | 232.1 | 159.5 |
| 32 producers | 2 | 19.9 | 53.6 | 52.6 |
| 32 producers | 3 | 19.8 | 51.9 | 51.1 |


