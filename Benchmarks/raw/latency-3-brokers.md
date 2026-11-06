
| Mode | Samples | p50 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- |
| `wal` | 2000 | 11.06 | 31.06 | 38.03 |
| `r2` | 100 | 278.40 | 284.75 | 285.28 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| latency `wal` | 1 (leader) | 20.5 | 30.8 | 30.9 |
| latency `wal` | 2 | 19.1 | 57.0 | 57.2 |
| latency `wal` | 3 | 19.1 | 57.9 | 58.3 |
| latency `r2` | 1 (leader) | 20.5 | 22.6 | 22.6 |
| latency `r2` | 2 | 20.1 | 20.7 | 20.7 |
| latency `r2` | 3 | 19.0 | 19.5 | 19.5 |


