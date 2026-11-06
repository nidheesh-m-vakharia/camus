
| Producers | Acks | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes/payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1697 | 2.6 | 5.65 | 12.53 | 1.02 | 1.07 |
| 8 | 8200 | 12.8 | 9.07 | 20.84 | 0.25 | 1.07 |
| 32 | 27024 | 42.2 | 11.09 | 22.60 | 0.06 | 1.07 |

| Run | Broker | Idle MiB | Peak MiB | After MiB |
| --- | --- | ---: | ---: | ---: |
| 1 producers | 1 (leader) | 19.4 | 34.5 | 34.5 |
| 8 producers | 1 (leader) | 19.4 | 202.4 | 174.2 |
| 32 producers | 1 (leader) | 20.2 | 652.0 | 144.4 |


