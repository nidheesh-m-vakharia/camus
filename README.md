# Camus

A Kafka-compatible streaming broker written in Rust. Producers, consumers, and
admin clients speak the Apache Kafka wire protocol. Long-term data lives in
Cloudflare R2. There is no JVM, no ZooKeeper, and no external database.

Status: **design accepted, not implemented**. The plan and ADRs are in
`docs/`. No broker code has been written. License: Apache-2.0.

## What a client changes

The bootstrap server address. Java `kafka-clients`, librdkafka (Python, Go,
.NET), franz-go, kafka-python, KafkaJS, and kcat are the compatibility
targets.

## Two durability modes

| Mode | Ack barrier | What you trade |
| --- | --- | --- |
| `wal` (default) | Quorum fsync of a short write-ahead log | Local disk and intra-region replication, for produce latency aimed under 20ms p99 |
| `r2` | The batch is in R2 and its offsets are committed in metadata | Brokers hold no log state. Produce latency is hundreds of milliseconds |

`acks=0` does not wait, in either mode. See [ADR 0005](docs/adr/0005-durability-and-acks.md).

R2 is the long-term source of truth in both modes. The WAL is a buffer of
seconds, then it is uploaded and truncated.

## Read this first

1. [docs/PLAN.md](docs/PLAN.md) — accepted decisions, and what Phase 0 will and will not do.
2. [docs/adr/](docs/adr/) — the decisions, in detail.
3. [docs/compatibility.md](docs/compatibility.md) — pinned Kafka tag and the API matrix. Everything is unimplemented.

## Layout that Phase 0 will create

A Cargo workspace: `protocol`, `broker`, `raft`, `metadata`, `wal`, `storage`,
`uploader`, `coordinator`, `compactor`, `cli`. Plus `docker-compose.yml`
(MinIO standing in for R2), `tests/compat/`, and Prometheus metrics.

Those crates do not exist yet.
