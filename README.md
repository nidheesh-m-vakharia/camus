# Camus

A Kafka-compatible streaming broker written in Rust. Producers, consumers, and
admin clients speak the Apache Kafka wire protocol. Long-term data lives in
Cloudflare R2. There is no JVM, no ZooKeeper, and no external database.

Status: **Phase 2 is done: `wal` and `r2` produce and fetch are in the
tree.** Topics default to `wal`: a produce acks once a majority of the WAL
shard's voters has fsynced the batch, and the R2 upload follows. `r2` topics
ack after the object is stored and the metadata group has committed the
offset. `kafka-clients` 4.4.0, librdkafka, and franz-go pass both, over
PLAINTEXT and SASL_SSL. Losing the shard leader or a follower loses no
acknowledged offset; R2 outages throttle produce instead of dropping acks.
Consumer groups are Phase 3.
The plan and ADRs are in `docs/`. License: Apache-2.0.

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
3. [docs/compatibility.md](docs/compatibility.md) — pinned Kafka tag and the API matrix.

## Phase 0

```
cargo test --workspace
docker compose up -d minio create-bucket
CAMUS_MINIO=1 cargo test -p camus-storage --test minio
```

`cargo test -p camus-broker --test cluster` starts three broker processes,
commits a metadata write, kills the leader, and checks that a new write
commits. The MinIO image is built locally from the archived community
release (`deploy/minio`), because Docker Hub and dl.min.io no longer serve
it. `docker compose --profile brokers up` builds the broker image as well.

## Layout

A Cargo workspace: `protocol`, `broker`, `raft`, `metadata`, `wal`, `storage`,
`uploader`, `coordinator`, `compactor`, `cli`, plus `engine-bench`.
`docker-compose.yml` runs MinIO as the R2 stand-in. The broker exposes
`/metrics`. `wal`, `uploader`, `coordinator`, and `compactor` are empty
crates. `camus` (the cli) exits 2.
