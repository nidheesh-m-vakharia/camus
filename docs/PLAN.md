# Camus plan

Status: accepted. Date: 2026-11-05.

Phase 0 foundations are in the workspace. Produce, Fetch, groups, and the
uploader are not. The ADRs under `docs/adr/` are accepted. This file is
the map of those decisions and of what Phase 0 is allowed to touch.

## Product

Camus is a single Rust binary that accepts Kafka TCP connections and stores
the durable log in Cloudflare R2 (S3-compatible). A deployment is a small set
of those binaries. They replicate metadata, and in `wal` mode a short
write-ahead log, with Raft ([openraft](https://github.com/databendlabs/openraft)).
They do not depend on Kafka, ZooKeeper, KRaft, or a separate database.

Existing clients work by changing the bootstrap address. Camus does not
implement the Kafka Streams RPC, the broker-to-broker Kafka replication
protocol, or the legacy ZooKeeper admin surface. Clients that only use the
public produce, fetch, group, and admin APIs are the target.

## How the pieces fit

```
clients ──Kafka TCP──▶ broker
                         │
                         ├─ metadata Raft group (topics, offsets, groups, txns)
                         │
                         ├─ wal mode: per-shard Raft log ──uploader──▶ R2
                         │                 ▲
                         │                 └── tail fetches
                         │
                         └─ r2 mode: memory buffer ──uploader──▶ R2
                                                    │
                         fetches of sealed data ◀───┘ range GET + cache
```

One metadata Raft group per cluster. In `wal` mode, partitions are grouped
into WAL shards and each shard is its own Raft group. In `r2` mode the WAL
groups are not started. Any broker can serve a sealed offset from R2 once
metadata says which object holds it. Details: [ADR 0001](adr/0001-system-architecture.md),
[ADR 0004](adr/0004-metadata-raft.md), [ADR 0006](adr/0006-wal-shards.md).

## Decisions

| # | Decision | ADR |
| --- | --- | --- |
| 1 | Behavior reference is Apache Kafka **4.4.0**, commit `1156b2752aaa28eafc0b4c4d387e63356793268d`. Where a KIP and that tree disagree, the tree wins. | [0002](adr/0002-kafka-compatibility-pin.md) |
| 2 | Protocol types are generated from that tree's message JSON. `kafka-protocol` 0.18.0 (schemas from Kafka 4.1.0) is the starting generator, then regenerated. Field layouts are not hand-written. | [0003](adr/0003-wire-protocol.md) |
| 3 | Consensus library is **openraft 0.9.25** (stable). Several Raft instances per process, with a router we write. The 0.10 alpha and `openraft-multi` stay out until 0.10 is stable. | [0004](adr/0004-metadata-raft.md) |
| 4 | `acks=1` and `acks=all` both wait for the configured durability barrier. `acks=0` does not. Leader-only acks are not offered, because they can lose data when a minority of replicas fail. | [0005](adr/0005-durability-and-acks.md) |
| 5 | Losing a majority of a WAL shard before upload takes the affected partitions offline. Camus does not silently truncate acked offsets. An operator can later truncate to the R2 prefix explicitly. | [0008](adr/0008-failure-and-recovery.md) |
| 6 | Ingest objects are immutable bundles, one R2 PUT per flush for every partition in the window. R2 allows one concurrent write per key per second and returns 429 above that. The compactor later rewrites cold data into per-partition segments. Recovery rebuilds the log index from object footers. | [0007](adr/0007-r2-layout-and-cost.md) |
| 7 | The broker writes the assigned base offset and leader epoch into each batch. The CRC-covered bytes, including compression, are copied through. | [0009](adr/0009-offsets-and-record-batches.md) |
| 8 | Raft logs are custom append-only segment files. State machines use `redb`. Both are open-source Rust. RocksDB and LMDB are out. Phase 0 benchmarks the segment file; it does not pick a different engine. | [0011](adr/0011-local-log-storage.md) |
| 9 | Dev listeners are PLAINTEXT and must be opted into. The durable target for Phase 1 is SASL/PLAIN over TLS. | [0012](adr/0012-auth.md) |
| 10 | Work lands in phases 0–5. A phase is done only when its acceptance tests pass. Unimplemented APIs return a Kafka error and a row in `compatibility.md`. They do not return success. | [0013](adr/0013-delivery-phases.md) |
| 11 | The project license is Apache-2.0. Dependencies must be OSI-approved open source. Rust is preferred, and required for anything on the produce ack path. | `LICENSE` |

## Defaults

Metadata voters default to 3. WAL shard replication factor defaults to 3.
Both are config. A 5-voter metadata group is allowed and is not the default.
`durability.mode` defaults to `wal`.

## What Phase 0 will contain

Phase 0 is foundations only. No Produce or Fetch handler that accepts data.

- Cargo workspace and the crate boundaries in ADR 0001.
- `protocol` crate: frame codec, generated messages from the Kafka 4.4.0
  schemas, `ApiVersions` advertisement list (empty of data APIs, or limited
  to what the codec can round-trip in tests).
- `raft` crate: one openraft 0.9.25 group, in-process, with the segment-file
  log and a `redb` state machine from ADR 0011.
- `storage` crate: `object_store` against MinIO, then the same code path
  against R2. Put, ranged get, delete. Conditional put enabled (`etag`),
  which R2 and MinIO both support.
- `docker-compose.yml`: MinIO, three broker processes that boot and join a
  metadata group, Prometheus scrape endpoint that serves metrics.
- Clone `apache/kafka` at tag `4.4.0` into `vendor/kafka` (gitignored) for
  local reading. The pin itself lives in `docs/compatibility.md`.
- Unit tests: protocol round-trip, Raft election and a committed write,
  object put/get against MinIO.
- The storage-engine benchmark, with the numbers appended to ADR 0011. The
  engines under test are the segment file, `redb`, and `fjall`. Only the
  first two are linked into the broker.

Phase 0 will not implement Produce, Fetch, consumer groups, or the WAL
uploader. Those are Phases 1 and 2.

## What "done" means later

Each phase ends with a build that can be demoed and a green acceptance
suite. The suite grows: unit tests, cases ported from the Kafka tree, real
Java / librdkafka / franz-go clients against MinIO, fault injection, a small
linearizability checker for offsets, and produce latency benchmarks for
`wal` versus `r2`. The matrix in `docs/compatibility.md` is updated in the
same change as the API.

## Sources checked while writing this

- Kafka downloads and tag `4.4.0` (annotated tag, commit `1156b275…`), 2026-11-05.
- Cloudflare R2 limits (page updated 2026-10-30), consistency model, and pricing.
- `kafka-protocol` 0.18.0 crate docs (generated from Kafka 4.1.0 schemas).
- `openraft` 0.9.25 (stable) and 0.10.0-alpha.37 (not adopted).
- `object_store` S3 conditional put: `S3ConditionalPut::ETagMatch` documents
  R2 and MinIO as If-Match / If-None-Match stores.

Kafka source files named in the ADRs are paths in the 4.4.0 tree. Phase 0
confirms they still exist at the pin and updates the paths if they moved.
