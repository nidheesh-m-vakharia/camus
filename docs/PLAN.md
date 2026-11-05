# Camus plan

Status: waiting for review. Date: 2026-11-05.

Phase 0 implementation has not started. This document is the review packet.
The ADRs under `docs/adr/` are the decisions. This file says what they add up
to, which choices are still open, and what the first implementation phase is
allowed to touch.

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

## Decisions proposed for acceptance

| # | Decision | ADR |
| --- | --- | --- |
| 1 | Behavior reference is Apache Kafka **4.4.0**, commit `1156b2752aaa28eafc0b4c4d387e63356793268d`. Where a KIP and that tree disagree, the tree wins. | [0002](adr/0002-kafka-compatibility-pin.md) |
| 2 | Protocol types are generated from that tree's message JSON. `kafka-protocol` 0.18.0 (schemas from Kafka 4.1.0) is the starting generator, then regenerated. Field layouts are not hand-written. | [0003](adr/0003-wire-protocol.md) |
| 3 | Consensus library is **openraft 0.9.25** (stable). Several Raft instances per process, with a router we write. The 0.10 alpha and `openraft-multi` stay out until 0.10 is stable. | [0004](adr/0004-metadata-raft.md) |
| 4 | `acks=1` and `acks=all` both wait for the configured durability barrier. `acks=0` does not. Leader-only acks are not offered, because they can lose data when a minority of replicas fail. | [0005](adr/0005-durability-and-acks.md) |
| 5 | Losing a majority of a WAL shard before upload takes the affected partitions offline. Camus does not silently truncate acked offsets. An operator can later truncate to the R2 prefix explicitly. | [0008](adr/0008-failure-and-recovery.md) |
| 6 | Data objects are immutable and uniquely keyed. R2 allows one concurrent write per key per second and returns 429 above that. Recovery can rebuild the log index from object keys if the metadata tail is lost. | [0007](adr/0007-r2-layout-and-cost.md) |
| 7 | The broker writes the assigned base offset and leader epoch into each batch. The CRC-covered bytes, including compression, are copied through. | [0009](adr/0009-offsets-and-record-batches.md) |
| 8 | The on-disk Raft engine is **not chosen yet**. Phase 0 runs the benchmark in [ADR 0011](adr/0011-local-log-storage.md) and stops for a one-line follow-up before any log format is committed. | [0011](adr/0011-local-log-storage.md) |
| 9 | Dev listeners are PLAINTEXT and must be opted into. The durable target for Phase 1 is SASL/PLAIN over TLS. | [0012](adr/0012-auth.md) |
| 10 | Work lands in phases 0–5. A phase is done only when its acceptance tests pass. Unimplemented APIs return a Kafka error and a row in `compatibility.md`. They do not return success. | [0013](adr/0013-delivery-phases.md) |

## Still open, on purpose

**Local storage engine.** `redb`, `fjall`, and a custom append-only segment
file are the candidates. The hypothesis, not a decision, is segment files for
the Raft log and `redb` for state machines. Phase 0 measures fsync p99 before
either format is locked. See ADR 0011.

**Project license.** `NOTICE` attributes Apache Kafka. The license for
Camus's own code is unset.

**Cluster size defaults.** Metadata voters default to 3. WAL shard replication
factor defaults to 3. Both are config. A 5-voter metadata group is allowed
and not the default.

## What Phase 0 will contain, after this review

Phase 0 is foundations only. No Produce or Fetch handler that accepts data.

- Cargo workspace and the crate boundaries in ADR 0001.
- `protocol` crate: frame codec, generated messages from the Kafka 4.4.0
  schemas, `ApiVersions` advertisement list (empty of data APIs, or limited
  to what the codec can round-trip in tests).
- `raft` crate: one openraft 0.9.25 group, in-process, with the storage
  engine selected by the benchmark.
- `storage` crate: `object_store` against MinIO, then the same code path
  against R2. Put, ranged get, delete. Conditional put enabled (`etag`),
  which R2 and MinIO both support.
- `docker-compose.yml`: MinIO, three broker processes that boot and join a
  metadata group, Prometheus scrape endpoint that serves metrics.
- Clone `apache/kafka` at tag `4.4.0` into `vendor/kafka` (gitignored) for
  local reading. The pin itself lives in `docs/compatibility.md`.
- Unit tests: protocol round-trip, Raft election and a committed write,
  object put/get against MinIO.
- The storage-engine benchmark and a short result note appended to ADR 0011.

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
