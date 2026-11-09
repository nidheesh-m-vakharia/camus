# Camus plan

Status: accepted. Date: 2026-11-05.

Phases 0, 1, and 2 are done; ADR 0013 lists the test for each Phase 2
acceptance item. Topics default to `wal`, which acks after a majority of the
WAL shard's voters has the batch on disk. SASL_SSL is covered by a Rust test
and by Java, librdkafka, and franz-go, which now run on `wal` topics. After a
majority loss, `camus admin truncate-to-sealed` resumes a shard at its
sealed end. Phase 3 classic consumer groups work with Java, librdkafka,
and franz-go and survive a coordinator failover. The classic coordinator
cases and KIP-848 `ConsumerGroupHeartbeat` are in (ADR 0013). Phase 4
idempotent produce and its admin APIs are in. Phase 5 transactions,
`read_committed`, log compaction, and ACLs are in.
`kafka-clients` 4.4.0, librdkafka, and franz-go pass the transaction,
compaction, and ACL checks, and exercise the implemented admin APIs
their libraries expose. The ADRs under
`docs/adr/` are accepted.
This file is the map of those decisions and of what Phase 0 is allowed to touch.

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

## What Phase 0 contained

This section is the original Phase 0 scope, kept as a record. Phase 0 is done.

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

## Write path

Added 2026-11-05, after Phase 2. Kafka's write path is one sequential
append of the client's bytes, with batching and zero-copy reads. Camus's
`wal` path still has stages that cost more than the quorum fsync it is
built around. This track removes them before Phase 3 adds more load. Each
step lands with `broker/tests/throughput.rs` numbers before and after.

The fsync before each ack is not on the list. ADR 0005 chose it; it is the
latency floor, not overhead.

Baseline, three brokers on one laptop SSD, 16 × 1 KiB records per batch:

| Producers | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes / payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.1 | 12.9 | 32.8 | 1.04 | 4.20 |
| 8 | 2.4 | 48.0 | 108.2 | 1.02 | 4.20 |
| 32 | failed: shard leadership flapped under load | | | | |

Steps, in order:

1. **Throughput benchmark.** Done. `broker/tests/throughput.rs`, with fsync
   and byte counters on the shard log (`wal_log_*` in `/admin/metrics`).
2. **Group commit.** Done. A committer task packs batches that arrive
   while an entry commits into the next entry (up to 1 MiB). One entry in
   flight: more split groups and cost throughput (measured, see
   `IN_FLIGHT` in `crates/wal`). The 32-producer failure was not load: the
   follower's HTTP route refused bodies over axum's 2 MB default, which one
   1 MiB client batch already exceeded once JSON-encoded. The Raft routes
   now take 64 MB and an AppendEntries carries at most 8 entries.
3. **Binary entries.** Done. Log entries and both groups' Raft RPC bodies
   are postcard (`camus_raft::codec`); batch bytes are `serde_bytes`.

After steps 1–3, same machine and benchmark:

| Producers | MiB/s | p50 ms | p99 ms | fsyncs/ack | log bytes / payload byte |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.2 | 11.9 | 37.0 | 1.04 | 1.07 |
| 8 | 4.6 | 24.3 | 62.1 | 0.26 | 1.07 |
| 32 | 16.5 | 27.7 | 61.9 | 0.06 | 1.07 |

4. **Store each batch once.** Done. The state machine keeps a header per
   unsealed batch (offsets, log index and position, length, epoch); fetch
   and the uploader read the bytes from the log entry and re-seal them.
   Before openraft builds a snapshot, which lets it purge the log, every
   unsealed batch is copied to a `detached` table and into the snapshot,
   so a purge or a follower install keeps them. Memory now holds headers,
   not up to `wal.disk.limit` of bytes. Throughput did not change: the
   second write was not fsynced, so it was not the bottleneck.
5. **Fewer copies.** Done for the produce path: `Append.raw` is a `Bytes`
   view of the request buffer.

After steps 1–5:

| Brokers | Producers | MiB/s | p50 ms | p99 ms | fsyncs/ack |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 1 | 1.2 | 12.0 | 33.2 | 1.04 |
| 3 | 8 | 4.4 | 26.6 | 57.5 | 0.26 |
| 3 | 32 | 15.9 | 29.5 | 57.6 | 0.06 |
| 1 | 1 | 2.9 | 5.0 | 11.5 | 1.02 |
| 1 | 8 | 12.5 | 9.4 | 19.9 | 0.25 |
| 1 | 32 | 41.4 | 11.1 | 25.2 | 0.06 |

Tried and reverted: syncing the log on a background thread so `append`
returns before the `fsync`. openraft 0.9 does not submit the next append
until the previous one reports flushed, so nothing overlapped, and the
handoff cost a single producer 2ms. With that, more than one packed
entry in flight only made groups smaller (0.67 fsyncs per ack at 8
producers instead of 0.26).

What is left between one broker and three is serial: leader `fsync`, then
the follower's `fsync` behind an HTTP round trip, and on this laptop all
three brokers share one SSD. Next, in order of expected gain:

6. **Overlap the leader's and follower's `fsync`.** Needs openraft to send
   entries before the local flush finishes, which 0.9 does not. openraft
   0.10 is still `0.10.0-alpha.37` (2026-10-10), so this waits for a 0.10
   release rather than putting an alpha under both Raft groups.
7. **A persistent binary stream** for shard Raft. Measured first and set
   aside: the leader sees an AppendEntries take 6.42 ms (1 producer) and
   8.97 ms (32), of which the follower spends 6.06 and 8.51 ms in its own
   `append_entries`, which is its `fsync`. HTTP adds under 0.5 ms. The
   counters stay (`wal_rpc_*` in `/admin/metrics`, printed by
   `throughput.rs`); revisit if that difference grows.
8. **Tail fetches from the log file** instead of decoding entries. Not
   measured yet; needs a consumer side to `throughput.rs`.
9. **Measure on separate disks.** One broker per machine in one region, to
   see what the laptop's shared SSD hides. This is now the measurement
   that decides what to do next for produce.

Steps 3 and 4 changed the WAL's on-disk format. Before launch that means a
fresh data directory, not a migration.

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
