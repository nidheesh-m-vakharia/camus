# Architecture decision records

Records 0001–0013 were accepted on 2026-11-05 and are implemented, with the
amendments their files note. 0014 is proposed.

| ADR | Title |
| --- | --- |
| [0001](0001-system-architecture.md) | System architecture and crate boundaries |
| [0002](0002-kafka-compatibility-pin.md) | Pin Apache Kafka 4.4.0 as the behavior reference |
| [0003](0003-wire-protocol.md) | Generated wire protocol, ApiVersions, errors |
| [0004](0004-metadata-raft.md) | Metadata Raft group and snapshots |
| [0005](0005-durability-and-acks.md) | `wal` and `r2` ack barriers |
| [0006](0006-wal-shards.md) | Multi-raft WAL shards and the uploader |
| [0007](0007-r2-layout-and-cost.md) | Bundled R2 objects, consistency, cost model |
| [0008](0008-failure-and-recovery.md) | Minority loss, quorum loss, restore from R2 |
| [0009](0009-offsets-and-record-batches.md) | Offsets and which batch bytes may change |
| [0010](0010-reads-retention-compaction.md) | Fetch, cache, retention, object merge |
| [0011](0011-local-log-storage.md) | Segment-file Raft log, `redb` state machine |
| [0012](0012-auth.md) | SASL/PLAIN + TLS, and opt-in dev PLAINTEXT |
| [0013](0013-delivery-phases.md) | Phases 0–5 and their acceptance tests |
| [0014](0014-multiple-wal-shards.md) | Multiple WAL shards and leadership balance (proposed) |
| [0015](0015-transaction-spans.md) | Transaction spans without a metadata write per produce (proposed) |
| [0016](0016-quic-control-plane.md) | QUIC for Raft control traffic (proposed) |
| [0017](0017-ignored-topic-configs.md) | Accepted topic configs that do not change behavior |

Read [../PLAN.md](../PLAN.md) for the summary.
