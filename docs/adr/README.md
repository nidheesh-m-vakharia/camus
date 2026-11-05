# Architecture decision records

Status of every record is **Proposed** as of 2026-11-05. None of this is
implemented.

| ADR | Title |
| --- | --- |
| [0001](0001-system-architecture.md) | System architecture and crate boundaries |
| [0002](0002-kafka-compatibility-pin.md) | Pin Apache Kafka 4.4.0 as the behavior reference |
| [0003](0003-wire-protocol.md) | Generated wire protocol, ApiVersions, errors |
| [0004](0004-metadata-raft.md) | Metadata Raft group and snapshots |
| [0005](0005-durability-and-acks.md) | `wal` and `r2` ack barriers |
| [0006](0006-wal-shards.md) | Multi-raft WAL shards and the uploader |
| [0007](0007-r2-layout-and-cost.md) | R2 keys, consistency, cost model |
| [0008](0008-failure-and-recovery.md) | Minority loss, quorum loss, restore from R2 |
| [0009](0009-offsets-and-record-batches.md) | Offsets and which batch bytes may change |
| [0010](0010-reads-retention-compaction.md) | Fetch, cache, retention, object merge |
| [0011](0011-local-log-storage.md) | Local engine benchmark; choice deferred |
| [0012](0012-auth.md) | SASL/PLAIN + TLS, and opt-in dev PLAINTEXT |
| [0013](0013-delivery-phases.md) | Phases 0–5 and their acceptance tests |

Read [../PLAN.md](../PLAN.md) for the review summary.
