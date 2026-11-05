# ADR 0013: Delivery phases

Status: Proposed
Date: 2026-11-05

## Context

The full broker is a multi-month surface. The brief splits it into phases
and says not to start the next one until the current phase's acceptance
tests pass. It also says to stop for review before Phase 0 code.

## Decision

Phases below are the contract. A phase may add tests and error returns for
later APIs. It may not add a success path for a later phase's API.

### Phase 0 — Foundations

Acceptance:

- Workspace builds on stable Rust with `cargo test`.
- Protocol frames round-trip, generated from Kafka 4.4.0 schemas.
- Three processes form a metadata Raft group and survive killing one voter.
- Put, ranged get, and delete work against MinIO through `object_store`.
- Prometheus endpoint answers a scrape.
- ADR 0011 benchmark has numbers appended, and the log format is still not
  locked unless the review accepts the result in the same breath.
- `docs/compatibility.md` exists and lists later APIs as unimplemented.

No produce, no fetch, no client compatibility suite yet.

### Phase 1 — Produce and consume, `r2` mode

APIs that return success: `ApiVersions`, `Metadata`, `Produce`, `Fetch`,
`ListOffsets`, `CreateTopics`, `DeleteTopics`, `DescribeConfigs`, and
`SaslHandshake` / `SaslAuthenticate` for PLAIN.

Acceptance:

- Java client, librdkafka via confluent-kafka-python, and franz-go each
  create a topic, produce, and consume the same bytes, including gzip,
  snappy, lz4, and zstd.
- `acks=0` returns without waiting. `acks=1` and `acks=all` survive a
  broker kill with no acknowledged batch missing (the `r2` rule).
- Unknown APIs and unimplemented versions return the codes in ADR 0003.
- Golden bytes from a 4.4.0 broker for the implemented requests.
- Dev PLAINTEXT and SASL_SSL both covered.

### Phase 2 — WAL

Acceptance:

- `wal` p50/p99 produce latency reported next to `r2` on the same hardware.
- Kill leader, kill one follower: no acknowledged offset missing, clients
  recover after metadata refresh.
- Kill a majority: partitions stay offline; `truncate-to-sealed` is required
  to resume, and the test shows the unsealed suffix is gone only after that
  command.
- Inject R2 delay, 5xx, and a full outage: produce throttles with
  `throttle_time_ms`, does not drop acks, and catches up when R2 returns.
- Disk limit throttles. ENOSPC returns `KAFKA_STORAGE_ERROR`.
- Restart resumes upload without duplicating extents.

### Phase 3 — Consumer groups

Classic protocol first: `FindCoordinator`, `JoinGroup`, `SyncGroup`,
`Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`, `ListGroups`,
`DescribeGroups`, `DeleteGroups`. Then KIP-848 `ConsumerGroupHeartbeat`.

Acceptance: the three clients rebalance, commit, restart, and do not lose
committed offsets. A coordinator leader failover does not split-brain the
group. Cases ported from the 4.4.0 group coordinator tests that apply to
the classic protocol.

### Phase 4 — Idempotence and admin

`InitProducerId`, `CreatePartitions`, `AlterConfigs`,
`IncrementalAlterConfigs`, `DeleteRecords`, `OffsetDelete`,
`DescribeCluster`.

Acceptance: a producer with `enable.idempotence=true` retries without
duplicates across a leader change. `DeleteRecords` honors the whole-object
rule in ADR 0010 and documents it.

### Phase 5 — Transactions, log compaction, ACLs

`AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`, `TxnOffsetCommit`,
fetch isolation `read_committed`, `cleanup.policy=compact`, ACL APIs.

Acceptance: a transactional produce plus offset commit is atomic with
respect to `read_committed` consumers. Compaction preserves the latest key
per the Kafka log rules ported from `UnifiedLog` at the pin. ACL denial
returns the authorization errors, not a timeout.

### Cross-phase rules

- No `todo!()` or `unimplemented!()` on a request path. The dispatcher
  returns a Kafka error.
- No silent success stubs.
- Small commits, tests green at each commit.
- PRs name the Kafka files they were checked against and any deliberate
  difference.
- A Jepsen-style checker for offset linearizability starts simple in Phase 1
  (a single partition, one producer, crash the leader process) and grows.
  It is not postponed to Phase 5.
- Benchmarks from Phase 2 are kept runnable.

## Consequences

- The first code review after this packet is Phase 0 only.
- Slip in an early phase blocks later ones. That is the point.
- KIP-848 can slip to the end of Phase 3 without blocking classic clients.
  It cannot slip past Phase 3's definition of done unless this ADR is
  updated.

## Alternatives

- Implementing `wal` before `r2`. Rejected. `r2` has fewer moving parts and
  still exercises protocol, metadata, and R2. The brief says `r2` first.
- One phase for "all of produce plus groups." Rejected. Too large to review
  or to bisect when a client fails.
