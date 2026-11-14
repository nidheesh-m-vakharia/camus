# ADR 0020: One region per cluster, MirrorMaker 2 between regions

Status: Accepted
Date: 2026-11-14

## Context

Feedback asked whether Camus runs one cluster across regions
([issue 147](https://github.com/nidheesh-m-vakharia/camus/issues/147)). It
does not, and nothing wrote down why or what to do instead.

Every ack waits on a Raft majority. Metadata and each WAL shard send a
heartbeat every 200 ms and elect after 400–800 ms of silence
([0016](0016-quic-control-plane.md)). An ack in `wal` mode is a WAL
majority fsync; in `r2` mode it is an R2 put plus a metadata commit
([0005](0005-durability-and-acks.md)). Inside one region a round trip is
under 2 ms. Between US East and US West it is about 60–70 ms, and between
US East and Europe about 80–90 ms. With voters split across regions:

- Every produce pays at least one cross-region round trip, so `wal` acks
  go from a few milliseconds to the inter-region RTT plus fsync.
- A heartbeat that crosses an ocean uses a fifth of the election window
  before any queueing. A slow link starts elections, and each election
  pauses every partition on the shard.
- Losing the region that holds a majority stops every shard that has one
  there. With two regions, three voters cannot be spread so that either
  region can go. A third region keeps quorum, but puts every ack and
  heartbeat on a cross-region round trip.

[ADR 0018](0018-multi-region-leaderless.md) proposes a native copy of
sealed objects between regions. It is not built.

## Decision

A Camus cluster is one region: its brokers, its metadata group, its WAL
shards, and its bucket. Voters may sit in different zones of that region,
which keeps a zone loss inside the minority-loss case of
[0008](0008-failure-and-recovery.md).

Two regions are two clusters. MirrorMaker 2 from Apache Kafka 4.4.0 copies
between them, in dedicated mode, with Camus as the source, the target, or
both. The target sees `<source alias>.<topic>`, with its own offsets.
`MirrorCheckpointConnector` translates committed offsets of the listed
consumer groups, and with `sync.group.offsets.enabled=true` writes them to
the target, so a group that fails over resumes where it stopped.

The ecosystem suite tests three directions every night: Apache Kafka to
Camus, Camus to Camus, and Camus to Apache Kafka. Each checks that the
records arrive, that the group's committed offsets are translated onto the
target, and that the group, consuming from the target, reads only records
it had not read on the source.

## Consequences

- Copying is asynchronous. A record acked in the source region and not
  yet copied is lost to the target if the source region goes. The window
  is MirrorMaker's replication lag, not an ack barrier.
- Offset translation is at the granularity of MirrorMaker's offset syncs.
  With the default `offset.lag.max=100`, a failed-over group can re-read
  up to that many records per partition. Consumers must tolerate
  duplicates on failover, as they do with Kafka.
- Transactions are not atomic across the copy. MirrorMaker's source
  consumer reads `read_uncommitted` unless the operator sets
  `<source>.consumer.isolation.level = read_committed`; without it,
  aborted records reach the target as plain records. Camus as a source
  has no control batches, so the target has none from Camus either
  ([issue 140](https://github.com/nidheesh-m-vakharia/camus/issues/140)).
- Topic names differ between regions unless the operator configures
  `IdentityReplicationPolicy`. Clients that fail over must know the
  target topic name.
- Failing back is a second flow in the other direction, set up by the
  operator. Camus adds no automatic promotion.

## Alternatives

- One Raft group stretched across regions. Rejected for the latency and
  election reasons above. ADR 0018 rejects it for the same reasons.
- Raising the election timeout so a stretched cluster stays stable.
  Rejected. It slows every in-region failover to suit the rare
  cross-region one, and the ack still waits on the far region.
- A native object copy between regions. Proposed in ADR 0018, not built.
  MirrorMaker 2 works today with stock Kafka tooling.

## As built

`crates/broker/tests/ecosystem.rs` runs
`mirror_maker_copies_from_apache_kafka`,
`mirror_maker_copies_from_camus_to_camus`, and
`mirror_maker_copies_from_camus_to_apache_kafka` in the nightly
`ecosystem` workflow. `docs/compatibility.md` has the two-region setup.
