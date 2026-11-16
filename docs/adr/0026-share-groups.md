# ADR 0026: Share groups (KIP-932), keeping state in the WAL shard

Status: Proposed
Date: 2026-11-16

## Context

Issue #149. In feedback, people compared Camus with job queues such as
BullMQ and Restate. A queue needs three things a Kafka consumer group does
not give:

- per-record acknowledgement;
- redelivery of a record whose consumer died;
- more consumers than partitions.

KIP-932 share groups add all three to Kafka. Several members fetch from the
same partition, and each record is locked to one member until it is
acknowledged, released, rejected, or its lock times out.

In Kafka 4.4.0 these APIs are stable. Version 0 was early access in 4.0
and was removed in 4.1. The vendored message schemas name:

| API | Key | Versions |
| --- | --- | --- |
| ShareGroupHeartbeat | 76 | 1 |
| ShareGroupDescribe | 77 | 1 |
| ShareFetch | 78 | 1–2 |
| ShareAcknowledge | 79 | 1–2 |
| DescribeShareGroupOffsets | 90 | 0–1 |
| AlterShareGroupOffsets | 91 | 0 |
| DeleteShareGroupOffsets | 92 | 0 |
| Initialize/Read/Write/DeleteShareGroupState, ReadShareGroupStateSummary | 83–87 | inter-broker only |

The Java client has `KafkaShareConsumer`, and `kafka-share-groups.sh`
manages the groups. Support in librdkafka, franz-go, and KafkaJS was not
checked for this record. Step 0 below checks it before any code is
written.

Today `FindCoordinator` refuses key type 2 (`SHARE`, which Kafka brokers
use to find a share coordinator) with `INVALID_REQUEST` (42), and keys
76–92 are absent from `ApiVersions`. A share consumer
therefore fails at startup with `UnsupportedVersionException`.

## Decision

Implement share groups, after the broker benchmarks (#136, #141), in the
four steps below. Until then, the current answers stay as they are.

### Where the state lives

A share-partition is one share group reading one partition. Its state is:
- the share-partition start offset (SPSO);
- the in-flight records past the SPSO, each with its acquisition state
  (`AVAILABLE`, `ACQUIRED`, `ACKNOWLEDGED`, `ARCHIVED`), its delivery
  count, and, while acquired, its owning member and lock deadline.

Kafka keeps this state in the partition leader's memory and persists it
through a share coordinator into the internal `__share_group_state`
topic.

In Camus, the partition leader is the WAL shard leader. The state goes in
that shard's Raft log and state machine, the same way transaction spans do
(ADR 0019):

- An acknowledgement is one shard log entry. It is acknowledged to the
  client after the same WAL majority fsync that a produce waits for, so an
  acknowledged record is never redelivered after a failover.
- Acquisitions and lock expiries are leader-only and are not logged. A new
  leader treats every non-acknowledged record past the SPSO as
  `AVAILABLE`, which redelivers it. That matches KIP-932, where a failover
  can redeliver acquired records and increments their delivery count.
- When the SPSO moves past a sealed boundary, the shard leader commits it
  to metadata (`share/<group>/<topic-id>/<partition>`), in the same place
  where `sealed_end` is committed. A shard rebuilt with
  `truncate-to-sealed` resumes from that row and redelivers what came
  after it.

There is no share coordinator and no `__share_group_state` topic. Keys
83–87 stay unimplemented, because Kafka uses them only between brokers.

### Memory

Every in-flight window is bounded by `group.share.partition.max.record.locks`
(Kafka's default, 2,000 records). Each record costs about 24 bytes, so one
share-partition holds at most about 48 KiB. A shard holds at most
`SHARE_PARTITIONS` (4,096) active share-partitions, about 192 MiB in the
worst case. Past that cap, a new share-partition acquires no records: it
gets an empty `ShareFetch` partition, the same as an empty partition, until
one frees, and `camus_share_partitions_refused_total` counts it. A
share-partition that no member fetches for one lock duration frees its
window. It keeps only its SPSO and acknowledged ranges, as a compact run
list in the shard state machine.

### Membership

`ShareGroupHeartbeat` reuses the KIP-848 member and epoch machinery in
`camus-coordinator`, on the metadata leader. Kafka's share assignor may give
one partition to several members. Camus uses Kafka's `SimpleAssignor`
rule, which spreads partitions over members and shares one partition when
there are more members than partitions, so the same members see the same
assignment on either broker.

### Configs

These are supported with Kafka's names and defaults:
- the group configs `share.auto.offset.reset`,
  `share.record.lock.duration.ms` (30 s), `share.session.timeout.ms`,
  `share.heartbeat.interval.ms`, and `share.isolation.level`;
- the broker setting `group.share.delivery.count.limit` (5). A record that
  reaches it is `ARCHIVED`.

Step 0 checks the names against Kafka 4.4.0 before they are implemented.
`share.isolation.level=read_committed` uses the same last stable offset and
aborted spans as a consumer at `read_committed` (ADR 0021).

### Steps

0. **Client check.** Run `KafkaShareConsumer` and `kafka-share-groups.sh`
   against Kafka 4.4.0 in the ecosystem harness, and record which other
   clients support share groups. This sets the interop matrix.
1. **Membership and admin.** `ShareGroupHeartbeat`, `ShareGroupDescribe`,
   and share groups in `ListGroups` and `DeleteGroups`. Share consumers find
   the group coordinator with key type 0, which already works. Key type 2
   stays refused, because there is no share coordinator. Acceptance: members join, get assignments, and leave, as
   shown by `kafka-share-groups.sh --describe`.
2. **Fetch and acknowledge.** The share-partition state machine in the WAL
   shard, and `ShareFetch` and `ShareAcknowledge` v1–v2. Version 2 adds the
   acquire mode and renew acknowledgements (KIP-1206, KIP-1222). Acceptance: three members on
   one partition each receive a disjoint set of records. A killed member's
   records come back after the lock timeout. A record that is rejected or
   released, or whose lock expires, `share.delivery.count.limit` times is
   archived. A shard leader failover redelivers nothing that was
   acknowledged. The #151 checker covers every case.
3. **Offsets admin.** `DescribeShareGroupOffsets`,
   `AlterShareGroupOffsets`, `DeleteShareGroupOffsets`.

Estimate: one to two weeks for step 1, which builds on the KIP-848
coordinator; three weeks for step 2; one week for step 3.

## Consequences

- Camus serves queue workloads through ordinary Kafka clients. It gives
  per-record acknowledgement, redelivery, a delivery limit, and as many
  consumers as needed on one partition.
- Each acknowledgement costs one WAL majority fsync, batched with produce
  in the shard's group commit. That fsync already sets the latency of a
  Camus produce.
- What share groups still do not give, compared with a job queue:
  delayed jobs, priorities, and a dead-letter topic. KIP-932 archives a
  record that reaches the delivery limit and does not move it anywhere.
  These are recorded as differences, not planned.
- A failover redelivers acquired records, as on Kafka. Consumers must be
  idempotent, as they must for any at-least-once queue.

## Alternatives

- **Do not implement; point queue users at consumer groups.** Rejected.
  The feedback was specifically about having more consumers than
  partitions, and per-record retries.
- **Keep the state in metadata, like committed offsets.** Each
  acknowledgement would be a second Raft write through the metadata
  leader, the same cost ADR 0019 removed for transactions. Rejected.
- **A share coordinator and `__share_group_state` topic, as in Kafka.**
  This adds an internal topic and a coordinator role, only to persist what
  the WAL shard already replicates. Rejected.
