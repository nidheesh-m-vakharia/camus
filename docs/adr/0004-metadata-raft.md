# ADR 0004: Metadata Raft group

Status: Accepted
Date: 2026-11-05

## Context

Topics, partition leadership, the offset-to-object index, consumer-group
state, producer ids, and transactions must be linearizable with respect to
each other. Two brokers accepting `CreateTopics` for the same name, or a
commit of an object index that clients can read before it is durable,
would break Kafka semantics. The cluster also has to be rebuildable from
R2 when every local disk is gone, with a documented loss bound.

openraft 0.10 is alpha (newest checked: 0.10.0-alpha.37, 2026-10-10) and
includes `openraft-multi`. The stable line is 0.9.25 (2026-07-28),
bugfix-only. Multi-raft does not require the alpha helper: it requires
more than one `Raft` instance in the process.

## Decision

### Library

Pin **openraft 0.9.25** for every Raft group, metadata and WAL.

Run one `Raft` instance per group inside the broker process. Groups share
a TCP RPC server that demultiplexes on a group id. We write that router.
We do not take a dependency on `openraft-multi` or on 0.10 until 0.10 has
a stable release and a storage-format migration note. Revisiting this pin
is a new ADR.

`RaftLogStorage::append` must not return until the new entries are fsynced.
That requirement is part of the ack contract in ADR 0005. The storage
engine behind the trait is chosen in ADR 0011.

### Group topology

One metadata group per cluster.

- Default: 3 voters. Config may set 5. Even counts are rejected at startup.
- Every other broker is a learner (non-voter) of this group, so it can
  serve reads of metadata that Raft has committed, after applying them.
- Linearizable reads of metadata go through the leader (openraft
  read-index), or through a lease the leader still holds. A learner may
  answer `Metadata` requests with a slightly stale committed snapshot only
  for fields Kafka already treats as cacheable (topic ids, partition
  leaders). Group coordinator reads that must be linearizable are forwarded
  to the leader.
- Clients never talk Raft. They talk Kafka. A broker that is not the
  metadata leader forwards metadata mutations (`CreateTopics`,
  `OffsetCommit`, …) to the leader over the internal RPC and waits.

Membership changes use openraft's joint-consensus change. Adding a voter
is an operator action in Phase 0 (`camus admin`), not an automatic join.
Learners may join by config so a new broker can boot.

### State machine contents

Committed as Raft log entries, applied in order:

- Broker registration: node id, advertised host/port, rack (failure domain).
- Topic and partition records: name, topic id (UUID), partition count,
  replication assignment, configs.
- Partition leadership: which WAL shard owns the partition, and the leader
  node id. In `r2` mode the "leader" is the metadata leader, or a broker
  the metadata leader assigns so produce traffic spreads. Assignment is
  still metadata.
- Sealed log index: for each partition, a list of non-overlapping
  `(start_offset, end_offset, object_key, byte_start, byte_len)` extents.
- Log start offset and high watermark as published to clients. The high
  watermark of sealed data is the end of the last extent. In `wal` mode the
  client-visible high watermark is `max(sealed_end, wal_commit_end)` and is
  served by the shard leader for the tail.
- Consumer group metadata and committed offsets (Phase 3).
- Producer id, epoch, and last sequence per partition (Phase 4).
- Transaction state (Phase 5).

Entries are idempotent. Re-applying a snapshot plus the following log must
produce the same machine. Keys are the topic id and partition, never the
topic name alone, after creation. Renames are not a Kafka feature.

### Snapshots to R2

The leader writes a metadata snapshot to R2 on a timer and a size threshold,
default **30 seconds or 10,000 applied entries**, whichever comes first.

Object key: `meta/snap/{epoch}-{last_log_index}.snap`, written once.
A pointer object `meta/snap/CURRENT` is updated at most **once per second**
so it stays under R2's per-key write limit (ADR 0007). If a second snapshot
completes inside that second, `CURRENT` moves on the next tick. Readers
that cannot trust `CURRENT` list `meta/snap/` (R2 list is strongly
consistent) and take the highest index.

The snapshot is a serialization of the state machine plus the Raft
membership. It is not a copy of the Raft log. Local Raft logs still hold
the tail after the snapshot index so voters can catch up without R2.

The loss bound when **every** metadata disk is destroyed is in ADR 0008.
The short version: log bytes already sealed to R2 are recoverable from
object keys; metadata mutations newer than the last snapshot are not,
unless they were derived from those keys.

### As built: group requests (Phase 3)

A broker that is not the metadata leader answers group requests with
`NOT_COORDINATOR` instead of forwarding them. `FindCoordinator` names the
leader, and every Kafka client already rediscovers on that error, so a
forward would add a hop for no client-visible gain. Group records and
offsets are `SaveGroup`, `CommitOffsets`, and `DeleteGroup` commands;
`CommitOffsets` checks the stored generation at apply time.

## Consequences

- Phase 0 stands up this group and nothing else that looks like Kafka data.
- 0.9 will not receive new Raft features upstream. We accept that in
  exchange for a stable storage format while the WAL design is still moving.
- A custom RPC router is code we must test: a message for group A must not
  be applied to group B. The test is in Phase 0 even though the second
  group arrives in Phase 2. Phase 0 uses two throwaway groups in-process.
- Forwarding mutations adds a hop when the client contacted a learner.
  `Metadata` responses list the leader so clients prefer it, the same way
  they prefer a Kafka partition leader.

## Alternatives

- openraft 0.10 alpha now, for `openraft-multi`. Rejected for the metadata
  log's durability. The connection-sharing helper is not worth an alpha
  storage format. The router is small.
- A single-node metadata file with R2 as the only replication. Rejected.
  Two brokers would split-brain `CreateTopics` and offset commits.
- etcd or another external store. Rejected by the brief.
