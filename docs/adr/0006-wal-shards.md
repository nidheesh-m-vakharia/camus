# ADR 0006: WAL shards

Status: Accepted
Date: 2026-11-05

## Context

A single Raft group for all produce traffic would cap cluster throughput at
one consensus log. Kafka scales by partitioning. Camus needs the same shape
without running a Raft group per partition, which would be too many groups
to elect and snapshot.

The WAL is a buffer. Upload and truncation have to move as fast as produce,
or the disk limit becomes the steady state and every produce throttles.

## Decision

### Shards

Partitions are mapped onto WAL shards. Each shard is an openraft group with
its own log, membership, and leader. The mapping is `shard = partition_id
mod shard_count` within a topic, stored explicitly in metadata when the
partition is created so a later shard-count change does not silently move
data. Resharding is not part of Phases 0–2.

Default `shard_count` is 1 in dev. Production config sets it. A reasonable
starting point to document, not to hard-wire, is one shard per broker so
each broker leads something. Replication factor defaults to 3, and must be
odd. Placement spreads voters across `broker.rack` values when racks are
configured. With no racks, placement is a round-robin over node ids.

The partition leader is the shard leader. Leadership is not assigned
independently. When Raft elects a new shard leader, metadata is updated and
the old leader stops acknowledging. Clients in flight receive
`NOT_LEADER_OR_FOLLOWER` or `NOT_LEADER_FOR_PARTITION` and refresh metadata.
Both codes are retriable in Kafka clients. We use `NOT_LEADER_OR_FOLLOWER`
(6) for produce and fetch, matching current Kafka broker behavior for this
case.

### What goes in a log entry

One Raft entry holds one or more partition batches that the leader ordered:

```
entry := repeated {
  topic_id, partition, leader_epoch,
  base_offset,   // filled at commit; see below
  batch_bytes    // client bytes, with the rewrite in ADR 0009
}
```

The leader may pack several produce requests that arrived together into one
append, to amortize the quorum round trip. Order inside the entry is the
order offsets will be assigned. Packing does not reorder two batches for
the same partition relative to the order the leader received them.

Offsets are assigned when the entry commits, not when it is first appended.
The leader then fills `base_offset` in its applied state and in the bytes
it will upload. Followers assign the same offsets because they apply the
same committed sequence. A follower that has not committed the entry does
not serve those offsets.

This avoids advertising offsets for an entry that an election might still
truncate. Raft will not truncate a committed entry as long as a majority
survives (ADR 0008).

### Uploader

A background task on the shard leader reads the committed prefix that is
not yet sealed:

- Flush when the buffered committed bytes reach 8 MiB, or 250ms after the
  first unsealed byte, whichever comes first. Both values are config
  (`upload.flush.bytes`, `upload.flush.ms`).
- Write one R2 object covering many partitions (ADR 0007).
- Commit the extent list to the metadata group.
- Truncate the shard log up to the last index included in that metadata
  commit. Truncation before the metadata commit is a bug.

Followers may upload if they are caught up, but only the leader truncates,
and only one uploader wins the metadata append for a given offset range.
The metadata entry is conditional on the partition's current sealed end, so
two uploaders cannot publish overlapping extents. The loser deletes its
unreferenced object (delete is free on R2; ADR 0007) or leaves it for the
compactor's orphan sweep.

### Tail reads

Fetches with `fetch_offset >= sealed_end` are read from the shard state
machine on the leader (and from followers only if `replica_id` is a
follower fetch and they have the commit). Fetches below `sealed_end` go to
R2 via `storage`. A fetch that spans the seal point is stitched: R2 bytes,
then WAL bytes, in offset order.

### Restart

On process start the broker opens its local Raft stores, rejoins as voter
or learner, waits until it has applied the commit index it is told about,
and resumes upload from `sealed_end` stored in metadata. It does not
re-upload extents metadata already lists. Local WAL entries at or below
`sealed_end` are truncated even if the process died between the metadata
commit and the truncate.

### Backpressure

Covered in ADR 0005. The shard refuses to grow the unsealed prefix past the
disk limit by throttling produce. It keeps electing and replicating so a
stuck uploader does not look like a leadership failure.

## Consequences

- Phase 2 cannot start until metadata can store extents and shard
  membership. That storage shape is sketched in Phase 0's state machine as
  types, with no produce API.
- Packing produce batches into one Raft entry means one slow fsync covers
  many partitions. A single hot partition does not get its own quorum.
- Shard leader failure freezes produce on every partition in the shard
  until election finishes. Election timeouts should be short inside a
  region (proposed `election_timeout_min` 150ms, to be tuned with the
  latency benchmark, and recorded here when it changes).
- `r2` mode does not create these groups. The uploader still runs, fed by
  memory.

## Alternatives

- One Raft group per partition. Rejected for election and snapshot overhead
  at tens of thousands of partitions.
- A non-Raft primary-backup WAL. Rejected. The brief asks for openraft, and
  a hand-rolled replication protocol is a second consensus implementation.
- Uploading from every follower always. Rejected as the steady state. It
  multiplies Class A operations by the replication factor. Leader upload is
  enough when the leader is up; follower upload is the failover path only.
