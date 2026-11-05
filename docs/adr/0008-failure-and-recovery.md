# ADR 0008: Failure and recovery

Status: Accepted
Date: 2026-11-05

## Context

The brief asks for an explicit statement of what is lost when a WAL quorum
dies before upload, and for `r2` mode to lose nothing that was acknowledged
when a broker is killed. It also asks for the bound on rebuilding a cluster
from R2 alone after every local disk is gone.

Raft's intersection property: if any majority of voters is still alive, every
committed entry is on at least one of them. The converse matters. After a
majority is gone, a committed entry may be on none of the survivors.

## Decision

### `r2` mode

Brokers keep the not-yet-acked buffer in memory only. Killing any broker,
or all of them, loses that buffer and nothing that was acknowledged.

Acknowledged means the put succeeded and the metadata commit that publishes
those offsets is applied on a metadata quorum. A client that received the
produce response can fetch the batches from any broker that can read
metadata and R2.

There is no WAL replay. A restart is an empty process plus a metadata
catch-up.

`acks=0` is the exception in both modes. Those batches may disappear
without an error. Callers chose that.

### `wal` mode, minority loss

Losing fewer than a majority of a shard's voters loses no acknowledged
data. A remaining majority elects a leader. That leader's log contains
every committed entry. Produce returns `NOT_LEADER_OR_FOLLOWER` during the
election, then succeeds. Upload resumes from metadata's `sealed_end`.

Killing the leader is the common case of minority loss when RF is 3. The
same guarantee applies. Tests in Phase 2 kill the leader and a follower
mid-produce and assert every acked offset is still fetchable.

### `wal` mode, majority loss before upload

If a majority of a shard's voters are gone, and some acknowledged entries
existed only on those voters and were not yet published in metadata:

- Survivors must not elect a leader and must not serve a new end offset.
- Affected partitions return `NOT_LEADER_OR_FOLLOWER` (or
  `LEADER_NOT_AVAILABLE`, 5, if we have no leader to name) until the shard
  can form a quorum again.
- Camus does not copy a survivor's suffix and call it the log. That would
  discard acknowledged offsets silently. This matches Kafka with
  `unclean.leader.election.enable=false`, which is Kafka's default.
- Restoring one of the lost voters with its disk intact restores the
  quorum and the data. This is the preferred recovery.
- If those disks are destroyed, an operator may run
  `camus admin truncate-to-sealed --partition …`. That command forces a new
  membership from the survivors and sets the client-visible end to
  metadata's sealed offset. Offsets that were acknowledged and not sealed
  are then gone. The command refuses to run unless the operator passes
  `--i-accept-data-loss`. It is not automatic.

What the client observes after that explicit truncate: the next produce
appends at `sealed_end`, and fetch past `sealed_end` behaves as an offset
out of range (`OFFSET_OUT_OF_RANGE`, 1). A producer that already has a
response saying the batch landed at a higher offset will not find it.
Idempotent producers (Phase 4) cannot reconstruct batches the cluster no
longer has. This is data loss, and the operator command is the only way to
enter it.

Until Phase 2, `wal` mode does not exist, so this command does not exist.

### Whole cluster disk loss

Assume every broker disk is wiped and R2 still has its objects. Recovery
starts a new metadata group and loads the newest snapshot under
`meta/snap/`.

Bound, with the default snapshot interval of 30 seconds or 10,000 entries:

- **Sealed log bytes** are recoverable even when they are newer than the
  snapshot. The recovery tool lists `bundle/` and `seg/` (R2 list is
  strongly consistent), reads each object's footer, and rebuilds extents
  whose offset ranges form a gap-free prefix from the snapshot's sealed end
  or from offset 0. Overlapping uploads of the same range: keep the object
  named by the snapshot if there is one, otherwise the lexicographically
  smallest ULID, and require the batch bytes to match. If they do not, that
  partition stays offline until an operator picks an object.
- **Unsealed WAL entries** were never in R2. They are gone. In `wal` mode
  that includes acknowledged data that had not been uploaded. The bound is
  the upload window, default 250ms of batches, or more if R2 was failing
  and the disk limit had not yet stopped producers. Worst case before
  throttle is `wal.disk.limit` of acknowledged-but-unsealed data, and only
  when the entire shard majority's disks are in the lost set. State this
  plainly to operators: a total loss of a shard's disks loses up to the
  unsealed WAL, which is meant to be seconds and can be as large as the
  configured disk limit if R2 has been down.
- **Metadata mutations newer than the snapshot** are not in R2. That
  includes topic creates, config changes, group offsets, producer epochs,
  and transaction state applied after the last snapshot. The bound is the
  snapshot interval: default 30 seconds, or fewer entries when the 10,000
  entry threshold fires first. Group offsets can move backward by that
  much on this kind of restore. Log data does not move backward except for
  the unsealed WAL case above, because extents are rebuilt from object keys
  rather than trusted from the stale snapshot alone.
- Objects that never made it into a gap-free prefix (holes, partial
  uploads) stay orphans and are not served.

A metadata majority that is still alive does not use this path. It still
has the Raft tail, which is newer than the snapshot. The bound above is
only for "all local disks gone."

### Network partition

A broker that cannot reach a majority of its group does not acknowledge
`wal` produces and does not commit metadata. It may still serve sealed
fetches from R2 if it can reach R2 and its local metadata is only used as
a hint; if it cannot confirm leadership or a read-index, it returns
`NOT_LEADER_OR_FOLLOWER` rather than a stale tail. Sealed fetches are
answered from the last applied metadata plus R2. Serving sealed data while
partitioned from the metadata leader is allowed because those objects are
immutable. Serving the tail is not.

### Disk full

Raft appends that fail on fsync return an error to the produce handler.
The client sees `KAFKA_STORAGE_ERROR` (56), which Kafka clients treat as
retriable. The broker does not acknowledge. Throttle behavior for the soft
limit is ADR 0005; a hard ENOSPC is this error.

### R2 faults

- Slow R2: WAL grows, then produce throttles via `throttle_time_ms`.
  Acknowledged `wal` data stays on the quorum.
- HTTP 5xx and timeouts: uploader retries. Same backpressure. No ack in
  `r2` mode until a put succeeds.
- Prolonged outage: `wal` clusters continue until the disk limit, then
  throttle. `r2` clusters throttle as soon as the memory buffer is full.
  Already acknowledged data remains readable if it was sealed (`wal` tail
  is readable from the WAL even during an R2 outage).
- R2 total loss is out of scope. It is a loss of the source of truth.
  Camus has no second object store.

## Consequences

- Fault tests in Phase 2 have a crisp oracle: minority failures, no acked
  offset missing; majority failures, partitions offline unless the operator
  truncates.
- The admin truncate command is deliberately awkward to type.
- Recovery from R2 can resurrect a log prefix without the latest consumer
  offsets. Applications that cannot tolerate a group rewind of one snapshot
  interval after a total disk loss need an external checkpoint. That is a
  property of the snapshot interval, which operators can shorten at the
  cost of more Class A puts.

## Alternatives

- Automatic unclean election to keep the partition online. Rejected as the
  default. It hides loss. The operator command is the explicit form.
- Streaming the Raft log itself to R2 before ack. That is `r2` mode with
  extra steps. It is already available by setting `durability.mode=r2`.
