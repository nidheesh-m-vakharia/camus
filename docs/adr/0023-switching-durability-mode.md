# ADR 0023: Switching `durability.mode` on a topic that has data

Status: Proposed
Date: 2026-11-16

## Context

Issue #150. ADR 0005 refuses changing `durability.mode` on a topic that has
data "until an ADR describes the migration". Today the config is
create-only: `IncrementalAlterConfigs` rejects it on any topic. Teams want
to move a topic to `r2` once it becomes an audit trail, or to `wal` for
lower latency.

The two modes give out offsets at different moments:

- **`wal`.** The shard leader assigns offsets when it appends the entry
  (ADR 0006) and acknowledges after a majority fsync. Metadata learns the
  offsets later, when the uploader seals them: its `Publish` must start at
  the partition's `next_offset`. Producer sequences live in the WAL state
  machine until they are sealed into metadata as `ProducerNote`s.
- **`r2`.** The metadata leader buffers the batch, puts the object, and
  assigns offsets when `Publish` commits at `next_offset`. It acknowledges
  after that commit. Producer sequences are checked against metadata's
  `sequence_key` rows.

`Publish` refusing any part that does not start at `next_offset` keeps
metadata gap-free and overlap-free. That check alone does not make a
switch safe:

- **`wal` to `r2`.** Offsets `[sealed, wal_end)` are acknowledged in the
  WAL but not yet in metadata. An `r2` publish at `sealed` would commit
  first and take those offsets. The WAL's seal would then be refused, and
  acknowledged records would be lost.
- **`r2` to `wal`.** Batches in the leader's `r2` buffer get their offsets
  only when they publish. If the WAL accepts appends from the floor
  `next_offset` while those batches wait, both sides hand out the same
  offsets.
- **Either way.** A producer whose last batch was acknowledged on one path
  retries on the other. The other path must know its last sequence, or
  the retry is written twice.

## Decision

A switch is a topic-wide fence, drain, and flip, run by the metadata leader.
Only one path accepts produce for the topic at a time, and the new path
starts exactly where the old one ended.

1. **Fence.** `IncrementalAlterConfigs` (and `AlterConfigs`) setting
   `durability.mode` to the other value commits `Topic.durability_target`.
   The response returns once that commit has applied. From then on, both
   paths refuse new produce to the topic with `NOT_LEADER_OR_FOLLOWER` (6),
   which clients retry after refreshing metadata. Batches already appended
   to the WAL, or already queued in the `r2` buffer, finish on the old
   path. Fetch is unaffected.
2. **Drain.** The metadata leader waits, per partition, until the old path
   holds nothing that metadata lacks:
   - **From `wal`:** the shard's end offset, from `/wal/{shard}/end-offset`,
     equals metadata's `next_offset`. The uploader seals on its usual
     timer. The drain does not force a flush beyond requesting one.
   - **From `r2`:** the leader's buffer and its in-flight flush hold no item
     for the topic.

   Every partition must also have no open transaction span. An open
   transaction holds the drain until `EndTxn` or the transaction timeout
   finishes it.
3. **Flip.** One metadata command sets `durability` to the target and
   clears `durability_target`. Produce resumes on the new path.

Producer sequences carry over in both directions:

- **To `r2`.** The drain sealed every WAL sequence into metadata, which
  `r2` already reads.
- **To `wal`.** When a WAL shard first sees a producer on a partition it
  has no state for, `write_wal` reads that producer's `sequence_key` row
  from metadata. The row goes into the append (`Append::known`), and the
  WAL state machine starts the producer from it instead of treating the
  producer as fresh. The row is read on the shard leader, and the append
  carries it, so every replica applies the same state.

**Timing and failures.** A switch pauses produce on the topic for about one
upload interval in the `wal` to `r2` direction, or one `r2` flush in the
other. If the metadata leader changes mid-drain, the new leader sees
`durability_target` and resumes the drain. Setting the mode back to the
current value while a switch is pending clears `durability_target`. That
cancels the switch, and produce resumes on the old path.
`DescribeConfigs` reports the target value while the switch is pending,
with the synonym source `DYNAMIC_TOPIC_CONFIG`. `camus admin config
--topic` also shows `pending: <target>`.

**Ordering and loss.** No offset is ever acknowledged by both paths. The
new path's first offset is the old path's end. Records keep their order.
After a flip to `r2`, fetch reads only metadata's extents. That loses
nothing, because the drain sealed the whole WAL tail first. After a flip
to `wal`, fetch reads the extents and then the WAL tail, which starts at
their end.

## Test plan

A cluster test with MinIO. Three brokers, a `wal` topic with four
partitions, and an idempotent producer at `acks=all` running throughout:

1. Switch to `r2` under load. Then check that:
   - every acknowledged offset is readable;
   - offsets are gap-free and in order per partition;
   - no record appears twice;
   - every batch after the flip is in R2 by the time it is acknowledged,
     because the topic's WAL end stops moving.
2. Switch back to `wal` under the same checks. Retry a batch whose
   response was dropped just before the flip, through a proxy that drops
   one response, and check that it is not written twice.
3. Kill the metadata leader mid-drain. The new leader finishes the switch.
4. A transactional producer holds a transaction open across a switch. The
   flip waits for `EndTxn`, and `read_committed` shows the committed
   records exactly once.

Unit tests cover the config edit (pending target, cancel), the fence on
both produce paths, and the WAL seeding a producer from `Append::known`.

## Consequences

- `durability.mode` becomes alterable. compatibility.md drops "Set at
  creation, then read-only".
- A switch costs one produce pause per topic, bounded by the drain. A
  topic with a long-open transaction stays fenced until that transaction
  ends or times out.
- The topic row gains one optional field. Rows written before this
  change have none, which means no switch is pending.

## Alternatives

- **Per-partition switch.** This would shorten the pause on one partition,
  but it leaves a topic in mixed modes with no single answer for
  `DescribeConfigs`. Rejected.
- **Switch at a sealed boundary without fencing, by letting the new path
  start at a reserved offset.** This needs a gap in offsets or a
  reservation protocol between the two paths. Fencing is simpler, and its
  cost is a pause of about a second.
- **Copy the topic to a new one in the other mode.** This is what users do
  today. It changes the topic id and offsets, so it is not a migration.
