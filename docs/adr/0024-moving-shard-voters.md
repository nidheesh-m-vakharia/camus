# ADR 0024: Adding and removing brokers by moving shard voters

Status: Proposed
Date: 2026-11-16

## Context

Issue #179. A partition is placed on a WAL shard when it is created and
never moves (ADR 0014). A shard's voters are chosen at `init` and recorded
in `wal/shards`. The metadata group's voters are every peer passed to
`init` (ADR 0004). That leaves an operator with no way to grow or shrink a
cluster:

- A broker started after `init` is in no group. It cannot serve metadata
  and leads nothing.
- A broker that is gone for good stays a voter of the metadata group and
  of each of its shards. Each of those shards runs on two of three voters
  from then on, so one more failure stops it.
- `POST /admin/add-wal-voter` exists, but only to rebuild a shard after
  `truncate-to-sealed`. It adds a voter and never removes one, and it does
  not update `wal/shards`, so the balancer still prefers the old voter.

Kafka answers this with `AlterPartitionReassignments`: it adds new replicas,
copies the partition's whole retained log to them, then drops the old ones.
On a large topic that copy runs for hours and competes with produce for
disk and network.

## Decision

Grow and shrink a cluster by moving shard voters between brokers with Raft
membership changes. Shards are not split or merged, and partitions stay on
their shards.

### Why moving a voter is cheap here

A shard's Raft log holds only the unsealed tail: what has been
acknowledged and not yet uploaded. Everything older is in R2 and indexed
in metadata, and any broker reads it from there (ADR 0007, ADR 0010). A
new voter therefore copies at most `wal.disk.limit` (default 8 GiB) per
broker, and in steady state only one flush interval's worth of data. That
copy does not grow with retention or with the size of the topics. Kafka's
reassignment copies every retained byte of every moved replica.

### Joining the metadata group

A new broker starts with `CAMUS_JOIN=<addr of any broker>`. It asks the
metadata leader to add it as a learner. A learner applies every committed
entry, so it can answer `Metadata` and route requests (ADR 0004), and it
appears in the broker list clients see. The metadata group keeps an odd
voter count of three or five. `camus admin metadata promote <id>` and
`camus admin metadata demote <id>` change voters one at a time. Promotion
waits until the learner has caught up with the leader's commit index, the
same check `add_voter` makes for a WAL shard.

### Moving a shard voter

`camus admin move-voter --shard S --from A --to B` runs on the shard
leader:

1. `join-wal` on `B` opens an empty shard there, as it does today.
2. Add `B` as a learner and wait until it has the leader's log
   (`add_learner` with `blocking` set to true).
3. One joint-consensus change replaces `A` with `B`
   (`ChangeMembers::ReplaceAllVoters` with the new set, openraft 0.9).
   Commits need a majority of both the old and the new voter set until it finishes, so
   no acknowledged entry depends on `A` alone.
4. Write `wal/shards` with `B` in `A`'s position. When `A` was the first,
   preferred voter, `B` becomes preferred, and the balancer moves
   leadership to `B` after one interval (ADR 0014).
5. `A` closes the shard and moves its directory aside, as `join-wal` does.

When `A` leads the shard, it hands leadership to another voter first,
through the existing hand-over, so the change is never driven by the
voter being removed.

A move that fails before step 3 leaves a learner, and the next attempt
reuses it. A move that fails during step 3 is finished by openraft once a
new leader of the joint configuration is elected. Step 4 runs again on the
next `move-voter` or on the leader's restart. A broker compares the shard's
Raft membership with `wal/shards` and rewrites the row when they differ.

### Adding and removing brokers

- **Add a broker.** Start it with `CAMUS_JOIN`, then `camus admin
  rebalance-voters`. That computes the layout `init` would have chosen for
  the new broker count, the same round-robin as `shard_voters`, and runs
  the minimum set of moves to reach it, one shard at a time. The new
  broker becomes the preferred leader of its share of shards.
- **Remove a broker.** `camus admin drain <id>` moves every voter slot the
  broker holds to the remaining brokers, demotes it from the metadata
  voters if it is one, and removes it from the metadata membership. Drain
  refuses to leave any shard with fewer voters than it had, or the
  metadata group with fewer than three voters, unless `--force` is passed.

Default shard counts already give a new broker leadership to take. `init`
creates up to eight shards per voter (ADR 0014), so a three-broker cluster
has up to 24 shards to spread over four or six brokers. A cluster created
with `CAMUS_WAL_SHARDS=voters` has one shard per broker and gains no
leadership from a new broker until resharding exists.

### `AlterPartitionReassignments` meanwhile

`AlterPartitionReassignments`, `ListPartitionReassignments`, and
`AlterReplicaLogDirs` stay unimplemented and absent from `ApiVersions`.
The admin client sees the key missing from `ApiVersions`, so
`kafka-reassign-partitions.sh` fails with `UnsupportedVersionException`
before it sends anything. A partition's replicas are its shard's
voters, so a per-partition reassignment would move every partition of that
shard. Doing that silently would be surprising, and moving one partition
alone is resharding. `docs/compatibility.md` points operators to
`camus admin move-voter`, `rebalance-voters`, and `drain`.

## Test plan

A cluster test in `crates/broker/tests/`, run nightly with MinIO:

1. Three brokers, `wal` durability, six shards, an idempotent producer at
   `acks=all` on twelve partitions, and a `read_committed` consumer
   running throughout.
2. Start a fourth broker with `CAMUS_JOIN`. Check that it shows up in
   `Metadata` as a learner within a few seconds.
3. Run `rebalance-voters`. Check that the fourth broker holds two of the
   six shards and is the preferred leader of at least one, and that the
   balancer hands that shard's leadership to it.
4. Repeat the move with faults injected:
   - kill the shard leader during the learner catch-up;
   - kill the leader during the joint change;
   - kill `B` after it becomes a voter, before `wal/shards` is written.

   Each case must converge after the restart, either to the new layout or
   to the old one, and never to a shard with fewer voters than it started
   with.
5. `drain` the third broker. Check that every shard keeps three voters and
   the metadata group keeps three voters, then stop the broker.
6. After every step, the checker from #151 verifies:
   - every acknowledged offset is readable;
   - offsets are unique and have no gaps;
   - `read_committed` shows no aborted record.

   Report client errors and the longest produce stall. The stall bound is
   one election timeout per leadership change.

Unit tests cover the layout planner (fewest moves, one voter per broker
per shard, preferred leaders spread evenly) and the membership versus
`wal/shards` repair.

## Consequences

- Scaling out moves leadership, not data. Each move copies one shard's
  unsealed tail, and produce on that shard pauses once, for the hand-over
  when the preferred leader changes.
- Brokers become one of two kinds in metadata: voters and learners. The
  metadata group stays at three or five voters however large the cluster
  grows.
- A cluster can grow to more brokers than shard voter slots. Extra brokers
  serve fetches from R2 and lead nothing until resharding exists.
- The broker gains a membership repair loop: Raft membership is the truth
  and `wal/shards` follows it.

## Alternatives

- **Split and merge shards.** This changes the partition-to-shard map,
  which would need a copy or a seal-boundary handoff with offsets carried
  across. It is useful for a hot shard, but it is not needed to add or
  remove a broker. It is left to a later resharding ADR.
- **Implement `AlterPartitionReassignments` as a shard move.** Rejected
  because one request would move partitions it did not name.
- **Replace a lost broker under the same node id.** This works today for a
  dead disk, through `join-wal` and `add-wal-voter`, but it cannot change
  the number of brokers.
