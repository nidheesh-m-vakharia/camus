# ADR 0014: Multiple WAL shards and leadership balance

Status: Proposed
Date: 2026-11-09

## Context

ADR 0006 chose several WAL shards, each an openraft group, with
`shard = partition mod shard_count`. What was built has one shard. The
shard key is fixed at `wal/0`, each broker opens one shard from `wal_dir`,
the WAL RPC routes have no shard in them, and `truncate-to-sealed` replaces
that one shard. Every broker is a voter, and the shard leader leads every
`wal` partition.

That leaves three Kafka behaviors out of reach:

- **Write load.** One broker takes every produce and every upload. Kafka
  spreads partition leaders across brokers.
- **Leader balance.** Kafka moves leadership back to each partition's
  preferred replica (`auto.leader.rebalance.enable`, default `true`) and
  offers `ElectLeaders` with `PREFERRED`. With one shard there is nothing
  to balance.
- **Failure scope.** A shard leader failure pauses produce on every
  partition until the election ends. With several shards it pauses one
  shard's partitions.

Issues #47, #48, #49, and #54 assumed the shards existed. This ADR records
how to build them before that work starts.

## Decision

### Shards and their voters

The cluster has `shard_count` shards, fixed when the cluster is initialized.
`POST /admin/init` accepts an optional `shards` count. When it is absent,
the count is the number of brokers in the request, so each broker can be the
preferred leader of one shard (#47). A shard's voters are
`min(3, brokers)` brokers, chosen round-robin starting at a different broker
for each shard, spread across racks when racks are configured (ADR 0006).
The first voter in the list is the shard's preferred leader.

Metadata keeps the layout, so every broker agrees on it:

- `wal/shards`: the shard count and each shard's voters.
- `wal/<shard>`: the shard view (leader, voters, epoch) the shard leader
  announces, as `wal/0` is today.
- `wal/<shard>/epoch-base`: the per-shard epoch base (ADR 0006 as amended
  by the leader-epoch work), raised by that shard's `truncate-to-sealed`.

Changing the shard count, or moving a partition to another shard, is
resharding and stays out of scope.

### Partition placement

Each partition records its shard when it is created, as ADR 0006 required:
a `shard` field on the partition row. The state machine chooses it, so every
replica decides the same way: a cluster-wide counter in metadata
(`wal/next-shard`) assigns shards round-robin across all partitions of all
topics, not per topic (#49). Otherwise a cluster of one-partition topics
would put everything on shard 0. `CreatePartitions` continues the same
counter. A partition row without the field, from a cluster created before
this change, is on shard 0.

### Runtime

The broker's single `WalSlot` becomes a set of shards keyed by id. Shard 0
stays in `wal_dir`, where the single-shard build keeps it, so an upgraded
broker moves nothing; shard `n > 0` lives next to it, in
`<wal_dir>-shards/<n>`, so a WAL on its own disk keeps every shard there. A
broker opens every shard in `wal/shards` as soon as it sees the layout; a
shard it is not a voter of stays empty and receives no RPC.

WAL RPC moves to `/wal/<shard>/raft-*` and `/wal/<shard>/init`. The current
`/wal/raft-*` routes stay as aliases for shard 0 for one release, so a rolling
upgrade can mix versions.

Every place that today calls `self.wal()` looks up the partition's shard
first: produce, fetch, ListOffsets, DeleteRecords' high watermark,
`OffsetForLeaderEpoch` and the unsealed-epoch scan, `current_leader_epoch`
checks, and Metadata, which reports each partition's own shard leader,
voters, and epoch. A broker that is not the leader of a partition's shard
answers `NOT_LEADER_OR_FOLLOWER` (6), as now.

Each shard leader runs its own uploader. Bundles stay per shard: one object
holds the partitions of one shard (ADR 0007). With `N` shards, up to `N`
puts happen per flush interval instead of one; the flush size and interval
are unchanged. `wal.disk.limit` stays a per-broker total over all its shards,
so a broker's disk bound does not grow with its shard count.

`truncate-to-sealed` takes the shard: `/admin/truncate-to-sealed` with a
`shard` field, required once there is more than one shard.

### Leadership balance (#48)

openraft 0.9 has no leadership transfer. It has `Raft::trigger().elect()`,
which makes a node campaign. A node whose log is current wins the vote, and
the old leader steps down when it sees the higher term.

Each broker runs a balancer, every `CAMUS_LEADER_IMBALANCE_CHECK_MS`
(default 300000, Kafka's `leader.imbalance.check.interval.seconds`): for each
shard whose preferred leader it is, if another broker has led the shard for
a whole interval, the shard has a quorum, and this broker's applied index has
caught up with the leader's commit index, it calls `elect()`.
`CAMUS_AUTO_LEADER_REBALANCE` (default `true`, Kafka's
`auto.leader.rebalance.enable`) turns it off.

The cost is one election per move: produce on that shard pauses for about an
election timeout, and the new term is a new leader epoch, which clients see
as a leader change. Moving leadership only after a whole interval keeps
flapping out.

### ElectLeaders (#54)

`ElectLeaders` with `PREFERRED` runs the same move for the shard of each
requested partition. The request is forwarded to the shard's preferred
broker, which calls `elect()`. Because leadership belongs to the shard,
every partition of that shard moves together, and the response reports that
for each partition requested:

- `ELECTION_NOT_NEEDED` (84) when the preferred broker already leads.
- `PREFERRED_LEADER_NOT_AVAILABLE` (80) when it is down or behind.
- `0` once it has started the election.

`UNCLEAN` stays unsupported (ADR 0008) and answers
`ELIGIBLE_LEADERS_NOT_AVAILABLE` (83). Authorization is cluster `ALTER`, as
in Kafka.

## As built

- `init` sets each shard's membership on every voter, not just the first,
  and returns only once every voter of every shard has it. A voter that
  granted a vote before its own initialize learns the membership from the
  leader instead; without the wait, a shard whose first leader died at
  once was left with no voter able to elect.
- Handovers are driven by the current leader, which alone can see whether
  the preferred voter has every entry. The balancer runs on each shard's
  leader. `ElectLeaders` asks the leader through `/wal/<id>/hand-over`.
- Followers that heard from a leader within its lease refuse votes, so one
  campaign can end in an election someone else wins. A handover therefore
  campaigns again, past the lease, until the preferred voter leads (up to
  about three seconds); `ElectLeaders` answers `0` only once it has.

## Delivery

Each step lands with its tests and leaves the cluster working.

1. **Data model, one shard.** The `shard` field, `wal/shards`, the shard set
   with a single member, per-shard routes with the old aliases, and lookups
   by partition. No behavior changes. Every existing test still passes.
2. **Several shards.** `init` with `shards`, placement, per-shard uploader
   and truncate, the data-dir move. Cluster test: three brokers, three
   shards, each broker leads one; killing a broker moves only its shard;
   every partition stays readable and writable through it.
3. **Balance and ElectLeaders.** Cluster test: after the killed broker
   returns, its shard comes back within one interval; `ElectLeaders`
   answers `ELECTION_NOT_NEEDED`, `0`, and `PREFERRED_LEADER_NOT_AVAILABLE`
   in the cases above; `kafka-leader-election.sh` in the ecosystem harness.
4. **Upgrade.** A cluster initialized by the current single-shard build
   restarts on the new build with its data in shard 0.

## Consequences

- `N` Raft groups per cluster: more heartbeats and more open logs per
  broker, bounded by `min(3, brokers)` voters per shard.
- Up to `N` object puts per flush interval, which matters for the R2 cost
  model in ADR 0007 at small flush sizes.
- Leadership moves pause one shard briefly and bump its epoch.
- A failed broker pauses only the partitions of the shards it led.
- Partitions on one shard still share one quorum and one leader, so a hot
  partition's load moves with its shard, not on its own.

## Alternatives

- **Raft group per partition.** Rejected in ADR 0006 for election and
  snapshot overhead.
- **Leadership transfer in openraft 0.10.** Cleaner than campaigning, but
  0.10 is not stable (ADR 0004). Revisit when it is.
- **Moving partitions between shards to balance load.** Resharding; needs
  a copy protocol and is a separate decision.

## Resolved in review

- Default shard count: one per broker, as above. A multiple of the broker
  count would spread a lost broker's leadership wider but multiplies Raft
  groups, heartbeats, and object puts.
- Bundling: one object per shard per flush, as above, so a slow or
  truncated shard never holds up another's publish.
