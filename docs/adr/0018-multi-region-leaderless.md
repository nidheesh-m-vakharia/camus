# ADR 0018: Multi-region leaderless replication

Status: Proposed
Date: 2026-11-12

## Context

A Camus cluster is one region. Metadata and each WAL shard elect inside
that region, on a 50 ms heartbeat and a 150–300 ms election timeout
([0016](0016-quic-control-plane.md)). A round trip into another region is
the same size as that window, so a voter set that crosses regions stalls
elections and every ack. [Issue 147](https://github.com/nidheesh-m-vakharia/camus/issues/147)
asks for that limit to be written down, and for MirrorMaker 2 to be tested
with Camus as the source. [Issue 59](https://github.com/nidheesh-m-vakharia/camus/issues/59)
already covers Apache Kafka 4.4.0 into Camus.

Offsets are a gap-free sequence assigned by one home for the partition
([0009](0009-offsets-and-record-batches.md), [0001](0001-system-architecture.md)).
An ack means a WAL majority fsync, or an R2 put plus a metadata commit
([0005](0005-durability-and-acks.md)). Sealed bytes are the object, and any
broker that can read metadata and the bucket can serve them.

Three systems get cited for a multi-region mode:

- Apache Pulsar keeps a total order inside one cluster. BookKeeper accepts
  a ledger append from one writer onto a quorum of bookies, with no storage
  leader. Geo-replication is a second cluster with its own cursors, copying
  asynchronously. The clusters do not vote together.
- Apache Cassandra and ScyllaDB take a write in any region and reconcile
  replicas with a point timestamp (last writer wins). Scylla uses Raft for
  schema and topology. The data path is still that quorum. A clock that
  runs ahead publishes a value that a slower region cannot displace.
- Google's TrueTime, from the Spanner paper (Corbett et al., OSDI 2012),
  treats the clock as an interval `[earliest, latest]` that is guaranteed
  to contain absolute time. Between synchronizations the interval widens at
  a maximum drift rate (their deployment uses 200 μs/s, on GPS and atomic
  clocks). A commit picks `s = latest` and waits until `earliest > s`
  before the timestamp is published. Drift becomes a wait, not a reorder.

Pulsar is the replication shape that can sit beside ADR 0009. TrueTime is
the clock rule. Cassandra's quorum is the failure rule worth keeping: a
dead region must not start an election somewhere else.

## Decision

A region is one Camus cluster. It has its own metadata Raft group, its own
WAL shards, and its own bucket. No broker is a voter in another region's
group. Election timeouts stay the ones in ADR 0016.

A partition has one home region. Only that region assigns offsets. A
produce sent to any other region is forwarded to the home. The home's ack
is still the ADR 0005 barrier. Forwarding does not add a second Raft group.

Replication copies sealed objects. Any broker in a destination region may
pull an object and install it. There is no leader election across regions
and no cross-region vote. The destination publishes those bytes under its
own replication cursor. A Kafka client in the destination reads that
cursor. It does not see the source partition's offsets. Translating
between them is MirrorMaker 2, which is the interop path in issues 59 and
147, not a second offset assignment.

### Clocks

Multi-region code uses TrueTime's rule, not a wall-clock instant.

- The broker reads the host clock daemon's error bound `ε` and its drift
  rate. The time of a sample is the interval `[sample − ε, sample + ε]`.
  Until the next sample, both ends move outward by `drift × elapsed`.
- A replicated batch is stamped `s = latest` at the home when the object
  is sealed. A destination applies it only after `earliest > s` on the
  destination's own interval. A home that publishes the replication record
  does the same wait. Overlapping intervals cannot be ordered, so they are
  not applied in timestamp order until the wait ends.
- Camus does not assume a GPS or atomic clock. Google's 1–7 ms sawtooth is
  their measurement on that hardware. It is not a Camus target. `ε` is
  whatever the daemon reports.
- If the daemon has no source, or it will not report a bound, the broker
  refuses the multi-region publish and the destination refuses the apply.
  The home region still acks local produces under ADR 0005. A drifted
  clock cannot move an offset.

Promoting a destination to home is an operator action, in the same class
as `truncate-to-sealed` ([0008](0008-failure-and-recovery.md)). A source
region going away does not elect a home anywhere else. Partitions homed in
a surviving region keep taking produces. Partitions homed in the dead
region stop at the last sealed object a destination has applied.
Unreplicated acks in the dead region follow ADR 0008 for that region.

Two regions both producing into one offset sequence is not this mode. That
would be two homes and two cursors. It can be added later. It does not
merge into one gap-free sequence.

## Consequences

- A replicated batch becomes visible in another region only after the
  object copy and the commit-wait. The wait is the width of the interval.
  On a daemon with a large `ε`, that wait dominates. Local `wal` and `r2`
  acks do not take the wait.
- Destination offsets and source offsets differ. Clients that assume one
  global offset are wrong. The difference belongs in
  `docs/compatibility.md` when this is built.
- [Issue 151](https://github.com/nidheesh-m-vakharia/camus/issues/151) is
  the check that two regions did not fork one offset sequence.
  [Issue 141](https://github.com/nidheesh-m-vakharia/camus/issues/141) is
  still the one-region R2 measurement. Cross-region cost is not claimed
  from it.
- [Issue 147](https://github.com/nidheesh-m-vakharia/camus/issues/147)
  stays the documentation and MirrorMaker work. This ADR is the native
  copy beside that. Building it is a later change. Nothing in the tree
  speaks this mode yet.
- Object layout stays write-once ([0007](0007-r2-layout-and-cost.md)). A
  replicated object is a new key in the destination bucket. The source key
  is not rewritten.

## Alternatives

- One Raft group stretched across regions. Set aside. The election window
  is 150–300 ms, and every ack would wait on the far region. A blip there
  stops the metadata group.
- Cassandra or ScyllaDB last-writer-wins on one partition. Set aside. Two
  regions would both append, and ADR 0009 forbids a gap, a reuse, or a
  silent reorder. A point timestamp publishes the write of whichever clock
  is ahead. Scylla's topology Raft does not order the log. The quorum idea
  that survives is local to the home region, which ADR 0005 already is.
- A BookKeeper ensemble spread across regions, one writer, a cross-region
  write quorum. Set aside for the same reason as stretched Raft: the ack
  waits on the far region, on every produce.
- Assigning the next offset from a quorum of TrueTime bounds, with no home.
  A record at `s` could take an offset only once every region in the quorum
  had promised it would not commit an earlier `s`. A region that stops
  reporting blocks the log. Removing it is the same operator step as
  promoting a home, so the home region is the smaller rule.
- Trusting NTP as an instant. Set aside. That is the Cassandra failure this
  clock rule is here to avoid.
