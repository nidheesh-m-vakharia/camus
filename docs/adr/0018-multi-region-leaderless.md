# ADR 0018: Mirror topics between regions

Status: Accepted
Date: 2026-11-12

Note, 2026-11-14: one cluster per region, with MirrorMaker 2 between
regions, is accepted and tested as [ADR 0020](0020-one-region-per-cluster.md).
This record stays proposed for the native copy of sealed objects.

Note, 2026-11-16: accepted after the revision below. Nothing is built
yet. The phases are under Delivery.

Note, 2026-11-16: the first draft left four things open: how a
destination finds the source's sealed objects, which offsets a destination
client reads, how a produce in a non-home region reaches the home, and
what the commit-wait protects. This revision answers them. It also adds a
block index to metadata snapshots, so a destination reads only the
snapshot rows of the topics it mirrors. It changes two earlier choices.
Destination offsets now equal source offsets, and a produce to a mirror
is refused instead of forwarded. The draft's reasons are kept under
Alternatives.

## Context

A Camus cluster is one region. Metadata and each WAL shard elect inside
that region, on a 200 ms heartbeat and a 400–800 ms election timeout
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

A region is one Camus cluster, as in ADR 0020. It has its own metadata
Raft group, its own WAL shards, and its own bucket. No broker is a voter
in another region's group. Election timeouts stay the ones in ADR 0016.

### Mirror topics

A partition has one home region. Only that region assigns offsets.

A destination region holds the topic as a **mirror topic**. It is
read-only, and it has the source's partition count and the source's
offsets. A Kafka client in the destination fetches offset `N` and gets
the same batch the home assigned at `N`. A record's offset is the same in
every region that holds it.

An operator creates a mirror with
`camus admin mirror create --source <alias> --topic <name>`. The
destination's metadata records the topic with a new topic id and a
`mirror_of` field naming the source alias and the source topic id. A
topic that already exists in the destination cannot become a mirror.

`Produce` to a mirror topic returns `INVALID_TOPIC_EXCEPTION` (17), with
the error message "Cannot append to mirror topic `<name>` of `<alias>`".
That is what Kafka 4.4.0 returns for an append to an internal topic
(`ReplicaManager.appendToLocalLog`): the topic exists, and clients may
not write to it. The Java client does not retry it, so a producer that
points at the wrong region fails fast. Producers write to the home
region's cluster.

`DeleteRecords`, `CreatePartitions`, and `AlterConfigs` on a mirror topic
return `POLICY_VIOLATION` (44), with the same message. That is Kafka's
error when the broker refuses an admin change. Retention configs stay
alterable, because a mirror's retention is its own.

### Linking clusters

Brokers in different clusters never connect to each other. Inside a
cluster, brokers use the HTTP port for Raft, WAL shard calls, and
`/admin/*`, and QUIC when `CAMUS_QUIC` is on (ADR 0016). That port has no
authentication, so it stays on the cluster's private network. A link
between clusters is configuration on the destination only. The source
is not told that it is being mirrored, and it is not configured for it.

A link is a source alias. An operator adds one with:

```
camus admin mirror link add --alias us-east \
  --endpoint https://… --region auto --bucket camus-us-east \
  [--prefix …] [--virtual-host]
```

- The alias is 1–32 characters of `[a-z0-9-]`, and it is unique in the
  destination. It names the link in `mirror_of`, in metrics, and in the
  write errors.
- The endpoint, region, bucket, prefix, and addressing style are a
  metadata record, the same fields as the cluster's own `s3.*` settings.
  Every broker learns them through Raft.
- The access key and secret key are not stored in metadata. Snapshots are
  uploaded to the destination's bucket, so a key in metadata would end up
  in that bucket. Each broker reads `CAMUS_MIRROR_<ALIAS>_ACCESS_KEY` and
  `CAMUS_MIRROR_<ALIAS>_SECRET_KEY`, where `<ALIAS>` is the alias in upper
  case with `-` turned into `_`. That is how `CAMUS_S3_SECRET_KEY` works
  today. Keys are not logged. To rotate them, restart the brokers one at
  a time.
- The copier runs on the metadata leader, so every metadata voter needs
  the keys. A leader without them does not copy. It sets
  `camus_mirror_stalled` to 1 for each of that link's partitions and logs
  which variable is missing.
- The key should be read-only on the source bucket. Camus never writes,
  deletes, or lists through a link, so a key that can do more is not
  needed.

`link add` checks the link before it commits it. It reads the source's
`meta/snap/CURRENT` and that snapshot's trailer with the given keys, and
it refuses the link if either read fails or the snapshot is not
`CAMUSKV3`. `camus admin mirror link remove --alias` refuses while a
mirror topic still names the alias. To remove one, promote or delete
those topics first.

### Finding sealed objects

A destination does not talk to source brokers. It reads the source
bucket with a read-only credential configured for that source alias:

1. GET the source's `meta/snap/CURRENT`, then the index at the end of the
   snapshot it names (ADR 0004, and the format below). A snapshot is one
   committed state of the source's metadata. It names only published
   extents and never an orphan (ADR 0007).
2. For each mirrored partition, ranged-GET only the snapshot blocks that
   hold its rows, and take the extents from the destination's
   `sealed_end` onward.
3. Ranged-GET each extent's bytes from the source bucket. Concatenate the
   extents for one flush into one new object under the destination's own
   `bundle/{ulid}.seg` key, with the same footer format. PUT it to the
   destination bucket.
4. Commit the extents to destination metadata with the ADR 0007
   compare-and-append on `sealed_end`. The batch bytes, base offsets, and
   leader epochs are the source's, byte for byte.

The destination publishes a mirrored batch only after its own bucket holds
the bytes. That is the ADR 0005 `r2` barrier: an R2 put plus a metadata
commit. A mirror never serves bytes from the source bucket, so losing the
source region does not lose anything the destination has published.

No source broker has to be up for this. A source region whose brokers are
gone but whose bucket answers can still be drained.

### Snapshot block index

A snapshot body is the metadata rows in key order (`CAMUSKV2`). A
topic's rows are contiguous in it: `part/{topic_id}/…` for its
partitions, `ext/{topic_id}/{partition}/{end}` for its extents, and
`offset/{group}/{topic_id}/…` for one group's offsets. This ADR adds
`CAMUSKV3`: the same rows, followed by a block index and a fixed
trailer.

- A block is the rows that start in one 64 KiB stretch of the body. It
  always ends on a row boundary.
- The index lists each block's first key and byte offset.
- The trailer holds the index's offset and length, a CRC32C of the
  index, and the magic again.

The index is part of the one snapshot object. It adds no put and no
write to `CURRENT`. Its size is about one entry per 64 KiB of body,
under 1 MiB for a 1 GiB snapshot. Recovery and `decode_rows` read
`CAMUSKV3` by ignoring the index. They still read `CAMUSKV2` and the
older JSON body.

Amended 2026-11-16, before any broker wrote `CAMUSKV3`. The trailer's
CRC32C covered only the index, so a flipped byte in a row still decoded
and could be restored as wrong metadata. Each index entry now also holds
the CRC32C of its block's bytes. A copier checks only the blocks it
reads, which one CRC over the whole body would not allow. Recovery and
`decode_rows` no longer ignore the index. They check every block, and
they check that each entry starts a row with that entry's key, beginning
at the first row.

A copy reads `CURRENT`, then the trailer and index in one ranged GET of
the object's tail, then one ranged GET for each mirrored topic's run of
blocks. The bytes read follow the mirrored topics' metadata, not the
whole source cluster's. The index is capped at 4 MiB when it is read.
An index over the cap fails the copy with `camus_mirror_stalled` set to
1. The copier does not fall back to the whole body.

The copy runs on the destination's metadata leader, one task per source
alias. Its object buffer is capped at 8 MiB, the same as the flush cap.
It decodes the snapshot blocks it fetched one at a time and keeps only
the mirrored partitions' rows.

Copy lag is the source's snapshot interval plus one copy, about 30 seconds
at the ADR 0004 defaults. The source's WAL tail is not copied. Only sealed
objects are.

### Gaps and deletes at the source

- The source's compactor can replace an extent and delete the old key
  after the snapshot was read. A GET that returns 404 makes the copier
  read `CURRENT` again. The new snapshot names the replacement key, and
  the offsets in it are unchanged (ADR 0010).
- If source retention has moved the log start past the destination's
  `sealed_end`, the mirror cannot stay gap-free. That partition's copy
  stops. `camus_mirror_stalled` goes to 1, and the destination keeps
  serving what it has. `camus admin mirror reset --topic --partition`
  moves the destination's log start to the source's log start, which
  drops the destination's copy of that partition. It refuses without
  `--i-accept-data-loss`.
- A mirror's retention is its own topic config, so a destination can keep
  data longer than the source does.

### Transactions and consumer groups

Each copied extent carries the source's committed and aborted
transaction spans from the same snapshot. ADR 0021 makes markers at
fetch, so a `read_committed` consumer in the destination sees the same
records that it would see at the source. The destination's last stable
offset is its sealed end, or the start of the first span the snapshot
still shows open, whichever is lower.

Offsets are equal, so committed group offsets can be copied without
translating them. For the groups an operator lists, the copier writes the
snapshot's committed offsets into destination metadata. It does this only
while the group has no members in the destination, which is the
`sync.group.offsets.enabled` rule from MirrorMaker 2. It clamps each
offset to the destination's high watermark.

### Promotion

`camus admin mirror promote --topic <name>` stops the copy, clears
`mirror_of`, and makes the topic writable. The next produce is assigned
the destination's `next_offset`, the end of what was copied. Metadata
keeps the promoted end per partition as `promoted_at`.

Promotion is an operator action, in the same class as
`truncate-to-sealed` (ADR 0008). The destination does not promote itself
when the source goes silent. Batches the source acked and had not sealed,
or sealed after the last snapshot that was copied, exist only in the
source. If the source region comes back, its log and the promoted log
both hold offsets past `promoted_at`. That is a fork.
[Issue 151](https://github.com/nidheesh-m-vakharia/camus/issues/151) is
the check that finds it. Mirroring back into the old source has to start
from `promoted_at`, and the old source's suffix is discarded with the
same `--i-accept-data-loss` gate.

### Clocks, for promoting several partitions at one cut

Phases 1 and 2 below do not read a clock. Offsets come from the home, and
a source snapshot is already one consistent cut of every partition homed
in that region. Promoting every mirror of one source at the same snapshot
gives a consistent failover for that region.

The clock rule is for one destination that mirrors from several source
regions. An operator who wants one cut across all of them needs
timestamps that respect real-time order across homes. That is the only
thing the commit-wait is for. It is phase 3, and it uses TrueTime's rule,
not a wall-clock instant:

- The broker reads the kernel's error bound with `ntp_adjtime(2)`
  (`maxerror`, which the kernel grows by the 500 ppm tolerance each
  second between syncs). The time of a sample is the interval
  `[now − maxerror, now + maxerror]`.
- The home stamps each extent with `s = latest` when it is sealed, and
  waits until `earliest > s` before it commits the extent to metadata.
  If extent `a` commits before extent `b` is sealed, in absolute time and
  at any home, then `s_a < s_b`.
- A destination publishes `applied_s` for each source: the `s` of the
  snapshot it last finished copying. A promotion at cut `c` uses, for each
  source, only extents with `s ≤ c`. It refuses unless every source's
  `applied_s ≥ c`.
- If `ntp_adjtime` reports `TIME_ERROR` or the clock is unsynchronised,
  the home stamps nothing, and the extent is not eligible for a cross-region
  cut. Local `wal` and `r2` acks never wait on the clock. A drifted clock
  cannot move an offset.

Camus does not assume a GPS or atomic clock. Google's 1–7 ms sawtooth is
their measurement on that hardware, not a Camus target.

Amended 2026-11-25. The sketch above stamps an extent when it is sealed.
That does not give the property it states. A `wal` produce is
acknowledged at the WAL fsync, often well before its batch is sealed, so
a record acknowledged first can be stamped later than a record produced
after it in another region. The sketch also has the commit wait in front
of the metadata commit, and in `r2` mode that commit is the
acknowledgement. Both points are replaced by "Phase 3" at the end of this
record, which was accepted on the same day.

### Delivery

0. Reading `CAMUSKV3` snapshots, in one release, before any broker
   writes them. Then the writer.
1. Link add and remove, mirror create, copying from the indexed
   snapshot, the write errors above, the gap stall and reset, and
   metrics. Acceptance test: Camus to Camus with MinIO as two buckets.
   Offsets and batch bytes are equal. Killing every source broker after
   the last snapshot loses nothing the destination published. A link
   with a wrong key is refused at `link add`.
2. Transaction spans, consumer group offset sync, promotion, and the
   issue 151 fork check.
3. Clock stamping, `applied_s`, and promotion at a cut across sources.

## Consequences

- A record in the destination has the same offset, the same bytes, and the
  same `read_committed` visibility as at the source. A consumer that fails
  over keeps its committed offset without translation. This replaces the
  MirrorMaker 2 offset-sync granularity in ADR 0020 for Camus to Camus.
  Kafka to Camus and Camus to Kafka stay on MirrorMaker 2.
- The copy is asynchronous. A batch that the source acked and the
  destination has not copied is lost to the destination if the source
  region goes. The window is the snapshot interval plus one copy, plus
  the source's unsealed WAL tail. It is not an ack barrier.
- A mirror topic is read-only. Mirror topics are not a Kafka 4.4.0
  feature. The write errors, `mirror_of`, and equal offsets go in
  `docs/compatibility.md`.
- Reading the snapshot for one copy costs `2 + T + G` GETs per source:
  `CURRENT`, the snapshot tail, one range for each of the `T` mirrored
  topics, and one for each of the `G` groups whose offsets are synced.
  The GETs for batch bytes come on top. At the 30 second interval
  that is Class B, and it does not depend on partition count. Snapshot
  bytes read follow the mirrored topics' metadata and not the whole
  source cluster's, which matters on S3 where transfer between regions
  is billed. R2 does not charge egress.
- Snapshots change format to `CAMUSKV3`. A broker that only reads
  `CAMUSKV2` cannot read the new snapshot, so the reader ships one
  release before the writer.
- Object layout stays write-once (ADR 0007). A copied object is a new key
  in the destination bucket. The source key is never rewritten.
- The destination needs a read-only credential for each source bucket.
  It needs no network path to source brokers, only an HTTPS path to the
  source bucket. An operator who loses that key cannot copy until a new
  one is set on the voters.
- [Issue 141](https://github.com/nidheesh-m-vakharia/camus/issues/141)
  is still the one-region R2 measurement. Cross-region cost is not
  claimed from it.

## Alternatives

- One Raft group stretched across regions. Set aside. The election window
  is 400–800 ms, and every ack would wait on the far region. A blip there
  stops the metadata group.
- Cassandra or ScyllaDB last-writer-wins on one partition. Set aside. Two
  regions would both append, and ADR 0009 forbids a gap, a reuse, or a
  silent reorder. A point timestamp publishes the write of whichever clock
  is ahead.
- A BookKeeper ensemble spread across regions, one writer, a cross-region
  write quorum. Set aside for the same reason as stretched Raft: the ack
  waits on the far region, on every produce.
- Destination offsets of its own, under a replication cursor (the first
  draft). Set aside. A client would produce at the home, get offset `N`,
  and then fetch a different `N` in its own region. Failover would need
  offset translation again, which MirrorMaker 2 already does.
- Forwarding a produce from a non-home region to the home (the first
  draft). Set aside for now. Kafka 4.x producers are idempotent by
  default, and producer ids are per cluster. A forwarded idempotent or
  transactional produce would need the home to own the producer id that
  the destination handed out. Refusing the write is a fast, clear error.
  It can be revisited once mirrors work.
- Reading the source over a broker API instead of its bucket. Set aside.
  It needs an authenticated cross-cluster port, and it fails when the
  source brokers are down, which is the case mirroring is for.
- Reading the whole source snapshot each interval. Set aside. The bytes
  read would grow with every topic and group at the source, including
  ones that are never mirrored.
- A per-topic manifest that the source writes beside each snapshot. Set
  aside. That is an extra put per mirrored topic per snapshot, and the
  block index gets the same reads without any new writes.
- `POLICY_VIOLATION` for `Produce` on a mirror. Set aside. It is not one
  of Kafka's produce errors, and 4.4.0 already uses
  `INVALID_TOPIC_EXCEPTION` for an append to a topic that clients may not
  write.
- Listing the source bucket and reading footers, as recovery does
  (ADR 0008). Set aside. A listing cannot tell a published object from an
  orphan, and LIST is Class A.
- Assigning the next offset from a quorum of TrueTime bounds, with no home.
  A region that stops reporting blocks the log. Removing it is the same
  operator step as promoting a home, so the home region is the smaller
  rule.
- Trusting NTP as an instant. Set aside. That is the Cassandra failure the
  clock rule avoids.

## As built

Phase 0, the reader. `decode_rows` in `crates/metadata/src/machine.rs`
reads `CAMUSKV3`. Snapshot install, `camus admin restore-cluster`, and
`camus admin verify-snapshot` all go through it.

- The body is `CAMUSKV3`, then the rows exactly as after `CAMUSKV2`. Next
  comes the index, a postcard `Vec<(String, u64, u32)>`. Each entry holds
  a block's first key, that row's byte offset in the body, and the CRC32C
  of the block, which runs to the next block or to the index. Last is a
  28-byte trailer: the index offset (u64 LE), the index length (u64 LE),
  the CRC32C of the index (u32 LE), and `CAMUSKV3`.
- A row starts a new block when it begins at least 64 KiB past the start
  of the previous block. A block therefore always starts on a row.
- The reader refuses a body in any of these cases:
  - the trailer is short, or lacks its magic
  - the index does not end at the trailer
  - the index fails its CRC32C
  - the rows do not fill exactly the bytes before the index
  - the first entry is not the first row
  - an entry does not start a row, or names a different key
  - a block fails its CRC32C

  Before anything is written, install decodes and checks the whole body,
  so a refused body leaves the state machine unchanged.
- In the reader release, `build_snapshot` still wrote `CAMUSKV2`, and a
  test pinned that so a broker one release older could install every
  snapshot. The writer below changed this.

Phase 0, the writer, 2026-11-18. `build_snapshot` now writes `CAMUSKV3`
through `IndexedBody`, which streams rows from the `redb` table. A test
covers the indexed slice encoder, and it produces the same bytes. The
index holds about one entry per 64 KiB of body, and it is the only memory
added beyond the body itself. Snapshots from the release before
(`CAMUSKV2`) and the older JSON bodies still install. The writer shipped
after the reader image had been published on `main` and deployed, so a
rolling upgrade never puts a `CAMUSKV3` snapshot in front of a broker that
cannot read it.

Rollback: a broker older than the reader release cannot install a
`CAMUSKV3` snapshot. A cluster on the writer can roll back only to the
reader release or later. Rolling back further needs every voter's log to
cover its last snapshot, which is not something an operator can count on.

Phase 1, first part: links and read-only mirrors. Copying is not built.

- `crates/metadata/src/mirror.rs` holds `MirrorLink`, stored at
  `mirror/{alias}`, and the `mirror_of` field on a topic row. The commands
  `AddMirrorLink`, `RemoveMirrorLink`, and `CreateMirrorTopic` come after
  `Batch` in `Command`. The Raft log is postcard, which numbers variants by
  position, so existing entries keep their meaning.
- Adding the same link again is a no-op, so a retry succeeds.
  `POLICY_VIOLATION` is returned in three cases:
  - a different link is added under an alias that is in use
  - a link is removed while a mirror still names it
  - a link is added past the cap of 64

  An endpoint, region, bucket, or prefix longer than its limit (2048, 64,
  63, and 1024 bytes) is `INVALID_REQUEST`, and so is an alias that does
  not match `[a-z0-9-]{1,32}`.
- The metadata group enforces the read-only rule when it applies a
  command, so the rule holds whichever broker or API sends it.
  `CreatePartitions` and `DeleteRecords` on a mirror are refused, and so
  is a config change that alters the value of anything except
  `retention.ms` and `retention.bytes`. Configs are compared by value, so
  a legacy `AlterConfigs` that drops an override equal to its default is
  not a change.
- Produce is guarded twice. The broker refuses it with
  `INVALID_TOPIC_EXCEPTION`, checking its own copy of metadata. That copy
  can be behind. Picture a topic deleted and recreated under the same name
  as a mirror: a broker that has not caught up could still accept a batch.
  So `Publish` also refuses a mirror when it applies, with unknown-topic
  (3), the code a deleted topic gets. The uploader already drops a deleted
  topic's batches by topic id. Transaction span marking treats a mirror as
  a deleted topic, too.
- The broker's refusal message is fixed. It reads "Cannot append to a
  mirror topic. Produce to its source cluster." and names neither the
  topic nor the alias, since the response row already names the topic.
- The admin routes are `GET` and `POST /admin/mirror/links` and
  `DELETE /admin/mirror/links/{alias}`. The CLI commands are
  `camus admin mirror link add`, `remove`, and `list`. They return 400
  and 409 for the two refusal codes.
- Upgrade order: a broker older than this release cannot decode the three
  commands, and it drops `mirror_of` when it parses a topic row. Every
  broker must run this release before the first `link add`. A test pins
  the commands at log positions 29 to 31, after `Batch` at 28.
- `link list` reads the local broker's applied metadata, so a link just
  added through another broker can be missing for one commit.
- `crates/broker/tests/mirror.rs` is the end-to-end test. Only one path
  has no test: forwarding a link command from a broker that does not lead
  metadata. It uses the same leader lookup as `metadata_write`.

Phase 1, second part: reading the source.

- `crates/metadata/src/snapshot.rs` reads part of a `CAMUSKV3` body.
  `index_span` parses the trailer from the body's tail and refuses an
  index over 4 MiB. `SnapshotIndex::parse` checks the index CRC and that
  the entries are in order. `run(from, to)` names the blocks that can hold
  a key range, and `rows` checks each of those blocks against its CRC32C
  before decoding it. Trailer parsing is shared with the full-body reader
  in `machine.rs`.
- `Bucket::get_tail` reads an object's last bytes and its size in one GET.
- In the broker, `crates/broker/src/mirror.rs` opens a link's bucket with
  the `CAMUS_MIRROR_<ALIAS>_ACCESS_KEY` and `_SECRET_KEY` keys from the
  broker's environment. A missing key is an error that names the
  variable. `SourceSnapshot::open` takes two or three GETs:
  1. `CURRENT`
  2. the snapshot's last 64 KiB
  3. the index, only when the index and trailer together are over 64 KiB,
     which is a source of roughly 70 MiB or more

  After that, `rows` reads each key range with one ranged GET. A source
  whose snapshot is not `CAMUSKV3` is refused.
- Every read from a source is bounded by the bytes received, not only by
  the size the server claims:
  - `CURRENT` is at most 1 KiB, the tail 64 KiB, and the index 4 MiB with
    at most 131,072 entries, checked before decoding.
  - One `rows` run is at most 8 MiB.
  - `get_tail` and `get_exact` stop reading once a response passes the
    length asked for.
- A failed source read returns 502. The response body gives only the
  kind of failure, from `camus_storage::public_reason`, because an S3
  error body can echo the access key id and the signed string. The full
  error goes to the broker log.
- `link add` now opens the source before it commits. It returns 400 when
  this broker lacks the keys, and 502 when the source cannot be read,
  including with a wrong key.
- `camus admin mirror create --alias --topic`, or
  `POST /admin/mirror/topics`, reads the source's `topic/{name}` row and
  commits `CreateMirrorTopic`. The mirror takes the same name, the
  source's partition count and topic id in `mirror_of`, and a new id of
  its own. It returns 404 for an unknown link or source topic, and 409
  when the topic already exists here.
- `crates/broker/tests/mirror.rs` has a MinIO test,
  `a_mirror_is_created_from_the_source_snapshot`. It runs two brokers,
  each under its own key prefix in one bucket, and checks three things:
  that a link with a wrong secret is refused, that the link is added once
  the source has a snapshot, and that the mirror's row names the source
  topic's id.

Phase 1, third part: the copier.

- `run_copier` in `crates/broker/src/mirror.rs` runs on every broker but
  copies only while that broker leads metadata. It runs a round every
  `CAMUS_MIRROR_COPY_MS` (5 s, at least 50 ms), opening each link's source
  once per round. For each mirror partition it:
  1. reads the source's `part/` row
  2. finds the source extent that ends past the mirror's next offset,
     reading the source's extent keys a block at a time
  3. ranged-reads at most 8 MiB of that extent's batches, the largest
     batch Camus accepts. The next window of the same extent reads on from
     where the last one stopped.
  4. checks every batch's CRC with `batch::inspect`
  5. writes the whole batches into a new `bundle/{ulid}.seg` here, with
     the uploader's footer, so recovery from footers (ADR 0008) sees
     mirrors as it sees anything else
  6. commits them with `PublishMirror`. The object is put before the
     commit, as in ADR 0005.

  A partition copies at most eight windows per round. When a commit loses
  a race, the partition stops for that round. The object is left to the
  sweep.
- The planning step is the pure function `plan`, and it is
  unit-tested. It accepts a window only if every batch starts past the
  one before and ends inside the source extent, so the offsets published
  always match the bytes. Parts split wherever the leader epoch changes,
  so the mirror's epoch history matches the source's. When a window
  reaches the end of the source extent, its last part ends at the
  extent's end, not at the last batch. A source compaction can drop
  records at the tail of an extent, and this keeps those offsets
  accounted for. When every batch left in an extent is below the mirror's
  end, the copier commits a part with no bytes. That part moves the next
  offset to the extent's end and stores no extent.
- `PublishMirror` (log position 32) applies only to a mirror topic, and
  `Publish` never does. A part must start at the partition's next offset.
  A partition that holds nothing is the exception: its first part may
  start further on. In that case the copier starts at the source's log
  start, even when that falls inside a batch, as a Kafka log start can.
  So a new mirror never serves records the source has deleted. The same
  rule lets a mirror that its own retention has emptied go on from the
  source's log start, skipping only records the source no longer has.
  The copier logs that jump.
- A mirror's leader epoch is the larger of the metadata term and the
  newest source epoch it has copied. A consumer that fetched a copied
  batch can then ask `OffsetForLeaderEpoch` for that epoch. Answered with
  the term alone, a source that had failed over would look truncated to
  the consumer, and the consumer would reset.
- A partition stalls, without publishing, in five cases:
  - source retention has passed its end
  - the source's next extent starts past its end
  - the source has no such partition
  - its end falls inside a source batch
  - a batch does not fit its extent. Phase 1 also stalled on
    transactional batches; phase 2 replaced that with waiting, below.
- Metrics:
  - `camus_mirror_stalled_partitions` counts the partitions stalled in
    the last round.
  - `camus_mirror_copied_bytes_total` and `camus_mirror_copy_errors_total`
    count what was copied and what failed.

  Every series is a static atomic. Errors include a source that cannot be
  read and a failed put or commit.
- Memory is one 8 MiB window and the object built from it, one partition
  at a time. Arithmetic on a source extent's offsets is checked. Each
  round reads this cluster's topic rows to find the mirrors, which is
  bounded by the topic count, as a `Metadata` response is.
- Upgrade order: `PublishMirror` is a new command, so every metadata
  voter must run this release before a mirror is created. A voter on the
  part-one release cannot decode it.
- The acceptance test is `a_mirror_copies_the_source_and_outlives_it` in
  `crates/broker/tests/mirror.rs`, run with MinIO:
  1. A three-broker source produces 40 records, and a destination mirrors
     them.
  2. The mirror serves the same records at the same offsets.
  3. The source's WAL leader is killed, so the epoch rises past the
     destination's term, and 20 more records follow.
  4. A raw fetch of both logs is byte-identical.
  5. The mirror reports an epoch at least as high as the last copied
     batch's, and answers `OffsetForLeaderEpoch` for that epoch with 60.
  6. With every source broker gone, the mirror still serves all 60 and
     refuses produce.
- Not built yet: transaction spans, consumer group offsets, and promotion
  (phase 2).

Phase 1, fourth part: reset.

- `camus admin mirror reset --topic --partition --i-accept-data-loss`, or
  `POST /admin/mirror/reset`, commits `ResetMirrorPartition` (log position
  33).
- The reset moves the partition's log start up to its next offset, drops
  its extents, and prunes its transaction spans, so the partition holds
  nothing. It does not read the source. The copier's empty-partition rule
  then goes on from the later of the old end and the source's log start.
  For a stalled partition, that is the source's log start, past the gap.
  A partition that was not stalled only loses its history and goes on
  from where it was.
- The reset deletes every extent key of the partition in one apply, as
  `DeleteRecords` to the high watermark would. The partition's own
  retention bounds how many there are.
- It is refused without the flag (400), on a plain topic (400), and for
  an unknown topic or partition (404).
- The MinIO test `a_stalled_mirror_starts_again_after_a_reset` covers it:
  1. A mirror copies 40 records, and copying pauses.
  2. The source takes 20 more and deletes the records below 50.
  3. The mirror reports one stalled partition and stays at 40.
  4. After the reset, the mirror starts at 50, catches up to 60, and
     serves `record-50` first.

Phase 1 is complete. A Camus cluster can mirror another Camus cluster's
non-transactional topics with the same offsets and bytes, and keeps them
after the source is gone.

Phase 2, first part: transactions.

- The copier copies a transactional batch only once the source's snapshot
  holds a committed or aborted span over it, matched by offset and
  producer id. At the first transaction the source has not ended, the
  window stops just before it.
  - When that transaction is the window's first batch, the partition
    waits. Waiting is not a stall, so it does not count in
    `camus_mirror_stalled_partitions`.
  - A later snapshot that shows the span lets the copy go on.
  - Non-transactional records after an open transaction wait with it, as
    they wait behind the last stable offset for a `read_committed`
    consumer.
- `PublishMirror` now carries the source's committed and aborted spans
  that overlap the published offsets. Applying it adds any the partition
  does not already hold. A mirror's `read_committed` fetch then reads as
  the source's (ADR 0021): ABORT markers in place of aborted batches, and
  committed batches served. A `read_uncommitted` fetch sees everything
  copied. `PublishMirror` had not shipped, so its shape changed in place.
- The MinIO test `a_mirror_copies_ended_transactions_and_waits_at_an_open_one`
  covers this:
  1. A source writes an aborted transaction, a committed one, a plain
     record, and a transaction left open.
  2. The mirror stops at offset 3 and does not report a stall.
  3. `read_committed` gives the same records on the mirror as on the
     source.
  4. After the source commits, the mirror reaches 4.

  With the spans left out, `read_committed` on the mirror returns
  nothing, and the test fails.
- Spans travel in `PublishMirror` as `CopiedSpan`, which always writes
  `producer_id`. `Span` leaves out a `None` producer id, as spans stored
  before ADR 0015 have, and the Raft log's postcard encoding cannot skip a
  field. A test round-trips the command with a `None` span. `TxnSpan` in
  `NoteTxn` carries the same attribute. It is a latent issue for spans
  without a producer, outside this change.
- A mirror's own retention now applies. Retention used to send its cuts
  as `DeleteRecords`, which a mirror refuses, so a mirror kept every
  extent and span forever. It now sends a mirror's cuts as `TrimMirror`
  (log position 34): the same cut and span pruning, accepted only for a
  mirror. A client's `DeleteRecords` on a mirror is still refused. The
  empty-partition jump also prunes spans that end below the new start.
- `camus_mirror_waiting_partitions` counts the partitions waiting at an
  open transaction, and the copier logs once when a partition starts
  waiting. While the source's snapshot key has not changed, a waiting
  partition is not read again. That memory is capped at 65,536
  partitions and cleared when it is full.
- While a transaction is open at the source, the mirror's high watermark,
  and so `read_uncommitted` and `ListOffsets` latest, stop at that
  transaction's first batch. At the source they go past it. A source that
  can no longer end the transaction, because its brokers are gone while
  its bucket is readable, holds the partition there indefinitely. A reset
  does not move it past the transaction. Promotion (phase 2) is the way
  out.
- The MinIO test checks that the source has sealed the open
  transaction's batch, that the mirror reports one waiting partition, and
  that the gauge clears after the commit. The acceptance test also sets
  `retention.bytes` on the mirror and waits for its log start to move.
  With mirror cuts sent as `DeleteRecords`, the log start never moves and
  the test fails.

Phase 2, second part: consumer group offsets.

- `camus admin mirror groups --alias --group …`, or
  `POST /admin/mirror/groups`, commits `SetMirrorGroups` (log position
  35). That stores the link's group list at `mirror-groups/{alias}`, at
  most 256 ids of 1 to 1024 bytes each, sorted and deduplicated. An empty
  list stops the sync. Removing the link removes the list.
- Each copy round, after a link's partitions, `sync_groups` handles each
  listed group:
  1. It skips the group if it has members here, because a group consuming
     the mirror owns its own offsets.
  2. It reads the group's committed offsets from the source snapshot in
     one ranged read of its `offset/{group}/` keys.
  3. For each offset on a source topic this link mirrors, it clamps the
     offset to the mirror partition's next offset.
  4. It writes the offset only when it moves the group forward, in one
     `CommitOffsets` with no generation.

  Mirror offsets equal source offsets, so nothing is translated, and the
  sync is as fine-grained as the source's commits. MirrorMaker 2 can be up
  to `offset.lag.max` behind. A commit with no generation is refused once
  the group's members are stored, so a consumer that has joined wins. A
  classic group still in its first JoinGroup round holds its members only
  in memory, and can take one synced commit first. Its members then fetch
  that offset, as they would one committed just before they joined.
- Each synced record is stamped with this broker's clock, not the
  source's, as MirrorMaker 2's commit through the target coordinator is.
  The mirror's offset retention then counts from the sync, so a row that
  is old at the source does not expire on arrival and get written again
  every round. A group here that has been empty longer than the retention
  still expires offsets that are synced into it. That matches Kafka 4.4.0,
  which counts an empty group's retention from when it became empty.
- A clamped offset names a record the mirror does not hold yet, so it is
  written with leader epoch -1. A failed-over consumer then has no epoch
  to validate against, and never reads the clamp as truncation. An
  unclamped offset keeps its epoch, which the mirror holds.
- A successful sync tells this broker's coordinator about the group, as
  OffsetCommit does, so `ListGroups` on the mirror shows it.
- Groups sync one at a time. An error in one is logged and counted, and
  the rest go on. A group's offsets are read a snapshot block at a time,
  up to 65,536 per round, so a group with offsets on many source topics
  does not overrun a single read. A refused commit is logged.
- `camus_mirror_group_offsets_synced_total` counts the offsets written.
- The MinIO test `a_mirror_syncs_a_groups_offsets_forward_only` covers
  the sync:
  1. A listed group's source commits of 25 and then 30 appear on the
     mirror.
  2. A consumer then commits 35 on the mirror, and the source's 32 does
     not pull it back.
  3. A source commit of 1000 is clamped to the mirror's end, 40.
  4. A group list for an unknown link returns 400.

  The test also checks two more things:
  - The synced group appears in `ListGroups` on the mirror. The
    coordinator there is loaded before the sync. Without the coordinator
    notice the group is missing, and the test fails.
  - The clamped offset carries epoch -1.

  The skip for a group with members rests on the metadata group's commit
  check, which has its own tests. This test does not join a member.

Phase 2, third part: promotion and the fork check.

- `camus admin mirror promote --topic --i-accept-data-loss`, or
  `POST /admin/mirror/promote`, commits `PromoteMirror` (log position 36).
  It clears `mirror_of` and stores `promoted`: the link, the source topic
  id, and each partition's next offset at that moment as `at`. The
  copier, retention's `TrimMirror`, reset, and group sync all key on
  `mirror_of`, so they leave the topic alone. Its own retention goes
  through `DeleteRecords`, and produce and the admin APIs work as on any
  `r2` topic. Without the flag the promote returns 400, on a topic that
  is not a mirror 400, and on an unknown topic 404. Records the source
  acked and had not sealed into a snapshot this cluster read are not
  here, which is why the flag is required.
- Promotion starts a new leader epoch at each partition's promotion
  offset, one past the newest copied epoch, as a Kafka leader change
  does. A promoted topic keeps the mirror's epoch rule: its epoch is the
  larger of the metadata term and the newest epoch in its history. `r2`
  produce stamps batches with that epoch, so the first record this
  cluster writes is in the new epoch. The new epoch matters to a consumer
  that fetched records the source acked past the promotion offset, and
  then fails over keeping its position. When it asks `OffsetForLeaderEpoch`
  for the copied epoch, the answer ends at the promotion offset, and the
  consumer truncates there. Without the new epoch it would skip what this
  cluster wrote at those offsets.
- A promote that is retried, for example after its response was lost,
  changes nothing and returns 200. A partition row missing at promotion
  rejects the command, rather than failing the apply.
- The group sync commits through `SyncMirrorOffsets` (log position 37). It
  applies as a commit with no generation, but only while every topic it
  names is still a mirror. A sync that was in flight when its topic was
  promoted is refused, so it cannot write a source offset over offsets
  this cluster has since produced.
- A copy that was in flight during a promotion is refused with 3, the
  topic left the mirror set. The copier stops that topic for the round
  without counting an error. One orphan object at most is left for the
  sweep.
- Produce checks `mirror_of` after leadership. A broker that has not yet
  applied a promotion sends the producer to the leader with a retriable
  error. It does not fail the producer with `INVALID_TOPIC_EXCEPTION`.
- `camus admin mirror fork-check --topic`, or `GET
  /admin/mirror/fork-check`, is the issue 151 check. It reads only. It
  reads the old source through the link, which must still exist, and
  reports each partition's `promoted_at` and the source's end. The source
  has forked a partition when its end is past `promoted_at`: it holds
  records at offsets this cluster has since assigned again.
  - The source's end comes from its newest snapshot, which the response
    names as `source_snapshot`. Records it acked and had not yet sealed
    into that snapshot are not seen, so `forked: false` means no fork yet.
  - A partition the source has no row for is reported as unknown
    (`forked: null`, `unknown: true`).
  - The CLI exits 1 on a fork or an unknown.
- Mirroring back into the old source is not built. That cluster's topic
  already exists, and a topic that exists cannot become a mirror, so
  failing back still goes through MirrorMaker 2, or through a new topic.
- The MinIO test `a_promoted_mirror_is_writable_and_a_fork_is_reported`
  covers this:
  1. A promote without the flag returns 400.
  2. After the promote, the topic takes a produce at offset 40 and still
     serves `record-39`. A retried promote returns 200. The record at 40
     is in a newer epoch than the copied record at 39, and
     `OffsetForLeaderEpoch` for the copied epoch ends at 40. Without the
     new epoch at promotion, both records share an epoch and the test
     fails.
  3. The old source writes on to 42, and the fork check reports
     `promoted_at` 40, `source_end` 42, and a fork, while the promoted
     topic stays at 41.
  4. The link can then be removed.

## Phase 3 (proposed 2026-11-25, accepted 2026-11-25)

A destination that mirrors several source regions can promote all its
mirrors at one cut `c` that is consistent across the sources. This
section replaces the clock sketch above. It is accepted, and is being
built in the steps under "Delivery".

### What a consistent cut means

Take two records `y` and `x`, possibly from different sources.

- If `y` was acknowledged before `x` was produced, in real time, then a
  cut that keeps `x` keeps `y`.
- A cut never keeps a record without keeping every record before it in
  the same partition.

A single source snapshot already gives this for that source's own
partitions. The rest of this section is about pairs across sources.

### Stamps are per batch, taken at append

Each batch gets a stamp `s` when its partition's leader appends it. That
is the WAL shard leader for `wal`, or the `r2` buffer on the metadata
leader. The stamp is `latest`, the upper edge of the host's clock
interval at that moment, raised if needed so stamps never decrease within
a partition.

Stamping at seal does not work. An acknowledgement comes at the append,
and sealing can follow it by seconds, or by as long as R2 is down.

### The clock interval

The kernel reports its error bound through `ntp_adjtime(2)`. That is
`maxerror`, which the kernel grows by the 500 ppm tolerance each second
between syncs. A sample's interval is `[now − maxerror, now + maxerror]`.

The call lives in one small `unsafe` function behind a safe wrapper, in
`camus-raft`. That crate already holds the workspace's only `unsafe`, for
`io_uring`. The broker and the other crates keep `#![forbid(unsafe_code)]`.
On a host where the call is missing, or reports `TIME_ERROR` or an
unsynchronised clock, a batch gets no stamp. Stamps are not taken on
macOS, so development hosts mirror as before and cannot be part of a
cut.

### Acknowledgements do not wait by default

Waiting for `earliest > s` before acknowledging would give strict
ordering, but it adds up to `2 × maxerror` to every produce. That is
milliseconds on a host synced to a nearby source, and far more on one
that is not.

The default does not wait. Strictly, `y` sorts before `x` whenever `y`'s
acknowledgement comes more than `2ε` before `x`'s append. `ε` is the
larger of the two hosts' `maxerror` at their stamps.

A topic config, `mirror.commit.wait` (default `false`), turns on the wait
for that topic's produces. It is a Camus config with no Kafka
equivalent. `docs/compatibility.md` lists it.

### Where stamps are kept

- **WAL entries.** An appended batch carries its stamp in a new WAL entry
  variant. Like the metadata commands, every WAL voter must run the
  release before any broker stamps.
- **Object footers.** Today a footer lists one entry per slice, and a
  slice is one partition's run of batches. Footer v2 adds a list under
  each slice of `(batch base offset, stamp)`, one pair per batch: 16
  bytes a batch. The reader takes v1, which has no stamps, and v2. This
  keeps per-batch data out of metadata. The mirror copier reads the source footer of each
  object it copies, and writes the stamps into its own footer.
- **Extents.** Each extent gains `s_min` and `s_max` in its JSON row. A
  promotion uses these to find the few extents it must open. They travel
  in a new `PublishStamped` command, because `Publish` is in released
  logs and its postcard shape cannot change.

### What a source guarantees: its frontier

The cut needs to know that a source has published every batch stamped
below some value.

- Each WAL shard leader, and the `r2` buffer, reports a frontier `f`
  about once a second through a `Frontier { shard, f }` command. `f` is
  the smallest stamp among batches that are acknowledged and not yet
  published, or the shard's `earliest` when there are none. A batch
  appended after the report is stamped at least `latest` of a later time,
  which is above `f`.
- A source snapshot's watermark `W` is the smallest frontier across its
  shards. Every batch stamped below `W` is in that snapshot's extents.
- A shard with an unstamped batch reports no frontier, so its source's
  `W` stops moving until stamps resume. The source counts this in
  `camus_mirror_unstamped_batches_total`.
- The reports cost one small metadata entry per shard each second, and
  the group commit packs them together.

### The destination's watermark and the cut

- For each source, the copier keeps `applied_w`: the `W` of the newest
  snapshot whose extents below `W` it has copied in full. It is exported
  as `camus_mirror_applied_watermark_us{alias}`, with a label per link,
  capped by the 64-link limit.
- `camus admin mirror promote --cut c --all` checks that `c < applied_w`
  for every source a mirror names, and refuses otherwise.
- It then truncates each mirror partition to the last batch stamped at
  or below `c`. It reads the footers of the extents whose `s_min ≤ c <
  s_max`, which is at most the newest few per partition.
- It commits one `TruncateMirror` per partition, which sets
  `next_offset` and shortens the last extent. Then it promotes each
  mirror as in phase 2.
- A partition holding an unstamped batch at or below the cut refuses the
  whole promotion. It cannot be ordered.

### Costs and limits

- No acknowledgement waits on a clock unless a topic sets
  `mirror.commit.wait`.
- Each WAL entry gains 8 bytes, and each footer gains 16 bytes per batch.
- Each shard writes one metadata entry per second for its frontier.
- A promotion makes some extra GETs to read footers.
- The `2ε` window bounds how close in time two records from different
  sources can be and still be ordered by the cut, unless their topics
  wait.
- A source whose clock loses sync holds every cut at its last frontier.
  The cut is not wrong; it falls further behind. The unstamped-batch
  metric shows why.

### Alternatives

- **Stamp at seal, as sketched above.** Set aside. An early `wal`
  acknowledgement can be stamped after a later produce in another region.
- **Always wait.** Set aside as the default for the latency above, and
  kept as the per-topic option.
- **Per-batch stamps in metadata.** Set aside. One row per batch grows
  metadata with the data. Footers hold them instead.
- **Hybrid logical clocks.** Set aside. An HLC orders events that send
  messages to each other. Sources here never talk to one another, so an
  HLC adds nothing over the physical bound.
- **No phase 3.** Each source's snapshot is already a consistent cut for
  that source. Phase 3 is only worth building for an operator who must
  fail over several regions to one moment.

### Delivery

1. The clock wrapper and per-batch stamps in WAL entries and the `r2`
   buffer, with the stamp counters. Nothing reads the stamps yet.
2. Footer v2, `PublishStamped`, and the frontier reports. The copier
   carries stamps through to its footers.
3. `applied_w`, `TruncateMirror`, and `promote --cut --all`. Acceptance
   test: two MinIO sources and one destination. The test stamps with an
   injected clock, a test-only fake that implements the same interval
   interface. Records in a known real-time order across the sources, cut
   between them, must keep exactly the earlier records.
4. `mirror.commit.wait`, with a test that a waiting topic's
   acknowledgement waits until `earliest` passes its stamp.

### As built

Step 1, reader half, 2026-11-25. Stamps can be taken, stored, and read,
but no broker takes them yet.

- `camus_raft::clock` holds `Interval`, a `Clock` trait, and `Host`, which
  reads `ntp_adjtime` on Linux with glibc. That covers the shipped image,
  which is Debian. `Host` gives no reading on other hosts, on
  `TIME_ERROR`, or when `STA_UNSYNC` is set. `Fixed` is for tests. The
  `unsafe` block is the one call, and its safety comment explains why it
  is sound.
- The WAL log gains `StampedAppends { appends, stamps }` at position 5,
  pinned by a test. It is `Appends` with one optional stamp per batch,
  and every path that handles `Appends` handles it the same way.
- Apply raises a stamp to the partition's last applied stamp, kept in
  `stamp_floor` in the shard's JSON state. Stamps therefore never go
  back within a partition, even across leaders whose clocks disagree.
- Each unsealed head's stamp is kept in a `STAMPS` table under the head's
  key, outside the head's postcard row.
- Snapshots gain the stamps and the floor at the end of the body. An older
  broker reads the new body as its own shape and ignores them, and a body
  written before stamps still installs.
- `Batch.stamp` carries the stamp to the uploader. Nothing writes it to an
  object yet; that is step 2.
- A shard stamps only after `Shard::stamp_with(clock)`. It takes one
  reading per submitted request, and writes `StampedAppends` only when a
  batch in the group has a stamp. A shard never given a clock writes the
  same entries as before.
- The broker does not call `stamp_with` in this release. Every WAL voter
  must read `StampedAppends` before any leader writes it. The writer half
  of step 1 calls `stamp_with(Host)` and adds the stamp to the `r2`
  buffer and the unstamped-batch counter.
- Tests:
  - The clock's stamp floor.
  - A shard with a scripted clock: a reading of 1100, then a clock that
    stepped back to 1000, then no reading. It stamps 1100, 1100, and
    none, and keeps those stamps across a restart.
  - Snapshots read both ways.
  - The variant's log position.

