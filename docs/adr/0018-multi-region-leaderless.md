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
  - a batch does not fit its extent, or is transactional. Spans are
    copied in phase 2. Copying the batch now would show aborted records
    to `read_committed`.
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
