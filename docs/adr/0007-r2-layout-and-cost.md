# ADR 0007: R2 object layout and cost

Status: Accepted
Date: 2026-11-05

## Context

R2 is the long-term copy of every acknowledged batch, except `acks=0`
batches that never reached the durability barrier. The layout has to match
how R2 actually behaves, checked against Cloudflare's docs on 2026-11-05
(limits page dated 2026-10-30).

Facts that constrain the layout:

- Strong read-after-write consistency for put, metadata update, delete, and
  list. Last completed writer wins if two clients put the same key.
- One concurrent write to the same key per second. Faster writes get HTTP
  429. Footnote on the limits page: "Concurrent writes to the same object
  name (key) at a higher rate return HTTP 429."
- Single-part upload up to 5 GiB (the same page's footnote 4 says the max
  upload size is 5 MiB under 5 GiB, so 4.995 GiB). Multipart up to 10,000
  parts, object size up to about 4.995 TiB. Camus objects are far smaller.
- Object key length ≤ 1024 bytes. User metadata ≤ 8192 bytes.
- Class A (put, list, multipart steps, copy): $4.50 per million.
  Class B (get, head): $0.36 per million.
  `DeleteObject` is free. Storage is $0.015 per GB-month.
  Egress to the internet is free. Infrequent Access is out of scope.
- Bucket management calls are limited to 50 per second per bucket. Object
  reads and writes are not under that cap. Do not use the `r2.dev` public
  URL. Use the S3 endpoint `https://<accountid>.r2.cloudflarestorage.com`.
- The Cloudflare REST API (1,200 requests per five minutes) is not the data
  path. The S3 API is.
- Conditional put with `If-Match` / `If-None-Match` works on R2.
  `object_store`'s `S3ConditionalPut::ETagMatch` targets R2 and MinIO.

MinIO in docker-compose is the dev stand-in. Tests that depend on R2-only
behavior (the 429-per-key rule, exact error XML) are marked and run only
against R2. Consistency and conditional put are tested on both.

## Decision

### Keys are write-once

Data keys are never overwritten. The uploader does not issue a second put
to a key it already used. That stays clear of the 1 write/second/key limit
and of last-writer-wins.

```
bundle/{ulid}.seg
```

One flush is one object, covering every partition that received bytes in
that window. The object footer lists `(topic_id, partition, start, end,
file_offset, length)` and metadata extents point at those byte ranges.
Fetch uses a ranged GET of one slice, so a consumer does not download other
partitions' batches.

`topic_id` is the Kafka topic UUID, not the name, so a deleted and
recreated name does not collide. `end` is exclusive. The ULID makes two
attempted uploads distinct. After metadata commits one of them, the other
is garbage.

This is the ingest layout from Phase 1, not a later optimization. Class A
puts are the expensive R2 operation ($4.50 per million, against $0.36 for
Class B). A per-partition object on the 250ms timer would multiply puts by
the number of active partitions. Bundling keeps puts at four per second
until the 8 MiB cap, regardless of partition count. The compactor (ADR
0010) later rewrites cold bundles into per-partition 128 MiB segments so
historical reads stay one GET per segment.

```
seg/{topic_id}/p{partition:08}/o{start_offset:020}-{end_offset:020}/{ulid}.seg
```

Those segment keys are written by the compactor only. They are also
write-once.

The metadata pointer `meta/snap/CURRENT` is the only hot key, and ADR 0004
caps it at one put per second. No other key is updated in place.

### Object body

The body is a sequence of Kafka record batches at their sealed base
offsets, concatenated, plus a small Camus footer the fetch path strips:

- magic, version
- batch count
- for each batch: file offset, length, base offset, last offset exclusive,
  base timestamp, max timestamp
- CRC32C of the footer

Fetch returns the batch bytes only. The footer exists so a range GET can
be planned from the object itself when the metadata extent is missing
(recovery, ADR 0008). Timestamps in the footer support `ListOffsets` by
timestamp without decoding compressed records. They are copied from the
batch header, which is outside the compressed payload.

Puts under 8 MiB use a single `PutObject`. The flush cap is 8 MiB, under
the single-part limit, so the produce path does not need multipart.
Multipart is reserved for the compactor's larger segments (target 128 MiB).

### Metadata commit is the publish

An object is invisible to fetch until the metadata group commits an extent
covering it. A put that succeeded and a metadata commit that did not is an
orphan. Orphans are not served. The orphan sweep deletes objects under
`bundle/` and `seg/` that no snapshot and no live extent references and
that are older than one hour.

Extent commit is a compare-and-append on `(topic, partition, sealed_end)`.
The new extent's start must equal `sealed_end`. This is what makes offsets
gap-free across uploaders (ADR 0009).

### Retention deletes

`DeleteObject` on whole keys, which R2 documents as free. `DeleteObjects`
(plural) is not in the free list as checked on 2026-11-05, so Phase 1 uses
single deletes. Revisit if a bulk delete is confirmed free.

### Cost model

Prices below are R2 Standard as published on the pricing page checked
2026-11-05. They are estimates for planning, not a bill.

Let `T` be produced MiB/s of record-batch bytes, sustained, one cluster.
Flush policy: 250ms or 8 MiB. Replication factor `RF = 3` in `wal` mode.

**Class A, bundled objects (chosen path).**
One put per flush for the broker, independent of partition count. At 1
MiB/s the timer binds: 4 puts/s, about **$0.018 per GiB** of ingest
(`4 * 1024` puts/GiB * $4.50 / 1e6). At 100 MiB/s the size cap binds:
12.5 puts/s, 0.125 puts per MiB, about **$0.0006 per GiB**. Storage is
$0.015 per GiB-month, so after the first month storage dominates except
on the low-rate bundled path, where ingest ops and storage are the same
order of magnitude.

**Class A, per-partition objects (rejected for ingest).**
A flush would write one object per active partition. With `P` partitions
receiving data in the flush window:

- Timer binds when a partition produces under 32 MiB/s (8 MiB / 0.25 s).
  Puts/s ≈ `P * 4` if every partition is active every 250ms, which
  overstates bursty traffic.
- Size binds for a partition at or above 32 MiB/s: puts per MiB = 1/8.

Worked point: **1 MiB/s total, 1 active partition.** 4 puts/s.
Class A ≈ 4 per MiB ≈ **$0.018 per GiB** of ingest
(`4 * 1024` puts/GiB * $4.50 / 1e6). Storage dominates after a short time:
1 GiB-month ≈ $0.015, and ingest ops for that GiB are about $0.018 once.

Worked point: **100 MiB/s, 1 partition.** Size binds. 12.5 puts/s.
Class A per MiB = 0.125. Per GiB ≈ **$0.0006**. Storage again dominates.

Worked point: **1 MiB/s spread over 1,000 partitions.** Per-partition
objects still produce 4,000 puts/s because the timer binds on each
partition. That is **$64.80 per hour** in Class A
(4000 * 3600 * $4.50 / 1e6). The bundled path stays at 4 puts/s for the
same bytes. Empty partitions are not flushed either way.

Metadata commits are Raft, not R2, except snapshots: one extra put each
30s, negligible.

**Class B.** Tail reads are served from the WAL or the `r2` leader's
memory and cost no R2 ops. A consumer reading sealed data issues one ranged
GET per object. At the 1 MiB/s, 250ms layout that is 4 GETs per MiB,
≈ **$0.0015 per GiB** (`4 * 1024 * $0.36 / 1e6`). After compaction to
128 MiB segments, one GET per 128 MiB ≈ **$0.000003 per GiB**. Read-ahead
does not change the count. Cache hits are free. List calls during recovery
are Class A; they are not on the produce path.

**WAL disk and network, per MiB/s of produce, `wal` mode.**

- Disk write bandwidth ≈ `RF * T` MiB/s until truncation. Steady-state
  capacity is the unsealed window, not topic retention. At a 1s upload lag
  and `RF = 3`, disk footprint ≈ `3T` MiB. The cap is `wal.disk.limit`
  (default 8 GiB per broker), shared by the shards that broker stores.
- Leader egress to followers ≈ `(RF - 1) * T` MiB/s, plus client ingress
  `T`. This is ordinary VM network, not an R2 charge. Cross-AZ pricing, if
  the hoster bills it, is the main `wal` cost besides disk.
- `r2` mode writes no log disk. Network is client ingress plus `T` MiB/s
  out to R2. R2 ingress is free.

**Snapshot and compaction overhead.** One metadata snapshot put per 30s.
Compaction reads a segment (Class B) and writes a larger one (Class A),
then deletes the inputs (free). For data rewritten once, add roughly one
Class A and one Class B per final MiB, at the 128 MiB size: ~0.008 ops per
MiB. Small compared with a 250ms unbundled flush, large compared with a
size-bound bundled flush. Compaction is off until Phase 2 has something to
merge, and the policy is ADR 0010.

### Flush arithmetic, amended 2026-11-12

The Class A and Class B products above assume every stream flushes every
250 ms or at 8 MiB, whichever comes first. `wal` upload no longer does
that for a quiet shard. A full 8 MiB object still uploads immediately. A
shard with at least 4 MiB unsealed still uses `CAMUS_FLUSH_MS` (250 ms).
A smaller prefix waits until it reaches 8 MiB or `CAMUS_QUIET_FLUSH_MS`
(30 s). `r2` mode still flushes on `CAMUS_FLUSH_MS`, because that
acknowledgement waits for the put. ADR 0006 records the uploader.

A stream stays on the 250 ms timer when its rate is at least half an
object per flush interval, 16 MiB/s at the defaults. Below that, puts per
second for one stream are `max(MiB/s / 8, 1/30)`.

- **1 MiB/s, one stream.** Quiet. 0.125 puts/s, one put per 8 MiB.
  Class A is `0.125 * 1024 * $4.50 / 1e6` = **$0.000576 per GiB**, the
  same product as the 100 MiB/s size-bound path, rounded to $0.0006.
  The **$0.018 per GiB** figure above is the old 4 puts/s timer applied
  to 1 MiB/s (`4 * 1024 * $4.50 / 1e6` = $0.018432).
- **20 MiB/s, one stream.** The 250 ms timer still binds at 4 puts/s.
- **100 MiB/s.** Unchanged. The 8 MiB cap binds at 12.5 puts/s.
- **1 MiB/s over 1,000 partitions on one stream.** Still one bundled
  object, 0.125 puts/s. The rejected per-partition path is one put per
  quiet partition per 30 s, about 33.3 puts/s, not 4,000. **$64.80 per
  hour** remains the cost of that rejected 250 ms per-partition timer
  (`4000 * 3600 * $4.50 / 1e6`).

Class B for the quiet 1 MiB/s stream is one GET per 8 MiB. The
**$0.0015 per GiB** figure is 4 GETs per MiB, which is the old timer
layout (`4 * 1024 * $0.36 / 1e6` = $0.00147456). `scripts/cost.py
--check` locks the current rates and those historical products.

## Consequences

- Recovery lists `bundle/` and `seg/` and reads footers. It does not assume
  one partition per object. ADR 0008 follows that.
- A bug that puts twice to the same key will surface as 429s, not as
  silent corruption. Treat 429 on a data key as a defect, not as backpressure.
- MinIO does not reproduce the 429 rule. A unit test can mock it. An
  integration test against real R2 is optional and not required for Phase 0.
- A single-partition topic pays the same Class A cost as a wide topic at
  the same total byte rate. That is the point of bundling.

## Alternatives

- One object per partition per flush. Rejected for ingest. At 1,000
  partitions producing every 250ms it is about 4,000 puts/s, $64.80
  per hour in Class A, for the same bytes one bundled put stream would
  write. Compaction still produces per-partition segments, where the extra
  put buys cheaper historical reads.
- Mutable segment keys (`.../segment-0`, rewritten). Rejected. Fights the
  per-key write limit and last-writer-wins.
- SQLite or a manifest object rewritten every flush. Rejected for the same
  reason. The extent list lives in the metadata Raft log.
- `aws-sdk-s3` instead of `object_store`. Rejected.
  `object_store` is Apache-2.0, speaks R2 conditional puts and ranged gets,
  and keeps the storage crate smaller. Revisit only if we need an S3
  feature it cannot express.
