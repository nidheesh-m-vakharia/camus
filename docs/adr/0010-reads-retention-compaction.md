# ADR 0010: Reads, retention, and compaction

Status: Accepted
Date: 2026-11-05

## Context

Sealed data is in R2. Tail data is in the WAL (`wal` mode) or in the leader's
memory until flush (`r2` mode). Consumers may read either, or a range that
crosses the boundary. Retention and compaction have to delete or rewrite
whole objects, because partial objects would cost a get plus a put to punch
a hole.

## Decision

### Fetch

`Fetch` (Phase 1) returns batches in offset order for each requested
partition, up to `max_bytes` / `fetch_max_bytes`, and waits up to
`max_wait_ms` for `min_bytes` the way Kafka does. The wait is served from
the tail, not by polling R2.

Source selection:

| Offset range | Source |
| --- | --- |
| Below log start | `OFFSET_OUT_OF_RANGE` |
| Sealed | Ranged GET of the extent, or the cache |
| Unsealed tail | WAL state machine, or the `r2` memory buffer on the leader |
| Spans the seal | Stitch sealed bytes then tail bytes |

Follower fetch of the tail in `wal` mode is served by any shard voter that
has committed that index. Follower fetch in `r2` mode is not served from a
peer's memory. Non-leaders either forward to the leader or return
`NOT_LEADER_OR_FOLLOWER`. Phase 1 (`r2` only) forwards sealed reads to local
R2 and forwards tail reads to the leader.

### Cache

Two levels, both optional and bounded:

- Memory LRU, default 256 MiB per broker, keyed by `(object_key, byte_range)`
  aligned to whole batches.
- Disk cache, default 4 GiB, same key, write-through on GET. The disk cache
  is a file per object key under the cache dir, not a second source of
  truth. It may be deleted at any time.

Read-ahead: if a fetch consumes the last batch of an object and the client's
fetch offset has been moving forward, GET the next extent before the next
fetch arrives. One read-ahead object per partition per connection. No
read-ahead for offset-rewinding consumers.

Cache misses on R2 are errors the fetch handler returns as
`KAFKA_STORAGE_ERROR` (56) after retries. They are not empty success.

### Compaction of objects

Not log compaction (`cleanup.policy=compact`), which is Phase 5. This job
only merges small objects into larger per-partition segments.

- Trigger: a partition has more than 8 objects under the target size, or a
  schedule (default hourly).
- Target object size: 128 MiB.
- The compactor writes a new object, commits replacement extents in one
  metadata entry (old ranges removed, new range inserted, same offsets),
  then deletes the old keys.
- Readers that still hold the old key may GET it until delete. Metadata
  commits are linearizable, so a fetch that starts after the commit uses
  the new key. A fetch that resolved the old key before the commit may
  still read it. Both contain the same batch bytes.
- The job runs on any broker. The metadata compare-and-swap on the extent
  list prevents two compactors from publishing conflicting replacements.

### Retention

`retention.ms` and `retention.bytes` are topic configs. Defaults match
Kafka's order of magnitude: 7 days, and -1 (no byte limit) until we confirm
the 4.4.0 default at implementation time and write it down.

Deletion removes a prefix of whole objects. An object is eligible only when
every batch in it is expired (time) or when dropping the whole object keeps
the partition under `retention.bytes`. The log start offset jumps to that
object's end offset. Data inside the newest object is kept even if some
records are old. Effective retention can therefore exceed `retention.ms` by
one object (up to the flush size, or 128 MiB after compaction).

`DeleteRecords` (Phase 4) uses the same prefix rule. A request to delete to
an offset in the middle of an object advances the log start only to the
next object boundary at or after the requested offset, and the response's
low watermark is the offset actually deleted. This is coarser than Kafka,
which can split a segment. The difference goes in `compatibility.md` when
the API is implemented. Until Phase 4 the API returns `UNSUPPORTED_VERSION`.

Empty topics and deleted topics: `DeleteTopics` (Phase 1) removes metadata
and enqueues object deletes. After the metadata commit, produce returns
`UNKNOWN_TOPIC_OR_PARTITION`. Object deletion is best-effort and retried.
Listings during that window must not resurrect the topic.

## Consequences

- Phase 1 fetch can be correct with the cache disabled. The cache is a
  latency feature, tested for equivalence: cached and uncached fetches
  return the same bytes.
- Retention tests should use objects smaller than the retention window so
  the whole-object rule does not hide a failure to delete.
- Log compaction (`cleanup.policy=compact`) is a different algorithm and is
  not this job. Setting that policy before Phase 5 returns `INVALID_CONFIG`.

## Alternatives

- Splitting objects on a retention boundary. Rejected for Phase 1–4. It
  doubles Class A traffic on the delete path. The operator-visible skew is
  bounded by object size.
- Always proxying tail reads through the leader even in `wal` mode.
  Rejected for the tail. Followers that have committed the entry already
  hold the bytes; using them keeps fetch available during a leader blip
  only for data they have. Produce still needs the new leader.
