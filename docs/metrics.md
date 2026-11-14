# Metrics

`GET /metrics` on each broker's HTTP address serves Prometheus text. Every
series is listed here with what it costs in broker memory. The Grafana
dashboard in [`deploy/grafana/camus.json`](../deploy/grafana/camus.json)
plots consumer lag, upload lag, and an estimate of R2 spend from these
series.

Nothing here allocates per record. Counters and histograms are fixed
atomics in static memory; a sample is a few relaxed adds and never takes a
lock. The per-scrape cost is the response text, which is bounded by the
caps below.

## Cardinality and memory

| Family | Labels | Series per broker | Memory |
| --- | --- | --- | --- |
| Process, Raft, sweep, snapshot | none | 11 | existing counters; Raft values are read at scrape |
| `camus_object_store_*` | `op` (5 values) | 13 | 13 atomics |
| `camus_wal_*` gauges | `shard` | 4 per open shard | read from the shard at scrape; the oldest unsealed age adds 8 bytes to each unsealed batch's in-memory index entry, which `CAMUS_WAL_DISK_LIMIT` already bounds |
| `camus_wal_upload_seconds`, `camus_wal_seal_seconds` | `shard`, `le` | 17 per open shard each | 256 shard slots plus one `overflow` slot, 66,048 bytes static for all histograms |
| `camus_produce_request_seconds`, `camus_fetch_request_seconds` | `le` | 17 each | included above |
| `camus_consumer_group_*` | `group`, `topic`, `partition` | at most 2,000 `(group, partition)` pairs, 3 series each, plus 1 | none kept; built per scrape from at most 100,000 committed-offset rows |

Shard ids at or past 256 share the `shard="overflow"` histogram series.
Shard counts are fixed at `init` and follow the core count, so this cap is
not expected to bind.

The lag cap is `LAG_SERIES` (2,000 pairs) and the scan cap is
`LAG_SCAN_ROWS` (100,000 rows), both in `crates/broker/src/metrics.rs`.
Past the pair cap, `camus_consumer_group_lag_series_dropped` counts what
was left out. A scrape at the caps writes roughly 2,000 × 3 lines of about
120 bytes, under 1 MiB, and frees it when the response is sent.

## Consumer groups

Each `(group, partition)` is reported by the broker that leads the
partition: the WAL shard leader for a `wal` topic, the metadata leader for
an `r2` topic. Summing across brokers does not double count.

| Series | Type | Meaning |
| --- | --- | --- |
| `camus_consumer_group_committed_offset{group,topic,partition}` | gauge | Next offset the group will read. |
| `camus_consumer_group_log_end_offset{group,topic,partition}` | gauge | The partition's high watermark: the sealed end, or the WAL end for a `wal` topic when that is further. |
| `camus_consumer_group_lag{group,topic,partition}` | gauge | Log end minus committed, at least 0. |
| `camus_consumer_group_lag_series_dropped` | gauge | Pairs this broker leads that were left out past the 2,000 cap. |

Offsets of a deleted topic are not reported. A group with no committed
offset for a partition has no series for it.

## WAL shards and uploads

| Series | Type | Meaning |
| --- | --- | --- |
| `camus_wal_unsealed_bytes{shard}` | gauge | Committed WAL bytes not yet sealed into object storage. `CAMUS_WAL_DISK_LIMIT` bounds this. |
| `camus_wal_oldest_unsealed_age_seconds{shard}` | gauge | Seconds since this broker applied the oldest batch not yet sealed, or 0 when nothing is unsealed. After a restart it counts from when the shard opened, so it can read low for one upload interval. |
| `camus_wal_leader{shard}` | gauge | 1 when this broker leads the shard. |
| `camus_wal_storage_failed{shard}` | gauge | 1 when a WAL write failed and the shard stopped. |
| `camus_wal_upload_seconds{shard}` | histogram | One upload pass: putting its objects and publishing their extents to metadata. Recorded on the shard leader for passes that succeeded. |
| `camus_wal_seal_seconds{shard}` | histogram | Committing the `Seal` that drops uploaded batches from the WAL. Recorded on the shard leader. |

Upload lag is `camus_wal_unsealed_bytes` and
`camus_wal_oldest_unsealed_age_seconds` on the shard leader. Followers
report the same bytes, so use `max by (shard)` rather than `sum`.

## Object storage

Counted in `crates/storage` per call through `Bucket`. `object_store` may
retry a call internally; that counts once.

| Series | Type | Meaning |
| --- | --- | --- |
| `camus_object_store_requests_total{op}` | counter | Calls by `op`: `put`, `get`, `head`, `list`, `delete`. A list counts once however many pages it reads. |
| `camus_object_store_errors_total{op}` | counter | Calls that returned an error. A conditional put that lost is an error; a head that finds nothing is not. |
| `camus_object_store_listed_keys_total` | counter | Keys returned by lists. The store reads 1,000 keys per page. |
| `camus_object_store_put_bytes_total` | counter | Bytes written. |
| `camus_object_store_get_bytes_total` | counter | Bytes read. |

R2 bills Class A for puts and list pages and Class B for gets and heads;
deletes are free ([ADR 0007](adr/0007-r2-layout-and-cost.md)). The list
page count is about `lists + listed_keys / 1000`. The dashboard's estimate:

```
class_a_per_s = rate(requests_total{op="put"}) + rate(requests_total{op="list"})
              + rate(listed_keys_total) / 1000
class_b_per_s = rate(requests_total{op=~"get|head"})
usd_per_month = (class_a_per_s * 4.50 + class_b_per_s * 0.36) / 1e6 * 2592000
```

That is request spend before the free tier; storage is
`$0.015` per GB-month of bucket size, which `/metrics` does not measure.
`scripts/cost.py` models the whole bill.

## Requests

| Series | Type | Meaning |
| --- | --- | --- |
| `camus_produce_request_seconds` | histogram | From reading a Produce request to having its response: the WAL majority fsync, or the `r2` put and metadata commit. |
| `camus_fetch_request_seconds` | histogram | Time to answer a Fetch, including its `max.wait.ms` long poll, so an idle consumer's fetches sit near its wait. |

Histogram buckets are 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500 ms, and
1, 2.5, 5, 10 s.

## Process, Raft, and maintenance

| Series | Type | Meaning |
| --- | --- | --- |
| `camus_up` | gauge | 1 while the process serves. |
| `camus_raft_term` | gauge | Current metadata Raft term. |
| `camus_raft_leader` | gauge | 1 when this broker leads metadata. |
| `camus_raft_current_leader` | gauge | Node id of the metadata leader, or NaN. |
| `camus_retention_cuts_total` | counter | Partitions whose log start retention moved. |
| `camus_sweeps_total` | counter | Completed orphan object sweeps. |
| `camus_sweep_objects_deleted_total` | counter | Objects the sweep deleted. |
| `camus_sweep_bytes_deleted_total` | counter | Bytes in those objects. |
| `camus_produce_hold_ms_total` | counter | Milliseconds produce waited at the WAL disk limit. |
| `camus_meta_snapshot_index` | gauge | Log index of the newest metadata snapshot in object storage. |
| `camus_meta_snapshot_unix_ms` | gauge | Unix milliseconds of that snapshot, or 0. |
