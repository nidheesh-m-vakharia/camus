# ADR 0005: Durability modes and acks

Status: Proposed
Date: 2026-11-05

## Context

The brief requires two ack barriers, selectable per cluster and ideally per
topic, plus `acks=0` as an explicit skip. Kafka's own `acks` has three
values (`0`, `1`, `all`). Mapping them naively onto a Raft quorum makes
`acks=1` weaker than "a minority of WAL replicas can die without losing
acked data."

## Decision

`durability.mode` is `wal` or `r2`.

- Cluster default: `wal`.
- Topic override: `durability.mode` in the topic config, set at
  `CreateTopics` and later via `IncrementalAlterConfigs` (Phase 4). Until
  Phase 4, the cluster default is the only switch, and a topic-level value
  in `CreateTopics` configs is accepted and stored if it is `wal` or `r2`,
  rejected with `INVALID_CONFIG` otherwise. Changing it on a topic that
  already has data is refused until an ADR describes the migration. Phase 1
  only runs `r2`. Phase 2 turns on `wal`.

### What an ack waits for

| Client `acks` | `wal` | `r2` |
| --- | --- | --- |
| `0` | Leader attempts a local append and does not wait. No response body is required; Kafka clients in `acks=0` often do not read one. The batch may be lost. | Same. The buffer may be dropped on process death. |
| `1` | Wait for the WAL shard to commit and fsync the entry on a quorum of voters. | Wait for the R2 PUT to succeed and the metadata commit that publishes the offsets to be applied. |
| `all` (or `-1`) | Same barrier as `acks=1`. | Same barrier as `acks=1`. |

`acks=1` is intentionally stronger than Kafka. A leader-only fsync would
acknowledge data that a leader crash can lose before a follower has it.
That violates the minority-failure rule, so Camus does not offer it.

If a client sends any other `acks` value, respond with `INVALID_REQUEST`.

The quorum is a majority of the shard's current voters: 2 of 3, or 3 of 5.
A voter counts only after its `append` has fsynced. "In memory on a
follower" is not a commit.

### `wal` produce path

1. Partition leader (the shard leader) validates the topic, partition,
   batch CRC, and size.
2. It appends the raw batch to the shard Raft log and waits for commit.
3. On commit, it assigns offsets in log order (ADR 0009) and sends the
   produce response.
4. A later upload copies the committed bytes to R2 and only then truncates
   the WAL (ADR 0006, ADR 0007).

Target: p99 produce latency under 20ms for `acks=1`/`all` inside one
region, with local NVMe and intra-region Raft. This is a target, not a
claim. The Phase 2 benchmark measures it. The produce path does not wait
for R2.

### `r2` produce path

1. The assigned leader validates the batch and buffers it in memory.
2. The uploader flushes on the same time and size limits as `wal`
   (default 250ms or 8 MiB).
3. After the PUT returns success and the metadata Raft entry that publishes
   the extent is committed, the produce response is sent.
4. Process death before that point loses the buffer. Clients retry.
   Nothing was acknowledged.

Latency is expected in the hundreds of milliseconds, dominated by the R2
round trip and the flush interval. There is no local log to recover.

### Backpressure

If the WAL disk used by untruncated entries exceeds `wal.disk.limit`
(default 8 GiB per broker), or if the `r2` memory buffer exceeds
`r2.buffer.limit` (default 256 MiB), the broker still accepts the TCP
connection but holds the produce response and sets `throttle_time_ms` the
way Kafka does. It does not drop the batch, does not return a fake success,
and does not return `REQUEST_TIMED_OUT` until the client's timeout is
actually reached. Fetch can be throttled the same way if the read cache is
not the problem; disk pressure throttles produce first.

When R2 returns errors, the uploader retries with jitter. The WAL grows.
Throttling starts at the disk limit. It clears when a metadata-committed
upload truncates the log below the limit.

### Per-topic mode and a single cluster

A cluster may mix topic modes. `wal` topics use shards. `r2` topics use the
memory buffer. They share the metadata group and the uploader's R2 client.
A topic's mode is part of the metadata record so every broker agrees.

## Consequences

- Clients written to expect `acks=1` to be faster and weaker than `acks=all`
  will see `acks=1` take a quorum round trip in `wal` mode. Data they got
  an ack for survives a minority outage. This difference is recorded in
  `docs/compatibility.md`.
- `acks=0` producers can create gaps only if we assigned offsets before
  durability. We do not. `acks=0` batches that are lost leave no hole,
  because offsets are assigned at commit. A lost `acks=0` batch is simply
  absent. An `acks=0` batch that happens to commit is durable.
- Phase 1 can ship without a WAL and still honor this table for `r2`.

## Alternatives

- True Kafka `acks=1` (leader fsync only) behind a flag. Deferred. It can
  be added later as `durability.acks1=leader` without changing the default.
  Not in the first implementation, so the default cannot be mis-set.
- Waiting for R2 on every `wal` ack "to be safe." Rejected. That erases the
  reason `wal` exists and misses the latency target.
