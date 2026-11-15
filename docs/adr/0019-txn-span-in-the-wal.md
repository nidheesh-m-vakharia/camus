# ADR 0019: Transaction spans live in the WAL until EndTxn

Status: Accepted
Date: 2026-11-12

## Context

[0015](0015-transaction-spans.md) stopped a metadata write per batch. The
first batch a transaction writes to a partition still sends `NoteTxn` after
the WAL commit, and a broker that is not the metadata leader forwards it
([#112](https://github.com/nidheesh-m-vakharia/camus/issues/112)). That is a
second Raft round trip on the produce path.

`read_committed` shows a transactional batch inside a committed span, skips
one inside an aborted span, and stops before one that is in neither. An
open transaction therefore needs no metadata row: absence is what hides it.
The span has to exist, with its start, by the time `EndTxn` commits or
aborts it. Two transactions on one partition still match by producer id, so
the start must not reach back into the same producer's previous span.

## Decision

The WAL state machine records the open span when it applies a transactional
batch. The key is the partition and producer id. The start is the first
batch's offset; later batches move only the end. One shard holds at most
65,536 open spans. A new producer past that cap is refused, and the batch
is not appended.

Metadata learns the span at `EndTxn`, which asks each shard leader for the
open span and commits that range. The produce ack does not wait on
metadata. After `EndTxn` is stored, the shard closes the span so the next
transaction starts fresh. Closing is idempotent. A retry of `EndTxn` after
the metadata commit still closes the span and returns success.

A seal and an r2 produce already make one metadata commit. The span is
carried in that same commit, so a sealed object still has a span if the
WAL is later rebuilt, and an r2 produce does not add a second write.

A batch that applies after the transaction has already ended is marked
aborted, as `NoteTxn` does today, so `read_committed` can move past it.

## Consequences

- A successful transactional produce in `wal` mode is one WAL commit.
- `EndTxn` still fans out to the shard leaders, as it does to read the log
  end. The close is one WAL command per partition the transaction wrote.
- An aborted span is checked before a committed one, so a later span that
  overlaps an aborted range does not reveal those batches.
- Commands written before this keep working: `EndTxn` with no ends still
  publishes the spans `NoteTxn` stored.

## As built

Accepted 2026-11-12. The WAL records the open span on apply, `EndTxn`
finishes and reads it before the metadata commit, and seal and r2 produce
carry the span in the metadata commit they already make. A transactional
produce in `wal` mode is one WAL commit.

## Alternatives

- Keep 0015's one `NoteTxn` per partition. The produce path still pays a
  metadata round trip.
- Take the start from `AddPartitionsToTxn`. That write already exists, but
  the high watermark then can include another producer's batches, and the
  same producer's previous transaction if its end is not stored yet.
