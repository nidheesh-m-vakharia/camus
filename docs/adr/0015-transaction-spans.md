# ADR 0015: Transaction spans without a metadata write per produce

Status: Accepted
Date: 2026-11-09

## Context

Camus has no transaction control markers (`docs/compatibility.md`). A
`read_committed` fetch decides what to show from spans in the partition's
metadata: a transactional batch inside a `txn_committed` span is shown,
inside a `txn_aborted` span is skipped, and in neither it is still open, so
the fetch stops before it and that offset is the last stable offset.

Each span is one batch's exact `[start, end)`. After a transactional batch
commits in the WAL, the broker writes it to the transaction's metadata
record with `NoteTxn`, and `EndTxn` copies the spans to the partitions as
committed or aborted (`crates/metadata/src/txn.rs`). So every transactional
produce pays a second Raft round trip, to the metadata group, after its WAL
ack: per partition, and on a broker that does not lead metadata, over HTTP
(#112). Spans carry no producer id, which is why they must be exact: two
transactions open on one partition at once interleave their batches, and a
wider span would claim the other's.

## Options

1. **Keep a span per batch, but write it in the WAL.** The WAL state
   machine already reads each batch's producer id, epoch and transactional
   flag at apply. It could keep open spans per producer and partition, and
   `EndTxn` would collect them from each partition's shard leader before
   committing. This moves the second write into the first, but `EndTxn`
   becomes a call to every shard holding the transaction's partitions, and a
   span lives in two groups until `EndTxn` lands in both: a crash in between
   needs reconciling on recovery.
2. **Give spans a producer id, and write one per partition per transaction.**
   With a producer id on the span, a span may be wider than the batches it
   covers: a fetch matches a transactional batch to a span by offset and by
   the batch's producer id, so another transaction's batches inside the range
   are not claimed. Then the start is recorded once, by the first produce of
   the transaction on that partition (one `NoteTxn`, not one per batch), and
   `EndTxn` supplies the end: the partition's end offset when `EndTxn` is
   handled, which is past every batch the producer had acked. Spans written
   before this have no producer id and keep matching any producer.
3. **Take the start from `AddPartitionsToTxn`.** As option 2, but with the
   partition's sealed end at `AddPartitionsToTxn` as the start, so a
   transactional produce writes nothing to metadata at all. The sealed end is
   a lower bound on the first batch, which is enough with a producer id, but
   it can reach back over the same producer's previous transaction on that
   partition, and both spans would then claim those batches. The start would
   have to be raised to that span's end, which needs the previous end before
   `EndTxn` has it.

## Decision

Option 2.

- `Span` gains `producer_id` (absent on stored spans: any producer).
- The broker sends `NoteTxn` only for the first batch a transaction writes
  to a partition. It already knows from `accept_txn` whether this is the
  transaction's first write there.
- `EndTxn` asks each of the transaction's partitions for its end offset,
  from the shard leader, and stores each span as `[start, end)` with the
  producer id. A partition whose leader cannot be reached fails `EndTxn`
  with a retriable error, as a coordinator does when a partition is
  unavailable.
- A `read_committed` fetch matches a transactional batch to a span by offset
  and producer id.

A transaction then costs one metadata write per partition it touches, plus
`EndTxn`, instead of one per batch.

## Consequences

- `EndTxn` becomes a fan-out to shard leaders. It is a client-initiated,
  infrequent request, and the shard leaders are already known from Metadata.
- Spans are fewer but wider; the producer id keeps them precise.
- Mixed-version brokers: an older broker reading a span with a producer id
  ignores the field and matches any producer, which is wrong only when two
  transactions are open on one partition at once during the upgrade.

## Not decided here

- Whether `TxnOffsetCommit` and `AddOffsetsToTxn` should batch into
  `EndTxn` as well.
