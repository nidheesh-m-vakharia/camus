# ADR 0021: Transaction markers are made at fetch, not stored

Status: Accepted
Date: 2026-11-14

## Context

Kafka writes a control batch after each transaction on every partition it
touched: one COMMIT or ABORT record, which takes an offset of its own.
Camus stores no markers. `read_committed` reads committed and aborted
offset ranges from metadata ([0015](0015-transaction-spans.md),
[0019](0019-txn-span-in-the-wal.md)) and the broker drops aborted batches
from the response.
[Issue 140](https://github.com/nidheesh-m-vakharia/camus/issues/140) asks
whether that difference breaks any tool.

These read transactional data from a broker:

| Reader | What it relies on |
| --- | --- |
| `kafka-clients` consumer, Kafka Connect sinks, Streams, MirrorMaker 2 source | position moves past aborted ranges and markers; `endOffsets` at `read_committed` is the last stable offset (LSO) |
| librdkafka (confluent-kafka-*, kcat) | same; it moves its position past an ABORT marker, not past a batch that has no records |
| KafkaJS | skips control batches |
| Lag tools (`kafka-consumer-groups`, exporters) | committed offset reaches the log end once a consumer has read everything |
| `kafka-dump-log.sh` | reads segment files on the broker's disk. Camus has none, so it does not apply |

An ecosystem test that commits and aborts on one partition, ending on an
abort, found three gaps. None of them was the missing offset:

1. **A consumer stopped short of an abort at the end of the log.** The fetch
   removed the aborted batch, so the response held nothing at those offsets.
   kafka-clients and librdkafka kept their position at the start of the
   abort, one short of the end. Lag stayed at 1. A reader that waits to
   reach `endOffsets`, such as a Streams restore or a bounded read, would
   never finish.
2. **ListOffsets ignored `isolation_level`.** `read_committed` got the high
   watermark, not the LSO. A consumer that reset to the end while a
   transaction was open started past that transaction's batches and never
   saw them after the commit. Kafka returns the LSO.
3. **Committed records could be hidden.** A seal reads a transaction's open
   span from the WAL and carries it in its own metadata commit. If `EndTxn`
   applied first, `NoteTxn` found the transaction ended and stored the
   range as aborted. Aborted spans are checked first, so the committed
   records disappeared from `read_committed`. Separately, in `r2` mode
   `EndTxn` ended the span at the transaction's first batch, so its later
   batches on that partition stayed open and stopped `read_committed`
   there.

## Decision

Camus does not store markers. Offsets stay one per record, and nothing is
added to the WAL, the objects, or `EndTxn`.

- A `read_committed` fetch puts an ABORT control batch where an aborted
  batch was. It has one record at the aborted batch's last offset, with
  that batch's producer, epoch, leader epoch, and timestamp, and no offset
  of its own (`camus_protocol::batch::abort_marker`). Clients skip it and
  move their position past the aborted range, as they do past Kafka's
  marker. `read_uncommitted` still gets the aborted batches, as in Kafka.
- ListOffsets for the latest offset at `read_committed` returns the LSO.
  That is the first offset of the earliest open span: on the WAL shard for
  `wal` topics, and in the open transaction's metadata record for `r2`
  topics and spans a seal stored. With nothing open, it is the high
  watermark. A timestamp lookup at `read_committed` keeps its offset only
  below the LSO and answers -1 past it, as Kafka 4.4.0's
  `Partition.fetchOffsetForTimestamp` does.
- `NoteTxn` for a range that `EndTxn` already committed or aborted for the
  same producer changes nothing. A batch past the committed end is still a
  late write and is aborted. The rule applies only to `NoteTxn` commands
  that carry `skip_published: true`, which this binary always sets. A
  command in the log from an older binary has no field, reads as `false`,
  and applies the old rule, so replaying the log gives the state the
  cluster had when it applied it.
- In `r2` mode, `EndTxn` ends each span at the partition's committed next
  offset, as [0015](0015-transaction-spans.md) describes. An `r2` produce is
  acked after its metadata commit, so that offset is past every batch the
  producer had acked.
- An empty fetch carries an empty record set, not null. librdkafka treats
  a null set as a parse failure and drops the whole response.

## Consequences

- Offsets differ from Kafka for transactional topics: Kafka's marker
  takes an offset after each transaction, and Camus has none. Offset-based
  counts on Camus equal record counts. A tool that hard-codes Kafka's
  offsets for a transactional log would see different numbers. No tool in
  the harness does this.
- A synthesized marker is under 80 bytes per aborted batch in a response.
  Nothing is cached or stored.
- `read_committed` ListOffsets reads the transaction records a page of 256
  at a time, at most 16,384 records per call (`TXN_SCAN_ROWS`). It is
  called when a consumer resets or looks up a timestamp, not per fetch.
  Past the cap the rest is not read: an open span stored only in metadata
  for a transactional id past the first 16,384 does not hold the offset
  back, and the broker logs a warning. `wal` topics also get the LSO from
  the shard, which has no cap.
- During a rolling upgrade, a voter still on the old binary ignores
  `skip_published` and applies the old rule to a late-sealed `NoteTxn`.
  It stores that committed range as aborted, and new voters do not. The
  difference does not heal: nothing later removes the abort range from the
  old voter's state, and it stays after that voter is upgraded and in any
  snapshot it builds, so if it becomes leader the range stays hidden from
  `read_committed`. It needs a seal to land after `EndTxn` on a committed
  transaction while voters run mixed binaries. To avoid it, upgrade while
  no transactional producer is writing.
- A COMMIT marker is not synthesized. A committed batch's own records move
  the position past it.

## Tests

- `crates/broker/tests/ecosystem.rs`
  `clients_read_a_transactional_topic_without_stored_markers`:
  kafka-clients (`TxnMarkers.java`), the console consumer, librdkafka
  through confluent-kafka-go, and KafkaJS read a partition that ends in an
  abort. Each reaches the end offset, and a consumer reset to the end while
  a transaction is open sees it after the commit. The same checks pass
  against Apache Kafka 4.4.0, apart from the offsets.
- `mirror_maker_copies_only_committed_records_from_camus`: MirrorMaker 2
  with a `read_committed` source consumer copies the committed records
  from one Camus cluster to another and none of the aborted ones.
- `crates/broker/tests/txn.rs`: a raw fetch shows the marker, and
  ListOffsets returns the LSO. `an_r2_transaction_of_several_batches_commits_them_all`
  needs MinIO.
- `crates/broker/tests/txn.rs`
  `read_committed_list_offsets_reads_past_finished_transactions`: 300
  finished transactional ids, more than a page, then ListOffsets answers.
  `a_read_committed_timestamp_lookup_stops_at_an_open_transaction` (MinIO):
  the open span is only in metadata past that page, the latest offset stops
  at it, and a timestamp lookup answers -1 until the commit.
- `camus-metadata` `a_late_seal_note_leaves_a_committed_range_visible`, and
  `an_old_note_keeps_the_old_rule` for a `NoteTxn` written without
  `skip_published`.

## Alternatives

- **Store markers in the log.** `EndTxn` already makes one WAL command per
  partition to close the span, and a marker could ride in it. Each
  transaction would then take one extra offset per partition, as in Kafka.
  That changes offsets on existing topics, adds a write to `r2` mode, and
  compaction would have to keep or drop markers by Kafka's rules. No
  reader in the harness needed the offset itself, only the position move,
  and a fetch-time marker gives that.
- **An empty batch (record count 0) in place of the abort.** Kafka's log
  cleaner leaves these, and kafka-clients moves past them. librdkafka 2.16
  does not, so the position still stopped one short.
- **Return the aborted batches with `aborted_transactions` and let the
  client filter.** Clients drop an aborted producer's batches until they
  see its ABORT marker. Without one they would also drop that producer's
  next transaction.
