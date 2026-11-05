# ADR 0009: Offsets and record batches

Status: Proposed
Date: 2026-11-05

## Context

Clients expect partition offsets to be monotonic, gap-free, and stable once
a produce response has named them. Order within a partition is the order of
successful appends. Kafka record batches also carry a CRC over most of the
batch, and compression (gzip, snappy, lz4, zstd) applies to the record
array inside that CRC. The brief says to keep batches byte-for-byte. Kafka
itself rewrites two fields that sit outside the CRC.

v2 batch layout (magic = 2), as implemented by `DefaultRecordBatch` in the
pinned tree:

| Field | Width | Inside CRC32C |
| --- | --- | --- |
| baseOffset | 8 | no |
| batchLength | 4 | no |
| partitionLeaderEpoch | 4 | no |
| magic | 1 | no |
| crc | 4 | no; covers the rest |
| attributes … records | rest | yes |

The broker can set `baseOffset` and `partitionLeaderEpoch` without
recomputing the CRC and without touching compressed records, headers, or
control-batch markers.

## Decision

### Assignment

Offsets are signed int64, per partition, starting at 0.

A batch that commits receives `baseOffset = next_offset` and consumes
`recordsCount` offsets, including control records and transaction markers.
`next_offset` advances by `recordsCount`. The next batch starts there.
Clients observe no gaps and no reuse.

Assignment happens at Raft commit for `wal`, and at metadata publish for
`r2`, in the order batches were accepted by the leader (ADR 0006). Two
batches for the same partition cannot pass each other on one leader.

`partitionLeaderEpoch` is a monotonic int32 stored in metadata for that
partition. It increments when the shard leader changes (the Raft term is
not used directly, because a partition that moves shards must still see an
increasing epoch). The epoch is written into the batch header next to the
base offset.

### What is preserved

All CRC-covered bytes are copied from the client buffer. Camus does not
decompress, recompress, or rewrite record headers on the produce path.
gzip, snappy, lz4, and zstd therefore pass through. Fetch returns those
same bytes. CRC mismatches on ingest return `CORRUPT_MESSAGE` (2) and
consume no offset.

The "byte-for-byte" rule in the brief is implemented as "byte-for-byte
inside the CRC." Rewriting `baseOffset` and `partitionLeaderEpoch` is
required for Kafka clients to see the offsets the produce response names.
Leaving the client's base offset (often 0) in place would make fetch
disagree with the produce response.

### Validation before durability

In the same order Kafka's produce path uses, as checked against
`KafkaApis` when Phase 1 is implemented (update this sentence with the
method name once read at the pin):

- unknown topic or partition → `UNKNOWN_TOPIC_OR_PARTITION` (3)
- batch larger than `max.message.bytes` → `MESSAGE_TOO_LARGE` (10)
- bad magic or truncated batch → `CORRUPT_MESSAGE` (2)
- CRC failure → `CORRUPT_MESSAGE` (2)
- unsupported compression codec in the attributes nibble is still accepted
  if the CRC is valid, because the payload is opaque. A codec we cannot
  name is the client's problem at decompress time. We do not reject zstd
  or any of the four standard codecs.

Validation failure does not append and does not advance `next_offset`.

### Idempotence

Phase 1 and Phase 2 do not deduplicate. A retried produce can land twice,
as two offset ranges. `InitProducerId` returns `UNSUPPORTED_VERSION` until
Phase 4. Document this in `compatibility.md` so nobody expects
exactly-once from the MVP. Phase 4 stores `(producer_id, epoch, sequence)`
in metadata and, on a duplicate sequence, returns the original offset
without appending. That logic is ported from `ProducerStateManager` at the
pin, with attribution.

### Transactions

Out of scope until Phase 5. Control batches are stored opaquely if a client
sends them, but transactional produces require `InitProducerId` and are
rejected with `INVALID_TXN_STATE` (48) until Phase 5 implements them.
Rejecting is safer than accepting a transactional batch we would then
expose to `read_uncommitted` consumers as committed data.

### ListOffsets

Phase 1 implements the offset-for-earliest, offset-for-latest, and
offset-for-timestamp queries that the Java client uses to start consuming.

- earliest = log start offset (0 until retention moves it)
- latest = high watermark (the next offset to be assigned, i.e. the end of
  the acknowledged prefix)
- timestamp = lowest batch whose `maxTimestamp` is ≥ the query, using the
  footer index from ADR 0007, then the batch header if the footer is absent

Timestamps are the client's. Camus does not invent them.

### High watermark and log start

The high watermark is the committed, acknowledged end. Consumers do not
read past it. There is no leader-local end that is ahead of the high
watermark and visible to fetches. That is stricter than Kafka, where the
leader's log end can exceed the high watermark, and it follows from
assigning offsets only at commit.

Log start offset advances when retention or `DeleteRecords` removes a
prefix of whole objects (ADR 0010).

## Consequences

- Fetch and produce agree on offsets without a CRC rewrite.
- A test that hashes the CRC-covered slice before and after produce must
  see equal bytes. That test is a Phase 1 acceptance test for all four
  compression codecs.
- Phase 1 clients that enable `enable.idempotence=true` will fail at
  `InitProducerId` rather than silently duplicate. That is the right
  failure.

## Alternatives

- Decoding every batch into records and re-encoding. Rejected. It breaks
  unknown compression and changes CRCs.
- Preserving the client base offset and translating in the fetch response
  only. Rejected. Downstream tools and the produce response both use the
  batch's base offset. Kafka writes it into the stored batch.
