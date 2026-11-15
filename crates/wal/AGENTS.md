# camus-wal

One WAL shard: a Raft group that acks produce before the R2 upload.
ADRs 0005, 0006, and 0014.

- Produce is acked only after a majority of voters has fsynced the batch.
  The upload to R2 happens after the ack. `Seal` drops a batch only after
  metadata has published its extent.
- `CAMUS_WAL_DISK_LIMIT` bounds unsealed bytes. When it is reached, produce
  waits. Data is never dropped.
- Shards pass leadership as a whole. A partition never moves to another
  shard.
- Broker tests: `shards`, `backpressure`, `epochs`, `cluster`.
