# camus-raft

Segment-file Raft log shared by the metadata group and WAL shards.
ADR 0011.

- An append is visible to readers only after `fsync`. On Linux, write and
  fsync are one io_uring submission; elsewhere `pwrite` plus `sync_all`. Keep
  both paths equivalent.
- Group commit lives in `submit.rs`. Batching may delay an fsync but never
  skip one.
- Read the openraft storage trait docs before changing `store.rs`.
- No leader election and no Kafka in this crate.
