# camus-metadata

Metadata Raft group: topics, offsets, groups, producers, transactions, and
ACLs, stored in a `redb` state machine (`machine.rs`). ADRs 0004, 0011,
and 0015.

- New persistent state is a new `Command` variant applied in `machine.rs`.
  Apply must be deterministic on every voter. Never decide state from a
  local clock or randomness; carry timestamps in the command.
- Snapshots are uploaded to object storage so a lost disk can be rebuilt.
  New state must be in the snapshot.
- Writes are group-committed. Linearizable reads share read rounds.
- Tests: `tests/group.rs` and `tests/group_commit.rs`.
  `tests/write_throughput.rs` is an ignored benchmark.
