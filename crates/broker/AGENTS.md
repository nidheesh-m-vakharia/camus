# camus-broker

The binary. HTTP carries the metadata Raft group, the WAL shard (`/wal/`),
and admin routes. `--kafka-addr` serves Kafka.

- `src/kafka.rs` is the Kafka connection loop and API dispatch. Handlers
  for larger APIs are in `src/kafka/`. Read the `kafka-api` skill before
  you add one.
- `src/main.rs` reads flags and `CAMUS_*` env. Document a new variable in
  the README "Configuration" table, and in `docs/compatibility.md`
  "Defaults" if it maps to a Kafka setting.
- Integration tests start real processes through `tests/support/mod.rs`.
  See the `testing` skill.
