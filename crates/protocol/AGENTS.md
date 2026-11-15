# camus-protocol

Kafka 4.4.0 wire types. ADRs 0002 and 0003.

- `src/messages/` is generated from the 4.4.0 JSON schemas (see `NOTICE`).
  Don't edit it by hand.
- `src/frame.rs` `IMPLEMENTED` is the source of truth for what `ApiVersions`
  advertises. Keep it equal to the matrix in `docs/compatibility.md`.
- Hand-written parts: `batch.rs`, `compression*`, `idempotence.rs`,
  `compact.rs`, `frame.rs`, `error.rs`.
- Tests: `tests/roundtrip.rs`.
