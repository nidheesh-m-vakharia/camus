---
name: kafka-api
description: Add a Kafka API, widen its version range, or change how one behaves. Covers the protocol crate, broker dispatch, ApiVersions, the compatibility matrix, and client interop tests.
---

# Kafka API work

Kafka 4.4.0 is the reference, at the commit pinned in
`docs/compatibility.md`. If Kafka's behaviour is unclear, read its source at
that tag (`core/`, `clients/`, `server/`, `group-coordinator/`). Don't go by
KIP text.

## Steps

1. Types: `crates/protocol/src/messages/<name>_{request,response}.rs`
   already exist for every 4.4.0 message and are generated. Don't edit
   them.
2. Advertise: add the key and version range to `IMPLEMENTED` in
   `crates/protocol/src/frame.rs`. `ApiVersions` reads that table. A version
   outside the range gets `UNSUPPORTED_VERSION`.
3. Handle: add a `match` arm in `crates/broker/src/kafka.rs` (search for
   `ApiKey::Produce =>`). Put a large handler in a module under
   `crates/broker/src/kafka/` (`admin`, `groups`, `txn`, `acl`,
   `describe`, ...).
4. Authorize: check ACLs the way neighbouring handlers do (`caller`).
5. Persistent state goes through the metadata group as a new `Command`
   (`crates/metadata`). Read `crates/metadata/AGENTS.md` first.
6. Test: add a raw-protocol test in `crates/broker/tests/` using
   `support::{send, recv}`. Use a real client from `tests/java`,
   `tests/python`, or `tests/go` when client behaviour matters (see
   `testing`).
7. Document: update the API matrix row in `docs/compatibility.md`
   (versions, status, client columns). If behaviour differs from Kafka, add
   a row to "Deliberate differences" with the ADR that accepts it.

## Errors

Return the Kafka error code Kafka would return for the same state. A request
the broker can't parse closes the connection. Any other failure is answered
with an error code.
