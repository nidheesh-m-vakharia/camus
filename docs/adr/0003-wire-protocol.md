# ADR 0003: Wire protocol

Status: Accepted
Date: 2026-11-05

## Context

Kafka clients negotiate a version per API key, then encode a request that
may include flexible-version tagged fields. A broker that advertises a
version it does not implement, or that returns a successful empty response
for an unknown call, will corrupt client state in ways that look like
application bugs. The framing also has to survive malformed input without
taking down the connection task.

`kafka-protocol` 0.18.0 generates Rust types from Kafka's JSON schemas, but
those schemas are from Kafka **4.1.0**. The pin is 4.4.0 (ADR 0002).

## Decision

### Encoding

- Use the `kafka-protocol` generator (crate 0.18.0 as the code base) and
  regenerate messages from `vendor/kafka` at the pinned tag.
- If upstream cannot regenerate cleanly against 4.4.0, fork it under
  `crates/protocol/codegen` and keep the fork's diff small and documented.
  Do not hand-write request or response field layouts.
- The `protocol` crate re-exports only the types Camus uses, plus the frame
  codec. Handlers do not depend on the generator crate directly.
- Record batches on the data path are `bytes::Bytes`, not decoded records.
  ADR 0009 covers the two header fields the broker is allowed to rewrite.

### Framing

A request is `int32 size`, then the request header, then the body. Header
version follows Kafka's rule: request header v1 for non-flexible APIs,
v2 (tagged fields) for flexible APIs. Response header v0 or v1 matches.
The size counts everything after itself.

The connection task:

- Never panics on bad input. Library bugs that panic are still bugs; the
  accept loop isolates a connection so one panic cannot be the design.
- On a short read or a size that exceeds `socket.request.max.bytes`
  (default 100 MiB, matching Kafka's order of magnitude), close that
  connection. Do not close other connections.
- On a body that fails to decode, return `INVALID_REQUEST` (42) when a
  correlation id was recovered, otherwise close the connection.
- Never drop a connection merely because the API is unimplemented.

### ApiVersions

`ApiVersions` advertises only API keys whose handler is implemented and
tested. The advertised max version is the highest version that handler
implements, which may be lower than the schema crate knows about.

A key that is generated but not implemented is absent from the response.
Clients treat absence as "broker does not support this API", which is the
truth. This is stricter than a full Kafka broker, which advertises every
key it implements. It is the behavior the project brief requires.

`ApiVersions` itself is always advertised once Phase 1 starts. Phase 0 may
advertise it only in codec tests.

### Errors for everything else

| Situation | Error | Code |
| --- | --- | --- |
| API key known to Kafka, not implemented here | `UNSUPPORTED_VERSION` | 35 |
| API key not in the generated schema | `INVALID_REQUEST` | 42 |
| Version outside the implemented range | `UNSUPPORTED_VERSION` | 35 |
| Version inside the schema range but the handler rejects the body | the specific Kafka code for that case | |
| Authorization failure, once auth exists | `TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED` / `CLUSTER_AUTHORIZATION_FAILED` | 29 / 30 / 31 |

Error responses use a version the codec can encode. If the client asked for
a version we cannot encode, respond at the highest implemented version of
that API with `UNSUPPORTED_VERSION`, and only if that response header can
still carry the client's correlation id. If it cannot, close the connection.

No handler returns a zeroed success struct for an unsupported path. Adding
an API means updating `docs/compatibility.md` in the same change. Until
that row says implemented, the dispatcher returns the error above.

### Correlation and ordering

Responses carry the request's correlation id. Pipelining is supported:
decoding and handling may overlap, but the socket writes responses in the
order requests were read, matching Kafka. A slow produce does not let a
later metadata response overtake it on the same connection.

## Consequences

- The first protocol PR is a regeneration plus golden-byte tests captured
  from a real 4.4.0 broker (or from Kafka's own protocol tests when a live
  broker is unnecessary). It does not add Produce.
- Schema drift against crates.io `kafka-protocol` is expected. We do not
  track crates.io releases blindly.
- Flexible tags we do not understand are preserved by the generated types
  on decode/encode. Handlers must not rebuild messages field-by-field in a
  way that drops tags, unless the response is a new struct the schema
  defines.

## Alternatives

- Hand-written structs for the Phase 1 subset. Rejected. The brief forbids
  it, and tagged-field mistakes are silent.
- Advertising the full 4.4.0 version range and returning errors later.
  Rejected. Clients cache `ApiVersions` and will send versions we would
  then reject. Advertising the truth avoids that round trip for keys we
  will not implement in this phase, and matches the brief.
