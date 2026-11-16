# ADR 0025: Per-principal usage first, then Kafka client quotas

Status: Proposed
Date: 2026-11-16

## Context

Issue #178. `DescribeClientQuotas` (48) and `AlterClientQuotas` (49) are
unimplemented, and Camus has only one throttle: the produce hold. When
unsealed WAL bytes pass `wal.disk.limit`, a produce waits, `throttle_time_ms`
stays 0, and the wait is counted in `camus_produce_hold_ms_total`
(compatibility.md). That protects the broker, but it is not fair between
tenants. One producer can fill the WAL, and every producer on that broker
then waits.

A cluster shared by several teams, or run as a hosted service, needs two
things:

- **Attribution.** Bytes and requests per principal, for chargeback and
  for finding the noisy tenant.
- **Limits.** Per-principal byte rates, so one tenant cannot use up the
  headroom before the hold.

Every connection already has a principal: `User:<name>` from SASL, the
certificate subject on an mTLS listener, or `User:ANONYMOUS`. The ACL code
uses it.

## Decision

Build attribution first and ship it on its own. Build Kafka's quota APIs
second, with Kafka's semantics, once a shared cluster needs limits.

### Step 1: usage per principal

Each broker counts, per principal:
- produce bytes in;
- fetch bytes out;
- requests.

These are exported as:

```
camus_client_bytes_total{principal, direction="in"|"out"}
camus_client_requests_total{principal}
```

The counters live in a fixed table of `CLIENT_SLOTS` (1,024) entries. Each
entry holds a 64-byte principal name and three atomics, about 96 KiB per
broker in all. The first time a principal is seen, it takes a free slot.
Once the table is full, further principals share one `principal="other"`
row, and `camus_client_series_dropped` counts how many principals went
there. Slots are never reused while the process runs, so a counter never
goes backwards for a principal that is still connected. A restart empties
the table, and Prometheus handles that as an ordinary counter reset.

Chargeback is a Prometheus query summed over brokers. Camus stores no usage
history of its own: Prometheus already keeps the series, and a second
store in metadata would add writes on the produce path.

### Step 2: Kafka client quotas

`AlterClientQuotas` v0–v1 and `DescribeClientQuotas` v0–v1, with the
entity types and keys that Kafka 4.4.0 accepts:

- entity types `user` and `client-id`, each one either named or a
  default;
- quota keys `producer_byte_rate` and `consumer_byte_rate`.

`request_percentage` and `controller_mutation_rate` are rejected with
`INVALID_REQUEST` (42) and a message saying they are not supported.
Request time is not a meaningful limit when one tokio runtime serves every
connection, and topic mutations already go through one metadata leader.
Altering quotas needs cluster `ALTER_CONFIGS`, and describing them needs
cluster `DESCRIBE_CONFIGS`, as in Kafka.

**Where quotas live.** In metadata, under `quota/<entity>`, as JSON, and
replicated like topic configs. A quota is a few dozen bytes on disk, so
the store needs no cap of its own. Every broker loads the rows into an
in-memory map that is rebuilt on every quota write. The map is capped at
`QUOTA_ROWS` (10,000) entries, and `AlterClientQuotas` past that cap
answers `POLICY_VIOLATION` (44).

**Where counters live.** Each broker enforces its own share, as Kafka
does. A quota is a per-broker rate, and the operator divides a cluster
budget by the broker count. Each broker measures rates the way Kafka
does: `quota.window.num` (11) samples of `quota.window.size.seconds` (1)
each, per quota entity. Counters exist only for the `(user, client-id)`
entities that have a quota and are currently sending. They are held in a
table of `QUOTA_SLOTS` (4,096) entries of about 128 bytes each, 512 KiB in
total. When the table is full, the entry idle the longest is evicted.
Eviction resets that entity's window, which can only let it send more,
and only for one window. It never throttles anyone by mistake.

**How throttling works.** Camus follows KIP-219, as Kafka 4.4.0 does:
1. The request is handled in full.
2. The broker computes the delay needed to bring the entity back under its
   rate, and puts it in `throttle_time_ms`.
3. The broker stops reading from that connection for that long. Replies
   already queued are still written.

Clients from Kafka 2.0 on also wait out `throttle_time_ms` on their side,
so a client that obeys it is not delayed twice.

### How quotas and the produce hold combine

They stay separate:

- The hold protects the broker's disk. It applies to every producer and is
  still reported only in `camus_produce_hold_ms_total`, with
  `throttle_time_ms` 0 for its part.
- A quota is fairness between tenants. A request that was held is counted
  against its quota only by the bytes it wrote. Time spent in the hold does
  not lower the throttle it then gets.

So `throttle_time_ms` is always the quota delay and never the hold, and a
client that obeys it waits once for each cause.

With quotas set, the hold should rarely be reached. Quotas limit each
tenant's rate, and the hold only catches what all tenants send together
when R2 falls behind.

## Consequences

- Step 1 adds about 96 KiB per broker and one table lookup for each
  request, with no lock (the slot index is cached on the connection).
- Step 2 adds a metadata row type, two APIs, and per-connection muting.
  A throttled connection holds its socket and buffers while muted, which
  the connection limits already bound.
- Limits are per broker, as in Kafka. A tenant that spreads its traffic
  over more brokers gets a larger total rate, which matches Kafka and the
  tools built for it.
- `kafka-configs.sh --entity-type users|clients` works once step 2 ships.

## Alternatives

- **Usage metrics only, never quotas.** Not enough for a hosted service,
  where one tenant can fill the WAL for everyone.
- **Cluster-wide quotas enforced through metadata.** That puts a
  coordination round trip on the produce path. Rejected.
- **Report the produce hold as `throttle_time_ms`.** Rejected earlier,
  because a client that obeys it would wait twice. Keeping the two separate
  preserves that.
- **Usage history in metadata or R2.** This duplicates Prometheus, and it
  would need writes driven by the produce path. It could be revisited if
  billing has to run without Prometheus.
