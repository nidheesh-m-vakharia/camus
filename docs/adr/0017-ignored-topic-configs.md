# ADR 0017: Accepted topic configs that do not change behavior

Status: Accepted
Date: 2026-11-11

## Context

Admin tools and client libraries set Kafka topic configs on create, including
`min.insync.replicas`, `segment.*`, and `flush.*`. Rejecting those names
breaks `CreateTopics` for those tools. The mechanisms they tune are not how
Camus stores or acknowledges a log: there are no Kafka segments, no ISR, and
no dirty-ratio cleaner.

Silently storing them surprises an operator who set `min.insync.replicas=2`
and expects a produce to wait for two replicas. Every acknowledgement already
waits for a WAL majority, or, in `r2` mode, for the object put and the
metadata commit (ADR 0005). That barrier is stricter than a leader-local
write, and it does not read `min.insync.replicas`.

## Decision

Accept the configs listed in `docs/compatibility.md`, store the value, and
return it from `DescribeConfigs`. When the client sets
`include_documentation`, the documentation field says why the value is
ignored and which setting applies.

A value other than the Kafka default is logged once per config name for the
life of the process. The default itself is not logged, because clients send
defaults on every create.

`LogAppendTime`, a `compression.type` other than `producer`,
`unclean.leader.election.enable=true`, `remote.storage.enable=true`, and any
unknown name stay `INVALID_CONFIG` (40). Those values would claim a behavior
Camus cannot perform.

## Consequences

- Existing tools keep creating topics.
- An operator reading `DescribeConfigs` sees both the stored value and the
  effective behavior.
- Durability does not become configurable through `min.insync.replicas`.
  Changing `durability.mode` on a topic that has data is a separate decision.
