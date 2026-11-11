# camus-coordinator

Classic consumer groups and KIP-848, run on the metadata leader. ADR 0013.

- Live membership is in memory. Anything that must survive failover goes
  through `Store` (the metadata group) before any member hears of it: a new
  generation before it is announced, an assignment before a member
  receives it.
- Broker tests: `groups`, `group_failover`, `offsets`, `produce_group`.
