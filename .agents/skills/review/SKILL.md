---
name: review
description: Review a Camus diff for durability, ordering, memory-budget, protocol, and error-handling invariants before a PR. Reports findings; changes nothing.
---

# Invariant review

Get the diff with `git diff origin/main...HEAD` (add the working tree if
there are uncommitted changes). For each changed crate, read its
`AGENTS.md` and the ADRs it names. Check:

1. **Durability.** No ack, response, or visibility before the barrier: WAL
   majority fsync, or R2 put plus metadata commit (ADR 0005). Recovery must
   still work from R2 and metadata alone (ADR 0008).
2. **Ordering.** State that must survive failover is committed through Raft
   before anyone sees it. Object keys are write-once. Deletes happen only
   for unreferenced objects, after the delay.
3. **Memory.** Every new collection, cache, channel, or buffer has a cap.
   Anything that grows with partitions, data, or time is a finding.
4. **Protocol.** Each version range matches `frame.rs` `IMPLEMENTED` and
   `docs/compatibility.md`. Error codes match Kafka 4.4.0. No edits to
   generated `messages/`.
5. **Errors.** No added `.unwrap()` or `.expect(` (run
   `scripts/agent-check.sh --no-test`). No ignored `Result` on I/O.
6. **Tests.** The behaviour change has a test. Any gated test needed to
   prove it is named, with its env var.

Report each finding as `file:line`, the invariant, and a concrete failure
scenario. Leave out style comments. If nothing fails, say so in one line.
