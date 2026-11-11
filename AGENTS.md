# Camus

A Kafka broker in one Rust binary. Kafka wire protocol in; the log lives in
R2 or any S3 bucket. Metadata and a short write-ahead log replicate with Raft
(openraft). Kafka behaviour is pinned to Apache Kafka 4.4.0.

## Map

- `crates/protocol`: generated Kafka 4.4.0 wire types
- `crates/broker`: the binary; `src/kafka.rs` dispatches every API
- `crates/raft`: segment-file Raft log
- `crates/metadata`: metadata Raft group, `redb` state machine
- `crates/wal`: WAL shard Raft group
- `crates/storage`: S3 client and disk cache
- `crates/uploader`: object bundling
- `crates/coordinator`: consumer groups
- `crates/compactor`: retention and sweep planning
- `crates/cli`: the `camus admin` CLI
- `crates/engine-bench`: storage-engine benchmark
- `docs/PLAN.md`: what is built
- `docs/adr/`: decisions
- `docs/compatibility.md`: API matrix and differences from Kafka
- `site/`: the website
- `Benchmarks/`: benchmark results

Some crates have their own `AGENTS.md` with invariants. Read it before you
edit that crate.

## Commands

```
cargo build --workspace
cargo test --workspace --locked            # what CI runs
cargo test -p <crate> --test <file>        # one suite
scripts/agent-check.sh                     # rules below + tests, before a PR
```

## Rules

- No `.unwrap()` or `.expect(` in code you add, tests included. Return
  `Result` and use `?`.
- Memory is a fixed budget. Any cache, map, or buffer you add has a stated
  byte or entry cap. If it can't be capped, change the on-disk layout.
- Never weaken durability. An ack means a WAL majority fsync, or an R2 put
  plus a metadata commit (ADR 0005).
- Kafka 4.4.0 decides behaviour. Record any intended difference in
  `docs/compatibility.md`.
- Don't hand-edit `crates/protocol/src/messages/`; it is generated.
- Run `cargo fmt --all` before committing. CI checks it.
- Commit subject: one sentence saying what changed and why, ending in a
  period. Example: "Start a second WAL commit only when a full group is
  waiting."

## Skills

Before a task, read the `SKILL.md` whose line below matches it. Skills are in
`.agents/skills/<name>/SKILL.md`.

- `testing`: run, choose, or write tests; the env gates and interop clients
- `run-cluster`: start brokers and MinIO locally, then init and inspect them
- `kafka-api`: add or change a Kafka API or version
- `perf`: performance work, benchmarks, `Benchmarks/` results
- `review`: check a diff against durability, memory, and protocol invariants
- `adr`: record a design decision
- `deploy`: Docker, compose, Railway, the GHCR image
- `site`: edit `site/` and deploy it
- `ship`: branch, commit, PR, CI

For openraft, redb, tokio, or object_store APIs, use Context7 when you have
it. Otherwise read the crate source in `~/.cargo/registry`.
