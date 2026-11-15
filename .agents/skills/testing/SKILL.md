---
name: testing
description: Run, choose, or write Camus tests. Covers the env gates (MinIO, ecosystem, interop clients), the ignored benchmarks, and the test helpers.
---

# Testing

## Tiers

| Tier | Command | Needs |
| --- | --- | --- |
| Default (CI) | `cargo test --workspace --locked` | nothing; gated tests print "skipping" and pass |
| One suite | `cargo test -p camus-broker --test groups` | |
| Bucket | `docker compose up -d minio create-bucket`, then `CAMUS_MINIO=1 cargo test ...` | Docker |
| Interop clients | `cargo test -p camus-broker --test clients` | `target/kafka-jars/kafka-clients-4.4.0.jar` (+ deps), `target/pykafka/bin/python`, `target/go/bin/go`, JDK 21 |
| Ecosystem | `CAMUS_ECOSYSTEM=1 cargo test -p camus-broker --test ecosystem -- --test-threads=1` | JDK, npm, Go + librdkafka, .NET SDK; `CAMUS_ECOSYSTEM_DOCKER=1` adds Schema Registry |
| Benchmarks | see the `perf` skill | `--release -- --ignored` |

A gated test that skips still passes. If the test you need to run printed
"skipping", say so in your report. Don't claim it passed.

## Choosing a suite

Each file in `crates/broker/tests/` is named for its feature: `acl`,
`groups`, `group_failover`, `retention`, `compact`, `shards`, `sasl`,
`scram_mtls`, `meta_snapshot`, `epochs`, and so on. Run the file for the area
you changed, then the workspace.

## Writing tests

- Broker integration tests start real broker processes through
  `crates/broker/tests/support/mod.rs`: `Node`, `Opts`, `single`,
  `produce`, `read_all`, `create_wal_topic`, `wait_settled`,
  `partition_leader`, and `Proxy` for fault injection. Reuse them; don't add
  a second harness.
- Speed tests up with env knobs instead of sleeps: `CAMUS_FLUSH_MS`,
  `CAMUS_COMPACT_MS`, `CAMUS_RETENTION_CHECK_MS`, `CAMUS_SWEEP_MS`,
  `CAMUS_META_SNAPSHOT_MS`.
- Gate anything that needs a bucket on `CAMUS_MINIO=1`, the way existing
  tests do.
- New tests return `Result` and use `?`. Existing tests use `unwrap`; don't
  copy that.
- Protocol encoding: `crates/protocol/tests/roundtrip.rs` and `testdata/`.
