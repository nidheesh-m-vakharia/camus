# Camus

![Camus](site/banner.png)

[![License: Apache-2.0](https://img.shields.io/github/license/nidheesh-m-vakharia/camus?color=3e63dd)](LICENSE)
[![CI](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ci.yml/badge.svg)](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ci.yml)
[![Kafka 4.4.0](https://img.shields.io/badge/Kafka-4.4.0-171717)](docs/compatibility.md)
[![Rust](https://img.shields.io/badge/rust-stable-934c1a)](rust-toolchain.toml)

Camus is a broker compatible with Apache Kafka, in one Rust binary.
Producers, consumers, and admin clients use the Apache Kafka wire protocol.
The log is stored in Cloudflare R2, or
any S3-compatible bucket. Brokers replicate metadata, and a short
write-ahead log, with Raft. A deployment does not need a JVM, ZooKeeper,
KRaft, or a separate database.

Kafka compatibility is pinned to Apache Kafka 4.4.0. The API matrix, and
the places Camus deliberately differs, are in
[docs/compatibility.md](docs/compatibility.md).

## Run one broker

Docker listens on 18001 (HTTP admin) and 9092 (Kafka PLAINTEXT), then
initializes a one-node group. Clients use `127.0.0.1:9092`.

```
docker build -t camus .
docker run --rm -p 18001:18001 -p 9092:9092 camus
```

From a checkout, the same shape is:

```
cargo run -p camus-broker -- \
  --node-id 1 \
  --http-addr 127.0.0.1:18080 \
  --kafka-addr 127.0.0.1:19092 \
  --data-dir /tmp/camus \
  --dev-plaintext
curl -sf -X POST http://127.0.0.1:18080/admin/init \
  -H 'content-type: application/json' \
  -d '[{"id":1,"addr":"127.0.0.1:18080"}]'
```

`/admin/init` creates one WAL shard per broker marked `"wal"` (the default),
each with up to three voters, so each broker leads one shard. To choose the
count, send `{"peers": [...], "shards": N}` instead of the bare list.

`--dev-plaintext` is refused on a non-loopback address unless you also pass
`--allow-non-loopback`. SASL_SSL needs `--tls-cert`, `--tls-key`,
`CAMUS_SASL_USER`, and `CAMUS_SASL_PASSWORD`.

The bucket defaults to MinIO at `http://127.0.0.1:9100`, bucket `camus`,
keys `camus` / `camus-dev-secret`. Override with `CAMUS_S3_ENDPOINT`,
`CAMUS_S3_BUCKET`, `CAMUS_S3_ACCESS_KEY`, and `CAMUS_S3_SECRET_KEY`.
`CAMUS_S3_PREFIX` puts every key under a prefix. The broker deletes objects
it no longer references, so each cluster needs its own bucket or prefix. A
container does not see MinIO on the host at `127.0.0.1`; point
`CAMUS_S3_ENDPOINT` at the host. `CAMUS_S3_REGION` defaults to `us-east-1`.
Set `CAMUS_S3_VIRTUAL_HOST=1` for a virtual-hosted endpoint such as a
Railway Bucket. `CAMUS_ADVERTISED_HOST` and `CAMUS_ADVERTISED_PORT` are
the address clients should dial. On Railway those fall back to
`RAILWAY_TCP_PROXY_DOMAIN` and `RAILWAY_TCP_PROXY_PORT`.

`GET /admin/metrics` returns JSON, including `current_leader` and
`wal_leader`. `GET /metrics` is Prometheus: consumer lag, WAL upload lag,
object-store calls by operation, and produce and fetch latency.
[docs/metrics.md](docs/metrics.md) lists every series and its memory cost,
and [deploy/grafana/camus.json](deploy/grafana/camus.json) is a Grafana
dashboard for lag, upload lag, and estimated R2 spend.

Three brokers plus MinIO: `docker compose --profile brokers up`. Broker 1
listens on 9092. `init` runs `camus admin init` and retries until the
brokers answer, so a Kafka client can use `localhost:9092` without a
manual curl. The image
is published from `main` to `ghcr.io/<owner>/camus`, tagged `latest` and
the commit sha, after the test workflow passes. The GitHub package stays
private until you change that in the package settings.

## Railway

`.railway/railway.ts` is one broker from this repo, a volume at `/data`,
a Railway Bucket named `logs` for sealed extents, and a public TCP proxy
on 9092. The Kafka listener is PLAINTEXT. Admin HTTP listens on Railway's
`PORT` and stays off the public internet. `GET /health` is the healthcheck.

```
npm install --prefix .railway
railway login
railway link
railway config plan
railway config apply
```

Bootstrap servers are `RAILWAY_TCP_PROXY_DOMAIN:RAILWAY_TCP_PROXY_PORT`.
The bucket variables are references to `logs`, so credentials stay in
Railway.

## What clients can do

Change the bootstrap server. These have been run against Camus:
`kafka-clients` 4.4.0, librdkafka (confluent-kafka 2.16.1), franz-go
v1.22.0, KafkaJS 2, and, from Apache Kafka 4.4.0, the CLI tools, Kafka
Connect, Kafka Streams with `exactly_once_v2`, and MirrorMaker 2.

- Produce and fetch, including idempotent produce. A retried sequence
  returns the original offset.
- Consumer groups: classic join, sync, heartbeat, commit, and leave, plus
  list, describe, and delete. The Java client can use
  `group.protocol=consumer` (KIP-848 `ConsumerGroupHeartbeat`).
- Transactions. `read_committed` hides a batch until `EndTxn` commits it.
  An abort stays hidden. There are no control-batch markers; visibility is
  the committed and aborted offset ranges.
- Topic admin: create topics with configs (or auto-create them), describe
  cluster, topic and broker configs, create partitions, delete records at
  any offset, delete topics, ACLs, log dirs, producers, and KIP-848 group
  descriptions.
- Retention by time and size, and deletion of objects nothing references.
- Leader epochs and `OffsetForLeaderEpoch`, so consumers detect a
  truncated log.
- Users: SASL/PLAIN, SASL/SCRAM-SHA-256 and -512 (managed with the Kafka
  admin APIs), and mTLS client certificates, with Kafka's ACL rules on
  every API.
- `cleanup.policy=compact` keeps the latest record for each key, at its
  original offset, over sealed extents, every `CAMUS_COMPACT_MS` (default
  300000; 0 turns it off). Tombstones go after `delete.retention.ms`, and
  `min.compaction.lag.ms` keeps recent records uncompacted.

A few Kafka behaviors are different on purpose:

| | Camus |
| --- | --- |
| `acks=1` | Same barrier as `acks=all` |
| `acks=0` | No wait, and the batch may be lost |
| No ACL on a resource | Denied with SASL or mTLS, allowed on the dev PLAINTEXT listener; `CAMUS_ACL_ALLOW_IF_NO_ACL` overrides |
| Partition leaders | Leadership belongs to WAL shards (one per core by default, shared evenly across brokers); a shard's partitions move together when it rebalances or on `ElectLeaders`; no reassignment (ADR 0014) |
| Broker configs | Set at startup, read-only; no client quotas |

The full list is in [docs/compatibility.md](docs/compatibility.md).

## Durability

| Mode | When a produce is acknowledged |
| --- | --- |
| `wal` (default) | A majority of the WAL shard has fsynced the batch. The upload to R2 follows, and the WAL is truncated. |
| `r2` | The batch is in R2 and the metadata group has committed its offsets. |

`wal` keeps a few seconds of log on local disk so acknowledgements are not
limited by object-storage latency. `r2` keeps no log bytes on the broker.
See [ADR 0005](docs/adr/0005-durability-and-acks.md).

The metadata leader writes a snapshot to `meta/snap/` every 30 seconds or
10,000 applied entries, and moves `meta/snap/CURRENT` at most once a
second. If every broker disk is lost, metadata newer than that snapshot is
gone. Sealed log bytes can still be rebuilt from the objects (ADR 0008).
`CAMUS_META_SNAPSHOT_MS=0` turns the upload off.
`camus admin restore-cluster` loads that snapshot onto a fresh metadata
group and rebuilds sealed extents from object footers. Unsealed WAL
entries stay lost. Group offsets can move back by one snapshot interval.

Set the mode at topic creation with `durability.mode`. The cluster default
is `CAMUS_DEFAULT_DURABILITY`.

`acks=1` waits for the same barrier as `acks=all`, so it costs what
`acks=all` costs rather than a leader-local write. One producer on three
brokers, 1 KiB `acks=all`, measured:

| Environment | `wal` p50 / p99 | `r2` p50 / p99 |
| --- | --- | --- |
| Linux 7.0 (aarch64), OrbStack VM on an Apple M4 Pro, 12 CPUs, 12 GiB, broker data on a Docker volume, MinIO on the Mac over host networking. The VM disk may not pass every `fsync` through to the SSD. [Table](Benchmarks/README.md#linux), [raw](Benchmarks/raw/linux-orbstack.md) | 7.10 / 16.55 ms | 293.81 / 576.16 ms |
| Apple M4 Pro, macOS, APFS on the internal SSD, MinIO in Docker on that machine. [Table](Benchmarks/README.md#latency-3-brokers-one-producer-1-kib-acksall), [raw](Benchmarks/raw/latency-3-brokers.md) | 11.06 / 31.06 ms | 278.40 / 284.75 ms |

An acknowledged record survives the loss of the leader.

## Configuration

Broker settings come from `camus.toml` at startup (Kafka's name, or
`durability.mode`, `wal.disk.limit`, and `[s3]`). Pass `--config` to use
another path. A variable below overrides the file, and a flag overrides
both. The process prints `key=value source` as it starts. Kafka's name is
in parentheses where there is one;
[docs/compatibility.md](docs/compatibility.md#defaults) lists each default
next to Kafka's. `camus admin config` prints the same list from a running
broker.

| Variable | Default | Meaning |
| --- | --- | --- |
| `CAMUS_S3_ENDPOINT`, `CAMUS_S3_BUCKET`, `CAMUS_S3_ACCESS_KEY`, `CAMUS_S3_SECRET_KEY` | local MinIO, only with `--dev-plaintext` | Object storage |
| `CAMUS_S3_PREFIX` | empty | Key prefix; one per cluster sharing a bucket |
| `CAMUS_SASL_USER`, `CAMUS_SASL_PASSWORD` | — | Bootstrap SASL/PLAIN user, always a super user |
| `CAMUS_SUPER_USERS` | — | `;`-separated principals (`super.users`) |
| `CAMUS_ACL_ALLOW_IF_NO_ACL` | `false` on authenticated listeners | `allow.everyone.if.no.acl.found` |
| `CAMUS_DEFAULT_DURABILITY` | `wal` | `durability.mode` for new topics |
| `CAMUS_AUTO_CREATE_TOPICS` | `true` | `auto.create.topics.enable` |
| `CAMUS_NUM_PARTITIONS` | 1 | `num.partitions` |
| `CAMUS_MESSAGE_MAX_BYTES` | 1048588 | `message.max.bytes` |
| `CAMUS_WAL_DISK_LIMIT` | 8 GiB | Unsealed WAL bytes before produce is held |
| `CAMUS_FLUSH_MS` | 250 | Busy upload interval. A shard with at least 4 MiB unsealed uses this |
| `CAMUS_QUIET_FLUSH_MS` | 30000 | How long a smaller unsealed prefix waits before upload |
| `CAMUS_RACK` | — | `broker.rack`. Fetch v11 sends a client in this zone to a local voter |
| `CAMUS_RETENTION_CHECK_MS` | 300000 | `log.retention.check.interval.ms` |
| `CAMUS_COMPACT_MS` | 300000 | Compaction interval; `0` turns it off |
| `CAMUS_SWEEP_MS` | 300000 | Orphaned-object sweep interval |
| `CAMUS_SWEEP_MIN_AGE_MS` | 3600000 | An object younger than this is never swept |
| `CAMUS_FILE_DELETE_DELAY_MS` | 60000 | `file.delete.delay.ms` |
| `CAMUS_TXN_MAX_TIMEOUT_MS` | 900000 | `transaction.max.timeout.ms` |
| `CAMUS_TXN_ABORT_INTERVAL_MS` | 10000 | How often timed-out transactions are aborted |
| `CAMUS_TRANSACTIONAL_ID_EXPIRATION_MS` | 604800000 | `transactional.id.expiration.ms` |
| `CAMUS_TXN_CLEANUP_INTERVAL_MS` | 3600000 | How often idle transactional ids are removed |
| `CAMUS_PRODUCER_ID_EXPIRATION_MS` | 86400000 | `producer.id.expiration.ms` |
| `CAMUS_PRODUCER_ID_CHECK_INTERVAL_MS` | 600000 | `producer.id.expiration.check.interval.ms` |
| `CAMUS_OFFSETS_RETENTION_MINUTES` | 10080 | `offsets.retention.minutes` (`CAMUS_OFFSETS_RETENTION_MS` overrides, for tests) |
| `CAMUS_OFFSETS_RETENTION_CHECK_INTERVAL_MS` | 600000 | `offsets.retention.check.interval.ms` |
| `CAMUS_GROUP_INITIAL_DELAY_MS` | 3000 | `group.initial.rebalance.delay.ms` |
| `CAMUS_GROUP_MIN_SESSION_MS` | 6000 | `group.min.session.timeout.ms` |
| `CAMUS_CONSUMER_SESSION_MS`, `CAMUS_CONSUMER_HEARTBEAT_MS` | 45000, 5000 | KIP-848 session and heartbeat |
| `CAMUS_AUTO_LEADER_REBALANCE` | `true` | `auto.leader.rebalance.enable` |
| `CAMUS_LEADER_IMBALANCE_CHECK_MS` | 300000 | `leader.imbalance.check.interval.seconds` |
| `CAMUS_QUIC` | off | Raft votes, empty heartbeats, and shard announcements. `on` sends them over QUIC |
| `CAMUS_QUIC_PORT` | the HTTP port | UDP port for that traffic; set the same value on every broker |
| `CAMUS_SHARD_ANNOUNCE_MS` | 200 | How often a shard leader writes its view to metadata |

Flags: `--kafka-addr`, `--advertised-addr` (`advertised.listeners`),
`--tls-cert`/`--tls-key` (SASL_SSL), `--tls-client-ca` (mTLS),
`--require-sasl`, `--dev-plaintext`, `--allow-non-loopback`.

## Develop

```
cargo test --workspace
docker compose up -d minio create-bucket
CAMUS_MINIO=1 cargo test -p camus-storage --test minio
```

`cargo test -p camus-broker --test cluster` starts three processes, commits
a metadata write, kills the leader, and checks that a new write commits.
MinIO is built from `deploy/minio` because current community images are no
longer published. Tests that need a bucket skip unless `CAMUS_MINIO=1`.

`CAMUS_ECOSYSTEM=1 cargo test -p camus-broker --test ecosystem -- --test-threads=1`
runs the Apache Kafka 4.4.0 CLI tools, Kafka Connect, Kafka Streams with
`exactly_once_v2`, MirrorMaker 2 from a real Kafka broker, KafkaJS,
confluent-kafka-go, and confluent-kafka-dotnet against Camus. It downloads
the Kafka distribution into `target/` and needs a JDK, `npm`, Go with
librdkafka, and the .NET SDK. Schema Registry runs in Docker and needs the
broker on a non-loopback address, so it also needs `CAMUS_ECOSYSTEM_DOCKER=1`.

`--advertised-addr host:port` is what Metadata hands out, as Kafka's
`advertised.listeners`; use it behind NAT or for clients in containers.

GitHub Actions runs `cargo test --workspace --locked` on pull requests and
on `main`.

## When not to use Camus

The fit is a log with several consumers and replay, without a full disk replica on every broker. These want something else. The longer list is [site/limits.html](site/limits.html#when-not).

- A p99 under 10 ms on the object-storage path. Measured `r2` p99 is 576.16 ms (Linux OrbStack) and 284.75 ms (macOS). `wal` on that Linux run is 16.55 ms p99. Those are the rows in the durability section above and in [Benchmarks/](Benchmarks/README.md).
- One cluster across regions. Election waits 400–800 ms. MirrorMaker 2 is tested from Apache Kafka 4.4.0 into Camus, not the other way, in [docs/compatibility.md](docs/compatibility.md).
- A consensus group per tiny partition. Partitions share a shard log ([ADR 0006](docs/adr/0006-wal-shards.md)). The default count follows CPU cores and stops at eight shards per voter.
- A queue that acknowledges one message. Consumers commit an offset. Share groups are not in the API matrix.
- A Kafka deployment whose disk and JVM cost is already fine.

## Docs

- [docs/PLAN.md](docs/PLAN.md) — what has been accepted, phase by phase
- [docs/adr/](docs/adr/) — the decisions
- [docs/compatibility.md](docs/compatibility.md) — Kafka 4.4.0 pin and the API matrix
- [site/limits.html](site/limits.html) — production limits
- [site/delivery.html](site/delivery.html) — ordering, retries, transactions, loss windows
- [site/compare.html](site/compare.html) — AutoMQ, Redpanda, Iggy, Nisshi, WarpStream, Bufstream

The workspace crates are `protocol`, `broker`, `raft`, `metadata`, `wal`,
`storage`, `uploader`, `coordinator`, `compactor`, `cli`, and
`engine-bench`. `camus admin truncate-to-sealed` resumes a WAL shard that
has lost a majority, at its sealed end. That drops acknowledged data that
was not sealed yet.

License: Apache-2.0.

Apache, Apache Kafka and Kafka are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. Camus is not affiliated with or endorsed by the ASF.
