# Kafka compatibility to-do

Status: draft. Date: 2026-11-08.

Gaps between Camus and Apache Kafka 4.4.0 found by reading the code, and a
plan for each. Priorities:

- **P0**: stops Camus replacing Kafka for ordinary deployments.
- **P1**: a behavior differs from Kafka, or a common tool breaks.
- **P2**: polish, ecosystem coverage, docs.

Items marked *deliberate* are accepted in an ADR. For those, the plan says
whether to keep the difference or change it. Changing one means amending the
ADR first.

## Summary

| # | Item | Pri | Kind |
| --- | --- | --- | --- |
| 1 | [CreateTopics drops topic configs](#1-createtopics-drops-topic-configs) | P0 | gap |
| 2 | [Retention is never enforced](#2-retention-is-never-enforced) | P0 | gap |
| 3 | [Bucket objects are never deleted](#3-bucket-objects-are-never-deleted) | P0 | gap |
| 4 | [Only three topic configs can be changed](#4-only-three-topic-configs-can-be-changed) | P0 | gap |
| 5 | [One SASL/PLAIN identity, no SCRAM or mTLS](#5-one-saslplain-identity-no-scram-or-mtls) | P0 | gap |
| 6 | [ACLs allow when no ACL exists, and match host `*` only](#6-acls-allow-when-no-acl-exists-and-match-host--only) | P1 | deliberate |
| 7 | [Leader epoch never changes; no OffsetForLeaderEpoch](#7-leader-epoch-never-changes-no-offsetforleaderepoch) | P1 | gap |
| 8 | [DeleteRecords trims to whole objects](#8-deleterecords-trims-to-whole-objects) | P1 | deliberate |
| 9 | [Compaction is off by default and keeps tombstones](#9-compaction-is-off-by-default-and-keeps-tombstones) | P1 | deliberate |
| 10 | [One shard leader takes every write](#10-one-shard-leader-takes-every-write) | P1 | deliberate |
| 11 | [Missing operations APIs](#11-missing-operations-apis) | P1 | gap |
| 12 | [Ecosystem is untested](#12-ecosystem-is-untested) | P1 | test |
| 13 | [No automatic topic creation](#13-no-automatic-topic-creation) | P2 | gap |
| 14 | [`throttle_time_ms` makes clients wait twice](#14-throttle_time_ms-makes-clients-wait-twice) | P2 | deliberate |
| 15 | [JoinGroup reports client host `/unknown`](#15-joingroup-reports-client-host-unknown) | P2 | gap |
| 16 | [Default KIP-848 assignor is not sticky](#16-default-kip-848-assignor-is-not-sticky) | P2 | deliberate |
| 17 | [`acks=1` waits for the full barrier](#17-acks1-waits-for-the-full-barrier) | — | deliberate, keep |
| 18 | [Transactions have no control markers](#18-transactions-have-no-control-markers) | — | deliberate, keep |
| 19 | [Documentation drift](#19-documentation-drift) | P2 | docs |

Suggested order: 1 → 4 → 2 → 3 → 19, then 12 to find what breaks next, then 5,
6, 7. Items 1, 2 and 4 share the topic-config code, so they fit in one PR
series.

---

## P0

### 1. CreateTopics drops topic configs

- [ ] Done

**Problem.** `create_topics` reads only `durability.mode` from the request.
Every other config, including `cleanup.policy=compact`, `retention.ms` and
`retention.bytes`, is dropped without an error. `replication.factor` is
ignored too.

**Evidence.** `crates/broker/src/kafka.rs:901-925`. `crates/metadata/src/catalog.rs:159`
creates every topic with default retention and `cleanup.policy=delete`. The
compaction tests set `compact` afterwards through AlterConfigs
(`crates/broker/tests/compact.rs:195`, `crates/broker/tests/java/Phase5RoundTrip.java:32`).

**Impact.** Kafka Connect and Schema Registry create compacted internal
topics and then check the policy, so they will probably fail. Kafka Streams
changelog topics are never compacted.

**Plan.**
1. Pass the full config list to `Node::create_topic`
   (`crates/metadata/src/lib.rs:362`) and on to `catalog::create_topic`.
2. Reuse the parsing and validation in `apply_edits`
   (`crates/metadata/src/admin.rs:224`), so create and alter accept the same
   values and return the same errors.
3. Apply the item 4 table of accepted configs. An unknown name returns
   `INVALID_CONFIG` (40), as Kafka does.
4. `replication.factor`: accept `-1`, or any value from 1 to the shard voter
   count. Return `INVALID_REPLICATION_FACTOR` (38) above that.
5. Make `validate_only` run the same validation without writing anything.
6. Return the applied configs in `CreatableTopicResult.configs` (v5+).

**Done when.** A Rust test and the Java, librdkafka and franz-go harnesses
create a topic with `cleanup.policy=compact` and `retention.ms=60000`, and
DescribeConfigs returns both values.

### 2. Retention is never enforced

- [ ] Done

**Problem.** `retention.ms` and `retention.bytes` are stored and reported.
No code acts on them, so a partition only shrinks through DeleteRecords.

**Evidence.** The only readers are DescribeConfigs (`kafka.rs:1025-1026`)
and the alter path. `crates/compactor/src/lib.rs` says "Not started."

**Plan.**
1. Add a retention task on the metadata leader. Run it on the same loop as
   `compact_sealed` (`kafka.rs:434`), with its own interval:
   `CAMUS_RETENTION_CHECK_MS`, default 300000 (Kafka's
   `log.retention.check.interval.ms`).
2. Apply it only to topics whose `cleanup.policy` includes `delete`.
3. By time: drop each leading extent whose `max_timestamp` is older than
   `now - retention.ms`. Extents already carry `max_timestamp`
   (`catalog.rs:64`).
4. By size: walk extents from the newest, add up `length`, and drop whole
   extents once the total passes `retention.bytes`.
5. Commit the new `log_start` through the same metadata command
   DeleteRecords uses (`admin.rs:314`), so fetch and ListOffsets see it at
   once.
6. Skip unsealed WAL data, as Kafka never deletes the active segment.
7. Leave object deletion to item 3.
8. Add the ADR 0010 changes, and record that retention works at extent
   granularity, like Kafka's segment granularity.

**Done when.** A test produces old-timestamped batches, waits one check
interval, and sees log start advance and old offsets return
`OFFSET_OUT_OF_RANGE`.

### 3. Bucket objects are never deleted

- [ ] Done

**Problem.** DeleteRecords, compaction and DeleteTopics drop extents from
metadata, but nothing deletes the objects. `Bucket::delete` exists
(`crates/storage/src/lib.rs:95`) and nothing calls it. Once item 2 lands,
retention will also grow the bucket forever.

**Plan.**
1. Build this in `crates/compactor`, the crate set aside for it.
2. ADR 0007 makes ingest objects bundles that span partitions, so an object
   is garbage only when no extent in any partition points at it.
3. On the metadata leader, periodically:
   1. Take a metadata snapshot.
   2. Build the set of referenced object keys.
   3. List the bucket prefix.
   4. Delete any key that is unreferenced and older than a grace period.
4. Set the grace period with `CAMUS_GC_GRACE_MS`, default 1 hour. It has to
   cover in-flight uploads that are not committed yet, and readers still
   holding old extents.
5. This is the orphan sweep ADR 0007 describes (`0007-r2-layout-and-cost.md:106`).
   Check it against that text.
6. Add metrics: objects deleted, bytes reclaimed, sweep duration.

**Done when.** A MinIO test deletes a topic, runs one sweep after the grace
period, and the bucket holds no objects for that topic.

### 4. Only three topic configs can be changed

- [ ] Done

**Problem.** `apply_edits` accepts only `retention.ms`, `retention.bytes` and
`cleanup.policy` (`admin.rs:224-235`). Anything else returns
`INVALID_CONFIG`. Kafka Streams sets `segment.bytes`,
`message.timestamp.type` and others when it creates topics, and Terraform
providers send full config sets.

**Plan.** Use one table for create and alter, with three tiers:

| Tier | Configs | Behavior |
| --- | --- | --- |
| Enforced | `cleanup.policy`, `retention.ms`, `retention.bytes`, `durability.mode`, `max.message.bytes`, `delete.retention.ms` (item 9), `min.compaction.lag.ms` | Stored and applied |
| Accepted, no effect | `segment.bytes`, `segment.ms`, `segment.index.bytes`, `min.insync.replicas`, `unclean.leader.election.enable=false`, `message.timestamp.type=CreateTime`, `compression.type=producer`, `file.delete.delay.ms`, `flush.*`, `index.interval.bytes`, `preallocate`, `local.retention.*` | Stored and returned by DescribeConfigs, documented as no-ops |
| Rejected | `message.timestamp.type=LogAppendTime`, `unclean.leader.election.enable=true`, `compression.type` other than `producer`, unknown names | `INVALID_CONFIG` with an error message |

1. Store the second tier in a `BTreeMap<String, String>` on `Topic` with
   `#[serde(default)]`, so existing rows still load.
2. Have DescribeConfigs return every row, with `config_source` and
   `is_default` set correctly.
3. Enforce `max.message.bytes` on produce against batch size, under
   `MAX_FRAME`.
4. Add a "Topic configs" table to `docs/compatibility.md`.

**Done when.** Kafka Streams can create its internal topics (item 12), and
`kafka-configs.sh --describe` lists every row.

### 5. One SASL/PLAIN identity, no SCRAM or mTLS

- [ ] Done

**Problem.** The broker accepts exactly one username and password, from
`CAMUS_SASL_USER` and `CAMUS_SASL_PASSWORD` (`crates/broker/src/main.rs:548`).
The only mechanism is `PLAIN` (`kafka.rs:2124`). TLS has no client auth
(`main.rs:584`). With one identity, ACLs cannot separate anything.

**Plan.**
1. **SCRAM-SHA-256 and SCRAM-SHA-512.**
   1. Store salted credentials in the metadata Raft group.
   2. Implement `DescribeUserScramCredentials` (50) and
      `AlterUserScramCredentials` (51).
   3. Add the mechanisms to SaslHandshake.
   4. Keep the env-var PLAIN user as the bootstrap superuser, so a new cluster
      can create the first SCRAM users.
2. **Superusers.** Add `CAMUS_SUPER_USERS`, matching Kafka's `super.users`.
   ACL checks skip these principals.
3. **mTLS.** Add `--tls-client-ca`. Use rustls `WebPkiClientVerifier`, and
   map the certificate subject to `User:<DN>`, as Kafka's default principal
   builder does.
4. **OAUTHBEARER.** Later. Leave the row unimplemented until there is demand.
5. Write ADR 0012 amendments before starting the code.

**Done when.** The three client harnesses log in with SCRAM as two users,
and an ACL allows one user and denies the other.

---

## P1

### 6. ACLs allow when no ACL exists, and match host `*` only

- [ ] Done

*Deliberate, ADR 0012. Recommendation: change the default.*

**Problem.** A resource with no ACL is allowed. In Kafka, once an authorizer
is configured, a resource with no ACL is denied unless
`allow.everyone.if.no.acl.found=true`. Only host `*` matches. Once item 5
gives Camus real users, allow-by-default is a security risk.

**Plan.**
1. Add `CAMUS_ACL_ALLOW_IF_NO_ACL`, with the same meaning as Kafka's flag.
   Default it to `false` when SASL is required, and `true` on the dev
   PLAINTEXT listener.
2. Pass the peer address into the connection handler (`kafka.rs:648`). Then
   match ACL hosts by exact IP, and report it in JoinGroup (item 15).
3. Check the `TOPIC_AUTHORIZATION_FAILED` (29) row. Kafka also returns 29 for
   a denied produce or fetch. If the behavior matches, delete that row from
   the deliberate-differences tables in `README.md` and `compatibility.md`.
4. Amend ADR 0012.

**Done when.** With SASL on and no ACLs, a non-super user is denied. An ACL
for a specific host matches only that client.

### 7. Leader epoch never changes; no OffsetForLeaderEpoch

- [ ] Done

**Problem.** `leader_epoch` starts at 0 and does not change with the shard's
Raft term (`catalog.rs:181`, and `compatibility.md` says so). OffsetForLeaderEpoch
(23) is unimplemented. Clients therefore can't detect a truncated log.
`camus admin truncate-to-sealed` drops acknowledged offsets, and consumers
positioned past the cut get no signal before reading new records at reused
offsets.

**Plan.**
1. When a shard leader records itself in metadata, write the shard term as
   the `leader_epoch` of each partition in that shard.
2. Bump the epoch on `truncate-to-sealed`.
3. Keep an epoch history per partition: a list of `(epoch, start_offset)`
   pairs. Store it in `Partition` with `#[serde(default)]`.
4. Implement OffsetForLeaderEpoch v3–4 from that history.
5. In Produce and Fetch, check `current_leader_epoch`. Return
   `FENCED_LEADER_EPOCH` (74) or `UNKNOWN_LEADER_EPOCH` (76) as Kafka does.
6. Make Metadata return the real epoch.

**Done when.** A cluster test truncates a shard, and the Java consumer logs
truncation detection and resets according to `auto.offset.reset`.

### 8. DeleteRecords trims to whole objects

- [ ] Done

*Deliberate, ADR 0010. Recommendation: match Kafka logically, keep the
physical behavior.*

**Problem.** `delete_records` moves `log_start` forward to the next whole
extent boundary (`admin.rs:353-356`), not the requested offset. Kafka Streams
calls DeleteRecords on repartition topics with exact offsets.

**Plan.**
1. Split the state into two values:
   - a logical `log_start`: the requested offset;
   - a physical floor: the first extent still kept.
2. Keep extents until the logical start passes their end.
3. In fetch, start at the logical start. A batch that spans it is returned
   whole; Kafka does the same, and clients skip the earlier records.
4. Return the requested offset as the low watermark and from ListOffsets
   `EARLIEST`.
5. Remove the row from the deliberate-differences tables, and amend ADR 0010.

**Done when.** A DeleteRecords call at an offset inside an extent reports that
offset as log start, and a fetch below it returns `OFFSET_OUT_OF_RANGE`.

### 9. Compaction is off by default and keeps tombstones

- [ ] Done

*Deliberate, ADR 0010. Recommendation: turn it on by default and add tombstone
expiry, after item 3.*

**Problem.**
- Compaction runs only when `CAMUS_COMPACT_MS > 0` (`kafka.rs:190`).
- Tombstones are kept forever.
- There is no `min.compaction.lag.ms`.
- A record with a null key on a compacted topic returns `CORRUPT_MESSAGE` (2).
  Kafka returns `INVALID_RECORD` (87).

**Plan.**
1. Default `CAMUS_COMPACT_MS` to 300000 once item 3 exists, so rewritten
   objects are actually reclaimed.
2. Add `delete.retention.ms` (Kafka default 86400000). A tombstone is
   removed in a compaction pass when its extent's `max_timestamp` is older than
   `now - delete.retention.ms`.
3. Add `min.compaction.lag.ms`: skip extents newer than the lag.
4. Change the null-key error to `INVALID_RECORD` (87), with the Kafka
   message "Compacted topic cannot accept message without key".
5. Update ADR 0010 and the `compatibility.md` row.

**Done when.** The compaction test checks that a tombstone disappears after
`delete.retention.ms`, without `CAMUS_COMPACT_MS` set.

### 10. One shard leader takes every write

- [ ] Done

*Deliberate, ADR 0006. Recommendation: keep the shard model, and spread
leadership.*

**Problem.** The partition leader is the WAL shard leader. `shard_count`
defaults to 1, so one broker takes every produce. In Kafka, leaders spread
across brokers.

**Plan.**
1. Make the production default `shard_count = number of brokers`. Document
   it in the README "Run" section and in `docker-compose.yml`.
2. Add a leader-balancing task, the equivalent of Kafka's
   `auto.leader.rebalance.enable`. It assigns each shard a preferred leader,
   by shard id mod brokers, and transfers leadership when the current leader
   is not the preferred one. openraft 0.9 has no leadership transfer, so
   check for a workaround (step down plus election timeout) or wait for 0.10.
3. Assign partitions to shards round-robin across topics, not only by
   `partition mod shard_count` within each topic, so a cluster of
   single-partition topics does not land on shard 0.
4. Leave resharding out of scope, as ADR 0006 says.

**Done when.** A 3-broker test with 3 shards and 6 partitions shows each
broker leading 2 partitions.

### 11. Missing operations APIs

- [ ] Done

**Problem.** These APIs are unimplemented, and real tools call them:

| API | Key | Who calls it | Plan |
| --- | --- | --- | --- |
| ConsumerGroupDescribe | 69 | `kafka-consumer-groups.sh --describe` for `group.protocol=consumer` groups | Implement from coordinator state. Highest priority in this table. |
| DescribeTopicPartitions | 75 | `kafka-topics.sh` 4.x `--describe` | Implement from the Metadata handler. Check whether the 4.4 CLI falls back to Metadata first. |
| DescribeProducers | 61 | `kafka-transactions.sh`, hanging-transaction tooling | Implement from the producer state in `crates/metadata/src/producers.rs`. |
| DescribeLogDirs | 35 | AKHQ, Kafka UI, Cruise Control | Return one log dir per broker, named after the bucket, with partition sizes summed from extents. |
| DescribeClientQuotas, AlterClientQuotas | 48, 49 | `kafka-configs.sh --entity-type users` | Only with real byte-rate quotas. Keep unimplemented until then. |
| ElectLeaders | 43 | `kafka-leader-election.sh` | Map `PREFERRED` to the item 10 balancer. Keep `UNCLEAN` unimplemented (ADR 0008). |
| AlterPartitionReassignments, ListPartitionReassignments, AlterReplicaLogDirs | 45, 46, 34 | reassignment tools | Keep unimplemented. Replicas are shard voters, not per-partition. Document why. |
| Delegation tokens, telemetry, UpdateFeatures | 38–41, 71, 72, 57 | rare | Keep unimplemented. |

For each API that lands: generate the versions from the 4.4.0 schema, add it
to `IMPLEMENTED` in `crates/protocol/src/frame.rs`, run it with at least one
real client, and fill in the matrix cells.

### 12. Ecosystem is untested

- [ ] Done

**Problem.** Only `kafka-clients`, librdkafka and franz-go have run. Several
of the items above were found by inference, not by running these tools.

**Plan.** Add a `crates/broker/tests/ecosystem/` harness, skipped unless
`CAMUS_ECOSYSTEM=1`. Use Docker images against a 3-broker cluster.

| Tool | What to check |
| --- | --- |
| Kafka Connect, distributed mode | Worker starts, internal topics are compacted, a FileStream source and sink round-trip, restart resumes from the stored offsets |
| Schema Registry | Starts, registers a schema, restarts and reads it back from `_schemas` |
| Kafka Streams with `exactly_once_v2` | Word count with repartition and changelog topics; kill a broker and get the same output |
| MirrorMaker 2 | Mirror from Apache Kafka 4.4.0 into Camus, with offset sync |
| Kafka CLI tools | `kafka-topics`, `kafka-consumer-groups`, `kafka-configs`, `kafka-acls`, `kafka-transactions`, `kafka-delete-records` |
| kcat | Produce and consume smoke test (already listed as smoke) |
| KafkaJS, confluent-kafka-go, confluent-kafka-dotnet | The existing round-trip programs, ported |

Run this first after items 1 and 4, and add each new failure as an item in this
file.

---

## P2

### 13. No automatic topic creation

- [ ] Done

**Problem.** Metadata ignores `allow_auto_topic_creation`, and producing to a
missing topic returns `UNKNOWN_TOPIC_OR_PARTITION`. Kafka's default
`auto.create.topics.enable` is `true`, though many production clusters turn
it off.

**Plan.**
1. Add `CAMUS_AUTO_CREATE_TOPICS`, default `false`, and document the
   difference from Kafka.
2. When it is on, a Metadata request with `allow_auto_topic_creation=true`
   creates the missing topic. Use `CAMUS_DEFAULT_PARTITIONS` (default 1) and
   the cluster durability default.
3. Return `LEADER_NOT_AVAILABLE` (5) for the first response, as Kafka does,
   so clients retry.

### 14. `throttle_time_ms` makes clients wait twice

- [ ] Done

*Deliberate. Recommendation: change it.*

**Problem.** When the WAL is over `wal.disk.limit`, produce is held, and
`throttle_time_ms` is set to the time held (`kafka.rs:1176`). A client that
honors the throttle then waits the same time again.

**Plan.**
1. Return `throttle_time_ms = 0` after a hold, because the hold has already
   provided the backpressure.
2. Export the hold time as a Prometheus histogram, so operators still see it.
3. Remove the double-wait note from `compatibility.md`.

### 15. JoinGroup reports client host `/unknown`

- [ ] Done

**Problem.** `crates/broker/src/kafka/groups.rs:237` hardcodes
`/unknown`, so `kafka-consumer-groups.sh --describe` shows no host.

**Plan.** Use the peer address plumbed for item 6, formatted as `/<ip>` to
match Kafka. Store it with the member, so DescribeGroups returns it after a
failover.

### 16. Default KIP-848 assignor is not sticky

- [ ] Done

*Deliberate. Recommendation: change it when consumer-protocol groups are
used in production.*

**Problem.** The default `uniform` assignor is round-robin per topic. Kafka's
`uniform` is sticky, so with Camus a rebalance moves more partitions than
necessary.

**Plan.**
1. Make `uniform` sticky: keep each member's current partitions, then hand out
   the unassigned ones to the members with the fewest.
2. Add a test that adding one member moves only the partitions it needs.

---

## Keep as is

### 17. `acks=1` waits for the full barrier

*Deliberate, ADR 0005. Keep.* This avoids losing acknowledged data on leader
failure. The cost is latency.

- [ ] In the README, state the extra `acks=1` latency next to the benchmark
  numbers in `Benchmarks/`.

### 18. Transactions have no control markers

*Deliberate, ADR 0013. Keep.* Read-committed filtering on the server is
correct for the Java, librdkafka and franz-go consumers. `read_uncommitted`
consumers see aborted records, as they would in Kafka.

- [ ] Add a test that offsets stay correct across an abort for a
  `read_committed` consumer that seeks to a specific offset.
- [ ] Note in `compatibility.md` that tools reading raw log segments, or
  expecting control records in fetch output, will see no markers.

---

## 19. Documentation drift

- [ ] Done

Places where the docs and the code disagree:

| File | Drift | Fix |
| --- | --- | --- |
| `docs/compatibility.md`, intro | Says Phase 0 advertises no key and the broker "does not speak Kafka TCP yet" | Replace with a statement of the current `ApiVersions` behavior |
| `docs/compatibility.md`, intro | "A known key gets `UNSUPPORTED_VERSION`" contradicts the later paragraph that unimplemented keys are absent | Keep the later, correct description |
| `docs/compatibility.md` | No mention that retention is not enforced | Add a row until item 2 lands |
| `docs/compatibility.md` | No mention that CreateTopics ignores configs | Add a row until item 1 lands |
| `docs/compatibility.md` | No mention that bucket objects are never deleted | Add a row until item 3 lands |
| `docs/compatibility.md` | No mention of the single SASL identity or missing auto-create | Add rows for items 5 and 13 |
| `docs/compatibility.md`, ACL row | Lists error 29 as a difference; Kafka also returns 29 | Check, then remove (item 6) |
| `docs/compatibility.md`, matrix | Empty `-` cells for APIs the harnesses already call, such as `InitProducerId` with librdkafka and franz-go during idempotent produce | Re-run the harnesses and fill the cells |
| `README.md`, "What clients can do" | Lists `retention.ms` support only implicitly through DescribeConfigs; the deliberate-differences table omits retention, auto-create and auth | Add the same rows as `compatibility.md`, or link to them |
| `docs/PLAN.md`, "What Phase 0 will contain" | Written as future work, but Phase 0 is done | Mark it as historical, or move it to ADR 0013 |
| `crates/compactor/Cargo.toml` | "Not started." | Update when item 3 lands |

Fix this table first. It costs little, and it stops users from relying on
behavior that does not exist.
