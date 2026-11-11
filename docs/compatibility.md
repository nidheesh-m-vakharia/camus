# Compatibility

Behavior reference: Apache Kafka **4.4.0**.

| | |
| --- | --- |
| Tag | `4.4.0` |
| Commit | `1156b2752aaa28eafc0b4c4d387e63356793268d` |
| Annotated tag object | `9661b1ef7a0120e476399250b31d530792506787` |
| Released | 2026-10-22 |
| Tree | https://github.com/apache/kafka/tree/4.4.0 |

Where a KIP and this commit disagree, this commit wins. Note the
disagreement in `docs/adr/0002-kafka-compatibility-pin.md`.

The `protocol` crate is generated from this tag's JSON schemas, not from
the crates.io `kafka-protocol` 0.18.0 release (that crate is Kafka 4.1.0).
`ApiVersions` advertises only the keys and versions in the matrix below. A
key Camus knows but does not implement, or a version outside the advertised
range, gets `UNSUPPORTED_VERSION` (35) and the connection stays open. A key
the schema does not name closes the connection, because there is no
response body to put `INVALID_REQUEST` (42) in.

Kafka source is Apache-2.0. See `NOTICE`.

## Deliberate differences

These are accepted in the ADRs. An API Camus does not implement returns an
error rather than a partial imitation.

| Topic | Camus | Kafka 4.4.0 | ADR |
| --- | --- | --- | --- |
| `ApiVersions` contents | Only implemented keys and versions | Every key the broker implements | 0003 |
| `acks=1` | Same barrier as `acks=all`: WAL quorum fsync, or R2 put plus metadata commit | Leader-local ack | 0005 |
| Unclean leader election | Never automatic. Operator truncate only | Default off, can be enabled | 0008 |
| Idempotence | `InitProducerId` allocates a producer id. A retried sequence returns the original offset and is not appended | Supported | 0009 |
| Transactions | `read_committed` hides a batch until `EndTxn` commits its offset range. There is no control-batch marker | Control batches plus `read_committed` | 0013 |
| Compaction | Latest key, original offset, sealed extents only, on a timer rather than a dirty ratio. A tombstone's `delete.retention.ms` counts from its own timestamp | `LogCleaner` by dirty ratio; tombstone horizon counts from the first cleaning (KIP-534) | 0010 |
| ACLs | A resource with no ACL is denied on an authenticated listener (SASL or mTLS) and allowed on the dev PLAINTEXT listener, unless `CAMUS_ACL_ALLOW_IF_NO_ACL` says otherwise | `allow.everyone.if.no.acl.found`, default false | 0012 |
| Partition leadership | Leadership belongs to WAL shards, one per core by default (rounded so each broker leads the same number, capped at eight per broker), each with up to three voters; partitions are placed on shards round-robin at creation and never move. A shard's first voter is its preferred leader: the balancer and `ElectLeaders PREFERRED` move all of the shard's partitions together, by election, so the shard pauses briefly and its epoch rises | Leaders and preferred replicas per partition, reassignment | 0006, 0014 |
| Topic configs | Segment, flush, and `min.insync.replicas` configs are accepted and stored with no effect; `LogAppendTime`, broker-side recompression, and tiered storage are rejected | All applied | — |
| Broker configs and quotas | Broker settings come from the environment at startup and are read-only in `DescribeConfigs`; client quotas are not implemented | Dynamic broker configs and quotas | — |

`acks=0` matches Kafka: the broker does not wait, and the batch may be lost.

## Defaults

Camus matches Kafka 4.4.0's default unless Kafka's can lose acknowledged
data or weaken security; then the safer value is used and listed above. Each
is set by an environment variable.

| Setting | Kafka 4.4.0 | Camus | Variable |
| --- | --- | --- | --- |
| `allow.everyone.if.no.acl.found` | `false` | `false`, except on the dev PLAINTEXT listener | `CAMUS_ACL_ALLOW_IF_NO_ACL` |
| `super.users` | none | none, plus the SASL user from the environment | `CAMUS_SUPER_USERS` |
| `auto.create.topics.enable` | `true` | `true`, with `CREATE` permission | `CAMUS_AUTO_CREATE_TOPICS` |
| `num.partitions` | 1 | 1 | `CAMUS_NUM_PARTITIONS` |
| `message.max.bytes` | 1048588 | 1048588, at most 8388608 | `CAMUS_MESSAGE_MAX_BYTES` |
| `unclean.leader.election.enable` | `false` | never automatic | — |
| `acks=1` | leader write | full quorum write (safer) | — |
| `min.insync.replicas` | 1 | WAL majority (safer) | — |
| `retention.ms` / `retention.bytes` | 604800000 / -1 | same, enforced | topic config |
| `log.retention.check.interval.ms` | 300000 | 300000 | `CAMUS_RETENTION_CHECK_MS` |
| `log.cleaner.enable` | `true` | on, every 300000 ms | `CAMUS_COMPACT_MS` (`0` turns it off) |
| `delete.retention.ms` / `min.compaction.lag.ms` | 86400000 / 0 | same | topic config |
| `file.delete.delay.ms` | 60000 | 60000 | `CAMUS_FILE_DELETE_DELAY_MS` |
| `transaction.max.timeout.ms` | 900000 | 900000 | `CAMUS_TXN_MAX_TIMEOUT_MS` |
| Timed-out transaction abort interval | 10000 | 10000 | `CAMUS_TXN_ABORT_INTERVAL_MS` |
| `transactional.id.expiration.ms` | 604800000 | 604800000 | `CAMUS_TRANSACTIONAL_ID_EXPIRATION_MS` |
| `producer.id.expiration.ms` | 86400000 | 86400000 | `CAMUS_PRODUCER_ID_EXPIRATION_MS` |
| `offsets.retention.minutes` | 10080 | 10080 | `CAMUS_OFFSETS_RETENTION_MINUTES` |
| `group.initial.rebalance.delay.ms` | 3000 | 3000 | `CAMUS_GROUP_INITIAL_DELAY_MS` |
| `group.min.session.timeout.ms` | 6000 | 6000 | `CAMUS_GROUP_MIN_SESSION_MS` |
| `group.consumer.session.timeout.ms` / heartbeat | 45000 / 5000 | same | `CAMUS_CONSUMER_SESSION_MS` / `CAMUS_CONSUMER_HEARTBEAT_MS` |
| `group.consumer.assignors` | `uniform` (sticky), `range` | same | — |
| `auto.leader.rebalance.enable` | `true` | `true`, per WAL shard | `CAMUS_AUTO_LEADER_REBALANCE` |
| `leader.imbalance.check.interval.seconds` | 300 | 300000 ms | `CAMUS_LEADER_IMBALANCE_CHECK_MS` |
| `ssl.client.auth` | `none` | `none`; `required` with `--tls-client-ca` | — |
| PLAINTEXT listener | allowed | opt-in with `--dev-plaintext`, loopback unless `--allow-non-loopback` | — |
| Object storage settings | — | required when serving TLS | `CAMUS_S3_*` |

## API matrix

Status values: `unimplemented`, `partial`, `implemented`.

Produce acks in `r2` mode after the object is stored and the extent is
committed. `wal` produce acks after the WAL shard commits, which means a
majority of its voters has fsynced the batch. Every broker is a voter.
Metadata reports the shard leader as the leader of each `wal` partition and
the voters as replicas and ISR. A broker that is not the shard leader
answers produce, fetch, and list-offsets with `NOT_LEADER_OR_FOLLOWER` (6).
A produce that cannot commit within `timeout_ms`, for example with no
quorum, gets `REQUEST_TIMED_OUT` (7); the entry may still commit later, so a
client without idempotence can duplicate on retry. The upload to object
storage follows the ack. A `wal` partition's leader epoch is the WAL shard's
Raft term plus a base kept in metadata; `truncate-to-sealed` raises the
base, so the epoch only grows. An `r2` partition's epoch is the metadata
group's term. Batches are sealed with that epoch, publish records where each
epoch starts, and `OffsetForLeaderEpoch` (v3–4) answers from that history
and, on the shard leader, from the epochs of batches still in the WAL, so a
consumer detects a truncation and a failover is not mistaken for one. Fetch,
ListOffsets, and `OffsetForLeaderEpoch` check `current_leader_epoch`: older
is `FENCED_LEADER_EPOCH` (74), newer is `UNKNOWN_LEADER_EPOCH` (76). The
shard leader records itself in metadata so every broker names the same
leader; a broker that has just restarted can name a stale leader for about a
second, and clients recover through the usual `NOT_LEADER_OR_FOLLOWER`
retry. After a shard loses a majority, its partitions stay offline until a
lost voter returns or an operator runs `camus admin truncate-to-sealed`,
which resumes them at the sealed end and drops acknowledged offsets past it
(ADR 0008). A topic created without `durability.mode` is `wal`
(`CAMUS_DEFAULT_DURABILITY` changes the cluster default). When unsealed WAL
bytes pass `wal.disk.limit` (`CAMUS_WAL_DISK_LIMIT`, default 8 GiB), produce
is held; it is answered with `REQUEST_TIMED_OUT` only when the client's
`timeout_ms` runs out, and a held batch is not appended, so nothing
acknowledged is lost. The hold has already slowed the producer, so
`throttle_time_ms` stays 0 and the time held is counted in `produce_hold_ms`
(`camus_produce_hold_ms_total`). A failed WAL write (for example a full
disk) answers `KAFKA_STORAGE_ERROR` (56) and stops that broker's shard until
restart.

Consumer groups use the classic protocol. The metadata leader coordinates
every group; another broker answers group requests with
`NOT_COORDINATOR` (16) and clients find the coordinator again. Each new
generation and each assignment is committed through the metadata group
before members hear of it, and an offset commit is checked against the
stored generation when it is applied, so a failover keeps the generation
and an old generation cannot commit. `FindCoordinator` serves groups and
transactions. Share groups answer `INVALID_REQUEST` (42).
`ConsumerGroupHeartbeat` (KIP-848) versions 0–1 are implemented. The
broker's default assignor is `uniform`, sticky like Kafka's `UniformAssignor`
(members keep their partitions up to a fair share and only the rest move),
and `range` is the other supported name. A member
that names any other assignor gets `UNSUPPORTED_ASSIGNOR` (112). Topic
regexes are Rust regexes. An offset commit is accepted at the member's
current epoch or the previous one. The Java client's default remains
`group.protocol=classic`. `group.protocol=consumer` is exercised by
`GroupConsumerRoundTrip`.
Committed offsets of a group with no members expire
`CAMUS_OFFSETS_RETENTION_MINUTES` (default 10080, Kafka's
`offsets.retention.minutes`) after it became empty, checked every
`CAMUS_OFFSETS_RETENTION_CHECK_INTERVAL_MS` (default 600000); a group that only
commits counts from each commit, as in Kafka. The group goes with its last
offset. A group with members keeps its offsets.
`DeleteRecords` moves the log start to the requested offset, as in Kafka, up
to the high watermark, which includes records still only in the WAL; `-1`
means the high watermark, and an offset past it is `OFFSET_OUT_OF_RANGE` (1).
A fetch below the log start is out of range; a fetch at it may receive the
whole batch it falls in, as with Kafka, and the client skips the earlier
records. Storage is reclaimed a whole object at a time by the sweep.
Metadata creates a missing topic it is asked about when
`CAMUS_AUTO_CREATE_TOPICS` (Kafka's `auto.create.topics.enable`, default
`true`) is on, the request allows it, and the caller has `CREATE` on the
cluster or the topic; it has `CAMUS_NUM_PARTITIONS` partitions (default 1)
and the first answer is `LEADER_NOT_AVAILABLE` (5). Topic names follow
Kafka's rules (1–249 of `[a-zA-Z0-9._-]`, not `.` or `..`, no collision when
`.` and `_` are treated alike); others are `INVALID_TOPIC_EXCEPTION` (17). `cleanup.policy=compact` keeps the
latest record for each key at its original offset. Compaction runs every
`CAMUS_COMPACT_MS` (default 300000; `0` turns it off) over the leading sealed
extents whose newest record is at least `min.compaction.lag.ms` old; the
unsealed tail is left alone. A tombstone whose timestamp is older than
`delete.retention.ms` (default 86400000) is removed with its key. Kafka starts
that clock at the tombstone's first cleaning, so Camus can remove one sooner
than Kafka would. A compacted range with nothing left keeps its offsets and the
log start does not move, as in Kafka. A
record with no key is `INVALID_RECORD` (87), as in Kafka. `CreatePartitions`
and `CreateTopics` reject an explicit replica assignment. A classic group that still has members refuses `OffsetDelete` with
`GROUP_SUBSCRIBED_TO_TOPIC` (86).
JoinGroup records the client host as `/<ip>`, as Kafka does.

Every implemented API is authorized as Kafka's `KafkaApis` does it, with the
same resource, operation, and error: `TOPIC_AUTHORIZATION_FAILED` (29),
`GROUP_AUTHORIZATION_FAILED` (30), `CLUSTER_AUTHORIZATION_FAILED` (31), or
`TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53). A matching `DENY` wins; an
`ALLOW` must match the principal and the client's IP, or `*`; `READ`, `WRITE`,
`DELETE`, and `ALTER` imply `DESCRIBE`, and `ALTER_CONFIGS` implies
`DESCRIBE_CONFIGS`. A resource no ACL names follows
`CAMUS_ACL_ALLOW_IF_NO_ACL` (Kafka's `allow.everyone.if.no.acl.found`), which
defaults to `false` when SASL is required and `true` on the dev PLAINTEXT
listener, where every client is `User:ANONYMOUS`. `CAMUS_SUPER_USERS` is
Kafka's `super.users`, a `;`-separated principal list; the SASL user from
`CAMUS_SASL_USER` is always a super user, so a new cluster can create its
first ACLs. SASL/SCRAM-SHA-256 and SCRAM-SHA-512 users are created and
removed with `AlterUserScramCredentials` and listed with
`DescribeUserScramCredentials` (cluster `ALTER` and `DESCRIBE`); metadata keeps
the salt, iterations (4096–16384), `StoredKey`, and `ServerKey`, never the
password. With `--tls-client-ca`, the TLS listener requires a client
certificate from that CA and, as Kafka's SSL listener, takes the principal
from its subject, `User:<RFC 2253 DN>` in the order Java prints it; SASL is then
off unless `--require-sasl` is set. Authenticated listeners deny a resource no
ACL names by default. A broker serving TLS refuses to start unless `CAMUS_S3_ENDPOINT`,
`CAMUS_S3_BUCKET`, `CAMUS_S3_ACCESS_KEY`, and `CAMUS_S3_SECRET_KEY` are set;
the MinIO defaults apply only with `--dev-plaintext`.

Topic configs are the same for `CreateTopics`, `AlterConfigs`, and
`IncrementalAlterConfigs`, and are listed in `crates/metadata/src/config.rs`
with their Kafka 4.4.0 defaults. `DescribeConfigs` returns every one with its
source.

| Configs | Camus |
| --- | --- |
| `cleanup.policy`, `retention.ms`, `retention.bytes`, `max.message.bytes`, `delete.retention.ms`, `min.compaction.lag.ms` | Stored and applied |
| `durability.mode` | Camus only. Set at creation, then read-only |
| `segment.*`, `flush.*`, `min.insync.replicas`, `index.interval.bytes`, `file.delete.delay.ms`, `preallocate`, `local.retention.*`, `max.compaction.lag.ms`, `min.cleanable.dirty.ratio`, `message.timestamp.before.max.ms`, `message.timestamp.after.max.ms`, `*.replication.throttled.replicas` | Accepted and stored, with no effect |
| `message.timestamp.type=LogAppendTime`, `compression.type` other than `producer`, `unclean.leader.election.enable=true`, `remote.storage.enable=true`, any other name | `INVALID_CONFIG` (40) with a message |

Legacy `AlterConfigs` replaces the topic's whole config set, so a config it
does not name returns to its default, as in Kafka. `max.message.bytes`
defaults to `CAMUS_MESSAGE_MAX_BYTES`, which defaults to Kafka's 1048588, and
cannot exceed 8388608. CreateTopics accepts `num_partitions=-1` (one
partition) and a `replication_factor` of -1 or up to the broker count;
partitions are replicated to every WAL shard voter either way.
`retention.ms` and `retention.bytes` are enforced on topics whose
`cleanup.policy` includes `delete`, every `CAMUS_RETENTION_CHECK_MS`
(default 300000, Kafka's `log.retention.check.interval.ms`). As Kafka works
in whole segments and never deletes the active one, Camus moves the log start
past whole sealed extents and never touches the unsealed WAL tail. Time uses
each extent's newest record timestamp. Size counts sealed bytes only.

Objects that no extent references any more, after retention, `DeleteRecords`,
compaction, or a topic delete, are deleted by an orphan sweep on the metadata
leader every `CAMUS_SWEEP_MS` (default 300000). An object is swept only when it
is older than `CAMUS_SWEEP_MIN_AGE_MS` (default 3600000, ADR 0007), so an upload
still waiting for its metadata commit is safe, and has been unreferenced for
`CAMUS_FILE_DELETE_DELAY_MS` (default 60000, Kafka's `file.delete.delay.ms`).
The sweep owns every `bundle/` object under the cluster's key prefix: give each
cluster its own bucket or its own `CAMUS_S3_PREFIX`.

Camus writes no transaction control records: `read_committed` filtering is
done by the broker from the committed and aborted offset ranges. Kafka
clients do not need the markers, and a `read_committed` consumer that seeks
into an aborted range gets the next committed record at its own offset. A
`read_uncommitted` consumer sees aborted records, as in Kafka. A tool that
reads the raw log expecting control batches, or counts them, will not find
any.

Transactions follow Kafka's coordinator timers. `InitProducerId` with a
`transaction_timeout_ms` of 0 or less, or above `CAMUS_TXN_MAX_TIMEOUT_MS`
(default 900000, Kafka's `transaction.max.timeout.ms`), is
`INVALID_TRANSACTION_TIMEOUT` (50). The metadata leader aborts a transaction
that has been ongoing longer than its timeout, checking every
`CAMUS_TXN_ABORT_INTERVAL_MS` (default 10000), and bumps the producer epoch,
so the old producer gets `PRODUCER_FENCED` (90) and may recover with
`InitProducerId` from the epoch it had (KIP-360). A transactional id that has
not changed for `CAMUS_TRANSACTIONAL_ID_EXPIRATION_MS` (default 604800000) is
removed, checked every `CAMUS_TXN_CLEANUP_INTERVAL_MS` (default 3600000).
Idempotent producer state for a partition is removed once the log start has
passed the producer's last batch, or once the sealed extent holding that batch
is older than `CAMUS_PRODUCER_ID_EXPIRATION_MS` (default 86400000), checked
every `CAMUS_PRODUCER_ID_CHECK_INTERVAL_MS` (default 600000). Kafka uses the
batch's own newest timestamp; Camus uses the extent's, which is never older.
The `producer/{id}` epoch row of an idempotent producer is kept.
DescribeLogDirs reports one log directory per broker, `s3://<bucket>/<prefix>`,
holding the partitions that broker leads, sized by their sealed bytes; total
and usable bytes are unknown (-1). DescribeProducers lists producers whose
sequence state is sealed in metadata, with `last_timestamp` and
`coordinator_epoch` of -1. ConsumerGroupDescribe answers for KIP-848 groups;
a classic or unknown group is `GROUP_ID_NOT_FOUND` (69).
AlterPartitionReassignments, ListPartitionReassignments, and
AlterReplicaLogDirs stay unimplemented: a partition's replicas are the voters
of its WAL shard, not a placement that can be moved on its own. Client quotas
stay unimplemented; throttling is internal and there is no byte-rate quota
API. Delegation tokens, client telemetry, and UpdateFeatures stay
unimplemented.
Unimplemented keys are absent from `ApiVersions`.
A client that sends a known key at a version the schema can encode receives
`UNSUPPORTED_VERSION` (35), and the connection stays open. A version the
schema cannot encode, or a key the schema does not name, still closes that
connection because there is no response body to put `INVALID_REQUEST` (42) in.

Tested-client columns are filled when a real client has passed the phase
acceptance test. `-` means not run.

| API | Key | Phase | Versions (intended) | Status | Java | librdkafka | franz-go |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Produce | 0 | 1 | 3-13 | implemented | pass | pass | pass |
| Fetch | 1 | 1 | 4-18 | implemented | pass | pass | pass |
| ListOffsets | 2 | 1 | 1-11 | implemented | pass | pass | pass |
| Metadata | 3 | 1 | 0-13 | implemented | pass | pass | pass |
| OffsetCommit | 8 | 3 | 2-10 | implemented | pass | pass | pass |
| OffsetFetch | 9 | 3 | 1-10 | implemented | pass | pass | pass |
| FindCoordinator | 10 | 3 | 0-6 | implemented | pass | pass | pass |
| JoinGroup | 11 | 3 | 0-9 | implemented | pass | pass | pass |
| Heartbeat | 12 | 3 | 0-4 | implemented | pass | pass | pass |
| LeaveGroup | 13 | 3 | 0-5 | implemented | pass | pass | pass |
| SyncGroup | 14 | 3 | 0-5 | implemented | pass | pass | pass |
| DescribeGroups | 15 | 3 | 0-6 | implemented | pass | pass | pass |
| ListGroups | 16 | 3 | 0-5 | implemented | pass | pass | pass |
| SaslHandshake | 17 | 1 | 0-1 | implemented (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) | pass | pass | pass |
| ApiVersions | 18 | 1 | 0-5 | implemented | pass | pass | pass |
| CreateTopics | 19 | 1 | 2-7 | implemented | pass | pass | pass |
| DeleteTopics | 20 | 1 | 1-6 | implemented | pass | pass | pass |
| DeleteRecords | 21 | 4 | 0-2 | implemented | pass | pass | pass |
| InitProducerId | 22 | 4 | 0-4 | implemented | pass | pass | pass |
| OffsetForLeaderEpoch | 23 | | 3-4 | implemented | - | - | - |
| AddPartitionsToTxn | 24 | 5 | 0-3 | implemented | pass | pass | pass |
| AddOffsetsToTxn | 25 | 5 | 0-3 | implemented | pass | pass | - |
| EndTxn | 26 | 5 | 0-3 | implemented | pass | pass | pass |
| WriteTxnMarkers | 27 | | broker-internal, not a client API | unimplemented | - | - | - |
| TxnOffsetCommit | 28 | 5 | 0-3 | implemented | pass | pass | - |
| DescribeAcls | 29 | 5 | 1-3 | implemented | pass | pass | pass |
| CreateAcls | 30 | 5 | 1-3 | implemented | pass | pass | pass |
| DeleteAcls | 31 | 5 | 1-3 | implemented | pass | pass | pass |
| DescribeConfigs | 32 | 1 | 1-4 | implemented | pass | pass | pass |
| AlterConfigs | 33 | 4 | 0-2 | implemented | - | pass | pass |
| AlterReplicaLogDirs | 34 | | not planned | unimplemented | - | - | - |
| DescribeLogDirs | 35 | | 1-4 | implemented | pass | - | - |
| SaslAuthenticate | 36 | 1 | 0-2 | implemented (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) | pass | pass | pass |
| CreatePartitions | 37 | 4 | 0-3 | implemented | pass | pass | pass |
| CreateDelegationToken | 38 | | not planned | unimplemented | - | - | - |
| RenewDelegationToken | 39 | | not planned | unimplemented | - | - | - |
| ExpireDelegationToken | 40 | | not planned | unimplemented | - | - | - |
| DescribeDelegationToken | 41 | | not planned | unimplemented | - | - | - |
| DeleteGroups | 42 | 3 | 0-3 | implemented | pass | pass | pass |
| ElectLeaders | 43 | | 0-2 (`PREFERRED`; `UNCLEAN` answers 83) | implemented | pass | - | - |
| IncrementalAlterConfigs | 44 | 4 | 0-1 | implemented | pass | pass | pass |
| AlterPartitionReassignments | 45 | | not planned | unimplemented | - | - | - |
| ListPartitionReassignments | 46 | | not planned | unimplemented | - | - | - |
| OffsetDelete | 47 | 4 | 0 | implemented | pass | - | pass |
| DescribeClientQuotas | 48 | | throttle is internal; API not planned for 0–5 | unimplemented | - | - | - |
| AlterClientQuotas | 49 | | not planned | unimplemented | - | - | - |
| DescribeUserScramCredentials | 50 | | 0 | implemented | - | - | - |
| AlterUserScramCredentials | 51 | | 0 | implemented | - | - | - |
| DescribeQuorum | 55 | | not planned (KRaft-specific) | unimplemented | - | - | - |
| UpdateFeatures | 57 | | not planned | unimplemented | - | - | - |
| Envelope | 58 | | not planned | unimplemented | - | - | - |
| DescribeCluster | 60 | 4 | 0-2 | implemented | pass | pass | - |
| DescribeProducers | 61 | | 0 | implemented | pass | - | - |
| UnregisterBroker | 64 | | not planned | unimplemented | - | - | - |
| DescribeTransactions | 65 | 5 | 0 | implemented | pass | - | pass |
| ListTransactions | 66 | 5 | 0-2 | implemented | pass | - | pass |
| AllocateProducerIds | 67 | | broker-internal | unimplemented | - | - | - |
| ConsumerGroupHeartbeat | 68 | 3 | 0-1 | implemented | pass | - | - |
| ConsumerGroupDescribe | 69 | 3 | 0-1 | implemented | pass | - | - |
| GetTelemetrySubscriptions | 71 | | not planned | unimplemented | - | - | - |
| PushTelemetry | 72 | | not planned | unimplemented | - | - | - |
| ListClientMetricsResources | 74 | | not planned | unimplemented | - | - | - |
| DescribeTopicPartitions | 75 | | 0 | implemented | pass | - | - |

Version numbers are filled in when the handler lands, from the generated
4.4.0 schema, capped at what the handler implements. "TBD" means the range
has not been chosen and must not be advertised.

APIs added after 4.1 that the generator does not know until regeneration are
still listed here once Phase 0 diffs the schema directory. If this table
misses a key that 4.4.0 advertises, Phase 0 adds the row as unimplemented.

## Clients

| Client | Library | Phase first required |
| --- | --- | --- |
| Java | `kafka-clients` 4.4.0, SASL_SSL, [CI `test`](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ci.yml) | 1 |
| Python / librdkafka | `confluent-kafka` 2.16.1, SASL_SSL, [CI `test`](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ci.yml) | 1 |
| Go | `franz-go` v1.22.0, SASL_SSL, [CI `test`](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ci.yml) | 1 |
| kcat | CLI against the dev listener | 1, smoke |
| KafkaJS 2 | round trip with a consumer group, in the ecosystem harness | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| confluent-kafka-go 2.11.1 | round trip with a consumer group, in the ecosystem harness | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| confluent-kafka-dotnet 2.11.1 | round trip with a consumer group, in the ecosystem harness | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| Apache Kafka 4.4.0 CLI tools | topics, configs (topic and broker), console clients, consumer groups, offsets, delete-records, log dirs, ACLs, transactions, describe-producers, preferred leader election, API versions | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| Kafka Connect 4.4.0, distributed | FileStream source and sink, compacted internal topics, resume after restart | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| Kafka Streams 4.4.0 | word count with `exactly_once_v2`, repartition and changelog topics | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| MirrorMaker 2 4.4.0 | Apache Kafka 4.4.0 to Camus, with checkpoints and heartbeats | [nightly](https://github.com/nidheesh-m-vakharia/camus/actions/workflows/ecosystem.yml) |
| Schema Registry | in the harness behind `CAMUS_ECOSYSTEM_DOCKER=1` | not run here |
| kafka-python | tracked; not in the harness | - |

The ecosystem harness (`crates/broker/tests/ecosystem.rs`) found and fixed
two gaps: DescribeConfigs did not answer for the broker resource, and a
ListOffsets v1-3 response carried a leader epoch those versions cannot
encode, closing the connection. DescribeConfigs for a broker returns that
broker's settings under Kafka's names, read-only; dynamic broker configs
are not supported. `--advertised-addr` sets what Metadata hands out.

## Compression

| Codec | Phase 1 target |
| --- | --- |
| none | pass-through |
| gzip | pass-through |
| snappy | pass-through |
| lz4 | pass-through |
| zstd | pass-through |

Pass-through means the CRC-covered bytes are unchanged (ADR 0009).

On 2026-11-05, three clients passed that check over SASL_SSL. Each creates a
topic, produces with `acks=all` and idempotence disabled, and fetches by
assigning the partition. None of them join a consumer group.

| Client | Program |
| --- | --- |
| `kafka-clients` 4.4.0 | `crates/broker/tests/java/SaslRoundTrip.java` |
| `confluent-kafka` 2.16.1 (librdkafka) | `crates/broker/tests/python/sasl_roundtrip.py` |
| `franz-go` v1.22.0 | `crates/broker/tests/go/sasl_roundtrip.go` |

`kafka-clients` 4.4.0, librdkafka, and franz-go each run a transaction
(`TxnRoundTrip`, `phase5_roundtrip.py`, `gophase5`): an open batch stays
hidden from `read_committed`, and an abort stays hidden. The Java and
librdkafka runs also commit the group offset in the same transaction.
The same three clients produce a compacted log and see an ACL denial.
The same three clients also run the admin path in `clients_run_the_admin_apis`:
describe and alter topic config, create partitions, delete records, delete
the topic, and delete the ACL they created. Java and librdkafka call
`DescribeCluster`. Java and franz-go call `OffsetDelete`. librdkafka and
franz-go call non-incremental `AlterConfigs`; the Java 4.4 admin client has
no such method. Java and franz-go call `DescribeTransactions` and
`ListTransactions`. librdkafka has no transaction-admin API and no
`OffsetDelete` method in the installed client. franz-go has no
`DescribeCluster` method.

Produce requests at version 13 and above name the topic by id. The Python and
Go tests skip when their toolchain is not installed under `target/`.
