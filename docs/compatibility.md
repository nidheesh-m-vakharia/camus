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
`ApiVersions` request versions in the schema are 0–5. Phase 0 does not
advertise any key. A known key gets `UNSUPPORTED_VERSION` (35). An unknown
key gets `INVALID_REQUEST` (42). The broker process does not speak Kafka
TCP yet.

Kafka source is Apache-2.0. See `NOTICE`.

## Deliberate differences

These are accepted in the ADRs and apply once the relevant phase exists.
Until then the API returns an error rather than a partial imitation.

| Topic | Camus | Kafka 4.4.0 | ADR |
| --- | --- | --- | --- |
| `ApiVersions` contents | Only implemented keys and versions | Every key the broker implements | 0003 |
| `acks=1` | Same barrier as `acks=all`: WAL quorum fsync, or R2 put plus metadata commit | Leader-local ack | 0005 |
| Unclean leader election | Never automatic. Operator truncate only | Default off, can be enabled | 0008 |
| `DeleteRecords` midpoint | Advances to the next whole-object boundary | Can split a segment | 0010 |
| Idempotence | `InitProducerId` allocates a producer id. A retried sequence returns the original offset and is not appended | Supported | 0009 |
| Transactions | `read_committed` hides a batch until `EndTxn` commits its offset range. There is no control-batch marker | Control batches plus `read_committed` | 0013 |
| Compaction | Latest key, original offset, sealed extents only. Tombstones stay. No dirty-ratio cleaner | `UnifiedLog` cleaner, `delete.retention.ms` | 0010 |
| ACLs | A resource with no ACL is allowed. `DENY` beats `ALLOW`. Only host `*` matches. Produce and fetch return 29 | Default deny when no ACL exists. Any client host | 0012 |

`acks=0` matches Kafka: the broker does not wait, and the batch may be lost.

## API matrix

Status values: `unimplemented`, `partial`, `implemented`.

Phase 1 answers `ApiVersions`, `Metadata`, `Produce`, `Fetch`, `ListOffsets`,
`CreateTopics`, `DeleteTopics`, `DescribeConfigs`, `SaslHandshake`, and
`SaslAuthenticate`. `ApiVersions` advertises only those keys. Produce acks
in `r2` mode after the object is stored and the extent is committed.
`wal` produce acks after the WAL shard commits, which means a majority of
its voters has fsynced the batch. Every broker is a voter. Metadata reports
the shard leader as the leader of each `wal` partition and the voters as
replicas and ISR. A broker that is not the shard leader answers produce,
fetch, and list-offsets with `NOT_LEADER_OR_FOLLOWER` (6). A produce that
cannot commit within `timeout_ms`, for example with no quorum, gets
`REQUEST_TIMED_OUT` (7); the entry may still commit later, so a client
without idempotence can duplicate on retry. The upload to object storage
follows the ack. The partition `leader_epoch` does not yet change with the
shard term. The shard leader records itself in metadata so every broker
names the same leader; a broker that has just restarted can name a stale
leader for about a second, and clients recover through the usual
`NOT_LEADER_OR_FOLLOWER` retry. After a shard loses a majority, its
partitions stay offline until a lost voter returns or an operator runs
`camus admin truncate-to-sealed`, which resumes them at the sealed end and
drops acknowledged offsets past it (ADR 0008).
A topic created without `durability.mode` is `wal` (`CAMUS_DEFAULT_DURABILITY`
changes the cluster default). When unsealed WAL bytes pass `wal.disk.limit`
(`CAMUS_WAL_DISK_LIMIT`, default 8 GiB), produce is held and the response's
`throttle_time_ms` is the time held; it is answered with `REQUEST_TIMED_OUT`
only when the client's `timeout_ms` runs out, and a held batch is not
appended, so nothing acknowledged is lost. Kafka 4.4.0 answers a quota
breach at once and lets the client wait out `throttle_time_ms`; Camus holds
first, so a client that also honors the throttle waits twice. A failed WAL
write (for example a full disk) answers `KAFKA_STORAGE_ERROR` (56) and stops
that broker's shard until restart.

Consumer groups use the classic protocol. The metadata leader coordinates
every group; another broker answers group requests with
`NOT_COORDINATOR` (16) and clients find the coordinator again. Each new
generation and each assignment is committed through the metadata group
before members hear of it, and an offset commit is checked against the
stored generation when it is applied, so a failover keeps the generation
and an old generation cannot commit. `FindCoordinator` serves groups and
transactions. Share groups answer `INVALID_REQUEST` (42).
`ConsumerGroupHeartbeat` (KIP-848) versions 0–1 are implemented. The
broker's default assignor is `uniform` (round-robin per topic; not Kafka's
sticky uniform assignor) and `range` is the other supported name. A member
that names any other assignor gets `UNSUPPORTED_ASSIGNOR` (112). Topic
regexes are Rust regexes. An offset commit is accepted at the member's
current epoch or the previous one. The Java client's default remains
`group.protocol=classic`. `group.protocol=consumer` is exercised by
`GroupConsumerRoundTrip`.
`DeleteRecords` moves the log start to the next whole-object boundary at or
after the requested offset, and the response low watermark is that offset.
An offset past the sealed end is `OFFSET_OUT_OF_RANGE` (1), including
records that are still only in the WAL. `cleanup.policy=compact` keeps the
latest record for each key at its original offset. Compaction runs on
sealed extents when `CAMUS_COMPACT_MS` is set; the unsealed tail is left
alone, tombstones are kept (`delete.retention.ms` is not applied), and a
record with no key is `INVALID_RECORD` (87), as in Kafka. `CreatePartitions`
and `CreateTopics` reject an explicit replica assignment. A classic group that still has members refuses `OffsetDelete` with
`GROUP_SUBSCRIBED_TO_TOPIC` (86).
JoinGroup reports the client host as `/unknown`.

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
| SaslHandshake | 17 | 1 | 0-1 | implemented (PLAIN) | pass | pass | pass |
| ApiVersions | 18 | 1 | 0-5 | implemented | pass | pass | pass |
| CreateTopics | 19 | 1 | 2-7 | implemented | pass | pass | pass |
| DeleteTopics | 20 | 1 | 1-6 | implemented | pass | pass | pass |
| DeleteRecords | 21 | 4 | 0-2 | implemented | pass | pass | pass |
| InitProducerId | 22 | 4 | 0-4 | implemented | pass | - | - |
| OffsetForLeaderEpoch | 23 | | not planned for phases 0–5 | unimplemented | - | - | - |
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
| DescribeLogDirs | 35 | | not planned | unimplemented | - | - | - |
| SaslAuthenticate | 36 | 1 | 0-2 | implemented (PLAIN) | pass | pass | pass |
| CreatePartitions | 37 | 4 | 0-3 | implemented | pass | pass | pass |
| CreateDelegationToken | 38 | | not planned | unimplemented | - | - | - |
| RenewDelegationToken | 39 | | not planned | unimplemented | - | - | - |
| ExpireDelegationToken | 40 | | not planned | unimplemented | - | - | - |
| DescribeDelegationToken | 41 | | not planned | unimplemented | - | - | - |
| DeleteGroups | 42 | 3 | 0-3 | implemented | pass | pass | pass |
| ElectLeaders | 43 | | internal leader changes follow Raft; API not planned | unimplemented | - | - | - |
| IncrementalAlterConfigs | 44 | 4 | 0-1 | implemented | pass | pass | pass |
| AlterPartitionReassignments | 45 | | not planned | unimplemented | - | - | - |
| ListPartitionReassignments | 46 | | not planned | unimplemented | - | - | - |
| OffsetDelete | 47 | 4 | 0 | implemented | pass | - | pass |
| DescribeClientQuotas | 48 | | throttle is internal; API not planned for 0–5 | unimplemented | - | - | - |
| AlterClientQuotas | 49 | | not planned | unimplemented | - | - | - |
| DescribeUserScramCredentials | 50 | | SCRAM not planned | unimplemented | - | - | - |
| AlterUserScramCredentials | 51 | | not planned | unimplemented | - | - | - |
| DescribeQuorum | 55 | | not planned (KRaft-specific) | unimplemented | - | - | - |
| UpdateFeatures | 57 | | not planned | unimplemented | - | - | - |
| Envelope | 58 | | not planned | unimplemented | - | - | - |
| DescribeCluster | 60 | 4 | 0-2 | implemented | pass | pass | - |
| DescribeProducers | 61 | | not planned before phase 5, then reconsider | unimplemented | - | - | - |
| UnregisterBroker | 64 | | not planned | unimplemented | - | - | - |
| DescribeTransactions | 65 | 5 | 0 | implemented | pass | - | pass |
| ListTransactions | 66 | 5 | 0-2 | implemented | pass | - | pass |
| AllocateProducerIds | 67 | | broker-internal | unimplemented | - | - | - |
| ConsumerGroupHeartbeat | 68 | 3 | 0-1 | implemented | pass | - | - |
| ConsumerGroupDescribe | 69 | 3 | TBD | unimplemented | - | - | - |
| GetTelemetrySubscriptions | 71 | | not planned | unimplemented | - | - | - |
| PushTelemetry | 72 | | not planned | unimplemented | - | - | - |
| ListClientMetricsResources | 74 | | not planned | unimplemented | - | - | - |
| DescribeTopicPartitions | 75 | | not planned for 0–5 | unimplemented | - | - | - |

Version numbers are filled in when the handler lands, from the generated
4.4.0 schema, capped at what the handler implements. "TBD" means the range
has not been chosen and must not be advertised.

APIs added after 4.1 that the generator does not know until regeneration are
still listed here once Phase 0 diffs the schema directory. If this table
misses a key that 4.4.0 advertises, Phase 0 adds the row as unimplemented.

## Clients

| Client | Library | Phase first required |
| --- | --- | --- |
| Java | `kafka-clients` 4.4.0, SASL_SSL, passed | 1 |
| Python / librdkafka | `confluent-kafka` 2.16.1, SASL_SSL, passed | 1 |
| Go | `franz-go` v1.22.0, SASL_SSL, passed | 1 |
| kcat | CLI against the dev listener | 1, smoke |
| KafkaJS, kafka-python, confluent-kafka-go, confluent-kafka-dotnet | tracked, not in the first harness | 1 if time, else follow-up |

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
`DescribeCluster` method. Compaction still runs only on sealed extents when
`CAMUS_COMPACT_MS` is set, and an ACL host other than `*` does not match.

Produce requests at version 13 and above name the topic by id. The Python and
Go tests skip when their toolchain is not installed under `target/`.
