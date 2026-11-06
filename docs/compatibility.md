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
| Idempotence before Phase 4 | `InitProducerId` is an error, so retries can duplicate if the client does not use idempotence | Supported | 0009 |

`acks=0` matches Kafka: the broker does not wait, and the batch may be lost.

## API matrix

Status values: `unimplemented`, `partial`, `implemented`.

Phase 1 answers `ApiVersions`, `Metadata`, `Produce`, `Fetch`, `ListOffsets`,
`CreateTopics`, `DeleteTopics`, `DescribeConfigs`, `SaslHandshake`, and
`SaslAuthenticate`. `ApiVersions` advertises only those keys. Produce acks
in `r2` mode after the object is stored and the extent is committed.
`wal` topics are stored and then rejected on produce until Phase 2.
Unimplemented keys are absent from `ApiVersions`.
A client that sends a known key at a version the schema can encode receives
`UNSUPPORTED_VERSION` (35), and the connection stays open. A version the
schema cannot encode, or a key the schema does not name, still closes that
connection because there is no response body to put `INVALID_REQUEST` (42) in.

Tested-client columns are filled when a real client has passed the phase
acceptance test. `-` means not run.

| API | Key | Phase | Versions (intended) | Status | Java | librdkafka | franz-go |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Produce | 0 | 1 | 3-13 | implemented (`r2` only) | pass | pass | pass |
| Fetch | 1 | 1 | 4-18 | implemented | pass | pass | pass |
| ListOffsets | 2 | 1 | 1-11 | implemented | pass | pass | pass |
| Metadata | 3 | 1 | 0-13 | implemented | pass | pass | pass |
| OffsetCommit | 8 | 3 | TBD | unimplemented | - | - | - |
| OffsetFetch | 9 | 3 | TBD | unimplemented | - | - | - |
| FindCoordinator | 10 | 3 | TBD | unimplemented | - | - | - |
| JoinGroup | 11 | 3 | TBD | unimplemented | - | - | - |
| Heartbeat | 12 | 3 | TBD | unimplemented | - | - | - |
| LeaveGroup | 13 | 3 | TBD | unimplemented | - | - | - |
| SyncGroup | 14 | 3 | TBD | unimplemented | - | - | - |
| DescribeGroups | 15 | 3 | TBD | unimplemented | - | - | - |
| ListGroups | 16 | 3 | TBD | unimplemented | - | - | - |
| SaslHandshake | 17 | 1 | 0-1 | implemented (PLAIN) | pass | pass | pass |
| ApiVersions | 18 | 1 | 0-5 | implemented | pass | pass | pass |
| CreateTopics | 19 | 1 | 2-7 | implemented | pass | pass | pass |
| DeleteTopics | 20 | 1 | 1-6 | implemented | - | - | - |
| DeleteRecords | 21 | 4 | TBD | unimplemented | - | - | - |
| InitProducerId | 22 | 4 | TBD | unimplemented | - | - | - |
| OffsetForLeaderEpoch | 23 | | not planned for phases 0–5 | unimplemented | - | - | - |
| AddPartitionsToTxn | 24 | 5 | TBD | unimplemented | - | - | - |
| AddOffsetsToTxn | 25 | 5 | TBD | unimplemented | - | - | - |
| EndTxn | 26 | 5 | TBD | unimplemented | - | - | - |
| WriteTxnMarkers | 27 | | broker-internal, not a client API | unimplemented | - | - | - |
| TxnOffsetCommit | 28 | 5 | TBD | unimplemented | - | - | - |
| DescribeAcls | 29 | 5 | TBD | unimplemented | - | - | - |
| CreateAcls | 30 | 5 | TBD | unimplemented | - | - | - |
| DeleteAcls | 31 | 5 | TBD | unimplemented | - | - | - |
| DescribeConfigs | 32 | 1 | 1-4 | implemented | - | - | - |
| AlterConfigs | 33 | 4 | TBD | unimplemented | - | - | - |
| AlterReplicaLogDirs | 34 | | not planned | unimplemented | - | - | - |
| DescribeLogDirs | 35 | | not planned | unimplemented | - | - | - |
| SaslAuthenticate | 36 | 1 | 0-2 | implemented (PLAIN) | pass | pass | pass |
| CreatePartitions | 37 | 4 | TBD | unimplemented | - | - | - |
| CreateDelegationToken | 38 | | not planned | unimplemented | - | - | - |
| RenewDelegationToken | 39 | | not planned | unimplemented | - | - | - |
| ExpireDelegationToken | 40 | | not planned | unimplemented | - | - | - |
| DescribeDelegationToken | 41 | | not planned | unimplemented | - | - | - |
| DeleteGroups | 42 | 3 | TBD | unimplemented | - | - | - |
| ElectLeaders | 43 | | internal leader changes follow Raft; API not planned | unimplemented | - | - | - |
| IncrementalAlterConfigs | 44 | 4 | TBD | unimplemented | - | - | - |
| AlterPartitionReassignments | 45 | | not planned | unimplemented | - | - | - |
| ListPartitionReassignments | 46 | | not planned | unimplemented | - | - | - |
| OffsetDelete | 47 | 4 | TBD | unimplemented | - | - | - |
| DescribeClientQuotas | 48 | | throttle is internal; API not planned for 0–5 | unimplemented | - | - | - |
| AlterClientQuotas | 49 | | not planned | unimplemented | - | - | - |
| DescribeUserScramCredentials | 50 | | SCRAM not planned | unimplemented | - | - | - |
| AlterUserScramCredentials | 51 | | not planned | unimplemented | - | - | - |
| DescribeQuorum | 55 | | not planned (KRaft-specific) | unimplemented | - | - | - |
| UpdateFeatures | 57 | | not planned | unimplemented | - | - | - |
| Envelope | 58 | | not planned | unimplemented | - | - | - |
| DescribeCluster | 60 | 4 | TBD | unimplemented | - | - | - |
| DescribeProducers | 61 | | not planned before phase 5, then reconsider | unimplemented | - | - | - |
| UnregisterBroker | 64 | | not planned | unimplemented | - | - | - |
| DescribeTransactions | 65 | 5 | TBD | unimplemented | - | - | - |
| ListTransactions | 66 | 5 | TBD | unimplemented | - | - | - |
| AllocateProducerIds | 67 | | broker-internal | unimplemented | - | - | - |
| ConsumerGroupHeartbeat | 68 | 3 | KIP-848, after classic groups | unimplemented | - | - | - |
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

Produce requests at version 13 and above name the topic by id. The Python and
Go tests skip when their toolchain is not installed under `target/`.
