# ADR 0001: System architecture

Status: Proposed
Date: 2026-11-05

## Context

Camus has to look like one Kafka cluster to clients and like a small set of
stateless-as-possible Rust processes to operators. The only required external
system is object storage. Durability and low latency pull in opposite
directions, so the architecture has two write paths that share one read path
and one metadata log.

## Decision

One binary, `camus-broker`, runs every role. Roles are enabled by config, not
by separate services.

| Crate | Responsibility |
| --- | --- |
| `protocol` | TCP framing, generated Kafka messages, error codes, record-batch primitives that do not interpret compressed payloads |
| `broker` | Accept loop, request dispatch, throttling, the metrics endpoint |
| `raft` | openraft integration, RPC between broker processes, log and state-machine traits |
| `metadata` | The single cluster Raft group and its state machine |
| `wal` | Per-shard Raft groups, used only in `wal` mode |
| `storage` | R2/S3 client, on-disk read cache |
| `uploader` | Seal committed batches into R2 objects and publish the offset map |
| `coordinator` | Consumer groups, then transactions. Empty until Phase 3 |
| `compactor` | Object merge and retention. Empty until it has data to merge |
| `cli` | Dev and operator commands (`camus admin …`) |

Request path:

1. Read a length-prefixed Kafka frame.
2. Decode with the generated schema for that API key and version.
3. If the key or version is not implemented, return the error in ADR 0003
   and leave the connection up.
4. Otherwise handle it. Produce and fetch fan out by durability mode.
5. Encode the response with the same version the client asked for, when that
   version is implemented.

Metadata is the authority for topic existence, partition count, leader
identity, the offset-to-object index, group state, producer ids, and
transaction state. The WAL is not authoritative past the upload cursor.
R2 objects plus that index are authoritative for sealed offsets.

Brokers that are not the leader for a partition do not append to it. They
return `NOT_LEADER_OR_FOLLOWER` (error 6) for produce, and for fetch when
the client asked the leader only. Any broker may serve a follower fetch of
sealed data from R2. Tail fetches in `wal` mode are served by the shard
leader, which is the partition leader.

There is no separate gateway, no JVM sidecar, and no per-request service
discovery beyond the Kafka `Metadata` response.

## Consequences

- Operators deploy and upgrade one artifact.
- A bug in the metadata state machine affects the whole cluster. Snapshots
  and the R2 recovery path in ADR 0008 are the mitigation, not a second
  control plane.
- Crate boundaries are allowed to be simplified inside Phase 0 if two crates
  would otherwise be empty shells. The boundaries above are the target by
  the end of Phase 2, when each one has a real job.
- `coordinator` and `compactor` are created as crates in Phase 0 with no
  public success paths. Calling them is not possible from the broker until
  later phases wire them in.

## Alternatives

- A standalone metadata process. Rejected. It adds a second binary and a
  failure domain the brief ruled out.
- Implementing KRaft and reusing Kafka's Raft messages. Rejected. Clients
  do not speak KRaft. Camus's replication is an internal detail.
