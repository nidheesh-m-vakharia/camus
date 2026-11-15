# ADR 0016: Derived views over the log

Status: Proposed
Date: 2026-11-14

## Context

Issue #153 asks whether Camus should go past the broker: keep the log as
the source of truth and serve tables, a graph, and other query shapes as
views derived from it. Building Vennbee showed how hard it is for a small
team to find a graph database that fits, and running a broker next to a
database means two logs that have to agree.

The idea is not new. Jay Kreps described the log as the core abstraction
in "The Log" (2013), and Martin Kleppmann's "Turning the Database Inside
Out" (2014) is this proposal almost word for word. Systems that build it:

| System | What it derives from a log |
| --- | --- |
| Kafka Streams, ksqlDB | Tables in local RocksDB, rebuilt from changelog topics |
| Materialize, RisingWave, Feldera | Incrementally maintained SQL views |
| Rama (Red Planet Labs) | Arbitrary indexes ("PStates") over logs ("depots"), including key-value, document, and graph shapes |
| Confluent Tableflow, Redpanda Iceberg Topics, WarpStream, Bufstream | Topics also exposed as Iceberg tables in object storage |
| Datomic, XTDB | Indexes over an immutable log of facts |

Camus also already works with the usual answer. Kafka Connect 4.4.0 passes
against it (`docs/compatibility.md`), so a sink into Postgres, Neo4j, or
Qdrant works today. A native view has to be worth more than that sink. What
it can offer that the sink cannot:

- Read-your-writes across the log and its views, by offset.
- Views rebuilt from R2 with no Connect workers, no changelog topics, and
  no second database to operate.
- Key-value, document, graph, and vector shapes from one binary.

Two facts from the current code shape the design:

- `cleanup.policy=compact` (Phase 5) keeps the latest record per key at
  extent granularity. A compacted log can still hold several records for a
  key. Compaction bounds the cost of a rebuild. It does not do the view's
  work.
- ADR 0010 already treats the disk cache as disposable: it "is not a
  second source of truth. It may be deleted at any time." A view is the
  same rule one level up.

## Decision (proposed)

### What a view is

A view is a deterministic fold over the partitions of one or more topics:

```
state(p, n) = fold(state(p, n-1), record(p, n))
```

Each view partition has a checkpoint, the next offset it will apply. Every
index kind below is a fold with a different state type. The runtime that
reads, decodes, applies, and checkpoints is shared by all of them.

Invariants every view kind keeps:

1. **Determinism.** View state is a function of the records below the
   checkpoint and nothing else: no wall clock, no randomness, no reads of
   other views. Deleting a view's directory and rebuilding from R2 yields
   the same state. Each kind has a test for that.
2. **Checkpoint commits with state.** The offset and the index change are
   written in the same `redb` transaction (ADR 0011). A crash never applies
   a record twice or skips one.
3. **Only committed data.** A view reads at `read_committed`, below the
   last stable offset. An aborted transaction never reaches a view.
4. **Off the produce path.** Views are consumers. A slow or failed view
   never throttles produce, never holds back upload, and never blocks
   retention.
5. **Bounded memory.** Index state lives on disk. A view kind whose usual
   implementation is in memory (vector search) must use an on-disk variant.
6. **Retention is respected.** If retention removes offsets a view has not
   applied yet, the view stops and reports `OFFSET_OUT_OF_RANGE` instead
   of skipping data. Rebuilding a view needs the topic's full history, so
   source topics are compacted or have unbounded retention. Creating a view
   on a topic without either is refused.

### Where views run

A `view` role in the one binary, enabled by config like `wal` (ADR 0001).
A view worker reads through the same fetch path a consumer uses, from the
cache or R2 for sealed data and from the shard leader for the tail. It does
not join a Raft group. View partitions are assigned to brokers the way a
consumer group assigns partitions, and a reassigned partition is rebuilt on
its new broker from the log.

View definitions live in metadata, so every broker agrees on them. Index
data stays on local disk in V1. Snapshots of view state to R2 are added
only if measured rebuild time requires them, and would be write-once keys
under `view/` (ADR 0007).

### Definitions, not user code

A view is declared: the source topics, a decoder, the key, and the indexed
fields. User code (a Streams-style topology, WASM) is out of scope.

Kafka values are opaque bytes, so each view names a decoder. V1 supports
JSON. Avro and Protobuf come with a schema-registry-compatible API, in a
separate ADR. A record the decoder rejects stops that view partition with
an error that names the offset. It is not skipped.

### Partitioning and queries

View state is partitioned like its source topic. A query by key goes to
one view partition. Other queries fan out to every partition and merge.

### Consistency

A produce response carries `(partition, offset)`. A view query may pass an
offset per partition and waits until the view's checkpoint is past it, up
to a timeout. Without a fence a query reads the view as it is. That fence
is the read-your-writes guarantee a sink into another database cannot give.

### Order of work

Each step reuses the runtime and storage of the one before. Each has an
acceptance test, as in ADR 0013.

| Step | View | Acceptance |
| --- | --- | --- |
| V1 | **Key-value table.** Latest value per key, values stored in `redb`. A tombstone deletes the key. Served over HTTP. | Point reads match a reference fold over the same records. Delete-and-rebuild gives the same state. The offset fence returns a value written just before. A view on a topic with finite retention and no compaction is refused. |
| V2 | **Secondary indexes** on decoded JSON fields, equality and range. This makes V1 a document store. | Indexed queries match a full scan on randomized data, including updates and tombstones. |
| V3 | **SQL.** See "Not decided here". | The chosen engine's answers match Postgres on the same data for an agreed query set. |
| V4 | **Graph.** An adjacency view from edge records keyed by source node, so out-edges are in one partition. Neighbors, k-hop, and shortest path over HTTP. No Cypher or Gremlin. | Traversals match a reference graph library on generated graphs, including edge deletes. |
| V5 | **Vector.** Embeddings arrive in the record; Camus does not compute them. On-disk approximate nearest-neighbor index per view partition, top-k merged across partitions. | Recall@10 above an agreed target against brute force. Peak memory stays flat as the index grows. |

Postgres CDC (logical replication into topics, from #153) is ingest, not a
view. It is independent of V1–V5 and gets its own ADR. Its acceptance test
is that killing the connector mid-stream loses and duplicates no rows,
because the replication slot position and the produced offsets commit
together.

### Priority

The broker stays first. V1 starts after the head-to-head benchmark (#136)
and the networked R2 benchmark (#141) are done.

## Consequences

- One runtime carries every view kind, so the invariants are tested once
  in the runtime and once per fold.
- Brokers running the `view` role need disk for index state on top of the
  WAL and the cache. That is a new capacity number to document per view kind.
- Views can lag the log. The fence makes the lag visible to a caller that
  cares, and a metric reports each view partition's lag.
- Source topics for views must be compacted or retained forever. Retention
  stays a topic config. The view refuses a topic that could lose its
  history.
- Cross-partition graph traversals take one fan-out per hop. Graphs whose
  queries are mostly multi-hop across partitions will be slower than in a
  single-node graph database.

## Alternatives

- **Document Kafka Connect sinks and stop there.** It works today and costs
  nothing to build. Rejected as the whole answer, because it gives no
  read-your-writes and keeps a second system to run. It stays the
  recommendation for anyone who already runs that database.
- **User code for views (Streams-style or WASM).** More general. Rejected
  for now: it brings sandboxing, versioning, and non-determinism into the
  broker before the declared views show what is needed.
- **One external database as the view store.** Rejected. It adds the
  second failure domain ADR 0001 ruled out.
- **Graph as a separate product.** Possible later. The runtime here is what
  such a product would need first.

## Not decided here

- **How SQL is served.** Two options, not the same cost:
  1. Embed DataFusion with a table provider over the V1/V2 views and raw
     topics, and serve it over the Postgres wire protocol (`pgwire`), so
     `psql` and ORMs connect as is.
  2. Expose compacted topics as Iceberg tables in R2, as Tableflow and
     Redpanda do, and let DuckDB, Trino, or Spark query them. Camus stores
     Kafka record batches, not Parquet, so this needs a Parquet writer in
     the compactor and doubles stored bytes for those topics.
  Option 2 gets analytical SQL without a query engine in the broker. Option
  1 gets transactional point queries and the offset fence. This is decided
  before V3 starts, with numbers from V1 and V2.
- The Avro and Protobuf decoders and the schema-registry API.
- Which on-disk ANN index V5 uses.
- Snapshots of view state to R2.
