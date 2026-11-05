# ADR 0011: Local log storage

Status: Proposed
Date: 2026-11-05

## Context

openraft needs a `RaftLogStorage` and a state-machine store on local disk.
The brief lists `redb`, `fjall`, or a custom append-only segment file, and
requires a benchmark before the choice is locked. This ADR writes the
benchmark and a hypothesis. It does not pick a winner. Phase 0 runs the
benchmark and appends the numbers. Implementation of the durable log format
waits on a one-line acceptance of that result.

What the log store must do:

- Append a batch of entries and fsync before returning. This is the `wal`
  ack barrier and the metadata durability barrier.
- Read by log index.
- Truncate a prefix after a snapshot and an upload (ADR 0006).
- Survive a crash between append and the process's next boot, including a
  crash during fsync.

What the state machine store must do:

- Point updates at apply time (extents, group offsets, last sequence).
- Snapshot to a byte blob and restore from one.
- Stay consistent with the log: no applied index without the entry that
  produced it.

p99 append latency matters more than scan throughput. A 20ms produce budget
(ADR 0005) has to cover a quorum of these fsyncs plus network inside one
region. A store that fsyncs in 15ms on an idle laptop will miss the target
under load.

## Decision

### Candidates

| Candidate | Why it is on the list |
| --- | --- |
| Custom append-only segments | Raft logs are append-only. A segment file plus a small index avoids LSM or B-tree write amplification on the fsync path. Truncation is dropping whole segments or punching a prefix. |
| `redb` | Single-file, transactional, simple operations. Comfortable for the state machine. Less obvious for a pure log. |
| `fjall` | LSM, high write throughput, more moving parts on the fsync/p99 path. |

No other engine is added unless this benchmark shows all three miss the
latency target. RocksDB is out of the first round to keep the dependency
tree smaller; it can enter later as a fourth column if needed.

### Benchmark protocol (Phase 0, before the format is locked)

Hardware note recorded with the results: machine, disk type (NVMe or SSD),
filesystem, and whether the disk is the OS volume. Dev laptops are allowed
for the first run. The comparison is between engines on the same box, not
a claim about production p99.

Workload, single thread, then 8 concurrent append tasks, 60 seconds each
after a 10 second warmup:

1. Append 1 KiB, fsync every append.
2. Append 64 KiB, fsync every append.
3. Append 1 MiB, fsync every append.
4. Group commit: append 32 KiB × 16, one fsync per group (this is the
   packed Raft entry shape from ADR 0006).

Report p50 and p99 latency of the fsync-returning call, and MiB/s. Crash
test, all three engines: kill -9 during append, reopen, and check the
prefix that reported success is still readable and a torn tail is ignored.

Pass bar for the **log** engine: p99 under 5ms for workload 4 on the test
NVMe, and a clean crash test. The 5ms figure leaves room for a second
replica's fsync and a network hop inside a 20ms budget. If none pass, the
budget or the engine list is revised in this ADR before coding the log.

Pass bar for the **state machine** engine: the crash test, plus apply of
10,000 small updates in under a second after the log fsync is excluded
(apply is not on the produce ack path for `wal`; the ack returns at log
commit, and apply follows). Ease of snapshotting counts. If `redb` is
within 2× of the fastest and the crash test is clean, prefer it for the
state machine for operational simplicity.

### Hypothesis, not a choice

Log: custom segments. State machine: `redb`. `fjall` is measured so the
hypothesis can lose.

Phase 0 may land the engines behind the `RaftLogStorage` trait with
in-memory storage as the default in unit tests, and run the benchmark as a
binary under `crates/raft/bench`. It may not write a production log format
that later has to be migrated, until the result is accepted.

### `unsafe`

The segment implementation should use `std::fs` and `File::sync_all`. If a
later optimization wants `mmap` or `O_DIRECT` and that requires `unsafe`,
the `unsafe` block needs a comment that says why safe Rust cannot express
it, and a test that would fail if the invariant broke. No `unsafe` in
Phase 0's first cut.

## Consequences

- The review can approve the rest of the design without pretending the
  engine choice is done.
- Phase 0's first coding tasks are still unblocked: protocol, MinIO,
  openraft on the in-memory store that openraft already ships for tests,
  and this benchmark.
- A wrong guess now would be expensive. A short benchmark is cheaper.

## Alternatives

- Picking `redb` for both in this ADR. Rejected until the numbers exist.
  The brief asked for the benchmark first.
- Picking the custom segment file now because the hypothesis is confident.
  Same answer.
