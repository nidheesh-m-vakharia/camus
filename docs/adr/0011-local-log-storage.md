# ADR 0011: Local log storage

Status: Accepted
Date: 2026-11-05

## Context

openraft needs a `RaftLogStorage` and a state-machine store on local disk.
The brief lists `redb`, `fjall`, or a custom append-only segment file.
The produce ack in `wal` mode is a quorum of fsyncs, with a 20ms p99
budget inside one region. That access pattern decides the engine. Every
dependency on this path has to be open source and Rust. C and C++ stores
are out.

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

Two stores, because the two jobs have different shapes.

| Job | Engine | License |
| --- | --- | --- |
| Raft log (metadata and WAL) | Custom append-only segment files in the `raft` crate | Project license, Apache-2.0 |
| State machine | [`redb`](https://github.com/cberner/redb) 2.x | MIT OR Apache-2.0 |

### Why the log is a segment file

A Raft log is appended, fsynced, read in order, and prefix-truncated. The
ack returns from `File::sync_all` on that append. A database under that
call adds its own write-ahead log, so the broker fsyncs twice, and it adds
B-tree or LSM write amplification on a path that never updates old records.

The format:

- Segment files of a fixed size (64 MiB), named by the first log index.
- Each entry is `u64 index`, `u32 length`, payload, `u32 crc32c`.
- A small index maps log index to `(segment, file_offset)`. It is rebuilt
  from the segments on open if it is missing.
- `append` writes the bytes, then `sync_all`, then returns. A crash during
  the write leaves a torn tail. Open scans the last segment, stops at the
  first bad CRC or short read, and truncates there. Entries that returned
  success have a completed fsync and must still be present.
- Prefix truncation after upload or snapshot deletes whole segments whose
  last index is below the new start. A partial first segment is rewritten
  once, or left in place and skipped. No in-place punch is required for
  correctness.
- `std::fs` only. No `mmap`, no `O_DIRECT`, no `unsafe` in the first cut.
  If a later change needs `unsafe`, the block must say why safe Rust cannot
  express it, and a test must fail when the invariant breaks.

Phase 0 still records a benchmark of this format against `redb` and `fjall`
on the same machine (workload below). The numbers go at the bottom of this
ADR. They check the implementation. They do not reopen the choice unless
the segment file cannot be made to hit the bar and the other engine can.
The bar is p99 under 5ms for the group-commit workload on the test NVMe,
plus a clean crash test. Five milliseconds leaves room for a second
replica's fsync and a network hop inside the 20ms budget.

### Why the state machine is redb

Apply happens after the ack. The machine stores topics, extent lists, group
offsets, and producer sequences: small point updates, occasional snapshots,
rare scans. `redb` is one Rust file, transactional, and a snapshot is a
read transaction copied out to bytes. That matches the metadata snapshot
uploaded to R2.

`fjall` would be the alternative if apply were a high-rate LSM workload.
It is not. Extent commits are about four per second per uploader. Paying
compaction stalls and a second on-disk format for that rate is the wrong
trade. `fjall` stays in the benchmark as a column and out of `Cargo.toml`.

### Benchmark protocol

Recorded with the results: machine, disk type, filesystem, and whether the
disk is the OS volume.

Workload, single thread, then 8 concurrent append tasks, 60 seconds each
after a 10 second warmup:

1. Append 1 KiB, fsync every append.
2. Append 64 KiB, fsync every append.
3. Append 1 MiB, fsync every append.
4. Group commit: append 32 KiB × 16, one fsync per group (the packed Raft
   entry from ADR 0006).

Report p50, p99, and MiB/s. Crash test: kill -9 during append, reopen, and
check that the prefix which reported success is readable and a torn tail is
ignored.

Unit tests use openraft's in-memory store. The on-disk stores are what a
process uses when it is given a data directory.

## Consequences

- The durable log format is part of Phase 0, not a follow-up vote.
- `redb` is the only third-party storage engine. It is dual licensed MIT OR
  Apache-2.0, which the Apache-2.0 project can depend on.
- A segment-file bug is ours to fix. There is no upstream LSM to blame for
  a latency spike, and no upstream to take a fix from either.

## Alternatives

- `redb` for the log and the state machine. Rejected for the log. Its
  transaction log would sit in front of our fsync, and a B-tree is the
  wrong structure for an append-only Raft log.
- `fjall` for the log. Rejected. LSM compaction causes p99 spikes, and the
  log does not need compaction. It is also a younger engine than `redb`
  with a more complicated crash story. License would have been fine
  (MIT OR Apache-2.0).
- RocksDB or LMDB. Rejected. Both are faster in some published log
  workloads and neither is Rust. RocksDB is C++ (Apache-2.0 or GPL-2.0)
  and LMDB is C (OpenLDAP Public License). They pull a non-Rust toolchain
  into every broker build. The latency budget is met by one fsync of a
  segment, which does not need them.
- Waiting for benchmark numbers before writing the format. Rejected once
  the access pattern was clear. The benchmark remains a Phase 0 check on
  the implementation.
