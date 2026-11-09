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
- No `mmap`. On Linux, a segment append whose offset and length are multiples
  of 4096 is one direct write with force-unit-access (`O_DIRECT` and
  `RWF_DSYNC`). The completion of that write is the ack, and the ring polls
  for it when the kernel accepts `IORING_SETUP_IOPOLL`. The bytes are padded
  with a record whose index is `u64::MAX`, which open skips. A drive or
  filesystem that rejects the direct write, and every other operating
  system, uses a buffered write linked to one `fsync`. The active segment is
  a registered file, updated when the segment rotates, and the write uses a
  1 MiB buffer pinned once for the life of the ring. The `io_uring` crate's
  queue and buffer-registration APIs are `unsafe` because the kernel reads
  that memory and uses the descriptor while the submission is in flight;
  safe Rust cannot name that lifetime. Those blocks stay in
  `crates/raft/src/submit.rs`. The buffer is not reallocated after
  registration, and the ring is dropped before the buffer.
  `append_reopen_and_torn_tail` fails if a synced prefix disappears or a
  torn tail is served.
- These `io_uring` features are not used. `IORING_SETUP_SQPOLL` would keep a
  kernel thread polling for a syscall that is noise next to the fsync.
  `IORING_SETUP_IOPOLL` applies to polled `O_DIRECT` reads and writes, and
  this ring's completion is an `fsync`, which is not a polled opcode.
  `IORING_OP_SEND_ZC` and multishot accept or receive would replace the
  tokio socket loop. `IORING_OP_URING_CMD` would bypass the filesystem the
  torn-tail recovery depends on, and it only exists on NVMe.

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

## Measurement

2026-11-05, Apple M4 Pro, 24 GiB RAM, APFS on the internal 494 GB SSD.
One thread. Measured for 5 seconds after 1 second of warmup
(`CAMUS_BENCH_SECONDS=5`, `CAMUS_BENCH_WARMUP=1`). That is shorter than
the 60s / 10s protocol above, so these percentiles are a short sample.
Eight threads were not run.

| Engine | Workload | p50 | p99 | MiB/s |
| --- | --- | ---: | ---: | ---: |
| segment | 1 KiB | 4.0 ms | 5.0 ms | 0.25 |
| redb | 1 KiB | 4.0 ms | 5.1 ms | 0.25 |
| fjall | 1 KiB | 4.0 ms | 5.9 ms | 0.24 |
| segment | 64 KiB | 4.1 ms | 8.3 ms | 13.9 |
| redb | 64 KiB | 4.9 ms | 6.3 ms | 13.9 |
| fjall | 64 KiB | 4.9 ms | 9.3 ms | 13.1 |
| segment | 1 MiB | 8.1 ms | 13.0 ms | 119 |
| redb | 1 MiB | 7.0 ms | 12.0 ms | 140 |
| fjall | 1 MiB | 6.3 ms | 29.9 ms | 110 |
| segment | 32 KiB × 16, one fsync | 7.9 ms | 16.0 ms | 62 |
| redb | 32 KiB × 16, one fsync | 7.0 ms | 14.9 ms | 67 |
| fjall | 32 KiB × 16, one fsync | 6.4 ms | 25.8 ms | 62 |

The 5 ms group-commit bar is not met on this machine. A 1 KiB append is
already one fsync at about 4.0 ms p50 and 5.0 ms p99 in every engine, so
the floor is the disk. The group-commit workload writes 512 KiB and then
fsyncs once. None of the three engines is under 5 ms. The segment file
matches that floor and is not the slow outlier (`fjall` p99 is), so the
engine choice stays. A 60 second run on a faster volume can replace this
table.

The crash check is `append_reopen_and_torn_tail`: two synced records, a
short tail written after them, reopen, the prefix still reads, and a
further append lands after that prefix. `truncate_and_purge` cuts a tail,
appends again, drops a prefix, and reopens.
