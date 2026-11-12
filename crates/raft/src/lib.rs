//! Raft log storage for Camus.
//!
//! The log is an append-only segment file. `fsync` happens before an append
//! is visible to readers. On Linux that write and `fsync` are one `io_uring`
//! submission; other systems use `pwrite` and `File::sync_all`. The state
//! machine lives in `camus-metadata` and uses `redb`. This crate does not
//! choose a leader or speak Kafka.

pub mod codec;
pub mod layout;
pub mod quic;
mod segment;
mod store;
mod submit;

pub use segment::{Payload, SegmentLog, DEFAULT_SEGMENT_BYTES, DEFAULT_TAIL_BYTES};
pub use store::{AppendStats, SegmentLogStore};
