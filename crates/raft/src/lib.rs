//! Raft log storage for Camus.
//!
//! The log is an append-only segment file. `fsync` happens before an append
//! is visible to readers. The state machine lives in `camus-metadata` and uses
//! `redb`. This crate does not choose a leader or speak Kafka.

#![forbid(unsafe_code)]

mod segment;
mod store;

pub use segment::{SegmentLog, DEFAULT_SEGMENT_BYTES};
pub use store::SegmentLogStore;
