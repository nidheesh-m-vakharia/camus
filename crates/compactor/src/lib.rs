//! Retention and the orphan object sweep.
//!
//! This crate decides what to remove. The broker reads metadata and the
//! bucket, asks these functions, and commits the result, so the decisions
//! can be tested without a bucket. Retention follows ADR 0010 and the sweep
//! follows ADR 0007.

#![forbid(unsafe_code)]

pub mod retention;
pub mod sweep;

use std::sync::atomic::AtomicU64;

/// Counters for `/metrics` and `/admin/metrics`.
#[derive(Default)]
pub struct Stats {
    /// Partitions whose log start retention moved.
    pub retention_cuts: AtomicU64,
    /// Completed orphan sweeps.
    pub sweeps: AtomicU64,
    /// Objects the sweep deleted.
    pub objects_deleted: AtomicU64,
    /// Bytes in the objects the sweep deleted.
    pub bytes_deleted: AtomicU64,
}

/// Process-wide counters.
pub static STATS: Stats = Stats {
    retention_cuts: AtomicU64::new(0),
    sweeps: AtomicU64::new(0),
    objects_deleted: AtomicU64::new(0),
    bytes_deleted: AtomicU64::new(0),
};
