//! Retention and the orphan object sweep.
//!
//! This crate decides what to remove. The broker reads metadata and the
//! bucket, asks these functions, and commits the result, so the decisions
//! can be tested without a bucket. Retention follows ADR 0010 and the sweep
//! follows ADR 0007.

#![forbid(unsafe_code)]

pub mod retention;
pub mod sweep;

/// How many leading lengths fit in `cap` bytes. The first length is always
/// included, so one object larger than the cap still makes progress.
pub fn window_len(lengths: &[u64], cap: u64) -> usize {
    let mut count = 0usize;
    let mut bytes = 0u64;
    for length in lengths {
        if count > 0 && bytes.saturating_add(*length) > cap {
            break;
        }
        bytes = bytes.saturating_add(*length);
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::window_len;

    #[test]
    fn a_window_stops_at_the_byte_cap_and_keeps_a_larger_first_object() {
        assert_eq!(window_len(&[10, 10, 10], 25), 2);
        assert_eq!(window_len(&[100], 25), 1);
        assert_eq!(window_len(&[], 25), 0);
    }
}

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
