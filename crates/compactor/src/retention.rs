//! `retention.ms` and `retention.bytes` for one partition.
//!
//! Kafka deletes whole segments and never the active one. Camus deletes whole
//! sealed extents and never the unsealed WAL tail. A time cut drops each
//! leading extent whose newest timestamp is older than the limit, stopping
//! at the first one that is not. A size cut drops leading extents while what
//! is left stays at or above `retention.bytes`, as Kafka's
//! `UnifiedLog.deleteRetentionSizeBreachedSegments` does. Only sealed bytes
//! count toward the size.

use camus_metadata::Extent;

/// The offset the log start should move to, or `None` to leave it.
pub fn cut(
    extents: &[Extent],
    log_start: i64,
    retention_ms: i64,
    retention_bytes: i64,
    now_ms: i64,
) -> Option<i64> {
    let mut end = log_start;
    if retention_ms >= 0 {
        let oldest_kept = now_ms.saturating_sub(retention_ms);
        for extent in extents {
            if extent.max_timestamp >= oldest_kept {
                break;
            }
            end = end.max(extent.end);
        }
    }
    if retention_bytes >= 0 {
        let total: i64 = extents.iter().map(|extent| i64::from(extent.length)).sum();
        let mut over = total - retention_bytes;
        for extent in extents {
            let length = i64::from(extent.length);
            if over - length < 0 {
                break;
            }
            over -= length;
            end = end.max(extent.end);
        }
    }
    (end > log_start).then_some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extent(start: i64, end: i64, length: u32, max_timestamp: i64) -> Extent {
        Extent {
            start,
            end,
            key: format!("bundle/{start}.seg"),
            file_offset: 0,
            length,
            base_timestamp: max_timestamp,
            max_timestamp,
        }
    }

    #[test]
    fn time_cut_stops_at_the_first_extent_that_is_young_enough() {
        let extents = [
            extent(0, 10, 100, 1_000),
            extent(10, 20, 100, 5_000),
            extent(20, 30, 100, 2_000),
        ];
        assert_eq!(cut(&extents, 0, 3_000, -1, 7_000), Some(10));
        assert_eq!(cut(&extents, 0, 10_000, -1, 7_000), None);
        assert_eq!(cut(&extents, 0, 0, -1, 7_000), Some(30));
    }

    #[test]
    fn minus_one_keeps_everything() {
        let extents = [extent(0, 10, 100, 0)];
        assert_eq!(cut(&extents, 0, -1, -1, i64::MAX), None);
    }

    #[test]
    fn size_cut_keeps_at_least_the_limit() {
        let extents = [
            extent(0, 10, 100, 0),
            extent(10, 20, 100, 0),
            extent(20, 30, 100, 0),
        ];
        assert_eq!(cut(&extents, 0, -1, 150, 0), Some(10));
        assert_eq!(cut(&extents, 0, -1, 100, 0), Some(20));
        assert_eq!(cut(&extents, 0, -1, 0, 0), Some(30));
        assert_eq!(cut(&extents, 0, -1, 300, 0), None);
    }

    #[test]
    fn the_larger_cut_wins_and_never_moves_backwards() {
        let extents = [extent(0, 10, 100, 0), extent(10, 20, 100, 9_000)];
        assert_eq!(cut(&extents, 0, 1_000, 150, 10_000), Some(10));
        assert_eq!(cut(&extents, 10, 1_000, 150, 10_000), None);
    }
}
