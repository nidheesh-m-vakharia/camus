//! Latency histograms and Prometheus text helpers for `/metrics`.
//!
//! Every series here is a fixed set of atomics in static memory, so
//! recording one sample is a few relaxed adds and never takes a lock. The
//! whole module costs 66,048 bytes of static memory whatever the load. `docs/metrics.md`
//! lists each series and its cost.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// Upper bounds of the histogram buckets, in microseconds: 0.5 ms to 10 s.
/// A sample above the last bound lands only in `+Inf`.
const BOUNDS_US: [u64; 14] = [
    500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
    2_500_000, 5_000_000, 10_000_000,
];

/// A latency histogram with fixed buckets.
pub struct Histogram {
    /// Samples at or under each bound and over the one before it.
    buckets: [AtomicU64; BOUNDS_US.len()],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Histogram {
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BOUNDS_US.len()],
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }

    pub fn record(&self, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        if let Some(at) = BOUNDS_US.iter().position(|bound| us <= *bound) {
            if let Some(bucket) = self.buckets.get(at) {
                bucket.fetch_add(1, Relaxed);
            }
        }
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
    }

    /// Write this histogram's `_bucket`, `_sum` and `_count` lines.
    /// `labels` is either empty or `key="value"` pairs without braces.
    pub fn render(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cumulative = 0u64;
        for (bound, bucket) in BOUNDS_US.iter().zip(&self.buckets) {
            cumulative += bucket.load(Relaxed);
            let le = *bound as f64 / 1e6;
            let _ = writeln!(
                out,
                "{name}_bucket{{{labels}{sep}le=\"{le}\"}} {cumulative}"
            );
        }
        let count = self.count.load(Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {count}");
        let sum = self.sum_us.load(Relaxed) as f64 / 1e6;
        if labels.is_empty() {
            let _ = writeln!(out, "{name}_sum {sum}\n{name}_count {count}");
        } else {
            let _ = writeln!(
                out,
                "{name}_sum{{{labels}}} {sum}\n{name}_count{{{labels}}} {count}"
            );
        }
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Time from reading a Produce request to having its response, including
/// the WAL commit or the `r2` put.
pub static PRODUCE_LATENCY: Histogram = Histogram::new();
/// Time to answer a Fetch request, including any `max.wait.ms` long poll.
pub static FETCH_LATENCY: Histogram = Histogram::new();

/// Shards with their own seal and upload series. Shard ids at or past this
/// share the `shard="overflow"` series, so the series count stays fixed.
pub const SHARD_SERIES: usize = 256;

/// One WAL shard's upload timings.
pub struct ShardTimes {
    /// Putting a pass's objects and publishing their extents to metadata.
    pub upload: Histogram,
    /// Committing the `Seal` that drops uploaded batches from the WAL.
    pub seal: Histogram,
}

impl ShardTimes {
    const fn new() -> Self {
        Self {
            upload: Histogram::new(),
            seal: Histogram::new(),
        }
    }
}

static SHARD_TIMES: [ShardTimes; SHARD_SERIES] = [const { ShardTimes::new() }; SHARD_SERIES];
static OVERFLOW_TIMES: ShardTimes = ShardTimes::new();

/// The timings for shard `id`, or the overflow series past [`SHARD_SERIES`].
pub fn shard_times(id: u32) -> &'static ShardTimes {
    usize::try_from(id)
        .ok()
        .and_then(|id| SHARD_TIMES.get(id))
        .unwrap_or(&OVERFLOW_TIMES)
}

/// Whether any shard past [`SHARD_SERIES`] has recorded a sample.
pub fn overflow_times() -> Option<&'static ShardTimes> {
    let used = OVERFLOW_TIMES.upload.count.load(Relaxed) > 0
        || OVERFLOW_TIMES.seal.count.load(Relaxed) > 0;
    used.then_some(&OVERFLOW_TIMES)
}

/// Bytes of static memory this module holds: 66,048 on a 64-bit target.
#[cfg(test)]
pub const STATIC_BYTES: usize =
    std::mem::size_of::<Histogram>() * 2 + std::mem::size_of::<ShardTimes>() * (SHARD_SERIES + 1);

/// Most `(group, partition)` lag series one broker reports per scrape. Pairs
/// past it are counted in `camus_consumer_group_lag_series_dropped`.
pub const LAG_SERIES: usize = 2_000;
/// Most bytes of consumer-lag text one scrape builds, so long group ids
/// cannot make a scrape large. Pairs past it are counted as dropped.
pub const LAG_BYTES: usize = 1 << 20;
/// Most committed-offset rows one scrape reads from metadata, so a cluster
/// with many groups cannot make a scrape slow or large.
pub const LAG_SCAN_ROWS: usize = 100_000;

/// `value` escaped for a Prometheus label: backslash, quote and newline.
pub fn label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            ch => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_histogram_renders_cumulative_buckets() -> anyhow::Result<()> {
        let histogram = Histogram::new();
        histogram.record(Duration::from_micros(300));
        histogram.record(Duration::from_millis(3));
        histogram.record(Duration::from_secs(60));
        let mut out = String::new();
        histogram.render(&mut out, "x_seconds", "shard=\"0\"");
        anyhow::ensure!(
            out.contains("x_seconds_bucket{shard=\"0\",le=\"0.0005\"} 1\n"),
            "{out}"
        );
        anyhow::ensure!(
            out.contains("x_seconds_bucket{shard=\"0\",le=\"0.005\"} 2\n"),
            "{out}"
        );
        anyhow::ensure!(
            out.contains("x_seconds_bucket{shard=\"0\",le=\"10\"} 2\n"),
            "{out}"
        );
        anyhow::ensure!(
            out.contains("x_seconds_bucket{shard=\"0\",le=\"+Inf\"} 3\n"),
            "{out}"
        );
        anyhow::ensure!(out.contains("x_seconds_count{shard=\"0\"} 3\n"), "{out}");
        anyhow::ensure!(
            out.contains("x_seconds_sum{shard=\"0\"} 60.0033\n"),
            "{out}"
        );
        Ok(())
    }

    #[test]
    fn an_unlabelled_histogram_has_bare_sum_and_count() {
        let histogram = Histogram::new();
        let mut out = String::new();
        histogram.render(&mut out, "y_seconds", "");
        assert!(out.contains("y_seconds_bucket{le=\"+Inf\"} 0\n"));
        assert!(out.contains("y_seconds_sum 0\ny_seconds_count 0\n"));
    }

    #[test]
    fn shards_past_the_cap_share_one_series() {
        assert!(std::ptr::eq(
            shard_times(u32::MAX),
            shard_times(SHARD_SERIES as u32)
        ));
        assert!(!std::ptr::eq(shard_times(0), shard_times(1)));
    }

    #[test]
    fn labels_escape_quotes_and_newlines() {
        assert_eq!(label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn the_static_cost_is_fixed() {
        assert_eq!(STATIC_BYTES, 66_048);
    }
}
