//! The host clock as an interval that contains the true time (ADR 0018,
//! phase 3).
//!
//! The kernel keeps an error bound for its clock, `maxerror`, which NTP
//! sets at each sync and the kernel grows by its 500 ppm tolerance every
//! second between syncs. A reading here is `[now − maxerror, now +
//! maxerror]`. A clock the kernel does not call synchronised gives no
//! reading, and anything stamped from it is left unstamped rather than
//! stamped wrong.

use std::time::{SystemTime, UNIX_EPOCH};

/// An interval of Unix microseconds that contains the true time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    /// No earlier than this.
    pub earliest_us: i64,
    /// No later than this.
    pub latest_us: i64,
}

/// A source of [`Interval`]s.
pub trait Clock: Send + Sync {
    /// The current interval, or `None` when the clock is not synchronised
    /// or the host cannot say how far off it is.
    fn now(&self) -> Option<Interval>;
}

/// The host's clock, bounded by the kernel's `maxerror`. Linux with glibc
/// only; other hosts give no reading.
#[derive(Debug, Clone, Copy, Default)]
pub struct Host;

impl Clock for Host {
    fn now(&self) -> Option<Interval> {
        let error_us = kernel_error_us()?;
        let now_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_micros(),
        )
        .ok()?;
        Some(Interval {
            earliest_us: now_us.saturating_sub(error_us),
            latest_us: now_us.saturating_add(error_us),
        })
    }
}

/// The kernel's `maxerror` in microseconds, when it calls the clock
/// synchronised.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn kernel_error_us() -> Option<i64> {
    // SAFETY: `timex` is plain old data, so all zeroes is a valid value
    // with `modes` 0, which makes `ntp_adjtime` read without changing
    // anything. The pointer is to a live local for the whole call.
    let (state, timex) = unsafe {
        let mut timex: libc::timex = std::mem::zeroed();
        let state = libc::ntp_adjtime(&mut timex);
        (state, timex)
    };
    if state < 0 || state == libc::TIME_ERROR || timex.status & libc::STA_UNSYNC != 0 {
        return None;
    }
    Some(i64::from(timex.maxerror)).filter(|error| *error >= 0)
}

/// Hosts without `ntp_adjtime`, or where Camus does not read it, give no
/// reading, so nothing they append is stamped.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn kernel_error_us() -> Option<i64> {
    None
}

/// A clock that reads a set interval, for tests, or a host with no clock
/// at all when `at` is `None`.
#[derive(Debug, Clone, Copy)]
pub struct Fixed {
    /// What every reading returns.
    pub at: Option<Interval>,
}

impl Clock for Fixed {
    fn now(&self) -> Option<Interval> {
        self.at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_reading_contains_now_when_there_is_one() {
        let Some(reading) = Host.now() else {
            // Not Linux with glibc, or the clock is not synchronised here.
            return;
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_micros() as i64)
            .unwrap_or_default();
        assert!(reading.earliest_us <= reading.latest_us);
        // A second passed at most between the two readings.
        assert!(reading.earliest_us <= now && now <= reading.latest_us + 1_000_000);
    }
}
