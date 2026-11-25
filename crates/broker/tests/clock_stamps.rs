//! `clock.stamps` turns on per-batch clock stamps in WAL shards (ADR 0018,
//! phase 3). With it on, every appended batch is counted as stamped, or as
//! unstamped when the host gives no clock reading, which is the case off
//! Linux with glibc. With it off, nothing is counted and nothing changes.

mod support;

use support::{kafka_addr, produce, single, Opts};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Stamped plus unstamped batches on the broker at `http`.
fn counted(http: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let metrics = reqwest::blocking::Client::new()
        .get(format!("http://{http}/metrics"))
        .send()?
        .text()?;
    let mut total = 0;
    for name in [
        "camus_wal_stamped_batches_total",
        "camus_wal_unstamped_batches_total",
    ] {
        total += metrics
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .ok_or_else(|| format!("no series {name}"))?
            .parse::<u64>()?;
    }
    Ok(total)
}

#[test]
fn stamps_count_every_batch_only_when_turned_on() -> TestResult {
    let on_dir = tempfile::tempdir()?;
    let on = single(
        on_dir.path(),
        Opts::default().env("CAMUS_CLOCK_STAMPS", "true"),
    );
    let off_dir = tempfile::tempdir()?;
    let off = single(off_dir.path(), Opts::default());
    for node in [&on, &off] {
        let addr = kafka_addr(node);
        for at in 0..5 {
            assert_eq!(produce(&addr, &format!("record-{at}")), (0, at));
        }
    }
    assert!(
        counted(&on.http)? >= 5,
        "every batch is stamped or counted unstamped"
    );
    assert_eq!(
        counted(&off.http)?,
        0,
        "nothing is stamped with the setting off"
    );
    Ok(())
}
