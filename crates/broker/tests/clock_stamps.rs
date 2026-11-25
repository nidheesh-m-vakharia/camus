//! `clock.stamps` turns on per-batch clock stamps in WAL shards (ADR 0018,
//! phase 3). With it on, every appended batch is counted as stamped, or as
//! unstamped when the host gives no clock reading, which is the case off
//! Linux with glibc. With it off, nothing is counted and nothing changes.

mod support;

use support::{kafka_addr, produce, single, Opts};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The unlabelled series `name` on the broker at `http`.
fn series(http: &str, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let metrics = reqwest::blocking::Client::new()
        .get(format!("http://{http}/metrics"))
        .send()?
        .text()?;
    Ok(metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .ok_or_else(|| format!("no series {name}"))?
        .parse()?)
}

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
    // The shard stamps once its members, here this broker alone, have said
    // in metadata that they read stamped entries.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&on.http, "camus_wal_stamping_shards")? == 0 {
        if std::time::Instant::now() > deadline {
            return Err("the shard never started stamping".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    for node in [&on, &off] {
        let addr = kafka_addr(node);
        for at in 0..5 {
            assert_eq!(produce(&addr, &format!("record-{at}")), (0, at));
        }
    }
    assert_eq!(
        counted(&on.http)?,
        5,
        "every batch is stamped or counted unstamped"
    );
    assert_eq!(
        counted(&off.http)?,
        0,
        "nothing is stamped with the setting off"
    );
    assert_eq!(series(&off.http, "camus_wal_stamping_shards")?, 0);
    Ok(())
}

/// A shard does not stamp while one of its members has not said it reads
/// stamped entries, as a broker on an older release never does, and starts
/// once every member has.
#[test]
fn a_shard_with_an_older_member_does_not_stamp() -> TestResult {
    let dirs = [
        tempfile::tempdir()?,
        tempfile::tempdir()?,
        tempfile::tempdir()?,
    ];
    let opts = |older: bool| {
        let opts = Opts::default().env("CAMUS_CLOCK_STAMPS", "true");
        if older {
            opts.env("CAMUS_FAILPOINT", "no-stamp-capability")
        } else {
            opts
        }
    };
    let mut nodes: Vec<support::Node> = dirs
        .iter()
        .zip(1..)
        .map(|(dir, id)| support::Node::with(id, dir.path(), opts(id == 3)))
        .collect();
    support::init(&nodes, &[1, 2, 3]);
    support::wait_settled(&nodes.iter().collect::<Vec<_>>());
    let stamping = |nodes: &[support::Node]| -> Result<u64, Box<dyn std::error::Error>> {
        let mut total = 0;
        for node in nodes {
            total += series(&node.http, "camus_wal_stamping_shards")?;
        }
        Ok(total)
    };
    // Every broker but the older one has said so: give that time to land.
    std::thread::sleep(std::time::Duration::from_secs(4));
    assert_eq!(
        stamping(&nodes)?,
        0,
        "an older member keeps the shard unstamped"
    );

    let older = nodes.get_mut(2).ok_or("three nodes")?;
    older.restart_with(opts(false));
    support::wait_settled(&nodes.iter().collect::<Vec<_>>());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while stamping(&nodes)? == 0 {
        if std::time::Instant::now() > deadline {
            return Err("the shard never started stamping after the upgrade".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    Ok(())
}
