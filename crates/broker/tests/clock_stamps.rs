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
    for node in &nodes {
        assert_eq!(
            series(&node.http, "camus_stamped_objects")?,
            0,
            "an older member keeps objects unstamped"
        );
    }

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

/// Produce `value` to `topic` partition 0, returning the error code.
fn produce_to(addr: &str, topic: &'static str, value: &str) -> i16 {
    use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use camus_protocol::messages::produce_response::ProduceResponse;
    use camus_protocol::messages::{ApiKey, ProduceRequest, TopicName};
    use camus_protocol::protocol::StrBytes;
    let mut stream = support::connect(addr);
    let mut data = TopicProduceData::default();
    data.name = TopicName(StrBytes::from_static_str(topic));
    data.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(support::batch(value)))];
    support::send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![data]),
    );
    support::recv::<ProduceResponse>(&mut stream, 9).responses[0].partition_responses[0].error_code
}

/// With every broker reading stamped objects, `wal` and `r2` uploads write
/// footer version 2 with each batch's stamp, and publish each extent with
/// the range of its batches' stamps.
#[test]
fn uploads_carry_stamps_into_footers_and_extents() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("stamped objects test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let prefix = format!("stamped-objects-{}/", std::process::id());
    let dir = tempfile::tempdir()?;
    let node = single(
        dir.path(),
        Opts::default()
            .env("CAMUS_CLOCK_STAMPS", "true")
            .env("CAMUS_FAILPOINT", "assumed-clock")
            .env("CAMUS_S3_PREFIX", &prefix),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&node.http, "camus_stamped_objects")? == 0
        || series(&node.http, "camus_wal_stamping_shards")? == 0
    {
        if std::time::Instant::now() > deadline {
            return Err("stamping never started".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let addr = kafka_addr(&node);
    support::create_topic_named(&addr, "events", "r2", 1);
    for at in 0..5 {
        assert_eq!(produce(&addr, &format!("wal-{at}")), (0, at));
        assert_eq!(produce_to(&addr, "events", &format!("r2-{at}")), 0);
    }
    support::wait_sealed(&node.http, 5);

    let bucket = camus_storage::Bucket::connect(&camus_storage::BucketConfig {
        endpoint: "http://127.0.0.1:9100".into(),
        bucket: "camus".into(),
        region: "us-east-1".into(),
        access_key: "camus".into(),
        secret_key: "camus-dev-secret".into(),
        prefix,
        virtual_hosted: false,
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for topic in [support::TOPIC, "events"] {
        let raw = support::read_key(&node.http, &camus_metadata::topic_storage_key(topic))
            .ok_or("no topic")?;
        let id = camus_metadata::parse_topic(&raw)?.id;
        let extents = support::scan_keys(&node.http, &format!("ext/{id}/"));
        assert!(!extents.is_empty(), "{topic} has extents");
        for (_, raw) in extents {
            let extent: camus_metadata::Extent = serde_json::from_str(&raw)?;
            let stamps = extent
                .stamps
                .ok_or_else(|| format!("{topic} extent unstamped: {raw}"))?;
            assert!(stamps.min <= stamps.max);
            let (body, _) = runtime.block_on(bucket.get_tail(&extent.key, 64 << 20))?;
            let footer = camus_uploader::decode_footer(&body)?;
            assert!(
                footer.iter().all(|entry| entry.stamp.is_some()),
                "{topic}: every footer entry is stamped"
            );
        }
    }
    Ok(())
}
