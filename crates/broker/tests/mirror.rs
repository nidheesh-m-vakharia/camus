//! Mirror links and mirror topics (ADR 0018, phase 1). On one broker: a
//! mirror topic refuses produce and every admin change but retention, and a
//! link a mirror names cannot be removed. With MinIO: `link add` reads the
//! source's snapshot before it commits, and `mirror create` takes the
//! source topic's partition count and id from it.

mod support;

use camus_metadata::mirror::{MirrorLink, MirrorOf};
use camus_metadata::Command;
use camus_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse;
use camus_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use camus_protocol::messages::create_partitions_response::CreatePartitionsResponse;
use camus_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use camus_protocol::messages::delete_records_response::DeleteRecordsResponse;
use camus_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
};
use camus_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    AlterConfigsRequest, ApiKey, CreatePartitionsRequest, DeleteRecordsRequest,
    IncrementalAlterConfigsRequest, ProduceRequest, TopicName,
};
use camus_protocol::protocol::StrBytes;
use reqwest::StatusCode;
use support::{batch, connect, kafka_addr, produce, recv, send, single, Opts};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MIRROR: &str = "mirrored";
const INVALID_TOPIC: i16 = 17;
const POLICY_VIOLATION: i16 = 44;

fn link(bucket: &str) -> MirrorLink {
    MirrorLink {
        alias: "us-east".into(),
        endpoint: "https://source.example".into(),
        region: "auto".into(),
        bucket: bucket.into(),
        prefix: String::new(),
        virtual_host: false,
    }
}

fn add(http: &str, link: &MirrorLink) -> Result<StatusCode, reqwest::Error> {
    Ok(reqwest::blocking::Client::new()
        .post(format!("http://{http}/admin/mirror/links"))
        .json(link)
        .send()?
        .status())
}

/// `(error code, has a message)` for one produce to the mirror.
fn produce_to_mirror(addr: &str) -> (i16, bool) {
    let mut stream = connect(addr);
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(MIRROR));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch("refused")))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![topic]),
    );
    let response = recv::<ProduceResponse>(&mut stream, 9);
    let part = &response.responses[0].partition_responses[0];
    (part.error_code, part.error_message.is_some())
}

#[test]
fn a_mirror_topic_is_read_only_and_holds_its_link() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let client = reqwest::blocking::Client::new();

    // This broker has no CAMUS_MIRROR_US_EAST_* keys, so `link add` cannot
    // check the source and refuses before committing anything.
    let refused = client
        .post(format!("http://{}/admin/mirror/links", node.http))
        .json(&link("logs"))
        .send()?;
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    assert!(refused.text()?.contains("CAMUS_MIRROR_US_EAST_ACCESS_KEY"));
    let mut bad = link("logs");
    bad.alias = "US_EAST".into();
    assert_eq!(add(&node.http, &bad)?, StatusCode::BAD_REQUEST);

    // Commit the link directly; the source check has its own MinIO test.
    let linked: camus_metadata::Response = client
        .post(format!("http://{}/admin/apply", node.http))
        .json(&Command::AddMirrorLink { link: link("logs") })
        .send()?
        .json()?;
    assert_eq!(linked.error_code, 0);
    let listed: Vec<MirrorLink> = client
        .get(format!("http://{}/admin/mirror/links", node.http))
        .send()?
        .json()?;
    assert_eq!(listed, vec![link("logs")]);

    let created: camus_metadata::Response = client
        .post(format!("http://{}/admin/apply", node.http))
        .json(&Command::CreateMirrorTopic {
            name: MIRROR.into(),
            id: "00000000-0000-4000-8000-000000000018".into(),
            partitions: 2,
            of: MirrorOf {
                alias: "us-east".into(),
                topic_id: "source-topic-id".into(),
            },
        })
        .send()?
        .json()?;
    assert_eq!(created.error_code, 0);

    assert_eq!(produce_to_mirror(&addr), (INVALID_TOPIC, true));
    assert_eq!(produce(&addr, "plain topics still take produce").0, 0);

    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::CreatePartitions,
        0,
        &CreatePartitionsRequest::default()
            .with_topics(vec![CreatePartitionsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(MIRROR)))
                .with_count(3)])
            .with_timeout_ms(5_000),
    );
    let grown = recv::<CreatePartitionsResponse>(&mut stream, 0);
    assert_eq!(grown.results[0].error_code, POLICY_VIOLATION);
    assert!(grown.results[0].error_message.is_some());

    let mut retention = AlterConfigsResource::default();
    retention.resource_type = 2;
    retention.resource_name = StrBytes::from_static_str(MIRROR);
    retention.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_value(Some(StrBytes::from_static_str("1000")))];
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        2,
        &AlterConfigsRequest::default().with_resources(vec![retention]),
    );
    let altered = recv::<AlterConfigsResponse>(&mut stream, 2);
    assert_eq!(
        altered.responses[0].error_code, 0,
        "retention is the mirror's own"
    );

    let mut compact = IncrementalResource::default();
    compact.resource_type = 2;
    compact.resource_name = StrBytes::from_static_str(MIRROR);
    compact.configs = vec![IncrementalConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_config_operation(0)
        .with_value(Some(StrBytes::from_static_str("compact")))];
    send(
        &mut stream,
        ApiKey::IncrementalAlterConfigs,
        1,
        &IncrementalAlterConfigsRequest::default().with_resources(vec![compact]),
    );
    let refused = recv::<IncrementalAlterConfigsResponse>(&mut stream, 1);
    assert_eq!(refused.responses[0].error_code, POLICY_VIOLATION);
    assert!(refused.responses[0].error_message.is_some());

    let mut cut = DeleteRecordsTopic::default();
    cut.name = TopicName(StrBytes::from_static_str(MIRROR));
    cut.partitions = vec![DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(0)];
    send(
        &mut stream,
        ApiKey::DeleteRecords,
        2,
        &DeleteRecordsRequest::default()
            .with_topics(vec![cut])
            .with_timeout_ms(5_000),
    );
    let deleted = recv::<DeleteRecordsResponse>(&mut stream, 2);
    assert_eq!(deleted.topics[0].partitions[0].error_code, POLICY_VIOLATION);

    let removed = client
        .delete(format!("http://{}/admin/mirror/links/us-east", node.http))
        .send()?
        .status();
    assert_eq!(
        removed,
        StatusCode::CONFLICT,
        "a mirror still names the link"
    );
    Ok(())
}

/// Start a broker whose bucket keys live under `prefix`, with snapshots
/// every 200 ms and `env` added.
fn spawn_under(dir: &std::path::Path, prefix: &str, env: &[(&str, &str)]) -> support::Node {
    let mut opts = Opts::default()
        .env("CAMUS_S3_PREFIX", prefix)
        .env("CAMUS_META_SNAPSHOT_MS", "200");
    for (key, value) in env {
        opts = opts.env(key, value);
    }
    single(dir, opts)
}

/// Retry `post` until it answers 200 or 15 seconds pass. The source's first
/// snapshot, or the one naming a new topic, can take a few intervals.
fn until_ok(
    post: impl Fn() -> Result<reqwest::blocking::Response, reqwest::Error>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let response = post()?;
        if response.status() == StatusCode::OK {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(format!("{}: {}", response.status(), response.text()?).into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

#[test]
fn a_mirror_is_created_from_the_source_snapshot() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror source test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-src-{pid}/");
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &[]);
    support::create_topic_named(&kafka_addr(&source), "events", "r2", 3);

    let dest_dir = tempfile::tempdir()?;
    let dest = spawn_under(
        dest_dir.path(),
        &format!("mirror-dst-{pid}/"),
        &[
            ("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus"),
            ("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret"),
            ("CAMUS_MIRROR_WRONG_ACCESS_KEY", "camus"),
            ("CAMUS_MIRROR_WRONG_SECRET_KEY", "not-the-secret"),
        ],
    );
    let client = reqwest::blocking::Client::new();
    let link_to = |alias: &str| MirrorLink {
        alias: alias.into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };

    let wrong = client
        .post(format!("http://{}/admin/mirror/links", dest.http))
        .json(&link_to("wrong"))
        .send()?;
    assert_eq!(
        wrong.status(),
        StatusCode::BAD_GATEWAY,
        "a wrong key is refused"
    );
    let said = wrong.text()?;
    for leak in ["Signature", "StringToSign", "AccessKeyId", "<?xml"] {
        assert!(
            !said.contains(leak),
            "the 502 echoes the store's reply: {said}"
        );
    }
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link_to("us-east"))
            .send()
    })?;
    let listed: Vec<MirrorLink> = client
        .get(format!("http://{}/admin/mirror/links", dest.http))
        .send()?
        .json()?;
    assert_eq!(listed, vec![link_to("us-east")]);

    let create = || {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": "events" }))
            .send()
    };
    until_ok(create)?;
    assert_eq!(
        create()?.status(),
        StatusCode::CONFLICT,
        "the mirror exists"
    );
    let missing = client
        .post(format!("http://{}/admin/mirror/topics", dest.http))
        .json(&serde_json::json!({ "alias": "us-east", "topic": "nope" }))
        .send()?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let row = |http: &str| -> Result<camus_metadata::Topic, Box<dyn std::error::Error>> {
        let raw = support::read_key(http, &camus_metadata::topic_storage_key("events"))
            .ok_or("no topic row")?;
        Ok(camus_metadata::parse_topic(&raw)?)
    };
    let (source_topic, mirror) = (row(&source.http)?, row(&dest.http)?);
    assert_eq!(mirror.partitions, 3);
    assert_eq!(
        mirror.mirror_of,
        Some(MirrorOf {
            alias: "us-east".into(),
            topic_id: source_topic.id.clone(),
        })
    );
    assert_ne!(mirror.id, source_topic.id, "the mirror has its own id");
    Ok(())
}

/// Every batch byte of `TOPIC` partition 0 below `end`, fetched in order.
fn raw_log(addr: &str, end: i64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use camus_protocol::messages::fetch_response::FetchResponse;
    use camus_protocol::messages::FetchRequest;
    let mut stream = connect(addr);
    let mut log = Vec::new();
    let mut offset = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while offset < end {
        if std::time::Instant::now() > deadline {
            return Err(format!("stuck at {offset} of {end}").into());
        }
        let mut part = FetchPartition::default();
        part.partition = 0;
        part.fetch_offset = offset;
        part.partition_max_bytes = 1024 * 1024;
        let mut topic = FetchTopic::default();
        topic.topic = TopicName(StrBytes::from_static_str(support::TOPIC));
        topic.partitions = vec![part];
        send(
            &mut stream,
            ApiKey::Fetch,
            11,
            &FetchRequest::default()
                .with_max_wait_ms(0)
                .with_topics(vec![topic]),
        );
        let fetched = recv::<FetchResponse>(&mut stream, 11);
        let row = &fetched.responses[0].partitions[0];
        assert_eq!(row.error_code, 0);
        let Some(bytes) = row.records.clone() else {
            continue;
        };
        let batches =
            camus_protocol::batch::inspect(&bytes, camus_protocol::batch::MAX_BATCH_CEILING)
                .map_err(|err| format!("{err:?}"))?;
        let mut at = 0;
        for batch in batches {
            if batch.base_offset >= offset && batch.base_offset < end {
                log.extend_from_slice(bytes.get(at..at + batch.len).ok_or("short batch")?);
                offset = batch.last_offset + 1;
            }
            at += batch.len;
        }
    }
    Ok(log)
}

/// Phase 1's acceptance test (ADR 0018): a mirror has the source's offsets
/// and records, follows new produces, and keeps serving all of it after
/// every source broker is gone.
#[test]
fn a_mirror_copies_the_source_and_outlives_it() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror copy test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-copy-src-{pid}/");
    // Three source brokers, so the WAL leader can fail over and raise the
    // leader epoch the copied batches carry.
    let source_dirs = [
        tempfile::tempdir()?,
        tempfile::tempdir()?,
        tempfile::tempdir()?,
    ];
    let mut sources: Vec<support::Node> = source_dirs
        .iter()
        .zip(1..)
        .map(|(dir, id)| {
            support::Node::with(
                id,
                dir.path(),
                Opts::default()
                    .env("CAMUS_S3_PREFIX", &source_prefix)
                    .env("CAMUS_META_SNAPSHOT_MS", "200"),
            )
        })
        .collect();
    support::init(&sources, &[1, 2, 3]);
    support::wait_settled(&sources.iter().collect::<Vec<_>>());
    support::create_wal_topic(&kafka_addr(&sources[0]));
    let (leader, source_addr) = support::partition_leader(&kafka_addr(&sources[0]), None);
    let mut sent = Vec::new();
    for at in 0..40 {
        let value = format!("record-{at}");
        let (code, offset) = produce(&source_addr, &value);
        assert_eq!((code, offset), (0, at), "produce {at}");
        sent.push(value);
    }
    let survivor = sources
        .iter()
        .find(|node| node.id != leader)
        .map(|node| node.http.clone())
        .ok_or("no survivor")?;
    support::wait_sealed(&survivor, 40);

    // The destination has no `orders` of its own: the mirror takes the name.
    let dest_dir = tempfile::tempdir()?;
    let mut dest = support::Node::with(
        1,
        dest_dir.path(),
        Opts::default()
            .env("CAMUS_S3_PREFIX", &format!("mirror-copy-dst-{pid}/"))
            .env("CAMUS_MIRROR_COPY_MS", "200")
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret"),
    );
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    let client = reqwest::blocking::Client::new();
    let link = MirrorLink {
        alias: "us-east".into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link)
            .send()
    })?;
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": support::TOPIC }))
            .send()
    })?;

    support::wait_sealed(&dest.http, 40);
    let dest_addr = kafka_addr(&dest);
    assert_eq!(
        support::read_all(&dest_addr, 40),
        sent,
        "same records, same offsets"
    );

    // New produces at the source reach the mirror at the same offsets.
    // Fail the WAL leader over: later batches carry a higher epoch than
    // the destination's own term, as any source that has failed over has.
    let epoch_before = support::partition_epoch(&source_addr);
    sources.retain(|node| node.id != leader);
    let via = kafka_addr(&sources[0]);
    let (_, source_addr) = support::partition_leader(&via, Some(leader));
    for at in 40..60 {
        let value = format!("record-{at}");
        assert_eq!(support::produce_retrying(&via, &value).1, at);
        sent.push(value);
    }
    assert!(
        support::partition_epoch(&source_addr) > epoch_before,
        "the epoch rose"
    );
    support::wait_sealed(&sources[0].http, 60);
    support::wait_sealed(&dest.http, 60);

    assert_eq!(
        raw_log(&dest_addr, 60)?,
        raw_log(&source_addr, 60)?,
        "the mirror serves the source's batch bytes, offsets and epochs included"
    );
    // A consumer that fetched the last batch asks for its epoch's end: the
    // mirror must know that epoch, or the client reads it as truncation.
    let log = raw_log(&dest_addr, 60)?;
    let batches = camus_protocol::batch::inspect(&log, camus_protocol::batch::MAX_BATCH_CEILING)
        .map_err(|err| format!("{err:?}"))?;
    let last_at = log.len() - batches.last().ok_or("no batch")?.len;
    let last_epoch = camus_protocol::batch::leader_epoch(log.get(last_at..).ok_or("short")?);
    let current = support::partition_epoch(&dest_addr);
    assert!(
        current >= last_epoch,
        "mirror epoch {current} is below a copied batch's {last_epoch}"
    );
    assert_eq!(
        support::epoch_end(&dest_addr, current, last_epoch),
        (0, last_epoch, 60)
    );
    drop(sources);
    assert_eq!(
        support::read_all(&dest_addr, 60),
        sent,
        "the mirror serves everything it published with every source broker gone"
    );
    assert_eq!(produce(&dest_addr, "refused").0, INVALID_TOPIC);

    let metrics = client
        .get(format!("http://{}/metrics", dest.http))
        .send()?
        .text()?;
    let copied = metrics
        .lines()
        .find_map(|line| line.strip_prefix("camus_mirror_copied_bytes_total "))
        .ok_or("no copied-bytes series")?
        .parse::<u64>()?;
    assert!(copied > 0, "{copied} bytes copied");

    // A mirror's retention is its own: it moves the mirror's log start,
    // though a client's DeleteRecords may not. The test records' timestamps
    // are old, so retention runs only from here on.
    let opts = dest.opts.clone().env("CAMUS_RETENTION_CHECK_MS", "200");
    dest.restart_with(opts);
    support::wait_settled(&[&dest]);
    let mut retention = AlterConfigsResource::default();
    retention.resource_type = 2;
    retention.resource_name = StrBytes::from_static_str(support::TOPIC);
    retention.configs = vec![AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.bytes"))
        .with_value(Some(StrBytes::from_static_str("1")))];
    let mut stream = connect(&dest_addr);
    send(
        &mut stream,
        ApiKey::AlterConfigs,
        2,
        &AlterConfigsRequest::default().with_resources(vec![retention]),
    );
    assert_eq!(
        recv::<AlterConfigsResponse>(&mut stream, 2).responses[0].error_code,
        0
    );
    let raw = support::read_key(
        &dest.http,
        &camus_metadata::topic_storage_key(support::TOPIC),
    )
    .ok_or("no topic")?;
    let id = camus_metadata::parse_topic(&raw)?.id;
    let log_start = || -> Result<i64, Box<dyn std::error::Error>> {
        let raw = support::read_key(&dest.http, &camus_metadata::partition_storage_key(&id, 0))
            .ok_or("no partition")?;
        Ok(camus_metadata::parse_partition(&raw)?.log_start)
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while log_start()? == 0 {
        if std::time::Instant::now() > deadline {
            return Err("retention never moved the mirror's log start".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    Ok(())
}

/// The value of the unlabelled series `name` on `http`.
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

/// `camus_mirror_stalled_partitions` on `http`.
fn stalled(http: &str) -> Result<u64, Box<dyn std::error::Error>> {
    series(http, "camus_mirror_stalled_partitions")
}

/// A mirror whose source deleted records it had not copied stalls, and a
/// reset starts it again at the source's log start.
#[test]
fn a_stalled_mirror_starts_again_after_a_reset() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror reset test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-reset-src-{pid}/");
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &[]);
    let source_addr = kafka_addr(&source);
    for at in 0..40 {
        assert_eq!(produce(&source_addr, &format!("record-{at}")), (0, at));
    }
    support::wait_sealed(&source.http, 40);

    let dest_dir = tempfile::tempdir()?;
    let dest_opts = |copy_ms: &str| {
        Opts::default()
            .env("CAMUS_S3_PREFIX", &format!("mirror-reset-dst-{pid}/"))
            .env("CAMUS_MIRROR_COPY_MS", copy_ms)
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret")
    };
    let mut dest = support::Node::with(1, dest_dir.path(), dest_opts("200"));
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    let client = reqwest::blocking::Client::new();
    let link = MirrorLink {
        alias: "us-east".into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link)
            .send()
    })?;
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": support::TOPIC }))
            .send()
    })?;
    support::wait_sealed(&dest.http, 40);

    // Pause copying, then let the source move its log start past 40.
    dest.restart_with(dest_opts("3600000"));
    support::wait_settled(&[&dest]);
    for at in 40..60 {
        assert_eq!(produce(&source_addr, &format!("record-{at}")), (0, at));
    }
    support::wait_sealed(&source.http, 60);
    let mut cut = DeleteRecordsTopic::default();
    cut.name = TopicName(StrBytes::from_static_str(support::TOPIC));
    cut.partitions = vec![DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(50)];
    let mut stream = connect(&source_addr);
    send(
        &mut stream,
        ApiKey::DeleteRecords,
        2,
        &DeleteRecordsRequest::default()
            .with_topics(vec![cut])
            .with_timeout_ms(5_000),
    );
    let deleted = recv::<DeleteRecordsResponse>(&mut stream, 2);
    assert_eq!(deleted.topics[0].partitions[0].error_code, 0);
    // Wait until `CURRENT`, which the copier reads, names a snapshot taken
    // after the delete. An older one still names the records below 50, and
    // their objects are still there, so copying from it is fine but would
    // not stall.
    let applied: u64 = client
        .get(format!("http://{}/metrics", source.http))
        .send()?
        .text()?
        .lines()
        .find_map(|line| line.strip_prefix("camus_raft_last_applied "))
        .ok_or("no applied series")?
        .parse()?;
    let bucket = camus_storage::Bucket::connect(&camus_storage::BucketConfig {
        endpoint: "http://127.0.0.1:9100".into(),
        bucket: "camus".into(),
        region: "us-east-1".into(),
        access_key: "camus".into(),
        secret_key: "camus-dev-secret".into(),
        prefix: source_prefix.clone(),
        virtual_hosted: false,
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let (pointer, _) = runtime.block_on(bucket.get_tail("meta/snap/CURRENT", 1024))?;
        let named = String::from_utf8(pointer.to_vec())?;
        // `meta/snap/{term}-{index}.snap`
        let index: u64 = named
            .trim()
            .trim_end_matches(".snap")
            .rsplit('-')
            .next()
            .ok_or("no index")?
            .parse()?;
        if index >= applied {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Err(format!("CURRENT stayed at {named}").into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    dest.restart_with(dest_opts("200"));
    support::wait_settled(&[&dest]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while stalled(&dest.http)? != 1 {
        if std::time::Instant::now() > deadline {
            return Err("the mirror never reported a stall".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert_eq!(
        support::sealed_end(&dest.http),
        40,
        "a stalled mirror does not jump"
    );

    let reset = |accept: bool| {
        client
            .post(format!("http://{}/admin/mirror/reset", dest.http))
            .json(&serde_json::json!({
                "topic": support::TOPIC,
                "partition": 0,
                "i_accept_data_loss": accept,
            }))
            .send()
    };
    assert_eq!(reset(false)?.status(), StatusCode::BAD_REQUEST);
    assert_eq!(reset(true)?.status(), StatusCode::OK);
    support::wait_sealed(&dest.http, 60);
    let raw = support::read_key(
        &dest.http,
        &camus_metadata::topic_storage_key(support::TOPIC),
    )
    .ok_or("no topic")?;
    let id = camus_metadata::parse_topic(&raw)?.id;
    let part = support::read_key(&dest.http, &camus_metadata::partition_storage_key(&id, 0))
        .ok_or("no partition")?;
    assert_eq!(camus_metadata::parse_partition(&part)?.log_start, 50);
    let (code, values) = support::fetch_at(&kafka_addr(&dest), 50);
    assert_eq!(code, 0);
    assert_eq!(values.first().map(String::as_str), Some("record-50"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while stalled(&dest.http)? != 0 {
        if std::time::Instant::now() > deadline {
            return Err("the mirror stayed stalled after the reset".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    Ok(())
}

/// A transactional producer of `TOPIC` partition 0, for the source.
mod txn {
    use bytes::{Bytes, BytesMut};
    use camus_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use camus_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
    use camus_protocol::messages::end_txn_response::EndTxnResponse;
    use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use camus_protocol::messages::produce_response::ProduceResponse;
    use camus_protocol::messages::{
        AddPartitionsToTxnRequest, ApiKey, EndTxnRequest, InitProducerIdRequest,
        InitProducerIdResponse, ProduceRequest, TopicName, TransactionalId,
    };
    use camus_protocol::protocol::StrBytes;
    use camus_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PARTITION_LEADER_EPOCH,
    };
    use std::net::TcpStream;

    use super::support::{recv, send, TOPIC};

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    fn id() -> TransactionalId {
        TransactionalId(StrBytes::from_static_str("mirror-checkout"))
    }

    pub struct Producer {
        stream: TcpStream,
        id: i64,
        epoch: i16,
        sequence: i32,
    }

    impl Producer {
        pub fn init(addr: &str) -> Result<Self> {
            let mut stream = super::connect(addr);
            send(
                &mut stream,
                ApiKey::InitProducerId,
                4,
                &InitProducerIdRequest::default()
                    .with_transactional_id(Some(id()))
                    .with_transaction_timeout_ms(60_000)
                    .with_producer_id((-1).into())
                    .with_producer_epoch(-1),
            );
            let init = recv::<InitProducerIdResponse>(&mut stream, 4);
            if init.error_code != 0 {
                return Err(format!("InitProducerId: {}", init.error_code).into());
            }
            Ok(Self {
                stream,
                id: init.producer_id.0,
                epoch: init.producer_epoch,
                sequence: 0,
            })
        }

        /// Begin a transaction on partition 0 and append `value` to it.
        /// Returns the batch's offset.
        pub fn send(&mut self, value: &str) -> Result<i64> {
            send(
                &mut self.stream,
                ApiKey::AddPartitionsToTxn,
                3,
                &AddPartitionsToTxnRequest::default()
                    .with_v3_and_below_transactional_id(id())
                    .with_v3_and_below_producer_id(self.id.into())
                    .with_v3_and_below_producer_epoch(self.epoch)
                    .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                        .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                        .with_partitions(vec![0])]),
            );
            let added = recv::<AddPartitionsToTxnResponse>(&mut self.stream, 3);
            let code =
                added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code;
            if code != 0 {
                return Err(format!("AddPartitionsToTxn: {code}").into());
            }
            let records = vec![Record {
                transactional: true,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: self.id,
                producer_epoch: self.epoch,
                sequence: self.sequence,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                timestamp: 9,
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::copy_from_slice(value.as_bytes())),
                headers: Default::default(),
            }];
            let mut buf = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut buf,
                &records,
                &RecordEncodeOptions {
                    version: 2,
                    compression: Compression::None,
                },
            )?;
            self.sequence += 1;
            let mut topic = TopicProduceData::default();
            topic.name = TopicName(StrBytes::from_static_str(TOPIC));
            topic.partition_data = vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(buf.freeze()))];
            send(
                &mut self.stream,
                ApiKey::Produce,
                9,
                &ProduceRequest::default()
                    .with_acks(-1)
                    .with_timeout_ms(5_000)
                    .with_transactional_id(Some(id()))
                    .with_topic_data(vec![topic]),
            );
            let produced = recv::<ProduceResponse>(&mut self.stream, 9);
            let part = &produced.responses[0].partition_responses[0];
            if part.error_code != 0 {
                return Err(format!("Produce: {}", part.error_code).into());
            }
            Ok(part.base_offset)
        }

        pub fn end(&mut self, commit: bool) -> Result<()> {
            send(
                &mut self.stream,
                ApiKey::EndTxn,
                3,
                &EndTxnRequest::default()
                    .with_transactional_id(id())
                    .with_producer_id(self.id.into())
                    .with_producer_epoch(self.epoch)
                    .with_committed(commit),
            );
            let ended = recv::<EndTxnResponse>(&mut self.stream, 3);
            if ended.error_code != 0 {
                return Err(format!("EndTxn: {}", ended.error_code).into());
            }
            Ok(())
        }
    }
}

/// `(offset, value)` for each record a fetch of `TOPIC` partition 0 from 0
/// returns at `isolation` (0 uncommitted, 1 committed), control records
/// left out as a client leaves them out.
fn records_at(addr: &str, isolation: i8) -> Result<Vec<(i64, String)>, Box<dyn std::error::Error>> {
    use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use camus_protocol::messages::fetch_response::FetchResponse;
    use camus_protocol::messages::FetchRequest;
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = 0;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str(support::TOPIC));
    topic.partitions = vec![part];
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::Fetch,
        11,
        &FetchRequest::default()
            .with_max_wait_ms(50)
            .with_isolation_level(isolation)
            .with_topics(vec![topic]),
    );
    let fetched = recv::<FetchResponse>(&mut stream, 11);
    let Some(mut bytes) = fetched.responses[0].partitions[0].records.clone() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for set in camus_protocol::records::RecordBatchDecoder::decode_all(&mut bytes)? {
        for record in set.records {
            if record.control {
                continue;
            }
            let value = record.value.ok_or("a record without a value")?;
            out.push((record.offset, String::from_utf8(value.to_vec())?));
        }
    }
    Ok(out)
}

/// A mirror copies a transactional topic once the source ends each
/// transaction, with its spans, so both isolation levels read as on the
/// source, and waits at a transaction the source has not ended.
#[test]
fn a_mirror_copies_ended_transactions_and_waits_at_an_open_one() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror transaction test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-txn-src-{pid}/");
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &[]);
    let source_addr = kafka_addr(&source);
    let mut producer = txn::Producer::init(&source_addr)?;
    assert_eq!(producer.send("aborted")?, 0);
    producer.end(false)?;
    assert_eq!(producer.send("committed")?, 1);
    producer.end(true)?;
    assert_eq!(produce(&source_addr, "plain"), (0, 2));
    assert_eq!(producer.send("open")?, 3);

    let dest_dir = tempfile::tempdir()?;
    let dest = support::Node::with(
        1,
        dest_dir.path(),
        Opts::default()
            .env("CAMUS_S3_PREFIX", &format!("mirror-txn-dst-{pid}/"))
            .env("CAMUS_MIRROR_COPY_MS", "200")
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret"),
    );
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    let client = reqwest::blocking::Client::new();
    let link = MirrorLink {
        alias: "us-east".into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link)
            .send()
    })?;
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": support::TOPIC }))
            .send()
    })?;

    // The source has sealed the open transaction's batch, so the copier
    // reaches it, and waits there rather than stopping for want of data.
    support::wait_sealed(&source.http, 4);
    support::wait_sealed(&dest.http, 3);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&dest.http, "camus_mirror_waiting_partitions")? != 1 {
        if std::time::Instant::now() > deadline {
            return Err("the mirror never reported waiting".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert_eq!(
        support::sealed_end(&dest.http),
        3,
        "waits at the open transaction"
    );
    assert_eq!(stalled(&dest.http)?, 0, "waiting is not a stall");
    let dest_addr = kafka_addr(&dest);
    let committed = vec![(1, "committed".to_string()), (2, "plain".to_string())];
    assert_eq!(records_at(&dest_addr, 1)?, committed);
    assert_eq!(
        records_at(&source_addr, 1)?,
        committed,
        "the source reads the same"
    );
    let uncommitted: Vec<(i64, String)> = records_at(&dest_addr, 0)?;
    assert_eq!(
        uncommitted
            .iter()
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    producer.end(true)?;
    support::wait_sealed(&dest.http, 4);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&dest.http, "camus_mirror_waiting_partitions")? != 0 {
        if std::time::Instant::now() > deadline {
            return Err("the mirror stayed waiting".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    let mut after = committed;
    after.push((3, "open".to_string()));
    assert_eq!(records_at(&dest_addr, 1)?, after);
    assert_eq!(records_at(&source_addr, 1)?, after);
    Ok(())
}

/// Commit `offset` for `group` on `TOPIC` partition 0, outside membership.
fn commit(addr: &str, group: &'static str, offset: i64) -> Result<(), Box<dyn std::error::Error>> {
    use camus_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use camus_protocol::messages::offset_commit_response::OffsetCommitResponse;
    use camus_protocol::messages::{GroupId, OffsetCommitRequest};
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::OffsetCommit,
        8,
        &OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str(group)))
            .with_generation_id_or_member_epoch(-1)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(support::TOPIC)))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(offset)])]),
    );
    let code = recv::<OffsetCommitResponse>(&mut stream, 8).topics[0].partitions[0].error_code;
    if code != 0 {
        return Err(format!("OffsetCommit: {code}").into());
    }
    Ok(())
}

/// `group`'s committed offset on `TOPIC` partition 0, or -1.
fn committed(addr: &str, group: &'static str) -> i64 {
    committed_with_epoch(addr, group).0
}

/// The group ids `ListGroups` reports on `addr`.
fn listed_groups(addr: &str) -> Vec<String> {
    use camus_protocol::messages::list_groups_response::ListGroupsResponse;
    use camus_protocol::messages::ListGroupsRequest;
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::ListGroups,
        4,
        &ListGroupsRequest::default(),
    );
    recv::<ListGroupsResponse>(&mut stream, 4)
        .groups
        .iter()
        .map(|group| group.group_id.0.to_string())
        .collect()
}

/// `group`'s committed offset and its leader epoch on `TOPIC` partition 0.
fn committed_with_epoch(addr: &str, group: &'static str) -> (i64, i32) {
    use camus_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use camus_protocol::messages::offset_fetch_response::OffsetFetchResponse;
    use camus_protocol::messages::{GroupId, OffsetFetchRequest};
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::OffsetFetch,
        7,
        &OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str(group)))
            .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(support::TOPIC)))
                .with_partition_indexes(vec![0])])),
    );
    let response = recv::<OffsetFetchResponse>(&mut stream, 7);
    let part = &response.topics[0].partitions[0];
    (part.committed_offset, part.committed_leader_epoch)
}

/// Wait until `group`'s offset on `addr` is `offset`.
fn wait_committed(
    addr: &str,
    group: &'static str,
    offset: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while committed(addr, group) != offset {
        if std::time::Instant::now() > deadline {
            return Err(
                format!("{group} never reached {offset}: {}", committed(addr, group)).into(),
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    Ok(())
}

/// A listed group's committed offsets follow the source onto the mirror,
/// clamped to what the mirror holds, and never move a group backward.
#[test]
fn a_mirror_syncs_a_groups_offsets_forward_only() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror group sync test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-groups-src-{pid}/");
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &[]);
    let source_addr = kafka_addr(&source);
    for at in 0..40 {
        assert_eq!(produce(&source_addr, &format!("record-{at}")), (0, at));
    }
    support::wait_sealed(&source.http, 40);
    commit(&source_addr, "billing", 25)?;

    let dest_dir = tempfile::tempdir()?;
    let dest = support::Node::with(
        1,
        dest_dir.path(),
        Opts::default()
            .env("CAMUS_S3_PREFIX", &format!("mirror-groups-dst-{pid}/"))
            .env("CAMUS_MIRROR_COPY_MS", "200")
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret"),
    );
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    let client = reqwest::blocking::Client::new();
    let link = MirrorLink {
        alias: "us-east".into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link)
            .send()
    })?;
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": support::TOPIC }))
            .send()
    })?;
    let set_groups = |alias: &str| {
        client
            .post(format!("http://{}/admin/mirror/groups", dest.http))
            .json(&serde_json::json!({ "alias": alias, "groups": ["billing"] }))
            .send()
    };
    // Load the mirror's coordinator before the sync, so a group it lists
    // later can only have come from the sync telling it.
    let dest_addr = kafka_addr(&dest);
    assert!(!listed_groups(&dest_addr).contains(&"billing".to_string()));
    assert_eq!(set_groups("nope")?.status(), StatusCode::BAD_REQUEST);
    assert_eq!(set_groups("us-east")?.status(), StatusCode::OK);
    support::wait_sealed(&dest.http, 40);
    wait_committed(&dest_addr, "billing", 25)?;
    assert!(
        listed_groups(&dest_addr).contains(&"billing".to_string()),
        "a synced group is listed on the mirror"
    );

    commit(&source_addr, "billing", 30)?;
    wait_committed(&dest_addr, "billing", 30)?;

    // A consumer on the mirror moved ahead; the source going back does not
    // pull it back.
    commit(&dest_addr, "billing", 35)?;
    commit(&source_addr, "billing", 32)?;
    std::thread::sleep(std::time::Duration::from_millis(2_000));
    assert_eq!(committed(&dest_addr, "billing"), 35);

    // An offset past what the mirror holds is clamped to its end.
    commit(&source_addr, "billing", 1_000)?;
    wait_committed(&dest_addr, "billing", 40)?;
    assert_eq!(
        committed_with_epoch(&dest_addr, "billing").1,
        -1,
        "a clamped offset names no epoch the mirror holds"
    );
    Ok(())
}

/// A promoted mirror takes produces from where it stopped copying, copies
/// no more, and the fork check sees a source that went on past that point.
#[test]
fn a_promoted_mirror_is_writable_and_a_fork_is_reported() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror promotion test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-promote-src-{pid}/");
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &[]);
    let source_addr = kafka_addr(&source);
    for at in 0..40 {
        assert_eq!(produce(&source_addr, &format!("record-{at}")), (0, at));
    }
    support::wait_sealed(&source.http, 40);

    let dest_dir = tempfile::tempdir()?;
    let dest = support::Node::with(
        1,
        dest_dir.path(),
        Opts::default()
            .env("CAMUS_S3_PREFIX", &format!("mirror-promote-dst-{pid}/"))
            .env("CAMUS_MIRROR_COPY_MS", "200")
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret"),
    );
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    let client = reqwest::blocking::Client::new();
    let link = MirrorLink {
        alias: "us-east".into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link)
            .send()
    })?;
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": support::TOPIC }))
            .send()
    })?;
    support::wait_sealed(&dest.http, 40);

    let promote = |accept: bool| {
        client
            .post(format!("http://{}/admin/mirror/promote", dest.http))
            .json(&serde_json::json!({ "topic": support::TOPIC, "i_accept_data_loss": accept }))
            .send()
    };
    assert_eq!(promote(false)?.status(), StatusCode::BAD_REQUEST);
    assert_eq!(promote(true)?.status(), StatusCode::OK);
    assert_eq!(
        promote(true)?.status(),
        StatusCode::OK,
        "a retried promote succeeds"
    );

    let dest_addr = kafka_addr(&dest);
    assert_eq!(
        produce(&dest_addr, "after-0"),
        (0, 40),
        "writable from where it stopped"
    );
    let (code, values) = support::fetch_at(&dest_addr, 39);
    assert_eq!(code, 0);
    assert_eq!(values.first().map(String::as_str), Some("record-39"));

    // Promotion starts a new epoch at 40: the copied epoch ends there, and
    // the record produced at 40 carries a newer one.
    let log = raw_log(&dest_addr, 41)?;
    let batches = camus_protocol::batch::inspect(&log, camus_protocol::batch::MAX_BATCH_CEILING)
        .map_err(|err| format!("{err:?}"))?;
    let mut epochs = Vec::new();
    let mut at = 0;
    for batch in &batches {
        epochs.push(camus_protocol::batch::leader_epoch(
            log.get(at..).ok_or("short")?,
        ));
        at += batch.len;
    }
    let (copied, produced) = (epochs[39], epochs[40]);
    assert!(
        produced > copied,
        "epoch {produced} at 40 is not past the copied {copied}"
    );
    let current = support::partition_epoch(&dest_addr);
    assert_eq!(
        support::epoch_end(&dest_addr, current, copied),
        (0, copied, 40)
    );

    // The old source writes on: the same offsets now hold other records.
    for at in 40..42 {
        assert_eq!(produce(&source_addr, &format!("source-{at}")), (0, at));
    }
    support::wait_sealed(&source.http, 42);
    let check = || -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        Ok(client
            .get(format!("http://{}/admin/mirror/fork-check", dest.http))
            .query(&[("topic", support::TOPIC)])
            .send()?
            .json()?)
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let report = loop {
        let report = check()?;
        if report["forked"].as_bool() == Some(true) {
            break report;
        }
        if std::time::Instant::now() > deadline {
            return Err(format!("no fork reported: {report}").into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    };
    assert_eq!(report["partitions"][0]["promoted_at"], 40);
    assert_eq!(report["partitions"][0]["source_end"], 42);
    assert_eq!(report["unknown"], false);
    assert!(report["source_snapshot"]
        .as_str()
        .is_some_and(|key| key.starts_with("meta/snap/")));
    assert_eq!(
        support::sealed_end(&dest.http),
        41,
        "the source's later records are not copied in"
    );
    let removed = client
        .delete(format!("http://{}/admin/mirror/links/us-east", dest.http))
        .send()?
        .status();
    assert_eq!(removed, StatusCode::OK, "no mirror names the link any more");
    Ok(())
}

/// Every footer entry of `topic`'s extents on `http`, from the bucket under
/// `prefix`: `(start, end, stamp)`, and each extent's offsets and stamp
/// range.
#[allow(clippy::type_complexity)]
fn footer_stamps(
    http: &str,
    prefix: &str,
    topic: &str,
) -> Result<
    (
        Vec<(i64, i64, Option<i64>)>,
        Vec<(i64, i64, Option<camus_metadata::StampRange>)>,
    ),
    Box<dyn std::error::Error>,
> {
    let bucket = camus_storage::Bucket::connect(&camus_storage::BucketConfig {
        endpoint: "http://127.0.0.1:9100".into(),
        bucket: "camus".into(),
        region: "us-east-1".into(),
        access_key: "camus".into(),
        secret_key: "camus-dev-secret".into(),
        prefix: prefix.into(),
        virtual_hosted: false,
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let raw =
        support::read_key(http, &camus_metadata::topic_storage_key(topic)).ok_or("no topic")?;
    let id = camus_metadata::parse_topic(&raw)?.id;
    let topic_id = *uuid::Uuid::parse_str(&id)?.as_bytes();
    let mut entries = Vec::new();
    let mut ranges = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (_, raw) in support::scan_keys(http, &format!("ext/{id}/")) {
        let extent: camus_metadata::Extent = serde_json::from_str(&raw)?;
        ranges.push((extent.start, extent.end, extent.stamps));
        if !seen.insert(extent.key.clone()) {
            continue;
        }
        let (body, _) = runtime.block_on(bucket.get_tail(&extent.key, 64 << 20))?;
        for entry in camus_uploader::decode_footer(&body)? {
            if entry.topic_id == topic_id && entry.partition == 0 {
                entries.push((entry.start, entry.end, entry.stamp));
            }
        }
    }
    entries.sort();
    Ok((entries, ranges))
}

/// With stamps on at both ends, the copier gives each copied batch its
/// source stamp, in the destination's footers, and each mirror extent the
/// range of its batches' stamps (ADR 0018, phase 3).
#[test]
fn a_mirror_carries_the_sources_stamps() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror stamps test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let source_prefix = format!("mirror-stamps-src-{pid}/");
    let dest_prefix = format!("mirror-stamps-dst-{pid}/");
    let stamps = [
        ("CAMUS_CLOCK_STAMPS", "true"),
        ("CAMUS_FAILPOINT", "assumed-clock"),
    ];
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &stamps);
    let source_addr = kafka_addr(&source);
    // Stamping starts once this broker has recorded that it reads stamps.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&source.http, "camus_stamped_objects")? == 0
        || series(&source.http, "camus_wal_stamping_shards")? == 0
    {
        if std::time::Instant::now() > deadline {
            return Err("the source never started stamping".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    for at in 0..10 {
        assert_eq!(produce(&source_addr, &format!("record-{at}")), (0, at));
    }
    support::wait_sealed(&source.http, 10);

    let dest_dir = tempfile::tempdir()?;
    // The destination has no `orders` of its own: the mirror takes the name.
    let dest = support::Node::with(
        1,
        dest_dir.path(),
        Opts::default()
            .env("CAMUS_S3_PREFIX", &dest_prefix)
            .env("CAMUS_CLOCK_STAMPS", "true")
            .env("CAMUS_FAILPOINT", "assumed-clock")
            .env("CAMUS_MIRROR_COPY_MS", "200")
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret"),
    );
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&dest.http, "camus_stamped_objects")? == 0 {
        if std::time::Instant::now() > deadline {
            return Err("the destination never took stamped objects".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let client = reqwest::blocking::Client::new();
    let link = MirrorLink {
        alias: "us-east".into(),
        endpoint: "http://127.0.0.1:9100".into(),
        region: "us-east-1".into(),
        bucket: "camus".into(),
        prefix: source_prefix.clone(),
        virtual_host: false,
    };
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/links", dest.http))
            .json(&link)
            .send()
    })?;
    until_ok(|| {
        client
            .post(format!("http://{}/admin/mirror/topics", dest.http))
            .json(&serde_json::json!({ "alias": "us-east", "topic": support::TOPIC }))
            .send()
    })?;
    support::wait_sealed(&dest.http, 10);

    let (from, _) = footer_stamps(&source.http, &source_prefix, support::TOPIC)?;
    let mut at_source = std::collections::BTreeMap::new();
    for (start, end, stamp) in from {
        for offset in start..end {
            at_source.insert(offset, stamp.ok_or("an unstamped source batch")?);
        }
    }
    let (copied, ranges) = footer_stamps(&dest.http, &dest_prefix, support::TOPIC)?;
    assert!(!copied.is_empty(), "the mirror has objects");
    let mut covered = 0;
    for (start, end, stamp) in copied {
        let stamp = stamp.ok_or("an unstamped mirror piece")?;
        for offset in start..end {
            assert_eq!(
                at_source.get(&offset),
                Some(&stamp),
                "offset {offset} keeps its source stamp"
            );
            covered += 1;
        }
    }
    assert_eq!(covered, 10, "every copied offset is in a stamped piece");
    for (start, end, range) in ranges {
        let range = range.ok_or("an unstamped mirror extent")?;
        let inside = || at_source.range(start..end).map(|(_, stamp)| *stamp);
        assert_eq!(
            (Some(range.min), Some(range.max)),
            (inside().min(), inside().max()),
            "extent {start}..{end} takes its batches' range"
        );
    }

    // The source's frontiers pass every stamp once its batches are sealed,
    // and the mirror, caught up with a snapshot carrying them, records it.
    let highest = at_source.values().copied().max().ok_or("no stamps")?;
    let raw = support::read_key(
        &dest.http,
        &camus_metadata::topic_storage_key(support::TOPIC),
    )
    .ok_or("no mirror topic")?;
    let key = camus_metadata::mirror::applied_key(&camus_metadata::parse_topic(&raw)?.id);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let applied = loop {
        let applied = support::read_key(&dest.http, &key)
            .map(|raw| raw.parse::<i64>())
            .transpose()?;
        if let Some(applied) = applied.filter(|applied| *applied > highest) {
            break applied;
        }
        if std::time::Instant::now() > deadline {
            return Err(format!("the applied watermark {applied:?} never passed {highest}").into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    };
    let metrics = reqwest::blocking::get(format!("http://{}/metrics", dest.http))?.text()?;
    let shown = metrics
        .lines()
        .find_map(|line| line.strip_prefix("camus_mirror_applied_watermark_us{alias=\"us-east\"} "))
        .ok_or("no applied watermark series")?
        .parse::<i64>()?;
    assert!(
        shown >= applied,
        "the metric shows {shown}, below {applied}"
    );
    Ok(())
}

/// Produce `value` to partition 0 of `topic`: `(error code, base offset)`.
fn produce_named(addr: &str, topic: &'static str, value: &str) -> (i16, i64) {
    let mut stream = connect(addr);
    let mut data = TopicProduceData::default();
    data.name = TopicName(StrBytes::from_static_str(topic));
    data.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch(value)))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![data]),
    );
    let response = recv::<ProduceResponse>(&mut stream, 9);
    let partition = &response.responses[0].partition_responses[0];
    (partition.error_code, partition.base_offset)
}

/// Unix microseconds now.
fn now_us() -> Result<i64, Box<dyn std::error::Error>> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?)
}

/// `topic`'s id and partition 0's next offset on `http`.
fn next_offset(http: &str, topic: &str) -> Result<(String, i64), Box<dyn std::error::Error>> {
    let raw =
        support::read_key(http, &camus_metadata::topic_storage_key(topic)).ok_or("no topic")?;
    let id = camus_metadata::parse_topic(&raw)?.id;
    let raw = support::read_key(http, &camus_metadata::partition_storage_key(&id, 0))
        .ok_or("no partition")?;
    Ok((id, camus_metadata::parse_partition(&raw)?.next_offset))
}

/// The phase 3 acceptance test (ADR 0018): two sources, one destination.
/// Records go to the sources in a known real-time order. A cut between two
/// of them keeps, in both mirrors at once, exactly the records produced
/// before it, and promotes both.
#[test]
fn a_cut_keeps_exactly_what_came_before_it_across_sources() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("mirror cut test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let pid = std::process::id();
    let stamps = [
        ("CAMUS_CLOCK_STAMPS", "true"),
        ("CAMUS_FAILPOINT", "assumed-clock"),
    ];
    let prefixes = [format!("cut-a-{pid}/"), format!("cut-b-{pid}/")];
    let dirs = [tempfile::tempdir()?, tempfile::tempdir()?];
    // Source A checks for uploads every three seconds and holds a small
    // one three seconds from its first batch, so its three records, within
    // about half a second, share one object and extent, and the cut
    // falls inside it: the mirror's extent keeps a front read from its
    // footer. Source B's land one per extent.
    let a = spawn_under(
        dirs[0].path(),
        &prefixes[0],
        &[
            stamps[0],
            stamps[1],
            ("CAMUS_FLUSH_MS", "3000"),
            ("CAMUS_QUIET_FLUSH_MS", "3000"),
        ],
    );
    let b = spawn_under(dirs[1].path(), &prefixes[1], &stamps);
    support::create_topic_named(&kafka_addr(&b), "events", "wal", 1);
    for source in [&a, &b] {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while series(&source.http, "camus_stamped_objects")? == 0
            || series(&source.http, "camus_wal_stamping_shards")? == 0
        {
            if std::time::Instant::now() > deadline {
                return Err("a source never started stamping".into());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    let gap = || std::thread::sleep(std::time::Duration::from_millis(100));
    let (a_addr, b_addr) = (kafka_addr(&a), kafka_addr(&b));
    assert_eq!(produce_named(&a_addr, support::TOPIC, "a0"), (0, 0));
    gap();
    assert_eq!(produce_named(&b_addr, "events", "b0"), (0, 0));
    gap();
    assert_eq!(produce_named(&a_addr, support::TOPIC, "a1"), (0, 1));
    gap();
    // Every stamp so far is at most its append's time plus the 1 ms bound,
    // and every later one at least its own append's time.
    let cut = now_us()? + 2_000;
    gap();
    assert_eq!(produce_named(&b_addr, "events", "b1"), (0, 1));
    gap();
    assert_eq!(produce_named(&a_addr, support::TOPIC, "a2"), (0, 2));
    support::wait_sealed(&a.http, 3);

    let dest_dir = tempfile::tempdir()?;
    let dest = support::Node::with(
        1,
        dest_dir.path(),
        Opts::default()
            .env("CAMUS_S3_PREFIX", &format!("cut-dst-{pid}/"))
            .env("CAMUS_CLOCK_STAMPS", "true")
            .env("CAMUS_FAILPOINT", "assumed-clock")
            .env("CAMUS_MIRROR_COPY_MS", "200")
            .env("CAMUS_MIRROR_US_EAST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_US_EAST_SECRET_KEY", "camus-dev-secret")
            .env("CAMUS_MIRROR_EU_WEST_ACCESS_KEY", "camus")
            .env("CAMUS_MIRROR_EU_WEST_SECRET_KEY", "camus-dev-secret"),
    );
    support::init(std::slice::from_ref(&dest), &[1]);
    support::wait_settled(&[&dest]);
    // Copies carry stamps, and cuts apply, once this broker says it can.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while series(&dest.http, "camus_stamped_objects")? == 0
        || support::read_key(&dest.http, "capability/applies-mirror-cuts/1").is_none()
    {
        if std::time::Instant::now() > deadline {
            return Err("the destination never took stamps or cuts".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let client = reqwest::blocking::Client::new();
    for ((alias, prefix), topic) in ["us-east", "eu-west"]
        .into_iter()
        .zip(&prefixes)
        .zip([support::TOPIC, "events"])
    {
        let link = MirrorLink {
            alias: alias.into(),
            endpoint: "http://127.0.0.1:9100".into(),
            region: "us-east-1".into(),
            bucket: "camus".into(),
            prefix: prefix.clone(),
            virtual_host: false,
        };
        until_ok(|| {
            client
                .post(format!("http://{}/admin/mirror/links", dest.http))
                .json(&link)
                .send()
        })?;
        until_ok(|| {
            client
                .post(format!("http://{}/admin/mirror/topics", dest.http))
                .json(&serde_json::json!({ "alias": alias, "topic": topic }))
                .send()
        })?;
    }
    // Both mirrors hold everything, and are copied past the cut.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let mut ready = true;
        for (topic, end) in [(support::TOPIC, 3), ("events", 2)] {
            let (id, next) = next_offset(&dest.http, topic)?;
            let applied = support::read_key(&dest.http, &camus_metadata::mirror::applied_key(&id))
                .map(|raw| raw.parse::<i64>())
                .transpose()?;
            ready &= next == end && applied.is_some_and(|applied| applied > cut);
        }
        if ready {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Err("the mirrors never caught up past the cut".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    let post_cut = |at: i64| {
        client
            .post(format!("http://{}/admin/mirror/cut", dest.http))
            .json(&serde_json::json!({ "cut": at, "i_accept_data_loss": true }))
            .send()
    };
    // Past what the mirrors are copied to: refused, and nothing changes.
    let refused = post_cut(now_us()? + 60_000_000)?;
    assert_eq!(
        refused.status(),
        StatusCode::CONFLICT,
        "{}",
        refused.text()?
    );
    assert_eq!(next_offset(&dest.http, support::TOPIC)?.1, 3);

    let done = post_cut(cut)?;
    assert_eq!(done.status(), StatusCode::OK, "{}", done.text()?);
    assert_eq!(
        next_offset(&dest.http, support::TOPIC)?.1,
        2,
        "a0 and a1 came before the cut, a2 after"
    );
    assert_eq!(
        next_offset(&dest.http, "events")?.1,
        1,
        "b0 came before the cut, b1 after"
    );
    let dest_addr = kafka_addr(&dest);
    assert_eq!(
        support::read_all(&dest_addr, 2),
        vec!["a0".to_string(), "a1".to_string()]
    );
    let (id, _) = next_offset(&dest.http, support::TOPIC)?;
    let extents = support::scan_keys(&dest.http, &format!("ext/{id}/"));
    assert_eq!(extents.len(), 1, "one extent, cut inside: {extents:?}");
    // Both are promoted: writable, at the offsets after the cut.
    assert_eq!(produce_named(&dest_addr, support::TOPIC, "here"), (0, 2));
    assert_eq!(produce_named(&dest_addr, "events", "here"), (0, 1));
    Ok(())
}
