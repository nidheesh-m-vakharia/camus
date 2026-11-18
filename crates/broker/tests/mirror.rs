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
    let source_dir = tempfile::tempdir()?;
    let source = spawn_under(source_dir.path(), &source_prefix, &[]);
    let source_addr = kafka_addr(&source);
    let mut sent = Vec::new();
    for at in 0..40 {
        let value = format!("record-{at}");
        let (code, offset) = produce(&source_addr, &value);
        assert_eq!((code, offset), (0, at), "produce {at}");
        sent.push(value);
    }
    support::wait_sealed(&source.http, 40);

    // The destination has no `orders` of its own: the mirror takes the name.
    let dest_dir = tempfile::tempdir()?;
    let dest = support::Node::with(
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
    for at in 40..60 {
        let value = format!("record-{at}");
        assert_eq!(produce(&source_addr, &value), (0, at));
        sent.push(value);
    }
    support::wait_sealed(&source.http, 60);
    support::wait_sealed(&dest.http, 60);

    assert_eq!(
        raw_log(&dest_addr, 60)?,
        raw_log(&source_addr, 60)?,
        "the mirror serves the source's batch bytes, offsets and epochs included"
    );
    drop(source);
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
    Ok(())
}
