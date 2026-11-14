//! A transactional produce stays hidden from `read_committed` until `EndTxn`.
//! Abort keeps the batch hidden. `read_uncommitted` sees both.

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::frame;
use camus_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use camus_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::end_txn_response::EndTxnResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::{FetchResponse, PartitionData};
use camus_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use camus_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use camus_protocol::messages::offset_fetch_response::OffsetFetchResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use camus_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
use camus_protocol::messages::{
    AddPartitionsToTxnRequest, ApiKey, CreateTopicsRequest, DescribeTransactionsRequest,
    DescribeTransactionsResponse, EndTxnRequest, FetchRequest, GroupId, InitProducerIdRequest,
    InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse, ListTransactionsRequest,
    ListTransactionsResponse, OffsetFetchRequest, ProduceRequest, RequestHeader, TopicName,
    TransactionalId, TxnOffsetCommitRequest,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH,
};

struct Node {
    child: Child,
    http: String,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_file(path: &Path) -> String {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if let Ok(text) = std::fs::read_to_string(path) {
            if !text.is_empty() {
                return text;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {}", path.display());
}

fn wait_leader(http: &str) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client.get(format!("http://{http}/admin/metrics")).send() {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1)
                    && json.get("wal_leader").and_then(|value| value.as_u64()) == Some(1)
                {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("node 1 did not become leader");
}

fn batch(value: &[u8], producer_id: i64, epoch: i16, sequence: i32) -> Bytes {
    let records = vec![Record {
        transactional: true,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id,
        producer_epoch: epoch,
        sequence,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        timestamp: 9,
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::copy_from_slice(value)),
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
    )
    .unwrap();
    buf.freeze()
}

fn header(key: ApiKey, version: i16, correlation: i32) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("camus-txn")))
}

fn send<M: Encodable>(
    stream: &mut TcpStream,
    key: ApiKey,
    version: i16,
    correlation: i32,
    message: &M,
) {
    let frame =
        frame::encode_request(&header(key, version, correlation), message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> M {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

fn fetch_at(stream: &mut TcpStream, isolation: i8) -> PartitionData {
    let mut fetch_part = FetchPartition::default();
    fetch_part.partition = 0;
    fetch_part.fetch_offset = 0;
    fetch_part.partition_max_bytes = 1024 * 1024;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_static_str("orders"));
    fetch_topic.partitions = vec![fetch_part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        9,
        &FetchRequest::default()
            .with_max_wait_ms(50)
            .with_isolation_level(isolation)
            .with_topics(vec![fetch_topic]),
    );
    recv::<FetchResponse>(stream, 11)
        .responses
        .remove(0)
        .partitions
        .remove(0)
}

#[test]
fn read_committed_hides_an_open_transaction_and_an_abort() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    std::fs::create_dir_all(dir).unwrap();
    let log = std::fs::File::create(dir.join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .env("CAMUS_WAL_SHARDS", "voters")
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            dir.to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .env("CAMUS_FLUSH_MS", "40")
        .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    let node = Node { child, http, kafka };
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);

    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str("orders"));
    created.num_partitions = 1;
    created.replication_factor = 1;
    let mut durability = CreatableTopicConfig::default();
    durability.name = StrBytes::from_static_str("durability.mode");
    durability.value = Some(StrBytes::from_static_str("wal"));
    created.configs = vec![durability];
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");

    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        2,
        &InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("checkout"))))
            .with_transaction_timeout_ms(60_000),
    );
    let started = recv::<InitProducerIdResponse>(&mut stream, 4);
    assert_eq!(started.error_code, 0, "{started:?}");
    let producer = started.producer_id.0;
    let epoch = started.producer_epoch;

    send(
        &mut stream,
        ApiKey::AddPartitionsToTxn,
        3,
        3,
        &AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                "checkout",
            )))
            .with_v3_and_below_producer_id(producer.into())
            .with_v3_and_below_producer_epoch(epoch)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0])]),
    );
    let added = recv::<AddPartitionsToTxnResponse>(&mut stream, 3);
    assert_eq!(
        added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code, 0,
        "{added:?}"
    );

    produce(&mut stream, 4, batch(b"one", producer, epoch, 0), producer);
    let hidden = fetch_at(&mut stream, 1);
    assert_eq!(hidden.error_code, 0, "{hidden:?}");
    assert!(hidden.records.as_ref().is_none_or(|bytes| bytes.is_empty()));
    assert_eq!(hidden.high_watermark, 0);
    let open = fetch_at(&mut stream, 0);
    assert!(
        open.records.as_ref().is_some_and(|bytes| !bytes.is_empty()),
        "{open:?}"
    );
    let described = describe_txn(&mut stream, 20);
    assert_eq!(described.error_code, 0, "{described:?}");
    assert_eq!(described.transaction_state.as_str(), "Ongoing");
    assert_eq!(described.topics[0].partitions, vec![0]);
    assert!(listed(&mut stream, 21, &["Ongoing"])
        .iter()
        .any(|state| state.as_str() == "Ongoing"));

    send(
        &mut stream,
        ApiKey::TxnOffsetCommit,
        3,
        5,
        &TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("checkout")))
            .with_group_id(GroupId(StrBytes::from_static_str("readers")))
            .with_producer_id(producer.into())
            .with_producer_epoch(epoch)
            .with_topics(vec![TxnOffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![TxnOffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(1)])]),
    );
    let staged = recv::<TxnOffsetCommitResponse>(&mut stream, 3);
    assert_eq!(staged.topics[0].partitions[0].error_code, 0, "{staged:?}");
    assert_eq!(committed_offset(&mut stream), -1);

    send(
        &mut stream,
        ApiKey::EndTxn,
        3,
        10,
        &EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("checkout")))
            .with_producer_id(producer.into())
            .with_producer_epoch(epoch)
            .with_committed(true),
    );
    let ended = recv::<EndTxnResponse>(&mut stream, 3);
    assert_eq!(ended.error_code, 0, "{ended:?}");
    assert_eq!(committed_offset(&mut stream), 1);
    let visible = fetch_at(&mut stream, 1);
    assert_eq!(visible.high_watermark, 1, "{visible:?}");
    assert!(visible
        .records
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty()));
    let described = describe_txn(&mut stream, 22);
    assert_eq!(described.transaction_state.as_str(), "CompleteCommit");
    assert!(described.topics.is_empty(), "{described:?}");
    assert!(listed(&mut stream, 23, &["Ongoing"]).is_empty());
    send(
        &mut stream,
        ApiKey::ListTransactions,
        0,
        24,
        &ListTransactionsRequest::default()
            .with_state_filters(vec![StrBytes::from_static_str("Nope")]),
    );
    let unknown = recv::<ListTransactionsResponse>(&mut stream, 0);
    assert_eq!(unknown.error_code, 42, "{unknown:?}");
    assert_eq!(unknown.unknown_state_filters[0].as_str(), "Nope");

    send(
        &mut stream,
        ApiKey::AddPartitionsToTxn,
        3,
        6,
        &AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                "checkout",
            )))
            .with_v3_and_below_producer_id(producer.into())
            .with_v3_and_below_producer_epoch(epoch)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0])]),
    );
    let added = recv::<AddPartitionsToTxnResponse>(&mut stream, 3);
    assert_eq!(
        added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code, 0,
        "{added:?}"
    );
    produce(&mut stream, 7, batch(b"two", producer, epoch, 1), producer);
    let held = fetch_at(&mut stream, 1);
    assert_eq!(held.high_watermark, 1, "{held:?}");

    send(
        &mut stream,
        ApiKey::EndTxn,
        3,
        8,
        &EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("checkout")))
            .with_producer_id(producer.into())
            .with_producer_epoch(epoch)
            .with_committed(false),
    );
    let aborted = recv::<EndTxnResponse>(&mut stream, 3);
    assert_eq!(aborted.error_code, 0, "{aborted:?}");
    let after = fetch_at(&mut stream, 1);
    assert_eq!(after.error_code, 0, "{after:?}");
    assert_eq!(after.high_watermark, 2, "{after:?}");
    assert!(after
        .records
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty()));
}

fn committed_offset(stream: &mut TcpStream) -> i64 {
    let topic = OffsetFetchRequestTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("orders")))
        .with_partition_indexes(vec![0]);
    send(
        stream,
        ApiKey::OffsetFetch,
        7,
        11,
        &OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("readers")))
            .with_topics(Some(vec![topic])),
    );
    let response = recv::<OffsetFetchResponse>(stream, 7);
    assert_eq!(response.error_code, 0, "{response:?}");
    response.topics[0].partitions[0].committed_offset
}

#[test]
fn java_transaction_hides_an_open_batch_and_commits_its_offset() {
    let jars = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-jars");
    if !jars.join("kafka-clients-4.4.0.jar").is_file() {
        support::skipped("java; run the SASL tests once to download the jars");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let dir = root.path();
    let log = std::fs::File::create(dir.join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .env("CAMUS_WAL_SHARDS", "voters")
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            dir.to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .env("CAMUS_FLUSH_MS", "40")
        .env("CAMUS_DEFAULT_DURABILITY", "wal")
        .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    let node = Node {
        child,
        http: http.clone(),
        kafka: kafka.clone(),
    };
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{http}/admin/init"))
        .json(&serde_json::json!([{"id": 1, "addr": http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut registered = false;
    while Instant::now() < deadline {
        if let Ok(body) = client
            .get(format!("http://{}/admin/read", node.http))
            .query(&[("key", "broker/1")])
            .send()
        {
            if body.status().is_success() && !body.text().unwrap_or_default().is_empty() {
                registered = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(registered, "broker address was not registered");

    let classes = dir.join("classes");
    std::fs::create_dir_all(&classes).unwrap();
    let mut cp = vec![classes.display().to_string()];
    for entry in std::fs::read_dir(&jars).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jar") {
            cp.push(path.display().to_string());
        }
    }
    let cp = cp.join(":");
    let source =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/TxnRoundTrip.java");
    let javac = Command::new(jdk("javac"))
        .args(["--release", "17", "-cp", &cp, "-d"])
        .arg(&classes)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        javac.status.success(),
        "javac\n{}",
        String::from_utf8_lossy(&javac.stderr)
    );
    let ran = Command::new(jdk("java"))
        .args(["-cp", &cp, "TxnRoundTrip", &kafka])
        .output()
        .unwrap();
    if !ran.status.success() {
        let broker_log = std::fs::read_to_string(dir.join("broker.log")).unwrap_or_default();
        panic!(
            "java\nstdout:\n{}\nstderr:\n{}\nbroker:\n{}",
            String::from_utf8_lossy(&ran.stdout),
            String::from_utf8_lossy(&ran.stderr),
            broker_log
        );
    }
}

fn describe_txn(
    stream: &mut TcpStream,
    correlation: i32,
) -> camus_protocol::messages::describe_transactions_response::TransactionState {
    send(
        stream,
        ApiKey::DescribeTransactions,
        0,
        correlation,
        &DescribeTransactionsRequest::default()
            .with_transactional_ids(vec![TransactionalId(StrBytes::from_static_str("checkout"))]),
    );
    recv::<DescribeTransactionsResponse>(stream, 0)
        .transaction_states
        .remove(0)
}

fn listed(stream: &mut TcpStream, correlation: i32, states: &[&str]) -> Vec<StrBytes> {
    send(
        stream,
        ApiKey::ListTransactions,
        0,
        correlation,
        &ListTransactionsRequest::default().with_state_filters(
            states
                .iter()
                .map(|state| StrBytes::from_string((*state).to_string()))
                .collect(),
        ),
    );
    recv::<ListTransactionsResponse>(stream, 0)
        .transaction_states
        .into_iter()
        .map(|state| state.transaction_state)
        .collect()
}

fn jdk(tool: &str) -> String {
    let homebrew = format!("/opt/homebrew/opt/openjdk@21/bin/{tool}");
    if Path::new(&homebrew).is_file() {
        homebrew
    } else {
        tool.to_string()
    }
}

fn produce(stream: &mut TcpStream, correlation: i32, raw: Bytes, _producer: i64) {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("orders"));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(raw))];
    send(
        stream,
        ApiKey::Produce,
        9,
        correlation,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("checkout"))))
            .with_topic_data(vec![topic]),
    );
    let produced = recv::<ProduceResponse>(stream, 9);
    let part = &produced.responses[0].partition_responses[0];
    assert_eq!(part.error_code, 0, "{produced:?}");
}

/// One broker with `orders` created, and its connection.
fn start(root: &Path, env: &[(&str, &str)]) -> (Node, TcpStream) {
    let log = std::fs::File::create(root.join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .env("CAMUS_WAL_SHARDS", "voters")
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            root.to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .env("CAMUS_FLUSH_MS", "40")
        .envs(env.iter().copied())
        .stdout(std::fs::File::create(root.join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let http = wait_file(&root.join("listen-addr"));
    let kafka = wait_file(&root.join("kafka-addr"));
    let node = Node { child, http, kafka };
    let init = reqwest::blocking::Client::new()
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str("orders"));
    created.num_partitions = 1;
    created.replication_factor = 1;
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");
    (node, stream)
}

fn init_producer(
    stream: &mut TcpStream,
    correlation: i32,
    timeout_ms: i32,
    current: Option<(i64, i16)>,
) -> InitProducerIdResponse {
    let (producer, epoch) = current.unwrap_or((-1, -1));
    send(
        stream,
        ApiKey::InitProducerId,
        4,
        correlation,
        &InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("checkout"))))
            .with_transaction_timeout_ms(timeout_ms)
            .with_producer_id(producer.into())
            .with_producer_epoch(epoch),
    );
    recv::<InitProducerIdResponse>(stream, 4)
}

#[test]
fn a_transaction_past_its_timeout_is_aborted_and_its_producer_fenced() {
    let root = tempfile::tempdir().unwrap();
    let (_node, mut stream) = start(root.path(), &[("CAMUS_TXN_ABORT_INTERVAL_MS", "100")]);

    let too_long = init_producer(&mut stream, 1, 900_001, None);
    assert_eq!(too_long.error_code, 50, "{too_long:?}");
    let zero = init_producer(&mut stream, 2, 0, None);
    assert_eq!(zero.error_code, 50, "{zero:?}");

    let started = init_producer(&mut stream, 3, 1_000, None);
    assert_eq!(started.error_code, 0, "{started:?}");
    let producer = started.producer_id.0;
    let epoch = started.producer_epoch;
    send(
        &mut stream,
        ApiKey::AddPartitionsToTxn,
        3,
        4,
        &AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                "checkout",
            )))
            .with_v3_and_below_producer_id(producer.into())
            .with_v3_and_below_producer_epoch(epoch)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0])]),
    );
    let added = recv::<AddPartitionsToTxnResponse>(&mut stream, 3);
    assert_eq!(
        added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code, 0,
        "{added:?}"
    );
    produce(&mut stream, 5, batch(b"one", producer, epoch, 0), producer);
    assert_eq!(
        fetch_at(&mut stream, 1).high_watermark,
        0,
        "open batch is hidden"
    );

    // The producer stops without ending the transaction. The coordinator
    // aborts it once its 1 s timeout passes, so read_committed moves on.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if describe_txn(&mut stream, 6).transaction_state.as_str() == "CompleteAbort" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never aborted");
        std::thread::sleep(Duration::from_millis(100));
    }
    let after = fetch_at(&mut stream, 1);
    assert_eq!(after.high_watermark, 1, "{after:?}");

    send(
        &mut stream,
        ApiKey::EndTxn,
        3,
        7,
        &EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("checkout")))
            .with_producer_id(producer.into())
            .with_producer_epoch(epoch)
            .with_committed(true),
    );
    let fenced = recv::<EndTxnResponse>(&mut stream, 3);
    assert_eq!(fenced.error_code, 90, "{fenced:?}");

    // KIP-360: the same producer may recover from the epoch it had.
    let recovered = init_producer(&mut stream, 8, 1_000, Some((producer, epoch)));
    assert_eq!(recovered.error_code, 0, "{recovered:?}");
    assert_eq!(recovered.producer_id.0, producer);
    assert_eq!(recovered.producer_epoch, epoch + 2);
}

fn add_partition(stream: &mut TcpStream, correlation: i32, producer: i64, epoch: i16) {
    send(
        stream,
        ApiKey::AddPartitionsToTxn,
        3,
        correlation,
        &AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                "checkout",
            )))
            .with_v3_and_below_producer_id(producer.into())
            .with_v3_and_below_producer_epoch(epoch)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0])]),
    );
    let added = recv::<AddPartitionsToTxnResponse>(stream, 3);
    assert_eq!(
        added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code, 0,
        "{added:?}"
    );
}

fn end(stream: &mut TcpStream, correlation: i32, producer: i64, epoch: i16, commit: bool) {
    send(
        stream,
        ApiKey::EndTxn,
        3,
        correlation,
        &EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("checkout")))
            .with_producer_id(producer.into())
            .with_producer_epoch(epoch)
            .with_committed(commit),
    );
    assert_eq!(recv::<EndTxnResponse>(stream, 3).error_code, 0);
}

/// Offsets and values a fetch from `offset` returns. Control records are
/// skipped, as a client skips them.
fn read_from(stream: &mut TcpStream, isolation: i8, offset: i64) -> Vec<(i64, String)> {
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = offset;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("orders"));
    topic.partitions = vec![part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        30,
        &FetchRequest::default()
            .with_max_wait_ms(50)
            .with_isolation_level(isolation)
            .with_topics(vec![topic]),
    );
    let data = recv::<FetchResponse>(stream, 11)
        .responses
        .remove(0)
        .partitions
        .remove(0);
    let Some(mut bytes) = data.records else {
        return Vec::new();
    };
    camus_protocol::records::RecordBatchDecoder::decode_all(&mut bytes)
        .unwrap()
        .into_iter()
        .flat_map(|set| set.records)
        .filter(|record| record.offset >= offset && !record.control)
        .map(|record| {
            (
                record.offset,
                String::from_utf8(record.value.unwrap().to_vec()).unwrap(),
            )
        })
        .collect()
}

/// Camus stores no control records, so a `read_committed` consumer that
/// seeks into or past an aborted range must still get the committed records
/// at their own offsets, as with Kafka.
#[test]
fn a_read_committed_seek_skips_an_aborted_range_and_keeps_offsets() {
    let root = tempfile::tempdir().unwrap();
    let (node, mut stream) = start(root.path(), &[]);
    let started = init_producer(&mut stream, 1, 60_000, None);
    assert_eq!(started.error_code, 0);
    let (producer, epoch) = (started.producer_id.0, started.producer_epoch);
    for (round, (value, commit)) in [("a", true), ("b", false), ("c", true)]
        .into_iter()
        .enumerate()
    {
        let base = 10 * (round as i32 + 1);
        add_partition(&mut stream, base, producer, epoch);
        produce(
            &mut stream,
            base + 1,
            batch(value.as_bytes(), producer, epoch, round as i32),
            producer,
        );
        end(&mut stream, base + 2, producer, epoch, commit);
    }
    let committed = |offset| {
        let mut stream = TcpStream::connect(&node.kafka).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        read_from(&mut stream, 1, offset)
    };
    assert_eq!(committed(0), vec![(0, "a".into()), (2, "c".into())]);
    assert_eq!(committed(1), vec![(2, "c".into())], "seek into the abort");
    assert_eq!(committed(2), vec![(2, "c".into())]);
    // read_uncommitted sees the aborted record too, as in Kafka.
    assert_eq!(read_from(&mut stream, 0, 0).len(), 3);
}

/// The latest offset ListOffsets returns for partition 0 at `isolation`.
fn latest(stream: &mut TcpStream, correlation: i32, isolation: i8) -> i64 {
    let topic = ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("orders")))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(0)
            .with_timestamp(-1)]);
    send(
        stream,
        ApiKey::ListOffsets,
        7,
        correlation,
        &ListOffsetsRequest::default()
            .with_replica_id((-1).into())
            .with_isolation_level(isolation)
            .with_topics(vec![topic]),
    );
    let response = recv::<ListOffsetsResponse>(stream, 7);
    response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .map_or(-2, |row| row.offset)
}

/// Kafka writes a marker after each transaction; Camus stores none (ADR
/// 0021). A `read_committed` fetch puts an abort marker at the last offset
/// of an aborted batch, so a consumer moves past an abort at the end of the
/// log. ListOffsets for the latest offset at `read_committed` is the last
/// stable offset, which stops at an open transaction.
#[test]
fn read_committed_marks_an_abort_and_stops_list_offsets_at_an_open_transaction(
) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (_node, mut stream) = start(root.path(), &[]);
    let started = init_producer(&mut stream, 1, 60_000, None);
    assert_eq!(started.error_code, 0);
    let (producer, epoch) = (started.producer_id.0, started.producer_epoch);
    for (round, (value, commit)) in [("a", true), ("b", false)].into_iter().enumerate() {
        let base = 10 * (round as i32 + 1);
        add_partition(&mut stream, base, producer, epoch);
        produce(
            &mut stream,
            base + 1,
            batch(value.as_bytes(), producer, epoch, round as i32),
            producer,
        );
        end(&mut stream, base + 2, producer, epoch, commit);
    }

    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = 0;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("orders"));
    topic.partitions = vec![part];
    send(
        &mut stream,
        ApiKey::Fetch,
        11,
        40,
        &FetchRequest::default()
            .with_max_wait_ms(50)
            .with_isolation_level(1)
            .with_topics(vec![topic]),
    );
    let fetched = recv::<FetchResponse>(&mut stream, 11);
    let mut bytes = fetched
        .responses
        .first()
        .and_then(|topic| topic.partitions.first())
        .and_then(|row| row.records.clone())
        .ok_or("no records")?;
    let records: Vec<(i64, bool)> =
        camus_protocol::records::RecordBatchDecoder::decode_all(&mut bytes)?
            .into_iter()
            .flat_map(|set| set.records)
            .map(|record| (record.offset, record.control))
            .collect();
    assert_eq!(
        records,
        vec![(0, false), (1, true)],
        "the abort is a marker"
    );
    assert_eq!(read_from(&mut stream, 0, 0).len(), 2, "read_uncommitted");

    add_partition(&mut stream, 50, producer, epoch);
    produce(&mut stream, 51, batch(b"c", producer, epoch, 2), producer);
    assert_eq!(latest(&mut stream, 52, 0), 3, "high watermark");
    assert_eq!(latest(&mut stream, 53, 1), 2, "stops at the open batch");
    end(&mut stream, 54, producer, epoch, true);
    assert_eq!(latest(&mut stream, 55, 1), 3, "stable after commit");
    assert_eq!(read_from(&mut stream, 1, 2), vec![(2, "c".to_string())]);
    Ok(())
}

/// In `r2` mode the stored span starts at a transaction's first batch on the
/// partition. `EndTxn` must end it at the log end, so every later batch of
/// the transaction commits or aborts with it. The last stable offset comes
/// from the open span in metadata. Needs MinIO.
#[test]
fn an_r2_transaction_of_several_batches_commits_them_all() -> Result<(), Box<dyn std::error::Error>>
{
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("r2 transaction test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let (_node, mut stream) = start(root.path(), &[("CAMUS_DEFAULT_DURABILITY", "r2")]);
    let started = init_producer(&mut stream, 1, 60_000, None);
    assert_eq!(started.error_code, 0);
    let (producer, epoch) = (started.producer_id.0, started.producer_epoch);
    let mut sequence = 0;
    for (round, (values, commit)) in [(["a1", "a2"], true), (["b1", "b2"], false)]
        .into_iter()
        .enumerate()
    {
        let base = 10 * (round as i32 + 1);
        add_partition(&mut stream, base, producer, epoch);
        for (index, value) in values.into_iter().enumerate() {
            produce(
                &mut stream,
                base + 1 + index as i32,
                batch(value.as_bytes(), producer, epoch, sequence),
                producer,
            );
            sequence += 1;
        }
        end(&mut stream, base + 5, producer, epoch, commit);
    }
    add_partition(&mut stream, 40, producer, epoch);
    produce(
        &mut stream,
        41,
        batch(b"c", producer, epoch, sequence),
        producer,
    );
    assert_eq!(latest(&mut stream, 42, 1), 4, "stops at the open batch");
    end(&mut stream, 43, producer, epoch, true);
    assert_eq!(
        read_from(&mut stream, 1, 0),
        vec![
            (0, "a1".to_string()),
            (1, "a2".to_string()),
            (4, "c".to_string())
        ]
    );
    assert_eq!(latest(&mut stream, 44, 1), 5);
    Ok(())
}
