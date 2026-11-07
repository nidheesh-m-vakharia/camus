//! Concurrent produce, fetch, and transaction races.
//!
//! One broker, `wal` durability. The checks are about lost records, duplicate
//! offsets, and a transactional produce that is acknowledged but then hidden
//! from `read_committed`.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::frame;
use camus_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::{FetchResponse, PartitionData};
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, CreateTopicsRequest,
    EndTxnRequest, EndTxnResponse, FetchRequest, InitProducerIdRequest, InitProducerIdResponse,
    ProduceRequest, RequestHeader, TopicName, TransactionalId,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

struct Node {
    child: Child,
    dir: PathBuf,
    #[allow(dead_code)]
    http: String,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(dir: &Path) -> Node {
    let dir = dir.to_path_buf();
    std::fs::create_dir_all(&dir).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
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
        .stderr(std::fs::File::create(dir.join("broker.log")).unwrap())
        .spawn()
        .unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{http}/admin/init"))
        .json(&serde_json::json!([{"id": 1, "addr": http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client.get(format!("http://{http}/admin/metrics")).send() {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json["current_leader"].as_u64() == Some(1) && json["wal_leader"].as_u64() == Some(1)
                {
                    return Node {
                        child,
                        dir,
                        http,
                        kafka,
                    };
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("node 1 did not become leader");
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

fn connect(addr: &str) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
}

fn header(key: ApiKey, version: i16, correlation: i32) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("camus-stress")))
}

fn send<M: Encodable>(stream: &mut TcpStream, key: ApiKey, version: i16, correlation: i32, message: &M) {
    let frame = frame::encode_request(&header(key, version, correlation), message, version).unwrap();
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

fn batch(value: &[u8], transactional: bool, producer_id: i64, epoch: i16, sequence: i32) -> Bytes {
    let records = vec![Record {
        transactional,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id,
        producer_epoch: if producer_id < 0 { NO_PRODUCER_EPOCH } else { epoch },
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

fn create_topic(stream: &mut TcpStream, name: &'static str) {
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str(name));
    created.num_partitions = 1;
    created.replication_factor = 1;
    let mut durability = CreatableTopicConfig::default();
    durability.name = StrBytes::from_static_str("durability.mode");
    durability.value = Some(StrBytes::from_static_str("wal"));
    created.configs = vec![durability];
    send(
        stream,
        ApiKey::CreateTopics,
        7,
        1,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");
}

fn produce(
    stream: &mut TcpStream,
    topic: &str,
    correlation: i32,
    raw: Bytes,
    txn: Option<&str>,
) -> (i16, i64) {
    let mut row = TopicProduceData::default();
    row.name = TopicName(StrBytes::from_string(topic.to_string()));
    row.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(raw))];
    let mut request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(15_000)
        .with_topic_data(vec![row]);
    if let Some(id) = txn {
        request.transactional_id = Some(TransactionalId(StrBytes::from_string(id.to_string())));
    }
    send(stream, ApiKey::Produce, 9, correlation, &request);
    let produced = recv::<ProduceResponse>(stream, 9);
    let part = &produced.responses[0].partition_responses[0];
    (part.error_code, part.base_offset)
}

fn fetch_once(stream: &mut TcpStream, topic: &str, offset: i64, isolation: i8) -> PartitionData {
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = offset;
    part.partition_max_bytes = 8 * 1024 * 1024;
    let mut topic_row = FetchTopic::default();
    topic_row.topic = TopicName(StrBytes::from_string(topic.to_string()));
    topic_row.partitions = vec![part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        2,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_isolation_level(isolation)
            .with_topics(vec![topic_row]),
    );
    recv::<FetchResponse>(stream, 11)
        .responses
        .remove(0)
        .partitions
        .remove(0)
}

fn records_of(row: PartitionData) -> Vec<Record> {
    let Some(mut bytes) = row.records.filter(|bytes| !bytes.is_empty()) else {
        return Vec::new();
    };
    RecordBatchDecoder::decode_all(&mut bytes)
        .unwrap()
        .into_iter()
        .flat_map(|set| set.records)
        .collect()
}

fn fetch_all(stream: &mut TcpStream, topic: &str) -> Vec<Record> {
    let mut offset = 0i64;
    let mut out = Vec::new();
    for _ in 0..32 {
        let row = fetch_once(stream, topic, offset, 0);
        if row.error_code != 0 {
            panic!("fetch {offset}: {row:?}");
        }
        let high = row.high_watermark;
        let records = records_of(row);
        if records.is_empty() {
            assert!(
                offset >= high,
                "fetch at {offset} was empty below high watermark {high}"
            );
            return out;
        }
        assert_eq!(records[0].offset, offset, "fetch skipped {offset}");
        for pair in records.windows(2) {
            assert_eq!(
                pair[1].offset,
                pair[0].offset + 1,
                "gap at {}",
                pair[0].offset
            );
        }
        offset = records.last().unwrap().offset + 1;
        out.extend(records);
        if offset >= high {
            return out;
        }
    }
    panic!("fetch did not finish, held {}", out.len());
}

fn values_of(records: &[Record]) -> BTreeSet<Vec<u8>> {
    records
        .iter()
        .map(|record| record.value.clone().unwrap_or_default().to_vec())
        .collect()
}

#[test]
fn concurrent_produces_keep_every_record_once() {
    let root = tempfile::tempdir().unwrap();
    let mut node = spawn(root.path());
    let mut setup = connect(&node.kafka);
    create_topic(&mut setup, "events");

    const THREADS: usize = 8;
    const EACH: usize = 80;
    let kafka = node.kafka.clone();
    let offsets = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for thread in 0..THREADS {
            let kafka = kafka.clone();
            let offsets = &offsets;
            scope.spawn(move || {
                let mut stream = connect(&kafka);
                for index in 0..EACH {
                    let value = format!("{thread}:{index}");
                    let (code, offset) = produce(
                        &mut stream,
                        "events",
                        index as i32,
                        batch(value.as_bytes(), false, NO_PRODUCER_ID, 0, 0),
                        None,
                    );
                    assert_eq!(code, 0, "produce {value}");
                    offsets.lock().unwrap().push(offset);
                }
            });
        }
    });
    let offsets = offsets.into_inner().unwrap();
    let unique: BTreeSet<i64> = offsets.iter().copied().collect();
    assert_eq!(
        unique.len(),
        offsets.len(),
        "duplicate base offsets in {offsets:?}"
    );
    let expected: BTreeSet<Vec<u8>> = (0..THREADS)
        .flat_map(|thread| (0..EACH).map(move |index| format!("{thread}:{index}").into_bytes()))
        .collect();
    let fetched = fetch_all(&mut setup, "events");
    let got = values_of(&fetched);
    let missing: Vec<_> = expected.difference(&got).take(5).cloned().collect();
    let extra: Vec<_> = got.difference(&expected).take(5).cloned().collect();
    assert!(
        missing.is_empty() && extra.is_empty() && fetched.len() == expected.len(),
        "fetched {} of {}, missing {missing:?} extra {extra:?}",
        fetched.len(),
        expected.len()
    );
    let mut seen = BTreeSet::new();
    for record in &fetched {
        assert!(seen.insert(record.offset), "duplicate fetched offset {}", record.offset);
    }
    assert_eq!(*seen.iter().next().unwrap(), 0);
    assert_eq!(*seen.iter().next_back().unwrap(), expected.len() as i64 - 1);
    assert!(node.child.try_wait().unwrap().is_none(), "broker exited");
}

#[test]
fn a_committed_produce_stays_visible_when_end_txn_races_it() {
    let root = tempfile::tempdir().unwrap();
    let mut node = spawn(root.path());
    let mut setup = connect(&node.kafka);
    create_topic(&mut setup, "orders");

    let mut lost = Vec::new();
    let mut leaked = Vec::new();
    let rounds = 40;
    for round in 0..rounds {
        let id = format!("race-{round}");
        send(
            &mut setup,
            ApiKey::InitProducerId,
            4,
            10,
            &InitProducerIdRequest::default()
                .with_transactional_id(Some(TransactionalId(StrBytes::from_string(id.clone())))),
        );
        let started = recv::<InitProducerIdResponse>(&mut setup, 4);
        assert_eq!(started.error_code, 0, "{started:?}");
        let producer = started.producer_id.0;
        let epoch = started.producer_epoch;
        send(
            &mut setup,
            ApiKey::AddPartitionsToTxn,
            3,
            11,
            &AddPartitionsToTxnRequest::default()
                .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_string(id.clone())))
                .with_v3_and_below_producer_id(producer.into())
                .with_v3_and_below_producer_epoch(epoch)
                .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(vec![0])]),
        );
        let added = recv::<AddPartitionsToTxnResponse>(&mut setup, 3);
        assert_eq!(
            added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code,
            0,
            "{added:?}"
        );

        let value = format!("v{round}");
        let kafka = node.kafka.clone();
        let txn_id = id.clone();
        let produced = std::thread::scope(|scope| {
            let produce_thread = scope.spawn(|| {
                let mut stream = connect(&kafka);
                produce(
                    &mut stream,
                    "orders",
                    12,
                    batch(value.as_bytes(), true, producer, epoch, 0),
                    Some(&txn_id),
                )
            });
            let mut end = connect(&kafka);
            send(
                &mut end,
                ApiKey::EndTxn,
                3,
                13,
                &EndTxnRequest::default()
                    .with_transactional_id(TransactionalId(StrBytes::from_string(id.clone())))
                    .with_producer_id(producer.into())
                    .with_producer_epoch(epoch)
                    .with_committed(true),
            );
            let ended = recv::<EndTxnResponse>(&mut end, 3);
            (produce_thread.join().unwrap(), ended.error_code)
        });
        let ((produce_code, _), end_code) = produced;
        let row = fetch_once(&mut setup, "orders", 0, 1);
        assert_eq!(row.error_code, 0, "{row:?}");
        let visible = values_of(&records_of(row));
        let present = visible.iter().any(|item| item == value.as_bytes());
        if produce_code == 0 && end_code == 0 && !present {
            lost.push(round);
        }
        if produce_code != 0 && present {
            leaked.push((round, produce_code));
        }
    }
    assert!(
        node.child.try_wait().unwrap().is_none(),
        "broker exited\n{}",
        std::fs::read_to_string(node.dir.join("broker.log")).unwrap_or_default()
    );
    assert!(
        lost.is_empty() && leaked.is_empty(),
        "acked-then-hidden rounds {lost:?}; failed-produce-but-visible {leaked:?}"
    );
}
