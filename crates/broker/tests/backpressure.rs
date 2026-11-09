//! A produce that does not wait for acks is held at the disk limit like any
//! other (#96), and a fetch waits for `min_bytes` but returns as soon as
//! enough has arrived (#113).

mod support;

use std::net::TcpStream;
use std::time::{Duration, Instant};

use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::{ApiKey, FetchRequest, ProduceRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

fn counter(http: &str, name: &str) -> u64 {
    admin(http).unwrap()[name].as_u64().unwrap()
}

/// Send one batch with `acks`, and read no response.
fn send_produce(stream: &mut TcpStream, value: &str, acks: i16, timeout_ms: i32) {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch(value)))];
    send(
        stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(acks)
            .with_timeout_ms(timeout_ms)
            .with_topic_data(vec![topic]),
    );
}

/// Fetch partition 0 from `offset`, returning the record bytes it got.
fn fetch(stream: &mut TcpStream, offset: i64, min_bytes: i32, max_wait_ms: i32) -> usize {
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = offset;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partitions = vec![part];
    send(
        stream,
        ApiKey::Fetch,
        11,
        &FetchRequest::default()
            .with_min_bytes(min_bytes)
            .with_max_wait_ms(max_wait_ms)
            .with_topics(vec![topic]),
    );
    let fetched = recv::<FetchResponse>(stream, 11);
    let row = &fetched.responses[0].partitions[0];
    assert_eq!(row.error_code, 0, "{row:?}");
    row.records.as_ref().map_or(0, |bytes| bytes.len())
}

#[test]
fn a_produce_without_acks_is_held_at_the_disk_limit() {
    let dir = tempfile::tempdir().unwrap();
    // No uploads, so nothing is sealed and the limit stays reached.
    let node = single(
        dir.path(),
        Opts::default()
            .env("CAMUS_FLUSH_MS", "600000")
            .env("CAMUS_WAL_DISK_LIMIT", "1"),
    );
    let addr = kafka_addr(&node);
    // The first batch is accepted: an empty WAL is never over the limit.
    assert_eq!(produce(&addr, "first").0, 0);
    let unsealed = counter(&node.http, "wal_unsealed_bytes");
    assert!(unsealed > 0);
    let entries = counter(&node.http, "wal_log_entries");

    let mut stream = connect(&addr);
    for index in 0..5 {
        send_produce(&mut stream, &format!("dropped-{index}"), 0, 300);
    }
    std::thread::sleep(Duration::from_millis(1_000));
    assert_eq!(
        counter(&node.http, "wal_log_entries"),
        entries,
        "acks=0 batches past the limit are not appended"
    );
    assert_eq!(counter(&node.http, "wal_unsealed_bytes"), unsealed);
}

#[test]
fn a_fetch_waits_for_min_bytes_and_returns_once_they_arrive() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default().env("CAMUS_FLUSH_MS", "600000"));
    let addr = kafka_addr(&node);
    assert_eq!(produce(&addr, "small").0, 0);
    let mut stream = connect(&addr);
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();

    // One small batch is there, but less than `min_bytes`: wait it out.
    let started = Instant::now();
    let got = fetch(&mut stream, 0, 100_000, 600);
    assert!(got > 0);
    assert!(
        started.elapsed() >= Duration::from_millis(550),
        "returned after {:?} with {got} of 100000 bytes",
        started.elapsed()
    );

    // Nothing past offset 1 yet; a produce arrives during the wait.
    let producer = {
        let addr = addr.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            assert_eq!(produce(&addr, "later").0, 0);
        })
    };
    let started = Instant::now();
    let got = fetch(&mut stream, 1, 1, 10_000);
    producer.join().unwrap();
    assert!(got > 0);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a 10 s long-poll should return when the batch commits, took {:?}",
        started.elapsed()
    );
}
