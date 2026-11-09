//! Benchmark: one connection keeping five `acks=all` produces in flight, as a
//! Kafka client does by default (`max.in.flight.requests.per.connection`).
//! Reports acks per second over a fixed time (#99).
//!
//! ```text
//! cargo test --release -p camus-broker --test pipelined_throughput -- --ignored --nocapture
//! ```

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use camus_protocol::frame;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::{ApiKey, ProduceRequest, RequestHeader, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

const IN_FLIGHT: usize = 5;

fn send_produce(stream: &mut TcpStream, correlation: i32, value: &str) {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch(value)))];
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(9)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("pipelined")));
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![topic]);
    stream
        .write_all(&frame::encode_request(&header, &request, 9).unwrap())
        .unwrap();
}

fn skip_response(stream: &mut TcpStream) {
    let mut size = [0u8; 4];
    stream.read_exact(&mut size).unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).unwrap();
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn five_produces_in_flight_on_one_connection() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default().env("CAMUS_FLUSH_MS", "250"));
    let addr = kafka_addr(&node);
    assert_eq!(produce(&addr, "warm").0, 0);
    let mut stream = connect(&addr);
    stream.set_nodelay(true).unwrap();
    let value = "x".repeat(1024);
    let mut sent = 0i32;
    for _ in 0..IN_FLIGHT {
        sent += 1;
        send_produce(&mut stream, sent, &value);
    }
    let started = Instant::now();
    let mut acked = 0u64;
    while started.elapsed() < Duration::from_secs(5) {
        skip_response(&mut stream);
        acked += 1;
        sent += 1;
        send_produce(&mut stream, sent, &value);
    }
    for _ in 0..IN_FLIGHT {
        skip_response(&mut stream);
    }
    let seconds = started.elapsed().as_secs_f64();
    println!(
        "| {IN_FLIGHT} in flight | {acked} acks | {:.0} acks/s |",
        acked as f64 / seconds
    );
}
