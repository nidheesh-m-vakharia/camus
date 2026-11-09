//! A connection with several produces in flight answers each in order, and
//! a later one is queued while an earlier one commits (#99).

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;

use bytes::Bytes;
use camus_protocol::frame;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{ApiKey, ProduceRequest, RequestHeader, ResponseHeader, TopicName};
use camus_protocol::protocol::{Decodable, HeaderVersion, StrBytes};
use support::*;

const IN_FLIGHT: i32 = 5;

fn counter(http: &str, name: &str) -> u64 {
    admin(http).unwrap()[name].as_u64().unwrap()
}

fn send_produce(stream: &mut TcpStream, correlation: i32) {
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(batch(&format!("v{correlation}"))))];
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(9)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("pipelining")));
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(10_000)
        .with_topic_data(vec![topic]);
    stream
        .write_all(&frame::encode_request(&header, &request, 9).unwrap())
        .unwrap();
}

fn recv_produce(stream: &mut TcpStream) -> (i32, ProduceResponse) {
    let mut size = [0u8; 4];
    stream.read_exact(&mut size).unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let header = ResponseHeader::decode(&mut buf, ProduceResponse::header_version(9)).unwrap();
    (
        header.correlation_id,
        ProduceResponse::decode(&mut buf, 9).unwrap(),
    )
}

#[test]
fn produces_in_flight_are_answered_in_order_and_share_commits() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default().env("CAMUS_FLUSH_MS", "600000"));
    let addr = kafka_addr(&node);
    assert_eq!(produce(&addr, "warm").0, 0);
    let before = counter(&node.http, "wal_log_entries");

    let mut stream = connect(&addr);
    stream.set_nodelay(true).unwrap();
    for correlation in 1..=IN_FLIGHT {
        send_produce(&mut stream, correlation);
    }
    let mut offsets = Vec::new();
    for expected in 1..=IN_FLIGHT {
        let (correlation, response) = recv_produce(&mut stream);
        assert_eq!(correlation, expected, "replies come back in request order");
        let part = &response.responses[0].partition_responses[0];
        assert_eq!(part.error_code, 0, "{response:?}");
        offsets.push(part.base_offset);
    }
    assert_eq!(offsets, (1..=i64::from(IN_FLIGHT)).collect::<Vec<_>>());
    let entries = counter(&node.http, "wal_log_entries") - before;
    assert!(
        entries < IN_FLIGHT as u64,
        "{IN_FLIGHT} produces in flight took {entries} WAL entries; one at a time would take {IN_FLIGHT}"
    );
}
