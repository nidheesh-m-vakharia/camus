//! `DeleteRecords` moves the log start to the exact offset asked for, up to
//! the high watermark, as Kafka does, including records still only in the WAL.

mod support;

use camus_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use camus_protocol::messages::delete_records_response::DeleteRecordsResponse;
use camus_protocol::messages::{ApiKey, DeleteRecordsRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::{connect, fetch_code, kafka_addr, produce, recv, send, single, Opts, TOPIC};

fn delete_to(addr: &str, offset: i64) -> (i16, i64) {
    let mut stream = connect(addr);
    let topic = DeleteRecordsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
        .with_partitions(vec![DeleteRecordsPartition::default()
            .with_partition_index(0)
            .with_offset(offset)]);
    send(
        &mut stream,
        ApiKey::DeleteRecords,
        2,
        &DeleteRecordsRequest::default()
            .with_topics(vec![topic])
            .with_timeout_ms(5_000),
    );
    let response = recv::<DeleteRecordsResponse>(&mut stream, 2);
    let row = &response.topics[0].partitions[0];
    (row.error_code, row.low_watermark)
}

#[test]
fn a_delete_reaches_the_exact_offset_and_the_high_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    for value in ["a", "b", "c"] {
        assert_eq!(produce(&addr, value).0, 0);
    }
    assert_eq!(delete_to(&addr, 4), (1, -1), "past the high watermark");
    assert_eq!(delete_to(&addr, 2), (0, 2));
    assert_eq!(fetch_code(&addr, 1), 1, "below the log start");
    assert_eq!(fetch_code(&addr, 2), 0);
    assert_eq!(
        delete_to(&addr, 1),
        (0, 2),
        "the log start never moves back"
    );
    assert_eq!(delete_to(&addr, -1), (0, 3), "-1 means the high watermark");
}
