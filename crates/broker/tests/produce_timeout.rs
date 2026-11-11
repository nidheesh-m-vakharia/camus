//! A produce that times out may already be committed. Retrying it without
//! idempotence appends a second copy. The same retry with a producer id and
//! sequence returns the original offset.

mod support;

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use camus_protocol::messages::{ApiKey, InitProducerIdRequest};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH,
};
use support::*;

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        support::skipped("produce timeout test; set CAMUS_MINIO=1");
    }
    on
}

fn idempotent_batch(producer: i64, epoch: i16, value: &str) -> std::io::Result<Bytes> {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: producer,
        producer_epoch: epoch,
        sequence: 0,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        timestamp: 9,
        key: None,
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
    )
    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
    Ok(buf.freeze())
}

fn init_producer(addr: &str) -> (i64, i16) {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        &InitProducerIdRequest::default().with_transactional_id(None),
    );
    let started = recv::<InitProducerIdResponse>(&mut stream, 4);
    assert_eq!(started.error_code, 0);
    (started.producer_id.0, started.producer_epoch)
}

/// The timed-out batch is in the log before its ack is released.
fn wait_landed(addr: &str, want: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (code, got) = fetch_at(addr, 0);
        if code == 0 && got.iter().map(String::as_str).eq(want.iter().copied()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed-out produce never landed: code {code}, got {got:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_timed_out_produce_is_a_second_copy_without_idempotence() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let node = single(
        root.path(),
        Opts::default().env("CAMUS_ACK_DELAY_MS", "800"),
    );
    let kafka = kafka_addr(&node);
    let (code, _, _) = produce_full(&kafka, "v0", 200);
    assert_eq!(code, 7, "the ack misses timeout_ms");
    wait_landed(&kafka, &["v0"]);

    let (code, offset, _) = produce_full(&kafka, "v0", 5_000);
    assert_eq!((code, offset), (0, 1));
    assert_eq!(
        read_all(&kafka, 2),
        vec!["v0".to_string(), "v0".to_string()]
    );
    Ok(())
}

#[test]
fn a_timed_out_idempotent_produce_retries_at_the_same_offset() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let node = single(
        root.path(),
        Opts::default().env("CAMUS_ACK_DELAY_MS", "800"),
    );
    let kafka = kafka_addr(&node);
    let (producer, epoch) = init_producer(&kafka);
    let batch = idempotent_batch(producer, epoch, "once")?;
    let (code, _) = produce_batch_timeout(&kafka, batch.clone(), 200);
    assert_eq!(code, 7, "the ack misses timeout_ms");
    wait_landed(&kafka, &["once"]);

    let (code, offset) = produce_batch_timeout(&kafka, batch, 5_000);
    assert_eq!((code, offset), (0, 0));
    assert_eq!(read_all(&kafka, 1), vec!["once".to_string()]);
    Ok(())
}
