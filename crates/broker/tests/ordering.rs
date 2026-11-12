//! Ordering on one partition.
//!
//! Several producers may interleave, and each producer's own records stay in
//! the order that producer sent them. An idempotent producer may have five
//! batches in flight and lose the acks; retries come back at the original
//! offsets, with nothing duplicated or reordered.

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
        support::skipped("ordering tests; set CAMUS_MINIO=1");
    }
    on
}

fn batch(producer: i64, epoch: i16, sequence: i32, value: &str) -> std::io::Result<Bytes> {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: producer,
        producer_epoch: epoch,
        sequence,
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
    .map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok(buf.freeze())
}

fn init_producer(addr: &str) -> std::io::Result<(i64, i16)> {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        &InitProducerIdRequest::default().with_transactional_id(None),
    );
    let started = recv::<InitProducerIdResponse>(&mut stream, 4);
    if started.error_code != 0 {
        return Err(std::io::Error::other(format!(
            "InitProducerId returned {}",
            started.error_code
        )));
    }
    Ok((started.producer_id.0, started.producer_epoch))
}

/// Four producers write one partition at once. Offsets cover `0..n` once,
/// and each producer's values stay in the order it sent them.
#[test]
fn concurrent_producers_keep_each_send_order() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir().map_err(|err| std::io::Error::other(err))?;
    let node = single(root.path(), Opts::default());
    let kafka = kafka_addr(&node);
    const PRODUCERS: i32 = 4;
    const EACH: i32 = 20;
    let mut handles = Vec::new();
    for producer in 0..PRODUCERS {
        let addr = kafka.clone();
        handles.push(std::thread::spawn(move || {
            let mut landed = Vec::new();
            for seq in 0..EACH {
                let value = format!("p{producer}-{seq}");
                let deadline = Instant::now() + Duration::from_secs(15);
                loop {
                    let (code, offset) = produce(&addr, &value);
                    if code == 0 {
                        landed.push(offset);
                        break;
                    }
                    if Instant::now() > deadline {
                        return Err(std::io::Error::other(format!(
                            "producer {producer} sequence {seq} ended with {code}"
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            Ok(landed)
        }));
    }
    let mut all = Vec::new();
    for handle in handles {
        let landed = handle
            .join()
            .map_err(|_| std::io::Error::other("a producer thread panicked"))??;
        all.extend(landed);
    }
    let total = (PRODUCERS * EACH) as i64;
    let mut offsets = all;
    offsets.sort_unstable();
    let expected: Vec<i64> = (0..total).collect();
    if offsets != expected {
        return Err(std::io::Error::other(format!(
            "offsets are not 0..{total}: {offsets:?}"
        )));
    }
    // The value encodes the producer and its send sequence. Reading the
    // partition in offset order is the order the shard committed.
    let got = read_all(&kafka, total);
    if got.len() != total as usize {
        return Err(std::io::Error::other(format!(
            "fetched {} records, produced {total}",
            got.len()
        )));
    }
    let mut seen = vec![Vec::new(); PRODUCERS as usize];
    for value in &got {
        let (producer, seq) = split_value(value)?;
        let slot = seen.get_mut(producer).ok_or_else(|| {
            std::io::Error::other(format!("record {value} names producer {producer}"))
        })?;
        slot.push(seq);
    }
    for (producer, seqs) in seen.iter().enumerate() {
        let want: Vec<i32> = (0..EACH).collect();
        if seqs != &want {
            return Err(std::io::Error::other(format!(
                "producer {producer} committed {seqs:?}"
            )));
        }
    }
    Ok(())
}

/// Five idempotent batches are in flight, their acks are dropped, and every
/// retry returns the offset from the first successful append.
#[test]
fn idempotent_inflight_retries_keep_order() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let root = tempfile::tempdir().map_err(|err| std::io::Error::other(err))?;
    let node = single(
        root.path(),
        Opts::default().env("CAMUS_ACK_DELAY_MS", "500"),
    );
    let kafka = kafka_addr(&node);
    let (producer, epoch) = init_producer(&kafka)?;
    let mut batches = Vec::new();
    for seq in 0..5 {
        batches.push(batch(producer, epoch, seq, &format!("s{seq}"))?);
    }
    let mut handles = Vec::new();
    for raw in batches.clone() {
        let addr = kafka.clone();
        handles.push(std::thread::spawn(move || {
            produce_batch_timeout(&addr, raw, 120)
        }));
    }
    for handle in handles {
        let (code, _) = handle
            .join()
            .map_err(|_| std::io::Error::other("an in-flight produce panicked"))?;
        // 7 is the dropped ack. 45 is a batch that arrived before its
        // sequence, which the retry below appends.
        if code != 7 && code != 45 && code != 0 {
            return Err(std::io::Error::other(format!(
                "in-flight produce returned {code}"
            )));
        }
    }
    let mut offsets = Vec::new();
    for raw in &batches {
        let offset = produce_until(&kafka, raw.clone())?;
        offsets.push(offset);
    }
    for (raw, offset) in batches.iter().zip(offsets.iter()) {
        let (code, again) = produce_batch_timeout(&kafka, raw.clone(), 5_000);
        if code != 0 || again != *offset {
            return Err(std::io::Error::other(format!(
                "retry returned {code} at {again}, first append was at {offset}"
            )));
        }
    }
    let want: Vec<i64> = (0..5).collect();
    if offsets != want {
        return Err(std::io::Error::other(format!(
            "sequences landed at {offsets:?}"
        )));
    }
    let got = read_all(&kafka, 5);
    let want_values: Vec<String> = (0..5).map(|seq| format!("s{seq}")).collect();
    if got != want_values {
        return Err(std::io::Error::other(format!("log is {got:?}")));
    }
    Ok(())
}

fn produce_until(addr: &str, raw: Bytes) -> std::io::Result<i64> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (code, offset) = produce_batch_timeout(addr, raw.clone(), 5_000);
        if code == 0 {
            return Ok(offset);
        }
        // Out of order, not the leader yet, or the ack delay ate the timeout.
        if code != 45 && !(5..=7).contains(&code) {
            return Err(std::io::Error::other(format!("produce returned {code}")));
        }
        if Instant::now() > deadline {
            return Err(std::io::Error::other(format!(
                "produce still returning {code}"
            )));
        }
        std::thread::sleep(Duration::from_millis(30));
    }
}

fn split_value(value: &str) -> std::io::Result<(usize, i32)> {
    let rest = value
        .strip_prefix('p')
        .ok_or_else(|| std::io::Error::other(format!("record {value}")))?;
    let (producer, seq) = rest
        .split_once('-')
        .ok_or_else(|| std::io::Error::other(format!("record {value}")))?;
    let producer = producer
        .parse::<usize>()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    let seq = seq
        .parse::<i32>()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok((producer, seq))
}
