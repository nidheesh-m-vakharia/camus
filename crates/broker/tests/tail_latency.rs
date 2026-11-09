//! Benchmark: how soon a consumer waiting at the end of the log sees a new
//! batch (#113). A consumer long-polls at the high watermark while a producer
//! sends one batch every 50 ms; each sample is the time from the produce's
//! ack to the fetch returning that batch.
//!
//! ```text
//! cargo test --release -p camus-broker --test tail_latency -- --ignored --nocapture
//! ```

mod support;

use std::sync::mpsc;
use std::time::{Duration, Instant};

use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::{ApiKey, FetchRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

fn pct(sorted: &[Duration], p: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index].as_secs_f64() * 1000.0
}

#[test]
#[ignore = "benchmark; run with --ignored --release"]
fn a_waiting_consumer_sees_a_new_batch() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default().env("CAMUS_FLUSH_MS", "600000"));
    let addr = kafka_addr(&node);
    let samples = 200;
    let (acked, acks) = mpsc::channel::<Instant>();
    let producer = {
        let addr = addr.clone();
        std::thread::spawn(move || {
            for index in 0..samples {
                std::thread::sleep(Duration::from_millis(50));
                assert_eq!(produce(&addr, &format!("v{index}")).0, 0);
                acked.send(Instant::now()).unwrap();
            }
        })
    };
    let mut stream = connect(&addr);
    stream.set_nodelay(true).unwrap();
    let mut offset = 0i64;
    let mut out = Vec::with_capacity(samples);
    while out.len() < samples {
        let mut part = FetchPartition::default();
        part.partition = 0;
        part.fetch_offset = offset;
        part.partition_max_bytes = 1024 * 1024;
        let mut topic = FetchTopic::default();
        topic.topic = TopicName(StrBytes::from_static_str(TOPIC));
        topic.partitions = vec![part];
        send(
            &mut stream,
            ApiKey::Fetch,
            11,
            &FetchRequest::default()
                .with_max_wait_ms(500)
                .with_topics(vec![topic]),
        );
        let fetched = recv::<FetchResponse>(&mut stream, 11);
        let returned = Instant::now();
        let row = &fetched.responses[0].partitions[0];
        let Some(bytes) = row.records.as_ref().filter(|bytes| !bytes.is_empty()) else {
            continue;
        };
        let batches = camus_protocol::batch::inspect(bytes, 1 << 20).unwrap();
        for batch in &batches {
            offset = offset.max(batch.last_offset + 1);
            let acked_at = acks.recv().unwrap();
            out.push(returned.saturating_duration_since(acked_at));
        }
    }
    producer.join().unwrap();
    out.sort();
    println!("| Samples | p50 ms | p90 ms | p99 ms | max ms |");
    println!("| ---: | ---: | ---: | ---: | ---: |");
    println!(
        "| {} | {:.2} | {:.2} | {:.2} | {:.2} |",
        out.len(),
        pct(&out, 0.50),
        pct(&out, 0.90),
        pct(&out, 0.99),
        out.last().unwrap().as_secs_f64() * 1000.0
    );
}
