//! Helpers shared by the multi-broker `wal` tests.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::frame;
use camus_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::{
    ApiKey, CreateTopicsRequest, FetchRequest, MetadataRequest, ProduceRequest, RequestHeader,
    TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

pub const TOPIC: &str = "orders";

/// A test is leaving early. CI sets `CAMUS_REQUIRE_CLIENTS=1` so a skip
/// fails the job instead of counting as a pass.
pub fn skipped(reason: &str) {
    if std::env::var("CAMUS_REQUIRE_CLIENTS").ok().as_deref() == Some("1") {
        panic!("required test skipped: {reason}");
    }
    eprintln!("skipping {reason}");
}

/// Resident set size of a process in KiB, from `ps`. `None` once it exits.
pub fn rss_kib(pid: u32) -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// Polls broker RSS on a thread and keeps each process's highest reading.
/// Sampling every 20ms can miss a spike shorter than that.
pub struct MemorySampler {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<Vec<u64>>>,
}

impl MemorySampler {
    pub fn start(pids: Vec<u32>) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut peak = vec![0u64; pids.len()];
            while !flag.load(std::sync::atomic::Ordering::Relaxed) {
                for (slot, pid) in peak.iter_mut().zip(&pids) {
                    if let Some(kib) = rss_kib(*pid) {
                        *slot = (*slot).max(kib);
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            peak
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }

    /// Stop and return each process's peak RSS in KiB, in `start` order.
    pub fn finish(mut self) -> Vec<u64> {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap()
    }
}

/// Mean RSS in KiB of each process over `window`, sampled every 100ms.
pub fn steady_rss(pids: &[u32], window: Duration) -> Vec<u64> {
    let mut sums = vec![0u64; pids.len()];
    let mut samples = 0u64;
    let until = Instant::now() + window;
    while Instant::now() < until {
        for (sum, pid) in sums.iter_mut().zip(pids) {
            *sum += rss_kib(*pid).unwrap_or(0);
        }
        samples += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
    sums.into_iter().map(|sum| sum / samples.max(1)).collect()
}

/// Idle, peak, and after-load RSS per broker, in KiB.
#[derive(Debug, Clone)]
pub struct Memory {
    pub ids: Vec<i32>,
    pub idle: Vec<u64>,
    pub peak: Vec<u64>,
    pub after: Vec<u64>,
}

impl Memory {
    /// One markdown row per broker: `| label | id | idle | peak | after |` in MiB.
    pub fn rows(&self, label: &str, leader: Option<i32>) -> Vec<String> {
        let mib = |kib: u64| kib as f64 / 1024.0;
        self.ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                let role = if Some(*id) == leader { " (leader)" } else { "" };
                format!(
                    "| {label} | {id}{role} | {:.1} | {:.1} | {:.1} |",
                    mib(self.idle[index]),
                    mib(self.peak[index]),
                    mib(self.after[index]),
                )
            })
            .collect()
    }
}

/// What the [`Proxy`] does to object-storage traffic.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    /// Pass bytes through.
    None,
    /// Drop open connections and refuse new ones.
    Cut,
    /// Hold every chunk this long before forwarding it.
    Delay(Duration),
    /// Answer every request with `503 Service Unavailable`.
    Unavailable,
}

/// TCP proxy in front of MinIO, so a test can make R2 slow, failing, or gone
/// for the brokers without touching the shared MinIO.
pub struct Proxy {
    pub addr: String,
    fault: Arc<Mutex<Fault>>,
    open: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    pub fn start(upstream: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let fault = Arc::new(Mutex::new(Fault::None));
        let open = Arc::new(Mutex::new(Vec::new()));
        let (mode, streams) = (fault.clone(), open.clone());
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(mut client) = client else { continue };
                let now = *mode.lock().unwrap();
                match now {
                    Fault::Cut => {
                        let _ = client.shutdown(Shutdown::Both);
                        continue;
                    }
                    Fault::Unavailable => {
                        std::thread::spawn(move || answer_503(&mut client));
                        continue;
                    }
                    _ => {}
                }
                let Ok(server) = TcpStream::connect(upstream) else {
                    continue;
                };
                {
                    let mut open = streams.lock().unwrap();
                    open.push(client.try_clone().unwrap());
                    open.push(server.try_clone().unwrap());
                }
                pipe(
                    client.try_clone().unwrap(),
                    server.try_clone().unwrap(),
                    mode.clone(),
                );
                pipe(server, client, mode.clone());
            }
        });
        Self { addr, fault, open }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Switch fault mode. Any mode other than `None` and `Delay` also drops
    /// pooled connections so clients see it at once.
    pub fn set(&self, fault: Fault) {
        *self.fault.lock().unwrap() = fault;
        if matches!(fault, Fault::Cut | Fault::Unavailable) {
            for stream in self.open.lock().unwrap().drain(..) {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
    }

    pub fn cut(&self) {
        self.set(Fault::Cut);
    }

    pub fn restore(&self) {
        self.set(Fault::None);
    }
}

fn answer_503(client: &mut TcpStream) {
    let _ = client.set_read_timeout(Some(Duration::from_millis(500)));
    let mut buf = [0u8; 64 * 1024];
    loop {
        // Read the request head, then answer; any body is discarded.
        match client.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if buf[..n].windows(4).any(|window| window == b"\r\n\r\n") {
                    let _ = client.write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    break;
                }
            }
        }
    }
    let _ = client.shutdown(Shutdown::Both);
}

fn pipe(mut from: TcpStream, mut to: TcpStream, mode: Arc<Mutex<Fault>>) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Fault::Delay(by) = *mode.lock().unwrap() {
                        std::thread::sleep(by);
                    }
                    if to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = to.shutdown(Shutdown::Both);
    });
}

pub fn read_key(http: &str, key: &str) -> Option<String> {
    let body: serde_json::Value = reqwest::blocking::Client::new()
        .get(format!("http://{http}/admin/read"))
        .query(&[("key", key)])
        .send()
        .ok()?
        .json()
        .ok()?;
    body["value"].as_str().map(str::to_string)
}

/// Partition 0's sealed end as `http`'s metadata replica has it.
pub fn sealed_end(http: &str) -> i64 {
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let id = topic["id"].as_str().unwrap();
    let part: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("part/{id}/0")).unwrap()).unwrap();
    part["next_offset"].as_i64().unwrap()
}

/// Every key under `prefix` and its value, from `http`'s metadata replica.
pub fn scan_keys(http: &str, prefix: &str) -> Vec<(String, String)> {
    reqwest::blocking::Client::new()
        .get(format!("http://{http}/admin/scan"))
        .query(&[("prefix", prefix)])
        .send()
        .unwrap()
        .json()
        .unwrap()
}

/// Partition 0's extents as `(start, end)` from `http`'s metadata replica:
/// from the partition row if it predates extent keys, else from the keys.
pub fn extents(http: &str) -> Vec<(i64, i64)> {
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let id = topic["id"].as_str().unwrap();
    let part: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("part/{id}/0")).unwrap()).unwrap();
    let legacy = part["extents"].as_array().cloned().unwrap_or_default();
    let stored: Vec<serde_json::Value> = if legacy.is_empty() {
        scan_keys(http, &format!("ext/{id}/{:010}/", 0))
            .into_iter()
            .map(|(_, raw)| serde_json::from_str(&raw).unwrap())
            .collect()
    } else {
        legacy
    };
    stored
        .iter()
        .map(|extent| {
            (
                extent["start"].as_i64().unwrap(),
                extent["end"].as_i64().unwrap(),
            )
        })
        .collect()
}

pub fn wait_sealed(http: &str, end: i64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if sealed_end(http) == end {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("sealed end never reached {end}");
}

pub fn fetch_code(addr: &str, offset: i64) -> i16 {
    let mut stream = connect(addr);
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
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    recv::<FetchResponse>(&mut stream, 11).responses[0].partitions[0].error_code
}

/// One fetch at `offset`. The error code and the record values in the
/// response. An error leaves the values empty when the broker attached none,
/// so a consumer that stops on the code does not advance.
pub fn fetch_at(addr: &str, offset: i64) -> (i16, Vec<String>) {
    let mut stream = connect(addr);
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
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    let fetched = recv::<FetchResponse>(&mut stream, 11);
    let row = &fetched.responses[0].partitions[0];
    let code = row.error_code;
    let Some(bytes) = row.records.clone() else {
        return (code, Vec::new());
    };
    let Ok(info) = camus_protocol::batch::inspect(&bytes, 1024 * 1024) else {
        return (code, Vec::new());
    };
    let mut values = Vec::new();
    let mut cursor = 0;
    for batch in info {
        let raw = &bytes[cursor..cursor + batch.len];
        cursor += batch.len;
        let mut buf = Bytes::copy_from_slice(raw);
        let Ok(set) = RecordBatchDecoder::decode(&mut buf) else {
            continue;
        };
        for record in set.records {
            let Some(value) = record.value else {
                continue;
            };
            let Ok(text) = String::from_utf8(value.to_vec()) else {
                continue;
            };
            values.push(text);
        }
    }
    (code, values)
}

/// How to start a broker. Kept on the node so a restart uses the same.
#[derive(Clone, Default)]
pub struct Opts {
    /// Object storage endpoint (`http://…`), e.g. a [`Proxy`].
    pub s3: Option<String>,
    /// Extra environment, applied after the defaults.
    pub env: Vec<(String, String)>,
    /// Extra command-line arguments.
    pub args: Vec<String>,
}

impl Opts {
    pub fn s3(mut self, url: &str) -> Self {
        self.s3 = Some(url.to_string());
        self
    }

    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.to_string(), value.to_string()));
        self
    }

    pub fn arg(mut self, value: &str) -> Self {
        self.args.push(value.to_string());
        self
    }
}

pub struct Node {
    pub id: i32,
    pub child: Child,
    pub dir: PathBuf,
    pub http: String,
    pub opts: Opts,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    pub fn spawn(id: i32, dir: &Path) -> Self {
        Self::with(id, dir, Opts::default())
    }

    /// Spawn with object storage reached through `s3` (an `http://` URL).
    pub fn spawn_via(id: i32, dir: &Path, s3: &str) -> Self {
        Self::with(id, dir, Opts::default().s3(s3))
    }

    pub fn with(id: i32, dir: &Path, opts: Opts) -> Self {
        Self::spawn_at(id, dir, "127.0.0.1:0", "127.0.0.1:0", opts)
    }

    /// Restart on the addresses the node had: its peers know it by them.
    pub fn restart(&mut self) {
        let opts = self.opts.clone();
        self.restart_with(opts);
    }

    /// Restart on the same addresses with different options.
    pub fn restart_with(&mut self, opts: Opts) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let http = self.http.clone();
        let kafka = kafka_addr(self);
        let dir = self.dir.clone();
        *self = Self::spawn_at(self.id, &dir, &http, &kafka, opts);
    }

    fn spawn_at(id: i32, dir: &Path, http: &str, kafka: &str, opts: Opts) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let _ = std::fs::remove_file(dir.join("listen-addr"));
        let _ = std::fs::remove_file(dir.join("kafka-addr"));
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("broker.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_camus-broker"));
        command.env("CAMUS_WAL_SHARDS", "voters");
        command
            .args([
                "--node-id",
                &id.to_string(),
                "--http-addr",
                http,
                "--data-dir",
                dir.to_str().unwrap(),
                "--kafka-addr",
                kafka,
                "--dev-plaintext",
            ])
            .args(&opts.args)
            .env("CAMUS_FLUSH_MS", "40")
            .env("RUST_LOG", "info");
        if let Some(s3) = &opts.s3 {
            command.env("CAMUS_S3_ENDPOINT", s3);
        }
        for (key, value) in &opts.env {
            command.env(key, value);
        }
        let child = command
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let http = wait_file(&dir.join("listen-addr"));
        wait_file(&dir.join("kafka-addr"));
        Self {
            id,
            child,
            dir: dir.to_path_buf(),
            http,
            opts,
        }
    }

    pub fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

pub fn wait_file(path: &Path) -> String {
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

pub fn admin(http: &str) -> Option<serde_json::Value> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
        .get(format!("http://{http}/admin/metrics"))
        .send()
        .ok()?
        .json()
        .ok()
}

/// Wait until every node agrees on the metadata leader and every shard voter
/// agrees on the shard leader. Returns the metadata leader's id.
pub fn wait_settled(nodes: &[&Node]) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let views: Vec<_> = nodes.iter().filter_map(|node| admin(&node.http)).collect();
        let meta: BTreeSet<_> = views
            .iter()
            .map(|view| view["current_leader"].as_i64())
            .collect();
        let wal: BTreeSet<_> = views
            .iter()
            .filter(|view| {
                view["wal_voters"]
                    .as_array()
                    .is_some_and(|voters| !voters.is_empty())
            })
            .map(|view| view["wal_leader"].as_i64())
            .collect();
        if views.len() == nodes.len()
            && meta.len() == 1
            && wal.len() == 1
            && !meta.contains(&None)
            && !wal.contains(&None)
        {
            return meta.into_iter().next().unwrap().unwrap() as i32;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("nodes did not agree on leaders");
}

pub fn connect(addr: &str) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
}

pub fn send<M: Encodable>(stream: &mut TcpStream, key: ApiKey, version: i16, message: &M) {
    let header = RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("camus-wal-cluster")));
    let frame = frame::encode_request(&header, message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

pub fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> M {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(size_buf) as usize];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

pub fn kafka_addr(node: &Node) -> String {
    wait_file(&node.dir.join("kafka-addr"))
}

/// Ask `via` who leads the partition, as a client would after an error.
/// Returns `host:port` of that broker once it is listed and not `exclude`.
pub fn partition_leader(via: &str, exclude: Option<i32>) -> (i32, String) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let mut stream = connect(via);
        let mut topic = MetadataRequestTopic::default();
        topic.name = Some(TopicName(StrBytes::from_static_str(TOPIC)));
        send(
            &mut stream,
            ApiKey::Metadata,
            12,
            &MetadataRequest::default().with_topics(Some(vec![topic])),
        );
        let metadata = recv::<MetadataResponse>(&mut stream, 12);
        // A node that has not applied the topic yet reports it unknown.
        let Some(partition) = metadata.topics[0].partitions.first() else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let leader = partition.leader_id.0;
        if partition.error_code == 0 && leader >= 0 && Some(leader) != exclude {
            // A restarted broker's new port reaches metadata a moment after
            // it listens; a client reconnects after a refresh, so retry.
            if let Some(broker) = metadata.brokers.iter().find(|b| b.node_id.0 == leader) {
                let addr = format!("{}:{}", broker.host.as_str(), broker.port);
                if TcpStream::connect(&addr).is_ok() {
                    return (leader, addr);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("no partition leader reported by {via}");
}

pub fn batch(value: &str) -> Bytes {
    encode_batch(None, value)
}

/// One record with `key`, for compacted topics.
pub fn keyed_batch(key: &str, value: &str) -> Bytes {
    encode_batch(Some(key), value)
}

fn encode_batch(key: Option<&str>, value: &str) -> Bytes {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        sequence: 0,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        timestamp: 9,
        key: key.map(|key| Bytes::copy_from_slice(key.as_bytes())),
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
    .unwrap();
    buf.freeze()
}

/// Produce one batch with `acks=all`. Returns the partition error and offset.
pub fn produce(addr: &str, value: &str) -> (i16, i64) {
    let (code, offset, _) = produce_full(addr, value, 5_000);
    (code, offset)
}

/// Produce one batch with `acks=all` and `timeout_ms`. Returns the partition
/// error, the base offset, and the response's `throttle_time_ms`.
pub fn produce_full(addr: &str, value: &str, timeout_ms: i32) -> (i16, i64, i32) {
    produce_raw(addr, 0, batch(value), timeout_ms)
}

/// Produce one batch to `partition` with `acks=all`.
pub fn produce_partition(addr: &str, partition: i32, value: &str) -> (i16, i64) {
    let (code, offset, _) = produce_raw(addr, partition, batch(value), 5_000);
    (code, offset)
}

/// Produce `records` as one batch with `acks=all`. Returns the partition
/// error and offset.
pub fn produce_batch(addr: &str, records: Bytes) -> (i16, i64) {
    let (code, offset, _) = produce_raw(addr, 0, records, 5_000);
    (code, offset)
}

/// Produce `records` as one batch with `acks=all` and `timeout_ms`.
pub fn produce_batch_timeout(addr: &str, records: Bytes, timeout_ms: i32) -> (i16, i64) {
    let (code, offset, _) = produce_raw(addr, 0, records, timeout_ms);
    (code, offset)
}

fn produce_raw(addr: &str, partition: i32, records: Bytes, timeout_ms: i32) -> (i16, i64, i32) {
    let mut stream = connect(addr);
    stream
        .set_read_timeout(Some(Duration::from_millis(timeout_ms as u64 + 15_000)))
        .unwrap();
    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partition_data = vec![PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(records))];
    send(
        &mut stream,
        ApiKey::Produce,
        9,
        &ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(timeout_ms)
            .with_topic_data(vec![topic]),
    );
    let produced = recv::<ProduceResponse>(&mut stream, 9);
    let part = &produced.responses[0].partition_responses[0];
    (part.error_code, part.base_offset, produced.throttle_time_ms)
}

/// Produce as a client does: on a retriable error, refresh metadata through
/// `via` and send again. Returns the broker that accepted and the offset.
pub fn produce_retrying(via: &str, value: &str) -> (i32, i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (leader, addr) = partition_leader(via, None);
        let (code, offset) = produce(&addr, value);
        match code {
            0 => return (leader, offset),
            // LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, REQUEST_TIMED_OUT
            5..=7 if Instant::now() < deadline => {
                eprintln!("produce to {leader} got {code}; refreshing metadata");
                std::thread::sleep(Duration::from_millis(100));
            }
            other => panic!("produce {value} failed with {other}"),
        }
    }
}

/// Read the partition from offset 0 up to `end` and return each record value
/// in offset order. Single-record uncompressed batches only.
pub fn read_all(addr: &str, end: i64) -> Vec<String> {
    let mut stream = connect(addr);
    let mut values = Vec::new();
    let mut offset = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while offset < end {
        assert!(
            Instant::now() < deadline,
            "stuck at offset {offset} of {end}"
        );
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
                .with_max_wait_ms(200)
                .with_topics(vec![topic]),
        );
        let fetched = recv::<FetchResponse>(&mut stream, 11);
        let row = &fetched.responses[0].partitions[0];
        assert_eq!(row.error_code, 0, "{row:?}");
        let Some(bytes) = row.records.clone() else {
            continue;
        };
        let mut cursor = 0;
        for info in camus_protocol::batch::inspect(&bytes, 1024 * 1024).unwrap() {
            let raw = &bytes[cursor..cursor + info.len];
            let base = i64::from_be_bytes(raw[0..8].try_into().unwrap());
            cursor += info.len;
            if base < offset {
                continue;
            }
            assert_eq!(base, offset, "gap or overlap in fetched batches");
            let mut buf = Bytes::copy_from_slice(raw);
            let set = camus_protocol::records::RecordBatchDecoder::decode(&mut buf).unwrap();
            for record in set.records {
                values.push(String::from_utf8(record.value.unwrap().to_vec()).unwrap());
            }
            offset += i64::from(info.records);
        }
    }
    values
}

/// Create `TOPIC` with `durability.mode=wal` through the controller.
pub fn create_wal_topic(controller: &str) {
    create_topic(controller, "wal");
}

/// Create `TOPIC` with the given `durability.mode` through the controller.
pub fn create_topic(controller: &str, durability_mode: &'static str) {
    create_topic_with(controller, durability_mode, 1);
}

/// Create `TOPIC` with `partitions` partitions.
pub fn create_topic_with(controller: &str, durability_mode: &'static str, partitions: i32) {
    create_topic_named(controller, TOPIC, durability_mode, partitions);
}

/// Create the topic `name` with `partitions` partitions.
pub fn create_topic_named(
    controller: &str,
    name: &'static str,
    durability_mode: &'static str,
    partitions: i32,
) {
    let mut stream = connect(controller);
    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str(name));
    created.num_partitions = partitions;
    created.replication_factor = -1;
    let mut durability = CreatableTopicConfig::default();
    durability.name = StrBytes::from_static_str("durability.mode");
    durability.value = Some(StrBytes::from_static_str(durability_mode));
    created.configs = vec![durability];
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");
}

/// Initialize the cluster through `nodes[0]`. `wal` names the shard voters.
/// Start one broker, initialize it alone, and create `TOPIC` as `wal`.
pub fn single(dir: &Path, opts: Opts) -> Node {
    let node = Node::with(1, dir, opts);
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    create_wal_topic(&kafka_addr(&node));
    partition_leader(&kafka_addr(&node), None);
    node
}

pub fn init(nodes: &[Node], wal: &[i32]) {
    post_init(nodes, &serde_json::json!(peers(nodes, wal)));
}

/// [`init`] with exactly `shards` WAL shards.
pub fn init_shards(nodes: &[Node], wal: &[i32], shards: u32) {
    post_init(
        nodes,
        &serde_json::json!({"peers": peers(nodes, wal), "shards": shards}),
    );
}

fn peers(nodes: &[Node], wal: &[i32]) -> Vec<serde_json::Value> {
    nodes
        .iter()
        .map(|node| {
            serde_json::json!({"id": node.id, "addr": node.http, "wal": wal.contains(&node.id)})
        })
        .collect()
}

fn post_init(nodes: &[Node], body: &serde_json::Value) {
    let init = reqwest::blocking::Client::new()
        .post(format!("http://{}/admin/init", nodes[0].http))
        .json(body)
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
}

/// Partition 0's leader epoch as Metadata through `via` reports it.
pub fn partition_epoch(via: &str) -> i32 {
    let mut stream = connect(via);
    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(TopicName(StrBytes::from_static_str(TOPIC)));
    send(
        &mut stream,
        ApiKey::Metadata,
        12,
        &MetadataRequest::default().with_topics(Some(vec![topic])),
    );
    let metadata = recv::<MetadataResponse>(&mut stream, 12);
    metadata.topics[0].partitions[0].leader_epoch
}

/// `OffsetForLeaderEpoch` for partition 0: `(error, epoch, end_offset)`.
pub fn epoch_end(addr: &str, current_leader_epoch: i32, leader_epoch: i32) -> (i16, i32, i64) {
    use camus_protocol::messages::offset_for_leader_epoch_request::{
        OffsetForLeaderPartition, OffsetForLeaderTopic,
    };
    use camus_protocol::messages::offset_for_leader_epoch_response::OffsetForLeaderEpochResponse;
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::OffsetForLeaderEpoch,
        4,
        &camus_protocol::messages::OffsetForLeaderEpochRequest::default()
            .with_replica_id(camus_protocol::messages::BrokerId(-1))
            .with_topics(vec![OffsetForLeaderTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![OffsetForLeaderPartition::default()
                    .with_partition(0)
                    .with_current_leader_epoch(current_leader_epoch)
                    .with_leader_epoch(leader_epoch)])]),
    );
    let response = recv::<OffsetForLeaderEpochResponse>(&mut stream, 4);
    let row = &response.topics[0].partitions[0];
    (row.error_code, row.leader_epoch, row.end_offset)
}

/// Fetch partition 0 at `offset` naming `current_leader_epoch`; the error.
pub fn fetch_in_epoch(addr: &str, current_leader_epoch: i32, offset: i64) -> i16 {
    let mut stream = connect(addr);
    let mut part = FetchPartition::default();
    part.partition = 0;
    part.fetch_offset = offset;
    part.current_leader_epoch = current_leader_epoch;
    part.partition_max_bytes = 1024 * 1024;
    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str(TOPIC));
    topic.partitions = vec![part];
    send(
        &mut stream,
        ApiKey::Fetch,
        11,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_topics(vec![topic]),
    );
    recv::<FetchResponse>(&mut stream, 11).responses[0].partitions[0].error_code
}
