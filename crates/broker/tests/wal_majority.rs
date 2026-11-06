//! ADR 0008 majority loss. Five brokers form the metadata group; brokers 1–3
//! are the WAL shard. With R2 cut off, two batches are acknowledged but not
//! sealed. Killing two shard voters takes the partition offline. Only
//! `truncate-to-sealed`, with its data-loss flag, brings it back, and only
//! then are the unsealed offsets gone: the partition resumes at the sealed
//! end, and the next produce reuses those offsets.
//!
//! Needs MinIO (`CAMUS_MINIO=1`). The brokers reach it through a proxy the
//! test can cut.

mod support;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::{ApiKey, FetchRequest, TopicName};
use camus_protocol::protocol::StrBytes;
use support::*;

/// TCP proxy to MinIO. `cut` drops every open connection and refuses new
/// ones until `restore`.
struct Proxy {
    addr: String,
    down: Arc<AtomicBool>,
    open: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn start(upstream: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let down = Arc::new(AtomicBool::new(false));
        let open = Arc::new(Mutex::new(Vec::new()));
        let (flag, streams) = (down.clone(), open.clone());
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if flag.load(Ordering::SeqCst) {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
                let Ok(server) = TcpStream::connect(upstream) else {
                    continue;
                };
                {
                    let mut open = streams.lock().unwrap();
                    open.push(client.try_clone().unwrap());
                    open.push(server.try_clone().unwrap());
                }
                pipe(client.try_clone().unwrap(), server.try_clone().unwrap());
                pipe(server, client);
            }
        });
        Self { addr, down, open }
    }

    fn cut(&self) {
        self.down.store(true, Ordering::SeqCst);
        for stream in self.open.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn restore(&self) {
        self.down.store(false, Ordering::SeqCst);
    }
}

fn pipe(mut from: TcpStream, mut to: TcpStream) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = to.shutdown(Shutdown::Both);
    });
}

fn read_key(http: &str, key: &str) -> Option<String> {
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
fn sealed_end(http: &str) -> i64 {
    let topic: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("topic/{TOPIC}")).unwrap()).unwrap();
    let id = topic["id"].as_str().unwrap();
    let part: serde_json::Value =
        serde_json::from_str(&read_key(http, &format!("part/{id}/0")).unwrap()).unwrap();
    part["next_offset"].as_i64().unwrap()
}

fn wait_sealed(http: &str, end: i64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if sealed_end(http) == end {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("sealed end never reached {end}");
}

fn fetch_code(addr: &str, offset: i64) -> i16 {
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

fn truncate(http: &str, accept: bool) -> reqwest::blocking::Response {
    reqwest::blocking::Client::new()
        .post(format!("http://{http}/admin/truncate-to-sealed"))
        .json(&serde_json::json!({ "i_accept_data_loss": accept }))
        .send()
        .unwrap()
}

#[test]
fn majority_loss_needs_truncate_to_sealed() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping majority-loss test; set CAMUS_MINIO=1");
        return;
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let s3 = format!("http://{}", proxy.addr);
    let root = tempfile::tempdir().unwrap();
    let mut nodes: Vec<Node> = (1..=5)
        .map(|id| Node::spawn_via(id, &root.path().join(format!("node-{id}")), &s3))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    create_wal_topic(&kafka_addr(
        nodes.iter().find(|node| node.id == meta_leader).unwrap(),
    ));

    // Broker 4 is not in the shard; it learns the shard leader from metadata.
    let outsider = kafka_addr(&nodes[3]);
    assert_eq!(produce_retrying(&outsider, "v0").1, 0);
    assert_eq!(produce_retrying(&outsider, "v1").1, 1);
    wait_sealed(&nodes[3].http, 2);

    // R2 is unreachable: these two are acknowledged by the shard quorum but
    // never sealed.
    proxy.cut();
    assert_eq!(produce_retrying(&outsider, "v2").1, 2);
    assert_eq!(produce_retrying(&outsider, "v3").1, 3);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(sealed_end(&nodes[3].http), 2, "R2 is cut off");

    // Lose the shard leader and one more voter: no quorum left.
    let (leader, _) = partition_leader(&outsider, None);
    let other = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let survivor = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader && *id != other)
        .unwrap();
    for id in [leader, other] {
        nodes[(id - 1) as usize].kill();
    }
    let survivor_node = &nodes[(survivor - 1) as usize];
    let survivor_kafka = kafka_addr(survivor_node);

    // Offline: the survivor neither accepts produce nor serves the tail, and
    // stays that way. It does not elect itself.
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        assert_ne!(produce(&survivor_kafka, "lost").0, 0);
        assert_eq!(fetch_code(&survivor_kafka, 0), 6);
        std::thread::sleep(Duration::from_millis(200));
    }
    let view = admin(&survivor_node.http).unwrap();
    assert_ne!(view["wal_leader"].as_i64(), Some(i64::from(survivor)));

    // The command is refused without the flag and on a broker outside the shard.
    assert_eq!(truncate(&survivor_node.http, false).status().as_u16(), 400);
    assert_eq!(truncate(&nodes[3].http, true).status().as_u16(), 409);
    proxy.restore();
    let done = truncate(&survivor_node.http, true);
    assert!(done.status().is_success(), "{}", done.text().unwrap());

    // The survivor leads a one-voter shard that starts at the sealed end.
    // Offsets 2 and 3 were acknowledged and are gone.
    let (now, now_addr) = partition_leader(&outsider, Some(leader));
    assert_eq!(now, survivor);
    assert_eq!(
        fetch_code(&now_addr, 3),
        1,
        "OFFSET_OUT_OF_RANGE past the sealed end"
    );
    let (_, offset) = produce_retrying(&outsider, "v4");
    assert_eq!(offset, 2, "the next produce appends at the sealed end");
    assert_eq!(read_all(&now_addr, 3), vec!["v0", "v1", "v4"]);
    wait_sealed(&nodes[3].http, 3);
}
