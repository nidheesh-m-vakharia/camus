//! Three broker processes form a metadata group. Killing the leader leaves
//! the committed write readable and a new write can commit.

use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use serde_json::Value;

struct Node {
    id: u64,
    child: Child,
    addr: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Node {
    fn spawn(id: u64, dir: &Path) -> Self {
        let dir = dir.join(format!("node-{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join("listen-addr"));
        let bin = env!("CARGO_BIN_EXE_camus-broker");
        let log = std::fs::File::create(dir.join("broker.log")).unwrap();
        let child = Command::new(bin)
            .env("CAMUS_WAL_SHARDS", "voters")
            .args([
                "--node-id",
                &id.to_string(),
                "--http-addr",
                "127.0.0.1:0",
                "--data-dir",
                dir.to_str().unwrap(),
                "--dev-plaintext",
            ])
            .env("RUST_LOG", "info")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let addr = wait_file(&dir.join("listen-addr"));
        Self { id, child, addr }
    }
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

fn get(addr: &str, path: &str) -> Value {
    let response = reqwest::blocking::get(format!("http://{addr}{path}")).unwrap();
    response.json().unwrap()
}

fn post(addr: &str, path: &str, body: Value) -> reqwest::blocking::Response {
    reqwest::blocking::Client::new()
        .post(format!("http://{addr}{path}"))
        .json(&body)
        .send()
        .unwrap()
}

#[test]
fn three_processes_survive_losing_the_leader() {
    let root = tempfile::tempdir().unwrap();
    let mut nodes: Vec<Node> = (1..=3).map(|id| Node::spawn(id, root.path())).collect();
    let peers: Vec<Value> = nodes
        .iter()
        .map(|node| serde_json::json!({"id": node.id, "addr": node.addr}))
        .collect();
    let init = post(&nodes[0].addr, "/admin/init", Value::Array(peers));
    assert!(init.status().is_success(), "{}", init.text().unwrap());

    let (leader_id, leader_addr) = wait_leader(&nodes);
    let write = post(
        &leader_addr,
        "/admin/write",
        serde_json::json!({"key": "kept", "value": "yes"}),
    );
    assert!(write.status().is_success(), "{}", write.text().unwrap());
    for node in &nodes {
        wait_value(&node.addr, "kept", "yes");
    }

    let leader_index = nodes.iter().position(|node| node.id == leader_id).unwrap();
    nodes[leader_index].child.kill().unwrap();
    nodes[leader_index].child.wait().unwrap();
    let survivors: Vec<&Node> = nodes.iter().filter(|node| node.id != leader_id).collect();

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut wrote = false;
    while Instant::now() < deadline {
        for node in &survivors {
            let response = post(
                &node.addr,
                "/admin/write",
                serde_json::json!({"key": "after", "value": "elected"}),
            );
            if response.status().is_success() {
                wrote = true;
                break;
            }
        }
        if wrote {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(wrote, "no survivor became leader");
    for node in &survivors {
        wait_value(&node.addr, "after", "elected");
        wait_value(&node.addr, "kept", "yes");
    }

    let metrics = reqwest::blocking::get(format!("http://{}/metrics", survivors[0].addr))
        .unwrap()
        .text()
        .unwrap();
    assert!(metrics.contains("camus_up 1"));
    assert!(metrics.contains("camus_raft_term"));
    assert!(metrics.contains("camus_wal_unsealed_bytes{shard=\"0\"}"));
    assert!(metrics.contains("camus_object_store_requests_total{op=\"put\"}"));
}

fn wait_leader(nodes: &[Node]) -> (u64, String) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        for node in nodes {
            let body = get(&node.addr, "/admin/metrics");
            if body.get("current_leader").and_then(|value| value.as_u64()) == Some(node.id) {
                return (node.id, node.addr.clone());
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("no leader elected");
}

fn wait_value(addr: &str, key: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let body = get(addr, &format!("/admin/read?key={key}"));
        if body.get("value").and_then(|value| value.as_str()) == Some(expected) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{addr} never applied {key}={expected}");
}
