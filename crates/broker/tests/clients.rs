//! Real clients for the Phase 5 paths the broker tests already cover.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::time::{Duration, Instant};

struct Node {
    child: Child,
    dir: PathBuf,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(compact: bool) -> Node {
    let dir = tempfile::tempdir().unwrap().keep();
    let log = std::fs::File::create(dir.join("broker.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_camus-broker"));
    command
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            dir.to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .env("CAMUS_FLUSH_MS", "40")
        .env("CAMUS_DEFAULT_DURABILITY", "wal")
        .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(log);
    if compact {
        command.env("CAMUS_COMPACT_MS", "100");
    }
    let child = command.spawn().unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{http}/admin/init"))
        .json(&serde_json::json!([{"id": 1, "addr": http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client.get(format!("http://{http}/admin/metrics")).send() {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1) {
                    if let Ok(body) = client
                        .get(format!("http://{http}/admin/read"))
                        .query(&[("key", "broker/1")])
                        .send()
                    {
                        if body.status().is_success() && !body.text().unwrap_or_default().is_empty()
                        {
                            return Node { child, dir, kafka };
                        }
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("node 1 did not become leader");
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

fn show(node: &Node, ran: &Output, client: &str) {
    if ran.status.success() {
        return;
    }
    let log = std::fs::read_to_string(node.dir.join("broker.log")).unwrap_or_default();
    panic!(
        "{client}\nstdout:\n{}\nstderr:\n{}\nbroker:\n{log}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
}

fn jars() -> Option<String> {
    let jars = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-jars");
    if !jars.join("kafka-clients-4.4.0.jar").is_file() {
        eprintln!("skipping java; missing kafka jars");
        return None;
    }
    let mut cp = Vec::new();
    for entry in std::fs::read_dir(&jars).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jar") {
            cp.push(path.display().to_string());
        }
    }
    Some(cp.join(":"))
}

fn jdk(tool: &str) -> String {
    let homebrew = format!("/opt/homebrew/opt/openjdk@21/bin/{tool}");
    if Path::new(&homebrew).is_file() {
        homebrew
    } else {
        tool.to_string()
    }
}

fn java(node: &Node, class_source: &str, args: &[&str]) {
    let Some(cp_jars) = jars() else {
        return;
    };
    let classes = node.dir.join("classes");
    std::fs::create_dir_all(&classes).unwrap();
    let cp = format!("{}:{cp_jars}", classes.display());
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(class_source);
    let compiled = Command::new(jdk("javac"))
        .args(["--release", "17", "-cp", &cp, "-d"])
        .arg(&classes)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "javac\n{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let mut command = Command::new(jdk("java"));
    command.args(["-cp", &cp, "Phase5RoundTrip"]).args(args);
    show(node, &command.output().unwrap(), "java");
}

fn python(node: &Node, mode: &str) {
    let python = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/pykafka/bin/python");
    if !python.is_file() {
        eprintln!("skipping librdkafka; missing {}", python.display());
        return;
    }
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/python/phase5_roundtrip.py");
    let ran = Command::new(python)
        .arg(script)
        .arg(mode)
        .arg(&node.kafka)
        .output()
        .unwrap();
    show(node, &ran, "librdkafka");
}

fn go(node: &Node, mode: &str) {
    let go = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/go/bin/go");
    if !go.is_file() {
        eprintln!("skipping franz-go; missing {}", go.display());
        return;
    }
    let ran = Command::new(go)
        .args(["run", "."])
        .arg(mode)
        .arg(&node.kafka)
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/gophase5"))
        .env("GOTOOLCHAIN", "local")
        .output()
        .unwrap();
    show(node, &ran, "franz-go");
}

#[test]
fn clients_commit_and_abort_a_transaction() {
    let node = spawn(false);
    python(&node, "txn");
    go(&node, "txn");
}

#[test]
fn clients_see_a_compacted_log() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping compaction clients; set CAMUS_MINIO=1");
        return;
    }
    let node = spawn(true);
    java(
        &node,
        "tests/java/Phase5RoundTrip.java",
        &["compact", &node.kafka],
    );
    python(&node, "compact");
    go(&node, "compact");
}

#[test]
fn clients_run_the_admin_apis() {
    let node = spawn(false);
    java(&node, "tests/java/Phase5RoundTrip.java", &["admin", &node.kafka]);
    python(&node, "admin");
    go(&node, "admin");
}

#[test]
fn clients_are_denied_by_an_acl() {
    let node = spawn(false);
    java(
        &node,
        "tests/java/Phase5RoundTrip.java",
        &["acl", &node.kafka],
    );
    python(&node, "acl");
    go(&node, "acl");
}
