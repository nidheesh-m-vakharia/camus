//! Phase 3 acceptance with real clients: two consumers share a group,
//! commit, and leave; a third member resumes from the committed offsets.
//! Three brokers, PLAINTEXT. Needs MinIO (`CAMUS_MINIO=1`) and the client
//! toolchains the SASL tests use.

mod support;

use std::path::PathBuf;
use std::process::{Command, Output};

use support::*;

fn cluster(root: &std::path::Path) -> (Vec<Node>, String) {
    cluster_with(root, Opts::default())
}

/// Short group timers so a killed member's session ends inside the test.
fn fast_groups() -> Opts {
    Opts::default()
        .env("CAMUS_GROUP_MIN_SESSION_MS", "1000")
        .env("CAMUS_GROUP_INITIAL_DELAY_MS", "0")
        .env("CAMUS_CONSUMER_SESSION_MS", "6000")
        .env("CAMUS_CONSUMER_HEARTBEAT_MS", "500")
}

fn cluster_with(root: &std::path::Path, opts: Opts) -> (Vec<Node>, String) {
    let nodes: Vec<Node> = (1..=3)
        .map(|id| Node::with(id, &root.join(format!("node-{id}")), opts.clone()))
        .collect();
    init(&nodes, &[1, 2, 3]);
    wait_settled(&nodes.iter().collect::<Vec<_>>());
    // Every broker registered, so Metadata lists all three.
    for node in &nodes {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while read_key(&nodes[0].http, &format!("broker/{}", node.id)).is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "broker {} never registered",
                node.id
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    let bootstrap = nodes.iter().map(kafka_addr).collect::<Vec<_>>().join(",");
    (nodes, bootstrap)
}

fn check(root: &std::path::Path, ran: Output, client: &str) {
    let stdout = String::from_utf8_lossy(&ran.stdout);
    let stderr = String::from_utf8_lossy(&ran.stderr);
    if !ran.status.success() {
        for id in 1..=3 {
            let log = std::fs::read_to_string(root.join(format!("node-{id}/broker.log")))
                .unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(30).collect();
            eprintln!(
                "--- node {id} log tail\n{}",
                tail.into_iter().rev().collect::<Vec<_>>().join("\n")
            );
        }
        panic!("{client} failed\nstdout:\n{stdout}\nstderr:\n{stderr}");
    }
    eprintln!("{stdout}");
}

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        eprintln!("skipping; set CAMUS_MINIO=1");
    }
    on
}

#[test]
fn librdkafka_group_commits_and_resumes() {
    if !minio() {
        return;
    }
    let python = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/pykafka/bin/python");
    if !python.is_file() {
        eprintln!("skipping librdkafka; missing {}", python.display());
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (_nodes, bootstrap) = cluster(root.path());
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/python/group_roundtrip.py");
    let ran = Command::new(python)
        .arg(script)
        .arg(&bootstrap)
        .output()
        .unwrap();
    check(root.path(), ran, "librdkafka");
}

#[test]
fn java_group_commits_and_resumes() {
    if !minio() {
        return;
    }
    let jars = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-jars");
    if !jars.join("kafka-clients-4.4.0.jar").is_file() {
        eprintln!("skipping java; run the SASL tests once to download the jars");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let classes = root.path().join("classes");
    std::fs::create_dir_all(&classes).unwrap();
    let mut cp = vec![classes.display().to_string()];
    for entry in std::fs::read_dir(&jars).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jar") {
            cp.push(path.display().to_string());
        }
    }
    let cp = cp.join(":");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/GroupRoundTrip.java");
    let compiled = Command::new("javac")
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
    let (_nodes, bootstrap) = cluster(root.path());
    let ran = Command::new("java")
        .args(["-cp", &cp, "GroupRoundTrip", &bootstrap])
        .output()
        .unwrap();
    check(root.path(), ran, "java");
}

#[test]
fn java_consumer_group_commits_and_resumes() {
    if !minio() {
        return;
    }
    let jars = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-jars");
    if !jars.join("kafka-clients-4.4.0.jar").is_file() {
        eprintln!("skipping java; run the SASL tests once to download the jars");
        return;
    }
    // The server tells the client how often to heartbeat. A short interval
    // lets the two members finish revocation inside the test.
    std::env::set_var("CAMUS_CONSUMER_HEARTBEAT_MS", "200");
    std::env::set_var("CAMUS_CONSUMER_SESSION_MS", "10000");
    let root = tempfile::tempdir().unwrap();
    let classes = root.path().join("classes");
    std::fs::create_dir_all(&classes).unwrap();
    let mut cp = vec![classes.display().to_string()];
    for entry in std::fs::read_dir(&jars).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jar") {
            cp.push(path.display().to_string());
        }
    }
    let cp = cp.join(":");
    let source =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/GroupConsumerRoundTrip.java");
    let compiled = Command::new("javac")
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
    let (_nodes, bootstrap) = cluster(root.path());
    let ran = Command::new("java")
        .args(["-cp", &cp, "GroupConsumerRoundTrip", &bootstrap])
        .output()
        .unwrap();
    check(root.path(), ran, "java consumer protocol");
}

#[test]
fn franz_go_group_commits_and_resumes() {
    if !minio() {
        return;
    }
    let go = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/go/bin/go");
    if !go.is_file() {
        eprintln!("skipping franz-go; missing {}", go.display());
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (_nodes, bootstrap) = cluster(root.path());
    let ran = Command::new(go)
        .args(["run", "."])
        .arg(&bootstrap)
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/gogroup"))
        .env("GOTOOLCHAIN", "local")
        .output()
        .unwrap();
    check(root.path(), ran, "franz-go");
}

fn jars() -> std::io::Result<Option<String>> {
    let jars = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-jars");
    if !jars.join("kafka-clients-4.4.0.jar").is_file() {
        eprintln!("skipping java; run the SASL tests once to download the jars");
        return Ok(None);
    }
    let mut cp = Vec::new();
    for entry in std::fs::read_dir(&jars)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jar") {
            cp.push(path.display().to_string());
        }
    }
    Ok(Some(cp.join(":")))
}

#[test]
fn java_redelivers_records_from_a_killed_member() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let Some(cp) = jars()? else {
        return Ok(());
    };
    let root = tempfile::tempdir()?;
    let classes = root.path().join("classes");
    std::fs::create_dir_all(&classes)?;
    let cp = format!("{}:{cp}", classes.display());
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/GroupKill.java");
    let compiled = Command::new("javac")
        .args(["--release", "17", "-cp", &cp, "-d"])
        .arg(&classes)
        .arg(&source)
        .output()?;
    assert!(
        compiled.status.success(),
        "javac\n{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let (_nodes, bootstrap) = cluster_with(root.path(), fast_groups());
    let ran = Command::new("java")
        .args(["-cp", &cp, "GroupKill", &bootstrap])
        .output()?;
    check(root.path(), ran, "java kill");
    Ok(())
}

#[test]
fn librdkafka_redelivers_records_from_a_killed_member() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let python = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/pykafka/bin/python");
    if !python.is_file() {
        eprintln!("skipping librdkafka; missing {}", python.display());
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let (_nodes, bootstrap) = cluster_with(root.path(), fast_groups());
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/python/group_kill.py");
    let ran = Command::new(python).arg(script).arg(&bootstrap).output()?;
    check(root.path(), ran, "librdkafka kill");
    Ok(())
}

#[test]
fn franz_go_redelivers_records_from_a_killed_member() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let go = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/go/bin/go");
    if !go.is_file() {
        eprintln!("skipping franz-go; missing {}", go.display());
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let (_nodes, bootstrap) = cluster_with(root.path(), fast_groups());
    let ran = Command::new(go)
        .args(["run", "."])
        .arg(&bootstrap)
        .arg("kill")
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/gogroup"))
        .env("GOTOOLCHAIN", "local")
        .output()?;
    check(root.path(), ran, "franz-go kill");
    Ok(())
}
