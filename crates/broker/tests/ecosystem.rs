//! The Kafka ecosystem against Camus: the Apache Kafka 4.4.0 CLI tools, Kafka
//! Connect, Kafka Streams with `exactly_once_v2`, MirrorMaker 2 from a real
//! Apache Kafka broker, KafkaJS, confluent-kafka-go, confluent-kafka-dotnet,
//! Schema Registry, and Debezium.
//!
//! Opt-in with `CAMUS_ECOSYSTEM=1`; the first run downloads the Kafka
//! distribution into `target/kafka-dist`. A JDK is required (`JAVA_HOME`, or
//! Homebrew's `openjdk@21`), `node`/`npm` for KafkaJS, Go and librdkafka for
//! confluent-kafka-go, and the .NET SDK for confluent-kafka-dotnet. Schema
//! Registry and Debezium run in Docker and need the broker on a non-loopback
//! address, so they are a further opt-in: `CAMUS_ECOSYSTEM_DOCKER=1`.

mod support;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use support::{kafka_addr, single, Node, Opts};

const KAFKA: &str = "kafka_2.13-4.4.0";

fn enabled() -> bool {
    let on = std::env::var("CAMUS_ECOSYSTEM").ok().as_deref() == Some("1");
    if !on {
        eprintln!("skipping ecosystem test; set CAMUS_ECOSYSTEM=1");
    }
    on
}

fn target() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
}

/// The unpacked Apache Kafka distribution, downloaded on first use.
fn dist() -> PathBuf {
    let root = target().join("kafka-dist");
    let home = root.join(KAFKA);
    if home.join("bin/kafka-topics.sh").is_file() {
        return home;
    }
    std::fs::create_dir_all(&root).unwrap();
    let archive = root.join("kafka.tgz");
    let fetched = Command::new("curl")
        .args(["-sfL", "-o"])
        .arg(&archive)
        .arg(format!(
            "https://archive.apache.org/dist/kafka/4.4.0/{KAFKA}.tgz"
        ))
        .status()
        .unwrap();
    assert!(fetched.success(), "download failed");
    let unpacked = Command::new("tar")
        .arg("-xzf")
        .arg(&archive)
        .arg("-C")
        .arg(&root)
        .status()
        .unwrap();
    assert!(unpacked.success(), "unpack failed");
    home
}

fn java_home() -> String {
    if let Ok(home) = std::env::var("JAVA_HOME") {
        if !home.is_empty() {
            return home;
        }
    }
    if let Ok(output) = Command::new("java")
        .args(["-XshowSettings:properties", "-version"])
        .output()
    {
        let text = String::from_utf8_lossy(&output.stderr);
        for line in text.lines() {
            if let Some(home) = line.trim().strip_prefix("java.home = ") {
                return home.to_string();
            }
        }
    }
    "/opt/homebrew/opt/openjdk@21".into()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Run a distribution script and return its combined output. Fails the test
/// on a non-zero exit unless `allow_failure`.
fn tool(script: &str, args: &[&str], stdin: Option<&str>, allow_failure: bool) -> String {
    let mut command = Command::new(dist().join("bin").join(script));
    command
        .args(args)
        .env("JAVA_HOME", java_home())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    if let Some(input) = stdin {
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    let output: Output = child.wait_with_output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        allow_failure || output.status.success(),
        "{script} {args:?} failed:\n{text}"
    );
    text
}

/// A long-running distribution process, stopped when dropped.
struct Background(Child);

impl Background {
    fn start(script: &str, args: &[&str], dir: &Path, name: &str) -> Self {
        let log = std::fs::File::create(dir.join(format!("{name}.log"))).unwrap();
        let child = Command::new(dist().join("bin").join(script))
            .args(args)
            .env("JAVA_HOME", java_home())
            .env("LOG_DIR", dir.join(format!("{name}-logs")))
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        Self(child)
    }
}

impl Background {
    /// A Camus broker listening on every interface and advertising
    /// `host.docker.internal`, for clients in containers.
    fn broker(dir: &Path, port: u16, http_port: u16) -> Self {
        let log = std::fs::File::create(dir.join("broker.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
            .env("CAMUS_WAL_SHARDS", "voters")
            .args(["--node-id", "1", "--http-addr"])
            .arg(format!("127.0.0.1:{http_port}"))
            .arg("--kafka-addr")
            .arg(format!("0.0.0.0:{port}"))
            .arg("--advertised-addr")
            .arg(format!("host.docker.internal:{port}"))
            .args(["--allow-non-loopback", "--dev-plaintext", "--data-dir"])
            .arg(dir.join("data"))
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));
        Self(child)
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn broker(dir: &Path) -> (Node, String) {
    let node = single(dir, Opts::default());
    let addr = kafka_addr(&node);
    (node, addr)
}

#[test]
fn cli_tools_manage_topics_groups_configs_and_records() {
    if !enabled() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let b = bootstrap.as_str();
    let created = tool(
        "kafka-topics.sh",
        &[
            "--bootstrap-server",
            b,
            "--create",
            "--topic",
            "cli",
            "--partitions",
            "3",
            "--config",
            "cleanup.policy=compact",
        ],
        None,
        false,
    );
    assert!(created.contains("Created topic cli"), "{created}");
    let described = tool(
        "kafka-topics.sh",
        &["--bootstrap-server", b, "--describe", "--topic", "cli"],
        None,
        false,
    );
    assert!(described.contains("PartitionCount: 3"), "{described}");
    let configs = tool(
        "kafka-configs.sh",
        &[
            "--bootstrap-server",
            b,
            "--entity-type",
            "topics",
            "--entity-name",
            "cli",
            "--describe",
        ],
        None,
        false,
    );
    assert!(configs.contains("cleanup.policy=compact"), "{configs}");
    tool(
        "kafka-configs.sh",
        &[
            "--bootstrap-server",
            b,
            "--entity-type",
            "topics",
            "--entity-name",
            "cli",
            "--alter",
            "--add-config",
            "retention.ms=60000",
        ],
        None,
        false,
    );
    let broker_configs = tool(
        "kafka-configs.sh",
        &[
            "--bootstrap-server",
            b,
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            "--describe",
            "--all",
        ],
        None,
        false,
    );
    assert!(
        broker_configs.contains("message.max.bytes=1048588"),
        "{broker_configs}"
    );

    tool(
        "kafka-console-producer.sh",
        &[
            "--bootstrap-server",
            b,
            "--topic",
            "cli",
            "--reader-property",
            "parse.key=true",
            "--reader-property",
            "key.separator=:",
        ],
        Some("k1:v1\nk2:v2\n"),
        false,
    );
    let consumed = tool(
        "kafka-console-consumer.sh",
        &[
            "--bootstrap-server",
            b,
            "--topic",
            "cli",
            "--from-beginning",
            "--group",
            "cli-group",
            "--max-messages",
            "2",
            "--timeout-ms",
            "20000",
        ],
        None,
        false,
    );
    assert!(
        consumed.contains("v1") && consumed.contains("v2"),
        "{consumed}"
    );
    let producers = tool(
        "kafka-transactions.sh",
        &[
            "--bootstrap-server",
            b,
            "describe-producers",
            "--topic",
            "cli",
            "--partition",
            "0",
        ],
        None,
        false,
    );
    assert!(
        producers.contains("ProducerId") || producers.contains("producer"),
        "{producers}"
    );
    let groups = tool(
        "kafka-consumer-groups.sh",
        &[
            "--bootstrap-server",
            b,
            "--describe",
            "--group",
            "cli-group",
        ],
        None,
        false,
    );
    assert!(
        groups.contains("cli-group") && groups.contains("cli"),
        "{groups}"
    );
    let offsets = tool(
        "kafka-get-offsets.sh",
        &["--bootstrap-server", b, "--topic", "cli"],
        None,
        false,
    );
    assert!(offsets.contains("cli:0:"), "{offsets}");
    let plan = dir.path().join("delete.json");
    std::fs::write(
        &plan,
        r#"{"partitions":[{"topic":"cli","partition":0,"offset":0}],"version":1}"#,
    )
    .unwrap();
    let deleted = tool(
        "kafka-delete-records.sh",
        &[
            "--bootstrap-server",
            b,
            "--offset-json-file",
            plan.to_str().unwrap(),
        ],
        None,
        false,
    );
    assert!(deleted.contains("low_watermark: 0"), "{deleted}");
    let dirs = tool(
        "kafka-log-dirs.sh",
        &["--bootstrap-server", b, "--describe"],
        None,
        false,
    );
    assert!(dirs.contains("s3://"), "{dirs}");
    tool(
        "kafka-acls.sh",
        &[
            "--bootstrap-server",
            b,
            "--add",
            "--allow-principal",
            "User:bob",
            "--operation",
            "Read",
            "--topic",
            "acl-only",
        ],
        None,
        false,
    );
    let acls = tool(
        "kafka-acls.sh",
        &["--bootstrap-server", b, "--list"],
        None,
        false,
    );
    assert!(acls.contains("User:bob"), "{acls}");
    tool(
        "kafka-transactions.sh",
        &["--bootstrap-server", b, "list"],
        None,
        false,
    );
    let elected = tool(
        "kafka-leader-election.sh",
        &[
            "--bootstrap-server",
            b,
            "--election-type",
            "PREFERRED",
            "--all-topic-partitions",
        ],
        None,
        false,
    );
    assert!(
        elected.contains("Valid replica already elected"),
        "{elected}"
    );
    let versions = tool(
        "kafka-broker-api-versions.sh",
        &["--bootstrap-server", b],
        None,
        false,
    );
    assert!(versions.contains("Produce(0)"), "{versions}");
    tool(
        "kafka-consumer-groups.sh",
        &["--bootstrap-server", b, "--delete", "--group", "cli-group"],
        None,
        false,
    );
    tool(
        "kafka-topics.sh",
        &["--bootstrap-server", b, "--delete", "--topic", "cli"],
        None,
        false,
    );
}

#[test]
fn connect_copies_through_file_connectors_and_resumes() {
    if !enabled() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let rest = free_port();
    let properties = dir.path().join("connect.properties");
    std::fs::write(
        &properties,
        format!(
            "bootstrap.servers={bootstrap}\n\
             group.id=camus-connect\n\
             key.converter=org.apache.kafka.connect.storage.StringConverter\n\
             value.converter=org.apache.kafka.connect.storage.StringConverter\n\
             config.storage.topic=connect-configs\n\
             offset.storage.topic=connect-offsets\n\
             status.storage.topic=connect-status\n\
             config.storage.replication.factor=1\n\
             offset.storage.replication.factor=1\n\
             status.storage.replication.factor=1\n\
             listeners=http://127.0.0.1:{rest}\n\
             plugin.path={}\n\
             offset.flush.interval.ms=500\n",
            dist().join("libs/connect-file-4.4.0.jar").display()
        ),
    )
    .unwrap();
    let input = dir.path().join("in.txt");
    let output = dir.path().join("out.txt");
    std::fs::write(&input, "line-1\nline-2\nline-3\n").unwrap();
    let base = format!("http://127.0.0.1:{rest}");
    let http = reqwest::blocking::Client::new();
    let ready = |http: &reqwest::blocking::Client| {
        http.get(format!("{base}/connectors"))
            .send()
            .is_ok_and(|response| response.status().is_success())
    };
    let lines = || {
        std::fs::read_to_string(&output)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    {
        let _worker = Background::start(
            "connect-distributed.sh",
            &[properties.to_str().unwrap()],
            dir.path(),
            "connect",
        );
        wait_for("Connect REST", Duration::from_secs(60), || ready(&http));
        let configs = tool(
            "kafka-configs.sh",
            &[
                "--bootstrap-server",
                &bootstrap,
                "--entity-type",
                "topics",
                "--entity-name",
                "connect-configs",
                "--describe",
            ],
            None,
            false,
        );
        assert!(configs.contains("cleanup.policy=compact"), "{configs}");
        for (name, class, file, field) in [
            ("source", "FileStreamSource", &input, "topic"),
            ("sink", "FileStreamSink", &output, "topics"),
        ] {
            let body = serde_json::json!({
                "connector.class": class,
                "file": file.to_str().unwrap(),
                field: "lines",
                "tasks.max": "1",
            });
            let response = http
                .put(format!("{base}/connectors/{name}/config"))
                .json(&body)
                .send()
                .unwrap();
            assert!(
                response.status().is_success(),
                "{}",
                response.text().unwrap()
            );
        }
        wait_for("the sink file", Duration::from_secs(60), || {
            lines().len() >= 3
        });
        // Give the source time to commit its offset before the restart.
        std::thread::sleep(Duration::from_secs(3));
    }
    std::fs::write(&input, "line-1\nline-2\nline-3\nline-4\n").unwrap();
    let _worker = Background::start(
        "connect-distributed.sh",
        &[properties.to_str().unwrap()],
        dir.path(),
        "connect-again",
    );
    wait_for("the resumed sink", Duration::from_secs(60), || {
        lines().len() >= 4
    });
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(lines(), vec!["line-1", "line-2", "line-3", "line-4"]);
}

#[test]
fn streams_counts_words_exactly_once() {
    if !enabled() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let classes = dir.path().join("classes");
    std::fs::create_dir_all(&classes).unwrap();
    let classpath = format!("{}/*", dist().join("libs").display());
    let compiled = Command::new(format!("{}/bin/javac", java_home()))
        .args(["-proc:none", "-cp", &classpath, "-d"])
        .arg(&classes)
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ecosystem/WordCount.java"))
        .status()
        .unwrap();
    assert!(compiled.success());
    for topic in ["words-in", "words-out"] {
        tool(
            "kafka-topics.sh",
            &[
                "--bootstrap-server",
                &bootstrap,
                "--create",
                "--topic",
                topic,
                "--partitions",
                "2",
            ],
            None,
            false,
        );
    }
    let log = std::fs::File::create(dir.path().join("streams.log")).unwrap();
    let mut app = Command::new(format!("{}/bin/java", java_home()))
        .arg("-cp")
        .arg(format!("{}:{classpath}", classes.display()))
        .args(["WordCount", &bootstrap, "camus-wordcount"])
        .arg(dir.path().join("state"))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_secs(8));
    tool(
        "kafka-console-producer.sh",
        &["--bootstrap-server", &bootstrap, "--topic", "words-in"],
        Some("the cat\nthe dog\nthe end\n"),
        false,
    );
    let mut counts = std::collections::BTreeMap::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while counts.get("the").map(String::as_str) != Some("3") {
        assert!(
            Instant::now() < deadline,
            "counts never settled: {counts:?}"
        );
        let out = tool(
            "kafka-console-consumer.sh",
            &[
                "--bootstrap-server",
                &bootstrap,
                "--topic",
                "words-out",
                "--from-beginning",
                "--isolation-level",
                "read_committed",
                "--formatter-property",
                "print.key=true",
                "--timeout-ms",
                "5000",
            ],
            None,
            true,
        );
        for line in out.lines() {
            if let Some((word, count)) = line.split_once('\t') {
                counts.insert(word.to_string(), count.to_string());
            }
        }
    }
    let _ = app.kill();
    let _ = app.wait();
    assert_eq!(counts.get("cat").map(String::as_str), Some("1"));
    assert_eq!(counts.get("dog").map(String::as_str), Some("1"));
}

#[test]
fn mirror_maker_copies_from_apache_kafka() {
    if !enabled() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let (client, controller) = (free_port(), free_port());
    let source = format!("127.0.0.1:{client}");
    let properties = dir.path().join("server.properties");
    std::fs::write(
        &properties,
        format!(
            "process.roles=broker,controller\n\
             node.id=1\n\
             controller.quorum.voters=1@127.0.0.1:{controller}\n\
             listeners=PLAINTEXT://{source},CONTROLLER://127.0.0.1:{controller}\n\
             advertised.listeners=PLAINTEXT://{source}\n\
             controller.listener.names=CONTROLLER\n\
             listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT\n\
             log.dirs={}\n\
             offsets.topic.replication.factor=1\n\
             transaction.state.log.replication.factor=1\n\
             transaction.state.log.min.isr=1\n",
            dir.path().join("apache-data").display()
        ),
    )
    .unwrap();
    let cluster = tool("kafka-storage.sh", &["random-uuid"], None, false);
    let cluster = cluster.lines().last().unwrap().trim().to_string();
    tool(
        "kafka-storage.sh",
        &["format", "-t", &cluster, "-c", properties.to_str().unwrap()],
        None,
        false,
    );
    let _apache = Background::start(
        "kafka-server-start.sh",
        &[properties.to_str().unwrap()],
        dir.path(),
        "apache",
    );
    wait_for("Apache Kafka", Duration::from_secs(60), || {
        !tool(
            "kafka-topics.sh",
            &["--bootstrap-server", &source, "--list"],
            None,
            true,
        )
        .contains("Exception")
    });
    tool(
        "kafka-topics.sh",
        &[
            "--bootstrap-server",
            &source,
            "--create",
            "--topic",
            "orders",
            "--partitions",
            "2",
        ],
        None,
        false,
    );
    tool(
        "kafka-console-producer.sh",
        &["--bootstrap-server", &source, "--topic", "orders"],
        Some("o1\no2\no3\n"),
        false,
    );
    let config = dir.path().join("mm2.properties");
    std::fs::write(
        &config,
        format!(
            "clusters=apache, camus\n\
             apache.bootstrap.servers={source}\n\
             camus.bootstrap.servers={bootstrap}\n\
             apache->camus.enabled=true\n\
             apache->camus.topics=orders\n\
             replication.factor=1\n\
             checkpoints.topic.replication.factor=1\n\
             heartbeats.topic.replication.factor=1\n\
             offset-syncs.topic.replication.factor=1\n\
             offset.storage.replication.factor=1\n\
             status.storage.replication.factor=1\n\
             config.storage.replication.factor=1\n\
             sync.topic.acls.enabled=false\n"
        ),
    )
    .unwrap();
    let _mirror = Background::start(
        "connect-mirror-maker.sh",
        &[config.to_str().unwrap()],
        dir.path(),
        "mirror",
    );
    let mut seen = Vec::new();
    wait_for("mirrored records", Duration::from_secs(120), || {
        let out = tool(
            "kafka-console-consumer.sh",
            &[
                "--bootstrap-server",
                &bootstrap,
                "--topic",
                "apache.orders",
                "--from-beginning",
                "--timeout-ms",
                "5000",
            ],
            None,
            true,
        );
        seen = out
            .lines()
            .filter(|line| line.starts_with('o') && line.len() == 2)
            .map(str::to_string)
            .collect();
        seen.len() >= 3
    });
    seen.sort();
    assert_eq!(seen, vec!["o1", "o2", "o3"]);
}

#[test]
fn kafkajs_round_trips() {
    if !enabled() {
        return;
    }
    if Command::new("npm").arg("--version").output().is_err() {
        support::skipped("KafkaJS; npm is not installed");
        return;
    }
    let modules = target().join("kafkajs");
    if !modules.join("node_modules/kafkajs").is_dir() {
        std::fs::create_dir_all(&modules).unwrap();
        let installed = Command::new("npm")
            .args(["install", "--silent", "--prefix"])
            .arg(&modules)
            .arg("kafkajs@2")
            .status()
            .unwrap();
        assert!(installed.success(), "npm install kafkajs failed");
    }
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let ran = Command::new("node")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ecosystem/kafkajs_roundtrip.js"))
        .arg(&bootstrap)
        .env("NODE_PATH", modules.join("node_modules"))
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(
        ran.status.success() && text.contains("kafkajs ok"),
        "{text}"
    );
}

fn program(name: &str, fallbacks: &[&str]) -> Option<PathBuf> {
    if let Ok(path) = std::env::var("PATH") {
        for dir in path.split(':') {
            let candidate = PathBuf::from(dir).join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    fallbacks
        .iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
}

#[test]
fn confluent_kafka_go_round_trips() {
    if !enabled() {
        return;
    }
    let Some(go) = program("go", &["/opt/homebrew/bin/go"]) else {
        support::skipped("confluent-kafka-go; go is not installed");
        return;
    };
    let source =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ecosystem/confluent_kafka_go");
    let pkg = Command::new("pkg-config")
        .args(["--variable=pcfiledir", "rdkafka"])
        .output();
    let mut command = Command::new(go);
    command
        .args(["run", "-tags", "dynamic", "."])
        .current_dir(&source)
        .env("CGO_ENABLED", "1");
    if let Ok(output) = pkg {
        if output.status.success() {
            let dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !dir.is_empty() {
                let joined = match std::env::var("PKG_CONFIG_PATH") {
                    Ok(existing) if !existing.is_empty() => format!("{dir}:{existing}"),
                    _ => dir,
                };
                command.env("PKG_CONFIG_PATH", joined);
            }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let ran = command.arg(&bootstrap).output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(
        ran.status.success() && text.contains("confluent-kafka-go ok"),
        "{text}"
    );
}

#[test]
fn confluent_kafka_dotnet_round_trips() {
    if !enabled() {
        return;
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let fallback = format!("{home}/.dotnet/dotnet");
    let Some(dotnet) = program("dotnet", &["/opt/homebrew/bin/dotnet", &fallback]) else {
        support::skipped("confluent-kafka-dotnet; dotnet is not installed");
        return;
    };
    let source =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ecosystem/confluent_kafka_dotnet");
    let dir = tempfile::tempdir().unwrap();
    let (_node, bootstrap) = broker(dir.path());
    let ran = Command::new(dotnet)
        .args(["run", "--project"])
        .arg(&source)
        .arg("--")
        .arg(&bootstrap)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(
        ran.status.success() && text.contains("confluent-kafka-dotnet ok"),
        "{text}"
    );
}

struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// A fresh Schema Registry process. It stores nothing of its own: a restart
/// has to read `_schemas` back from the broker.
fn start_schema_registry(broker_port: u16) -> (Container, String) {
    let registry = free_port();
    let name = format!("camus-sr-{registry}");
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "--add-host",
            "host.docker.internal:host-gateway",
            "-p",
        ])
        .arg(format!("{registry}:8081"))
        .args([
            "-e",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
            "-e",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
            "-e",
        ])
        .arg(format!(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:{broker_port}"
        ))
        .args([
            "-e",
            "SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=1",
            "confluentinc/cp-schema-registry:latest",
        ])
        .output()
        .unwrap();
    assert!(
        started.status.success(),
        "docker run schema-registry failed: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    (Container(name), format!("http://127.0.0.1:{registry}"))
}

fn schema_registry_ready(http: &reqwest::blocking::Client, base: &str) {
    wait_for("Schema Registry", Duration::from_secs(120), || {
        http.get(format!("{base}/subjects"))
            .send()
            .is_ok_and(|response| response.status().is_success())
    });
}

/// Schema Registry in Docker. The container reaches the broker through
/// `host.docker.internal`, so the broker listens on every interface and
/// advertises that name. The registry is then thrown away and a new one must
/// load the same schema id from `_schemas`. Opt-in: `CAMUS_ECOSYSTEM_DOCKER=1`.
#[test]
fn schema_registry_stores_a_schema() {
    if !enabled() {
        return;
    }
    if std::env::var("CAMUS_ECOSYSTEM_DOCKER").ok().as_deref() != Some("1") {
        support::skipped("Schema Registry; set CAMUS_ECOSYSTEM_DOCKER=1");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let http_port = free_port();
    let _broker = Background::broker(dir.path(), port, http_port);
    let http = reqwest::blocking::Client::new();
    let init = http
        .post(format!("http://127.0.0.1:{http_port}/admin/init"))
        .json(&serde_json::json!([{"id": 1, "addr": format!("127.0.0.1:{http_port}")}]))
        .send()
        .unwrap();
    let init_status = init.status();
    assert!(init_status.is_success(), "{}", init.text().unwrap());

    let (container, base) = start_schema_registry(port);
    schema_registry_ready(&http, &base);
    let registered = http
        .post(format!("{base}/subjects/orders-value/versions"))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(r#"{"schema":"{\"type\":\"string\"}"}"#)
        .send()
        .unwrap();
    let registered_status = registered.status();
    let registered_body = registered.text().unwrap();
    assert!(registered_status.is_success(), "{registered_body}");
    let schema_id = serde_json::from_str::<serde_json::Value>(&registered_body).unwrap()["id"]
        .as_i64()
        .expect("schema id");
    drop(container);

    let (_restarted, base) = start_schema_registry(port);
    schema_registry_ready(&http, &base);
    let loaded = http
        .get(format!("{base}/subjects/orders-value/versions/latest"))
        .send()
        .unwrap();
    let loaded_status = loaded.status();
    let loaded_body = loaded.text().unwrap();
    assert!(loaded_status.is_success(), "{loaded_body}");
    let loaded: serde_json::Value = serde_json::from_str(&loaded_body).unwrap();
    assert_eq!(loaded["id"].as_i64(), Some(schema_id), "{loaded}");
    // Schema Registry stores the canonical Avro form, not the object we posted.
    assert_eq!(loaded["schema"].as_str(), Some("\"string\""), "{loaded}");
    assert_eq!(loaded["schemaType"].as_str(), Some("AVRO"), "{loaded}");
}

type Res<T> = Result<T, Box<dyn std::error::Error>>;

/// Debezium release whose Connect worker and sample databases the tests use.
const DEBEZIUM: &str = "3.7.1.Final";

/// Run `docker` and return its stdout, or an error carrying its stderr.
fn docker(args: &[&str]) -> Res<String> {
    let output = Command::new("docker").args(args).output()?;
    if !output.status.success() {
        return Err(format!(
            "docker {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Poll `done` every half second until it holds or `within` runs out.
fn poll(what: &str, within: Duration, mut done: impl FnMut() -> Res<bool>) -> Res<()> {
    let deadline = Instant::now() + within;
    while !done()? {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Ok(())
}

/// A Docker network, removed when dropped. Declare it before the containers
/// on it so they are dropped first.
struct Network(String);

impl Drop for Network {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["network", "rm", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// A Camus broker for containers, initialised, plus the network the
/// databases and the Connect worker share.
struct DebeziumSetup {
    _dir: tempfile::TempDir,
    _broker: Background,
    port: u16,
    network: Network,
}

impl DebeziumSetup {
    fn start() -> Res<Self> {
        let dir = tempfile::tempdir()?;
        let port = free_port();
        let http_port = free_port();
        let broker = Background::broker(dir.path(), port, http_port);
        let init = reqwest::blocking::Client::new()
            .post(format!("http://127.0.0.1:{http_port}/admin/init"))
            .json(&serde_json::json!([{"id": 1, "addr": format!("127.0.0.1:{http_port}")}]))
            .send()?;
        if !init.status().is_success() {
            return Err(format!("init failed: {}", init.text()?).into());
        }
        let name = format!("camus-dbz-{port}");
        docker(&["network", "create", &name])?;
        Ok(Self {
            _dir: dir,
            _broker: broker,
            port,
            network: Network(name),
        })
    }

    fn bootstrap(&self) -> String {
        format!("host.docker.internal:{}", self.port)
    }

    /// The Debezium sample Postgres (`inventory` schema, `wal_level=logical`).
    fn postgres(&self) -> Res<Container> {
        let name = format!("{}-postgres", self.network.0);
        docker(&[
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "--network",
            &self.network.0,
            "--network-alias",
            "postgres",
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            &format!("quay.io/debezium/example-postgres:{DEBEZIUM}"),
        ])?;
        let container = Container(name);
        poll("Postgres", Duration::from_secs(120), || {
            Ok(Command::new("docker")
                .args(["exec", &container.0, "pg_isready", "-U", "postgres"])
                .output()?
                .status
                .success())
        })?;
        Ok(container)
    }

    /// The Debezium sample MySQL (`inventory` database, binlog in row mode).
    fn mysql(&self) -> Res<Container> {
        let name = format!("{}-mysql", self.network.0);
        docker(&[
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "--network",
            &self.network.0,
            "--network-alias",
            "mysql",
            "-e",
            "MYSQL_ROOT_PASSWORD=debezium",
            "-e",
            "MYSQL_USER=mysqluser",
            "-e",
            "MYSQL_PASSWORD=mysqlpw",
            &format!("quay.io/debezium/example-mysql:{DEBEZIUM}"),
        ])?;
        let container = Container(name);
        // The image restarts mysqld once after loading `inventory`, so wait
        // for a query against it rather than for the port.
        poll("MySQL", Duration::from_secs(180), || {
            Ok(Command::new("docker")
                .args([
                    "exec",
                    &container.0,
                    "mysql",
                    "-uroot",
                    "-pdebezium",
                    "-e",
                    "SELECT COUNT(*) FROM inventory.customers",
                ])
                .output()?
                .status
                .success())
        })?;
        Ok(container)
    }

    /// A Debezium Connect worker on the shared network that reaches the
    /// broker through `host.docker.internal`. Returns the container and its
    /// REST base URL once REST answers.
    fn worker(&self, name: &str, exactly_once: bool) -> Res<(Worker, String)> {
        let rest = free_port();
        let container = format!("{}-{name}", self.network.0);
        let bootstrap = format!("BOOTSTRAP_SERVERS={}", self.bootstrap());
        let published = format!("{rest}:8083");
        let image = format!("quay.io/debezium/connect:{DEBEZIUM}");
        let mut args = vec![
            "run",
            "-d",
            "--rm",
            "--name",
            &container,
            "--network",
            &self.network.0,
            "--add-host",
            "host.docker.internal:host-gateway",
            "-p",
            &published,
            "-e",
            &bootstrap,
            "-e",
            "GROUP_ID=camus-debezium",
            "-e",
            "CONFIG_STORAGE_TOPIC=dbz-configs",
            "-e",
            "OFFSET_STORAGE_TOPIC=dbz-offsets",
            "-e",
            "STATUS_STORAGE_TOPIC=dbz-status",
            "-e",
            "OFFSET_FLUSH_INTERVAL_MS=500",
            "-e",
            "CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false",
            "-e",
            "CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false",
        ];
        if exactly_once {
            args.extend(["-e", "CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT=enabled"]);
        }
        args.push(&image);
        docker(&args)?;
        let worker = Worker(Container(container));
        let base = format!("http://127.0.0.1:{rest}");
        let http = reqwest::blocking::Client::new();
        poll("Connect REST", Duration::from_secs(120), || {
            Ok(http
                .get(format!("{base}/connectors"))
                .send()
                .is_ok_and(|response| response.status().is_success()))
        })?;
        Ok((worker, base))
    }
}

/// A Connect worker container. Its log is copied to
/// `target/debezium-logs/<name>.log` before the container is removed.
struct Worker(Container);

impl Drop for Worker {
    fn drop(&mut self) {
        let dir = target().join("debezium-logs");
        let Ok(output) = Command::new("docker").args(["logs", &self.0 .0]).output() else {
            return;
        };
        if std::fs::create_dir_all(&dir).is_ok() {
            let mut log = output.stdout;
            log.extend_from_slice(&output.stderr);
            let _ = std::fs::write(dir.join(format!("{}.log", self.0 .0)), log);
        }
    }
}

/// Stop a worker with SIGTERM so it commits its source offsets on the way out.
fn stop_worker(worker: Worker) -> Res<()> {
    docker(&["stop", "-t", "30", &worker.0 .0])?;
    drop(worker);
    Ok(())
}

fn put_connector(base: &str, name: &str, config: serde_json::Value) -> Res<()> {
    let response = reqwest::blocking::Client::new()
        .put(format!("{base}/connectors/{name}/config"))
        .json(&config)
        .send()?;
    if !response.status().is_success() {
        return Err(format!("connector {name}: {}", response.text()?).into());
    }
    Ok(())
}

/// The console consumer's output for every record on `topic`, read from the
/// beginning inside `worker`: one `key<TAB>value` per record, except that a
/// pretty-printed value (Debezium's schema history) spans several lines.
fn consume(worker: &Worker, bootstrap: &str, topic: &str, read_committed: bool) -> Res<String> {
    let mut args = vec![
        "exec",
        &worker.0 .0,
        "/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--from-beginning",
        "--timeout-ms",
        "8000",
        "--formatter-property",
        "print.key=true",
        "--formatter-property",
        "key.separator=\t",
    ];
    if read_committed {
        args.extend(["--isolation-level", "read_committed"]);
    }
    // The consumer exits non-zero when it times out on a topic that does not
    // exist yet; that reads as no events.
    let output = Command::new("docker").args(&args).output()?;
    Ok(String::from_utf8(output.stdout)?)
}

/// Every record on a Debezium change topic as `(key, value)`. A tombstone's
/// value is `None`.
fn change_events(
    worker: &Worker,
    bootstrap: &str,
    topic: &str,
    read_committed: bool,
) -> Res<Vec<(String, Option<serde_json::Value>)>> {
    let mut events = Vec::new();
    for line in consume(worker, bootstrap, topic, read_committed)?.lines() {
        let Some((key, value)) = line.split_once('\t') else {
            continue;
        };
        let value = match value {
            "null" => None,
            json => Some(
                serde_json::from_str(json)
                    .map_err(|e| format!("{topic}: {e} in record {line:?}"))?,
            ),
        };
        events.push((key.to_string(), value));
    }
    Ok(events)
}

fn ops(events: &[(String, Option<serde_json::Value>)], op: &str) -> usize {
    events
        .iter()
        .filter(|(_, value)| value.as_ref().is_some_and(|v| v["op"] == op))
        .count()
}

/// Created rows whose `after` column `column` equals `value`.
fn created(events: &[(String, Option<serde_json::Value>)], column: &str, value: &str) -> usize {
    events
        .iter()
        .filter(|(_, event)| {
            event
                .as_ref()
                .is_some_and(|v| v["op"] == "c" && v["after"][column] == value)
        })
        .count()
}

fn tombstones(events: &[(String, Option<serde_json::Value>)]) -> usize {
    events.iter().filter(|(_, value)| value.is_none()).count()
}

fn psql(postgres: &Container, sql: &str) -> Res<()> {
    docker(&[
        "exec",
        &postgres.0,
        "psql",
        "-U",
        "postgres",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ])?;
    Ok(())
}

fn mysql_exec(mysql: &Container, sql: &str) -> Res<()> {
    docker(&[
        "exec",
        &mysql.0,
        "mysql",
        "-uroot",
        "-pdebezium",
        "inventory",
        "-e",
        sql,
    ])?;
    Ok(())
}

fn postgres_connector() -> serde_json::Value {
    serde_json::json!({
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "topic.prefix": "pg",
        "table.include.list": "inventory.customers",
        "plugin.name": "pgoutput",
        "slot.name": "camus",
        "tasks.max": "1",
    })
}

fn docker_ecosystem() -> bool {
    if !enabled() {
        return false;
    }
    if std::env::var("CAMUS_ECOSYSTEM_DOCKER").ok().as_deref() != Some("1") {
        support::skipped("Debezium; set CAMUS_ECOSYSTEM_DOCKER=1");
        return false;
    }
    true
}

/// Debezium's Postgres and MySQL connectors on one Connect worker: snapshot,
/// streamed insert, update, delete and tombstone, a MySQL schema change kept
/// in the schema history topic, and a worker restart that resumes from the
/// committed offsets without a second snapshot. Opt-in:
/// `CAMUS_ECOSYSTEM_DOCKER=1`.
#[test]
fn debezium_captures_postgres_and_mysql_and_resumes() -> Res<()> {
    if !docker_ecosystem() {
        return Ok(());
    }
    let setup = DebeziumSetup::start()?;
    let bootstrap = setup.bootstrap();
    let postgres = setup.postgres()?;
    let mysql = setup.mysql()?;
    let (worker, base) = setup.worker("connect", false)?;
    put_connector(&base, "pg", postgres_connector())?;
    put_connector(
        &base,
        "my",
        serde_json::json!({
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "mysql",
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "dbz",
            "database.server.id": "184054",
            "topic.prefix": "my",
            "database.include.list": "inventory",
            "table.include.list": "inventory.customers",
            "schema.history.internal.kafka.bootstrap.servers": bootstrap,
            "schema.history.internal.kafka.topic": "my-schema-history",
            "tasks.max": "1",
        }),
    )?;

    let pg_topic = "pg.inventory.customers";
    let my_topic = "my.inventory.customers";
    for topic in [pg_topic, my_topic] {
        poll(
            &format!("the {topic} snapshot"),
            Duration::from_secs(120),
            || Ok(ops(&change_events(&worker, &bootstrap, topic, false)?, "r") >= 4),
        )?;
    }

    psql(
        &postgres,
        "INSERT INTO inventory.customers (first_name, last_name, email) \
         VALUES ('Ada', 'Lovelace', 'ada@example.com'); \
         UPDATE inventory.customers SET last_name = 'Byron' WHERE email = 'ada@example.com'; \
         DELETE FROM inventory.customers WHERE email = 'ada@example.com';",
    )?;
    mysql_exec(
        &mysql,
        "INSERT INTO customers (first_name, last_name, email) \
         VALUES ('Ada', 'Lovelace', 'ada@example.com'); \
         UPDATE customers SET last_name = 'Byron' WHERE email = 'ada@example.com'; \
         DELETE FROM customers WHERE email = 'ada@example.com'; \
         ALTER TABLE customers ADD COLUMN tier VARCHAR(16) DEFAULT 'basic'; \
         INSERT INTO customers (first_name, last_name, email, tier) \
         VALUES ('Alan', 'Turing', 'alan@example.com', 'gold');",
    )?;
    for topic in [pg_topic, my_topic] {
        poll(&format!("{topic} changes"), Duration::from_secs(60), || {
            let events = change_events(&worker, &bootstrap, topic, false)?;
            Ok(ops(&events, "d") >= 1 && tombstones(&events) >= 1)
        })?;
        let events = change_events(&worker, &bootstrap, topic, false)?;
        assert_eq!(created(&events, "email", "ada@example.com"), 1, "{topic}");
        assert_eq!(ops(&events, "u"), 1, "{topic}");
        assert_eq!(ops(&events, "d"), 1, "{topic}");
        assert_eq!(tombstones(&events), 1, "{topic}");
        // The tombstone follows the delete event, under the same key.
        let delete = events
            .iter()
            .position(|(_, v)| v.as_ref().is_some_and(|v| v["op"] == "d"))
            .ok_or("no delete event")?;
        let (key, value) = events.get(delete + 1).ok_or("no record after delete")?;
        assert!(value.is_none(), "{topic}: {value:?}");
        assert_eq!(key, &events[delete].0, "{topic}");
    }
    poll("the gold tier row", Duration::from_secs(60), || {
        Ok(created(
            &change_events(&worker, &bootstrap, my_topic, false)?,
            "tier",
            "gold",
        ) == 1)
    })?;

    // Debezium created its schema history topic: one partition, nothing
    // ever deleted, and the ALTER is in it.
    let topics = docker(&[
        "exec",
        &worker.0 .0,
        "/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        &bootstrap,
        "--describe",
        "--topic",
        "my-schema-history",
    ])?;
    assert!(topics.contains("PartitionCount: 1"), "{topics}");
    let configs = docker(&[
        "exec",
        &worker.0 .0,
        "/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        &bootstrap,
        "--entity-type",
        "topics",
        "--entity-name",
        "my-schema-history",
        "--describe",
        "--all",
    ])?;
    for expected in [
        "cleanup.policy=delete",
        "retention.ms=9223372036854775807",
        "retention.bytes=-1",
    ] {
        assert!(configs.contains(expected), "{expected}:\n{configs}");
    }
    // History records are pretty-printed JSON over several lines.
    let history = consume(&worker, &bootstrap, "my-schema-history", false)?;
    assert!(
        history.contains("ADD COLUMN tier"),
        "no ALTER in the schema history:\n{history}"
    );

    // Change both databases while no worker runs. The next worker resumes
    // from the committed offsets, recovers MySQL's schema (with `tier`) from
    // the history topic, and does not snapshot again.
    std::thread::sleep(Duration::from_secs(3));
    stop_worker(worker)?;
    psql(
        &postgres,
        "INSERT INTO inventory.customers (first_name, last_name, email) \
         VALUES ('Grace', 'Hopper', 'grace@example.com');",
    )?;
    mysql_exec(
        &mysql,
        "INSERT INTO customers (first_name, last_name, email, tier) \
         VALUES ('Grace', 'Hopper', 'grace@example.com', 'silver');",
    )?;
    let (worker, _base) = setup.worker("connect-again", false)?;
    poll(
        "the changes made while stopped",
        Duration::from_secs(120),
        || {
            Ok(created(
                &change_events(&worker, &bootstrap, pg_topic, false)?,
                "email",
                "grace@example.com",
            ) >= 1
                && created(
                    &change_events(&worker, &bootstrap, my_topic, false)?,
                    "tier",
                    "silver",
                ) >= 1)
        },
    )?;
    std::thread::sleep(Duration::from_secs(3));
    for topic in [pg_topic, my_topic] {
        let events = change_events(&worker, &bootstrap, topic, false)?;
        assert_eq!(ops(&events, "r"), 4, "{topic} was snapshotted again");
        assert_eq!(created(&events, "email", "grace@example.com"), 1, "{topic}");
        assert_eq!(created(&events, "email", "ada@example.com"), 1, "{topic}");
    }
    Ok(())
}

/// Debezium Postgres with exactly-once source tasks (KIP-618): the worker
/// writes records and source offsets in one transaction, and a
/// `read_committed` reader sees each change once across a restart. Opt-in:
/// `CAMUS_ECOSYSTEM_DOCKER=1`.
#[test]
fn debezium_postgres_exactly_once() -> Res<()> {
    if !docker_ecosystem() {
        return Ok(());
    }
    let setup = DebeziumSetup::start()?;
    let bootstrap = setup.bootstrap();
    let postgres = setup.postgres()?;
    let (worker, base) = setup.worker("connect-eos", true)?;
    let mut config = postgres_connector();
    config["exactly.once.support"] = "required".into();
    put_connector(&base, "pg", config)?;

    let topic = "pg.inventory.customers";
    poll("the snapshot", Duration::from_secs(120), || {
        Ok(ops(&change_events(&worker, &bootstrap, topic, true)?, "r") >= 4)
    })?;
    psql(
        &postgres,
        "INSERT INTO inventory.customers (first_name, last_name, email) \
         VALUES ('Ada', 'Lovelace', 'ada@example.com');",
    )?;
    poll("the streamed insert", Duration::from_secs(60), || {
        Ok(created(
            &change_events(&worker, &bootstrap, topic, true)?,
            "email",
            "ada@example.com",
        ) >= 1)
    })?;

    std::thread::sleep(Duration::from_secs(3));
    stop_worker(worker)?;
    psql(
        &postgres,
        "INSERT INTO inventory.customers (first_name, last_name, email) \
         VALUES ('Grace', 'Hopper', 'grace@example.com');",
    )?;
    let (worker, _base) = setup.worker("connect-eos-again", true)?;
    poll(
        "the insert made while stopped",
        Duration::from_secs(120),
        || {
            Ok(created(
                &change_events(&worker, &bootstrap, topic, true)?,
                "email",
                "grace@example.com",
            ) >= 1)
        },
    )?;
    std::thread::sleep(Duration::from_secs(3));
    let events = change_events(&worker, &bootstrap, topic, true)?;
    assert_eq!(ops(&events, "r"), 4, "snapshotted again");
    assert_eq!(created(&events, "email", "ada@example.com"), 1);
    assert_eq!(created(&events, "email", "grace@example.com"), 1);
    Ok(())
}
