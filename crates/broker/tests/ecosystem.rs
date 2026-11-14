//! The Kafka ecosystem against Camus: the Apache Kafka 4.4.0 CLI tools, Kafka
//! Connect, Kafka Streams with `exactly_once_v2`, MirrorMaker 2 from a real
//! Apache Kafka broker, KafkaJS, confluent-kafka-go, confluent-kafka-dotnet,
//! and Schema Registry.
//!
//! Opt-in with `CAMUS_ECOSYSTEM=1`; the first run downloads the Kafka
//! distribution into `target/kafka-dist`. A JDK is required (`JAVA_HOME`, or
//! Homebrew's `openjdk@21`), `node`/`npm` for KafkaJS, Go and librdkafka for
//! confluent-kafka-go, and the .NET SDK for confluent-kafka-dotnet. Schema
//! Registry runs in Docker and needs the broker on a non-loopback address, so
//! it is a further opt-in: `CAMUS_ECOSYSTEM_DOCKER=1`.

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

type Fallible = Result<(), Box<dyn std::error::Error>>;

/// The values a `read_committed` reader sees in a topic [`write_txn_topic`]
/// wrote, in order.
const COMMITTED: [&str; 5] = ["c1", "c2", "c3", "c4", "o1"];

/// Every value [`write_txn_topic`] writes, committed or aborted.
const WRITTEN: [&str; 9] = ["c1", "c2", "c3", "a1", "a2", "c4", "a3", "o1", "a4"];

/// Create `topic` with one partition and write commit, abort, commit, abort,
/// commit, abort to it with `TxnMarkers.java`, which also checks the Java
/// consumer against it (ADR 0021).
fn write_txn_topic(dir: &Path, bootstrap: &str, topic: &str) -> Fallible {
    tool(
        "kafka-topics.sh",
        &[
            "--bootstrap-server",
            bootstrap,
            "--create",
            "--topic",
            topic,
            "--partitions",
            "1",
        ],
        None,
        false,
    );
    let classes = dir.join("txn-classes");
    std::fs::create_dir_all(&classes)?;
    let classpath = format!("{}/*", dist().join("libs").display());
    let compiled = Command::new(format!("{}/bin/javac", java_home()))
        .args(["-proc:none", "-cp", &classpath, "-d"])
        .arg(&classes)
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ecosystem/TxnMarkers.java"))
        .status()?;
    if !compiled.success() {
        return Err("javac TxnMarkers failed".into());
    }
    let ran = Command::new(format!("{}/bin/java", java_home()))
        .arg("-cp")
        .arg(format!("{}:{classpath}", classes.display()))
        .args(["TxnMarkers", bootstrap, topic, &format!("{topic}-java")])
        .output()?;
    let text = String::from_utf8_lossy(&ran.stdout);
    if !ran.status.success() || !text.contains("txn markers ok") {
        return Err(format!("{text}{}", String::from_utf8_lossy(&ran.stderr)).into());
    }
    Ok(())
}

/// Camus stores no transaction markers. A `read_committed` fetch puts an
/// abort marker in place of each aborted batch, so kafka-clients,
/// librdkafka, and KafkaJS skip aborted ranges and reach the end offset as
/// they do on Kafka.
#[test]
fn clients_read_a_transactional_topic_without_stored_markers() -> Fallible {
    if !enabled() {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    let (_node, bootstrap) = broker(dir.path());
    write_txn_topic(dir.path(), &bootstrap, "txn-markers")?;
    let committed = COMMITTED.join(",");

    let printed = tool(
        "kafka-console-consumer.sh",
        &[
            "--bootstrap-server",
            &bootstrap,
            "--topic",
            "txn-markers",
            "--from-beginning",
            "--isolation-level",
            "read_committed",
            "--formatter-property",
            "print.offset=true",
            "--max-messages",
            "5",
            "--timeout-ms",
            "20000",
        ],
        None,
        false,
    );
    let records: Vec<&str> = printed
        .lines()
        .filter(|line| line.starts_with("Offset:"))
        .collect();
    // Kafka would put each record one offset later per marker before it.
    assert_eq!(
        records,
        [
            "Offset:0\tc1",
            "Offset:1\tc2",
            "Offset:2\tc3",
            "Offset:5\tc4",
            "Offset:7\to1"
        ],
        "{printed}"
    );

    match program("go", &["/opt/homebrew/bin/go"]) {
        Some(go) => {
            let mut command = Command::new(go);
            command
                .args(["run", "-tags", "dynamic", "./txnmarkers"])
                .args([bootstrap.as_str(), "txn-markers", committed.as_str()])
                .current_dir(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/ecosystem/confluent_kafka_go"),
                )
                .env("CGO_ENABLED", "1");
            let pkg = Command::new("pkg-config")
                .args(["--variable=pcfiledir", "rdkafka"])
                .output()?;
            let pc_dir = String::from_utf8_lossy(&pkg.stdout).trim().to_string();
            if pkg.status.success() && !pc_dir.is_empty() {
                let joined = match std::env::var("PKG_CONFIG_PATH") {
                    Ok(existing) if !existing.is_empty() => format!("{pc_dir}:{existing}"),
                    _ => pc_dir,
                };
                command.env("PKG_CONFIG_PATH", joined);
            }
            let ran = command.output()?;
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&ran.stdout),
                String::from_utf8_lossy(&ran.stderr)
            );
            assert!(
                ran.status.success() && text.contains("librdkafka txn markers ok"),
                "{text}"
            );
        }
        None => support::skipped("librdkafka transaction reader; go is not installed"),
    }

    if Command::new("npm").arg("--version").output().is_err() {
        support::skipped("KafkaJS transaction reader; npm is not installed");
        return Ok(());
    }
    let modules = target().join("kafkajs");
    if !modules.join("node_modules/kafkajs").is_dir() {
        std::fs::create_dir_all(&modules)?;
        let installed = Command::new("npm")
            .args(["install", "--silent", "--prefix"])
            .arg(&modules)
            .arg("kafkajs@2")
            .status()?;
        if !installed.success() {
            return Err("npm install kafkajs failed".into());
        }
    }
    let ran = Command::new("node")
        .arg(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/ecosystem/kafkajs_txn_markers.js"),
        )
        .args([bootstrap.as_str(), "txn-markers", committed.as_str()])
        .env("NODE_PATH", modules.join("node_modules"))
        .output()?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(
        ran.status.success() && text.contains("kafkajs txn markers ok"),
        "{text}"
    );
    Ok(())
}

/// MirrorMaker 2 reading a transactional topic from Camus with a
/// `read_committed` source consumer copies the committed records only.
#[test]
fn mirror_maker_copies_only_committed_records_from_camus() -> Fallible {
    if !enabled() {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    let source_dir = dir.path().join("source");
    let target_dir = dir.path().join("target");
    std::fs::create_dir_all(&source_dir)?;
    std::fs::create_dir_all(&target_dir)?;
    let (_source, source) = broker(&source_dir);
    let (_target, downstream) = broker(&target_dir);
    write_txn_topic(&source_dir, &source, "payments")?;
    let config = dir.path().join("mm2-txn.properties");
    std::fs::write(
        &config,
        format!(
            "clusters=upstream, downstream\n\
             upstream.bootstrap.servers={source}\n\
             downstream.bootstrap.servers={downstream}\n\
             upstream->downstream.enabled=true\n\
             upstream->downstream.topics=payments\n\
             upstream.consumer.isolation.level=read_committed\n\
             replication.factor=1\n\
             checkpoints.topic.replication.factor=1\n\
             heartbeats.topic.replication.factor=1\n\
             offset-syncs.topic.replication.factor=1\n\
             offset.storage.replication.factor=1\n\
             status.storage.replication.factor=1\n\
             config.storage.replication.factor=1\n\
             sync.topic.acls.enabled=false\n"
        ),
    )?;
    let config = config.to_str().ok_or("config path")?;
    let _mirror = Background::start("connect-mirror-maker.sh", &[config], dir.path(), "mirror");
    let mut seen = Vec::new();
    wait_for("mirrored transaction", Duration::from_secs(120), || {
        let out = tool(
            "kafka-console-consumer.sh",
            &[
                "--bootstrap-server",
                &downstream,
                "--topic",
                "upstream.payments",
                "--from-beginning",
                "--timeout-ms",
                "5000",
            ],
            None,
            true,
        );
        seen = out
            .lines()
            .filter(|line| WRITTEN.contains(line))
            .map(str::to_string)
            .collect();
        seen.len() >= COMMITTED.len()
    });
    seen.sort();
    let mut want: Vec<String> = COMMITTED.iter().map(|value| value.to_string()).collect();
    want.sort();
    assert_eq!(seen, want, "MirrorMaker copied an aborted record");
    Ok(())
}
