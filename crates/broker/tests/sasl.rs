//! SASL_SSL: TLS plus SASL/PLAIN, then a produce when MinIO is up.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use camus_protocol::messages::create_topics_request::CreatableTopic;
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use camus_protocol::messages::fetch_response::FetchResponse;
use camus_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use camus_protocol::messages::produce_response::ProduceResponse;
use camus_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use camus_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use camus_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use camus_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use camus_protocol::messages::{
    ApiKey, CreateTopicsRequest, FetchRequest, ProduceRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use camus_protocol::{frame, ResponseError};
use rustls::client::ClientConnection;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore, StreamOwned};

struct Node {
    child: Child,
    http: String,
    kafka: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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

fn spawn(dir: &Path, cert: &Path, key: &Path) -> Node {
    let log = std::fs::File::create(dir.join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            dir.to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--tls-cert",
            cert.to_str().unwrap(),
            "--tls-key",
            key.to_str().unwrap(),
        ])
        .env("CAMUS_SASL_USER", "camus")
        .env("CAMUS_SASL_PASSWORD", "camus-secret")
        .env("CAMUS_FLUSH_MS", "40")
        .env("RUST_LOG", "info,camus_broker=debug")
        .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    Node { child, http, kafka }
}

fn batch(value: &[u8], compression: Compression) -> Bytes {
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
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::copy_from_slice(value)),
        headers: Default::default(),
    }];
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .unwrap();
    buf.freeze()
}

fn wait_leader(http: &str) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client.get(format!("http://{http}/admin/metrics")).send() {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json.get("current_leader").and_then(|value| value.as_u64()) == Some(1) {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("node 1 did not become leader");
}

fn client(addr: &str, roots: RootCertStore) -> StreamOwned<ClientConnection, TcpStream> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connection =
        ClientConnection::new(Arc::new(config), ServerName::try_from("localhost").unwrap())
            .unwrap();
    let tcp = TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    StreamOwned::new(connection, tcp)
}

fn header(key: ApiKey, version: i16, correlation: i32) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("camus-sasl")))
}

fn send<M: Encodable>(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
    key: ApiKey,
    version: i16,
    correlation: i32,
    message: &M,
) {
    let frame =
        frame::encode_request(&header(key, version, correlation), message, version).unwrap();
    stream.write_all(&frame).unwrap();
    stream.flush().unwrap();
}

fn recv<M: Decodable + HeaderVersion>(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
    version: i16,
) -> M {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

#[test]
fn sasl_ssl_rejects_bad_credentials_and_accepts_plain() {
    let dir = tempfile::tempdir().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let signed = params.self_signed(&key_pair).unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, signed.pem()).unwrap();
    std::fs::write(&key, key_pair.serialize_pem()).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(signed.der().clone()).unwrap();

    let node = spawn(dir.path(), &cert, &key);
    let mut stream = client(&node.kafka, roots.clone());

    send(
        &mut stream,
        ApiKey::Produce,
        9,
        1,
        &ProduceRequest::default().with_acks(1),
    );
    let denied = recv::<ProduceResponse>(&mut stream, 9);
    assert_eq!(
        denied.responses[0].partition_responses[0].error_code,
        ResponseError::IllegalSaslState.code()
    );

    send(
        &mut stream,
        ApiKey::SaslHandshake,
        1,
        2,
        &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("SCRAM-SHA-256")),
    );
    let handshake = recv::<SaslHandshakeResponse>(&mut stream, 1);
    assert_eq!(
        handshake.error_code,
        ResponseError::UnsupportedSaslMechanism.code()
    );
    assert_eq!(handshake.mechanisms[0].as_str(), "PLAIN");

    send(
        &mut stream,
        ApiKey::SaslHandshake,
        1,
        3,
        &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
    );
    let handshake = recv::<SaslHandshakeResponse>(&mut stream, 1);
    assert_eq!(handshake.error_code, 0);

    send(
        &mut stream,
        ApiKey::SaslAuthenticate,
        2,
        4,
        &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from_static(b"\0camus\0wrong")),
    );
    let auth = recv::<SaslAuthenticateResponse>(&mut stream, 2);
    assert_eq!(auth.error_code, 58);

    send(
        &mut stream,
        ApiKey::SaslAuthenticate,
        2,
        5,
        &SaslAuthenticateRequest::default()
            .with_auth_bytes(Bytes::from_static(b"\0camus\0camus-secret")),
    );
    let auth = recv::<SaslAuthenticateResponse>(&mut stream, 2);
    assert_eq!(auth.error_code, 0, "{auth:?}");

    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        return;
    }
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);

    let mut created = CreatableTopic::default();
    created.name = TopicName(StrBytes::from_static_str("secure"));
    created.num_partitions = 1;
    created.replication_factor = 1;
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        6,
        &CreateTopicsRequest::default().with_topics(vec![created]),
    );
    let created = recv::<CreateTopicsResponse>(&mut stream, 7);
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");

    for (compression, value) in [
        (Compression::None, &b"plain"[..]),
        (Compression::Gzip, &b"gzip"[..]),
        (Compression::Snappy, &b"snappy"[..]),
        (Compression::Lz4, &b"lz4"[..]),
        (Compression::Zstd, &b"zstd"[..]),
    ] {
        let raw = batch(value, compression);
        let covered = raw[21..].to_vec();
        let mut topic = TopicProduceData::default();
        topic.name = TopicName(StrBytes::from_static_str("secure"));
        topic.partition_data = vec![PartitionProduceData::default()
            .with_index(0)
            .with_records(Some(raw.clone()))];
        send(
            &mut stream,
            ApiKey::Produce,
            9,
            7,
            &ProduceRequest::default()
                .with_acks(-1)
                .with_timeout_ms(5_000)
                .with_topic_data(vec![topic]),
        );
        let produced = recv::<ProduceResponse>(&mut stream, 9);
        let part = &produced.responses[0].partition_responses[0];
        assert_eq!(part.error_code, 0, "{produced:?}");

        let mut fetch_part = FetchPartition::default();
        fetch_part.partition = 0;
        fetch_part.fetch_offset = part.base_offset;
        fetch_part.partition_max_bytes = 1024 * 1024;
        let mut fetch_topic = FetchTopic::default();
        fetch_topic.topic = TopicName(StrBytes::from_static_str("secure"));
        fetch_topic.partitions = vec![fetch_part];
        send(
            &mut stream,
            ApiKey::Fetch,
            11,
            8,
            &FetchRequest::default()
                .with_max_wait_ms(200)
                .with_topics(vec![fetch_topic]),
        );
        let fetched = recv::<FetchResponse>(&mut stream, 11);
        let records = fetched.responses[0].partitions[0].records.clone().unwrap();
        assert_eq!(&records[21..21 + covered.len()], covered.as_slice());
    }
}

fn classpath(jars: &Path, classes: &Path) -> String {
    let mut parts = vec![classes.display().to_string()];
    for entry in std::fs::read_dir(jars).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jar") {
            parts.push(path.display().to_string());
        }
    }
    parts.join(":")
}

fn kafka_jars() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-jars");
    std::fs::create_dir_all(&dir).unwrap();
    let files = [
        (
            "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0.jar",
            "kafka-clients-4.4.0.jar",
        ),
        (
            "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.7-17/zstd-jni-1.5.7-17.jar",
            "zstd-jni-1.5.7-17.jar",
        ),
        (
            "https://repo1.maven.org/maven2/at/yawk/lz4/lz4-java/1.11.2/lz4-java-1.11.2.jar",
            "lz4-java-1.11.2.jar",
        ),
        (
            "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar",
            "snappy-java-1.1.10.7.jar",
        ),
        (
            "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar",
            "slf4j-api-1.7.36.jar",
        ),
        (
            "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar",
            "slf4j-simple-1.7.36.jar",
        ),
    ];
    for (url, name) in files {
        let path = dir.join(name);
        if path.is_file()
            && std::fs::metadata(&path)
                .map(|meta| meta.len() > 0)
                .unwrap_or(false)
        {
            continue;
        }
        let status = Command::new("curl")
            .args(["-fsSL", "-o"])
            .arg(&path)
            .arg(url)
            .status()
            .unwrap_or_else(|err| panic!("curl {url}: {err}"));
        assert!(status.success(), "download {url}");
    }
    dir
}

fn wait_registered(http: &str) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(body) = client
            .get(format!("http://{http}/admin/read"))
            .query(&[("key", "broker/1")])
            .send()
        {
            if let Ok(json) = body.json::<serde_json::Value>() {
                if json
                    .get("value")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| !value.is_empty())
                {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("broker address was not registered");
}

#[test]
fn java_kafka_clients_round_trip() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping java client; set CAMUS_MINIO=1");
        return;
    }
    let jars = kafka_jars();
    let dir = tempfile::tempdir().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let signed = params.self_signed(&key_pair).unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, signed.pem()).unwrap();
    std::fs::write(&key, key_pair.serialize_pem()).unwrap();

    let node = spawn(dir.path(), &cert, &key);
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);
    wait_registered(&node.http);

    let classes = dir.path().join("classes");
    std::fs::create_dir_all(&classes).unwrap();
    let cp = classpath(&jars, &classes);
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/SaslRoundTrip.java");
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
    let ran = Command::new("java")
        .args(["-cp", &cp, "SaslRoundTrip", &node.kafka])
        .arg(&cert)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&ran.stdout);
    let stderr = String::from_utf8_lossy(&ran.stderr);
    if !ran.status.success() {
        let log = std::fs::read_to_string(dir.path().join("broker.log")).unwrap_or_default();
        panic!("java client failed\n{stdout}\n{stderr}\nbroker:\n{log}");
    }
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        assert!(stdout.contains(&format!("ok {codec}")), "{stdout}");
    }
}

fn assert_client(dir: &Path, ran: std::process::Output, label: &str) {
    let stdout = String::from_utf8_lossy(&ran.stdout);
    let stderr = String::from_utf8_lossy(&ran.stderr);
    if !ran.status.success() {
        let log = std::fs::read_to_string(dir.join("broker.log")).unwrap_or_default();
        let out = std::fs::read_to_string(dir.join("out.log")).unwrap_or_default();
        panic!("{label} failed\n{stdout}\n{stderr}\nbroker:\n{log}\n{out}");
    }
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        assert!(stdout.contains(&format!("ok {codec}")), "{label}\n{stdout}");
    }
}

fn boot() -> (tempfile::TempDir, Node, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let signed = params.self_signed(&key_pair).unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, signed.pem()).unwrap();
    std::fs::write(&key, key_pair.serialize_pem()).unwrap();
    let node = spawn(dir.path(), &cert, &key);
    let client = reqwest::blocking::Client::new();
    let init = client
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success(), "{}", init.text().unwrap());
    wait_leader(&node.http);
    wait_registered(&node.http);
    (dir, node, cert)
}

#[test]
fn librdkafka_round_trip() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping librdkafka; set CAMUS_MINIO=1");
        return;
    }
    let python = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/pykafka/bin/python");
    if !python.is_file() {
        eprintln!("skipping librdkafka; missing {}", python.display());
        return;
    }
    let (dir, node, cert) = boot();
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/python/sasl_roundtrip.py");
    let ran = Command::new(python)
        .arg(script)
        .arg(&node.kafka)
        .arg(&cert)
        .output()
        .unwrap();
    assert_client(dir.path(), ran, "librdkafka");
}

#[test]
fn franz_go_round_trip() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping franz-go; set CAMUS_MINIO=1");
        return;
    }
    let go = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/go/bin/go");
    if !go.is_file() {
        eprintln!("skipping franz-go; missing {}", go.display());
        return;
    }
    let (dir, node, cert) = boot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/go");
    let ran = Command::new(go)
        .args(["run", "."])
        .arg(&node.kafka)
        .arg(&cert)
        .current_dir(&source)
        .env("GOTOOLCHAIN", "local")
        .output()
        .unwrap();
    assert_client(dir.path(), ran, "franz-go");
}
