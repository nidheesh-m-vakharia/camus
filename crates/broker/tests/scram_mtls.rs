//! Identities beyond the bootstrap user: SASL/SCRAM credentials created
//! through AlterUserScramCredentials, and client certificates on an mTLS
//! listener. Each identity is a separate principal under deny-by-default ACLs.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use bytes::Bytes;
use camus_protocol::frame;
use camus_protocol::messages::alter_user_scram_credentials_request::ScramCredentialUpsertion;
use camus_protocol::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResponse;
use camus_protocol::messages::create_topics_request::CreatableTopic;
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::describe_user_scram_credentials_request::UserName;
use camus_protocol::messages::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;
use camus_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use camus_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use camus_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use camus_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use camus_protocol::messages::{
    AlterUserScramCredentialsRequest, ApiKey, CreateTopicsRequest,
    DescribeUserScramCredentialsRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use ring::{digest, hmac};
use rustls::client::ClientConnection;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, StreamOwned};

type Tls = StreamOwned<ClientConnection, TcpStream>;

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

/// A CA, a server certificate for 127.0.0.1, and client certificates.
struct Pki {
    ca_pem: String,
    ca_der: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

impl Pki {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "camus test ca");
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        Self {
            ca_pem: ca.pem(),
            ca_der: ca.der().clone(),
            issuer: Issuer::new(params, key),
        }
    }

    fn server(&self, dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
                std::net::Ipv4Addr::LOCALHOST,
            )));
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        let cert_path = dir.join("server.pem");
        let key_path = dir.join("server.key");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    /// A client certificate for `<name>` in organization `example`.
    fn client(&self, name: &str) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::OrganizationName, "example");
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (
            cert.der().clone(),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
    }

    fn roots(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_der.clone()).unwrap();
        roots
    }
}

fn spawn(dir: &Path, pki: &Pki, client_ca: bool, env: &[(&str, &str)]) -> Node {
    let (cert, key) = pki.server(dir);
    let ca = dir.join("ca.pem");
    std::fs::write(&ca, &pki.ca_pem).unwrap();
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
            "--tls-cert",
            cert.to_str().unwrap(),
            "--tls-key",
            key.to_str().unwrap(),
        ])
        .env(
            "CAMUS_S3_ENDPOINT",
            std::env::var("CAMUS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9100".into()),
        )
        .env("CAMUS_S3_BUCKET", "camus")
        .env("CAMUS_S3_ACCESS_KEY", "camus")
        .env("CAMUS_S3_SECRET_KEY", "camus-dev-secret")
        .env("CAMUS_SASL_USER", "admin")
        .env("CAMUS_SASL_PASSWORD", "admin-secret")
        .envs(env.iter().copied())
        .stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(std::fs::File::create(dir.join("broker.log")).unwrap());
    if client_ca {
        command.args(["--tls-client-ca", ca.to_str().unwrap()]);
    }
    let child = command.spawn().unwrap();
    let http = wait_file(&dir.join("listen-addr"));
    let kafka = wait_file(&dir.join("kafka-addr"));
    let node = Node { child, http, kafka };
    let init = reqwest::blocking::Client::new()
        .post(format!("http://{}/admin/init", node.http))
        .json(&serde_json::json!([{"id": 1, "addr": node.http}]))
        .send()
        .unwrap();
    assert!(init.status().is_success());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let metrics: serde_json::Value =
            reqwest::blocking::get(format!("http://{}/admin/metrics", node.http))
                .unwrap()
                .json()
                .unwrap_or_default();
        if metrics["current_leader"].as_u64() == Some(1)
            && metrics["wal_leader"].as_u64() == Some(1)
        {
            break;
        }
        assert!(Instant::now() < deadline, "no leader");
        std::thread::sleep(Duration::from_millis(20));
    }
    node
}

fn connect(
    addr: &str,
    roots: RootCertStore,
    identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
) -> Tls {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match identity {
        Some((cert, key)) => builder.with_client_auth_cert(vec![cert], key).unwrap(),
        None => builder.with_no_client_auth(),
    };
    let connection =
        ClientConnection::new(Arc::new(config), ServerName::try_from("localhost").unwrap())
            .unwrap();
    let tcp = TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    StreamOwned::new(connection, tcp)
}

fn send<M: Encodable>(stream: &mut Tls, key: ApiKey, version: i16, message: &M) {
    let header = RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("camus-scram")));
    let frame = frame::encode_request(&header, message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

fn recv<M: Decodable + HeaderVersion>(stream: &mut Tls, version: i16) -> M {
    let mut size = [0u8; 4];
    stream.read_exact(&mut size).unwrap();
    let mut body = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let _ = camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
        .unwrap();
    M::decode(&mut buf, version).unwrap()
}

fn authenticate(
    stream: &mut Tls,
    mechanism: &'static str,
    token: Vec<u8>,
) -> SaslAuthenticateResponse {
    send(
        stream,
        ApiKey::SaslAuthenticate,
        2,
        &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(token)),
    );
    let _ = mechanism;
    recv::<SaslAuthenticateResponse>(stream, 2)
}

fn handshake(stream: &mut Tls, mechanism: &'static str) {
    send(
        stream,
        ApiKey::SaslHandshake,
        1,
        &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str(mechanism)),
    );
    assert_eq!(recv::<SaslHandshakeResponse>(stream, 1).error_code, 0);
}

fn create_topic(stream: &mut Tls, name: &'static str) -> i16 {
    let mut topic = CreatableTopic::default();
    topic.name = TopicName(StrBytes::from_static_str(name));
    topic.num_partitions = 1;
    topic.replication_factor = -1;
    send(
        stream,
        ApiKey::CreateTopics,
        7,
        &CreateTopicsRequest::default().with_topics(vec![topic]),
    );
    recv::<CreateTopicsResponse>(stream, 7).topics[0].error_code
}

fn salted(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    ring::pbkdf2::derive(
        ring::pbkdf2::PBKDF2_HMAC_SHA256,
        std::num::NonZeroU32::new(iterations).unwrap(),
        salt,
        password,
        &mut out,
    );
    out
}

/// A SCRAM-SHA-256 login, as a client does it. The final response.
fn scram_login(stream: &mut Tls, user: &str, password: &[u8]) -> SaslAuthenticateResponse {
    handshake(stream, "SCRAM-SHA-256");
    let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
    let bare = format!("n={user},r={client_nonce}");
    let first = authenticate(stream, "SCRAM-SHA-256", format!("n,,{bare}").into_bytes());
    if first.error_code != 0 {
        return first;
    }
    let server_first = String::from_utf8(first.auth_bytes.to_vec()).unwrap();
    let field = |name: &str| {
        server_first
            .split(',')
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
            .unwrap()
            .to_string()
    };
    let nonce = field("r");
    let salt = STANDARD.decode(field("s")).unwrap();
    let iterations: u32 = field("i").parse().unwrap();
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{bare},{server_first},{without_proof}");
    let salted_password = salted(password, &salt, iterations);
    let key = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
    let client_key = hmac::sign(&key, b"Client Key");
    let stored = digest::digest(&digest::SHA256, client_key.as_ref());
    let signature = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, stored.as_ref()),
        auth_message.as_bytes(),
    );
    let proof: Vec<u8> = client_key
        .as_ref()
        .iter()
        .zip(signature.as_ref())
        .map(|(left, right)| left ^ right)
        .collect();
    let final_message = format!("{without_proof},p={}", STANDARD.encode(proof));
    authenticate(stream, "SCRAM-SHA-256", final_message.into_bytes())
}

#[test]
fn scram_users_are_created_described_and_logged_in() {
    let dir = tempfile::tempdir().unwrap();
    let pki = Pki::new();
    let node = spawn(dir.path(), &pki, false, &[]);

    let mut admin = connect(&node.kafka, pki.roots(), None);
    handshake(&mut admin, "PLAIN");
    assert_eq!(
        authenticate(&mut admin, "PLAIN", b"\0admin\0admin-secret".to_vec()).error_code,
        0
    );
    let salt = b"pepper-and-salt".to_vec();
    let upsert = |name: &'static str, iterations: i32| {
        ScramCredentialUpsertion::default()
            .with_name(StrBytes::from_static_str(name))
            .with_mechanism(1)
            .with_iterations(iterations)
            .with_salt(Bytes::from(salt.clone()))
            .with_salted_password(Bytes::from(salted(b"alice-secret", &salt, 4096)))
    };
    send(
        &mut admin,
        ApiKey::AlterUserScramCredentials,
        0,
        &AlterUserScramCredentialsRequest::default()
            .with_upsertions(vec![upsert("alice", 4096), upsert("weak", 100)]),
    );
    let altered = recv::<AlterUserScramCredentialsResponse>(&mut admin, 0);
    let codes: Vec<i16> = altered.results.iter().map(|row| row.error_code).collect();
    assert_eq!(codes, vec![0, 93], "{altered:?}");

    send(
        &mut admin,
        ApiKey::DescribeUserScramCredentials,
        0,
        &DescribeUserScramCredentialsRequest::default().with_users(Some(vec![
            UserName::default().with_name(StrBytes::from_static_str("alice")),
            UserName::default().with_name(StrBytes::from_static_str("bob")),
        ])),
    );
    let described = recv::<DescribeUserScramCredentialsResponse>(&mut admin, 0);
    assert_eq!(described.results[0].error_code, 0);
    assert_eq!(described.results[0].credential_infos[0].mechanism, 1);
    assert_eq!(described.results[0].credential_infos[0].iterations, 4096);
    assert_eq!(described.results[1].error_code, 91);

    let mut alice = connect(&node.kafka, pki.roots(), None);
    let done = scram_login(&mut alice, "alice", b"alice-secret");
    assert_eq!(done.error_code, 0, "{done:?}");
    assert!(done.auth_bytes.starts_with(b"v="));
    // Alice is her own principal, not a super user: denied by default.
    assert_eq!(create_topic(&mut alice, "alice-topic"), 29);
    assert_eq!(create_topic(&mut admin, "admin-topic"), 0);

    let mut intruder = connect(&node.kafka, pki.roots(), None);
    assert_eq!(scram_login(&mut intruder, "alice", b"wrong").error_code, 58);
}

#[test]
fn a_client_certificate_is_the_principal_on_an_mtls_listener() {
    let dir = tempfile::tempdir().unwrap();
    let pki = Pki::new();
    let node = spawn(
        dir.path(),
        &pki,
        true,
        // rcgen encodes CN before O; Java, and so Kafka, prints the
        // encoded RDNs last to first.
        &[("CAMUS_SUPER_USERS", "User:O=example,CN=admin")],
    );
    let mut admin = connect(&node.kafka, pki.roots(), Some(pki.client("admin")));
    assert_eq!(
        create_topic(&mut admin, "by-admin"),
        0,
        "no SASL on an mTLS listener"
    );
    let mut alice = connect(&node.kafka, pki.roots(), Some(pki.client("alice")));
    assert_eq!(create_topic(&mut alice, "by-alice"), 29);

    let mut anonymous = connect(&node.kafka, pki.roots(), None);
    let mut topic = CreatableTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("by-nobody"));
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::CreateTopics as i16)
        .with_request_api_version(7)
        .with_correlation_id(1);
    let frame = frame::encode_request(
        &header,
        &CreateTopicsRequest::default().with_topics(vec![topic]),
        7,
    )
    .unwrap();
    let wrote = anonymous.write_all(&frame);
    let mut size = [0u8; 4];
    let read = anonymous.read_exact(&mut size);
    assert!(
        wrote.is_err() || read.is_err(),
        "a client without a certificate must be refused"
    );
}
