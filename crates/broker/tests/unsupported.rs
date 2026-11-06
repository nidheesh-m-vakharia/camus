//! An unimplemented API returns an error and leaves the connection open.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use bytes::Bytes;
use camus_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, InitProducerIdRequest, InitProducerIdResponse,
    RequestHeader,
};
use camus_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use camus_protocol::{frame, ResponseError};

struct Node {
    child: Child,
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

fn header(key: ApiKey, version: i16, correlation: i32) -> RequestHeader {
    RequestHeader::default()
        .with_request_api_key(key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str("camus-unsupported")))
}

fn send<M: Encodable>(
    stream: &mut TcpStream,
    key: ApiKey,
    version: i16,
    correlation: i32,
    message: &M,
) {
    let frame =
        frame::encode_request(&header(key, version, correlation), message, version).unwrap();
    stream.write_all(&frame).unwrap();
}

fn recv<M: Decodable + HeaderVersion>(stream: &mut TcpStream, version: i16) -> (i32, M) {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; size];
    stream.read_exact(&mut body).unwrap();
    let mut buf = Bytes::from(body);
    let response_header =
        camus_protocol::messages::ResponseHeader::decode(&mut buf, M::header_version(version))
            .unwrap();
    (
        response_header.correlation_id,
        M::decode(&mut buf, version).unwrap(),
    )
}

#[test]
fn an_unimplemented_api_returns_unsupported_version_and_the_connection_stays_up() {
    let dir = tempfile::tempdir().unwrap();
    let log = std::fs::File::create(dir.path().join("broker.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_camus-broker"))
        .args([
            "--node-id",
            "1",
            "--http-addr",
            "127.0.0.1:0",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--kafka-addr",
            "127.0.0.1:0",
            "--dev-plaintext",
        ])
        .stdout(std::fs::File::create(dir.path().join("out.log")).unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let kafka = wait_file(&dir.path().join("kafka-addr"));
    let node = Node { child, kafka };
    let mut stream = TcpStream::connect(&node.kafka).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // InitProducerId is Phase 4: known to the schema, not served yet.
    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        4,
        &InitProducerIdRequest::default(),
    );
    let (correlation, init) = recv::<InitProducerIdResponse>(&mut stream, 4);
    assert_eq!(correlation, 4);
    assert_eq!(init.error_code, ResponseError::UnsupportedVersion.code());

    send(
        &mut stream,
        ApiKey::ApiVersions,
        3,
        5,
        &ApiVersionsRequest::default(),
    );
    let (correlation, versions) = recv::<ApiVersionsResponse>(&mut stream, 3);
    assert_eq!(correlation, 5);
    assert!(versions
        .api_keys
        .iter()
        .any(|api| api.api_key == ApiKey::Produce as i16));
    assert!(versions
        .api_keys
        .iter()
        .all(|api| api.api_key != ApiKey::InitProducerId as i16));
}
