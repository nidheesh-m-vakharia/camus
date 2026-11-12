//! QUIC endpoint for Raft control traffic (ADR 0016, phase 1).
//!
//! Off unless `CAMUS_QUIC=on`. This module binds the endpoint, keeps one
//! connection per peer, and frames a vote or an empty heartbeat. The broker
//! does not open it yet, and Raft RPCs still go over HTTP.
//!
//! The peer cache holds at most [`PEER_CAP`] connections. A frame body is at
//! most [`MAX_BODY`] bytes.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Connection, Endpoint, Incoming, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;

/// Metadata Raft group.
pub const ROUTE_METADATA: u8 = 0;
/// A WAL shard. The shard number is the frame's `shard` field.
pub const ROUTE_WAL: u8 = 1;
/// A Raft vote.
pub const KIND_VOTE: u8 = 1;
/// An AppendEntries with no entries.
pub const KIND_HEARTBEAT: u8 = 2;

/// Cached connections. One per peer is enough; 64 covers a cluster with
/// room for members that have not been evicted yet.
const PEER_CAP: usize = 64;
/// Votes and empty heartbeats are small. A longer body is refused.
pub const MAX_BODY: usize = 256 * 1024;
const HEADER: usize = 10;
const SERVER_NAME: &str = "camus";

/// One control RPC on a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// [`ROUTE_METADATA`] or [`ROUTE_WAL`].
    pub route: u8,
    /// WAL shard, or 0 for the metadata group.
    pub shard: u32,
    /// [`KIND_VOTE`] or [`KIND_HEARTBEAT`].
    pub kind: u8,
    /// Postcard body, the same bytes the HTTP route takes.
    pub body: Vec<u8>,
}

/// Certificate and the CAs this process trusts for peers.
pub struct Identity {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    trusted: Vec<CertificateDer<'static>>,
}

impl Clone for Identity {
    fn clone(&self) -> Self {
        Self {
            certs: self.certs.clone(),
            key: self.key.clone_key(),
            trusted: self.trusted.clone(),
        }
    }
}

impl Identity {
    /// `trusted` holds the cluster CA certificates. An empty set is refused:
    /// a vote from an unauthenticated peer could unseat a leader.
    pub fn new(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        trusted: Vec<CertificateDer<'static>>,
    ) -> Result<Self, QuicError> {
        if certs.is_empty() || trusted.is_empty() {
            return Err(quic_error("identity needs a certificate and a trusted CA"));
        }
        Ok(Self {
            certs,
            key,
            trusted,
        })
    }
}

/// A QUIC endpoint that dials peers and accepts their control RPCs.
#[derive(Clone)]
pub struct ControlEndpoint {
    inner: Arc<Inner>,
}

struct Inner {
    endpoint: Endpoint,
    peers: tokio::sync::Mutex<BTreeMap<SocketAddr, Connection>>,
    /// Handler tasks. `close` aborts them so the UDP socket can be bound again.
    tasks: tokio::sync::Mutex<tokio::task::JoinSet<()>>,
}

/// `true` only when `CAMUS_QUIC` is `on`, `1`, or `true`. Unset stays off.
pub fn enabled() -> bool {
    matches!(
        std::env::var("CAMUS_QUIC").as_deref(),
        Ok("on") | Ok("1") | Ok("true")
    )
}

/// `CAMUS_QUIC_PORT` when it is set to a port. Unset means the HTTP port.
pub fn port_override() -> Result<Option<u16>, QuicError> {
    match std::env::var("CAMUS_QUIC_PORT") {
        Err(_) => Ok(None),
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|_| quic_error("CAMUS_QUIC_PORT is not a port")),
    }
}

impl ControlEndpoint {
    /// Bind a client and server on `addr`. `addr`'s port may be 0.
    pub fn bind(addr: SocketAddr, identity: Identity) -> Result<Self, QuicError> {
        let server = server_config(&identity)?;
        let mut endpoint = Endpoint::server(server, addr)?;
        endpoint.set_default_client_config(client_config(&identity)?);
        Ok(Self {
            inner: Arc::new(Inner {
                endpoint,
                peers: tokio::sync::Mutex::new(BTreeMap::new()),
                tasks: tokio::sync::Mutex::new(tokio::task::JoinSet::new()),
            }),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, QuicError> {
        self.inner.endpoint.local_addr().map_err(QuicError::from)
    }

    /// Send `frame` and read the reply. A dead cached connection is dropped
    /// and dialed once more, which is how a restarted peer is reached.
    pub async fn call(&self, peer: SocketAddr, frame: &Frame) -> Result<Vec<u8>, QuicError> {
        match self.call_once(peer, frame).await {
            Ok(body) => Ok(body),
            Err(first) => {
                self.drop_peer(peer).await;
                self.call_once(peer, frame).await.map_err(|_| first)
            }
        }
    }

    /// Accept RPCs until [`close`](Self::close). `handler` builds the reply.
    pub async fn serve<F>(&self, handler: F)
    where
        F: Fn(Frame) -> Result<Vec<u8>, QuicError> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        while let Some(incoming) = self.inner.endpoint.accept().await {
            let handler = Arc::clone(&handler);
            self.inner.tasks.lock().await.spawn(async move {
                let _ = answer(incoming, handler).await;
            });
        }
    }

    /// Close the endpoint and wait until its connections are gone.
    pub async fn close(&self) {
        self.inner
            .endpoint
            .close(quinn::VarInt::from_u32(0), b"closed");
        let mut tasks = self.inner.tasks.lock().await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        drop(tasks);
        self.inner.endpoint.wait_idle().await;
    }

    async fn call_once(&self, peer: SocketAddr, frame: &Frame) -> Result<Vec<u8>, QuicError> {
        let conn = self.connection(peer).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&encode(frame)?).await?;
        send.finish()?;
        let response = recv.read_to_end(MAX_BODY).await?;
        Ok(response)
    }

    async fn connection(&self, peer: SocketAddr) -> Result<Connection, QuicError> {
        {
            let peers = self.inner.peers.lock().await;
            if let Some(conn) = peers.get(&peer) {
                if conn.close_reason().is_none() {
                    return Ok(conn.clone());
                }
            }
        }
        let connecting = self.inner.endpoint.connect(peer, SERVER_NAME)?;
        let conn = connecting.await?;
        let mut peers = self.inner.peers.lock().await;
        if peers.len() >= PEER_CAP && !peers.contains_key(&peer) {
            let stale = peers
                .iter()
                .find(|(_, cached)| cached.close_reason().is_some())
                .map(|(addr, _)| *addr);
            let evict = stale.or_else(|| peers.keys().next().copied());
            if let Some(addr) = evict {
                if let Some(cached) = peers.remove(&addr) {
                    cached.close(quinn::VarInt::from_u32(0), b"cache");
                }
            }
        }
        peers.insert(peer, conn.clone());
        Ok(conn)
    }

    async fn drop_peer(&self, peer: SocketAddr) {
        if let Some(conn) = self.inner.peers.lock().await.remove(&peer) {
            conn.close(quinn::VarInt::from_u32(0), b"retry");
        }
    }
}

/// `route`, `shard`, `kind`, `len`, then `body`. Lengths are big-endian.
pub fn encode(frame: &Frame) -> Result<Vec<u8>, QuicError> {
    if frame.body.len() > MAX_BODY {
        return Err(quic_error("frame body exceeds 256 KiB"));
    }
    let len = u32::try_from(frame.body.len()).map_err(|_| quic_error("frame body length"))?;
    let mut out = Vec::with_capacity(HEADER + frame.body.len());
    out.push(frame.route);
    out.extend_from_slice(&frame.shard.to_be_bytes());
    out.push(frame.kind);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&frame.body);
    Ok(out)
}

/// Inverse of [`encode`]. A short buffer or a length that does not match is
/// an error.
pub fn decode(bytes: &[u8]) -> Result<Frame, QuicError> {
    if bytes.len() < HEADER {
        return Err(quic_error("short frame"));
    }
    let route = bytes[0];
    let shard = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let kind = bytes[5];
    let len = u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]) as usize;
    if len > MAX_BODY {
        return Err(quic_error("frame body exceeds 256 KiB"));
    }
    let body = bytes
        .get(HEADER..HEADER + len)
        .ok_or_else(|| quic_error("truncated frame"))?;
    if HEADER + len != bytes.len() {
        return Err(quic_error("frame length does not match"));
    }
    Ok(Frame {
        route,
        shard,
        kind,
        body: body.to_vec(),
    })
}

async fn answer(
    incoming: Incoming,
    handler: Arc<dyn Fn(Frame) -> Result<Vec<u8>, QuicError> + Send + Sync>,
) -> Result<(), QuicError> {
    let conn = incoming.await?;
    loop {
        let Ok((send, recv)) = conn.accept_bi().await else {
            break;
        };
        let handler = Arc::clone(&handler);
        answer_stream(send, recv, handler).await?;
    }
    Ok(())
}

async fn answer_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    handler: Arc<dyn Fn(Frame) -> Result<Vec<u8>, QuicError> + Send + Sync>,
) -> Result<(), QuicError> {
    let bytes = recv.read_to_end(HEADER + MAX_BODY).await?;
    let frame = decode(&bytes)?;
    let response = handler(frame)?;
    if response.len() > MAX_BODY {
        return Err(quic_error("reply exceeds 256 KiB"));
    }
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

fn server_config(identity: &Identity) -> Result<ServerConfig, QuicError> {
    let provider = crypto_provider();
    let mut roots = rustls::RootCertStore::empty();
    for cert in &identity.trusted {
        roots.add(cert.clone())?;
    }
    let verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build()?;
    let crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.certs.clone(), identity.key.clone_key())?;
    let mut server = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
    server.transport_config(transport()?);
    Ok(server)
}

fn client_config(identity: &Identity) -> Result<ClientConfig, QuicError> {
    let provider = crypto_provider();
    let mut roots = rustls::RootCertStore::empty();
    for cert in &identity.trusted {
        roots.add(cert.clone())?;
    }
    let crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_client_auth_cert(identity.certs.clone(), identity.key.clone_key())?;
    let mut client = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    client.transport_config(transport()?);
    Ok(client)
}

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn transport() -> Result<Arc<quinn::TransportConfig>, QuicError> {
    let mut transport = quinn::TransportConfig::default();
    // Enables unreliable datagrams for later announcement phases.
    transport.datagram_receive_buffer_size(Some(64 * 1024));
    transport.datagram_send_buffer_size(64 * 1024);
    let idle = Duration::from_secs(30)
        .try_into()
        .map_err(|_| quic_error("idle timeout"))?;
    transport.max_idle_timeout(Some(idle));
    Ok(Arc::new(transport))
}

/// Failure to bind, frame, or complete a control RPC.
#[derive(Debug)]
pub struct QuicError {
    message: String,
}

impl std::fmt::Display for QuicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for QuicError {}

impl From<std::io::Error> for QuicError {
    fn from(err: std::io::Error) -> Self {
        quic_error(err)
    }
}

impl From<quinn::ConnectError> for QuicError {
    fn from(err: quinn::ConnectError) -> Self {
        quic_error(err)
    }
}

impl From<quinn::ConnectionError> for QuicError {
    fn from(err: quinn::ConnectionError) -> Self {
        quic_error(err)
    }
}

impl From<quinn::WriteError> for QuicError {
    fn from(err: quinn::WriteError) -> Self {
        quic_error(err)
    }
}

impl From<quinn::ReadToEndError> for QuicError {
    fn from(err: quinn::ReadToEndError) -> Self {
        quic_error(err)
    }
}

fn quic_error(err: impl std::fmt::Display) -> QuicError {
    QuicError {
        message: err.to_string(),
    }
}

macro_rules! from_display {
    ($ty:ty) => {
        impl From<$ty> for QuicError {
            fn from(err: $ty) -> Self {
                quic_error(err)
            }
        }
    };
}

from_display!(rustls::Error);
from_display!(rustls::server::VerifierBuilderError);
from_display!(quinn::crypto::rustls::NoInitialCipherSuite);
from_display!(quinn::ClosedStream);

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    use super::{decode, encode, ControlEndpoint, Frame, Identity, KIND_VOTE, ROUTE_METADATA};

    struct Pair {
        server: Identity,
        client: Identity,
    }

    fn shared_pair() -> Result<Pair, String> {
        let ca_key = KeyPair::generate().map_err(|err| err.to_string())?;
        let mut ca_params =
            CertificateParams::new(vec!["camus-ca".to_string()]).map_err(|err| err.to_string())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = ca_params
            .self_signed(&ca_key)
            .map_err(|err| err.to_string())?;
        let ca_der = CertificateDer::from(ca.der().to_vec());
        let server = leaf(&ca_params, &ca_key, &ca_der)?;
        let client = leaf(&ca_params, &ca_key, &ca_der)?;
        Ok(Pair { server, client })
    }

    fn leaf(
        ca: &CertificateParams,
        ca_key: &KeyPair,
        ca_der: &CertificateDer<'static>,
    ) -> Result<Identity, String> {
        let key = KeyPair::generate().map_err(|err| err.to_string())?;
        let mut params =
            CertificateParams::new(vec!["camus".to_string()]).map_err(|err| err.to_string())?;
        let dns = "camus".try_into().map_err(|_| "server name".to_string())?;
        params.subject_alt_names = vec![SanType::DnsName(dns)];
        let issuer = rcgen::Issuer::from_params(ca, ca_key);
        let cert = params
            .signed_by(&key, &issuer)
            .map_err(|err| err.to_string())?;
        Identity::new(
            vec![CertificateDer::from(cert.der().to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            vec![ca_der.clone()],
        )
        .map_err(|err| err.to_string())
    }

    fn loopback() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    fn vote(body: &[u8]) -> Frame {
        Frame {
            route: ROUTE_METADATA,
            shard: 0,
            kind: KIND_VOTE,
            body: body.to_vec(),
        }
    }

    async fn rebind(addr: SocketAddr, identity: Identity) -> Result<ControlEndpoint, String> {
        let mut last = String::new();
        for _ in 0..40 {
            match ControlEndpoint::bind(addr, identity.clone()) {
                Ok(endpoint) => return Ok(endpoint),
                Err(err) => {
                    last = err.to_string();
                    if !last.contains("in use") {
                        return Err(last);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
            }
        }
        Err(last)
    }

    async fn echo(server: &ControlEndpoint) -> tokio::task::JoinHandle<()> {
        let serving = server.clone();
        tokio::spawn(async move {
            serving.serve(|frame| Ok(frame.body)).await;
        })
    }

    #[test]
    fn a_frame_round_trips() -> Result<(), String> {
        let frame = vote(b"postcard");
        let decoded = decode(&encode(&frame).map_err(|err| err.to_string())?)
            .map_err(|err| err.to_string())?;
        assert_eq!(decoded, frame);
        Ok(())
    }

    #[test]
    fn a_short_frame_is_refused() {
        assert!(decode(&[0, 0, 0]).is_err());
    }

    #[tokio::test]
    async fn request_response_over_loopback() -> Result<(), String> {
        let shared = shared_pair()?;
        let server =
            ControlEndpoint::bind(loopback(), shared.server).map_err(|err| err.to_string())?;
        let client =
            ControlEndpoint::bind(loopback(), shared.client).map_err(|err| err.to_string())?;
        let addr = server.local_addr().map_err(|err| err.to_string())?;
        let task = echo(&server).await;
        let reply = client
            .call(addr, &vote(b"vote"))
            .await
            .map_err(|err| err.to_string())?;
        assert_eq!(reply, b"vote");
        client.close().await;
        server.close().await;
        let _ = task.await;
        Ok(())
    }

    #[tokio::test]
    async fn a_restarted_peer_is_dialed_again() -> Result<(), String> {
        let shared = shared_pair()?;
        let server = ControlEndpoint::bind(loopback(), shared.server.clone())
            .map_err(|err| err.to_string())?;
        let client =
            ControlEndpoint::bind(loopback(), shared.client).map_err(|err| err.to_string())?;
        let addr = server.local_addr().map_err(|err| err.to_string())?;
        let task = echo(&server).await;
        let first = client
            .call(addr, &vote(b"one"))
            .await
            .map_err(|err| err.to_string())?;
        assert_eq!(first, b"one");
        server.close().await;
        let _ = task.await;
        drop(server);
        // Quinn drops the UDP socket on its driver task, after the last
        // handle is gone, so the port can stay busy for a few turns.
        let server = rebind(addr, shared.server).await?;
        let task = echo(&server).await;
        let second = client
            .call(addr, &vote(b"two"))
            .await
            .map_err(|err| err.to_string())?;
        assert_eq!(second, b"two");
        client.close().await;
        server.close().await;
        let _ = task.await;
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_certificate_is_refused() -> Result<(), String> {
        let shared = shared_pair()?;
        let stranger = shared_pair()?;
        let server =
            ControlEndpoint::bind(loopback(), shared.server).map_err(|err| err.to_string())?;
        let client =
            ControlEndpoint::bind(loopback(), stranger.client).map_err(|err| err.to_string())?;
        let addr = server.local_addr().map_err(|err| err.to_string())?;
        let task = echo(&server).await;
        let refused = client.call(addr, &vote(b"no")).await;
        assert!(refused.is_err());
        client.close().await;
        server.close().await;
        let _ = task.await;
        Ok(())
    }
}
