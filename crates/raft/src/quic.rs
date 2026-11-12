//! QUIC endpoint for Raft control traffic (ADR 0016).
//!
//! Off unless `CAMUS_QUIC=on`. Votes and empty heartbeats use one stream per
//! RPC. Shard announcements are datagrams. Data AppendEntries and snapshots
//! stay on HTTP. The peer cache holds at most [`PEER_CAP`] connections. A
//! frame body is at most [`MAX_BODY`] bytes. A datagram is at most
//! [`MAX_DATAGRAM`] bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Connection, Endpoint, Incoming, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use serde::{Deserialize, Serialize};

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
/// Announcements stay under a typical path MTU.
pub const MAX_DATAGRAM: usize = 1024;
/// Hint table and datagram-reader set. One entry per shard or connection.
const TRACK_CAP: usize = 256;
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

/// Shard leadership, sent as a datagram. A lower `term` is ignored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Announcement {
    pub shard: u32,
    pub term: u64,
    pub leader: u64,
    pub epoch: i32,
}

/// Certificate and the CAs this process trusts for peers.
pub struct Identity {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    trusted: Vec<CertificateDer<'static>>,
    /// `--dev-plaintext` on loopback. Peers are not pinned to a CA.
    loopback: bool,
}

impl Clone for Identity {
    fn clone(&self) -> Self {
        Self {
            certs: self.certs.clone(),
            key: self.key.clone_key(),
            trusted: self.trusted.clone(),
            loopback: self.loopback,
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
            loopback: false,
        })
    }

    /// A fresh self-signed certificate for `--dev-plaintext` on loopback.
    /// Refused when the bind address is not loopback.
    pub fn loopback_dev() -> Result<Self, QuicError> {
        let key = rcgen::KeyPair::generate().map_err(quic_error)?;
        let mut params =
            rcgen::CertificateParams::new(vec!["camus".to_string()]).map_err(quic_error)?;
        let dns = "camus".try_into().map_err(|_| quic_error("server name"))?;
        params.subject_alt_names = vec![rcgen::SanType::DnsName(dns)];
        let cert = params.self_signed(&key).map_err(quic_error)?;
        Ok(Self {
            certs: vec![CertificateDer::from(cert.der().to_vec())],
            key: PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
                key.serialize_der(),
            )),
            trusted: Vec::new(),
            loopback: true,
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
    datagrams: tokio::sync::Mutex<Option<DatagramHandler>>,
    /// Connections that already have a datagram reader. Capped at [`TRACK_CAP`].
    readers: tokio::sync::Mutex<BTreeMap<usize, ()>>,
    /// Peers with a handshake already in flight. Capped at [`PEER_CAP`].
    warming: tokio::sync::Mutex<BTreeSet<SocketAddr>>,
}

type DatagramHandler = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

static CURRENT: std::sync::OnceLock<ControlEndpoint> = std::sync::OnceLock::new();

/// `true` only when `CAMUS_QUIC` is `on`, `1`, or `true`. Unset is off.
pub fn enabled() -> bool {
    flag_enabled(std::env::var("CAMUS_QUIC").ok().as_deref())
}

pub fn flag_enabled(value: Option<&str>) -> bool {
    matches!(value, Some("on") | Some("1") | Some("true"))
}

/// Heartbeat and election timeouts, in milliseconds: 50, then 150–300.
/// ADR 0016 phase 5's 15 / 50–100 stayed off. With those values the
/// three-process metadata write in `cluster.rs` did not commit within the
/// client's 30 second timeout on the GitHub runner.
pub fn raft_timeouts() -> (u64, u64, u64) {
    (50, 150, 300)
}

/// The process endpoint, after [`install`](ControlEndpoint::install).
pub fn current() -> Option<ControlEndpoint> {
    CURRENT.get().cloned()
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
    /// A loopback identity is refused off loopback.
    pub fn bind(addr: SocketAddr, identity: Identity) -> Result<Self, QuicError> {
        if identity.loopback && !(addr.ip().is_loopback() || addr.ip().is_unspecified()) {
            return Err(quic_error("dev QUIC certificates are refused off loopback"));
        }
        let server = server_config(&identity)?;
        let mut endpoint = Endpoint::server(server, addr)?;
        endpoint.set_default_client_config(client_config(&identity)?);
        Ok(Self {
            inner: Arc::new(Inner {
                endpoint,
                peers: tokio::sync::Mutex::new(BTreeMap::new()),
                tasks: tokio::sync::Mutex::new(tokio::task::JoinSet::new()),
                datagrams: tokio::sync::Mutex::new(None),
                readers: tokio::sync::Mutex::new(BTreeMap::new()),
                warming: tokio::sync::Mutex::new(BTreeSet::new()),
            }),
        })
    }

    /// Publish this endpoint for Raft RPC in this process.
    pub fn install(self) -> Result<(), QuicError> {
        CURRENT
            .set(self)
            .map_err(|_| quic_error("quic endpoint already installed"))
    }

    /// True when `peer` already has a connection that has not closed.
    /// A heartbeat must not wait for a handshake: openraft cancels the RPC
    /// after the 50 ms heartbeat interval.
    pub async fn live(&self, peer: SocketAddr) -> bool {
        let peers = self.inner.peers.lock().await;
        peers
            .get(&peer)
            .is_some_and(|conn| conn.close_reason().is_none())
    }

    /// Start a handshake without tying it to the RPC that noticed it was missing.
    /// A second call for the same peer does nothing while the first is in flight.
    pub fn warm(&self, peer: SocketAddr) {
        let endpoint = self.clone();
        tokio::spawn(async move {
            {
                let mut warming = endpoint.inner.warming.lock().await;
                if warming.contains(&peer) || warming.len() >= PEER_CAP {
                    return;
                }
                warming.insert(peer);
            }
            let connected = endpoint.connection(peer).await;
            endpoint.inner.warming.lock().await.remove(&peer);
            let _ = connected;
        });
    }

    /// `http_addr` is `host:port`. `CAMUS_QUIC_PORT` replaces the port when set.
    pub fn peer_socket(http_addr: &str) -> Result<SocketAddr, QuicError> {
        let mut addr: SocketAddr = http_addr.parse().map_err(|_| quic_error("peer address"))?;
        if let Some(port) = port_override()? {
            addr.set_port(port);
        }
        Ok(addr)
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
    pub async fn serve<F, Fut>(&self, handler: F)
    where
        F: Fn(Frame) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>, QuicError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let handler: Handler = Arc::new(move |frame| Box::pin(handler(frame)));
        while let Some(incoming) = self.inner.endpoint.accept().await {
            let handler = Arc::clone(&handler);
            let inner = Arc::clone(&self.inner);
            self.inner.tasks.lock().await.spawn(async move {
                let _ = answer(incoming, handler, inner).await;
            });
        }
    }

    /// Keep `handler` for announcement datagrams. At most one is stored.
    pub async fn set_datagram_handler<F>(&self, handler: F)
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        *self.inner.datagrams.lock().await = Some(Arc::new(handler));
    }

    /// Send an announcement. A body longer than [`MAX_DATAGRAM`] is refused.
    pub async fn send_datagram(&self, peer: SocketAddr, bytes: &[u8]) -> Result<(), QuicError> {
        if bytes.len() > MAX_DATAGRAM {
            return Err(quic_error("datagram exceeds 1024 bytes"));
        }
        let conn = self.connection(peer).await?;
        conn.send_datagram(bytes.to_vec().into())?;
        Ok(())
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
        self.arm_datagrams(conn.clone()).await;
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

    async fn arm_datagrams(&self, conn: Connection) {
        watch_datagrams(&self.inner, conn).await;
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

async fn watch_datagrams(inner: &Arc<Inner>, conn: Connection) {
    let id = conn.stable_id();
    {
        let mut readers = inner.readers.lock().await;
        if readers.contains_key(&id) {
            return;
        }
        if readers.len() >= TRACK_CAP {
            if let Some(stale) = readers.keys().next().copied() {
                readers.remove(&stale);
            }
        }
        readers.insert(id, ());
    }
    let inner = Arc::clone(inner);
    tokio::spawn(async move {
        loop {
            let Ok(bytes) = conn.read_datagram().await else {
                break;
            };
            let handler = inner.datagrams.lock().await.clone();
            if let Some(handler) = handler {
                handler(bytes.to_vec());
            }
        }
    });
}

type Handler = Arc<
    dyn Fn(
            Frame,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<u8>, QuicError>> + Send>,
        > + Send
        + Sync,
>;

async fn answer(incoming: Incoming, handler: Handler, inner: Arc<Inner>) -> Result<(), QuicError> {
    let conn = incoming.await?;
    watch_datagrams(&inner, conn.clone()).await;
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
    handler: Handler,
) -> Result<(), QuicError> {
    let bytes = recv.read_to_end(HEADER + MAX_BODY).await?;
    let frame = decode(&bytes)?;
    let response = handler(frame).await?;
    if response.len() > MAX_BODY {
        return Err(quic_error("reply exceeds 256 KiB"));
    }
    send.write_all(&response).await?;
    send.finish()?;
    Ok(())
}

fn server_config(identity: &Identity) -> Result<ServerConfig, QuicError> {
    let provider = crypto_provider();
    let crypto = if identity.loopback {
        rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(Arc::new(LoopbackDev::new(provider)))
            .with_single_cert(identity.certs.clone(), identity.key.clone_key())?
    } else {
        let mut roots = rustls::RootCertStore::empty();
        for cert in &identity.trusted {
            roots.add(cert.clone())?;
        }
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()?;
        rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(verifier)
            .with_single_cert(identity.certs.clone(), identity.key.clone_key())?
    };
    let mut server = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
    server.transport_config(transport()?);
    Ok(server)
}

fn client_config(identity: &Identity) -> Result<ClientConfig, QuicError> {
    let provider = crypto_provider();
    let crypto = if identity.loopback {
        rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(LoopbackDev::new(provider)))
            .with_client_auth_cert(identity.certs.clone(), identity.key.clone_key())?
    } else {
        let mut roots = rustls::RootCertStore::empty();
        for cert in &identity.trusted {
            roots.add(cert.clone())?;
        }
        rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots)
            .with_client_auth_cert(identity.certs.clone(), identity.key.clone_key())?
    };
    let mut client = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    client.transport_config(transport()?);
    Ok(client)
}

/// `--dev-plaintext` on loopback. The handshake signature is checked. The
/// certificate is not pinned to a CA, and this verifier is not used off
/// loopback.
#[derive(Debug)]
struct LoopbackDev {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl LoopbackDev {
    fn new(provider: Arc<rustls::crypto::CryptoProvider>) -> Self {
        Self { provider }
    }

    fn schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl rustls::client::danger::ServerCertVerifier for LoopbackDev {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match server_name {
            rustls::pki_types::ServerName::DnsName(name) if name.as_ref() == SERVER_NAME => {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            _ => Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName,
            )),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.schemes()
    }
}

impl rustls::server::danger::ClientCertVerifier for LoopbackDev {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.schemes()
    }
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
from_display!(quinn::SendDatagramError);

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
            serving.serve(|frame| async move { Ok(frame.body) }).await;
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

    #[test]
    fn quic_stays_off_unless_it_is_turned_on() {
        assert!(!super::flag_enabled(None));
        assert!(super::flag_enabled(Some("on")));
        assert!(super::flag_enabled(Some("1")));
        assert!(super::flag_enabled(Some("true")));
        assert!(!super::flag_enabled(Some("off")));
        assert!(!super::flag_enabled(Some("0")));
        assert!(!super::flag_enabled(Some("false")));
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
    async fn a_cold_peer_becomes_live_after_a_background_dial() -> Result<(), String> {
        let shared = shared_pair()?;
        let server =
            ControlEndpoint::bind(loopback(), shared.server).map_err(|err| err.to_string())?;
        let client =
            ControlEndpoint::bind(loopback(), shared.client).map_err(|err| err.to_string())?;
        let addr = server.local_addr().map_err(|err| err.to_string())?;
        let task = echo(&server).await;
        if client.live(addr).await {
            return Err("a peer with no connection was live".into());
        }
        client.warm(addr);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut connected = false;
        while tokio::time::Instant::now() < deadline {
            if client.live(addr).await {
                connected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        client.close().await;
        server.close().await;
        let _ = task.await;
        if connected {
            Ok(())
        } else {
            Err("background dial did not connect".into())
        }
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
