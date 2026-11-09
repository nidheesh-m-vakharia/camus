//! SASL/SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802, RFC 7677), as Kafka's
//! `ScramSaslServer` does it, and the two credential admin APIs.
//!
//! Credentials live in metadata at `scram/<mechanism>/<hex user>` as the salt,
//! iteration count, `StoredKey`, and `ServerKey`. The password and the salted
//! password are never stored: `AlterUserScramCredentials` receives the salted
//! password from the client and keeps only the keys derived from it.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use bytes::Bytes;
use camus_protocol::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;
use camus_protocol::messages::describe_user_scram_credentials_response::{
    CredentialInfo, DescribeUserScramCredentialsResult,
};
use camus_protocol::messages::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, RequestHeader,
};
use camus_protocol::protocol::{Decodable, StrBytes};
use ring::rand::SecureRandom;
use ring::{digest, hmac};
use serde::{Deserialize, Serialize};

use super::{auth, frame_of, Caller, Kafka, Reply};

const RESOURCE_NOT_FOUND: i16 = 91;
const DUPLICATE_RESOURCE: i16 = 92;
const UNACCEPTABLE_CREDENTIAL: i16 = 93;
const UNSUPPORTED_SASL_MECHANISM: i16 = 33;

/// Kafka's bounds on SCRAM iterations.
const MIN_ITERATIONS: i32 = 4096;
const MAX_ITERATIONS: i32 = 16384;

/// The SCRAM mechanisms Kafka names, with their admin-API codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mechanism {
    Sha256,
    Sha512,
}

impl Mechanism {
    pub(super) fn from_name(name: &str) -> Option<Self> {
        match name {
            "SCRAM-SHA-256" => Some(Self::Sha256),
            "SCRAM-SHA-512" => Some(Self::Sha512),
            _ => None,
        }
    }

    fn from_code(code: i8) -> Option<Self> {
        match code {
            1 => Some(Self::Sha256),
            2 => Some(Self::Sha512),
            _ => None,
        }
    }

    fn code(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    fn hmac(self) -> hmac::Algorithm {
        match self {
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }

    fn digest(self) -> &'static digest::Algorithm {
        match self {
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }
}

/// A stored SCRAM credential.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Credential {
    salt: String,
    stored_key: String,
    server_key: String,
    iterations: i32,
}

impl Credential {
    /// Derive the stored keys from a salted password (RFC 5802 section 3).
    fn from_salted(mechanism: Mechanism, salt: &[u8], salted: &[u8], iterations: i32) -> Self {
        let key = hmac::Key::new(mechanism.hmac(), salted);
        let client_key = hmac::sign(&key, b"Client Key");
        let stored_key = digest::digest(mechanism.digest(), client_key.as_ref());
        let server_key = hmac::sign(&key, b"Server Key");
        Self {
            salt: STANDARD.encode(salt),
            stored_key: STANDARD.encode(stored_key.as_ref()),
            server_key: STANDARD.encode(server_key.as_ref()),
            iterations,
        }
    }
}

/// Metadata key of one user's credential for one mechanism.
pub(super) fn credential_key(mechanism: Mechanism, user: &str) -> String {
    let hex: String = user.bytes().map(|byte| format!("{byte:02x}")).collect();
    format!("scram/{}/{hex}", mechanism.name())
}

fn user_from_key(key: &str) -> Option<(Mechanism, String)> {
    let rest = key.strip_prefix("scram/")?;
    let (name, hex) = rest.split_once('/')?;
    let mechanism = Mechanism::from_name(name)?;
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    Some((mechanism, String::from_utf8(bytes).ok()?))
}

/// A SCRAM exchange between the client-first and client-final messages.
pub(super) struct Exchange {
    mechanism: Mechanism,
    user: String,
    credential: Credential,
    client_first_bare: String,
    server_first: String,
    nonce: String,
}

/// `saslname` unescaping: `=2C` is `,` and `=3D` is `=`.
fn unescape(name: &str) -> Option<String> {
    let mut out = String::new();
    let mut rest = name;
    while let Some(at) = rest.find('=') {
        out.push_str(&rest[..at]);
        match rest.get(at..at + 3)? {
            "=2C" => out.push(','),
            "=3D" => out.push('='),
            _ => return None,
        }
        rest = &rest[at + 3..];
    }
    out.push_str(rest);
    Some(out)
}

fn attribute<'a>(message: &'a str, name: char) -> Option<&'a str> {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(name)?.strip_prefix('='))
}

impl Exchange {
    /// Answer the client-first message, or `None` to fail authentication.
    pub(super) fn start(
        mechanism: Mechanism,
        client_first: &[u8],
        lookup: impl Fn(&str) -> Option<Credential>,
    ) -> Option<(Self, Vec<u8>)> {
        let message = std::str::from_utf8(client_first).ok()?;
        // gs2-header: channel binding flag, optional authzid, then the bare
        // message. Kafka does not support channel binding.
        let mut parts = message.splitn(3, ',');
        let binding = parts.next()?;
        let _authzid = parts.next()?;
        let bare = parts.next()?;
        if binding != "n" && binding != "y" {
            return None;
        }
        let user = unescape(attribute(bare, 'n')?)?;
        let client_nonce = attribute(bare, 'r')?;
        let credential = lookup(&user)?;
        let mut random = [0u8; 18];
        ring::rand::SystemRandom::new().fill(&mut random).ok()?;
        let nonce = format!("{client_nonce}{}", STANDARD.encode(random));
        let server_first = format!(
            "r={nonce},s={},i={}",
            credential.salt, credential.iterations
        );
        let reply = server_first.clone().into_bytes();
        Some((
            Self {
                mechanism,
                user,
                credential,
                client_first_bare: bare.to_string(),
                server_first,
                nonce,
            },
            reply,
        ))
    }

    /// Check the client-final message. On success, the user and the
    /// server-final message.
    pub(super) fn finish(self, client_final: &[u8]) -> Option<(String, Vec<u8>)> {
        let message = std::str::from_utf8(client_final).ok()?;
        let (without_proof, proof) = message.rsplit_once(",p=")?;
        if attribute(without_proof, 'r')? != self.nonce {
            return None;
        }
        let proof = STANDARD.decode(proof).ok()?;
        let stored_key = STANDARD.decode(&self.credential.stored_key).ok()?;
        let server_key = STANDARD.decode(&self.credential.server_key).ok()?;
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let signature = hmac::sign(
            &hmac::Key::new(self.mechanism.hmac(), &stored_key),
            auth_message.as_bytes(),
        );
        if proof.len() != signature.as_ref().len() {
            return None;
        }
        let client_key: Vec<u8> = proof
            .iter()
            .zip(signature.as_ref())
            .map(|(left, right)| left ^ right)
            .collect();
        let derived = digest::digest(self.mechanism.digest(), &client_key);
        if !super::constant_eq(derived.as_ref(), &stored_key) {
            return None;
        }
        let server_signature = hmac::sign(
            &hmac::Key::new(self.mechanism.hmac(), &server_key),
            auth_message.as_bytes(),
        );
        let reply = format!("v={}", STANDARD.encode(server_signature.as_ref()));
        Some((self.user, reply.into_bytes()))
    }
}

impl Kafka {
    /// The stored credential of `user` for `mechanism`.
    pub(super) fn scram_credential(&self, mechanism: Mechanism, user: &str) -> Option<Credential> {
        let raw = self
            .node
            .read_local(&credential_key(mechanism, user))
            .ok()??;
        serde_json::from_str(&raw).ok()
    }

    pub(super) async fn describe_user_scram_credentials(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) =
            DescribeUserScramCredentialsRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        let mut response = DescribeUserScramCredentialsResponse::default();
        if !self.allows(caller, auth::CLUSTER, auth::CLUSTER_NAME, auth::DESCRIBE) {
            response.error_code = auth::CLUSTER_FAILED;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let mut stored: std::collections::BTreeMap<String, Vec<CredentialInfo>> =
            std::collections::BTreeMap::new();
        for (key, raw) in self.node.scan_local("scram/").unwrap_or_default() {
            let Some((mechanism, user)) = user_from_key(&key) else {
                continue;
            };
            let Ok(credential) = serde_json::from_str::<Credential>(&raw) else {
                continue;
            };
            let mut info = CredentialInfo::default();
            info.mechanism = mechanism.code();
            info.iterations = credential.iterations;
            stored.entry(user).or_default().push(info);
        }
        let wanted: Vec<String> = match request.users {
            Some(users) if !users.is_empty() => {
                users.iter().map(|user| user.name.to_string()).collect()
            }
            _ => stored.keys().cloned().collect(),
        };
        for user in wanted {
            let mut result = DescribeUserScramCredentialsResult::default();
            result.user = StrBytes::from_string(user.clone());
            match stored.get(&user) {
                Some(infos) => result.credential_infos = infos.clone(),
                None => {
                    result.error_code = RESOURCE_NOT_FOUND;
                    result.error_message = Some(StrBytes::from_string(format!(
                        "Attempt to describe a user credential that does not exist: {user}"
                    )));
                }
            }
            response.results.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn alter_user_scram_credentials(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = AlterUserScramCredentialsRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        let allowed = self.allows(caller, auth::CLUSTER, auth::CLUSTER_NAME, auth::ALTER);
        // Kafka rejects a user named twice for the same mechanism.
        let mut seen: std::collections::BTreeMap<(String, i8), usize> =
            std::collections::BTreeMap::new();
        for (name, mechanism) in request
            .deletions
            .iter()
            .map(|row| (row.name.to_string(), row.mechanism))
            .chain(
                request
                    .upsertions
                    .iter()
                    .map(|row| (row.name.to_string(), row.mechanism)),
            )
        {
            *seen.entry((name, mechanism)).or_default() += 1;
        }
        let mut response = AlterUserScramCredentialsResponse::default();
        let mut answer = |user: &str, code: i16, message: Option<String>| {
            let mut result = AlterUserScramCredentialsResult::default();
            result.user = StrBytes::from_string(user.to_string());
            result.error_code = code;
            result.error_message = message.map(StrBytes::from_string);
            response.results.push(result);
        };
        for row in &request.deletions {
            let user = row.name.to_string();
            let code = if !allowed {
                auth::CLUSTER_FAILED
            } else if seen
                .get(&(user.clone(), row.mechanism))
                .copied()
                .unwrap_or(0)
                > 1
            {
                DUPLICATE_RESOURCE
            } else if let Some(mechanism) = Mechanism::from_code(row.mechanism) {
                if self.scram_credential(mechanism, &user).is_none() {
                    RESOURCE_NOT_FOUND
                } else {
                    match self
                        .meta_apply(camus_metadata::Command::Delete {
                            key: credential_key(mechanism, &user),
                        })
                        .await
                    {
                        Ok(_) => 0,
                        Err(_) => super::NOT_LEADER,
                    }
                }
            } else {
                UNSUPPORTED_SASL_MECHANISM
            };
            answer(&user, code, None);
        }
        for row in &request.upsertions {
            let user = row.name.to_string();
            let mechanism = Mechanism::from_code(row.mechanism);
            let (code, message) = if !allowed {
                (auth::CLUSTER_FAILED, None)
            } else if seen
                .get(&(user.clone(), row.mechanism))
                .copied()
                .unwrap_or(0)
                > 1
            {
                (DUPLICATE_RESOURCE, None)
            } else if mechanism.is_none() {
                (UNSUPPORTED_SASL_MECHANISM, None)
            } else if user.is_empty() {
                (
                    UNACCEPTABLE_CREDENTIAL,
                    Some("Username must not be empty".into()),
                )
            } else if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&row.iterations) {
                (
                    UNACCEPTABLE_CREDENTIAL,
                    Some(format!(
                        "Iterations must be between {MIN_ITERATIONS} and {MAX_ITERATIONS}"
                    )),
                )
            } else if row.salt.is_empty() || row.salted_password.is_empty() {
                (
                    UNACCEPTABLE_CREDENTIAL,
                    Some("Salt and salted password must not be empty".into()),
                )
            } else {
                let mechanism = mechanism.expect("checked");
                let credential = Credential::from_salted(
                    mechanism,
                    &row.salt,
                    &row.salted_password,
                    row.iterations,
                );
                let raw = serde_json::to_string(&credential).unwrap_or_default();
                match self.meta_write(credential_key(mechanism, &user), raw).await {
                    Ok(()) => (0, None),
                    Err(_) => (super::NOT_LEADER, None),
                }
            };
            answer(&user, code, message);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn salted(mechanism: Mechanism, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let algorithm = match mechanism {
            Mechanism::Sha256 => ring::pbkdf2::PBKDF2_HMAC_SHA256,
            Mechanism::Sha512 => ring::pbkdf2::PBKDF2_HMAC_SHA512,
        };
        let mut out = vec![0u8; mechanism.digest().output_len()];
        ring::pbkdf2::derive(
            algorithm,
            std::num::NonZeroU32::new(iterations).unwrap(),
            salt,
            password,
            &mut out,
        );
        out
    }

    /// RFC 7677's SCRAM-SHA-256 example exchange, end to end.
    #[test]
    fn the_rfc_7677_exchange_authenticates() {
        let salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let credential = Credential::from_salted(
            Mechanism::Sha256,
            &salt,
            &salted(Mechanism::Sha256, b"pencil", &salt, 4096),
            4096,
        );
        let client_first = b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let (exchange, server_first) = Exchange::start(Mechanism::Sha256, client_first, |_| {
            Some(credential.clone())
        })
        .unwrap();
        let server_first = String::from_utf8(server_first).unwrap();
        assert!(server_first.starts_with("r=rOprNGfwEbeRWgbNEkqO"));
        assert!(server_first.ends_with(",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"));

        // Compute the client proof as a client would.
        let nonce = attribute(&server_first, 'r').unwrap();
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("n=user,r=rOprNGfwEbeRWgbNEkqO,{server_first},{without_proof}");
        let salted_password = salted(Mechanism::Sha256, b"pencil", &salt, 4096);
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
        let client_final = format!("{without_proof},p={}", STANDARD.encode(proof));
        let (user, server_final) = exchange.finish(client_final.as_bytes()).unwrap();
        assert_eq!(user, "user");
        assert!(String::from_utf8(server_final).unwrap().starts_with("v="));
    }

    #[test]
    fn a_wrong_proof_or_nonce_fails() {
        let salt = b"salt".to_vec();
        let credential = Credential::from_salted(
            Mechanism::Sha512,
            &salt,
            &salted(Mechanism::Sha512, b"secret", &salt, 4096),
            4096,
        );
        let (exchange, server_first) =
            Exchange::start(Mechanism::Sha512, b"n,,n=bob,r=abc", |_| {
                Some(credential.clone())
            })
            .unwrap();
        let server_first = String::from_utf8(server_first).unwrap();
        let nonce = attribute(&server_first, 'r').unwrap().to_string();
        let bad = format!("c=biws,r={nonce},p={}", STANDARD.encode([0u8; 64]));
        assert!(exchange.finish(bad.as_bytes()).is_none());
        let (exchange, _) = Exchange::start(Mechanism::Sha512, b"n,,n=bob,r=abc", |_| {
            Some(credential.clone())
        })
        .unwrap();
        let wrong_nonce = format!("c=biws,r=other,p={}", STANDARD.encode([0u8; 64]));
        assert!(exchange.finish(wrong_nonce.as_bytes()).is_none());
        assert!(Exchange::start(Mechanism::Sha512, b"n,,n=nobody,r=abc", |_| None).is_none());
    }

    #[test]
    fn user_names_round_trip_through_keys_and_escapes() {
        let key = credential_key(Mechanism::Sha256, "a/b,c");
        assert_eq!(
            user_from_key(&key),
            Some((Mechanism::Sha256, "a/b,c".to_string()))
        );
        assert_eq!(unescape("a=2Cb=3Dc").as_deref(), Some("a,b=c"));
        assert_eq!(unescape("bad=2"), None);
    }
}
