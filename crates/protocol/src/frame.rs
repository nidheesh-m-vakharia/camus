//! Length-prefixed Kafka request frames.
//!
//! Message bodies are the types generated from the Apache Kafka 4.4.0 schemas.
//! This module only adds the `int32` size prefix and the rule for which error
//! to return when a key is not implemented. Phase 0 implements no API, so every
//! known key is [`ResponseError::UnsupportedVersion`].

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ResponseError;
use crate::messages::ApiKey;
use crate::protocol::{
    decode_request_header_from_buffer, encode_request_header_into_buffer, Decodable, Encodable,
};
use crate::messages::RequestHeader;

/// APIs this broker will answer with success. Empty until a later phase.
pub fn implemented_api_keys() -> &'static [ApiKey] {
    &[]
}

/// Why a request does not get a successful handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unimplemented {
    /// The key exists in the 4.4.0 schema and this build does not serve it.
    UnsupportedVersion,
    /// The key is not in the generated schema.
    InvalidRequest,
}

impl Unimplemented {
    /// Kafka error code a client should see.
    pub fn error(self) -> ResponseError {
        match self {
            Self::UnsupportedVersion => ResponseError::UnsupportedVersion,
            Self::InvalidRequest => ResponseError::InvalidRequest,
        }
    }

    /// Numeric Kafka error code.
    pub fn code(self) -> i16 {
        self.error().code()
    }
}

/// Classify an API key against the implemented set.
///
/// A key the schema names is [`Unimplemented::UnsupportedVersion`]. A key it
/// does not name is [`Unimplemented::InvalidRequest`].
pub fn reject_unimplemented(api_key: i16) -> Result<(), Unimplemented> {
    let Ok(key) = ApiKey::try_from(api_key) else {
        return Err(Unimplemented::InvalidRequest);
    };
    if implemented_api_keys().contains(&key) {
        Ok(())
    } else {
        Err(Unimplemented::UnsupportedVersion)
    }
}

/// Correlation id from the first eight header bytes.
///
/// `api_key`, `api_version`, and `correlation_id` sit in front of both header
/// versions, so a frame that is long enough yields the id even when the key is
/// unknown and the generated header decoder refuses it.
pub fn correlation_id(body: &[u8]) -> Option<i32> {
    if body.len() < 8 {
        return None;
    }
    Some(i32::from_be_bytes(body[4..8].try_into().ok()?))
}

/// Encode `size || header || body` for `version`.
///
/// `header.request_api_key` and `header.request_api_version` select the header
/// version. `version` must be that same API version.
pub fn encode_request<M: Encodable>(
    header: &RequestHeader,
    message: &M,
    version: i16,
) -> Result<Bytes, anyhow::Error> {
    let mut body = BytesMut::new();
    encode_request_header_into_buffer(&mut body, header)?;
    message.encode(&mut body, version)?;
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(i32::try_from(body.len()).map_err(|_| anyhow::anyhow!("request exceeds i32"))?);
    frame.extend_from_slice(&body);
    Ok(frame.freeze())
}

/// Inverse of [`encode_request`]. The size prefix must cover the rest of `frame`.
pub fn decode_request<M: Decodable>(frame: &[u8]) -> Result<(RequestHeader, M), anyhow::Error> {
    let mut buf = Bytes::copy_from_slice(frame);
    if buf.remaining() < 4 {
        anyhow::bail!("frame shorter than the size prefix");
    }
    let size = buf.get_i32();
    if size < 0 {
        anyhow::bail!("negative frame size {size}");
    }
    let size = size as usize;
    if buf.remaining() != size {
        anyhow::bail!("frame size {size} does not match the {} bytes that follow", buf.remaining());
    }
    let header = decode_request_header_from_buffer(&mut buf)?;
    let message = M::decode(&mut buf, header.request_api_version)?;
    if buf.remaining() != 0 {
        anyhow::bail!("{} trailing bytes after the request body", buf.remaining());
    }
    Ok((header, message))
}

/// Insert one unknown tagged field. Flexible versions must preserve it.
pub fn tagged(key: i32, value: &[u8]) -> BTreeMap<i32, Bytes> {
    let mut fields = BTreeMap::new();
    fields.insert(key, Bytes::copy_from_slice(value));
    fields
}
