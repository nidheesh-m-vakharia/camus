//! Length-prefixed Kafka request frames.
//!
//! Message bodies are the types generated from the Apache Kafka 4.4.0 schemas.
//! This module only adds the `int32` size prefix and the rule for which error
//! to return when a key is not implemented.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ResponseError;
use crate::messages::{ApiKey, RequestHeader, ResponseHeader};
use crate::protocol::{
    decode_request_header_from_buffer, encode_request_header_into_buffer, Decodable, Encodable,
    HeaderVersion,
};

/// An API key and the versions this build will answer with success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImplementedApi {
    /// Kafka API key.
    pub key: ApiKey,
    /// Lowest version the handler encodes.
    pub min: i16,
    /// Highest version the handler encodes.
    pub max: i16,
}

/// APIs the broker answers with success.
///
/// Anything else that the schema knows is [`Unimplemented::UnsupportedVersion`].
pub fn implemented_apis() -> &'static [ImplementedApi] {
    &IMPLEMENTED
}

const IMPLEMENTED: &[ImplementedApi] = &[
    ImplementedApi {
        key: ApiKey::Produce,
        min: 3,
        max: 13,
    },
    ImplementedApi {
        key: ApiKey::Fetch,
        min: 4,
        max: 18,
    },
    ImplementedApi {
        key: ApiKey::ListOffsets,
        min: 1,
        max: 11,
    },
    ImplementedApi {
        key: ApiKey::Metadata,
        min: 0,
        max: 13,
    },
    ImplementedApi {
        key: ApiKey::SaslHandshake,
        min: 0,
        max: 1,
    },
    ImplementedApi {
        key: ApiKey::ApiVersions,
        min: 0,
        max: 5,
    },
    ImplementedApi {
        key: ApiKey::CreateTopics,
        min: 2,
        max: 7,
    },
    ImplementedApi {
        key: ApiKey::DeleteTopics,
        min: 1,
        max: 6,
    },
    ImplementedApi {
        key: ApiKey::DescribeConfigs,
        min: 1,
        max: 4,
    },
    ImplementedApi {
        key: ApiKey::SaslAuthenticate,
        min: 0,
        max: 2,
    },
    ImplementedApi {
        key: ApiKey::FindCoordinator,
        min: 0,
        max: 6,
    },
    ImplementedApi {
        key: ApiKey::JoinGroup,
        min: 0,
        max: 9,
    },
    ImplementedApi {
        key: ApiKey::SyncGroup,
        min: 0,
        max: 5,
    },
    ImplementedApi {
        key: ApiKey::Heartbeat,
        min: 0,
        max: 4,
    },
    ImplementedApi {
        key: ApiKey::LeaveGroup,
        min: 0,
        max: 5,
    },
    ImplementedApi {
        key: ApiKey::OffsetCommit,
        min: 2,
        max: 10,
    },
    ImplementedApi {
        key: ApiKey::OffsetFetch,
        min: 1,
        max: 10,
    },
    ImplementedApi {
        key: ApiKey::ListGroups,
        min: 0,
        max: 5,
    },
    ImplementedApi {
        key: ApiKey::DescribeGroups,
        min: 0,
        max: 6,
    },
    ImplementedApi {
        key: ApiKey::DeleteGroups,
        min: 0,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::ConsumerGroupHeartbeat,
        min: 0,
        max: 1,
    },
    ImplementedApi {
        key: ApiKey::InitProducerId,
        min: 0,
        max: 4,
    },
    ImplementedApi {
        key: ApiKey::CreatePartitions,
        min: 0,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::AlterConfigs,
        min: 0,
        max: 2,
    },
    ImplementedApi {
        key: ApiKey::IncrementalAlterConfigs,
        min: 0,
        max: 1,
    },
    ImplementedApi {
        key: ApiKey::DeleteRecords,
        min: 0,
        max: 2,
    },
    ImplementedApi {
        key: ApiKey::OffsetDelete,
        min: 0,
        max: 0,
    },
    ImplementedApi {
        key: ApiKey::DescribeCluster,
        min: 0,
        max: 2,
    },
    ImplementedApi {
        key: ApiKey::AddPartitionsToTxn,
        min: 0,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::AddOffsetsToTxn,
        min: 0,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::EndTxn,
        min: 0,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::DescribeAcls,
        min: 1,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::CreateAcls,
        min: 1,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::DeleteAcls,
        min: 1,
        max: 3,
    },
    ImplementedApi {
        key: ApiKey::DescribeTransactions,
        min: 0,
        max: 0,
    },
    ImplementedApi {
        key: ApiKey::ListTransactions,
        min: 0,
        max: 2,
    },
    ImplementedApi {
        key: ApiKey::TxnOffsetCommit,
        min: 0,
        max: 3,
    },
];

/// Keys in [`implemented_apis`], for callers that only need the key.
pub fn implemented_api_keys() -> Vec<ApiKey> {
    IMPLEMENTED.iter().map(|api| api.key).collect()
}

/// Whether `version` of `key` has a success handler.
pub fn is_implemented(key: ApiKey, version: i16) -> bool {
    IMPLEMENTED
        .iter()
        .any(|api| api.key == key && version >= api.min && version <= api.max)
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
    if implemented_apis().iter().any(|api| api.key == key) {
        Ok(())
    } else {
        Err(Unimplemented::UnsupportedVersion)
    }
}

/// Encode `size || response header || body`.
pub fn encode_response<M: Encodable + HeaderVersion>(
    version: i16,
    correlation_id: i32,
    message: &M,
) -> Result<Bytes, anyhow::Error> {
    let mut body = BytesMut::new();
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    header.encode(&mut body, M::header_version(version))?;
    message.encode(&mut body, version)?;
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(i32::try_from(body.len()).map_err(|_| anyhow::anyhow!("response exceeds i32"))?);
    frame.extend_from_slice(&body);
    Ok(frame.freeze())
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
        anyhow::bail!(
            "frame size {size} does not match the {} bytes that follow",
            buf.remaining()
        );
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
