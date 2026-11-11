//! Length-prefixed Kafka request frames.
//!
//! Message bodies are the types generated from the Apache Kafka 4.4.0 schemas.
//! This module only adds the `int32` size prefix and the rule for which error
//! to return when a key is not implemented.

use std::collections::BTreeMap;
use std::ops::Range;

use bytes::buf::UninitSlice;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::buf::ByteBufMut;

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
        key: ApiKey::ElectLeaders,
        min: 0,
        max: 2,
    },
    ImplementedApi {
        key: ApiKey::DescribeUserScramCredentials,
        min: 0,
        max: 0,
    },
    ImplementedApi {
        key: ApiKey::AlterUserScramCredentials,
        min: 0,
        max: 0,
    },
    ImplementedApi {
        key: ApiKey::ConsumerGroupDescribe,
        min: 0,
        max: 1,
    },
    ImplementedApi {
        key: ApiKey::DescribeTopicPartitions,
        min: 0,
        max: 0,
    },
    ImplementedApi {
        key: ApiKey::DescribeProducers,
        min: 0,
        max: 0,
    },
    ImplementedApi {
        key: ApiKey::DescribeLogDirs,
        min: 1,
        max: 4,
    },
    ImplementedApi {
        key: ApiKey::OffsetForLeaderEpoch,
        min: 3,
        max: 4,
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

/// Encode `size || response header || body` as chunks.
///
/// `shared` are buffers the body already owns. A `put_slice` of one of them
/// becomes its own chunk instead of a copy into the frame. The chunks
/// concatenate to the same bytes as [`encode_response`].
pub fn encode_response_chunks<M: Encodable + HeaderVersion>(
    version: i16,
    correlation_id: i32,
    message: &M,
    shared: &[Bytes],
) -> Result<Vec<Bytes>, anyhow::Error> {
    let mut body = ShareBuf::new(shared);
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    header
        .encode(&mut body, M::header_version(version))
        .map_err(|err| anyhow::anyhow!(err))?;
    message
        .encode(&mut body, version)
        .map_err(|err| anyhow::anyhow!(err))?;
    let mut chunks = body.finish();
    let body_len: usize = chunks.iter().map(Bytes::len).sum();
    let mut size = BytesMut::with_capacity(4);
    size.put_i32(i32::try_from(body_len).map_err(|_| anyhow::anyhow!("response exceeds i32"))?);
    chunks.insert(0, size.freeze());
    Ok(chunks)
}

/// A write buffer that can emit a `Bytes` it was given instead of copying it.
struct ShareBuf {
    tail: BytesMut,
    chunks: Vec<Piece>,
    regions: Vec<Region>,
}

struct Region {
    ptr: usize,
    len: usize,
    bytes: Bytes,
}

enum Piece {
    Own(BytesMut),
    Shared(Bytes),
}

impl Piece {
    fn len(&self) -> usize {
        match self {
            Self::Own(bytes) => bytes.len(),
            Self::Shared(bytes) => bytes.len(),
        }
    }
}

impl ShareBuf {
    fn new(shared: &[Bytes]) -> Self {
        let regions = shared
            .iter()
            .filter(|bytes| !bytes.is_empty())
            .map(|bytes| Region {
                ptr: bytes.as_ptr() as usize,
                len: bytes.len(),
                bytes: bytes.clone(),
            })
            .collect();
        Self {
            tail: BytesMut::new(),
            chunks: Vec::new(),
            regions,
        }
    }

    fn finish(mut self) -> Vec<Bytes> {
        self.flush_tail();
        self.chunks
            .into_iter()
            .filter_map(|piece| match piece {
                Piece::Own(bytes) if !bytes.is_empty() => Some(bytes.freeze()),
                Piece::Shared(bytes) if !bytes.is_empty() => Some(bytes),
                _ => None,
            })
            .collect()
    }

    fn flush_tail(&mut self) {
        if self.tail.is_empty() {
            return;
        }
        let tail = std::mem::take(&mut self.tail);
        self.chunks.push(Piece::Own(tail));
    }

    fn share_of(&self, src: &[u8]) -> Option<Bytes> {
        if src.is_empty() {
            return None;
        }
        let ptr = src.as_ptr() as usize;
        let end = ptr + src.len();
        for region in &self.regions {
            let region_end = region.ptr + region.len;
            if ptr >= region.ptr && end <= region_end {
                let off = ptr - region.ptr;
                return Some(region.bytes.slice(off..off + src.len()));
            }
        }
        None
    }
}

// SAFETY: `chunk_mut` and `advance_mut` delegate to `BytesMut`, which upholds
// `BufMut`. `put_slice` either appends to that buffer or records a shared
// chunk and does not advance the tail.
unsafe impl BufMut for ShareBuf {
    fn remaining_mut(&self) -> usize {
        usize::MAX
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.tail.reserve(64);
        self.tail.chunk_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        unsafe { self.tail.advance_mut(cnt) };
    }

    fn put_slice(&mut self, src: &[u8]) {
        if let Some(shared) = self.share_of(src) {
            self.flush_tail();
            self.chunks.push(Piece::Shared(shared));
            return;
        }
        self.tail.extend_from_slice(src);
    }
}

impl ByteBufMut for ShareBuf {
    fn offset(&self) -> usize {
        self.chunks.iter().map(Piece::len).sum::<usize>() + self.tail.len()
    }

    fn seek(&mut self, offset: usize) {
        let len = self.offset();
        if offset >= len {
            self.tail.resize(self.tail.len() + (offset - len), 0);
            return;
        }
        let mut at = 0;
        for index in 0..self.chunks.len() {
            let chunk_len = self.chunks[index].len();
            if at + chunk_len <= offset {
                at += chunk_len;
                continue;
            }
            let keep = offset - at;
            if keep == 0 {
                self.chunks.truncate(index);
            } else {
                match &mut self.chunks[index] {
                    Piece::Own(bytes) => bytes.truncate(keep),
                    Piece::Shared(bytes) => {
                        let sliced = bytes.slice(..keep);
                        self.chunks[index] = Piece::Shared(sliced);
                    }
                }
                self.chunks.truncate(index + 1);
            }
            self.tail.clear();
            return;
        }
        self.tail.truncate(offset - at);
    }

    fn range(&mut self, r: Range<usize>) -> &mut [u8] {
        let mut at = 0;
        let mut found = None;
        for index in 0..self.chunks.len() {
            let len = self.chunks[index].len();
            if r.start >= at && r.end <= at + len {
                found = Some((index, at));
                break;
            }
            at += len;
        }
        if let Some((index, start_at)) = found {
            if matches!(&self.chunks[index], Piece::Shared(_)) {
                let piece = std::mem::replace(&mut self.chunks[index], Piece::Own(BytesMut::new()));
                if let Piece::Shared(bytes) = piece {
                    self.chunks[index] = Piece::Own(BytesMut::from(bytes.as_ref()));
                }
            }
            let start = r.start - start_at;
            let end = r.end - start_at;
            if let Piece::Own(bytes) = &mut self.chunks[index] {
                return &mut bytes[start..end];
            }
        }
        let start = r.start - at;
        let end = r.end - at;
        &mut self.tail[start..end]
    }
}

#[cfg(test)]
mod share_tests {
    use super::*;
    use crate::messages::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData};

    #[test]
    fn shared_fetch_records_keep_their_allocation() -> anyhow::Result<()> {
        let payload = Bytes::from(vec![7u8; 64]);
        let mut part = PartitionData::default();
        part.records = Some(payload.clone());
        let mut topic = FetchableTopicResponse::default();
        topic.partitions = vec![part];
        let mut response = FetchResponse::default();
        response.responses = vec![topic];
        let shared = response
            .responses
            .iter()
            .flat_map(|topic| topic.partitions.iter())
            .filter_map(|part| part.records.clone())
            .collect::<Vec<_>>();
        let chunks = encode_response_chunks(11, 7, &response, &shared)?;
        let classic = encode_response(11, 7, &response)?;
        let mut joined = Vec::new();
        for chunk in &chunks {
            joined.extend_from_slice(chunk);
        }
        assert_eq!(joined, classic.as_ref());
        assert!(
            chunks.iter().any(|chunk| {
                chunk.as_ptr() == payload.as_ptr() && chunk.len() == payload.len()
            }),
            "record bytes were copied into the frame"
        );
        Ok(())
    }
}

/// Insert one unknown tagged field. Flexible versions must preserve it.
pub fn tagged(key: i32, value: &[u8]) -> BTreeMap<i32, Bytes> {
    let mut fields = BTreeMap::new();
    fields.insert(key, Bytes::copy_from_slice(value));
    fields
}
