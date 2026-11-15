#![forbid(unsafe_code)]

//! Binary encoding for Raft log entries and Raft RPC bodies.
//!
//! postcard, not JSON: a produce batch's bytes are written as one
//! length-prefixed run instead of a JSON array of numbers, which cost about
//! four bytes per payload byte. Small files such as the vote stay JSON.

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Encode `value`.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_stdvec(value)
}

/// Encode `value` into `writer`.
///
/// Same bytes as [`encode`]. A produce uses this so the batch is written into
/// the durable buffer instead of a temporary `Vec`.
pub fn encode_into<T: Serialize + ?Sized, W: std::io::Write>(
    value: &T,
    writer: W,
) -> Result<(), postcard::Error> {
    postcard::to_io(value, writer).map(|_| ())
}

/// Byte length [`encode`] would produce.
pub fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, postcard::Error> {
    postcard::experimental::serialized_size(value)
}

/// Decode a value written by [`encode`].
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}

/// Content type for encoded RPC bodies.
pub const CONTENT_TYPE: &str = "application/x-camus-postcard";
