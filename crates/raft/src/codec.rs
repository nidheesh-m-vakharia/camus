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

/// Decode a value written by [`encode`].
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}

/// Content type for encoded RPC bodies.
pub const CONTENT_TYPE: &str = "application/x-camus-postcard";
