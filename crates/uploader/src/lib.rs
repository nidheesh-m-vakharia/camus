//! One flush, one object.
//!
//! The body is the sealed Kafka batches concatenated, then a footer the
//! fetch path does not return. The footer is how recovery can list the
//! slices inside the object when metadata is missing. Keys are write-once:
//! [`object_key`] is a new ULID every call.

#![forbid(unsafe_code)]

use bytes::{BufMut, Bytes, BytesMut};
use crc::{Crc, CRC_32_ISCSI};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const MAGIC: &[u8] = b"CAMUSFTR";

/// One partition's batches inside a bundle.
pub struct Piece {
    /// Topic id, 16 bytes.
    pub topic_id: [u8; 16],
    /// Partition index.
    pub partition: i32,
    /// Inclusive base offset.
    pub start: i64,
    /// Exclusive end offset.
    pub end: i64,
    /// First timestamp.
    pub base_timestamp: i64,
    /// Last timestamp.
    pub max_timestamp: i64,
    /// Sealed record batches.
    pub bytes: Bytes,
}

/// A new write-once object key.
pub fn object_key() -> String {
    format!("bundle/{}.seg", ulid::Ulid::generate())
}

/// Concatenate `pieces` and append the footer.
///
/// Returns `(file_offset, length)` for each piece, in order, and the object body.
pub fn encode(pieces: &[Piece]) -> (Vec<(u64, u32)>, Bytes) {
    let mut body = BytesMut::new();
    let mut index = Vec::with_capacity(pieces.len());
    for piece in pieces {
        let file_offset = body.len() as u64;
        let length = u32::try_from(piece.bytes.len()).expect("piece fits in u32");
        body.extend_from_slice(&piece.bytes);
        index.push((file_offset, length));
    }
    let mut footer = BytesMut::new();
    footer.extend_from_slice(MAGIC);
    footer.put_u16_le(1);
    footer.put_u32_le(u32::try_from(pieces.len()).expect("piece count fits in u32"));
    for (piece, (file_offset, length)) in pieces.iter().zip(&index) {
        footer.extend_from_slice(&piece.topic_id);
        footer.put_i32_le(piece.partition);
        footer.put_i64_le(piece.start);
        footer.put_i64_le(piece.end);
        footer.put_u64_le(*file_offset);
        footer.put_u32_le(*length);
        footer.put_i64_le(piece.base_timestamp);
        footer.put_i64_le(piece.max_timestamp);
    }
    let crc = CRC32C.checksum(&footer);
    footer.put_u32_le(crc);
    body.extend_from_slice(&footer);
    (index, body.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_crc_covers_the_index() {
        let piece = Piece {
            topic_id: [9; 16],
            partition: 0,
            start: 0,
            end: 1,
            base_timestamp: 1,
            max_timestamp: 2,
            bytes: Bytes::from_static(b"batch"),
        };
        let (index, body) = encode(&[piece]);
        assert_eq!(index, vec![(0, 5)]);
        assert!(body.starts_with(b"batch"));
        assert!(body.windows(MAGIC.len()).any(|window| window == MAGIC));
    }
}
