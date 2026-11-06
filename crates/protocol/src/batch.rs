//! Kafka record-batch v2 headers.
//!
//! The CRC covers every byte after the CRC field. [`seal`] rewrites
//! `baseOffset` and `partitionLeaderEpoch`, which sit in front of that CRC,
//! and copies the rest unchanged. Compression, headers, and record bodies
//! are not inspected.

use bytes::{Bytes, BytesMut};

use crate::records::crc32c;

const HEADER: usize = 12;
const EPOCH_AT: usize = 12;
const MAGIC_AT: usize = 16;
const CRC_AT: usize = 17;
const CONTENT_AT: usize = 21;
const ATTR_AT: usize = 21;
const BASE_TS_AT: usize = 27;
const MAX_TS_AT: usize = 35;
const COUNT_AT: usize = 57;
const PREFIX: usize = 61;

/// Why a record batch cannot be sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchError {
    /// Truncated, bad magic, or a CRC that does not match the body.
    Corrupt,
    /// One batch is larger than the broker limit.
    TooLarge,
    /// The transactional attribute is set. Transactions are a later phase.
    Transactional,
}

/// One v2 batch inside a produce payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchInfo {
    /// Byte length of this batch, including the offset and length fields.
    pub len: usize,
    /// Records in the batch, including control records.
    pub records: i32,
    /// First timestamp in the batch header.
    pub base_timestamp: i64,
    /// Last timestamp in the batch header.
    pub max_timestamp: i64,
}

/// Walk `bytes` and check magic, length, and CRC. Does not decompress.
pub fn inspect(bytes: &[u8], max_batch: usize) -> Result<Vec<BatchInfo>, BatchError> {
    let mut out = Vec::new();
    let mut offset = 0;
    if bytes.is_empty() {
        return Ok(out);
    }
    while offset < bytes.len() {
        if bytes.len() - offset < PREFIX {
            return Err(BatchError::Corrupt);
        }
        let batch_length = i32::from_be_bytes(bytes[offset + 8..offset + 12].try_into().unwrap());
        if batch_length < (PREFIX - HEADER) as i32 {
            return Err(BatchError::Corrupt);
        }
        let total = HEADER + batch_length as usize;
        if total > max_batch {
            return Err(BatchError::TooLarge);
        }
        if bytes.len() - offset < total {
            return Err(BatchError::Corrupt);
        }
        let batch = &bytes[offset..offset + total];
        if batch[MAGIC_AT] != 2 {
            return Err(BatchError::Corrupt);
        }
        let supplied = u32::from_be_bytes(batch[CRC_AT..CONTENT_AT].try_into().unwrap());
        if supplied != crc32c(&batch[CONTENT_AT..]) {
            return Err(BatchError::Corrupt);
        }
        let attributes = i16::from_be_bytes(batch[ATTR_AT..ATTR_AT + 2].try_into().unwrap());
        if attributes & (1 << 4) != 0 {
            return Err(BatchError::Transactional);
        }
        let records = i32::from_be_bytes(batch[COUNT_AT..COUNT_AT + 4].try_into().unwrap());
        if records <= 0 {
            return Err(BatchError::Corrupt);
        }
        out.push(BatchInfo {
            len: total,
            records,
            base_timestamp: i64::from_be_bytes(
                batch[BASE_TS_AT..BASE_TS_AT + 8].try_into().unwrap(),
            ),
            max_timestamp: i64::from_be_bytes(batch[MAX_TS_AT..MAX_TS_AT + 8].try_into().unwrap()),
        });
        offset += total;
    }
    Ok(out)
}

/// Copy `bytes` and write `base_offset` into the first batch, then continue
/// by each batch's record count. `epoch` is written into every batch.
///
/// The CRC-covered suffix of each batch is identical to the input.
pub fn seal(
    bytes: &[u8],
    base_offset: i64,
    epoch: i32,
    max_batch: usize,
) -> Result<Bytes, BatchError> {
    let info = inspect(bytes, max_batch)?;
    let mut out = BytesMut::from(bytes);
    let mut cursor = 0;
    let mut next = base_offset;
    for batch in info {
        out[cursor..cursor + 8].copy_from_slice(&next.to_be_bytes());
        out[cursor + EPOCH_AT..cursor + EPOCH_AT + 4].copy_from_slice(&epoch.to_be_bytes());
        next += i64::from(batch.records);
        cursor += batch.len;
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    };
    use bytes::BytesMut;

    fn sample(compression: Compression) -> bytes::Bytes {
        let records = vec![Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            sequence: 0,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            timestamp: 5,
            key: Some(bytes::Bytes::from_static(b"k")),
            value: Some(bytes::Bytes::from_static(b"hello")),
            headers: Default::default(),
        }];
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        buf.freeze()
    }

    #[test]
    fn seal_keeps_the_crc_covered_bytes() {
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let raw = sample(compression);
            let sealed = seal(&raw, 40, 7, 1024 * 1024).unwrap();
            assert_eq!(&raw[CONTENT_AT..], &sealed[CONTENT_AT..]);
            assert_eq!(i64::from_be_bytes(sealed[0..8].try_into().unwrap()), 40);
            assert_eq!(
                i32::from_be_bytes(sealed[EPOCH_AT..EPOCH_AT + 4].try_into().unwrap()),
                7
            );
            inspect(&sealed, 1024 * 1024).unwrap();
        }
    }

    #[test]
    fn bad_crc_is_corrupt() {
        let mut raw = sample(Compression::None).to_vec();
        raw[CONTENT_AT] ^= 0xff;
        assert_eq!(inspect(&raw, 1024 * 1024), Err(BatchError::Corrupt));
    }
}
