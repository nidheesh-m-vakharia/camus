//! Kafka record-batch v2 headers.
//!
//! The CRC covers every byte after the CRC field. [`seal`] rewrites
//! `baseOffset` and `partitionLeaderEpoch`, which sit in front of that CRC,
//! and copies the rest unchanged. Compression, headers, and record bodies
//! are not inspected.

use bytes::{Bytes, BytesMut};

use crate::records::{
    crc32c, Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

const HEADER: usize = 12;

/// Largest record batch any topic may accept, counted as Kafka counts
/// `max.message.bytes`: the whole batch including the 12-byte log overhead.
/// The broker, WAL, and uploader all check against this.
pub const MAX_BATCH_CEILING: usize = 8 * 1024 * 1024;
const EPOCH_AT: usize = 12;
const MAGIC_AT: usize = 16;
const CRC_AT: usize = 17;
const CONTENT_AT: usize = 21;
const ATTR_AT: usize = 21;
const LAST_DELTA_AT: usize = 23;
const BASE_TS_AT: usize = 27;
const MAX_TS_AT: usize = 35;
const COUNT_AT: usize = 57;
const PRODUCER_ID_AT: usize = 43;
const PRODUCER_EPOCH_AT: usize = 51;
const SEQUENCE_AT: usize = 53;
const PREFIX: usize = 61;

/// Producer identity of one batch. `producer_id` `-1` is not idempotent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    /// Idempotent producer id, or `-1`.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Sequence of the first record.
    pub base_sequence: i32,
    /// Records in the batch.
    pub records: i32,
}

/// Why a record batch cannot be sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchError {
    /// Truncated, bad magic, or a CRC that does not match the body.
    Corrupt,
    /// One batch is larger than the broker limit.
    TooLarge,
    /// Kept so older callers can still name the transactional attribute.
    /// Inspect no longer returns it; the broker decides from [`BatchInfo::transactional`].
    Transactional,
}

/// One v2 batch inside a produce payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchInfo {
    /// Byte length of this batch, including the offset and length fields.
    pub len: usize,
    /// Offset of the first record. Compaction can leave gaps after this.
    pub base_offset: i64,
    /// Offset of the last record.
    pub last_offset: i64,
    /// Records in the batch, including control records.
    pub records: i32,
    /// First timestamp in the batch header.
    pub base_timestamp: i64,
    /// Last timestamp in the batch header.
    pub max_timestamp: i64,
    /// The transactional attribute is set.
    pub transactional: bool,
}

/// The `partitionLeaderEpoch` of the first batch in `bytes`, or `-1`.
pub fn leader_epoch(bytes: &[u8]) -> i32 {
    bytes
        .get(EPOCH_AT..EPOCH_AT + 4)
        .map(|raw| i32::from_be_bytes(raw.try_into().expect("four bytes")))
        .unwrap_or(-1)
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
        let records = i32::from_be_bytes(batch[COUNT_AT..COUNT_AT + 4].try_into().unwrap());
        if records <= 0 {
            return Err(BatchError::Corrupt);
        }
        let base_offset = i64::from_be_bytes(batch[0..8].try_into().unwrap());
        let last_delta =
            i32::from_be_bytes(batch[LAST_DELTA_AT..LAST_DELTA_AT + 4].try_into().unwrap());
        if last_delta < 0 {
            return Err(BatchError::Corrupt);
        }
        out.push(BatchInfo {
            len: total,
            base_offset,
            last_offset: base_offset + i64::from(last_delta),
            records,
            base_timestamp: i64::from_be_bytes(
                batch[BASE_TS_AT..BASE_TS_AT + 8].try_into().unwrap(),
            ),
            max_timestamp: i64::from_be_bytes(batch[MAX_TS_AT..MAX_TS_AT + 8].try_into().unwrap()),
            transactional: attributes & (1 << 4) != 0,
        });
        offset += total;
    }
    Ok(out)
}

/// Producer id, epoch, and base sequence of each batch. Same checks as [`inspect`].
pub fn producer_batches(bytes: &[u8], max_batch: usize) -> Result<Vec<ProducerBatch>, BatchError> {
    let info = inspect(bytes, max_batch)?;
    let mut out = Vec::with_capacity(info.len());
    let mut offset = 0;
    for batch in &info {
        let raw = &bytes[offset..offset + batch.len];
        out.push(ProducerBatch {
            producer_id: i64::from_be_bytes(
                raw[PRODUCER_ID_AT..PRODUCER_EPOCH_AT].try_into().unwrap(),
            ),
            producer_epoch: i16::from_be_bytes(
                raw[PRODUCER_EPOCH_AT..SEQUENCE_AT].try_into().unwrap(),
            ),
            base_sequence: i32::from_be_bytes(raw[SEQUENCE_AT..COUNT_AT].try_into().unwrap()),
            records: batch.records,
        });
        offset += batch.len;
    }
    Ok(out)
}

/// Whether any record has no key. Uncompressed batches are scanned in place.
/// A compressed batch is decoded, because its keys sit inside the codec.
pub fn has_null_key(bytes: &[u8], max_batch: usize) -> Result<bool, BatchError> {
    let info = inspect(bytes, max_batch)?;
    let mut offset = 0;
    for batch in &info {
        let raw = &bytes[offset..offset + batch.len];
        let Ok(attr) = raw[ATTR_AT..ATTR_AT + 2].try_into() else {
            return Err(BatchError::Corrupt);
        };
        let attributes = i16::from_be_bytes(attr);
        let missing = if attributes & 0x07 != 0 {
            let mut owned = Bytes::copy_from_slice(raw);
            let sets = crate::records::RecordBatchDecoder::decode_all(&mut owned)
                .map_err(|_| BatchError::Corrupt)?;
            sets.iter()
                .flat_map(|set| &set.records)
                .any(|record| record.key.is_none())
        } else {
            null_key_uncompressed(raw)?
        };
        if missing {
            return Ok(true);
        }
        offset += batch.len;
    }
    Ok(false)
}

/// Records of one uncompressed batch. `true` when a key length is `-1`.
fn null_key_uncompressed(batch: &[u8]) -> Result<bool, BatchError> {
    if batch.len() < PREFIX {
        return Err(BatchError::Corrupt);
    }
    let Ok(count_bytes) = batch[COUNT_AT..COUNT_AT + 4].try_into() else {
        return Err(BatchError::Corrupt);
    };
    let count = i32::from_be_bytes(count_bytes);
    if count < 0 {
        return Err(BatchError::Corrupt);
    }
    let records = &batch[PREFIX..];
    let mut at = 0;
    for _ in 0..count {
        let (size, read) = read_ivar(records, at, 5)?;
        at += read;
        if size < 0 {
            return Err(BatchError::Corrupt);
        }
        let end = at + size as usize;
        if end > records.len() {
            return Err(BatchError::Corrupt);
        }
        if null_key_in_record(&records[at..end])? {
            return Ok(true);
        }
        at = end;
    }
    Ok(false)
}

fn null_key_in_record(record: &[u8]) -> Result<bool, BatchError> {
    if record.is_empty() {
        return Err(BatchError::Corrupt);
    }
    let mut at = 1;
    let (_, read) = read_ivar(record, at, 10)?;
    at += read;
    let (_, read) = read_ivar(record, at, 5)?;
    at += read;
    let (key_len, _) = read_ivar(record, at, 5)?;
    if key_len < -1 {
        return Err(BatchError::Corrupt);
    }
    Ok(key_len == -1)
}

/// Zigzag varint. `max_bytes` is 5 for a varint and 10 for a varlong.
fn read_ivar(buf: &[u8], at: usize, max_bytes: usize) -> Result<(i64, usize), BatchError> {
    let mut zigzag = 0u64;
    for i in 0..max_bytes {
        let Some(byte) = buf.get(at + i).copied() else {
            return Err(BatchError::Corrupt);
        };
        zigzag |= u64::from(byte & 0x7f) << (i * 7);
        if byte < 0x80 {
            if max_bytes == 5 {
                zigzag &= 0xffff_ffff;
            }
            let signed = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
            return Ok((signed, i + 1));
        }
    }
    Err(BatchError::Corrupt)
}

/// A control batch holding one ABORT marker at the last offset of the batch
/// at the front of `batch`, with that batch's producer, epoch and timestamp.
/// Camus stores no markers. A `read_committed` fetch sends this in place of
/// an aborted batch, so a client moves its position past the aborted range
/// as it would past Kafka's marker (ADR 0021). The marker takes no offset of
/// its own.
pub fn abort_marker(batch: &[u8]) -> Result<Bytes, BatchError> {
    let header = batch.get(..PREFIX).ok_or(BatchError::Corrupt)?;
    if header[MAGIC_AT] != 2 {
        return Err(BatchError::Corrupt);
    }
    let at = |from: usize, to: usize| header.get(from..to).ok_or(BatchError::Corrupt);
    let int = |from: usize| -> Result<[u8; 4], BatchError> {
        at(from, from + 4)?
            .try_into()
            .map_err(|_| BatchError::Corrupt)
    };
    let long = |from: usize| -> Result<[u8; 8], BatchError> {
        at(from, from + 8)?
            .try_into()
            .map_err(|_| BatchError::Corrupt)
    };
    let short = |from: usize| -> Result<[u8; 2], BatchError> {
        at(from, from + 2)?
            .try_into()
            .map_err(|_| BatchError::Corrupt)
    };
    let attributes = i16::from_be_bytes(short(ATTR_AT)?);
    let last = i64::from_be_bytes(long(0)?) + i64::from(i32::from_be_bytes(int(LAST_DELTA_AT)?));
    let marker = Record {
        transactional: true,
        control: true,
        delete_horizon: false,
        partition_leader_epoch: i32::from_be_bytes(int(EPOCH_AT)?),
        producer_id: i64::from_be_bytes(long(PRODUCER_ID_AT)?),
        producer_epoch: i16::from_be_bytes(short(PRODUCER_EPOCH_AT)?),
        timestamp_type: if attributes & (1 << 3) != 0 {
            TimestampType::LogAppend
        } else {
            TimestampType::Creation
        },
        offset: last,
        sequence: -1,
        timestamp: i64::from_be_bytes(long(MAX_TS_AT)?),
        // EndTransactionMarker: key version 0, type ABORT (0); value
        // version 0, coordinator epoch 0.
        key: Some(Bytes::from_static(&[0, 0, 0, 0])),
        value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
        headers: Default::default(),
    };
    let mut out = BytesMut::with_capacity(PREFIX + 32);
    RecordBatchEncoder::encode(
        &mut out,
        [&marker],
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|_| BatchError::Corrupt)?;
    Ok(out.freeze())
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
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
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
    fn an_abort_marker_is_a_control_record_at_the_batch_last_offset() -> anyhow::Result<()> {
        let sealed = seal(&sample(Compression::Gzip), 40, 7, 1024 * 1024)
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let marker = abort_marker(&sealed).map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let decoded = RecordBatchDecoder::decode(&mut marker.clone())?;
        let [record] = decoded.records.as_slice() else {
            anyhow::bail!("want one record, got {}", decoded.records.len());
        };
        assert!(record.control && record.transactional);
        assert_eq!(record.offset, 40);
        assert_eq!(record.partition_leader_epoch, 7);
        assert_eq!(record.key.as_deref(), Some(&[0u8, 0, 0, 0][..]));
        assert_eq!(
            abort_marker(&sealed[..PREFIX - 1]),
            Err(BatchError::Corrupt)
        );
        Ok(())
    }

    #[test]
    fn a_missing_key_is_visible_without_decoding_values() {
        let keyed = sample(Compression::None);
        assert_eq!(has_null_key(&keyed, 1024 * 1024), Ok(false));
        let compressed = sample(Compression::Gzip);
        assert_eq!(has_null_key(&compressed, 1024 * 1024), Ok(false));
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
            key: None,
            value: Some(bytes::Bytes::from_static(b"hello")),
            headers: Default::default(),
        }];
        let mut buf = BytesMut::new();
        let encoded = RecordBatchEncoder::encode(
            &mut buf,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        );
        assert!(encoded.is_ok());
        assert_eq!(has_null_key(&buf, 1024 * 1024), Ok(true));
    }

    #[test]
    fn bad_crc_is_corrupt() {
        let mut raw = sample(Compression::None).to_vec();
        raw[CONTENT_AT] ^= 0xff;
        assert_eq!(inspect(&raw, 1024 * 1024), Err(BatchError::Corrupt));
    }
}
