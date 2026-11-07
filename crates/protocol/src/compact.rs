//! Log compaction. The latest record for each key is kept at its original
//! offset. A null value is a tombstone and replaces every earlier value of
//! that key. Tombstones are kept; `delete.retention.ms` is not applied.
//!
//! Records are rebuilt with [`crate::records::RecordBatchEncoder`], so a
//! batch's base offset and last-offset delta still name the surviving
//! records. The high watermark does not move.

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};

use crate::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions};

/// Records that should replace `records` in offset order.
///
/// `None` means every keyed record is already the latest one, so a rewrite
/// would not drop anything. Control records are not part of the key index.
pub fn plan(records: &[Record]) -> Option<Vec<Record>> {
    let kept = retain(records);
    let comparable = records.iter().filter(|record| !record.control).count();
    if kept.len() == comparable {
        None
    } else {
        Some(kept)
    }
}

/// Latest record per key, plus any record that has no key, in offset order.
pub fn retain(records: &[Record]) -> Vec<Record> {
    let mut latest: BTreeMap<Vec<u8>, Record> = BTreeMap::new();
    let mut unkeyed = Vec::new();
    for record in records {
        if record.control {
            continue;
        }
        match &record.key {
            Some(key) => {
                latest.insert(key.to_vec(), record.clone());
            }
            None => unkeyed.push(record.clone()),
        }
    }
    let mut out: Vec<Record> = latest.into_values().collect();
    out.append(&mut unkeyed);
    out.sort_by_key(|record| record.offset);
    out
}

/// Encode `records` as v2 batches. Offsets on the records are preserved.
pub fn rebuild(records: &[Record]) -> Result<Bytes, anyhow::Error> {
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(buf.freeze())
}

/// `cleanup.policy` contains `compact`.
pub fn compacts(policy: &str) -> bool {
    policy.split(',').any(|part| part.trim() == "compact")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{
        RecordBatchDecoder, TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH,
        NO_PRODUCER_ID,
    };
    use bytes::Bytes;

    fn record(offset: i64, key: &[u8], value: Option<&[u8]>) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: 0,
            timestamp: offset,
            key: Some(Bytes::copy_from_slice(key)),
            value: value.map(Bytes::copy_from_slice),
            headers: Default::default(),
        }
    }

    #[test]
    fn the_latest_value_keeps_its_offset() {
        let records = vec![
            record(0, b"a", Some(b"1")),
            record(1, b"b", Some(b"1")),
            record(2, b"a", Some(b"2")),
        ];
        let kept = plan(&records).expect("a duplicate");
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].offset, 1);
        assert_eq!(kept[0].key.as_deref(), Some(b"b".as_slice()));
        assert_eq!(kept[1].offset, 2);
        assert_eq!(kept[1].value.as_deref(), Some(b"2".as_slice()));
        let bytes = rebuild(&kept).unwrap();
        let mut cursor = bytes.clone();
        let decoded = RecordBatchDecoder::decode_all(&mut cursor).unwrap();
        let flat: Vec<_> = decoded.into_iter().flat_map(|set| set.records).collect();
        assert_eq!(flat[0].offset, 1);
        assert_eq!(flat[1].offset, 2);
    }

    #[test]
    fn a_tombstone_replaces_the_older_value() {
        let records = vec![record(0, b"a", Some(b"1")), record(1, b"a", None)];
        let kept = plan(&records).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].offset, 1);
        assert!(kept[0].value.is_none());
    }

    #[test]
    fn unique_keys_are_left_alone() {
        let records = vec![record(0, b"a", Some(b"1")), record(1, b"b", Some(b"1"))];
        assert!(plan(&records).is_none());
    }
}
