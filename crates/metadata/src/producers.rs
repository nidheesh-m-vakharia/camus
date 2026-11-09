//! Producer ids and the per-partition sequences metadata records when a
//! batch is sealed. The WAL shard keeps its own copy so a leader change
//! before the upload still recognizes a retry. This is the copy that
//! survives the shard.

use std::collections::BTreeMap;

use camus_protocol::batch::ProducerBatch;
use camus_protocol::idempotence::{self, Class, ProducerSeq};
use serde::{Deserialize, Serialize};

use crate::catalog::{Outcome, PublishPart};

/// A batch whose sequence metadata should remember, included in the same
/// command as the extent that holds its bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerNote {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Producer id from `InitProducerId`.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Sequence of the first record.
    pub base_sequence: i32,
    /// Records in the batch.
    pub records: i32,
    /// Base offset assigned to that first record.
    pub base_offset: i64,
}

/// Key of one producer's sequence state.
pub fn sequence_key(topic: &str, partition: i32, producer_id: i64) -> String {
    format!("seq/{topic}/{partition}/{producer_id}")
}

fn id_key(producer_id: i64) -> String {
    format!("producer/{producer_id}")
}

const NEXT: &str = "producer/next";

/// Result of allocating or bumping a producer id.
pub enum Issued {
    /// Kafka error code. Nothing was written.
    Reject(i16),
    /// Keys to write, and `"{id},{epoch}"` for the response.
    Applied {
        /// Puts.
        put: Vec<(String, String)>,
        /// `producer_id,producer_epoch`.
        value: String,
    },
}

/// Allocate a producer id, or bump the epoch of an existing one.
///
/// A transactional id is rejected here. [`crate::txn::init`] owns that path.
pub fn init_producer(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<Issued, String> {
    if transactional {
        return Ok(Issued::Reject(48));
    }
    if producer_id < 0 {
        let next = match get(NEXT)? {
            Some(raw) => raw.parse::<i64>().map_err(|err| err.to_string())?,
            None => 1,
        };
        if next <= 0 {
            return Ok(Issued::Reject(42));
        }
        return Ok(Issued::Applied {
            put: vec![
                (id_key(next), "0".into()),
                (NEXT.to_string(), (next + 1).to_string()),
            ],
            value: format!("{next},0"),
        });
    }
    let Some(raw) = get(&id_key(producer_id))? else {
        return Ok(Issued::Reject(59));
    };
    let stored: i16 = raw
        .parse()
        .map_err(|err: std::num::ParseIntError| err.to_string())?;
    if producer_epoch != stored {
        return Ok(Issued::Reject(47));
    }
    let Some(bumped) = stored.checked_add(1) else {
        return Ok(Issued::Reject(47));
    };
    Ok(Issued::Applied {
        put: vec![(id_key(producer_id), bumped.to_string())],
        value: format!("{producer_id},{bumped}"),
    })
}

/// Check sequence notes against stored state. `parts` must contain the bytes.
pub fn apply_notes(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    parts: &[PublishPart],
    notes: &[ProducerNote],
) -> Result<Outcome, String> {
    let mut seqs: BTreeMap<(String, i32, i64), ProducerSeq> = BTreeMap::new();
    for note in notes {
        if note.producer_id < 0 || note.records <= 0 || note.producer_epoch < 0 {
            return Ok(Outcome::Reject(42));
        }
        let covered = parts.iter().any(|part| {
            part.topic == note.topic
                && part.partition == note.partition
                && part.start <= note.base_offset
                && note.base_offset + i64::from(note.records) <= part.end
        });
        if !covered {
            return Ok(Outcome::Reject(42));
        }
        let key = (note.topic.clone(), note.partition, note.producer_id);
        if !seqs.contains_key(&key) {
            if let Some(raw) = get(&sequence_key(&note.topic, note.partition, note.producer_id))? {
                seqs.insert(
                    key.clone(),
                    serde_json::from_str(&raw).map_err(|err| err.to_string())?,
                );
            }
        }
        let mut view = BTreeMap::new();
        if let Some(state) = seqs.get(&key) {
            view.insert(note.producer_id, state.clone());
        }
        let batch = ProducerBatch {
            producer_id: note.producer_id,
            producer_epoch: note.producer_epoch,
            base_sequence: note.base_sequence,
            records: note.records,
        };
        match idempotence::classify(&view, &[batch]) {
            Ok(Class::Fresh) => {
                let state = seqs.entry(key).or_default();
                idempotence::record(
                    state,
                    note.producer_epoch,
                    note.base_sequence,
                    note.records,
                    note.base_offset,
                );
            }
            Ok(Class::Replay { .. }) => return Ok(Outcome::Reject(46)),
            Ok(Class::Plain) => return Ok(Outcome::Reject(42)),
            Err(code) => return Ok(Outcome::Reject(code)),
        }
    }
    let mut put = Vec::new();
    for ((topic, partition, producer_id), state) in seqs {
        put.push((
            sequence_key(&topic, partition, producer_id),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        ));
    }
    Ok(Outcome::Change {
        put,
        del: Vec::new(),
        value: None,
    })
}

/// Remove producer sequence state Kafka would expire. For each candidate,
/// the state goes when the log start has passed the producer's last batch
/// (Kafka's `ProducerStateManager.truncateHead`), or when the sealed extent
/// holding that batch is older than `expiration_ms` (Kafka's
/// `producer.id.expiration.ms`, which uses the batch's newest timestamp). A
/// batch still only in the WAL, or a producer with an ongoing transaction,
/// keeps its state. `value` is the number of entries removed.
pub fn expire(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    scan: crate::catalog::Scan,
    keys: &[(String, i32, i64)],
    now_ms: i64,
    expiration_ms: i64,
) -> Result<Outcome, String> {
    let mut del = Vec::new();
    for (topic, partition, producer_id) in keys {
        let key = sequence_key(topic, *partition, *producer_id);
        let Some(raw) = get(&key)? else {
            continue;
        };
        if in_transaction(get, *producer_id)? {
            continue;
        }
        let state: ProducerSeq = serde_json::from_str(&raw).map_err(|err| err.to_string())?;
        let Some(last) = state.batches.last() else {
            del.push(key);
            continue;
        };
        let last_end = last.base_offset + i64::from(last.records);
        let Some(raw_topic) = get(&crate::catalog::topic_storage_key(topic))? else {
            del.push(key);
            continue;
        };
        let stored = crate::catalog::parse_topic(&raw_topic)?;
        let Some(raw_part) = get(&crate::catalog::partition_storage_key(
            &stored.id, *partition,
        ))?
        else {
            del.push(key);
            continue;
        };
        let part = crate::catalog::parse_partition(&raw_part)?;
        if last_end <= part.log_start {
            del.push(key);
            continue;
        }
        let holder = crate::catalog::extents_after(
            scan,
            &stored.id,
            *partition,
            &part,
            last.base_offset,
            1,
        )?
        .into_iter()
        .find(|extent| extent.start <= last.base_offset);
        if let Some(extent) = holder {
            if now_ms.saturating_sub(extent.max_timestamp) >= expiration_ms {
                del.push(key);
            }
        }
    }
    let removed = del.len().to_string();
    Ok(Outcome::Change {
        put: Vec::new(),
        del,
        value: Some(removed),
    })
}

fn in_transaction(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    producer_id: i64,
) -> Result<bool, String> {
    let Some(id) = get(&format!("txn-by-producer/{producer_id}"))? else {
        return Ok(false);
    };
    let Some(raw) = get(&format!("txn/{id}"))? else {
        return Ok(false);
    };
    let clock = crate::txn::clock(&raw)?;
    Ok(clock.producer_id == producer_id && clock.ongoing)
}

#[cfg(test)]
mod expiry_tests {
    use std::collections::HashMap;

    use camus_protocol::idempotence::StoredBatch;

    use super::*;
    use crate::catalog::{self, Extent};

    fn store(extent_ts: i64, log_start: i64, extent_end: i64) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let get = |key: &str| Ok(map.get(key).cloned());
        let Outcome::Change { put, .. } =
            catalog::create_topic(&get, "orders", "topic-1", 1, "wal", &[]).unwrap()
        else {
            panic!("create");
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        let part_key = catalog::partition_storage_key("topic-1", 0);
        let mut part = catalog::parse_partition(&map[&part_key]).unwrap();
        part.log_start = log_start;
        part.next_offset = 20;
        if extent_end > 0 {
            part.extents.push(Extent {
                start: 0,
                end: extent_end,
                key: "bundle/a.seg".into(),
                file_offset: 0,
                length: 10,
                base_timestamp: extent_ts,
                max_timestamp: extent_ts,
            });
        }
        map.insert(part_key, serde_json::to_string(&part).unwrap());
        let state = ProducerSeq {
            epoch: 0,
            batches: vec![StoredBatch {
                base_sequence: 0,
                last_sequence: 4,
                base_offset: 5,
                records: 5,
            }],
        };
        map.insert(
            sequence_key("orders", 0, 7),
            serde_json::to_string(&state).unwrap(),
        );
        map
    }

    fn removed(map: &HashMap<String, String>, now: i64) -> bool {
        let get = |key: &str| Ok(map.get(key).cloned());
        let scan = |from: &str, to: &str, limit: usize| catalog::scan_map(map, from, to, limit);
        let Outcome::Change { del, .. } =
            expire(&get, &scan, &[("orders".into(), 0, 7)], now, 1_000).unwrap()
        else {
            panic!("expire");
        };
        !del.is_empty()
    }

    #[test]
    fn state_expires_with_its_extent_or_the_log_start() {
        assert!(!removed(&store(10_000, 0, 20), 10_500), "fresh extent");
        assert!(removed(&store(10_000, 0, 20), 11_000), "old extent");
        assert!(
            removed(&store(10_000_000, 10, 20), 0),
            "log start passed it"
        );
        assert!(!removed(&store(0, 0, 0), i64::MAX), "still only in the WAL");
    }
}
