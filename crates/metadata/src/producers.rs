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
/// A transactional id is rejected. Transactions are Phase 5.
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
    let stored: i16 = raw.parse().map_err(|err: std::num::ParseIntError| err.to_string())?;
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
