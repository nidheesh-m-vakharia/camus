//! Idempotent produce sequences.
//!
//! The rules follow Kafka 4.4.0 `ProducerStateManager` / `ProducerAppendInfo`:
//! the next sequence is one past the last accepted record, a repeat of one of
//! the last five batches is the original append, and an older epoch is fenced.
//! A duplicate is not an error. The caller returns the stored base offset and
//! does not append.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::batch::ProducerBatch;
use crate::records::NO_PRODUCER_ID;

const OUT_OF_ORDER: i16 = 45;
const DUPLICATE: i16 = 46;
const INVALID_EPOCH: i16 = 47;
const INVALID_REQUEST: i16 = 42;
/// How many recent batches can still be acknowledged as duplicates.
const WINDOW: usize = 5;

/// One accepted batch, so a retry can be given its original offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredBatch {
    /// Sequence of the first record.
    pub base_sequence: i32,
    /// Sequence of the last record.
    pub last_sequence: i32,
    /// Base offset assigned when this batch was appended.
    pub base_offset: i64,
    /// Records in the batch.
    pub records: i32,
}

/// Per-partition producer state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProducerSeq {
    /// Epoch of the batches below.
    pub epoch: i16,
    /// Newest batches, oldest first, at most [`WINDOW`].
    pub batches: Vec<StoredBatch>,
}

/// What to do with a produce payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// No producer id. Append.
    Plain,
    /// Sequences continue. Append, then [`record`] each batch.
    Fresh,
    /// The payload repeats stored batches. Do not append.
    Replay {
        /// Base offset of the first batch.
        base_offset: i64,
    },
}

/// Decide whether `batches` append, replay, or fail.
///
/// `existing` is this partition's producers. It is not modified.
pub fn classify(
    existing: &BTreeMap<i64, ProducerSeq>,
    batches: &[ProducerBatch],
) -> Result<Class, i16> {
    if batches.is_empty() || batches.iter().all(|batch| batch.producer_id == NO_PRODUCER_ID) {
        return Ok(Class::Plain);
    }
    if batches.iter().any(|batch| batch.producer_id == NO_PRODUCER_ID) {
        return Err(INVALID_REQUEST);
    }
    let mut scratch = existing.clone();
    let mut replay: Option<i64> = None;
    let mut fresh = false;
    for batch in batches {
        if batch.producer_epoch < 0 || batch.records <= 0 {
            return Err(INVALID_EPOCH);
        }
        match one(scratch.get(&batch.producer_id), batch)? {
            One::Fresh => {
                if replay.is_some() {
                    return Err(OUT_OF_ORDER);
                }
                fresh = true;
                let state = scratch.entry(batch.producer_id).or_default();
                record(
                    state,
                    batch.producer_epoch,
                    batch.base_sequence,
                    batch.records,
                    -1,
                );
            }
            One::Replay { base_offset } => {
                if fresh {
                    return Err(OUT_OF_ORDER);
                }
                if replay.is_none() {
                    replay = Some(base_offset);
                }
            }
        }
    }
    if fresh {
        Ok(Class::Fresh)
    } else {
        Ok(Class::Replay {
            base_offset: replay.expect("replay"),
        })
    }
}

/// Remember a batch that was just appended at `base_offset`.
pub fn record(
    state: &mut ProducerSeq,
    epoch: i16,
    base_sequence: i32,
    records: i32,
    base_offset: i64,
) {
    if epoch != state.epoch {
        state.batches.clear();
        state.epoch = epoch;
    }
    let last_sequence = base_sequence.wrapping_add(records).wrapping_sub(1);
    state.batches.push(StoredBatch {
        base_sequence,
        last_sequence,
        base_offset,
        records,
    });
    if state.batches.len() > WINDOW {
        state.batches.drain(0..state.batches.len() - WINDOW);
    }
}

enum One {
    Fresh,
    Replay { base_offset: i64 },
}

fn one(state: Option<&ProducerSeq>, batch: &ProducerBatch) -> Result<One, i16> {
    // An empty record is the same as no record: the epoch is only real once
    // a batch has been accepted.
    let Some(state) = state.filter(|state| !state.batches.is_empty()) else {
        if batch.base_sequence != 0 {
            return Err(OUT_OF_ORDER);
        }
        return Ok(One::Fresh);
    };
    if batch.producer_epoch < state.epoch {
        return Err(INVALID_EPOCH);
    }
    if batch.producer_epoch > state.epoch {
        if batch.base_sequence != 0 {
            return Err(OUT_OF_ORDER);
        }
        return Ok(One::Fresh);
    }
    let expected = state
        .batches
        .last()
        .map(|stored| stored.last_sequence.wrapping_add(1))
        .unwrap_or(0);
    if batch.base_sequence == expected {
        return Ok(One::Fresh);
    }
    if let Some(stored) = state
        .batches
        .iter()
        .find(|stored| stored.base_sequence == batch.base_sequence)
    {
        let last = batch
            .base_sequence
            .wrapping_add(batch.records)
            .wrapping_sub(1);
        if stored.records == batch.records && stored.last_sequence == last {
            return Ok(One::Replay {
                base_offset: stored.base_offset,
            });
        }
        return Err(DUPLICATE);
    }
    Err(OUT_OF_ORDER)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(sequence: i32, records: i32) -> ProducerBatch {
        ProducerBatch {
            producer_id: 7,
            producer_epoch: 0,
            base_sequence: sequence,
            records,
        }
    }

    #[test]
    fn a_retry_returns_the_original_offset() {
        let mut map = BTreeMap::new();
        assert_eq!(classify(&map, &[batch(0, 2)]).unwrap(), Class::Fresh);
        let state = map.entry(7).or_default();
        record(state, 0, 0, 2, 10);
        assert_eq!(
            classify(&map, &[batch(0, 2)]).unwrap(),
            Class::Replay { base_offset: 10 }
        );
        assert_eq!(classify(&map, &[batch(2, 1)]).unwrap(), Class::Fresh);
        assert_eq!(classify(&map, &[batch(4, 1)]).unwrap_err(), OUT_OF_ORDER);
    }

    #[test]
    fn an_older_epoch_is_fenced() {
        let mut map = BTreeMap::new();
        record(map.entry(7).or_default(), 2, 0, 1, 0);
        let mut stale = batch(0, 1);
        stale.producer_epoch = 1;
        assert_eq!(classify(&map, &[stale]).unwrap_err(), INVALID_EPOCH);
        let mut next = batch(0, 1);
        next.producer_epoch = 3;
        assert_eq!(classify(&map, &[next]).unwrap(), Class::Fresh);
    }
}
