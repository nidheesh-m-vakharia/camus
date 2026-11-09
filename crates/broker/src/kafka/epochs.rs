//! `OffsetForLeaderEpoch`: where a leader epoch ended, so a client can tell
//! whether the log it read was truncated (KIP-101, KIP-320).
//!
//! The answer follows Kafka's `LeaderEpochFileCache.endOffsetFor`. The
//! sealed part of a partition's epoch history is in metadata. Records still
//! in the WAL carry their epoch in the batch header, so the leader reads
//! those too: after a failover the old term's batches survive unsealed, and
//! answering from the sealed history alone would report a truncation that
//! did not happen.

use bytes::Bytes;
use camus_metadata::{EpochStart, Topic};
use camus_protocol::batch;
use camus_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderTopicResult,
};
use camus_protocol::messages::{
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, RequestHeader,
};
use camus_protocol::protocol::Decodable;

use super::{auth, frame_of, topic_id, Caller, Kafka, Reply};

const NOT_LEADER: i16 = 6;
const UNKNOWN_TOPIC: i16 = 3;

/// How much unsealed WAL one lookup reads at a time.
const SCAN_CHUNK: usize = 1024 * 1024;

impl Kafka {
    pub(super) async fn offset_for_leader_epoch(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = OffsetForLeaderEpochRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        let mut response = OffsetForLeaderEpochResponse::default();
        for topic in request.topics {
            let name = topic.topic.0.to_string();
            let stored = self.topic(&name);
            let allowed = self.allows(caller, auth::TOPIC, &name, auth::DESCRIBE);
            let mut result = OffsetForLeaderTopicResult::default();
            result.topic = topic.topic.clone();
            for asked in topic.partitions {
                let mut row = EpochEndOffset::default();
                row.partition = asked.partition;
                row.leader_epoch = -1;
                row.end_offset = -1;
                row.error_code = match &stored {
                    _ if !allowed => auth::TOPIC_FAILED,
                    None => UNKNOWN_TOPIC,
                    Some(found) if asked.partition < 0 || asked.partition >= found.partitions => {
                        UNKNOWN_TOPIC
                    }
                    Some(found) if !self.leads_partition(found, asked.partition) => NOT_LEADER,
                    Some(found) => match self.check_leader_epoch(
                        found,
                        asked.partition,
                        asked.current_leader_epoch,
                    ) {
                        Err(code) => code,
                        Ok(()) => {
                            let (epoch, end) = self
                                .epoch_end(found, asked.partition, asked.leader_epoch)
                                .await;
                            row.leader_epoch = epoch;
                            row.end_offset = end;
                            0
                        }
                    },
                };
                result.partitions.push(row);
            }
            response.topics.push(result);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    /// The largest epoch at or below `requested`, and the offset where it
    /// ended, as `(epoch, end_offset)`.
    async fn epoch_end(&self, topic: &Topic, partition: i32, requested: i32) -> (i32, i64) {
        let current = self.leading_epoch(topic, partition);
        let Some(high_watermark) = self.high_watermark(topic, partition) else {
            return (-1, -1);
        };
        if requested < 0 || requested > current {
            return (-1, -1);
        }
        if requested == current {
            return (current, high_watermark);
        }
        let Some(state) = self.partition_state(topic, partition) else {
            return (-1, -1);
        };
        let mut starts = state.epochs.clone();
        if topic.durability == "wal" && !starts.iter().any(|entry| entry.epoch > requested) {
            self.unsealed_epochs(
                topic,
                partition,
                state.next_offset,
                high_watermark,
                requested,
                &mut starts,
            )
            .await;
        }
        if starts.last().is_none_or(|last| last.epoch < current) {
            starts.push(EpochStart {
                epoch: current,
                start: high_watermark,
            });
        }
        end_of(&starts, requested)
    }

    /// Append the epochs of unsealed WAL batches to `starts`, stopping at the
    /// first epoch past `requested`.
    async fn unsealed_epochs(
        &self,
        topic: &Topic,
        partition: i32,
        sealed: i64,
        high_watermark: i64,
        requested: i32,
        starts: &mut Vec<EpochStart>,
    ) {
        let id = topic_id(topic);
        let Some(wal) = self.wal_of(topic, partition) else {
            return;
        };
        let mut offset = sealed;
        while offset < high_watermark {
            let Ok(bytes) = wal.read(&id, partition, sealed, offset, SCAN_CHUNK).await else {
                return;
            };
            let Ok(batches) = batch::inspect(&bytes, batch::MAX_BATCH_CEILING) else {
                return;
            };
            if batches.is_empty() {
                return;
            }
            let mut cursor = 0;
            for info in batches {
                let epoch = batch::leader_epoch(&bytes[cursor..]);
                cursor += info.len;
                offset = info.last_offset + 1;
                if starts.last().is_none_or(|last| epoch > last.epoch) {
                    starts.push(EpochStart {
                        epoch,
                        start: info.base_offset,
                    });
                    if epoch > requested {
                        return;
                    }
                }
            }
        }
    }
}

/// Kafka's `endOffsetFor` over an epoch history: the floor epoch of
/// `requested` (or `requested` itself when it predates the history), and
/// the start of the first later epoch.
fn end_of(starts: &[EpochStart], requested: i32) -> (i32, i64) {
    let Some(higher) = starts.iter().find(|entry| entry.epoch > requested) else {
        return (-1, -1);
    };
    let floor = starts
        .iter()
        .rev()
        .find(|entry| entry.epoch <= requested)
        .map(|entry| entry.epoch)
        .unwrap_or(requested);
    (floor, higher.start)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn starts(entries: &[(i32, i64)]) -> Vec<EpochStart> {
        entries
            .iter()
            .map(|(epoch, start)| EpochStart {
                epoch: *epoch,
                start: *start,
            })
            .collect()
    }

    #[test]
    fn an_epoch_ends_where_the_next_one_starts() {
        let history = starts(&[(1, 0), (3, 10), (7, 25)]);
        assert_eq!(end_of(&history, 1), (1, 10));
        assert_eq!(end_of(&history, 2), (1, 10), "floor epoch of a gap");
        assert_eq!(end_of(&history, 3), (3, 25));
        assert_eq!(end_of(&history, 0), (0, 0), "older than the history");
        assert_eq!(end_of(&history, 7), (-1, -1), "the latest has no end here");
    }
}
