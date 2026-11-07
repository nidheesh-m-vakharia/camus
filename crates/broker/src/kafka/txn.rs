//! Transaction coordinator APIs. The metadata leader is the coordinator.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use camus_metadata::groups::{OffsetCommit, OffsetRecord};
use camus_metadata::Command;
use camus_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use camus_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use camus_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, EndTxnRequest, EndTxnResponse, RequestHeader, TopicName,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use camus_protocol::protocol::Decodable;

use super::{frame_of, Kafka, Reply};

const NOT_COORDINATOR: i16 = 16;
const INVALID_REQUEST: i16 = 42;
const UNKNOWN_TOPIC: i16 = 3;

impl Kafka {
    pub(super) async fn add_partitions_to_txn(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = AddPartitionsToTxnRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = AddPartitionsToTxnResponse::default();
        if !self.leader() {
            response.error_code = NOT_COORDINATOR;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let id = request.v3_and_below_transactional_id.0.to_string();
        let partitions: Vec<(String, i32)> = request
            .v3_and_below_topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| (topic.name.0.to_string(), *partition))
            })
            .collect();
        if id.is_empty() || partitions.is_empty() {
            response.error_code = INVALID_REQUEST;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let applied = self
            .node
            .apply(Command::AddTxnPartitions {
                transactional_id: id,
                producer_id: request.v3_and_below_producer_id.0,
                producer_epoch: request.v3_and_below_producer_epoch,
                partitions,
            })
            .await;
        let code = match applied {
            Ok(result) => result.error_code,
            Err(_) => NOT_COORDINATOR,
        };
        for topic in request.v3_and_below_topics {
            let mut row = AddPartitionsToTxnTopicResult::default();
            row.name = topic.name;
            row.results_by_partition = topic
                .partitions
                .into_iter()
                .map(|partition_index| {
                    let mut part = AddPartitionsToTxnPartitionResult::default();
                    part.partition_index = partition_index;
                    part.partition_error_code = code;
                    part
                })
                .collect();
            response.results_by_topic_v3_and_below.push(row);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn add_offsets_to_txn(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = AddOffsetsToTxnRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = AddOffsetsToTxnResponse::default();
        response.error_code = self.offsets_ready(&request).await;
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    async fn offsets_ready(&self, request: &AddOffsetsToTxnRequest) -> i16 {
        if !self.leader() {
            return NOT_COORDINATOR;
        }
        if request.transactional_id.0.is_empty() || request.group_id.0.is_empty() {
            return INVALID_REQUEST;
        }
        let get = |key: &str| self.node.read_local(key).map_err(|err| err.to_string());
        camus_metadata::txn_ongoing(
            &get,
            request.transactional_id.0.as_str(),
            request.producer_id.0,
            request.producer_epoch,
        )
        .err()
        .unwrap_or(0)
    }

    pub(super) async fn txn_offset_commit(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = TxnOffsetCommitRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = TxnOffsetCommitResponse::default();
        if !self.leader() {
            for topic in request.topics {
                response
                    .topics
                    .push(offset_topic(topic.name, UNKNOWN_TOPIC, &[]));
            }
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let mut offsets = Vec::new();
        let mut planned = Vec::new();
        for topic in &request.topics {
            let name = topic.name.0.to_string();
            let Some(stored) = self.topic(&name) else {
                planned.push((topic.name.clone(), UNKNOWN_TOPIC, topic.partitions.clone()));
                continue;
            };
            for partition in &topic.partitions {
                offsets.push(OffsetCommit {
                    topic_id: stored.id.clone(),
                    partition: partition.partition_index,
                    record: OffsetRecord {
                        offset: partition.committed_offset,
                        leader_epoch: partition.committed_leader_epoch,
                        metadata: partition
                            .committed_metadata
                            .as_ref()
                            .map(|value| value.to_string()),
                        commit_timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|time| time.as_millis() as i64)
                            .unwrap_or(0),
                    },
                });
            }
            planned.push((topic.name.clone(), 0, topic.partitions.clone()));
        }
        let code = if offsets.is_empty() {
            0
        } else {
            match self
                .node
                .apply(Command::StageTxnOffsets {
                    transactional_id: request.transactional_id.0.to_string(),
                    producer_id: request.producer_id.0,
                    producer_epoch: request.producer_epoch,
                    group: request.group_id.0.to_string(),
                    offsets,
                })
                .await
            {
                Ok(result) => result.error_code,
                Err(_) => NOT_COORDINATOR,
            }
        };
        for (name, topic_code, partitions) in planned {
            let partition_code = if topic_code == 0 { code } else { topic_code };
            response
                .topics
                .push(offset_topic(name, partition_code, &partitions));
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn end_txn(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = EndTxnRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = EndTxnResponse::default();
        if !self.leader() {
            response.error_code = NOT_COORDINATOR;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        if request.transactional_id.0.is_empty() {
            response.error_code = INVALID_REQUEST;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let applied = self
            .node
            .apply(Command::EndTransaction {
                transactional_id: request.transactional_id.0.to_string(),
                producer_id: request.producer_id.0,
                producer_epoch: request.producer_epoch,
                committed: request.committed,
            })
            .await;
        response.error_code = match applied {
            Ok(result) => result.error_code,
            Err(_) => NOT_COORDINATOR,
        };
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

fn offset_topic(
    name: TopicName,
    code: i16,
    partitions: &[camus_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequestPartition],
) -> TxnOffsetCommitResponseTopic {
    let mut topic = TxnOffsetCommitResponseTopic::default();
    topic.name = name;
    topic.partitions = partitions
        .iter()
        .map(|partition| {
            let mut row = TxnOffsetCommitResponsePartition::default();
            row.partition_index = partition.partition_index;
            row.error_code = code;
            row
        })
        .collect();
    topic
}
