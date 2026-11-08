//! Transaction coordinator APIs. The metadata leader is the coordinator.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use camus_metadata::groups::{OffsetCommit, OffsetRecord};
use camus_metadata::{txn_inspect, txn_state, Command, TxnInfo};
use camus_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use camus_protocol::messages::describe_transactions_response::{TopicData, TransactionState};
use camus_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use camus_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, DescribeTransactionsRequest, DescribeTransactionsResponse,
    EndTxnRequest, EndTxnResponse, ListTransactionsRequest, ListTransactionsResponse,
    RequestHeader, TopicName, TransactionalId, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use camus_protocol::protocol::{Decodable, StrBytes};

use super::{auth, frame_of, Caller, Kafka, Reply};

const NOT_COORDINATOR: i16 = 16;
const INVALID_REQUEST: i16 = 42;
const UNKNOWN_TOPIC: i16 = 3;
const NOT_FOUND: i16 = 72;
const TXN_AUTH: i16 = 53;
const OPERATION_NOT_ATTEMPTED: i16 = 55;
const CLUSTER_AUTH: i16 = 31;
const CLUSTER: i8 = 4;
const TXN_ID: i8 = 5;
const DESCRIBE: i8 = 8;

impl Kafka {
    pub(super) async fn add_partitions_to_txn(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
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
        // Kafka needs WRITE on the transactional id and on every topic. When
        // a topic is denied, nothing is added: denied partitions get 29 and
        // the others OPERATION_NOT_ATTEMPTED.
        let id_allowed = self.allows(caller, auth::TXN_ID, &id, auth::WRITE);
        let denied: Vec<bool> = request
            .v3_and_below_topics
            .iter()
            .map(|topic| !self.allows(caller, auth::TOPIC, topic.name.0.as_str(), auth::WRITE))
            .collect();
        if !id_allowed || denied.iter().any(|denied| *denied) {
            for (topic, topic_denied) in request.v3_and_below_topics.into_iter().zip(denied) {
                let code = if !id_allowed {
                    auth::TXN_ID_FAILED
                } else if topic_denied {
                    auth::TOPIC_FAILED
                } else {
                    OPERATION_NOT_ATTEMPTED
                };
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
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let applied = self
            .node
            .apply(Command::AddTxnPartitionsAt {
                transactional_id: id,
                producer_id: request.v3_and_below_producer_id.0,
                producer_epoch: request.v3_and_below_producer_epoch,
                partitions,
                started_ms: now_ms(),
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
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = AddOffsetsToTxnRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = AddOffsetsToTxnResponse::default();
        response.error_code = if !self.allows(
            caller,
            auth::TXN_ID,
            request.transactional_id.0.as_str(),
            auth::WRITE,
        ) {
            auth::TXN_ID_FAILED
        } else if !self.allows(caller, auth::GROUP, request.group_id.0.as_str(), auth::READ) {
            auth::GROUP_FAILED
        } else {
            self.offsets_ready(&request).await
        };
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

    pub(super) async fn txn_offset_commit(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
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
        let gate = if !self.allows(
            caller,
            auth::TXN_ID,
            request.transactional_id.0.as_str(),
            auth::WRITE,
        ) {
            auth::TXN_ID_FAILED
        } else if !self.allows(caller, auth::GROUP, request.group_id.0.as_str(), auth::READ) {
            auth::GROUP_FAILED
        } else {
            0
        };
        if gate != 0 {
            for topic in request.topics {
                let partitions = topic.partitions.clone();
                response
                    .topics
                    .push(offset_topic(topic.name, gate, &partitions));
            }
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let mut offsets = Vec::new();
        let mut planned = Vec::new();
        for topic in &request.topics {
            let name = topic.name.0.to_string();
            if !self.allows(caller, auth::TOPIC, &name, auth::READ) {
                planned.push((
                    topic.name.clone(),
                    auth::TOPIC_FAILED,
                    topic.partitions.clone(),
                ));
                continue;
            }
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

    pub(super) async fn end_txn(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
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
        if !self.allows(
            caller,
            auth::TXN_ID,
            request.transactional_id.0.as_str(),
            auth::WRITE,
        ) {
            response.error_code = auth::TXN_ID_FAILED;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let applied = self
            .node
            .apply(Command::EndTransaction {
                transactional_id: request.transactional_id.0.to_string(),
                producer_id: request.producer_id.0,
                producer_epoch: request.producer_epoch,
                committed: request.committed,
                now_ms: now_ms(),
            })
            .await;
        response.error_code = match applied {
            Ok(result) => result.error_code,
            Err(_) => NOT_COORDINATOR,
        };
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn describe_transactions(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DescribeTransactionsRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        let mut response = DescribeTransactionsResponse::default();
        let leader = self.leader();
        for id in request.transactional_ids {
            let name = id.0.to_string();
            let mut state = TransactionState::default();
            state.transactional_id = TransactionalId(StrBytes::from_string(name.clone()));
            if !leader {
                state.error_code = NOT_COORDINATOR;
            } else if !self.allows(caller, TXN_ID, &name, DESCRIBE) {
                state.error_code = TXN_AUTH;
            } else {
                match self.node.read_local(&format!("txn/{name}")) {
                    Ok(Some(raw)) => match txn_inspect(&name, &raw) {
                        Ok(info) => fill_described(&mut state, &info),
                        Err(_) => state.error_code = NOT_FOUND,
                    },
                    Ok(None) => state.error_code = NOT_FOUND,
                    Err(_) => state.error_code = NOT_COORDINATOR,
                }
            }
            response.transaction_states.push(state);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn list_transactions(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = ListTransactionsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = ListTransactionsResponse::default();
        if !self.leader() {
            response.error_code = NOT_COORDINATOR;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        if !self.allows(caller, CLUSTER, "kafka-cluster", DESCRIBE) {
            response.error_code = CLUSTER_AUTH;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let unknown: Vec<_> = request
            .state_filters
            .iter()
            .filter(|state| !txn_state(state.as_str()))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            response.error_code = INVALID_REQUEST;
            response.unknown_state_filters = unknown;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let pattern = request
            .transactional_id_pattern
            .as_ref()
            .map(|value| value.to_string())
            .filter(|value| !value.is_empty());
        let pattern = match pattern {
            Some(value) => match regex::Regex::new(&value) {
                Ok(pattern) => Some(pattern),
                Err(_) => {
                    response.error_code = INVALID_REQUEST;
                    return frame_of(header.request_api_version, header.correlation_id, &response);
                }
            },
            None => None,
        };
        let Ok(rows) = self.node.scan_local("txn/") else {
            response.error_code = NOT_COORDINATOR;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        };
        let states: Vec<String> = request
            .state_filters
            .iter()
            .map(|state| state.to_string())
            .collect();
        let producers: Vec<i64> = request.producer_id_filters.iter().map(|id| id.0).collect();
        let now = now_ms();
        for (key, raw) in rows {
            let Some(id) = key.strip_prefix("txn/") else {
                continue;
            };
            let Ok(info) = txn_inspect(id, &raw) else {
                continue;
            };
            if !states.is_empty() && !states.iter().any(|state| state == &info.state) {
                continue;
            }
            if !producers.is_empty() && !producers.contains(&info.producer_id) {
                continue;
            }
            if request.duration_filter >= 0
                && (info.state != "Ongoing"
                    || (info.started_ms >= 0
                        && now.saturating_sub(info.started_ms) < request.duration_filter))
            {
                continue;
            }
            if pattern
                .as_ref()
                .is_some_and(|pattern| !pattern.is_match(id))
            {
                continue;
            }
            let mut row =
                camus_protocol::messages::list_transactions_response::TransactionState::default();
            row.transactional_id = TransactionalId(StrBytes::from_string(info.transactional_id));
            row.producer_id = info.producer_id.into();
            row.transaction_state = StrBytes::from_string(info.state);
            response.transaction_states.push(row);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

pub(super) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_millis() as i64)
        .unwrap_or(0)
}

fn fill_described(state: &mut TransactionState, info: &TxnInfo) {
    state.transaction_state = StrBytes::from_string(info.state.clone());
    state.transaction_timeout_ms = info.timeout_ms;
    state.transaction_start_time_ms = info.started_ms;
    state.producer_id = info.producer_id.into();
    state.producer_epoch = info.epoch;
    let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in &info.partitions {
        topics.entry(topic.clone()).or_default().push(*partition);
    }
    state.topics = topics
        .into_iter()
        .map(|(topic, partitions)| {
            let mut row = TopicData::default();
            row.topic = TopicName(StrBytes::from_string(topic));
            row.partitions = partitions;
            row
        })
        .collect();
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
