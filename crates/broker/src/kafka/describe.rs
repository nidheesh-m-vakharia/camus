//! Describe APIs that operations tools call: ConsumerGroupDescribe (69),
//! DescribeTopicPartitions (75), DescribeProducers (61), and DescribeLogDirs
//! (35). Each follows Kafka 4.4.0's `KafkaApis` for authorization and errors.

use bytes::Bytes;
use camus_metadata::Topic;
use camus_protocol::messages::consumer_group_describe_response::{
    Assignment, DescribedGroup, Member, TopicPartitions,
};
use camus_protocol::messages::describe_log_dirs_response::{
    DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
};
use camus_protocol::messages::describe_producers_response::{
    PartitionResponse, ProducerState, TopicResponse,
};
use camus_protocol::messages::describe_topic_partitions_response::{
    Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
};
use camus_protocol::messages::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeProducersRequest, DescribeProducersResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, ProducerId, RequestHeader,
    TopicName,
};
use camus_protocol::protocol::{Decodable, StrBytes};
use uuid::Uuid;

use super::{auth, frame_of, topic_name, Caller, Kafka, Reply};

const NOT_LEADER: i16 = 6;
const UNKNOWN_TOPIC: i16 = 3;
const GROUP_ID_NOT_FOUND: i16 = 69;
/// KIP-848 member type in ConsumerGroupDescribe v1.
const CONSUMER_MEMBER: i8 = 1;

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

impl Kafka {
    pub(super) async fn consumer_group_describe(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = ConsumerGroupDescribeRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        let gate = self.coordinating().await.err().unwrap_or(0);
        let mut response = ConsumerGroupDescribeResponse::default();
        for id in request.group_ids {
            let mut out = DescribedGroup::default();
            out.group_id = id.clone();
            out.authorized_operations = i32::MIN;
            let name = id.0.to_string();
            if !self.allows(caller, auth::GROUP, &name, auth::DESCRIBE) {
                out.error_code = auth::GROUP_FAILED;
            } else if gate != 0 {
                out.error_code = gate;
            } else if let Some(group) = self.coordinator.describe_consumer(&name) {
                out.group_state = text(&group.state_label);
                out.group_epoch = group.group_epoch;
                out.assignment_epoch = group.assignment_epoch;
                out.assignor_name = text(&group.assignor);
                out.members =
                    group
                        .members
                        .into_iter()
                        .map(|(record, target)| {
                            let mut member = Member::default();
                            member.member_id = text(&record.member_id);
                            member.instance_id = record.group_instance_id.as_deref().map(text);
                            member.rack_id = record.rack_id.as_deref().map(text);
                            member.member_epoch = record.member_epoch;
                            member.client_id = text(&record.client_id);
                            member.client_host = text(&record.client_host);
                            member.subscribed_topic_names = record
                                .subscribed_topics
                                .iter()
                                .map(|topic| topic_name(topic))
                                .collect();
                            member.subscribed_topic_regex =
                                record.subscribed_regex.as_deref().map(text);
                            member.assignment =
                                self.assignment(record.assigned.iter().map(|(id, parts)| {
                                    (id.clone(), parts.keys().copied().collect())
                                }));
                            member.target_assignment = self.assignment(target.into_iter());
                            member.member_type = CONSUMER_MEMBER;
                            member
                        })
                        .collect();
            } else {
                out.error_code = GROUP_ID_NOT_FOUND;
                out.error_message = Some(text(&format!(
                    "Group {name} is not a consumer group or does not exist."
                )));
            }
            response.groups.push(out);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn assignment(&self, topics: impl Iterator<Item = (String, Vec<i32>)>) -> Assignment {
        let mut out = Assignment::default();
        for (id, mut partitions) in topics {
            let Ok(uuid) = Uuid::parse_str(&id) else {
                continue;
            };
            partitions.sort_unstable();
            let mut row = TopicPartitions::default();
            row.topic_id = uuid;
            row.topic_name = topic_name(&self.topic_name_by_id(&uuid).unwrap_or_default());
            row.partitions = partitions;
            out.topic_partitions.push(row);
        }
        out
    }

    pub(super) async fn describe_topic_partitions(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DescribeTopicPartitionsRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        let leader = self
            .node
            .raft
            .metrics()
            .borrow()
            .current_leader
            .map(|id| id as i32);
        // Named topics are reported even when denied or missing; a listing
        // leaves out what the caller may not describe, as Metadata does.
        let listing = request.topics.is_empty();
        let mut names: Vec<String> = if listing {
            self.topic_names()
        } else {
            request
                .topics
                .iter()
                .map(|topic| topic.name.0.to_string())
                .collect()
        };
        names.sort();
        names.dedup();
        let (start_topic, start_partition) = request
            .cursor
            .as_ref()
            .map(|cursor| (cursor.topic_name.0.to_string(), cursor.partition_index))
            .unwrap_or_default();
        let limit = usize::try_from(request.response_partition_limit.max(1)).unwrap_or(2000);
        let mut response = DescribeTopicPartitionsResponse::default();
        let mut budget = limit;
        for name in names.into_iter().filter(|name| *name >= start_topic) {
            let mut out = DescribeTopicPartitionsResponseTopic::default();
            out.name = Some(topic_name(&name));
            out.topic_authorized_operations = i32::MIN;
            if !self.allows(caller, auth::TOPIC, &name, auth::DESCRIBE) {
                if listing {
                    continue;
                }
                out.error_code = auth::TOPIC_FAILED;
                response.topics.push(out);
                continue;
            }
            let described = self.metadata_topic(&name, leader);
            out.error_code = described.error_code;
            out.topic_id = described.topic_id;
            let first = if name == start_topic {
                start_partition
            } else {
                0
            };
            for partition in described
                .partitions
                .into_iter()
                .filter(|partition| partition.partition_index >= first)
            {
                if budget == 0 {
                    let mut cursor = Cursor::default();
                    cursor.topic_name = topic_name(&name);
                    cursor.partition_index = partition.partition_index;
                    response.next_cursor = Some(cursor);
                    response.topics.push(out);
                    return frame_of(header.request_api_version, header.correlation_id, &response);
                }
                budget -= 1;
                let mut row = DescribeTopicPartitionsResponsePartition::default();
                row.error_code = partition.error_code;
                row.partition_index = partition.partition_index;
                row.leader_id = partition.leader_id;
                row.leader_epoch = partition.leader_epoch;
                row.replica_nodes = partition.replica_nodes;
                row.isr_nodes = partition.isr_nodes;
                row.eligible_leader_replicas = Some(Vec::new());
                row.last_known_elr = Some(Vec::new());
                row.offline_replicas = partition.offline_replicas;
                out.partitions.push(row);
            }
            response.topics.push(out);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn describe_producers(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DescribeProducersRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = DescribeProducersResponse::default();
        for topic in request.topics {
            let name = topic.name.0.to_string();
            let stored = self.topic(&name);
            let allowed = self.allows(caller, auth::TOPIC, &name, auth::READ);
            let mut out = TopicResponse::default();
            out.name = topic.name.clone();
            for partition in topic.partition_indexes {
                let mut row = PartitionResponse::default();
                row.partition_index = partition;
                match &stored {
                    _ if !allowed => row.error_code = auth::TOPIC_FAILED,
                    None => row.error_code = UNKNOWN_TOPIC,
                    Some(found) if partition < 0 || partition >= found.partitions => {
                        row.error_code = UNKNOWN_TOPIC;
                    }
                    Some(found) if !self.leads_partition(found, partition) => {
                        row.error_code = NOT_LEADER;
                    }
                    Some(found) => row.active_producers = self.active_producers(found, partition),
                }
                out.partitions.push(row);
            }
            response.topics.push(out);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    /// Producers with sequence state on a partition, from metadata. A
    /// producer with an ongoing transaction reports where it started here.
    fn active_producers(&self, topic: &Topic, partition: i32) -> Vec<ProducerState> {
        let prefix = format!("seq/{}/{partition}/", topic.name);
        let rows = self.node.scan_local(&prefix).unwrap_or_default();
        rows.into_iter()
            .filter_map(|(key, value)| {
                let producer_id: i64 = key.strip_prefix(&prefix)?.parse().ok()?;
                let state: camus_protocol::idempotence::ProducerSeq =
                    serde_json::from_str(&value).ok()?;
                let last = state.batches.last()?;
                let mut row = ProducerState::default();
                row.producer_id = ProducerId(producer_id);
                row.producer_epoch = i32::from(state.epoch);
                row.last_sequence = last.last_sequence;
                row.last_timestamp = -1;
                row.coordinator_epoch = -1;
                row.current_txn_start_offset = self
                    .txn_start(producer_id, &topic.name, partition)
                    .unwrap_or(-1);
                Some(row)
            })
            .collect()
    }

    /// The first offset of `producer_id`'s ongoing transaction on a partition.
    fn txn_start(&self, producer_id: i64, topic: &str, partition: i32) -> Option<i64> {
        let id = self
            .node
            .read_local(&format!("txn-by-producer/{producer_id}"))
            .ok()??;
        let raw = self.node.read_local(&format!("txn/{id}")).ok()??;
        camus_metadata::txn_spans(&raw)
            .ok()?
            .into_iter()
            .filter(|span| span.topic == topic && span.partition == partition)
            .map(|span| span.start)
            .min()
    }

    pub(super) async fn describe_log_dirs(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DescribeLogDirsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = DescribeLogDirsResponse::default();
        if !self.allows(caller, auth::CLUSTER, auth::CLUSTER_NAME, auth::DESCRIBE) {
            response.error_code = auth::CLUSTER_FAILED;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        // One log directory per broker: the bucket. Each broker reports the
        // partitions it leads, so a tool that sums brokers counts each once.
        let mut dir = DescribeLogDirsResult::default();
        dir.log_dir = text(&self.log_dir());
        dir.total_bytes = -1;
        dir.usable_bytes = -1;
        let wanted: Vec<(String, Option<Vec<i32>>)> = match request.topics {
            None => self
                .topic_names()
                .into_iter()
                .map(|name| (name, None))
                .collect(),
            Some(topics) => topics
                .into_iter()
                .map(|topic| (topic.topic.0.to_string(), Some(topic.partitions)))
                .collect(),
        };
        for (name, partitions) in wanted {
            let Some(topic) = self.topic(&name) else {
                continue;
            };
            let indexes: Vec<i32> = partitions
                .unwrap_or_else(|| (0..topic.partitions).collect())
                .into_iter()
                .filter(|partition| self.leads_partition(&topic, *partition))
                .collect();
            let mut out = DescribeLogDirsTopic::default();
            out.name = TopicName(text(&name));
            for partition in indexes {
                let Some(state) = self.partition_state(&topic, partition) else {
                    continue;
                };
                let mut row = DescribeLogDirsPartition::default();
                row.partition_index = partition;
                row.partition_size = self
                    .all_extents(&topic, partition, &state)
                    .map(|extents| extents.iter().map(|extent| i64::from(extent.length)).sum())
                    .unwrap_or(0);
                out.partitions.push(row);
            }
            if !out.partitions.is_empty() {
                dir.topics.push(out);
            }
        }
        response.results.push(dir);
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}
