//! Phase 4 admin APIs.
//!
//! CreatePartitions, AlterConfigs, and IncrementalAlterConfigs run on the
//! metadata leader. Another broker answers `NOT_CONTROLLER`. DeleteRecords
//! runs on the partition leader, which forwards the log-start change.
//! OffsetDelete runs on the group coordinator. DescribeCluster is answered
//! by any broker.

use bytes::Bytes;
use camus_metadata::{
    Command, ConfigChange, ConfigEdit, Item, OffsetDrop, PartitionIncrease, RecordCut, Topic,
};
use camus_protocol::messages::alter_configs_response::AlterConfigsResponse as AlterResponse;
use camus_protocol::messages::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use camus_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use camus_protocol::messages::describe_cluster_response::{
    DescribeClusterBroker, DescribeClusterResponse,
};
use camus_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use camus_protocol::messages::offset_delete_response::{
    OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use camus_protocol::messages::{
    AlterConfigsRequest, BrokerId, CreatePartitionsRequest, DeleteRecordsRequest,
    DescribeClusterRequest, IncrementalAlterConfigsRequest, OffsetDeleteRequest,
};
use camus_protocol::protocol::{Decodable, StrBytes};

use super::{frame_of, Kafka, Reply};

const NOT_LEADER: i16 = 6;
const NOT_COORDINATOR: i16 = 16;
const NOT_CONTROLLER: i16 = 41;

impl Kafka {
    pub(super) fn describe_cluster(&self, header: &super::RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = DescribeClusterRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = DescribeClusterResponse::default();
        response.endpoint_type = request.endpoint_type;
        response.cluster_id = StrBytes::from_static_str("camus");
        let leader = self
            .node
            .raft
            .metrics()
            .borrow()
            .current_leader
            .map(|id| id as i32);
        response.controller_id = BrokerId(leader.unwrap_or(-1));
        response.brokers = self
            .broker_list()
            .into_iter()
            .map(|broker| {
                let mut row = DescribeClusterBroker::default();
                row.broker_id = broker.node_id;
                row.host = broker.host;
                row.port = broker.port;
                row.rack = broker.rack;
                row
            })
            .collect();
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn create_partitions(
        &self,
        header: &super::RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = CreatePartitionsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if !self.leader() {
            return self.partition_error_response(&request, header, NOT_CONTROLLER);
        }
        let topics: Vec<PartitionIncrease> = request
            .topics
            .iter()
            .map(|topic| PartitionIncrease {
                name: topic.name.0.to_string(),
                count: topic.count,
                assigned: topic
                    .assignments
                    .as_ref()
                    .is_some_and(|rows| !rows.is_empty()),
            })
            .collect();
        let applied = self.node.apply(Command::CreatePartitions { topics }).await;
        let items = items_of(applied.ok());
        let mut response = CreatePartitionsResponse::default();
        for (topic, item) in request.topics.iter().zip(items_or(&request.topics, items)) {
            let mut row = CreatePartitionsTopicResult::default();
            row.name = topic.name.clone();
            row.error_code = item.code;
            response.results.push(row);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn partition_error_response(
        &self,
        request: &CreatePartitionsRequest,
        header: &super::RequestHeader,
        code: i16,
    ) -> Reply {
        let mut response = CreatePartitionsResponse::default();
        for topic in &request.topics {
            let mut row = CreatePartitionsTopicResult::default();
            row.name = topic.name.clone();
            row.error_code = code;
            response.results.push(row);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn alter_configs(
        &self,
        header: &super::RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = AlterConfigsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if !self.leader() {
            return configs_error(&request, header, NOT_CONTROLLER, false);
        }
        let changes = request
            .resources
            .iter()
            .map(|resource| ConfigChange {
                resource_type: resource.resource_type,
                name: resource.resource_name.to_string(),
                validate_only: request.validate_only,
                edits: resource
                    .configs
                    .iter()
                    .map(|config| ConfigEdit {
                        name: config.name.to_string(),
                        op: 0,
                        value: config.value.as_ref().map(|value| value.to_string()),
                    })
                    .collect(),
                replace: true,
            })
            .collect::<Vec<_>>();
        let messages = self.config_messages(&changes);
        let applied = self.node.apply(Command::AlterConfigs { changes }).await;
        configs_from(&request, header, items_of(applied.ok()), messages)
    }

    /// Why each change would be rejected, checked against this broker's copy
    /// of the topic. The metadata group returns only the code.
    fn config_messages(&self, changes: &[ConfigChange]) -> Vec<Option<String>> {
        changes
            .iter()
            .map(|change| {
                let topic = self.topic(&change.name)?;
                camus_metadata::config::apply_change(&topic, &change.edits, change.replace)
                    .err()
                    .map(|rejected| rejected.message)
            })
            .collect()
    }

    pub(super) async fn incremental_alter_configs(
        &self,
        header: &super::RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = IncrementalAlterConfigsRequest::decode(buf, header.request_api_version)
        else {
            return Reply::Close;
        };
        if !self.leader() {
            return incremental_error(&request, header, NOT_CONTROLLER);
        }
        let changes = request
            .resources
            .iter()
            .map(|resource| ConfigChange {
                resource_type: resource.resource_type,
                name: resource.resource_name.to_string(),
                validate_only: request.validate_only,
                edits: resource
                    .configs
                    .iter()
                    .map(|config| ConfigEdit {
                        name: config.name.to_string(),
                        op: config.config_operation,
                        value: config.value.as_ref().map(|value| value.to_string()),
                    })
                    .collect(),
                replace: false,
            })
            .collect::<Vec<_>>();
        let messages = self.config_messages(&changes);
        let applied = self.node.apply(Command::AlterConfigs { changes }).await;
        let items = items_or(&request.resources, items_of(applied.ok()));
        let mut response = IncrementalAlterConfigsResponse::default();
        for ((resource, item), message) in request.resources.iter().zip(items).zip(messages) {
            let mut row = camus_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse::default();
            row.resource_type = resource.resource_type;
            row.resource_name = resource.resource_name.clone();
            row.error_code = item.code;
            if item.code == camus_metadata::config::INVALID_CONFIG {
                row.error_message = message.map(StrBytes::from_string);
            }
            response.responses.push(row);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn delete_records(
        &self,
        header: &super::RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = DeleteRecordsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut cuts = Vec::new();
        let mut local: Vec<Option<Item>> = Vec::new();
        for topic in &request.topics {
            let stored = self.topic(topic.name.0.as_str());
            let led = stored.as_ref().is_some_and(|topic| self.leads_topic(topic));
            for partition in &topic.partitions {
                if stored.is_none() {
                    local.push(Some(Item { code: 3, low: -1 }));
                    continue;
                }
                if !led {
                    local.push(Some(Item {
                        code: NOT_LEADER,
                        low: -1,
                    }));
                    continue;
                }
                local.push(None);
                cuts.push(RecordCut {
                    topic: topic.name.0.to_string(),
                    partition: partition.partition_index,
                    offset: partition.offset,
                });
            }
        }
        let applied = if cuts.is_empty() {
            Vec::new()
        } else {
            match self.meta_apply(Command::DeleteRecords { cuts }).await {
                Ok(response) => items_of(Some(response)),
                Err(_) => {
                    return records_error(&request, header, NOT_LEADER);
                }
            }
        };
        let mut applied = applied.into_iter();
        let mut response = DeleteRecordsResponse::default();
        let mut cursor = local.into_iter();
        for topic in &request.topics {
            let mut row = DeleteRecordsTopicResult::default();
            row.name = topic.name.clone();
            for partition in &topic.partitions {
                let item = match cursor.next().flatten() {
                    Some(item) => item,
                    None => applied.next().unwrap_or(Item { code: 1, low: -1 }),
                };
                let mut part = DeleteRecordsPartitionResult::default();
                part.partition_index = partition.partition_index;
                part.low_watermark = item.low;
                part.error_code = item.code;
                row.partitions.push(part);
            }
            response.topics.push(row);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    fn leads_topic(&self, topic: &Topic) -> bool {
        if topic.durability == "wal" {
            self.wal().is_leader()
        } else {
            self.leader()
        }
    }

    pub(super) async fn offset_delete(
        &self,
        header: &super::RequestHeader,
        buf: &mut Bytes,
    ) -> Reply {
        let Ok(request) = OffsetDeleteRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        if !self.leader() {
            let mut response = OffsetDeleteResponse::default();
            response.error_code = NOT_COORDINATOR;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let forget = OffsetDrop {
            group: request.group_id.0.to_string(),
            topics: request
                .topics
                .iter()
                .map(|topic| {
                    (
                        topic.name.0.to_string(),
                        topic
                            .partitions
                            .iter()
                            .map(|partition| partition.partition_index)
                            .collect(),
                    )
                })
                .collect(),
        };
        let applied = self.node.apply(Command::OffsetDelete { forget }).await;
        let mut items = items_of(applied.ok()).into_iter();
        let mut response = OffsetDeleteResponse::default();
        let mut missing = false;
        for topic in &request.topics {
            let mut row = OffsetDeleteResponseTopic::default();
            row.name = topic.name.clone();
            for partition in &topic.partitions {
                let item = items.next().unwrap_or(Item { code: 69, low: 0 });
                if item.code == 69 {
                    missing = true;
                }
                let mut part = OffsetDeleteResponsePartition::default();
                part.partition_index = partition.partition_index;
                part.error_code = item.code;
                row.partitions.push(part);
            }
            response.topics.push(row);
        }
        if missing {
            response.error_code = 69;
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

fn items_of(response: Option<camus_metadata::Response>) -> Vec<Item> {
    let Some(response) = response else {
        return Vec::new();
    };
    if response.error_code != 0 {
        return Vec::new();
    }
    response
        .value
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default()
}

fn items_or<T>(rows: &[T], items: Vec<Item>) -> Vec<Item> {
    if items.len() == rows.len() {
        items
    } else {
        rows.iter().map(|_| Item { code: 6, low: 0 }).collect()
    }
}

fn configs_error(
    request: &AlterConfigsRequest,
    header: &super::RequestHeader,
    code: i16,
    _incremental: bool,
) -> Reply {
    let mut response = AlterResponse::default();
    for resource in &request.resources {
        let mut row =
            camus_protocol::messages::alter_configs_response::AlterConfigsResourceResponse::default(
            );
        row.resource_type = resource.resource_type;
        row.resource_name = resource.resource_name.clone();
        row.error_code = code;
        response.responses.push(row);
    }
    frame_of(header.request_api_version, header.correlation_id, &response)
}

fn configs_from(
    request: &AlterConfigsRequest,
    header: &super::RequestHeader,
    items: Vec<Item>,
    messages: Vec<Option<String>>,
) -> Reply {
    let items = items_or(&request.resources, items);
    let mut response = AlterResponse::default();
    for ((resource, item), message) in request.resources.iter().zip(items).zip(messages) {
        let mut row =
            camus_protocol::messages::alter_configs_response::AlterConfigsResourceResponse::default(
            );
        row.resource_type = resource.resource_type;
        row.resource_name = resource.resource_name.clone();
        row.error_code = item.code;
        if item.code == camus_metadata::config::INVALID_CONFIG {
            row.error_message = message.map(StrBytes::from_string);
        }
        response.responses.push(row);
    }
    frame_of(header.request_api_version, header.correlation_id, &response)
}

fn incremental_error(
    request: &IncrementalAlterConfigsRequest,
    header: &super::RequestHeader,
    code: i16,
) -> Reply {
    let mut response = IncrementalAlterConfigsResponse::default();
    for resource in &request.resources {
        let mut row = camus_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse::default();
        row.resource_type = resource.resource_type;
        row.resource_name = resource.resource_name.clone();
        row.error_code = code;
        response.responses.push(row);
    }
    frame_of(header.request_api_version, header.correlation_id, &response)
}

fn records_error(
    request: &DeleteRecordsRequest,
    header: &super::RequestHeader,
    code: i16,
) -> Reply {
    let mut response = DeleteRecordsResponse::default();
    for topic in &request.topics {
        let mut row = DeleteRecordsTopicResult::default();
        row.name = topic.name.clone();
        for partition in &topic.partitions {
            let mut part = DeleteRecordsPartitionResult::default();
            part.partition_index = partition.partition_index;
            part.low_watermark = -1;
            part.error_code = code;
            row.partitions.push(part);
        }
        response.topics.push(row);
    }
    frame_of(header.request_api_version, header.correlation_id, &response)
}
