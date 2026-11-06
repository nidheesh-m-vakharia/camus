//! Consumer group APIs (Phase 3): FindCoordinator, JoinGroup, SyncGroup,
//! Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch, ListGroups,
//! DescribeGroups, DeleteGroups.
//!
//! The metadata leader is the coordinator for every group. Another broker
//! answers NOT_COORDINATOR, and the client finds the coordinator again.
//! A broker that becomes leader loads the stored group records after a
//! linearizable read, so it sees every group write the old leader committed.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use camus_coordinator::{
    codes, Coordinator, JoinRequest, JoinResponse, Leaving, Store, SyncResponse,
};
use camus_metadata::groups::{self, GroupRecord, OffsetCommit, OffsetRecord};
use camus_metadata::{Command, MetadataNode};
use camus_protocol::messages::delete_groups_response::DeletableGroupResult;
use camus_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use camus_protocol::messages::find_coordinator_response::Coordinator as FoundCoordinator;
use camus_protocol::messages::join_group_response::JoinGroupResponseMember;
use camus_protocol::messages::leave_group_response::MemberResponse;
use camus_protocol::messages::list_groups_response::ListedGroup;
use camus_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use camus_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use camus_protocol::messages::{
    BrokerId, DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, RequestHeader, SyncGroupRequest,
    SyncGroupResponse,
};
use camus_protocol::protocol::{Decodable, StrBytes};
use uuid::Uuid;

use super::{frame_of, topic_name, Kafka, Reply};

const INVALID_REQUEST: i16 = 42;
const UNKNOWN_TOPIC: i16 = 3;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;

/// Group records go through the metadata group.
pub struct MetadataStore {
    pub node: Arc<MetadataNode>,
}

impl Store for MetadataStore {
    fn save(&self, group: String, record: GroupRecord) -> camus_coordinator::SaveFuture {
        let node = Arc::clone(&self.node);
        Box::pin(async move {
            match node.group_write(Command::SaveGroup { group, record }).await {
                Ok(0) => Ok(()),
                Ok(code) => Err(code),
                Err(_) => Err(codes::NOT_COORDINATOR),
            }
        })
    }
}

/// Coordinator settings from the environment, for tests. Kafka's defaults
/// otherwise.
pub fn coordinator_config() -> camus_coordinator::Config {
    let mut config = camus_coordinator::Config::default();
    let ms = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(std::time::Duration::from_millis)
    };
    if let Some(delay) = ms("CAMUS_GROUP_INITIAL_DELAY_MS") {
        config.initial_rebalance_delay = delay;
    }
    if let Some(min) = ms("CAMUS_GROUP_MIN_SESSION_MS") {
        config.min_session_timeout = min;
    }
    config
}

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

impl Kafka {
    /// Whether this broker coordinates groups now. Loads the stored groups
    /// the first time it does in a term. `Err` is the code to answer with.
    pub(super) async fn coordinating(&self) -> Result<(), i16> {
        if !self.leader() {
            self.lost_coordination();
            return Err(codes::NOT_COORDINATOR);
        }
        let term = self.node.raft.metrics().borrow().current_term;
        let mut loaded = self.coordinator_term.lock().await;
        if *loaded == Some(term) {
            return Ok(());
        }
        // Everything the old leader committed must be applied here first.
        if self.node.linearizable().await.is_err() {
            return Err(COORDINATOR_LOAD_IN_PROGRESS);
        }
        let rows = self
            .node
            .scan_local(groups::group_prefix())
            .map_err(|_| COORDINATOR_LOAD_IN_PROGRESS)?;
        let records = rows
            .into_iter()
            .filter_map(|(key, value)| {
                let group = groups::group_from_key(&key)?;
                let record: GroupRecord = serde_json::from_str(&value).ok()?;
                Some((group, record))
            })
            .collect();
        self.coordinator.load(records, Instant::now());
        *loaded = Some(term);
        Ok(())
    }

    /// Called when this broker is not the metadata leader: forget groups so
    /// a later term reloads them.
    pub(super) fn lost_coordination(&self) {
        if let Ok(mut loaded) = self.coordinator_term.try_lock() {
            if loaded.take().is_some() {
                self.coordinator.unload();
            }
        }
    }

    pub(super) async fn find_coordinator(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = FindCoordinatorRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let keys: Vec<String> = if version >= 4 {
            request
                .coordinator_keys
                .iter()
                .map(|key| key.to_string())
                .collect()
        } else {
            vec![request.key.to_string()]
        };
        let leader = self.node.raft.metrics().borrow().current_leader;
        let address = leader.and_then(|id| {
            let raw = self.node.read_local(&format!("broker/{id}")).ok()??;
            let (host, port) = raw.rsplit_once(':')?;
            Some((id as i32, host.to_string(), port.parse::<i32>().ok()?))
        });
        let answer = |key: &str| -> (i16, Option<String>, i32, String, i32) {
            if request.key_type != 0 {
                return (
                    INVALID_REQUEST,
                    Some("only consumer groups have a coordinator yet".into()),
                    -1,
                    String::new(),
                    -1,
                );
            }
            if key.is_empty() {
                return (codes::INVALID_GROUP_ID, None, -1, String::new(), -1);
            }
            match &address {
                Some((id, host, port)) => (0, None, *id, host.clone(), *port),
                None => (
                    codes::COORDINATOR_NOT_AVAILABLE,
                    None,
                    -1,
                    String::new(),
                    -1,
                ),
            }
        };
        let mut response = FindCoordinatorResponse::default();
        if version >= 4 {
            for key in keys {
                let (error, message, id, host, port) = answer(&key);
                let mut found = FoundCoordinator::default();
                found.key = text(&key);
                found.error_code = error;
                found.error_message = message.map(|value| text(&value));
                found.node_id = BrokerId(id);
                found.host = text(&host);
                found.port = port;
                response.coordinators.push(found);
            }
        } else {
            let (error, message, id, host, port) = answer(&keys[0]);
            response.error_code = error;
            response.error_message = message.map(|value| text(&value));
            response.node_id = BrokerId(id);
            response.host = text(&host);
            response.port = port;
        }
        frame_of(version, header.correlation_id, &response)
    }

    pub(super) async fn join_group(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = JoinGroupRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let group = request.group_id.0.to_string();
        let answer = match self.coordinating().await {
            Err(code) => JoinResponse::error(code, request.member_id.as_str()),
            Ok(()) => {
                let join = JoinRequest {
                    group,
                    member_id: request.member_id.to_string(),
                    group_instance_id: request.group_instance_id.map(|id| id.to_string()),
                    client_id: header
                        .client_id
                        .as_ref()
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    client_host: String::from("/unknown"),
                    protocol_type: request.protocol_type.to_string(),
                    protocols: request
                        .protocols
                        .iter()
                        .map(|protocol| (protocol.name.to_string(), protocol.metadata.to_vec()))
                        .collect(),
                    session_timeout_ms: request.session_timeout_ms,
                    // v0 has no rebalance timeout; Kafka uses the session's.
                    rebalance_timeout_ms: if version >= 1 {
                        request.rebalance_timeout_ms
                    } else {
                        request.session_timeout_ms
                    },
                    require_known_member_id: version >= 4,
                    supports_skip_assignment: version >= 9,
                };
                let member = join.member_id.clone();
                self.coordinator
                    .join(join, Instant::now())
                    .wait(|| JoinResponse::error(codes::NOT_COORDINATOR, &member))
                    .await
            }
        };
        let mut response = JoinGroupResponse::default();
        response.error_code = answer.error;
        response.generation_id = answer.generation;
        response.protocol_type = answer.protocol_type.map(|value| text(&value));
        response.protocol_name = Some(text(&answer.protocol_name.unwrap_or_default()));
        response.leader = text(&answer.leader);
        response.member_id = text(&answer.member_id);
        response.skip_assignment = answer.skip_assignment;
        response.members = answer
            .members
            .into_iter()
            .map(|member| {
                let mut out = JoinGroupResponseMember::default();
                out.member_id = text(&member.member_id);
                out.group_instance_id = member.group_instance_id.map(|id| text(&id));
                out.metadata = Bytes::from(member.metadata);
                out
            })
            .collect();
        frame_of(version, header.correlation_id, &response)
    }

    pub(super) async fn sync_group(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = SyncGroupRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let answer = match self.coordinating().await {
            Err(code) => SyncResponse::error(code),
            Ok(()) => {
                let assignments = request
                    .assignments
                    .iter()
                    .map(|given| (given.member_id.to_string(), given.assignment.to_vec()))
                    .collect();
                self.coordinator
                    .sync(
                        request.group_id.0.as_str(),
                        request.member_id.as_str(),
                        request.group_instance_id.as_ref().map(|id| id.as_str()),
                        request.generation_id,
                        request.protocol_type.as_ref().map(|value| value.as_str()),
                        request.protocol_name.as_ref().map(|value| value.as_str()),
                        assignments,
                        Instant::now(),
                    )
                    .wait(|| SyncResponse::error(codes::NOT_COORDINATOR))
                    .await
            }
        };
        let mut response = SyncGroupResponse::default();
        response.error_code = answer.error;
        response.protocol_type = answer.protocol_type.map(|value| text(&value));
        response.protocol_name = answer.protocol_name.map(|value| text(&value));
        response.assignment = Bytes::from(answer.assignment);
        frame_of(version, header.correlation_id, &response)
    }

    pub(super) async fn heartbeat(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = HeartbeatRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let code = match self.coordinating().await {
            Err(code) => code,
            Ok(()) => self.coordinator.heartbeat(
                request.group_id.0.as_str(),
                request.member_id.as_str(),
                request.group_instance_id.as_ref().map(|id| id.as_str()),
                request.generation_id,
                Instant::now(),
            ),
        };
        let mut response = HeartbeatResponse::default();
        response.error_code = code;
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn leave_group(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = LeaveGroupRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let group = request.group_id.0.to_string();
        let leaving: Vec<Leaving> = if version >= 3 {
            request
                .members
                .iter()
                .map(|member| Leaving {
                    member_id: member.member_id.to_string(),
                    group_instance_id: member.group_instance_id.as_ref().map(|id| id.to_string()),
                })
                .collect()
        } else {
            vec![Leaving {
                member_id: request.member_id.to_string(),
                group_instance_id: None,
            }]
        };
        let mut response = LeaveGroupResponse::default();
        let answered = match self.coordinating().await {
            Err(code) => Err(code),
            Ok(()) => self.coordinator.leave(&group, &leaving, Instant::now()),
        };
        match answered {
            Err(code) => response.error_code = code,
            Ok(codes_seen) => {
                if version < 3 {
                    response.error_code = codes_seen.first().copied().unwrap_or(0);
                } else {
                    for (one, code) in leaving.into_iter().zip(codes_seen) {
                        let mut out = MemberResponse::default();
                        out.member_id = text(&one.member_id);
                        out.group_instance_id = one.group_instance_id.map(|id| text(&id));
                        out.error_code = code;
                        response.members.push(out);
                    }
                }
            }
        }
        frame_of(version, header.correlation_id, &response)
    }

    pub(super) async fn offset_commit(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = OffsetCommitRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let group = request.group_id.0.to_string();
        let generation = request.generation_id_or_member_epoch;
        let member_id = request.member_id.to_string();
        let gate = match self.coordinating().await {
            Err(code) => code,
            Ok(()) => self.coordinator.check_commit(
                &group,
                &member_id,
                request.group_instance_id.as_ref().map(|id| id.as_str()),
                generation,
            ),
        };
        // Resolve each topic to its id; unknown topics and partitions fail
        // on their own without failing the rest.
        let mut response = OffsetCommitResponse::default();
        let mut commits = Vec::new();
        let mut slots: Vec<(usize, usize)> = Vec::new();
        let now = now_ms();
        for topic in &request.topics {
            let stored = if version >= 10 {
                self.topic_name_by_id(&topic.topic_id)
                    .and_then(|name| self.topic(&name))
            } else {
                self.topic(topic.name.0.as_str())
            };
            let mut out = OffsetCommitResponseTopic::default();
            out.name = topic.name.clone();
            out.topic_id = topic.topic_id;
            let topic_index = response.topics.len();
            for partition in &topic.partitions {
                let mut row = OffsetCommitResponsePartition::default();
                row.partition_index = partition.partition_index;
                match &stored {
                    _ if gate != 0 => row.error_code = gate,
                    Some(found)
                        if partition.partition_index >= 0
                            && partition.partition_index < found.partitions =>
                    {
                        slots.push((topic_index, out.partitions.len()));
                        commits.push(OffsetCommit {
                            topic_id: found.id.clone(),
                            partition: partition.partition_index,
                            record: OffsetRecord {
                                offset: partition.committed_offset,
                                leader_epoch: partition.committed_leader_epoch,
                                metadata: partition
                                    .committed_metadata
                                    .as_ref()
                                    .map(|value| value.to_string()),
                                commit_timestamp: now,
                            },
                        });
                    }
                    _ => row.error_code = UNKNOWN_TOPIC,
                }
                out.partitions.push(row);
            }
            response.topics.push(out);
        }
        if !commits.is_empty() {
            let code = match self
                .node
                .group_write(Command::CommitOffsets {
                    group: group.clone(),
                    generation,
                    member_id,
                    offsets: commits,
                })
                .await
            {
                Ok(code) => code,
                Err(_) => codes::NOT_COORDINATOR,
            };
            if code == 0 && generation < 0 {
                self.coordinator.touch(&group);
            }
            for (topic, partition) in slots {
                response.topics[topic].partitions[partition].error_code = code;
            }
        }
        frame_of(version, header.correlation_id, &response)
    }

    /// Committed offsets of one group: `(topic id, partition) -> record`.
    fn stored_offsets(&self, group: &str) -> Vec<(String, i32, OffsetRecord)> {
        self.node
            .scan_local(&groups::offset_prefix(group))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(key, value)| {
                let (topic_id, partition) = groups::offset_from_key(group, &key)?;
                let record: OffsetRecord = serde_json::from_str(&value).ok()?;
                Some((topic_id, partition, record))
            })
            .collect()
    }

    pub(super) async fn offset_fetch(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = OffsetFetchRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let mut gate = self.coordinating().await.err().unwrap_or(0);
        if gate == 0 && self.node.linearizable().await.is_err() {
            gate = codes::NOT_COORDINATOR;
        }
        // Each wanted group: id and, per topic, `(name, id, partitions)`.
        // `None` topics means every committed offset.
        type Wanted = Option<Vec<(String, Uuid, Vec<i32>)>>;
        let wanted: Vec<(String, Wanted)> = if version >= 8 {
            request
                .groups
                .iter()
                .map(|group| {
                    let topics = group.topics.as_ref().map(|topics| {
                        topics
                            .iter()
                            .map(|topic| {
                                let name = if version >= 10 {
                                    self.topic_name_by_id(&topic.topic_id).unwrap_or_default()
                                } else {
                                    topic.name.0.to_string()
                                };
                                (name, topic.topic_id, topic.partition_indexes.clone())
                            })
                            .collect()
                    });
                    (group.group_id.0.to_string(), topics)
                })
                .collect()
        } else {
            let topics = request.topics.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| {
                        (
                            topic.name.0.to_string(),
                            Uuid::nil(),
                            topic.partition_indexes.clone(),
                        )
                    })
                    .collect()
            });
            vec![(request.group_id.0.to_string(), topics)]
        };

        // One answer per group: rows of (name, id, partition, record).
        let mut answers = Vec::new();
        for (group, topics) in wanted {
            let mut rows: Vec<(String, Uuid, i32, Option<OffsetRecord>)> = Vec::new();
            if gate == 0 {
                let stored = self.stored_offsets(&group);
                match topics {
                    Some(topics) => {
                        for (name, id, partitions) in topics {
                            let topic = self.topic(&name);
                            let topic_id = topic
                                .as_ref()
                                .and_then(|topic| Uuid::parse_str(&topic.id).ok())
                                .unwrap_or(id);
                            for partition in partitions {
                                let record = topic.as_ref().and_then(|topic| {
                                    stored
                                        .iter()
                                        .find(|(id, index, _)| {
                                            *id == topic.id && *index == partition
                                        })
                                        .map(|(_, _, record)| record.clone())
                                });
                                rows.push((name.clone(), topic_id, partition, record));
                            }
                        }
                    }
                    None => {
                        for (topic_id, partition, record) in stored {
                            let Ok(id) = Uuid::parse_str(&topic_id) else {
                                continue;
                            };
                            // Offsets of a deleted topic are not reported.
                            let Some(name) = self.topic_name_by_id(&id) else {
                                continue;
                            };
                            rows.push((name, id, partition, Some(record)));
                        }
                    }
                }
            }
            answers.push((group, rows));
        }

        let mut response = OffsetFetchResponse::default();
        let row_parts = |record: &Option<OffsetRecord>| match record {
            Some(record) => (record.offset, record.leader_epoch, record.metadata.clone()),
            None => (-1, -1, Some(String::new())),
        };
        if version >= 8 {
            for (group, rows) in answers {
                let mut out = OffsetFetchResponseGroup::default();
                out.group_id = GroupId(text(&group));
                out.error_code = gate;
                let mut topics: Vec<OffsetFetchResponseTopics> = Vec::new();
                for (name, id, partition, record) in rows {
                    let at = match topics
                        .iter()
                        .position(|topic| topic.name.0.as_str() == name)
                    {
                        Some(at) => at,
                        None => {
                            let mut topic = OffsetFetchResponseTopics::default();
                            topic.name = topic_name(&name);
                            topic.topic_id = id;
                            topics.push(topic);
                            topics.len() - 1
                        }
                    };
                    let (offset, epoch, metadata) = row_parts(&record);
                    let mut part = OffsetFetchResponsePartitions::default();
                    part.partition_index = partition;
                    part.committed_offset = offset;
                    part.committed_leader_epoch = epoch;
                    part.metadata = metadata.map(|value| text(&value));
                    topics[at].partitions.push(part);
                }
                out.topics = topics;
                response.groups.push(out);
            }
        } else if let Some((_, rows)) = answers.into_iter().next() {
            response.error_code = gate;
            let mut topics: Vec<OffsetFetchResponseTopic> = Vec::new();
            for (name, _, partition, record) in rows {
                let at = match topics
                    .iter()
                    .position(|topic| topic.name.0.as_str() == name)
                {
                    Some(at) => at,
                    None => {
                        let mut topic = OffsetFetchResponseTopic::default();
                        topic.name = topic_name(&name);
                        topics.push(topic);
                        topics.len() - 1
                    }
                };
                let (offset, epoch, metadata) = row_parts(&record);
                let mut part = OffsetFetchResponsePartition::default();
                part.partition_index = partition;
                part.committed_offset = offset;
                part.committed_leader_epoch = epoch;
                part.metadata = metadata.map(|value| text(&value));
                // v0-1 put the coordinator error on each partition.
                if version < 2 {
                    part.error_code = gate;
                }
                topics[at].partitions.push(part);
            }
            response.topics = topics;
        }
        frame_of(version, header.correlation_id, &response)
    }

    pub(super) async fn list_groups(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = ListGroupsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = ListGroupsResponse::default();
        // A broker that does not coordinate has no groups to list; the admin
        // client asks every broker and merges.
        if self.coordinating().await.is_ok() {
            for (group, state, protocol_type) in self.coordinator.list() {
                let state_ok = request.states_filter.is_empty()
                    || request
                        .states_filter
                        .iter()
                        .any(|wanted| wanted.as_str().eq_ignore_ascii_case(state.name()));
                let type_ok = request.types_filter.is_empty()
                    || request
                        .types_filter
                        .iter()
                        .any(|wanted| wanted.as_str().eq_ignore_ascii_case("classic"));
                if !(state_ok && type_ok) {
                    continue;
                }
                let mut listed = ListedGroup::default();
                listed.group_id = GroupId(text(&group));
                listed.protocol_type = text(&protocol_type);
                listed.group_state = text(state.name());
                listed.group_type = text("classic");
                response.groups.push(listed);
            }
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn describe_groups(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = DescribeGroupsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let gate = self.coordinating().await.err().unwrap_or(0);
        let mut response = DescribeGroupsResponse::default();
        for group in &request.groups {
            let mut out = DescribedGroup::default();
            out.group_id = group.clone();
            out.authorized_operations = i32::MIN;
            if gate != 0 {
                out.error_code = gate;
                response.groups.push(out);
                continue;
            }
            match self.coordinator.describe(group.0.as_str()) {
                None => out.group_state = text("Dead"),
                Some(described) => {
                    out.group_state = text(described.state.name());
                    out.protocol_type = text(&described.protocol_type);
                    out.protocol_data = text(&described.protocol);
                    out.members = described
                        .members
                        .into_iter()
                        .map(|member| {
                            let mut row = DescribedGroupMember::default();
                            row.member_id = text(&member.member_id);
                            row.group_instance_id = member.group_instance_id.map(|id| text(&id));
                            row.client_id = text(&member.client_id);
                            row.client_host = text(&member.client_host);
                            row.member_metadata = Bytes::from(member.metadata);
                            row.member_assignment = Bytes::from(member.assignment);
                            row
                        })
                        .collect();
                }
            }
            response.groups.push(out);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn delete_groups(&self, header: &RequestHeader, buf: &mut Bytes) -> Reply {
        let Ok(request) = DeleteGroupsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let gate = self.coordinating().await.err().unwrap_or(0);
        let mut response = DeleteGroupsResponse::default();
        for group in &request.groups_names {
            let id = group.0.to_string();
            let code = if gate != 0 {
                gate
            } else if !self.coordinator.deletable(&id) {
                groups::codes::NON_EMPTY_GROUP
            } else {
                match self
                    .node
                    .group_write(Command::DeleteGroup { group: id.clone() })
                    .await
                {
                    Ok(code) => code,
                    Err(_) => codes::NOT_COORDINATOR,
                }
            };
            if code == 0 {
                self.coordinator.forget(&id);
            }
            let mut out = DeletableGroupResult::default();
            out.group_id = group.clone();
            out.error_code = code;
            response.results.push(out);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

/// Keep the coordinator's timers running and drop groups when this broker
/// stops leading metadata.
pub async fn tick(kafka: Arc<Kafka>, coordinator: Arc<Coordinator>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !kafka.leader() {
            kafka.lost_coordination();
            continue;
        }
        coordinator.tick(Instant::now());
    }
}
