//! Preferred leaders for WAL shards (ADR 0014): the balancer that moves a
//! shard's leadership back to its first voter, as Kafka's
//! `auto.leader.rebalance.enable` does, and ElectLeaders with `PREFERRED`.
//!
//! openraft 0.9 has no leadership transfer, so the preferred broker takes a
//! shard back by campaigning. Leadership belongs to the shard, so every
//! partition on it moves together.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use camus_metadata::NodeId;
use camus_protocol::messages::elect_leaders_response::{PartitionResult, ReplicaElectionResult};
use camus_protocol::messages::{ElectLeadersRequest, ElectLeadersResponse, RequestHeader};
use camus_protocol::protocol::{Decodable, StrBytes};

use super::{auth, frame_of, topic_name, Caller, Kafka, Reply};

const UNKNOWN_TOPIC: i16 = 3;
const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 83;
const ELECTION_NOT_NEEDED: i16 = 84;
const PREFERRED: i8 = 0;

impl Kafka {
    /// Each shard's preferred leader, its first voter, from the stored
    /// layout. Empty for a one-shard cluster, which has no preference.
    pub(super) fn preferred_leaders(&self) -> BTreeMap<u32, NodeId> {
        let Some(layout) = self
            .node
            .read_local(camus_metadata::SHARDS)
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        else {
            return BTreeMap::new();
        };
        layout["voters"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .filter_map(|(shard, voters)| {
                Some((
                    u32::try_from(shard).ok()?,
                    voters.as_array()?.first()?.as_u64()?,
                ))
            })
            .collect()
    }

    /// One balancer pass, on each shard's leader: hand a shard this broker
    /// leads to its preferred voter once that voter has caught up, if this
    /// broker has led it since the previous pass. `seen` holds the shards led
    /// here instead of by their preferred voter last time.
    pub(super) async fn balance_leaders(&self, seen: &mut BTreeSet<u32>) {
        let me = self.node_id as NodeId;
        let mut held = BTreeSet::new();
        for (shard, preferred) in self.preferred_leaders() {
            if preferred == me {
                continue;
            }
            let Some(wal) = self.wal.shard(shard) else {
                continue;
            };
            if !wal.is_leader() {
                continue;
            }
            held.insert(shard);
            if !seen.contains(&shard) || !wal.follower_caught_up(preferred) {
                continue;
            }
            let Some(addr) = wal.voter_addr(preferred) else {
                continue;
            };
            tracing::info!(
                shard,
                preferred,
                "handing shard back to its preferred leader"
            );
            let _ = self
                .http
                .post(format!("http://{addr}/wal/{shard}/campaign"))
                .send()
                .await;
        }
        *seen = held;
    }

    pub(super) async fn elect_leaders(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = ElectLeadersRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let version = header.request_api_version;
        let allowed = self.allows(caller, auth::CLUSTER, auth::CLUSTER_NAME, auth::ALTER);
        let mut response = ElectLeadersResponse::default();
        if !allowed && version >= 1 {
            response.error_code = auth::CLUSTER_FAILED;
            return frame_of(version, header.correlation_id, &response);
        }
        // v0 has no election type: it is always PREFERRED.
        let preferred_only = version == 0 || request.election_type == PREFERRED;
        let wanted: Vec<(String, Vec<i32>)> = match request.topic_partitions {
            Some(topics) => topics
                .into_iter()
                .map(|topic| (topic.topic.0.to_string(), topic.partitions))
                .collect(),
            None => self
                .topic_names()
                .into_iter()
                .filter_map(|name| {
                    let topic = self.topic(&name)?;
                    Some((name, (0..topic.partitions).collect()))
                })
                .collect(),
        };
        let preferred = self.preferred_leaders();
        let mut outcomes: BTreeMap<u32, i16> = BTreeMap::new();
        for (name, partitions) in wanted {
            let stored = self.topic(&name);
            let mut result = ReplicaElectionResult::default();
            result.topic = topic_name(&name);
            for partition in partitions {
                let mut row = PartitionResult::default();
                row.partition_id = partition;
                row.error_code = match &stored {
                    _ if !allowed => auth::CLUSTER_FAILED,
                    _ if !preferred_only => ELIGIBLE_LEADERS_NOT_AVAILABLE,
                    None => UNKNOWN_TOPIC,
                    Some(topic) if partition < 0 || partition >= topic.partitions => UNKNOWN_TOPIC,
                    // An `r2` partition is led by the metadata leader, and a
                    // one-shard cluster has no preferred leader.
                    Some(topic) if topic.durability != "wal" => ELECTION_NOT_NEEDED,
                    Some(topic) => {
                        let shard = self.partition_shard(topic, partition);
                        match preferred.get(&shard) {
                            None => ELECTION_NOT_NEEDED,
                            Some(target) => match outcomes.get(&shard) {
                                Some(code) => *code,
                                None => {
                                    let code = self.move_to_preferred(shard, *target).await;
                                    outcomes.insert(shard, code);
                                    code
                                }
                            },
                        }
                    }
                };
                if row.error_code == ELIGIBLE_LEADERS_NOT_AVAILABLE {
                    row.error_message = Some(StrBytes::from_static_str(
                        "Camus never elects an unclean leader; use truncate-to-sealed (ADR 0008)",
                    ));
                }
                result.partition_result.push(row);
            }
            response.replica_election_results.push(result);
        }
        frame_of(version, header.correlation_id, &response)
    }

    /// Ask the shard's leader to hand it to `target`, unless `target`
    /// already leads it.
    async fn move_to_preferred(&self, shard: u32, target: NodeId) -> i16 {
        let view = self.shard_view(shard);
        if view.leader == Some(target) {
            return ELECTION_NOT_NEEDED;
        }
        let Some(leader) = view.leader else {
            return PREFERRED_LEADER_NOT_AVAILABLE;
        };
        let Some(addr) = self
            .node
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_node(&leader)
            .map(|node| node.addr.clone())
        else {
            return PREFERRED_LEADER_NOT_AVAILABLE;
        };
        match self
            .http
            .post(format!("http://{addr}/wal/{shard}/hand-over"))
            .json(&serde_json::json!({ "to": target }))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => 0,
            _ => PREFERRED_LEADER_NOT_AVAILABLE,
        }
    }
}
