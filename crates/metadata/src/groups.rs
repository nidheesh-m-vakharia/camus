//! Consumer group records and committed offsets (ADR 0004, Phase 3).
//!
//! The coordinator keeps live membership in memory. What has to survive a
//! coordinator failover goes through the metadata group: the group record
//! (generation, protocol, members, assignments) each time a generation
//! starts or its assignment is set, and every offset commit. An offset commit
//! from a member is checked here, at apply time, against the stored
//! generation, so a coordinator that lost leadership cannot accept a commit
//! from a generation the cluster has moved past.
//!
//! Group ids are hex-encoded in keys, so a prefix scan for one group never
//! matches another whose id shares a prefix.

use serde::{Deserialize, Serialize};

/// One member as of the stored generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MemberRecord {
    /// Member id the coordinator assigned.
    pub member_id: String,
    /// Static membership id (KIP-345), if the client set one.
    pub group_instance_id: Option<String>,
    /// Client id from the request header.
    pub client_id: String,
    /// Client host as the coordinator saw it.
    pub client_host: String,
    /// Session timeout the member asked for.
    pub session_timeout_ms: i32,
    /// Rebalance timeout the member asked for.
    pub rebalance_timeout_ms: i32,
    /// The member's metadata for the chosen protocol.
    pub subscription: Vec<u8>,
    /// What the leader assigned it. Empty until the leader syncs.
    pub assignment: Vec<u8>,
}

/// A classic group as of its latest generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GroupRecord {
    /// Generation id. Starts at 0 and only increases.
    pub generation: i32,
    /// `consumer`, `connect`, ... `None` for a group that only has offsets.
    pub protocol_type: Option<String>,
    /// Chosen assignor, e.g. `range`.
    pub protocol: Option<String>,
    /// Leader member id.
    pub leader: Option<String>,
    /// Members of this generation. Empty for an empty group.
    pub members: Vec<MemberRecord>,
}

/// One committed offset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OffsetRecord {
    /// Next offset the group will read.
    pub offset: i64,
    /// Leader epoch of the record before `offset`, or -1.
    pub leader_epoch: i32,
    /// Client metadata string.
    pub metadata: Option<String>,
    /// Commit time, ms since the epoch, from the coordinator.
    pub commit_timestamp: i64,
}

/// One partition in an [`crate::Command::CommitOffsets`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OffsetCommit {
    /// Topic id, a UUID string.
    pub topic_id: String,
    /// Partition index.
    pub partition: i32,
    /// The offset and its metadata.
    pub record: OffsetRecord,
}

fn hex(text: &str) -> String {
    text.bytes().map(|byte| format!("{byte:02x}")).collect()
}

/// Metadata key of a group's record.
pub fn group_key(group: &str) -> String {
    format!("group/{}", hex(group))
}

/// Prefix of every group record key.
pub fn group_prefix() -> &'static str {
    "group/"
}

/// Group id from a key under [`group_prefix`].
pub fn group_from_key(key: &str) -> Option<String> {
    let encoded = key.strip_prefix(group_prefix())?;
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(encoded.get(at..at + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

/// Prefix of every offset key of one group.
pub fn offset_prefix(group: &str) -> String {
    format!("offset/{}/", hex(group))
}

/// Metadata key of one committed offset.
pub fn offset_key(group: &str, topic_id: &str, partition: i32) -> String {
    format!("{}{topic_id}/{partition}", offset_prefix(group))
}

/// `(topic_id, partition)` from a key under [`offset_prefix`].
pub fn offset_from_key(group: &str, key: &str) -> Option<(String, i32)> {
    let rest = key.strip_prefix(&offset_prefix(group))?;
    let (topic, partition) = rest.rsplit_once('/')?;
    Some((topic.to_string(), partition.parse().ok()?))
}

/// Kafka error codes these commands reject with.
pub mod codes {
    /// The generation is older than the stored one, or does not match it.
    pub const ILLEGAL_GENERATION: i16 = 22;
    /// The member is not in the stored generation.
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    /// The group still has members.
    pub const NON_EMPTY_GROUP: i16 = 68;
    /// No record and no offsets for the group.
    pub const GROUP_ID_NOT_FOUND: i16 = 69;
}

/// Read a stored group record.
pub fn read_group(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    group: &str,
) -> Result<Option<GroupRecord>, String> {
    match get(&group_key(group))? {
        Some(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|err| err.to_string()),
        None => Ok(None),
    }
}

/// Whether a commit of `generation` by `member_id` is allowed against the
/// stored record. `Err` is the Kafka code to reject with.
pub fn check_commit(
    stored: Option<&GroupRecord>,
    generation: i32,
    member_id: &str,
) -> Result<(), i16> {
    match stored {
        // A simple commit (no generation) is for a group with no members.
        _ if generation < 0 => match stored {
            Some(record) if !record.members.is_empty() => Err(codes::UNKNOWN_MEMBER_ID),
            _ => Ok(()),
        },
        None => Err(codes::ILLEGAL_GENERATION),
        Some(record) => {
            if !record
                .members
                .iter()
                .any(|member| member.member_id == member_id)
            {
                Err(codes::UNKNOWN_MEMBER_ID)
            } else if record.generation != generation {
                Err(codes::ILLEGAL_GENERATION)
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_do_not_collide_across_group_ids() {
        assert!(!offset_key("a/b", "t", 0).starts_with(&offset_prefix("a")));
        assert_eq!(
            group_from_key(&group_key("orders/eu")).as_deref(),
            Some("orders/eu")
        );
        assert_eq!(
            offset_from_key("g", &offset_key("g", "1234-abcd", 7)),
            Some(("1234-abcd".to_string(), 7))
        );
    }

    #[test]
    fn commits_are_fenced_by_the_stored_generation() {
        let record = GroupRecord {
            generation: 3,
            members: vec![MemberRecord {
                member_id: "m1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(check_commit(Some(&record), 3, "m1"), Ok(()));
        assert_eq!(
            check_commit(Some(&record), 2, "m1"),
            Err(codes::ILLEGAL_GENERATION)
        );
        assert_eq!(
            check_commit(Some(&record), 3, "m9"),
            Err(codes::UNKNOWN_MEMBER_ID)
        );
        assert_eq!(
            check_commit(Some(&record), -1, ""),
            Err(codes::UNKNOWN_MEMBER_ID)
        );
        assert_eq!(check_commit(None, -1, ""), Ok(()));
        assert_eq!(check_commit(None, 1, "m1"), Err(codes::ILLEGAL_GENERATION));
        let empty = GroupRecord {
            generation: 4,
            ..Default::default()
        };
        assert_eq!(check_commit(Some(&empty), -1, ""), Ok(()));
    }
}
