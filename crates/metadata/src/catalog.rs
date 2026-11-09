//! Topic and extent records stored in the metadata key/value map.
//!
//! The Raft log stores the command. These helpers turn that command into
//! puts and deletes the state machine applies in one transaction. A publish
//! whose first offset is not the partition's `next_offset` is rejected, so
//! a sealed object cannot open a gap.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A topic row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topic {
    /// Kafka topic name.
    pub name: String,
    /// Topic id, a UUID string.
    pub id: String,
    /// Partition count.
    pub partitions: i32,
    /// `r2` or `wal`.
    pub durability: String,
    /// Epoch written into record batches. Starts at 0.
    pub leader_epoch: i32,
    /// `retention.ms`. Kafka's 7 days. `-1` keeps data by time.
    #[serde(default = "default_retention_ms")]
    pub retention_ms: i64,
    /// `retention.bytes`. `-1` is no byte limit, matching Kafka.
    #[serde(default = "default_retention_bytes")]
    pub retention_bytes: i64,
    /// `cleanup.policy`. `delete`, `compact`, or `delete,compact`.
    #[serde(default = "default_cleanup")]
    pub cleanup_policy: String,
    /// Configs set on this topic, by name, in stored form. A name that is
    /// absent uses its default. The typed fields above mirror their entries.
    #[serde(default)]
    pub overrides: BTreeMap<String, String>,
}

impl Topic {
    /// A topic with every config at its default.
    pub fn new(name: &str, id: &str, partitions: i32, durability: &str) -> Self {
        Self {
            name: name.to_string(),
            id: id.to_string(),
            partitions,
            durability: durability.to_string(),
            leader_epoch: 0,
            retention_ms: default_retention_ms(),
            retention_bytes: default_retention_bytes(),
            cleanup_policy: default_cleanup(),
            overrides: BTreeMap::new(),
        }
    }

    /// The value of a config, set or default. Unknown names are empty.
    pub fn config(&self, name: &str) -> String {
        match name {
            "cleanup.policy" => return self.cleanup_policy.clone(),
            "retention.ms" => return self.retention_ms.to_string(),
            "retention.bytes" => return self.retention_bytes.to_string(),
            "durability.mode" => return self.durability.clone(),
            _ => {}
        }
        if let Some(value) = self.overrides.get(name) {
            return value.clone();
        }
        crate::config::spec(name)
            .map(|spec| spec.default.to_string())
            .unwrap_or_default()
    }

    /// A numeric config, set or default.
    pub fn config_i64(&self, name: &str) -> i64 {
        self.config(name).parse().unwrap_or_default()
    }

    /// The config was set on this topic rather than left at its default.
    pub fn is_set(&self, name: &str) -> bool {
        self.overrides.contains_key(name)
    }

    /// Set a checked value, or return to the default with `None`.
    pub(crate) fn set_config(&mut self, name: &str, value: Option<String>) {
        let default = || {
            crate::config::spec(name)
                .map(|spec| spec.default.to_string())
                .unwrap_or_default()
        };
        let effective = value.clone().unwrap_or_else(default);
        match name {
            "cleanup.policy" => self.cleanup_policy = effective,
            "retention.ms" => self.retention_ms = effective.parse().unwrap_or_default(),
            "retention.bytes" => self.retention_bytes = effective.parse().unwrap_or_default(),
            "durability.mode" => self.durability = effective,
            _ => {}
        }
        match value {
            Some(value) => {
                self.overrides.insert(name.to_string(), value);
            }
            None => {
                self.overrides.remove(name);
            }
        }
    }
}

fn no_epoch() -> i32 {
    -1
}

/// Where a leader epoch's records begin in a partition.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochStart {
    /// Leader epoch.
    pub epoch: i32,
    /// First offset written in it.
    pub start: i64,
}

fn default_retention_ms() -> i64 {
    604_800_000
}

fn default_retention_bytes() -> i64 {
    -1
}

fn default_cleanup() -> String {
    "delete".into()
}

/// One sealed object range for a partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Extent {
    /// Inclusive base offset.
    pub start: i64,
    /// Exclusive end offset.
    pub end: i64,
    /// Object key in the bucket.
    pub key: String,
    /// Byte offset of the batches inside the object.
    pub file_offset: u64,
    /// Byte length of the batches.
    pub length: u32,
    /// First batch timestamp in the range.
    pub base_timestamp: i64,
    /// Last batch timestamp in the range.
    pub max_timestamp: i64,
}

/// Per-partition offset state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Partition {
    /// Next offset to assign. Also the high watermark.
    pub next_offset: i64,
    /// Earliest offset still present.
    pub log_start: i64,
    /// Epoch copied into batches.
    pub leader_epoch: i32,
    /// Sealed ranges, in offset order.
    pub extents: Vec<Extent>,
    /// Transactional ranges that committed. `read_committed` shows these.
    #[serde(default)]
    pub txn_committed: Vec<Span>,
    /// Transactional ranges that aborted or were fenced. Hidden from
    /// `read_committed`.
    #[serde(default)]
    pub txn_aborted: Vec<Span>,
    /// Leader epochs in order, each with the first offset sealed in it.
    /// `OffsetForLeaderEpoch` answers from this.
    #[serde(default)]
    pub epochs: Vec<EpochStart>,
}

/// An offset range, inclusive start, exclusive end.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Span {
    /// Inclusive.
    pub start: i64,
    /// Exclusive.
    pub end: i64,
}

/// One partition slice inside a publish command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishPart {
    /// Leader epoch the part's batches were sealed with, or `-1`.
    #[serde(default = "no_epoch")]
    pub epoch: i32,
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Inclusive base offset the leader assigned.
    pub start: i64,
    /// Exclusive end.
    pub end: i64,
    /// Object key.
    pub object_key: String,
    /// Byte offset of this slice in the object.
    pub file_offset: u64,
    /// Byte length.
    pub length: u32,
    /// First timestamp.
    pub base_timestamp: i64,
    /// Last timestamp.
    pub max_timestamp: i64,
}

/// Mutations, or a Kafka error code and no mutations.
#[derive(Debug)]
pub enum Outcome {
    /// Apply these puts, then these deletes.
    Change {
        /// Keys to insert.
        put: Vec<(String, String)>,
        /// Keys to remove.
        del: Vec<String>,
        /// Extra result for the proposer, such as per-partition codes.
        value: Option<String>,
    },
    /// Do not change the map. `0` is not used here.
    Reject(i16),
}

const TOPICS: &str = "topics";

fn topic_key(name: &str) -> String {
    format!("topic/{name}")
}

fn id_key(id: &str) -> String {
    format!("id/{id}")
}

fn part_key(id: &str, partition: i32) -> String {
    format!("part/{id}/{partition}")
}

fn names(raw: Option<String>) -> Result<Vec<String>, String> {
    match raw {
        None => Ok(Vec::new()),
        Some(text) => serde_json::from_str(&text).map_err(|err| err.to_string()),
    }
}

/// Create a topic. `partitions` must be at least 1. `durability` is `r2` or `wal`.
/// `id` is chosen by the proposer and carried in the command, so every
/// replica stores the same one. `configs` are checked here as well as by the
/// proposer, so every replica decides the same way.
pub fn create_topic(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    name: &str,
    id: &str,
    partitions: i32,
    durability: &str,
    configs: &[(String, Option<String>)],
) -> Result<Outcome, String> {
    if id.is_empty() {
        return Ok(Outcome::Reject(42));
    }
    if name.is_empty() || partitions < 1 || (durability != "r2" && durability != "wal") {
        return Ok(Outcome::Reject(40));
    }
    if get(&topic_key(name))?.is_some() {
        return Ok(Outcome::Reject(36));
    }
    let id = id.to_string();
    let mut topic = Topic::new(name, &id, partitions, durability);
    for (config, value) in configs {
        if config == "durability.mode" {
            continue;
        }
        let edited = crate::config::edit(
            &mut topic,
            config,
            crate::config::Op::Set,
            value.as_deref(),
            true,
        );
        if let Err(rejected) = edited {
            return Ok(Outcome::Reject(rejected.code));
        }
    }
    if configs
        .iter()
        .any(|(config, _)| config == "durability.mode")
    {
        topic
            .overrides
            .insert("durability.mode".into(), durability.to_string());
    }
    let mut put = vec![
        (
            topic_key(name),
            serde_json::to_string(&topic).map_err(|err| err.to_string())?,
        ),
        (id_key(&id), name.to_string()),
    ];
    for partition in 0..partitions {
        let state = Partition {
            next_offset: 0,
            log_start: 0,
            leader_epoch: 0,
            extents: Vec::new(),
            txn_committed: Vec::new(),
            txn_aborted: Vec::new(),
            epochs: Vec::new(),
        };
        put.push((
            part_key(&id, partition),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        ));
    }
    let mut list = names(get(TOPICS)?)?;
    list.push(name.to_string());
    put.push((
        TOPICS.to_string(),
        serde_json::to_string(&list).map_err(|err| err.to_string())?,
    ));
    Ok(Outcome::Change {
        put,
        del: Vec::new(),
        value: None,
    })
}

/// Delete a topic by name. Missing names reject with unknown-topic (3).
pub fn delete_topic(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    name: &str,
) -> Result<Outcome, String> {
    let Some(raw) = get(&topic_key(name))? else {
        return Ok(Outcome::Reject(3));
    };
    let topic: Topic = serde_json::from_str(&raw).map_err(|err| err.to_string())?;
    let mut del = vec![topic_key(name), id_key(&topic.id)];
    for partition in 0..topic.partitions {
        del.push(part_key(&topic.id, partition));
    }
    let list: Vec<String> = names(get(TOPICS)?)?
        .into_iter()
        .filter(|item| item != name)
        .collect();
    let put = vec![(
        TOPICS.to_string(),
        serde_json::to_string(&list).map_err(|err| err.to_string())?,
    )];
    Ok(Outcome::Change {
        put,
        del,
        value: None,
    })
}

/// Publish extents. Each part must begin at that partition's next offset.
/// `notes` are the producer sequences inside those extents.
pub fn publish(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    parts: &[PublishPart],
    notes: &[crate::ProducerNote],
) -> Result<Outcome, String> {
    let mut pending: BTreeMap<(String, i32), Partition> = BTreeMap::new();
    let mut ids: BTreeMap<String, String> = BTreeMap::new();
    for part in parts {
        if part.end < part.start {
            return Ok(Outcome::Reject(42));
        }
        let topic = match get(&topic_key(&part.topic))? {
            Some(raw) => serde_json::from_str::<Topic>(&raw).map_err(|err| err.to_string())?,
            None => return Ok(Outcome::Reject(3)),
        };
        if part.partition < 0 || part.partition >= topic.partitions {
            return Ok(Outcome::Reject(3));
        }
        ids.insert(part.topic.clone(), topic.id.clone());
        let key = (part.topic.clone(), part.partition);
        if !pending.contains_key(&key) {
            let raw = get(&part_key(&topic.id, part.partition))?
                .ok_or_else(|| "missing partition".to_string())?;
            let state: Partition = serde_json::from_str(&raw).map_err(|err| err.to_string())?;
            pending.insert(key.clone(), state);
        }
        let state = pending.get_mut(&key).expect("inserted");
        if part.start != state.next_offset {
            return Ok(Outcome::Reject(1));
        }
        state.extents.push(Extent {
            start: part.start,
            end: part.end,
            key: part.object_key.clone(),
            file_offset: part.file_offset,
            length: part.length,
            base_timestamp: part.base_timestamp,
            max_timestamp: part.max_timestamp,
        });
        state.next_offset = part.end;
        if part.epoch >= 0
            && state
                .epochs
                .last()
                .is_none_or(|last| part.epoch > last.epoch)
        {
            state.epochs.push(EpochStart {
                epoch: part.epoch,
                start: part.start,
            });
        }
        // `DeleteRecords` may have moved the log start past records that were
        // still in the WAL. Their extent is not kept; the sweep removes the
        // object once nothing else points into it.
        let log_start = state.log_start;
        state.extents.retain(|extent| extent.end > log_start);
    }
    let mut put = Vec::new();
    for ((topic, partition), state) in pending {
        let id = ids.get(&topic).expect("topic id");
        put.push((
            part_key(id, partition),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        ));
    }
    let sequences = crate::producers::apply_notes(get, parts, notes)?;
    match sequences {
        Outcome::Reject(code) => Ok(Outcome::Reject(code)),
        Outcome::Change {
            put: extra, del, ..
        } => {
            put.extend(extra);
            Ok(Outcome::Change {
                put,
                del,
                value: None,
            })
        }
    }
}

/// Replace the sealed prefix ending at `covered_end` with one extent.
/// `next_offset` stays put, so surviving records keep the offsets they had.
pub fn compact_replace(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    topic: &str,
    partition: i32,
    covered_end: i64,
    object_key: &str,
    file_offset: u64,
    length: u32,
    base_timestamp: i64,
    max_timestamp: i64,
) -> Result<Outcome, String> {
    let Some(raw) = get(&topic_key(topic))? else {
        return Ok(Outcome::Reject(3));
    };
    let stored: Topic = serde_json::from_str(&raw).map_err(|err| err.to_string())?;
    if partition < 0 || partition >= stored.partitions {
        return Ok(Outcome::Reject(3));
    }
    let raw =
        get(&part_key(&stored.id, partition))?.ok_or_else(|| "missing partition".to_string())?;
    let mut state: Partition = serde_json::from_str(&raw).map_err(|err| err.to_string())?;
    let prefix: Vec<_> = state
        .extents
        .iter()
        .take_while(|extent| extent.end <= covered_end)
        .cloned()
        .collect();
    if prefix.is_empty() || prefix.last().is_none_or(|extent| extent.end != covered_end) {
        return Ok(Outcome::Reject(1));
    }
    let start = prefix[0].start;
    state.extents.drain(0..prefix.len());
    state.extents.insert(
        0,
        Extent {
            start,
            end: covered_end,
            key: object_key.to_string(),
            file_offset,
            length,
            base_timestamp,
            max_timestamp,
        },
    );
    state.extents.sort_by_key(|extent| extent.start);
    Ok(Outcome::Change {
        put: vec![(
            part_key(&stored.id, partition),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        )],
        del: Vec::new(),
        value: None,
    })
}

/// Decode a topic row.
pub fn parse_topic(raw: &str) -> Result<Topic, String> {
    serde_json::from_str(raw).map_err(|err| err.to_string())
}

/// Decode a partition row.
pub fn parse_partition(raw: &str) -> Result<Partition, String> {
    serde_json::from_str(raw).map_err(|err| err.to_string())
}

/// Key for a topic name.
pub fn topic_storage_key(name: &str) -> String {
    topic_key(name)
}

/// Key for one partition.
pub fn partition_storage_key(id: &str, partition: i32) -> String {
    part_key(id, partition)
}

/// Key whose value is the JSON list of topic names.
pub fn topic_index_key() -> &'static str {
    TOPICS
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn publish_rejects_a_gap() {
        let mut map = HashMap::new();
        let put = {
            let get = |key: &str| Ok(map.get(key).cloned());
            let Outcome::Change { put, .. } = create_topic(
                &get,
                "events",
                &uuid::Uuid::new_v4().to_string(),
                1,
                "r2",
                &[],
            )
            .unwrap() else {
                panic!("create");
            };
            put
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        let get = |key: &str| Ok(map.get(key).cloned());
        let part = PublishPart {
            epoch: -1,
            topic: "events".into(),
            partition: 0,
            start: 1,
            end: 2,
            object_key: "bundle/x.seg".into(),
            file_offset: 0,
            length: 10,
            base_timestamp: 0,
            max_timestamp: 0,
        };
        match publish(&get, &[part], &[]).unwrap() {
            Outcome::Reject(1) => {}
            other => panic!("expected offset rejection, got {other:?}"),
        }
    }

    #[test]
    fn compact_replace_keeps_the_high_watermark() {
        let mut map = HashMap::new();
        let put = {
            let get = |key: &str| Ok(map.get(key).cloned());
            let Outcome::Change { put, .. } =
                create_topic(&get, "events", "topic-1", 1, "r2", &[]).unwrap()
            else {
                panic!("create");
            };
            put
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        let published = {
            let get = |key: &str| Ok(map.get(key).cloned());
            publish(
                &get,
                &[PublishPart {
                    epoch: -1,
                    topic: "events".into(),
                    partition: 0,
                    start: 0,
                    end: 3,
                    object_key: "bundle/old.seg".into(),
                    file_offset: 0,
                    length: 30,
                    base_timestamp: 1,
                    max_timestamp: 3,
                }],
                &[],
            )
            .unwrap()
        };
        let Outcome::Change { put, .. } = published else {
            panic!("publish");
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        let replaced = {
            let get = |key: &str| Ok(map.get(key).cloned());
            compact_replace(&get, "events", 0, 3, "bundle/new.seg", 0, 12, 1, 3).unwrap()
        };
        let Outcome::Change { put, .. } = replaced else {
            panic!("compact");
        };
        let state: Partition = serde_json::from_str(&put[0].1).unwrap();
        assert_eq!(state.next_offset, 3);
        assert_eq!(state.extents.len(), 1);
        assert_eq!(state.extents[0].key, "bundle/new.seg");
        assert_eq!(state.extents[0].end, 3);
    }

    #[test]
    fn publish_records_where_each_epoch_starts() {
        let mut map = HashMap::new();
        let get = |key: &str| Ok(map.get(key).cloned());
        let Outcome::Change { put, .. } =
            create_topic(&get, "events", "topic-1", 1, "wal", &[]).unwrap()
        else {
            panic!("create");
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        let part = |start: i64, end: i64, epoch: i32| PublishPart {
            epoch,
            topic: "events".into(),
            partition: 0,
            start,
            end,
            object_key: "bundle/a".into(),
            file_offset: 0,
            length: 1,
            base_timestamp: 0,
            max_timestamp: 0,
        };
        let get = |key: &str| Ok(map.get(key).cloned());
        let Outcome::Change { put, .. } = publish(
            &get,
            &[part(0, 2, 1), part(2, 4, 1), part(4, 6, 3), part(6, 7, -1)],
            &[],
        )
        .unwrap() else {
            panic!("publish");
        };
        let state: Partition = serde_json::from_str(&put[0].1).unwrap();
        assert_eq!(
            state.epochs,
            vec![
                EpochStart { epoch: 1, start: 0 },
                EpochStart { epoch: 3, start: 4 }
            ]
        );
    }
}
