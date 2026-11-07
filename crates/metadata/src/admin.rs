//! Phase 4 admin commands: more partitions, topic configs, log start, offset delete.
//!
//! `DeleteRecords` follows ADR 0010. A cut in the middle of an object moves
//! the log start to that object's end, and the response low watermark is the
//! offset actually removed. An offset past the sealed end is
//! `OFFSET_OUT_OF_RANGE`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalog::{self, Outcome, Partition, Topic};
use crate::groups::{self, GroupRecord};

const INVALID_CONFIG: i16 = 40;
const UNKNOWN_TOPIC: i16 = 3;
const OFFSET_OUT_OF_RANGE: i16 = 1;
const INVALID_PARTITIONS: i16 = 37;
const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
const INVALID_REQUEST: i16 = 42;
const GROUP_SUBSCRIBED: i16 = 86;

/// One row in an admin command's response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Item {
    /// Kafka error code. `0` applied or already done.
    pub code: i16,
    /// Log start after a `DeleteRecords`. `0` for other commands.
    pub low: i64,
}

/// What the command changed, plus one [`Item`] per request row.
#[derive(Debug)]
pub struct Done {
    /// Keys to write.
    pub put: Vec<(String, String)>,
    /// Keys to remove.
    pub del: Vec<String>,
    /// Per-row results, in request order.
    pub items: Vec<Item>,
}

/// One topic whose partition count should become `count`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionIncrease {
    /// Topic name.
    pub name: String,
    /// Desired total partitions.
    pub count: i32,
    /// The client sent replica assignments. Camus places replicas itself.
    pub assigned: bool,
}

/// One config edit. `op` is Kafka's: 0 set, 1 delete, 2 append, 3 subtract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigEdit {
    /// Config name.
    pub name: String,
    /// 0 set, 1 delete, 2 append, 3 subtract.
    pub op: i8,
    /// New value. Ignored for delete.
    pub value: Option<String>,
}

/// Config edits for one resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigChange {
    /// Resource type. Only topic (`2`) is applied.
    pub resource_type: i8,
    /// Topic name when `resource_type` is 2.
    pub name: String,
    /// Check and do not write.
    pub validate_only: bool,
    /// Edits, applied only if every one is valid.
    pub edits: Vec<ConfigEdit>,
}

/// One partition to trim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordCut {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Delete records before this offset. `-1` means the sealed end.
    pub offset: i64,
}

/// Partitions whose committed offsets should be removed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OffsetDrop {
    /// Group id.
    pub group: String,
    /// Topic name, then partition indexes.
    pub topics: Vec<(String, Vec<i32>)>,
}

fn topic_of(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    name: &str,
) -> Result<Option<Topic>, String> {
    match get(&catalog::topic_storage_key(name))? {
        Some(raw) => Ok(Some(catalog::parse_topic(&raw)?)),
        None => Ok(None),
    }
}

fn partition_of(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    topic: &Topic,
    partition: i32,
) -> Result<Partition, String> {
    let raw = get(&catalog::partition_storage_key(&topic.id, partition))?
        .ok_or_else(|| "missing partition".to_string())?;
    catalog::parse_partition(&raw)
}

/// Add partitions. A count that does not grow the topic is `INVALID_PARTITIONS`.
pub fn create_partitions(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    topics: &[PartitionIncrease],
) -> Result<Done, String> {
    let mut put = Vec::new();
    let mut items = Vec::new();
    for topic in topics {
        if topic.assigned {
            items.push(Item {
                code: INVALID_REPLICA_ASSIGNMENT,
                low: 0,
            });
            continue;
        }
        let Some(mut stored) = topic_of(get, &topic.name)? else {
            items.push(Item {
                code: UNKNOWN_TOPIC,
                low: 0,
            });
            continue;
        };
        if topic.count < stored.partitions {
            items.push(Item {
                code: INVALID_PARTITIONS,
                low: 0,
            });
            continue;
        }
        if topic.count == stored.partitions {
            items.push(Item {
                code: INVALID_PARTITIONS,
                low: 0,
            });
            continue;
        }
        for partition in stored.partitions..topic.count {
            let state = Partition {
                next_offset: 0,
                log_start: 0,
                leader_epoch: stored.leader_epoch,
                extents: Vec::new(),
            };
            put.push((
                catalog::partition_storage_key(&stored.id, partition),
                serde_json::to_string(&state).map_err(|err| err.to_string())?,
            ));
        }
        stored.partitions = topic.count;
        put.push((
            catalog::topic_storage_key(&topic.name),
            serde_json::to_string(&stored).map_err(|err| err.to_string())?,
        ));
        items.push(Item { code: 0, low: 0 });
    }
    Ok(Done {
        put,
        del: Vec::new(),
        items,
    })
}

/// Apply or check topic config edits.
pub fn alter_configs(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    changes: &[ConfigChange],
) -> Result<Done, String> {
    let mut put = Vec::new();
    let mut items = Vec::new();
    for change in changes {
        if change.resource_type != 2 {
            items.push(Item {
                code: INVALID_REQUEST,
                low: 0,
            });
            continue;
        }
        let Some(mut topic) = topic_of(get, &change.name)? else {
            items.push(Item {
                code: UNKNOWN_TOPIC,
                low: 0,
            });
            continue;
        };
        match apply_edits(&mut topic, &change.edits) {
            Ok(()) => {
                if !change.validate_only {
                    put.push((
                        catalog::topic_storage_key(&change.name),
                        serde_json::to_string(&topic).map_err(|err| err.to_string())?,
                    ));
                }
                items.push(Item { code: 0, low: 0 });
            }
            Err(code) => items.push(Item { code, low: 0 }),
        }
    }
    Ok(Done {
        put,
        del: Vec::new(),
        items,
    })
}

fn apply_edits(topic: &mut Topic, edits: &[ConfigEdit]) -> Result<(), i16> {
    let mut next = topic.clone();
    for edit in edits {
        match edit.name.as_str() {
            "retention.ms" => assign_long(&mut next.retention_ms, edit, 604_800_000)?,
            "retention.bytes" => assign_long(&mut next.retention_bytes, edit, -1)?,
            "cleanup.policy" => assign_policy(&mut next.cleanup_policy, edit)?,
            _ => return Err(INVALID_CONFIG),
        }
    }
    *topic = next;
    Ok(())
}

fn assign_long(slot: &mut i64, edit: &ConfigEdit, default: i64) -> Result<(), i16> {
    match edit.op {
        0 => *slot = parse_long(edit.value.as_deref().unwrap_or(""))?,
        1 => *slot = default,
        _ => return Err(INVALID_CONFIG),
    }
    Ok(())
}

fn parse_long(value: &str) -> Result<i64, i16> {
    let parsed: i64 = value.parse().map_err(|_| INVALID_CONFIG)?;
    if parsed == -1 || parsed > 0 {
        Ok(parsed)
    } else {
        Err(INVALID_CONFIG)
    }
}

fn assign_policy(slot: &mut String, edit: &ConfigEdit) -> Result<(), i16> {
    let value = match edit.op {
        0 => edit.value.clone().unwrap_or_default(),
        1 => "delete".into(),
        2 => join_policy(slot, edit.value.as_deref().unwrap_or(""), true),
        3 => join_policy(slot, edit.value.as_deref().unwrap_or(""), false),
        _ => return Err(INVALID_CONFIG),
    };
    if value.split(',').any(|part| part.trim() == "compact") || value.is_empty() {
        return Err(INVALID_CONFIG);
    }
    if value.split(',').any(|part| part.trim() != "delete") {
        return Err(INVALID_CONFIG);
    }
    *slot = "delete".into();
    Ok(())
}

fn join_policy(current: &str, extra: &str, append: bool) -> String {
    let mut parts: Vec<&str> = current
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    for part in extra.split(',').map(str::trim).filter(|part| !part.is_empty()) {
        if append {
            if !parts.contains(&part) {
                parts.push(part);
            }
        } else {
            parts.retain(|have| *have != part);
        }
    }
    parts.join(",")
}

/// Move log starts to an object boundary at or after the requested offset.
pub fn delete_records(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    cuts: &[RecordCut],
) -> Result<Done, String> {
    let mut pending: BTreeMap<(String, i32), Partition> = BTreeMap::new();
    let mut ids: BTreeMap<String, String> = BTreeMap::new();
    let mut items = Vec::new();
    for cut in cuts {
        let Some(topic) = topic_of(get, &cut.topic)? else {
            items.push(Item {
                code: UNKNOWN_TOPIC,
                low: -1,
            });
            continue;
        };
        if cut.partition < 0 || cut.partition >= topic.partitions {
            items.push(Item {
                code: UNKNOWN_TOPIC,
                low: -1,
            });
            continue;
        }
        let key = (cut.topic.clone(), cut.partition);
        if !pending.contains_key(&key) {
            pending.insert(key.clone(), partition_of(get, &topic, cut.partition)?);
            ids.insert(cut.topic.clone(), topic.id.clone());
        }
        let state = pending.get_mut(&key).expect("inserted");
        let offset = if cut.offset < 0 {
            state.next_offset
        } else {
            cut.offset
        };
        if offset > state.next_offset {
            items.push(Item {
                code: OFFSET_OUT_OF_RANGE,
                low: -1,
            });
            continue;
        }
        let low = boundary(state, offset);
        state.log_start = state.log_start.max(low);
        state.extents.retain(|extent| extent.end > state.log_start);
        items.push(Item {
            code: 0,
            low: state.log_start,
        });
    }
    let mut put = Vec::new();
    for ((topic, partition), state) in pending {
        let id = ids.get(&topic).expect("id");
        put.push((
            catalog::partition_storage_key(id, partition),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        ));
    }
    Ok(Done {
        put,
        del: Vec::new(),
        items,
    })
}

/// The sealed boundary at or after `offset`. Inside an object, that is the
/// object's end. On a boundary, it is `offset` itself.
fn boundary(state: &Partition, offset: i64) -> i64 {
    if offset <= state.log_start {
        return state.log_start;
    }
    for extent in &state.extents {
        if offset < extent.end && offset > extent.start {
            return extent.end;
        }
    }
    offset
}

/// Delete committed offsets. A group with members cannot drop a topic it
/// subscribes to. A classic group with any members is treated as subscribed
/// to every topic, because its subscription bytes are not a topic list.
pub fn offset_delete(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    forget: &OffsetDrop,
) -> Result<Done, String> {
    let stored = groups::read_group(get, &forget.group)?;
    if stored.is_none() {
        let mut items = Vec::new();
        for (_, partitions) in &forget.topics {
            for _ in partitions {
                items.push(Item {
                    code: groups::codes::GROUP_ID_NOT_FOUND,
                    low: 0,
                });
            }
        }
        return Ok(Done {
            put: Vec::new(),
            del: Vec::new(),
            items,
        });
    }
    let record = stored.unwrap_or_default();
    let mut del = Vec::new();
    let mut items = Vec::new();
    for (name, partitions) in &forget.topics {
        let Some(topic) = topic_of(get, name)? else {
            for _ in partitions {
                items.push(Item {
                    code: UNKNOWN_TOPIC,
                    low: 0,
                });
            }
            continue;
        };
        let subscribed = subscribed(&record, name);
        for partition in partitions {
            if subscribed {
                items.push(Item {
                    code: GROUP_SUBSCRIBED,
                    low: 0,
                });
                continue;
            }
            if *partition < 0 || *partition >= topic.partitions {
                items.push(Item {
                    code: UNKNOWN_TOPIC,
                    low: 0,
                });
                continue;
            }
            del.push(groups::offset_key(&forget.group, &topic.id, *partition));
            items.push(Item { code: 0, low: 0 });
        }
    }
    Ok(Done {
        put: Vec::new(),
        del,
        items,
    })
}

fn subscribed(record: &GroupRecord, topic: &str) -> bool {
    if record.members.is_empty() {
        return false;
    }
    if record.kind == "consumer" {
        return record.members.iter().any(|member| {
            member.subscribed_topics.iter().any(|name| name == topic)
                || member.subscribed_regex.is_some()
        });
    }
    true
}

/// Encode [`Done`] as an [`Outcome`]. The response value is the items as JSON.
pub fn outcome(done: Done) -> Result<Outcome, String> {
    let value = serde_json::to_string(&done.items).map_err(|err| err.to_string())?;
    Ok(Outcome::Change {
        put: done.put,
        del: done.del,
        value: Some(value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{publish, PublishPart};
    use std::collections::HashMap;

    fn world() -> HashMap<String, String> {
        let mut map = HashMap::new();
        let get = |key: &str| Ok(map.get(key).cloned());
        let Outcome::Change { put, .. } =
            catalog::create_topic(&get, "events", "topic-1", 1, "r2").unwrap()
        else {
            panic!("create");
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        map
    }

    #[test]
    fn a_mid_object_delete_jumps_to_the_object_end() {
        let mut map = world();
        let get = |key: &str| Ok(map.get(key).cloned());
        let part = PublishPart {
            topic: "events".into(),
            partition: 0,
            start: 0,
            end: 10,
            object_key: "bundle/a".into(),
            file_offset: 0,
            length: 4,
            base_timestamp: 0,
            max_timestamp: 0,
        };
        let Outcome::Change { put, .. } = publish(&get, &[part], &[]).unwrap() else {
            panic!("publish");
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        let get = |key: &str| Ok(map.get(key).cloned());
        let done = delete_records(
            &get,
            &[RecordCut {
                topic: "events".into(),
                partition: 0,
                offset: 4,
            }],
        )
        .unwrap();
        assert_eq!(done.items[0].low, 10);
        let state: Partition = serde_json::from_str(&done.put[0].1).unwrap();
        assert!(state.extents.is_empty());
        assert_eq!(state.log_start, 10);
    }

    #[test]
    fn compact_is_rejected() {
        let map = world();
        let get = |key: &str| Ok(map.get(key).cloned());
        let done = alter_configs(
            &get,
            &[ConfigChange {
                resource_type: 2,
                name: "events".into(),
                validate_only: false,
                edits: vec![ConfigEdit {
                    name: "cleanup.policy".into(),
                    op: 0,
                    value: Some("compact".into()),
                }],
            }],
        )
        .unwrap();
        assert_eq!(done.items[0].code, INVALID_CONFIG);
        assert!(done.put.is_empty());
    }
}
