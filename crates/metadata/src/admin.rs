//! Phase 4 admin commands: more partitions, topic configs, log start, offset delete.
//!
//! `DeleteRecords` moves the log start to the requested offset, as in Kafka.
//! Extents wholly below it are dropped; an extent the cut falls inside stays,
//! and fetch starts at the log start within it. The offset may be anything up
//! to the high watermark the proposer saw, including records still only in
//! the WAL; past that it is `OFFSET_OUT_OF_RANGE`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalog::{self, Outcome, Partition, Topic};
use crate::config;
use crate::groups::{self, GroupRecord};

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
    /// Legacy `AlterConfigs`: configs not named in `edits` return to their
    /// defaults, as in Kafka.
    #[serde(default)]
    pub replace: bool,
}

/// One partition to trim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordCut {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Delete records before this offset. `-1` means the high watermark.
    pub offset: i64,
    /// The partition's high watermark as the proposer saw it, sealed or
    /// not. `-1` limits the cut to the sealed end.
    #[serde(default = "no_offset")]
    pub high_watermark: i64,
}

fn no_offset() -> i64 {
    -1
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
    let mut placer = catalog::Placer::load(get)?;
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
                txn_committed: Vec::new(),
                txn_aborted: Vec::new(),
                epochs: Vec::new(),
                shard: placer.next(),
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
    put.extend(placer.put());
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
        match config::apply_change(&topic, &change.edits, change.replace) {
            Ok(next) => {
                topic = next;
                if !change.validate_only {
                    put.push((
                        catalog::topic_storage_key(&change.name),
                        serde_json::to_string(&topic).map_err(|err| err.to_string())?,
                    ));
                }
                items.push(Item { code: 0, low: 0 });
            }
            Err(rejected) => items.push(Item {
                code: rejected.code,
                low: 0,
            }),
        }
    }
    Ok(Done {
        put,
        del: Vec::new(),
        items,
    })
}

/// Move log starts to the requested offsets.
pub fn delete_records(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    scan: catalog::Scan,
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
        let limit = state.next_offset.max(cut.high_watermark);
        let offset = if cut.offset < 0 { limit } else { cut.offset };
        if offset > limit {
            items.push(Item {
                code: OFFSET_OUT_OF_RANGE,
                low: -1,
            });
            continue;
        }
        state.log_start = state.log_start.max(offset);
        items.push(Item {
            code: 0,
            low: state.log_start,
        });
    }
    let mut put = Vec::new();
    let mut del = Vec::new();
    for ((topic, partition), mut state) in pending {
        let id = ids.get(&topic).expect("id");
        // Extents wholly below the new log start go.
        del.extend(catalog::extent_keys_through(
            scan,
            id,
            partition,
            state.log_start,
        )?);
        catalog::migrate_extents(id, partition, &mut state, &mut put)?;
        catalog::prune_spans(&mut state);
        put.push((
            catalog::partition_storage_key(id, partition),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        ));
    }
    Ok(Done { put, del, items })
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
            catalog::create_topic(&get, "events", "topic-1", 1, "r2", &[]).unwrap()
        else {
            panic!("create");
        };
        for (key, value) in put {
            map.insert(key, value);
        }
        map
    }

    #[test]
    fn a_delete_moves_the_log_start_to_the_exact_offset() {
        let mut map = world();
        let get = |key: &str| Ok(map.get(key).cloned());
        let part = PublishPart {
            epoch: -1,
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
        let scan = |from: &str, to: &str, limit: usize| catalog::scan_map(&map, from, to, limit);
        let done = delete_records(
            &get,
            &scan,
            &[RecordCut {
                topic: "events".into(),
                partition: 0,
                offset: 4,
                high_watermark: -1,
            }],
        )
        .unwrap();
        assert_eq!(done.items[0].low, 4);
        assert!(done.del.is_empty(), "the extent holding offset 4 stays");
        let state: Partition = serde_json::from_str(&done.put[0].1).unwrap();
        assert_eq!(state.log_start, 4);

        let cut = |offset: i64, high_watermark: i64| RecordCut {
            topic: "events".into(),
            partition: 0,
            offset,
            high_watermark,
        };
        let past = delete_records(&get, &scan, &[cut(15, -1)]).unwrap();
        assert_eq!(past.items[0].code, OFFSET_OUT_OF_RANGE);
        let unsealed = delete_records(&get, &scan, &[cut(15, 20)]).unwrap();
        assert_eq!(unsealed.items[0].low, 15);
        assert_eq!(unsealed.del, vec![catalog::extent_key("topic-1", 0, 10)]);
    }

    #[test]
    fn an_extent_published_below_the_log_start_is_dropped() {
        let mut map = world();
        let get = |key: &str| Ok(map.get(key).cloned());
        let scan = |from: &str, to: &str, limit: usize| catalog::scan_map(&map, from, to, limit);
        let done = delete_records(
            &get,
            &scan,
            &[RecordCut {
                topic: "events".into(),
                partition: 0,
                offset: 5,
                high_watermark: 10,
            }],
        )
        .unwrap();
        for (key, value) in done.put {
            map.insert(key, value);
        }
        let get = |key: &str| Ok(map.get(key).cloned());
        let part = |start: i64, end: i64| PublishPart {
            epoch: -1,
            topic: "events".into(),
            partition: 0,
            start,
            end,
            object_key: format!("bundle/{start}"),
            file_offset: 0,
            length: 4,
            base_timestamp: 0,
            max_timestamp: 0,
        };
        let Outcome::Change { put, .. } = publish(&get, &[part(0, 3), part(3, 8)], &[]).unwrap()
        else {
            panic!("publish");
        };
        let extents: Vec<catalog::Extent> = put
            .iter()
            .filter(|(key, _)| key.starts_with("ext/"))
            .map(|(_, value)| serde_json::from_str(value).unwrap())
            .collect();
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].start, 3);
    }

    #[test]
    fn moving_the_log_start_drops_the_transaction_spans_below_it() -> Result<(), String> {
        let mut map = world();
        let key = catalog::partition_storage_key("topic-1", 0);
        let row = map.get(&key).ok_or("no partition row")?;
        let mut state = catalog::parse_partition(row)?;
        state.next_offset = 20;
        state.txn_committed = vec![
            catalog::Span {
                start: 0,
                end: 4,
                producer_id: None,
            },
            catalog::Span {
                start: 8,
                end: 12,
                producer_id: None,
            },
        ];
        state.txn_aborted = vec![
            catalog::Span {
                start: 4,
                end: 8,
                producer_id: None,
            },
            catalog::Span {
                start: 12,
                end: 14,
                producer_id: None,
            },
        ];
        map.insert(
            key.clone(),
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        );
        let get = |key: &str| Ok(map.get(key).cloned());
        let scan = |from: &str, to: &str, limit: usize| catalog::scan_map(&map, from, to, limit);
        let done = delete_records(
            &get,
            &scan,
            &[RecordCut {
                topic: "events".into(),
                partition: 0,
                offset: 10,
                high_watermark: -1,
            }],
        )?;
        let (_, row) = done
            .put
            .iter()
            .find(|(stored, _)| *stored == key)
            .ok_or("partition row not written")?;
        let state: Partition = serde_json::from_str(row).map_err(|err| err.to_string())?;
        assert_eq!(state.log_start, 10);
        assert_eq!(
            state.txn_committed,
            vec![catalog::Span {
                start: 8,
                end: 12,
                producer_id: None
            }],
            "a span reaching past the log start stays"
        );
        assert_eq!(
            state.txn_aborted,
            vec![catalog::Span {
                start: 12,
                end: 14,
                producer_id: None
            }]
        );
        Ok(())
    }

    #[test]
    fn compact_is_stored() {
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
                replace: false,
            }],
        )
        .unwrap();
        assert_eq!(done.items[0].code, 0);
        let topic: crate::catalog::Topic = serde_json::from_str(&done.put[0].1).unwrap();
        assert_eq!(topic.cleanup_policy, "compact");
    }

    #[test]
    fn legacy_alter_resets_configs_it_does_not_name() {
        let mut map = world();
        let change = |name: &str, value: &str, replace: bool| ConfigChange {
            resource_type: 2,
            name: "events".into(),
            validate_only: false,
            edits: vec![ConfigEdit {
                name: name.into(),
                op: 0,
                value: Some(value.into()),
            }],
            replace,
        };
        let get = |key: &str| Ok(map.get(key).cloned());
        let done = alter_configs(&get, &[change("retention.ms", "60000", false)]).unwrap();
        let (key, value) = done.put[0].clone();
        map.insert(key, value);
        let get = |key: &str| Ok(map.get(key).cloned());
        let done = alter_configs(&get, &[change("segment.bytes", "1048576", true)]).unwrap();
        let topic: crate::catalog::Topic = serde_json::from_str(&done.put[0].1).unwrap();
        assert_eq!(topic.retention_ms, 604_800_000);
        assert!(!topic.is_set("retention.ms"));
        assert_eq!(topic.config("segment.bytes"), "1048576");
    }
}
