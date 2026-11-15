//! Plan a whole-cluster restore from a metadata snapshot and object footers.
//!
//! ADR 0008: keep a gap-free prefix from the snapshot's sealed end (or from
//! offset 0 when the snapshot has no sealed end). Overlapping uploads of the
//! same range keep the object the snapshot names, otherwise the
//! lexicographically smallest ULID. Disagreeing batch bytes take the
//! partition offline.

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{self, Extent, Partition, PublishPart};

/// One footer slice inside an object under `bundle/` or `seg/`.
#[derive(Debug, Clone)]
pub struct ObjectSlice {
    /// Object key.
    pub key: String,
    /// Topic id, the UUID string stored in metadata.
    pub topic_id: String,
    /// Partition index.
    pub partition: i32,
    /// Inclusive base offset.
    pub start: i64,
    /// Exclusive end offset.
    pub end: i64,
    /// Byte offset of the batches inside the object.
    pub file_offset: u64,
    /// Byte length of the batches.
    pub length: u32,
    /// First timestamp.
    pub base_timestamp: i64,
    /// Last timestamp.
    pub max_timestamp: i64,
    /// Batch bytes, set when more than one object covers this range.
    pub bytes: Option<Vec<u8>>,
}

/// What to write into a fresh metadata group.
#[derive(Debug, Clone)]
pub struct RestorePlan {
    /// Snapshot rows, then one key per offline partition.
    pub puts: Vec<(String, String)>,
    /// Extents past the snapshot's sealed end.
    pub publish: Vec<PublishPart>,
    /// `(topic id, partition)` whose overlapping objects disagree.
    pub offline: Vec<(String, i32)>,
}

/// `rows` are the snapshot body. `slices` are every footer entry under
/// `bundle/` and `seg/`.
pub fn plan_restore(rows: &[(String, String)], slices: &[ObjectSlice]) -> RestorePlan {
    let names = topic_names(rows);
    let sealed = sealed_ends(rows);
    let named = named_objects(rows);
    let mut offline = BTreeSet::new();
    let mut groups: BTreeMap<(String, i32, i64, i64), Vec<usize>> = BTreeMap::new();
    for (index, slice) in slices.iter().enumerate() {
        if slice.end <= slice.start {
            continue;
        }
        groups
            .entry((
                slice.topic_id.clone(),
                slice.partition,
                slice.start,
                slice.end,
            ))
            .or_default()
            .push(index);
    }
    for ((topic_id, partition, _, _), indexes) in &groups {
        if indexes.len() < 2 {
            continue;
        }
        let chosen = preferred(slices, indexes, &named);
        let Some(chosen_bytes) = slices[chosen].bytes.as_deref() else {
            offline.insert((topic_id.clone(), *partition));
            continue;
        };
        let disagree = indexes.iter().any(|&index| {
            slices[index]
                .bytes
                .as_deref()
                .is_none_or(|bytes| bytes != chosen_bytes)
        });
        if disagree {
            offline.insert((topic_id.clone(), *partition));
        }
    }

    let mut by_start: BTreeMap<(String, i32, i64), Vec<usize>> = BTreeMap::new();
    for (index, slice) in slices.iter().enumerate() {
        if slice.end <= slice.start || offline.contains(&(slice.topic_id.clone(), slice.partition))
        {
            continue;
        }
        by_start
            .entry((slice.topic_id.clone(), slice.partition, slice.start))
            .or_default()
            .push(index);
    }

    let mut publish = Vec::new();
    let mut cursors: BTreeMap<(String, i32), i64> = sealed
        .iter()
        .map(|((id, partition), end)| ((id.clone(), *partition), *end))
        .collect();
    for slice in slices {
        cursors
            .entry((slice.topic_id.clone(), slice.partition))
            .or_insert(0);
    }
    let partitions: Vec<(String, i32)> = cursors.keys().cloned().collect();
    for (topic_id, partition) in partitions {
        if offline.contains(&(topic_id.clone(), partition)) {
            continue;
        }
        let Some(name) = names.get(&topic_id) else {
            continue;
        };
        let epoch = sealed_epoch(rows, &topic_id, partition);
        let mut cursor = cursors[&(topic_id.clone(), partition)];
        while let Some(indexes) = by_start.get(&(topic_id.clone(), partition, cursor)) {
            let chosen = &slices[preferred(slices, indexes, &named)];
            if chosen.end <= cursor {
                break;
            }
            publish.push(PublishPart {
                epoch,
                topic: name.clone(),
                partition,
                start: chosen.start,
                end: chosen.end,
                object_key: chosen.key.clone(),
                file_offset: chosen.file_offset,
                length: chosen.length,
                base_timestamp: chosen.base_timestamp,
                max_timestamp: chosen.max_timestamp,
            });
            cursor = chosen.end;
        }
    }

    let mut puts: Vec<(String, String)> = rows.to_vec();
    let mut offline_list: Vec<(String, i32)> = offline.into_iter().collect();
    for (topic_id, partition) in &offline_list {
        puts.push((
            offline_key(topic_id, *partition),
            "object bytes disagree".to_string(),
        ));
    }
    offline_list.sort();
    RestorePlan {
        puts,
        publish,
        offline: offline_list,
    }
}

/// Key that keeps a restored partition from being served.
pub fn offline_key(topic_id: &str, partition: i32) -> String {
    format!("restore/offline/{topic_id}/{partition}")
}

fn topic_names(rows: &[(String, String)]) -> BTreeMap<String, String> {
    rows.iter()
        .filter_map(|(key, value)| {
            let id = key.strip_prefix("id/")?;
            if id.is_empty() || value.is_empty() {
                return None;
            }
            Some((id.to_string(), value.clone()))
        })
        .collect()
}

fn sealed_ends(rows: &[(String, String)]) -> BTreeMap<(String, i32), i64> {
    let mut ends = BTreeMap::new();
    for (key, value) in rows {
        let Some((id, partition)) = part_of(key) else {
            continue;
        };
        let Ok(state) = serde_json::from_str::<Partition>(value) else {
            continue;
        };
        ends.insert((id, partition), state.next_offset);
    }
    ends
}

fn sealed_epoch(rows: &[(String, String)], topic_id: &str, partition: i32) -> i32 {
    let key = catalog::partition_storage_key(topic_id, partition);
    rows.iter()
        .find(|(stored, _)| stored == &key)
        .and_then(|(_, value)| serde_json::from_str::<Partition>(value).ok())
        .map(|state| state.leader_epoch)
        .unwrap_or(-1)
}

fn named_objects(rows: &[(String, String)]) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for (key, value) in rows {
        if let Some(extent) = extent_of(key, value) {
            keys.insert(extent.key);
            continue;
        }
        let Some((id, partition)) = part_of(key) else {
            continue;
        };
        let Ok(state) = serde_json::from_str::<Partition>(value) else {
            continue;
        };
        let _ = (id, partition);
        for extent in state.extents {
            keys.insert(extent.key);
        }
    }
    keys
}

fn extent_of(key: &str, value: &str) -> Option<Extent> {
    let rest = key.strip_prefix("ext/")?;
    let mut parts = rest.rsplitn(3, '/');
    let _end = parts.next()?;
    let _partition = parts.next()?;
    let _id = parts.next()?;
    serde_json::from_str(value).ok()
}

fn part_of(key: &str) -> Option<(String, i32)> {
    let rest = key.strip_prefix("part/")?;
    let (id, partition) = rest.rsplit_once('/')?;
    Some((id.to_string(), partition.parse().ok()?))
}

fn preferred(slices: &[ObjectSlice], indexes: &[usize], named: &BTreeSet<String>) -> usize {
    if let Some(index) = indexes
        .iter()
        .copied()
        .find(|index| named.contains(&slices[*index].key))
    {
        return index;
    }
    indexes
        .iter()
        .copied()
        .min_by_key(|index| ulid_of(&slices[*index].key))
        .unwrap_or(indexes[0])
}

fn ulid_of(key: &str) -> &str {
    let name = key.rsplit('/').next().unwrap_or(key);
    name.strip_suffix(".seg").unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slice(key: &str, start: i64, end: i64, bytes: Option<&[u8]>) -> ObjectSlice {
        ObjectSlice {
            key: key.to_string(),
            topic_id: "topic-1".into(),
            partition: 0,
            start,
            end,
            file_offset: 0,
            length: 4,
            base_timestamp: 1,
            max_timestamp: 2,
            bytes: bytes.map(|bytes| bytes.to_vec()),
        }
    }

    fn rows(next: i64) -> Vec<(String, String)> {
        let topic = catalog::Topic::new("orders", "topic-1", 1, "wal");
        let part = Partition {
            next_offset: next,
            log_start: 0,
            leader_epoch: 3,
            extents: Vec::new(),
            txn_committed: Vec::new(),
            txn_aborted: Vec::new(),
            epochs: Vec::new(),
            shard: 0,
        };
        vec![
            (
                catalog::topic_storage_key("orders"),
                serde_json::to_string(&topic).unwrap(),
            ),
            ("id/topic-1".into(), "orders".into()),
            (
                catalog::partition_storage_key("topic-1", 0),
                serde_json::to_string(&part).unwrap(),
            ),
        ]
    }

    #[test]
    fn a_gap_free_suffix_is_published_and_a_hole_is_left_behind() {
        let rows = rows(2);
        let slices = vec![
            slice("bundle/01.seg", 0, 2, None),
            slice("bundle/02.seg", 2, 4, None),
            slice("bundle/03.seg", 6, 8, None),
        ];
        let plan = plan_restore(&rows, &slices);
        assert!(plan.offline.is_empty());
        assert_eq!(plan.publish.len(), 1);
        assert_eq!(plan.publish[0].start, 2);
        assert_eq!(plan.publish[0].end, 4);
        assert_eq!(plan.publish[0].object_key, "bundle/02.seg");
        assert_eq!(plan.publish[0].topic, "orders");
        assert_eq!(plan.publish[0].epoch, 3);
    }

    #[test]
    fn the_same_range_keeps_the_smaller_ulid_when_the_bytes_match() {
        let slices = vec![
            slice("bundle/0B.seg", 0, 2, Some(b"same")),
            slice("bundle/0A.seg", 0, 2, Some(b"same")),
        ];
        let plan = plan_restore(&rows(0), &slices);
        assert!(plan.offline.is_empty());
        assert_eq!(plan.publish.len(), 1);
        assert_eq!(plan.publish[0].object_key, "bundle/0A.seg");
    }

    #[test]
    fn disagreeing_bytes_take_the_partition_offline() {
        let slices = vec![
            slice("bundle/0A.seg", 0, 2, Some(b"one")),
            slice("bundle/0B.seg", 0, 2, Some(b"two")),
        ];
        let plan = plan_restore(&rows(0), &slices);
        assert!(plan.publish.is_empty());
        assert_eq!(plan.offline, vec![("topic-1".into(), 0)]);
        assert!(plan
            .puts
            .iter()
            .any(|(key, _)| key == "restore/offline/topic-1/0"));
    }

    #[test]
    fn a_snapshot_object_wins_an_overlap() {
        let mut rows = rows(2);
        let extent = Extent {
            start: 0,
            end: 2,
            key: "bundle/0B.seg".into(),
            file_offset: 0,
            length: 4,
            base_timestamp: 1,
            max_timestamp: 2,
        };
        rows.push((
            catalog::extent_key("topic-1", 0, 2),
            serde_json::to_string(&extent).unwrap(),
        ));
        let slices = vec![
            slice("bundle/0A.seg", 0, 2, Some(b"same")),
            slice("bundle/0B.seg", 0, 2, Some(b"same")),
            slice("bundle/0C.seg", 2, 3, None),
        ];
        let plan = plan_restore(&rows, &slices);
        assert!(plan.offline.is_empty());
        assert_eq!(plan.publish.len(), 1);
        assert_eq!(plan.publish[0].object_key, "bundle/0C.seg");
    }
}
