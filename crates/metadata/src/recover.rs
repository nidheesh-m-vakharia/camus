//! Plan a whole-cluster restore from a metadata snapshot and object footers.
//!
//! ADR 0008: keep a gap-free prefix from the snapshot's sealed end (or from
//! offset 0 when the snapshot has no sealed end). Overlapping uploads of the
//! same range keep the object the snapshot names, otherwise the
//! lexicographically smallest ULID. Disagreeing batch bytes take the
//! partition offline.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use serde::Serialize;

use crate::catalog::{self, Extent, Partition, PublishPart};
use crate::{Command, RedbMachine, TypeConfig};

/// Commands per apply when rebuilding, as `restore-cluster` batches them.
const REBUILD_BATCH: usize = 128;

/// Rows read per page when listing rebuilt partitions.
const REBUILD_PAGE: usize = 1024;

/// Most partitions [`rebuild`] lists. Past it, only the totals count them.
pub const REBUILD_LISTED: usize = 100_000;

/// One partition as a restore from a plan would leave it.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RestoredPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Earliest offset still present.
    pub log_start: i64,
    /// Next offset, the end of what the restore can serve.
    pub end: i64,
    /// Kept from being served because overlapping objects disagree.
    pub offline: bool,
}

/// What [`rebuild`] found.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Rebuilt {
    /// Topics in the rebuilt state.
    pub topics: usize,
    /// Partitions in the rebuilt state.
    pub partitions: usize,
    /// The first [`REBUILD_LISTED`] partitions, in topic id order.
    pub listed: Vec<RestoredPartition>,
}

/// Apply `plan` to a new state machine in `dir`, the way `restore-cluster`
/// applies it to a fresh metadata group, and read back every partition.
/// `dir` should be empty; the caller removes it.
pub async fn rebuild(dir: &Path, plan: &RestorePlan) -> anyhow::Result<Rebuilt> {
    let mut machine = Arc::new(RedbMachine::open(&dir.join("state.redb"))?);
    let mut commands: Vec<Command> = Vec::new();
    for chunk in plan.puts.chunks(REBUILD_BATCH) {
        commands.push(Command::Batch(
            chunk
                .iter()
                .cloned()
                .map(|(key, value)| Command::Put { key, value })
                .collect(),
        ));
    }
    for chunk in plan.publish.chunks(REBUILD_BATCH) {
        commands.push(Command::Publish {
            parts: chunk.to_vec(),
        });
    }
    for (index, command) in commands.into_iter().enumerate() {
        let entry: Entry<TypeConfig> = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index as u64 + 1),
            payload: EntryPayload::Normal(command),
        };
        let responses = machine.apply([entry]).await?;
        if let Some(code) = responses
            .iter()
            .map(|response| response.error_code)
            .find(|code| *code != 0)
        {
            anyhow::bail!("the rebuilt state rejected a command ({code})");
        }
    }

    let topics: Vec<String> = match machine.get("topics")? {
        Some(raw) => serde_json::from_str(&raw)?,
        None => Vec::new(),
    };
    let mut names: BTreeMap<String, String> = BTreeMap::new();
    let mut partitions = 0;
    let mut listed = Vec::new();
    let mut from = "part/".to_string();
    loop {
        let page = machine.scan_range(&from, "part0", REBUILD_PAGE)?;
        let Some((last, _)) = page.last() else {
            break;
        };
        from = format!("{last}\0");
        for (key, value) in &page {
            let Some((id, partition)) = part_of(key) else {
                continue;
            };
            let state: Partition = serde_json::from_str(value)?;
            partitions += 1;
            if listed.len() >= REBUILD_LISTED {
                continue;
            }
            let topic = match names.get(&id) {
                Some(name) => name.clone(),
                None => {
                    let name = machine.get(&format!("id/{id}"))?.unwrap_or_default();
                    // Only the current topic's name is kept, so the map
                    // holds at most one entry while partitions are in id
                    // order.
                    names.clear();
                    names.insert(id.clone(), name.clone());
                    name
                }
            };
            listed.push(RestoredPartition {
                topic,
                partition,
                log_start: state.log_start,
                end: state.next_offset,
                offline: machine.get(&offline_key(&id, partition))?.is_some(),
            });
        }
    }
    Ok(Rebuilt {
        topics: topics.len(),
        partitions,
        listed,
    })
}

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
            stamps: None,
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

    #[tokio::test]
    async fn a_rebuild_serves_the_snapshot_plus_the_published_suffix() -> anyhow::Result<()> {
        let mut rows = rows(2);
        rows.push(("topics".into(), serde_json::to_string(&["orders"])?));
        let slices = vec![
            slice("bundle/01.seg", 0, 2, None),
            slice("bundle/02.seg", 2, 4, None),
            slice("bundle/03.seg", 6, 8, None),
        ];
        let plan = plan_restore(&rows, &slices);
        let dir = tempfile::tempdir()?;
        let rebuilt = rebuild(dir.path(), &plan).await?;
        assert_eq!(rebuilt.topics, 1);
        assert_eq!(rebuilt.partitions, 1);
        assert_eq!(
            rebuilt.listed,
            vec![RestoredPartition {
                topic: "orders".into(),
                partition: 0,
                log_start: 0,
                end: 4,
                offline: false,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_rebuild_reports_a_partition_whose_objects_disagree() -> anyhow::Result<()> {
        let rows = rows(0);
        let slices = vec![
            slice("bundle/01.seg", 0, 2, Some(b"aaaa")),
            slice("bundle/02.seg", 0, 2, Some(b"bbbb")),
        ];
        let plan = plan_restore(&rows, &slices);
        let dir = tempfile::tempdir()?;
        let rebuilt = rebuild(dir.path(), &plan).await?;
        assert_eq!(rebuilt.listed.len(), 1);
        assert!(rebuilt.listed[0].offline);
        assert_eq!(rebuilt.listed[0].end, 0);
        Ok(())
    }
}
