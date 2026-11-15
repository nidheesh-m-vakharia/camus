//! Load the newest metadata snapshot and the gap-free extent prefix in R2.
//!
//! This is the "every disk is gone" path in ADR 0008. Unsealed WAL entries
//! are not in the bucket, and metadata newer than the snapshot is not either.

use camus_metadata::{plan_restore, snapshot_rows, Command, MetadataNode, ObjectSlice};
use camus_storage::Bucket;
use uuid::Uuid;

const PUTS: usize = 128;

/// Apply `plan_restore` on the metadata leader of a fresh group.
pub async fn restore(node: &MetadataNode, bucket: &Bucket) -> anyhow::Result<serde_json::Value> {
    let metrics = node.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(metrics.id) {
        anyhow::bail!("this broker does not lead metadata");
    }
    if let Some(raw) = node.read_local("topics")? {
        let names: Vec<String> = serde_json::from_str(&raw).unwrap_or_default();
        if !names.is_empty() {
            anyhow::bail!("metadata already has topics; restore starts from an empty group");
        }
    }
    let body = snapshot_body(bucket).await?;
    let rows = snapshot_rows(&body).map_err(|err| anyhow::anyhow!(err))?;
    let mut slices = object_slices(bucket).await?;
    load_overlap_bytes(bucket, &mut slices).await;
    let plan = plan_restore(&rows, &slices);
    for chunk in plan.puts.chunks(PUTS) {
        let commands = chunk
            .iter()
            .cloned()
            .map(|(key, value)| Command::Put { key, value })
            .collect();
        let response = node.apply(Command::Batch(commands)).await?;
        if response.error_code != 0 {
            anyhow::bail!("snapshot row was rejected ({})", response.error_code);
        }
    }
    for chunk in plan.publish.chunks(PUTS) {
        let response = node
            .apply(Command::Publish {
                parts: chunk.to_vec(),
            })
            .await?;
        if response.error_code != 0 {
            anyhow::bail!("rebuilt extent was rejected ({})", response.error_code);
        }
    }
    Ok(serde_json::json!({
        "rows": plan.puts.len(),
        "extents": plan.publish.len(),
        "offline": plan.offline,
    }))
}

async fn snapshot_body(bucket: &Bucket) -> anyhow::Result<Vec<u8>> {
    let objects = bucket.list("meta/snap/").await?;
    if let Some((_, size, _)) = objects
        .iter()
        .find(|(key, _, _)| key == "meta/snap/CURRENT")
    {
        if let Ok(pointer) = bucket.get_range("meta/snap/CURRENT", 0..*size).await {
            if let Ok(key) = String::from_utf8(pointer.to_vec()) {
                if let Some((_, snap_size, _)) = objects.iter().find(|(name, _, _)| name == &key) {
                    let body = bucket.get_range(&key, 0..*snap_size).await?;
                    return Ok(body.to_vec());
                }
            }
        }
    }
    let mut snaps: Vec<_> = objects
        .iter()
        .filter(|(key, _, _)| key.ends_with(".snap"))
        .collect();
    snaps.sort_by_key(|(key, _, _)| snap_index(key));
    let Some((key, size, _)) = snaps.last() else {
        anyhow::bail!("no metadata snapshot under meta/snap/");
    };
    Ok(bucket.get_range(key, 0..*size).await?.to_vec())
}

fn snap_index(key: &str) -> u64 {
    key.rsplit(['/', '-'])
        .next()
        .and_then(|name| name.strip_suffix(".snap"))
        .and_then(|index| index.parse().ok())
        .unwrap_or(0)
}

async fn object_slices(bucket: &Bucket) -> anyhow::Result<Vec<ObjectSlice>> {
    let mut slices = Vec::new();
    for prefix in ["bundle/", "seg/"] {
        for (key, size, _) in bucket.list(prefix).await? {
            if !key.ends_with(".seg") || size == 0 {
                continue;
            }
            let Ok(body) = bucket.get_range(&key, 0..size).await else {
                continue;
            };
            let Ok(entries) = camus_uploader::decode_footer(&body) else {
                continue;
            };
            for entry in entries {
                let end = entry.file_offset.saturating_add(u64::from(entry.length));
                if end > size {
                    continue;
                }
                slices.push(ObjectSlice {
                    key: key.clone(),
                    topic_id: Uuid::from_bytes(entry.topic_id).to_string(),
                    partition: entry.partition,
                    start: entry.start,
                    end: entry.end,
                    file_offset: entry.file_offset,
                    length: entry.length,
                    base_timestamp: entry.base_timestamp,
                    max_timestamp: entry.max_timestamp,
                    bytes: None,
                });
            }
        }
    }
    Ok(slices)
}

async fn load_overlap_bytes(bucket: &Bucket, slices: &mut [ObjectSlice]) {
    let mut groups: std::collections::BTreeMap<(String, i32, i64, i64), Vec<usize>> =
        std::collections::BTreeMap::new();
    for (index, slice) in slices.iter().enumerate() {
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
    for indexes in groups.into_values() {
        if indexes.len() < 2 {
            continue;
        }
        for index in indexes {
            let slice = &slices[index];
            let end = slice.file_offset.saturating_add(u64::from(slice.length));
            if let Ok(bytes) = bucket.get_range(&slice.key, slice.file_offset..end).await {
                slices[index].bytes = Some(bytes.to_vec());
            }
        }
    }
}
