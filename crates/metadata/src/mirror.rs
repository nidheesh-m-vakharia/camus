//! Mirror links and mirror topics (ADR 0018).
//!
//! A link names a source cluster's bucket. A mirror topic is a read-only copy
//! of one source topic, with the source's offsets. Copying is a later phase;
//! this module holds the records and the rules that keep a mirror read-only.

use serde::{Deserialize, Serialize};

use crate::catalog::{self, Outcome, Topic};

/// Kafka `INVALID_REQUEST`: a malformed link or mirror.
pub const INVALID_REQUEST: i16 = 42;
/// Kafka `POLICY_VIOLATION`: the change is refused on a mirror, or the link
/// is in use or already names another bucket.
pub const POLICY_VIOLATION: i16 = 44;

/// Longest alias. It is also an environment variable name fragment.
const MAX_ALIAS: usize = 32;

/// Most links in one cluster. Every broker holds the list, and the copier
/// runs one task per link.
pub const MAX_LINKS: usize = 64;

/// Longest endpoint URL, region, bucket, and prefix. A bucket name is at most
/// 63 bytes in S3 and R2, and an object key at most 1024.
const MAX_ENDPOINT: usize = 2048;
const MAX_REGION: usize = 64;
const MAX_BUCKET: usize = 63;
const MAX_PREFIX: usize = 1024;

/// A source cluster's bucket, as the destination reads it. The keys to read
/// it are not stored here: snapshots are uploaded to this cluster's bucket,
/// so each broker reads them from its environment instead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MirrorLink {
    /// 1–32 characters of `[a-z0-9-]`, unique in this cluster.
    pub alias: String,
    /// S3 endpoint URL.
    pub endpoint: String,
    /// S3 region. R2 uses `auto`.
    pub region: String,
    /// Source bucket.
    pub bucket: String,
    /// Key prefix inside the bucket, as the source's `s3.prefix`.
    #[serde(default)]
    pub prefix: String,
    /// Virtual-host addressing, as the source's `s3.virtual_host`.
    #[serde(default)]
    pub virtual_host: bool,
}

/// What a mirror topic copies.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MirrorOf {
    /// The link's alias.
    pub alias: String,
    /// The source topic's id.
    pub topic_id: String,
}

/// A transaction span as `PublishMirror` carries it. Unlike
/// [`catalog::Span`], it always writes `producer_id`: the Raft log is
/// postcard, which cannot skip a field, and a source span stored before
/// ADR 0015 has none.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CopiedSpan {
    /// Inclusive.
    pub start: i64,
    /// Exclusive.
    pub end: i64,
    /// The span's producer, or `None` for a span that matches any.
    pub producer_id: Option<i64>,
}

impl From<&catalog::Span> for CopiedSpan {
    fn from(span: &catalog::Span) -> Self {
        Self {
            start: span.start,
            end: span.end,
            producer_id: span.producer_id,
        }
    }
}

impl From<&CopiedSpan> for catalog::Span {
    fn from(span: &CopiedSpan) -> Self {
        Self {
            start: span.start,
            end: span.end,
            producer_id: span.producer_id,
        }
    }
}

/// Most consumer groups one link syncs. The copier reads each group's
/// offsets from the source every round.
pub const MAX_GROUPS: usize = 256;

/// Longest group id a link syncs.
const MAX_GROUP_BYTES: usize = 1024;

/// Metadata key of the groups a link syncs: a JSON list of group ids.
pub fn groups_key(alias: &str) -> String {
    format!("mirror-groups/{alias}")
}

/// Metadata key of a mirror topic's applied watermark (ADR 0018, phase 3):
/// Unix microseconds in decimal. Every batch its source stamped below it is
/// copied, as the source snapshot that carried that watermark held it.
pub fn applied_key(topic_id: &str) -> String {
    format!("mirror-applied/{topic_id}")
}

/// Set the consumer groups whose committed offsets the copier syncs from a
/// link's source into its mirrors. An empty list stops syncing. The link
/// must exist; at most [`MAX_GROUPS`] ids, each 1 to 1024 bytes.
pub fn set_groups(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    alias: &str,
    groups: &[String],
) -> Result<Outcome, String> {
    let sized = groups
        .iter()
        .all(|group| !group.is_empty() && group.len() <= MAX_GROUP_BYTES);
    if get(&link_key(alias))?.is_none() || groups.len() > MAX_GROUPS || !sized {
        return Ok(Outcome::Reject(INVALID_REQUEST));
    }
    let mut groups = groups.to_vec();
    groups.sort();
    groups.dedup();
    let key = groups_key(alias);
    if groups.is_empty() {
        return Ok(Outcome::Change {
            put: Vec::new(),
            del: vec![key],
            value: None,
        });
    }
    Ok(Outcome::Change {
        put: vec![(
            key,
            serde_json::to_string(&groups).map_err(|err| err.to_string())?,
        )],
        del: Vec::new(),
        value: None,
    })
}

/// The consumer groups a link syncs.
pub fn groups_of(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    alias: &str,
) -> Result<Vec<String>, String> {
    match get(&groups_key(alias))? {
        Some(raw) => serde_json::from_str(&raw).map_err(|err| err.to_string()),
        None => Ok(Vec::new()),
    }
}

/// What a promoted mirror was, and where each partition stopped copying.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Promoted {
    /// The link and the source topic id it mirrored.
    pub of: MirrorOf,
    /// Each partition's next offset at promotion: the first offset this
    /// cluster assigned itself. Offsets the source holds at or past one of
    /// these are a fork.
    pub at: Vec<i64>,
}

/// Make a mirror topic writable: copying stops, and each partition's next
/// offset is kept as where it was promoted. Refused on a plain topic; a
/// topic already promoted changes nothing, so a retried promote succeeds.
///
/// Each partition that holds copied batches starts a new epoch at its
/// promotion offset, one past the newest copied epoch, as a Kafka leader
/// change does. A consumer that fetched records the source acked past that
/// offset then sees its epoch end there and truncates, rather than skipping
/// what this cluster writes at those offsets.
pub fn promote(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    name: &str,
) -> Result<Outcome, String> {
    let Some(raw) = get(&catalog::topic_storage_key(name))? else {
        return Ok(Outcome::Reject(3));
    };
    let mut topic = catalog::parse_topic(&raw)?;
    let Some(of) = topic.mirror_of.take() else {
        if topic.promoted.is_some() {
            return Ok(unchanged());
        }
        return Ok(Outcome::Reject(INVALID_REQUEST));
    };
    let mut at = Vec::with_capacity(topic.partitions.max(0) as usize);
    let mut put = Vec::new();
    for partition in 0..topic.partitions {
        let key = catalog::partition_storage_key(&topic.id, partition);
        let Some(raw) = get(&key)? else {
            return Ok(Outcome::Reject(3));
        };
        let mut state = catalog::parse_partition(&raw)?;
        at.push(state.next_offset);
        if let Some(last) = state.epochs.last().cloned() {
            state.epochs.push(catalog::EpochStart {
                epoch: last.epoch.saturating_add(1),
                start: state.next_offset,
            });
            put.push((
                key,
                serde_json::to_string(&state).map_err(|err| err.to_string())?,
            ));
        }
    }
    topic.promoted = Some(Promoted { of, at });
    put.push((
        catalog::topic_storage_key(name),
        serde_json::to_string(&topic).map_err(|err| err.to_string())?,
    ));
    Ok(Outcome::Change {
        put,
        del: Vec::new(),
        value: None,
    })
}

/// Whether every topic id in `topic_ids` is still a mirror. A group sync
/// commits only then, so one that was in flight when a topic was promoted
/// cannot write a source offset into what this cluster has since produced.
pub fn all_mirrors<'a>(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    topic_ids: impl IntoIterator<Item = &'a str>,
) -> Result<bool, String> {
    for id in topic_ids {
        let Some(name) = get(&catalog::id_key(id))? else {
            return Ok(false);
        };
        let Some(raw) = get(&catalog::topic_storage_key(&name))? else {
            return Ok(false);
        };
        if catalog::parse_topic(&raw)?.mirror_of.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Metadata key of a link.
pub fn link_key(alias: &str) -> String {
    format!("{}{alias}", links_prefix())
}

/// Prefix of every link key.
pub fn links_prefix() -> &'static str {
    "mirror/"
}

/// Whether `alias` is 1–32 characters of `[a-z0-9-]`.
pub fn valid_alias(alias: &str) -> bool {
    (1..=MAX_ALIAS).contains(&alias.len())
        && alias
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Add a link. Adding the same link again changes nothing, so a retried add
/// succeeds. A different link under a used alias is refused: the mirrors
/// that name it would start reading another bucket. So is a link past
/// [`MAX_LINKS`].
pub fn add_link(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    scan: catalog::Scan,
    link: &MirrorLink,
) -> Result<Outcome, String> {
    let sized = |text: &str, max: usize| !text.is_empty() && text.len() <= max;
    if !valid_alias(&link.alias)
        || !sized(&link.endpoint, MAX_ENDPOINT)
        || !sized(&link.region, MAX_REGION)
        || !sized(&link.bucket, MAX_BUCKET)
        || link.prefix.len() > MAX_PREFIX
    {
        return Ok(Outcome::Reject(INVALID_REQUEST));
    }
    let key = link_key(&link.alias);
    if let Some(raw) = get(&key)? {
        let stored: MirrorLink = serde_json::from_str(&raw).map_err(|err| err.to_string())?;
        if stored == *link {
            return Ok(unchanged());
        }
        return Ok(Outcome::Reject(POLICY_VIOLATION));
    }
    // `0` sorts right after `/`, so this bounds the link keys.
    let end = format!("{}0", links_prefix().trim_end_matches('/'));
    if scan(links_prefix(), &end, MAX_LINKS)?.len() >= MAX_LINKS {
        return Ok(Outcome::Reject(POLICY_VIOLATION));
    }
    Ok(Outcome::Change {
        put: vec![(
            key,
            serde_json::to_string(link).map_err(|err| err.to_string())?,
        )],
        del: Vec::new(),
        value: None,
    })
}

/// Remove a link. A missing alias is not an error. A link that a mirror
/// topic still names is refused.
pub fn remove_link(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    alias: &str,
) -> Result<Outcome, String> {
    if !valid_alias(alias) {
        return Ok(Outcome::Reject(INVALID_REQUEST));
    }
    let key = link_key(alias);
    if get(&key)?.is_none() {
        return Ok(unchanged());
    }
    for name in catalog::topic_names(get)? {
        let Some(raw) = get(&catalog::topic_storage_key(&name))? else {
            continue;
        };
        let topic = catalog::parse_topic(&raw)?;
        if topic.mirror_of.is_some_and(|of| of.alias == alias) {
            return Ok(Outcome::Reject(POLICY_VIOLATION));
        }
    }
    Ok(Outcome::Change {
        put: Vec::new(),
        del: vec![key, groups_key(alias)],
        value: None,
    })
}

/// Create a mirror topic: an `r2` topic with default configs and
/// `mirror_of` set. The link must exist.
pub fn create_topic(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    name: &str,
    id: &str,
    partitions: i32,
    of: &MirrorOf,
) -> Result<Outcome, String> {
    if of.topic_id.is_empty() || get(&link_key(&of.alias))?.is_none() {
        return Ok(Outcome::Reject(INVALID_REQUEST));
    }
    catalog::create_topic_as(get, name, id, partitions, "r2", &[], Some(of.clone()))
}

/// Drop a mirror partition's copy, for a partition whose source retention
/// passed its end. The log start moves up to the next offset and the
/// extents below it go, so the partition holds nothing. The copier then
/// goes on from the later of that offset and the source's log start: past
/// the gap for a stalled partition, from where it was for one that was not.
/// Refused on a plain topic.
pub fn reset_partition(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    scan: catalog::Scan,
    name: &str,
    partition: i32,
) -> Result<Outcome, String> {
    let Some(raw) = get(&catalog::topic_storage_key(name))? else {
        return Ok(Outcome::Reject(3));
    };
    let topic = catalog::parse_topic(&raw)?;
    if topic.mirror_of.is_none() {
        return Ok(Outcome::Reject(INVALID_REQUEST));
    }
    if partition < 0 || partition >= topic.partitions {
        return Ok(Outcome::Reject(3));
    }
    let key = catalog::partition_storage_key(&topic.id, partition);
    let Some(raw) = get(&key)? else {
        return Ok(Outcome::Reject(3));
    };
    let mut state = catalog::parse_partition(&raw)?;
    let mut put = Vec::new();
    // Move the log start first, as `delete_records` does, so migrating
    // legacy extents writes none of the ones this drops.
    state.log_start = state.next_offset;
    catalog::migrate_extents(&topic.id, partition, &mut state, &mut put)?;
    let del = catalog::extent_keys_through(scan, &topic.id, partition, state.log_start)?;
    catalog::prune_spans(&mut state);
    put.push((
        key,
        serde_json::to_string(&state).map_err(|err| err.to_string())?,
    ));
    Ok(Outcome::Change {
        put,
        del,
        value: None,
    })
}

/// Whether a mirror topic may go from `before` to `after`. Only retention is
/// the destination's own; every other config follows the source. Configs
/// compare by value, so a legacy `AlterConfigs` that drops an override equal
/// to its default changes nothing.
pub fn may_change(before: &Topic, after: &Topic) -> bool {
    let own = |name: &str| name == "retention.ms" || name == "retention.bytes";
    let configs_same = crate::config::SPECS
        .iter()
        .filter(|spec| !own(spec.name))
        .all(|spec| before.config(spec.name) == after.config(spec.name));
    let without_configs = |topic: &Topic| {
        let mut topic = topic.clone();
        topic.retention_ms = 0;
        topic.retention_bytes = 0;
        topic.cleanup_policy.clear();
        topic.overrides.clear();
        topic
    };
    configs_same && without_configs(before) == without_configs(after)
}

/// One mirror partition's part in a consistent cut (ADR 0018, phase 3):
/// where its log ends after the cut. Every field is always written, as the
/// log is postcard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MirrorCut {
    /// Mirror topic name.
    pub topic: String,
    /// Its id, so a topic deleted and made again under the name is refused.
    pub topic_id: String,
    /// Partition index.
    pub partition: i32,
    /// The partition's next offset after the cut.
    pub end: i64,
    /// When `end` falls inside an extent, what that extent keeps.
    pub shorten: Option<Shorten>,
}

/// The front of an extent that a cut keeps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Shorten {
    /// The extent's end before the cut, which names it.
    pub extent_end: i64,
    /// The object it must point at.
    pub object_key: String,
    /// Bytes of the batches it keeps, from its start.
    pub length: u32,
    /// Latest timestamp of the batches it keeps.
    pub max_timestamp: i64,
    /// Stamp range of the batches it keeps.
    pub stamps: Option<catalog::StampRange>,
}

/// Extents a cut reads at a time while it finds those past the cut.
const CUT_PAGE: usize = 1024;

/// Most extents one cut drops, over every partition. Each voter holds them
/// in one apply, so a cut further behind its mirrors is refused; let
/// retention or the copier's lag shrink it first.
pub const MAX_CUT_EXTENTS: usize = 65_536;

/// Cut every partition of each named mirror at its `end`, then promote
/// those topics, all in one change or none (ADR 0018, phase 3). Every
/// partition of a named topic must have exactly one cut, and with `all`
/// every mirror topic must be named.
///
/// - Extents that start at or after the cut are dropped, and the one it
///   falls inside keeps the front `shorten` names.
/// - Leader epochs that start at or after the cut are dropped. The highest
///   dropped one stays as an empty range at the cut, so a client that read
///   under it learns its end is the cut, and promotion's new epoch is above
///   every epoch the partition had.
/// - The groups the link syncs have their offsets past the cut brought
///   back to it.
/// - A transaction the cut falls inside had not ended by the cut, so its
///   kept batches become an aborted span. A committed one is aborted, as
///   its commit came after the cut.
pub fn cut(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    scan: catalog::Scan,
    cuts: &[MirrorCut],
    all: bool,
) -> Result<Outcome, String> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut put: BTreeMap<String, String> = BTreeMap::new();
    let mut del: BTreeSet<String> = BTreeSet::new();
    let mut topics: BTreeMap<String, (Topic, BTreeSet<i32>)> = BTreeMap::new();
    let mut dropped = 0usize;
    for cut in cuts {
        if !topics.contains_key(&cut.topic) {
            let Some(raw) = get(&catalog::topic_storage_key(&cut.topic))? else {
                return Ok(Outcome::Reject(3));
            };
            let topic = catalog::parse_topic(&raw)?;
            if topic.mirror_of.is_none() {
                return Ok(Outcome::Reject(INVALID_REQUEST));
            }
            if topic.id != cut.topic_id {
                return Ok(Outcome::Reject(3));
            }
            topics.insert(cut.topic.clone(), (topic, BTreeSet::new()));
        }
        let Some((topic, seen)) = topics.get_mut(&cut.topic) else {
            return Ok(Outcome::Reject(3));
        };
        if cut.partition < 0 || cut.partition >= topic.partitions || !seen.insert(cut.partition) {
            return Ok(Outcome::Reject(INVALID_REQUEST));
        }
        let id = topic.id.clone();
        let key = catalog::partition_storage_key(&id, cut.partition);
        let Some(raw) = get(&key)? else {
            return Ok(Outcome::Reject(3));
        };
        let mut state = catalog::parse_partition(&raw)?;
        if cut.end < state.log_start || cut.end > state.next_offset {
            return Ok(Outcome::Reject(INVALID_REQUEST));
        }
        // Extents that end after the cut: from a row that predates extent
        // keys, which this migrates, or from the keys.
        let mut after: Vec<(String, catalog::Extent)> = Vec::new();
        let mut migrated = Vec::new();
        catalog::migrate_extents(&id, cut.partition, &mut state, &mut migrated)?;
        if migrated.is_empty() {
            let (mut from, to) = catalog::extent_range(&id, cut.partition, cut.end);
            loop {
                let page = scan(&from, &to, CUT_PAGE)?;
                let full = page.len() >= CUT_PAGE;
                dropped += page.len();
                if dropped > MAX_CUT_EXTENTS {
                    return Ok(Outcome::Reject(POLICY_VIOLATION));
                }
                for (key, raw) in page {
                    from = format!("{key}\0");
                    after.push((key, serde_json::from_str(&raw).map_err(|e| e.to_string())?));
                }
                if !full {
                    break;
                }
            }
        } else {
            for (key, raw) in migrated {
                let extent: catalog::Extent =
                    serde_json::from_str(&raw).map_err(|e| e.to_string())?;
                if extent.end > cut.end {
                    after.push((key, extent));
                } else {
                    put.insert(key, raw);
                }
            }
        }
        let mut shortened = false;
        for (key, extent) in after {
            del.insert(key);
            if extent.start >= cut.end {
                continue;
            }
            match &cut.shorten {
                Some(keep)
                    if keep.extent_end == extent.end
                        && keep.object_key == extent.key
                        && keep.length <= extent.length =>
                {
                    shortened = true;
                    // An empty front keeps nothing readable: drop it whole.
                    if keep.length == 0 {
                        continue;
                    }
                    let kept = catalog::Extent {
                        end: cut.end,
                        length: keep.length,
                        max_timestamp: keep.max_timestamp,
                        stamps: keep.stamps,
                        ..extent
                    };
                    let (key, raw) = catalog::extent_put(&id, cut.partition, &kept)?;
                    del.remove(&key);
                    put.insert(key, raw);
                }
                _ => return Ok(Outcome::Reject(INVALID_REQUEST)),
            }
        }
        if cut.shorten.is_some() && !shortened {
            return Ok(Outcome::Reject(INVALID_REQUEST));
        }
        let end = cut.end;
        state.next_offset = end;
        let first = state.epochs.first().cloned();
        let highest = state.epochs.iter().map(|epoch| epoch.epoch).max();
        state.epochs.retain(|epoch| epoch.start < end);
        if state.epochs.is_empty() {
            state.epochs.extend(first);
        }
        if let Some(highest) = highest.filter(|highest| {
            state
                .epochs
                .last()
                .is_some_and(|last| last.epoch < *highest)
        }) {
            state.epochs.push(catalog::EpochStart {
                epoch: highest,
                start: end,
            });
        }
        // Synced group offsets past the cut would skip what producers
        // write there after promotion.
        let alias = topic
            .mirror_of
            .as_ref()
            .map(|of| of.alias.clone())
            .unwrap_or_default();
        for group in groups_of(get, &alias)? {
            let key = crate::groups::offset_key(&group, &id, cut.partition);
            let Some(raw) = get(&key)? else {
                continue;
            };
            let mut record: crate::groups::OffsetRecord =
                serde_json::from_str(&raw).map_err(|err| err.to_string())?;
            if record.offset > end {
                record.offset = end;
                record.leader_epoch = -1;
                put.insert(
                    key,
                    serde_json::to_string(&record).map_err(|err| err.to_string())?,
                );
            }
        }
        let mut aborted: Vec<catalog::Span> = Vec::new();
        for span in std::mem::take(&mut state.txn_committed) {
            if span.end <= end {
                state.txn_committed.push(span);
            } else if span.start < end {
                aborted.push(catalog::Span { end, ..span });
            }
        }
        for span in std::mem::take(&mut state.txn_aborted) {
            if span.end <= end {
                aborted.push(span);
            } else if span.start < end {
                aborted.push(catalog::Span { end, ..span });
            }
        }
        state.txn_aborted = aborted;
        put.insert(
            key,
            serde_json::to_string(&state).map_err(|err| err.to_string())?,
        );
    }
    for (topic, seen) in topics.values() {
        if seen.len() != usize::try_from(topic.partitions).unwrap_or(0) {
            return Ok(Outcome::Reject(INVALID_REQUEST));
        }
    }
    // Every mirror, when the cut says it covers all of them: one made
    // since the plan is refused, not left uncut.
    if all {
        let (mut from, to) = ("topic/".to_string(), "topic0".to_string());
        loop {
            let page = scan(&from, &to, CUT_PAGE)?;
            let full = page.len() >= CUT_PAGE;
            for (key, raw) in page {
                from = format!("{key}\0");
                let topic = catalog::parse_topic(&raw)?;
                if topic.mirror_of.is_some() && !topics.contains_key(&topic.name) {
                    return Ok(Outcome::Reject(INVALID_REQUEST));
                }
            }
            if !full {
                break;
            }
        }
    }
    for name in topics.keys() {
        let promoted = {
            let overlay = |key: &str| -> Result<Option<String>, String> {
                if del.contains(key) {
                    return Ok(None);
                }
                match put.get(key) {
                    Some(raw) => Ok(Some(raw.clone())),
                    None => get(key),
                }
            };
            promote(&overlay, name)?
        };
        match promoted {
            Outcome::Change {
                put: more,
                del: gone,
                ..
            } => {
                for (key, raw) in more {
                    del.remove(&key);
                    put.insert(key, raw);
                }
                for key in gone {
                    put.remove(&key);
                    del.insert(key);
                }
            }
            Outcome::Reject(code) => return Ok(Outcome::Reject(code)),
        }
    }
    Ok(Outcome::Change {
        put: put.into_iter().collect(),
        del: del.into_iter().collect(),
        value: None,
    })
}

fn unchanged() -> Outcome {
    Outcome::Change {
        put: Vec::new(),
        del: Vec::new(),
        value: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn link(alias: &str, bucket: &str) -> MirrorLink {
        MirrorLink {
            alias: alias.into(),
            endpoint: "https://source.example".into(),
            region: "auto".into(),
            bucket: bucket.into(),
            prefix: String::new(),
            virtual_host: false,
        }
    }

    fn apply(map: &mut HashMap<String, String>, outcome: Outcome) -> Result<i16, String> {
        match outcome {
            Outcome::Change { put, del, .. } => {
                for key in del {
                    map.remove(&key);
                }
                map.extend(put);
                Ok(0)
            }
            Outcome::Reject(code) => Ok(code),
        }
    }

    fn add(map: &mut HashMap<String, String>, to: &MirrorLink) -> Result<i16, String> {
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let scan = |from: &str, to: &str, limit: usize| {
            let mut rows: Vec<(String, String)> = snapshot
                .iter()
                .filter(|(key, _)| key.as_str() >= from && key.as_str() < to)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            rows.sort();
            rows.truncate(limit);
            Ok(rows)
        };
        let outcome = add_link(&get, &scan, to)?;
        apply(map, outcome)
    }

    fn remove(map: &mut HashMap<String, String>, alias: &str) -> Result<i16, String> {
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let outcome = remove_link(&get, alias)?;
        apply(map, outcome)
    }

    fn mirror(map: &mut HashMap<String, String>, name: &str, alias: &str) -> Result<i16, String> {
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let of = MirrorOf {
            alias: alias.into(),
            topic_id: "source-id".into(),
        };
        let outcome = create_topic(&get, name, &format!("{name}-id"), 3, &of)?;
        apply(map, outcome)
    }

    #[test]
    fn aliases_are_short_lowercase_names() {
        for good in ["a", "us-east", "r2-0", &"x".repeat(32)] {
            assert!(valid_alias(good), "{good}");
        }
        for bad in ["", "US", "us_east", "us.east", "a/b", &"x".repeat(33)] {
            assert!(!valid_alias(bad), "{bad}");
        }
    }

    #[test]
    fn a_link_is_added_once_and_a_retry_is_a_no_op() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(add(&mut map, &link("us-east", "other"))?, POLICY_VIOLATION);
        assert_eq!(add(&mut map, &link("US", "logs"))?, INVALID_REQUEST);
        assert_eq!(add(&mut map, &link("eu", ""))?, INVALID_REQUEST);
        assert_eq!(
            add(&mut map, &link("eu", &"b".repeat(64)))?,
            INVALID_REQUEST
        );
        let stored = map.get(&link_key("us-east")).ok_or("no link")?;
        assert_eq!(
            serde_json::from_str::<MirrorLink>(stored)?,
            link("us-east", "logs")
        );
        Ok(())
    }

    #[test]
    fn a_mirror_needs_its_link_and_holds_it() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(mirror(&mut map, "events", "us-east")?, INVALID_REQUEST);
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        let raw = map
            .get(&catalog::topic_storage_key("events"))
            .ok_or("no topic")?;
        let topic = catalog::parse_topic(raw)?;
        assert_eq!(topic.durability, "r2");
        assert_eq!(
            topic.mirror_of.map(|of| of.alias),
            Some("us-east".to_string())
        );

        assert_eq!(remove(&mut map, "us-east")?, POLICY_VIOLATION);
        assert_eq!(remove(&mut map, "missing")?, 0);
        assert_eq!(remove(&mut map, "a/b")?, INVALID_REQUEST);
        Ok(())
    }

    #[test]
    fn a_mirror_refuses_a_publish_from_a_stale_broker() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        let get = |key: &str| Ok(map.get(key).cloned());
        let part = catalog::PublishPart {
            epoch: 0,
            topic: "events".into(),
            partition: 0,
            start: 0,
            end: 1,
            object_key: "bundle/x.seg".into(),
            file_offset: 0,
            length: 10,
            base_timestamp: 0,
            max_timestamp: 0,
        };
        assert!(matches!(
            catalog::publish(&get, &[part], &[])?,
            Outcome::Reject(3)
        ));
        Ok(())
    }

    fn part(topic: &str, start: i64, end: i64) -> catalog::PublishPart {
        catalog::PublishPart {
            epoch: 0,
            topic: topic.into(),
            partition: 0,
            start,
            end,
            object_key: "bundle/copied.seg".into(),
            file_offset: 0,
            length: 10,
            base_timestamp: 0,
            max_timestamp: 0,
        }
    }

    fn publish_copied(
        map: &mut HashMap<String, String>,
        parts: &[catalog::PublishPart],
    ) -> Result<i16, String> {
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let outcome = catalog::publish_mirror(&get, parts, &[], &[])?;
        apply(map, outcome)
    }

    fn partition(map: &HashMap<String, String>, topic: &str) -> Result<catalog::Partition, String> {
        let raw = map
            .get(&catalog::topic_storage_key(topic))
            .ok_or("no topic")?;
        let topic = catalog::parse_topic(raw)?;
        let raw = map
            .get(&catalog::partition_storage_key(&topic.id, 0))
            .ok_or("no partition")?;
        catalog::parse_partition(raw)
    }

    #[test]
    fn a_copy_publishes_only_into_a_mirror_and_in_order() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        // A new mirror starts at the source's log start, 100.
        assert_eq!(publish_copied(&mut map, &[part("events", 100, 150)])?, 0);
        let state = partition(&map, "events")?;
        assert_eq!((state.log_start, state.next_offset), (100, 150));
        // After that, only the next offset.
        assert_eq!(publish_copied(&mut map, &[part("events", 160, 170)])?, 1);
        assert_eq!(publish_copied(&mut map, &[part("events", 140, 170)])?, 1);
        assert_eq!(publish_copied(&mut map, &[part("events", 150, 170)])?, 0);
        assert_eq!(partition(&map, "events")?.next_offset, 170);

        // A plain topic takes no copied batches.
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let created = catalog::create_topic(&get, "plain", "plain-id", 1, "r2", &[])?;
        assert_eq!(apply(&mut map, created)?, 0);
        assert_eq!(publish_copied(&mut map, &[part("plain", 0, 10)])?, 3);
        Ok(())
    }

    #[test]
    fn a_copy_carries_the_source_spans_once() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        let span = |start, end| catalog::Span {
            start,
            end,
            producer_id: Some(7),
        };
        for parts in [[part("events", 0, 10)], [part("events", 10, 20)]] {
            let snapshot = map.clone();
            let get = |key: &str| Ok(snapshot.get(key).cloned());
            let outcome = catalog::publish_mirror(&get, &parts, &[span(2, 3)], &[span(0, 1)])?;
            assert_eq!(apply(&mut map, outcome)?, 0);
        }
        let state = partition(&map, "events")?;
        assert_eq!(
            state.txn_committed,
            vec![span(2, 3)],
            "a span copied twice is held once"
        );
        assert_eq!(state.txn_aborted, vec![span(0, 1)]);
        Ok(())
    }

    #[test]
    fn a_reset_empties_a_mirror_partition_for_a_fresh_start() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        assert_eq!(publish_copied(&mut map, &[part("events", 0, 40)])?, 0);
        let scan_of = |map: &HashMap<String, String>, from: &str, to: &str, limit: usize| {
            let mut rows: Vec<(String, String)> = map
                .iter()
                .filter(|(key, _)| key.as_str() >= from && key.as_str() < to)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            rows.sort();
            rows.truncate(limit);
            rows
        };
        assert!(!scan_of(&map, "ext/", "ext0", 10).is_empty());
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let scan = |from: &str, to: &str, limit: usize| Ok(scan_of(&snapshot, from, to, limit));
        let reset = reset_partition(&get, &scan, "events", 0)?;
        assert_eq!(apply(&mut map, reset)?, 0);
        let state = partition(&map, "events")?;
        assert_eq!((state.log_start, state.next_offset), (40, 40));
        assert!(
            scan_of(&map, "ext/", "ext0", 10).is_empty(),
            "the copy is dropped"
        );
        // Empty again, so the copier may start at the source's log start.
        assert_eq!(publish_copied(&mut map, &[part("events", 90, 100)])?, 0);
        assert_eq!(partition(&map, "events")?.log_start, 90);

        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let scan = |_: &str, _: &str, _: usize| Ok(Vec::new());
        assert!(matches!(
            reset_partition(&get, &scan, "events", 5)?,
            Outcome::Reject(3)
        ));
        assert!(matches!(
            reset_partition(&get, &scan, "nope", 0)?,
            Outcome::Reject(3)
        ));
        Ok(())
    }

    fn cut_all(map: &mut HashMap<String, String>, cuts: &[MirrorCut]) -> Result<i16, String> {
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let scan = |from: &str, to: &str, limit: usize| {
            let mut rows: Vec<(String, String)> = snapshot
                .iter()
                .filter(|(key, _)| key.as_str() >= from && key.as_str() < to)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            rows.sort();
            rows.truncate(limit);
            Ok(rows)
        };
        let outcome = cut(&get, &scan, cuts, true)?;
        apply(map, outcome)
    }

    #[test]
    fn a_cut_truncates_every_partition_and_promotes_all_or_nothing() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        for (start, end) in [(0, 10), (10, 20), (20, 30)] {
            assert_eq!(publish_copied(&mut map, &[part("events", start, end)])?, 0);
        }
        // A committed transaction the cut falls inside, and an aborted one
        // wholly after it.
        let mut state = partition(&map, "events")?;
        let span = |start, end| catalog::Span {
            start,
            end,
            producer_id: Some(7),
        };
        state.txn_committed = vec![span(2, 4), span(12, 25)];
        state.txn_aborted = vec![span(26, 29)];
        state.epochs = vec![
            catalog::EpochStart { epoch: 1, start: 0 },
            catalog::EpochStart {
                epoch: 2,
                start: 10,
            },
            catalog::EpochStart {
                epoch: 3,
                start: 20,
            },
        ];
        // A synced group that read past where the cut will fall.
        map.insert(groups_key("us-east"), serde_json::to_string(&["g"])?);
        let offset = |offset| crate::groups::OffsetRecord {
            offset,
            leader_epoch: 3,
            metadata: None,
            commit_timestamp: 0,
        };
        map.insert(
            crate::groups::offset_key("g", "events-id", 0),
            serde_json::to_string(&offset(25))?,
        );
        map.insert(
            crate::groups::offset_key("g", "events-id", 1),
            serde_json::to_string(&offset(0))?,
        );
        map.insert(
            catalog::partition_storage_key("events-id", 0),
            serde_json::to_string(&state)?,
        );
        let at = |partition, end, shorten| MirrorCut {
            topic: "events".into(),
            topic_id: "events-id".into(),
            partition,
            end,
            shorten,
        };
        let keep = |extent_end, length| Shorten {
            extent_end,
            object_key: "bundle/copied.seg".into(),
            length,
            max_timestamp: 14,
            stamps: Some(catalog::StampRange { min: 1, max: 2 }),
        };
        let before = map.clone();
        // A partition left out, a cut past the end, a front that names
        // another extent or none, and a topic made again under the name
        // are refused, and change nothing.
        for cuts in [
            vec![at(0, 15, Some(keep(20, 5))), at(1, 0, None)],
            vec![at(0, 31, None), at(1, 0, None), at(2, 0, None)],
            vec![at(0, 15, Some(keep(30, 5))), at(1, 0, None), at(2, 0, None)],
            vec![at(0, 15, None), at(1, 0, None), at(2, 0, None)],
            vec![
                MirrorCut {
                    topic_id: "another-id".into(),
                    ..at(0, 15, Some(keep(20, 5)))
                },
                at(1, 0, None),
                at(2, 0, None),
            ],
        ] {
            assert_ne!(cut_all(&mut map, &cuts)?, 0);
            assert_eq!(map, before);
        }

        let cuts = [at(0, 15, Some(keep(20, 5))), at(1, 0, None), at(2, 0, None)];
        assert_eq!(cut_all(&mut map, &cuts)?, 0);
        let state = partition(&map, "events")?;
        assert_eq!(state.next_offset, 15);
        let epochs: Vec<(i32, i64)> = state
            .epochs
            .iter()
            .map(|epoch| (epoch.epoch, epoch.start))
            .collect();
        assert_eq!(
            epochs,
            vec![(1, 0), (2, 10), (3, 15), (4, 15)],
            "epoch 3 ends at the cut, and the new epoch is above it"
        );
        let synced = |partition| -> Result<i64, Box<dyn std::error::Error>> {
            let raw = map
                .get(&crate::groups::offset_key("g", "events-id", partition))
                .ok_or("no offset")?;
            Ok(serde_json::from_str::<crate::groups::OffsetRecord>(raw)?.offset)
        };
        assert_eq!((synced(0)?, synced(1)?), (15, 0), "brought back to the cut");
        assert_eq!(state.txn_committed, vec![span(2, 4)]);
        assert_eq!(
            state.txn_aborted,
            vec![span(12, 15)],
            "a commit after the cut did not happen by it"
        );
        let ends: Vec<String> = {
            let mut keys: Vec<String> = map
                .keys()
                .filter(|key| key.starts_with("ext/"))
                .cloned()
                .collect();
            keys.sort();
            keys
        };
        assert_eq!(
            ends,
            vec![
                catalog::extent_key("events-id", 0, 10),
                catalog::extent_key("events-id", 0, 15),
            ]
        );
        let raw = map
            .get(&catalog::extent_key("events-id", 0, 15))
            .ok_or("no shortened extent")?;
        let kept: catalog::Extent = serde_json::from_str(raw)?;
        assert_eq!((kept.start, kept.length, kept.max_timestamp), (10, 5, 14));
        let raw = map
            .get(&catalog::topic_storage_key("events"))
            .ok_or("no topic")?;
        let topic = catalog::parse_topic(raw)?;
        assert!(topic.mirror_of.is_none(), "promoted");
        assert_eq!(
            topic.promoted.map(|promoted| promoted.at),
            Some(vec![15, 0, 0])
        );
        // Promoted, so a second cut is refused.
        assert_eq!(cut_all(&mut map, &cuts)?, INVALID_REQUEST);
        Ok(())
    }

    #[test]
    fn a_cut_of_all_mirrors_refuses_one_left_out() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        assert_eq!(mirror(&mut map, "audit", "us-east")?, 0);
        let cuts: Vec<MirrorCut> = (0..3)
            .map(|partition| MirrorCut {
                topic: "events".into(),
                topic_id: "events-id".into(),
                partition,
                end: 0,
                shorten: None,
            })
            .collect();
        let before = map.clone();
        assert_eq!(cut_all(&mut map, &cuts)?, INVALID_REQUEST);
        assert_eq!(map, before);
        Ok(())
    }

    fn set(map: &mut HashMap<String, String>, alias: &str, groups: &[&str]) -> Result<i16, String> {
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let groups: Vec<String> = groups.iter().map(|group| group.to_string()).collect();
        let outcome = set_groups(&get, alias, &groups)?;
        apply(map, outcome)
    }

    #[test]
    fn a_link_syncs_a_capped_list_of_groups() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(
            set(&mut map, "us-east", &["g"])?,
            INVALID_REQUEST,
            "no link yet"
        );
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(set(&mut map, "us-east", &["b", "a", "b"])?, 0);
        let get = |key: &str| Ok(map.get(key).cloned());
        assert_eq!(
            groups_of(&get, "us-east")?,
            vec!["a".to_string(), "b".to_string()]
        );
        let many: Vec<String> = (0..=MAX_GROUPS).map(|at| format!("g{at}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert_eq!(set(&mut map, "us-east", &many)?, INVALID_REQUEST);
        assert_eq!(set(&mut map, "us-east", &[""])?, INVALID_REQUEST);
        assert_eq!(set(&mut map, "us-east", &[])?, 0);
        assert!(
            !map.contains_key(&groups_key("us-east")),
            "an empty list clears it"
        );
        assert_eq!(set(&mut map, "us-east", &["a"])?, 0);
        assert_eq!(remove(&mut map, "us-east")?, 0);
        assert!(
            !map.contains_key(&groups_key("us-east")),
            "removed with its link"
        );
        Ok(())
    }

    #[test]
    fn a_promoted_mirror_is_writable_and_keeps_where_it_stopped() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(mirror(&mut map, "events", "us-east")?, 0);
        assert_eq!(publish_copied(&mut map, &[part("events", 0, 40)])?, 0);
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        let promoted = promote(&get, "events")?;
        assert_eq!(apply(&mut map, promoted)?, 0);
        let raw = map
            .get(&catalog::topic_storage_key("events"))
            .ok_or("no topic")?;
        let topic = catalog::parse_topic(raw)?;
        assert!(topic.mirror_of.is_none());
        assert_eq!(
            topic.promoted.as_ref().map(|it| it.at.clone()),
            Some(vec![40, 0, 0])
        );
        let epochs = partition(&map, "events")?.epochs;
        assert_eq!(
            epochs.last().map(|last| (last.epoch, last.start)),
            Some((1, 40)),
            "a new epoch starts where it was promoted"
        );
        let get = |key: &str| Ok(map.get(key).cloned());
        assert!(!all_mirrors(&get, [topic.id.as_str()])?);
        // No longer a mirror: copies are refused, the link can go.
        assert_eq!(publish_copied(&mut map, &[part("events", 40, 41)])?, 3);
        assert_eq!(remove(&mut map, "us-east")?, 0);
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        assert!(
            matches!(promote(&get, "events")?, Outcome::Change { ref put, .. } if put.is_empty()),
            "a retried promote changes nothing"
        );
        assert!(matches!(promote(&get, "nope")?, Outcome::Reject(3)));
        Ok(())
    }

    #[test]
    fn an_unused_link_is_removed() -> TestResult {
        let mut map = HashMap::new();
        assert_eq!(add(&mut map, &link("us-east", "logs"))?, 0);
        assert_eq!(remove(&mut map, "us-east")?, 0);
        assert!(!map.contains_key(&link_key("us-east")));
        Ok(())
    }

    #[test]
    fn links_stop_at_the_cap() -> TestResult {
        let mut map = HashMap::new();
        for at in 0..MAX_LINKS {
            assert_eq!(add(&mut map, &link(&format!("l{at}"), "logs"))?, 0);
        }
        assert_eq!(add(&mut map, &link("one-more", "logs"))?, POLICY_VIOLATION);
        assert_eq!(add(&mut map, &link("l0", "logs"))?, 0, "a retry at the cap");
        Ok(())
    }

    #[test]
    fn a_restated_default_is_not_a_change() {
        let mut before = Topic::new("events", "id", 1, "r2");
        before
            .overrides
            .insert("max.message.bytes".into(), "1048588".into());
        let after = Topic::new("events", "id", 1, "r2");
        assert!(may_change(&before, &after));
    }

    #[test]
    fn only_retention_may_change_on_a_mirror() {
        let before = Topic::new("events", "id", 1, "r2");
        let mut retention = before.clone();
        retention.retention_ms = 1_000;
        retention
            .overrides
            .insert("retention.ms".into(), "1000".into());
        assert!(may_change(&before, &retention));
        let mut cleanup = before.clone();
        cleanup.cleanup_policy = "compact".into();
        assert!(!may_change(&before, &cleanup));
        let mut grown = before.clone();
        grown.partitions = 2;
        assert!(!may_change(&before, &grown));
    }
}
