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
/// offset is kept as where it was promoted. Refused on a plain topic.
pub fn promote(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    name: &str,
) -> Result<Outcome, String> {
    let Some(raw) = get(&catalog::topic_storage_key(name))? else {
        return Ok(Outcome::Reject(3));
    };
    let mut topic = catalog::parse_topic(&raw)?;
    let Some(of) = topic.mirror_of.take() else {
        return Ok(Outcome::Reject(INVALID_REQUEST));
    };
    let mut at = Vec::with_capacity(topic.partitions.max(0) as usize);
    for partition in 0..topic.partitions {
        let raw = get(&catalog::partition_storage_key(&topic.id, partition))?
            .ok_or_else(|| format!("{name} has no partition {partition}"))?;
        at.push(catalog::parse_partition(&raw)?.next_offset);
    }
    topic.promoted = Some(Promoted { of, at });
    Ok(Outcome::Change {
        put: vec![(
            catalog::topic_storage_key(name),
            serde_json::to_string(&topic).map_err(|err| err.to_string())?,
        )],
        del: Vec::new(),
        value: None,
    })
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
        // No longer a mirror: copies are refused, the link can go.
        assert_eq!(publish_copied(&mut map, &[part("events", 40, 41)])?, 3);
        assert_eq!(remove(&mut map, "us-east")?, 0);
        let snapshot = map.clone();
        let get = |key: &str| Ok(snapshot.get(key).cloned());
        assert!(matches!(
            promote(&get, "events")?,
            Outcome::Reject(INVALID_REQUEST)
        ));
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
