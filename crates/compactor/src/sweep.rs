//! The orphan sweep: delete bundle objects that no extent references.
//!
//! An object becomes garbage when retention, `DeleteRecords`, compaction, or
//! a topic delete removes the last extent that points into it, or when an
//! upload's metadata commit never happened. Two delays keep the sweep from
//! deleting what is still needed:
//!
//! - `min_age_ms` (ADR 0007: one hour) skips objects whose upload may still
//!   be waiting for its metadata commit. Age comes from the ULID in the key.
//! - `delete_delay_ms` (Kafka's `file.delete.delay.ms`, 60 s) waits after an
//!   object is first seen unreferenced, so a fetch that read the extent just
//!   before it was dropped can still read the bytes.

use std::collections::{BTreeMap, BTreeSet};

/// One object from a bucket listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// Object key.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Last-modified time from the listing, in Unix milliseconds.
    pub modified_ms: i64,
}

/// When `key` was created: the ULID timestamp in `bundle/<ulid>.seg`, or
/// `fallback_ms` for any other key.
pub fn created_ms(key: &str, fallback_ms: i64) -> i64 {
    key.strip_prefix("bundle/")
        .and_then(|rest| rest.strip_suffix(".seg"))
        .and_then(|id| ulid::Ulid::from_string(id).ok())
        .map(|id| i64::try_from(id.timestamp_ms()).unwrap_or(i64::MAX))
        .unwrap_or(fallback_ms)
}

/// The sweep's state between passes: when each unreferenced object was
/// first seen that way.
#[derive(Debug, Default)]
pub struct Sweeper {
    unreferenced_since: BTreeMap<String, i64>,
}

impl Sweeper {
    /// Objects to delete now. `listed` must be read before `referenced`, so
    /// an object published between the two reads is either referenced or
    /// too young to delete.
    pub fn plan(
        &mut self,
        listed: &[Listed],
        referenced: &BTreeSet<String>,
        now_ms: i64,
        min_age_ms: i64,
        delete_delay_ms: i64,
    ) -> Vec<Listed> {
        let mut seen = BTreeMap::new();
        let mut delete = Vec::new();
        for object in listed {
            if referenced.contains(&object.key) {
                continue;
            }
            let age = now_ms.saturating_sub(created_ms(&object.key, object.modified_ms));
            if age < min_age_ms {
                continue;
            }
            let since = self
                .unreferenced_since
                .get(&object.key)
                .copied()
                .unwrap_or(now_ms);
            seen.insert(object.key.clone(), since);
            if now_ms.saturating_sub(since) >= delete_delay_ms {
                delete.push(object.clone());
            }
        }
        for object in &delete {
            seen.remove(&object.key);
        }
        self.unreferenced_since = seen;
        delete
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;

    fn bundle(created: i64) -> Listed {
        let id = ulid::Ulid::from_parts(created as u64, 7);
        Listed {
            key: format!("bundle/{id}.seg"),
            size: 10,
            modified_ms: created,
        }
    }

    #[test]
    fn the_key_carries_its_creation_time() {
        let object = bundle(1_700_000_000_000);
        assert_eq!(created_ms(&object.key, 0), 1_700_000_000_000);
        assert_eq!(created_ms("other/key", 42), 42);
    }

    #[test]
    fn referenced_and_young_objects_are_kept() {
        let mut sweeper = Sweeper::default();
        let now = 10 * HOUR;
        let live = bundle(0);
        let young = bundle(now - HOUR / 2);
        let referenced = BTreeSet::from([live.key.clone()]);
        let plan = sweeper.plan(&[live, young], &referenced, now, HOUR, 0);
        assert!(plan.is_empty());
    }

    #[test]
    fn an_orphan_is_deleted_only_after_the_delay() {
        let mut sweeper = Sweeper::default();
        let orphan = bundle(0);
        let listed = [orphan.clone()];
        let none = BTreeSet::new();
        let now = 10 * HOUR;
        assert!(sweeper.plan(&listed, &none, now, HOUR, 60_000).is_empty());
        assert!(sweeper
            .plan(&listed, &none, now + 30_000, HOUR, 60_000)
            .is_empty());
        assert_eq!(
            sweeper.plan(&listed, &none, now + 60_000, HOUR, 60_000),
            vec![orphan]
        );
    }

    #[test]
    fn an_object_referenced_again_restarts_its_delay() {
        let mut sweeper = Sweeper::default();
        let object = bundle(0);
        let listed = [object.clone()];
        let none = BTreeSet::new();
        let live = BTreeSet::from([object.key.clone()]);
        let now = 10 * HOUR;
        assert!(sweeper.plan(&listed, &none, now, HOUR, 60_000).is_empty());
        assert!(sweeper
            .plan(&listed, &live, now + 30_000, HOUR, 60_000)
            .is_empty());
        assert!(sweeper
            .plan(&listed, &none, now + 61_000, HOUR, 60_000)
            .is_empty());
        assert_eq!(
            sweeper
                .plan(&listed, &none, now + 121_000, HOUR, 60_000)
                .len(),
            1
        );
    }
}
