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

/// Orphans remembered between passes. A larger set is not tracked until a
/// later pass, which delays deletion and does not delete a live object.
const ORPHAN_CAP: usize = 65_536;

/// The sweep's state between passes: when each unreferenced object was
/// first seen that way. At most [`ORPHAN_CAP`] keys.
#[derive(Debug, Default)]
pub struct Sweeper {
    unreferenced_since: BTreeMap<String, i64>,
    /// Keys observed in the pass that is open. Empty when no pass is open.
    seen: BTreeSet<String>,
}

impl Sweeper {
    /// Start a pass. Pages of one listing call [`Sweeper::observe`] and then
    /// [`Sweeper::finish`], so a key that is only on a later page keeps the
    /// time it was first seen.
    pub fn begin(&mut self) {
        self.seen.clear();
    }

    /// Objects in this page to delete now. `listed` is one page of a listing
    /// that was started before `referenced` was read.
    pub fn observe(
        &mut self,
        listed: &[Listed],
        referenced: &BTreeSet<String>,
        now_ms: i64,
        min_age_ms: i64,
        delete_delay_ms: i64,
    ) -> Vec<Listed> {
        let mut delete = Vec::new();
        for object in listed {
            if referenced.contains(&object.key) {
                continue;
            }
            let age = now_ms.saturating_sub(created_ms(&object.key, object.modified_ms));
            if age < min_age_ms {
                continue;
            }
            let known = self.unreferenced_since.contains_key(&object.key);
            if !known && self.unreferenced_since.len() >= ORPHAN_CAP {
                continue;
            }
            let since = self
                .unreferenced_since
                .get(&object.key)
                .copied()
                .unwrap_or(now_ms);
            self.unreferenced_since.insert(object.key.clone(), since);
            self.seen.insert(object.key.clone());
            if now_ms.saturating_sub(since) >= delete_delay_ms {
                delete.push(object.clone());
            }
        }
        for object in &delete {
            self.unreferenced_since.remove(&object.key);
            self.seen.remove(&object.key);
        }
        delete
    }

    /// Drop orphans that this pass's pages did not list. They were deleted
    /// by someone else, or the listing no longer contains them.
    pub fn finish(&mut self) {
        let seen = std::mem::take(&mut self.seen);
        self.unreferenced_since.retain(|key, _| seen.contains(key));
    }

    /// One listing, as a single page. `listed` must be read before
    /// `referenced`, so an object published between the two reads is either
    /// referenced or too young to delete.
    pub fn plan(
        &mut self,
        listed: &[Listed],
        referenced: &BTreeSet<String>,
        now_ms: i64,
        min_age_ms: i64,
        delete_delay_ms: i64,
    ) -> Vec<Listed> {
        self.begin();
        let delete = self.observe(listed, referenced, now_ms, min_age_ms, delete_delay_ms);
        self.finish();
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
    fn a_pass_split_across_pages_remembers_both_orphans() {
        let mut sweeper = Sweeper::default();
        let first = bundle(0);
        let second = bundle(1);
        let none = BTreeSet::new();
        let now = 10 * HOUR;
        sweeper.begin();
        assert!(sweeper
            .observe(&[first.clone()], &none, now, HOUR, 60_000)
            .is_empty());
        assert!(sweeper
            .observe(&[second.clone()], &none, now, HOUR, 60_000)
            .is_empty());
        sweeper.finish();
        sweeper.begin();
        assert_eq!(
            sweeper.observe(&[first.clone()], &none, now + 60_000, HOUR, 60_000),
            vec![first]
        );
        sweeper.finish();
        assert!(sweeper
            .plan(&[second], &none, now + 60_000, HOUR, 60_000)
            .is_empty());
    }

    #[test]
    fn past_the_cap_a_new_orphan_is_not_deleted() {
        let mut sweeper = Sweeper::default();
        let held: Vec<Listed> = (0..ORPHAN_CAP)
            .map(|i| Listed {
                key: format!("orphan-{i}"),
                size: 1,
                modified_ms: 0,
            })
            .collect();
        let extra = Listed {
            key: "orphan-extra".to_string(),
            size: 1,
            modified_ms: 0,
        };
        let none = BTreeSet::new();
        let now = 10 * HOUR;
        sweeper.begin();
        assert!(sweeper.observe(&held, &none, now, HOUR, 60_000).is_empty());
        assert!(sweeper.observe(&[extra], &none, now, HOUR, 0).is_empty());
        sweeper.finish();
        assert_eq!(
            sweeper.plan(&held, &none, now + 60_000, HOUR, 60_000).len(),
            ORPHAN_CAP
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
