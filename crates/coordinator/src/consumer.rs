//! KIP-848 consumer groups (`ConsumerGroupHeartbeat`).
//!
//! Checked against Kafka 4.4.0 `GroupMetadataManager.consumerGroupHeartbeat`,
//! `modern/consumer/CurrentAssignmentBuilder`, `ConsumerGroup.validateOffsetCommit`,
//! and `assignor/RangeAssignor`. `uniform` is sticky, like Kafka's
//! `UniformAssignor`: a member keeps the partitions it has while it stays
//! subscribed and under its fair share, and only the rest move. Rack-aware
//! placement is not implemented. A topic regex is a Rust `regex` pattern. An
//! offset commit is accepted at the member's current epoch or the one just
//! before it.
//!
//! The group epoch and each member's epoch, subscription, and assignment are
//! stored in the group record before the response is sent.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use regex::Regex;
use uuid::Uuid;

use camus_metadata::groups::{GroupRecord, MemberRecord};

use super::{codes, Config, Group, Member, State};

/// One topic the broker currently has.
#[derive(Clone, Debug)]
pub struct TopicMeta {
    pub name: String,
    /// Topic id.
    pub id: Uuid,
    pub partitions: i32,
}

/// A `ConsumerGroupHeartbeat` request.
#[derive(Clone, Debug)]
pub struct ConsumerHeartbeat {
    pub group: String,
    pub member_id: String,
    /// 0 joins, -1 leaves, -2 is a static member leaving temporarily.
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    /// -1 when the member left the field out.
    pub rebalance_timeout_ms: i32,
    /// `None` when the subscription did not change.
    pub subscribed_topics: Option<Vec<String>>,
    pub subscribed_regex: Option<String>,
    /// `None` when the member left the assignor out. Empty is the same.
    pub server_assignor: Option<String>,
    /// Partitions the member still owns. `None` when it left the field out.
    pub owned: Option<Vec<(Uuid, Vec<i32>)>>,
    pub client_id: String,
    /// `/<ip>` of the client, as Kafka reports it.
    pub client_host: String,
    pub version: i16,
}

/// A `ConsumerGroupHeartbeat` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerHeartbeatResponse {
    pub error: i16,
    pub message: Option<String>,
    pub member_id: String,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    /// `None` when the assignment did not change.
    pub assignment: Option<Vec<(Uuid, Vec<i32>)>>,
}

/// What to put back if the metadata write fails.
pub(super) enum Restore {
    Keep,
    Remove,
    Record(GroupRecord),
}

pub(super) struct Outcome {
    pub response: ConsumerHeartbeatResponse,
    pub save: Option<GroupRecord>,
    pub restore: Restore,
}

type Parts = BTreeMap<String, BTreeMap<i32, i32>>;

pub(super) fn apply(
    groups: &mut HashMap<String, Group>,
    config: &Config,
    request: &ConsumerHeartbeat,
    topics: &[TopicMeta],
    now: Instant,
) -> Outcome {
    if request.group.is_empty() {
        return done(fail(codes::INVALID_GROUP_ID, &request.member_id));
    }
    if request.member_epoch < -2 {
        return done(fail(codes::INVALID_REQUEST, &request.member_id));
    }
    if request.member_epoch == -1 || request.member_epoch == -2 {
        return leave(groups, request);
    }
    if request.version >= 1 && request.member_id.is_empty() {
        return done(fail(codes::INVALID_REQUEST, ""));
    }
    if let Some(name) = request
        .server_assignor
        .as_ref()
        .filter(|name| !name.is_empty())
    {
        if name != "uniform" && name != "range" {
            return done(fail(codes::UNSUPPORTED_ASSIGNOR, &request.member_id));
        }
    }
    if let Some(pattern) = request
        .subscribed_regex
        .as_ref()
        .filter(|pattern| !pattern.is_empty())
    {
        if Regex::new(pattern).is_err() {
            return done(fail(codes::INVALID_REQUEST, &request.member_id));
        }
    }

    let member_id = if request.member_id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        request.member_id.clone()
    };
    if let Some(group) = groups.get(&request.group) {
        if !group.consumer && !group.members.is_empty() {
            return done(fail(
                codes::INCONSISTENT_GROUP_PROTOCOL,
                &request.member_id,
            ));
        }
        if let Err(code) = admit(group, config, request, &member_id) {
            return done(fail(code, &member_id));
        }
    } else if request.member_epoch != 0 {
        return done(fail(codes::UNKNOWN_MEMBER_ID, &member_id));
    } else if config.max_size == 0 {
        return done(fail(codes::GROUP_MAX_SIZE_REACHED, &member_id));
    }

    let existed = groups.get(&request.group).map(Group::record);
    let group = groups.entry(request.group.clone()).or_insert_with(|| {
        let mut group = Group::new();
        group.consumer = true;
        group.protocol_type = Some("consumer".into());
        group
    });
    group.consumer = true;
    group.protocol_type = Some("consumer".into());
    if group.state == State::Dead {
        group.state = State::Empty;
    }
    group.topic_cache = topics
        .iter()
        .map(|topic| (topic.name.clone(), (topic.id.to_string(), topic.partitions)))
        .collect();
    let before = group.record();
    let (replaced, subscription_changed) = upsert_member(group, config, request, &member_id, now);
    retarget(group, replaced || subscription_changed);
    let epoch_before = group
        .members
        .get(&member_id)
        .map(|member| member.record.member_epoch)
        .unwrap_or(0);
    let assigned_before = group
        .members
        .get(&member_id)
        .map(|member| member.record.assigned.clone())
        .unwrap_or_default();
    reconcile(
        group,
        &member_id,
        request.owned.as_deref(),
        subscription_changed,
    );
    if let Some(member) = group.members.get_mut(&member_id) {
        member.last_seen = now;
        if member.record.member_state == "unrevoked" {
            if member.revoke_since.is_none() {
                member.revoke_since = Some(now);
            }
        } else {
            member.revoke_since = None;
        }
    }
    refresh_state(group);
    let assigned_after = group
        .members
        .get(&member_id)
        .map(|member| member.record.assigned.clone())
        .unwrap_or_default();
    let epoch = group
        .members
        .get(&member_id)
        .map(|member| member.record.member_epoch)
        .unwrap_or(0);
    let full = request.rebalance_timeout_ms != -1
        && (request.subscribed_topics.is_some() || request.subscribed_regex.is_some())
        && request.owned.is_some();
    let changed = assigned_before != assigned_after || epoch_before != epoch;
    let assignment = if request.member_epoch == 0 || full || changed {
        Some(assignment_list(&assigned_after))
    } else {
        None
    };
    let response = ConsumerHeartbeatResponse {
        error: codes::NONE,
        message: None,
        member_id,
        member_epoch: epoch,
        heartbeat_interval_ms: millis(config.consumer_heartbeat_interval),
        assignment,
    };
    let after = group.record();
    if before == after {
        return done(response);
    }
    Outcome {
        response,
        save: Some(after),
        restore: match existed {
            Some(record) => Restore::Record(record),
            None => Restore::Remove,
        },
    }
}

/// Drop members whose session, or revocation budget, has run out.
/// The caller stores the record.
pub(super) fn expire(group: &mut Group, now: Instant) -> bool {
    if !group.consumer || group.state == State::Dead {
        return false;
    }
    let expired: Vec<String> = group
        .members
        .values()
        .filter(|member| {
            let silent = now.duration_since(member.last_seen) > member.session();
            let revoke = member.record.member_state == "unrevoked"
                && member.revoke_since.is_some_and(|since| {
                    now.duration_since(since)
                        > std::time::Duration::from_millis(
                            member.record.rebalance_timeout_ms.max(0) as u64,
                        )
                });
            silent || revoke
        })
        .map(|member| member.record.member_id.clone())
        .collect();
    if expired.is_empty() {
        return false;
    }
    for member_id in expired {
        group.members.remove(&member_id);
    }
    retarget(group, true);
    refresh_state(group);
    true
}

pub(super) fn label(group: &Group) -> &'static str {
    if !group.consumer {
        return group.state.name();
    }
    if group.state == State::Dead {
        return "Dead";
    }
    if group.members.is_empty() {
        return "Empty";
    }
    let reconciling = group.members.values().any(|member| {
        member.record.member_state != "stable" || member.record.member_epoch != group.generation
    });
    if reconciling {
        "Reconciling"
    } else {
        "Stable"
    }
}

fn leave(groups: &mut HashMap<String, Group>, request: &ConsumerHeartbeat) -> Outcome {
    let Some(group) = groups.get_mut(&request.group) else {
        return done(fail(codes::GROUP_ID_NOT_FOUND, &request.member_id));
    };
    if !group.consumer {
        return done(fail(
            codes::INCONSISTENT_GROUP_PROTOCOL,
            &request.member_id,
        ));
    }
    let member_id = if let Some(instance) = &request.instance_id {
        match group.static_member(instance) {
            None => return done(fail(codes::UNKNOWN_MEMBER_ID, &request.member_id)),
            Some(existing) if existing != request.member_id => {
                return done(fail(codes::FENCED_INSTANCE_ID, &request.member_id));
            }
            Some(existing) => existing,
        }
    } else if group.members.contains_key(&request.member_id) {
        request.member_id.clone()
    } else {
        return done(fail(codes::UNKNOWN_MEMBER_ID, &request.member_id));
    };

    let restore = group.record();
    if request.member_epoch == -2 {
        if let Some(member) = group.members.get_mut(&member_id) {
            member.record.previous_member_epoch = member.record.member_epoch;
            member.record.member_epoch = -2;
            member.record.revoking.clear();
            for topic in member.record.assigned.values_mut() {
                for epoch in topic.values_mut() {
                    *epoch = 0;
                }
            }
            member.record.member_state = "stable".into();
            member.revoke_since = None;
        }
        return Outcome {
            response: ConsumerHeartbeatResponse {
                error: codes::NONE,
                message: None,
                member_id: request.member_id.clone(),
                member_epoch: -2,
                heartbeat_interval_ms: 0,
                assignment: None,
            },
            save: Some(group.record()),
            restore: Restore::Record(restore),
        };
    }
    group.members.remove(&member_id);
    retarget(group, true);
    refresh_state(group);
    Outcome {
        response: ConsumerHeartbeatResponse {
            error: codes::NONE,
            message: None,
            member_id: request.member_id.clone(),
            member_epoch: -1,
            heartbeat_interval_ms: 0,
            assignment: None,
        },
        save: Some(group.record()),
        restore: Restore::Record(restore),
    }
}

/// `Err` when this heartbeat must not change the group.
fn admit(
    group: &Group,
    config: &Config,
    request: &ConsumerHeartbeat,
    member_id: &str,
) -> Result<(), i16> {
    let existing = if let Some(instance) = &request.instance_id {
        group.static_member(instance)
    } else {
        group
            .members
            .contains_key(member_id)
            .then(|| member_id.to_string())
    };
    let joining = request.member_epoch == 0;
    if !joining {
        let Some(existing) = existing else {
            return Err(codes::UNKNOWN_MEMBER_ID);
        };
        if request.instance_id.is_some() && existing != member_id {
            return Err(codes::FENCED_INSTANCE_ID);
        }
        return epoch_ok(&group.members[&existing].record, request);
    }
    if let Some(existing) = &existing {
        if request.instance_id.is_some() {
            let member = &group.members[existing].record;
            if member.member_epoch != -2 {
                return Err(codes::UNRELEASED_INSTANCE_ID);
            }
        }
        return Ok(());
    }
    if group.members.len() >= config.max_size {
        return Err(codes::GROUP_MAX_SIZE_REACHED);
    }
    Ok(())
}

fn epoch_ok(member: &MemberRecord, request: &ConsumerHeartbeat) -> Result<(), i16> {
    let received = request.member_epoch;
    if received == member.member_epoch {
        return Ok(());
    }
    if received > member.member_epoch {
        return Err(codes::FENCED_MEMBER_EPOCH);
    }
    let subset = request.owned.as_ref().is_some_and(|owned| {
        owned.iter().all(|(topic, partitions)| {
            partitions.iter().all(|partition| {
                member
                    .assigned
                    .get(&topic.to_string())
                    .is_some_and(|held| held.contains_key(partition))
            })
        })
    });
    if received == member.previous_member_epoch && subset {
        Ok(())
    } else {
        Err(codes::FENCED_MEMBER_EPOCH)
    }
}

/// Whether the member was added or replaced, and whether its subscription changed.
fn upsert_member(
    group: &mut Group,
    config: &Config,
    request: &ConsumerHeartbeat,
    member_id: &str,
    now: Instant,
) -> (bool, bool) {
    let joining = request.member_epoch == 0;
    let mut replaced = false;
    if joining {
        if let Some(instance) = &request.instance_id {
            if let Some(old) = group.static_member(instance) {
                if old != member_id {
                    let mut record = group.members.remove(&old).expect("static").record;
                    record.member_id = member_id.to_string();
                    record.member_epoch = 0;
                    record.previous_member_epoch = 0;
                    group.members.insert(member_id.to_string(), blank(record, group.next_order, now));
                    group.next_order += 1;
                    replaced = true;
                }
            }
        }
    }
    if !group.members.contains_key(member_id) {
        group.members.insert(
            member_id.to_string(),
            blank(
                MemberRecord {
                    member_id: member_id.to_string(),
                    member_state: "stable".into(),
                    ..Default::default()
                },
                group.next_order,
                now,
            ),
        );
        group.next_order += 1;
        replaced = true;
    }
    let member = group.members.get_mut(member_id).expect("member");
    member.record.client_id = request.client_id.clone();
    member.record.client_host = request.client_host.clone();
    member.record.session_timeout_ms = millis(config.consumer_session_timeout);
    if request.rebalance_timeout_ms >= 0 {
        member.record.rebalance_timeout_ms = request.rebalance_timeout_ms;
    } else if member.record.rebalance_timeout_ms == 0 {
        member.record.rebalance_timeout_ms = 300_000;
    }
    if let Some(instance) = &request.instance_id {
        member.record.group_instance_id = Some(instance.clone());
    }
    if let Some(rack) = &request.rack_id {
        member.record.rack_id = Some(rack.clone());
    }
    if let Some(assignor) = request
        .server_assignor
        .as_ref()
        .filter(|name| !name.is_empty())
    {
        member.record.server_assignor = Some(assignor.clone());
    }
    let mut subscription_changed = replaced;
    if let Some(topics) = &request.subscribed_topics {
        if topics != &member.record.subscribed_topics {
            member.record.subscribed_topics = topics.clone();
            subscription_changed = true;
        }
    }
    if let Some(pattern) = &request.subscribed_regex {
        let next = if pattern.is_empty() {
            None
        } else {
            Some(pattern.clone())
        };
        if next != member.record.subscribed_regex {
            member.record.subscribed_regex = next;
            subscription_changed = true;
        }
    }
    (replaced, subscription_changed)
}

fn blank(record: MemberRecord, order: u64, now: Instant) -> Member {
    Member {
        record,
        protocols: Vec::new(),
        last_seen: now,
        join: None,
        sync: None,
        new_since: None,
        join_order: order,
        revoke_since: None,
    }
}

fn retarget(group: &mut Group, force: bool) {
    let assignor = preferred(group);
    let changed = group.protocol.as_deref() != Some(assignor.as_str());
    let target = assign(group, &assignor);
    if force || changed || target != group.target {
        group.generation = group.generation.saturating_add(1);
        group.target = target;
        group.protocol = Some(assignor);
    }
}

fn preferred(group: &Group) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for member in group.members.values() {
        if let Some(name) = &member.record.server_assignor {
            *counts.entry(name.clone()).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then(right.0.cmp(&left.0)))
        .map(|(name, _)| name)
        .unwrap_or_else(|| "uniform".into())
}

struct Seat<'a> {
    member: &'a str,
    instance: Option<&'a str>,
    topics: BTreeSet<String>,
}

fn assign(group: &Group, assignor: &str) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    let mut seats: Vec<Seat<'_>> = group
        .members
        .values()
        .map(|member| Seat {
            member: &member.record.member_id,
            instance: member.record.group_instance_id.as_deref(),
            topics: topic_ids(group, &member.record),
        })
        .collect();
    seats.sort_by(|left, right| match (left.instance, right.instance) {
        (Some(a), Some(b)) => a.cmp(b).then(left.member.cmp(right.member)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.member.cmp(right.member),
    });
    if seats.is_empty() {
        return BTreeMap::new();
    }
    let counts: BTreeMap<String, i32> = group
        .topic_cache
        .values()
        .map(|(id, partitions)| (id.clone(), *partitions))
        .collect();
    if assignor == "range" {
        assign_range(&seats, &counts)
    } else {
        assign_uniform(&seats, &counts, &group.target)
    }
}

fn topic_ids(group: &Group, member: &MemberRecord) -> BTreeSet<String> {
    let mut names: BTreeSet<&str> = member.subscribed_topics.iter().map(String::as_str).collect();
    if let Some(pattern) = &member.subscribed_regex {
        if let Ok(pattern) = Regex::new(pattern) {
            for name in group.topic_cache.keys() {
                if pattern.is_match(name) {
                    names.insert(name);
                }
            }
        }
    }
    names
        .into_iter()
        .filter_map(|name| {
            let (id, partitions) = group.topic_cache.get(name)?;
            (*partitions > 0).then(|| id.clone())
        })
        .collect()
}

/// Kafka's sticky `UniformAssignor`, simplified. Every member gets a fair
/// share of the partitions it can take: the total split evenly, with the
/// remainder going to the members that already hold the most, so the fewest
/// partitions move. A member keeps what it has, up to its share, while it is
/// still subscribed. Partitions left over go one at a time to the subscribed
/// member holding the fewest. When subscriptions differ, a partition only
/// some members can take may push one of them past its share.
fn assign_uniform(
    seats: &[Seat<'_>],
    counts: &BTreeMap<String, i32>,
    previous: &BTreeMap<String, BTreeMap<String, Vec<i32>>>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    let mut wanted: Vec<(String, i32)> = Vec::new();
    let mut topics: BTreeSet<&str> = BTreeSet::new();
    for seat in seats {
        topics.extend(seat.topics.iter().map(String::as_str));
    }
    for topic in &topics {
        let partitions = counts.get(*topic).copied().unwrap_or(0);
        wanted.extend((0..partitions).map(|partition| (topic.to_string(), partition)));
    }
    let wanted_set: BTreeSet<(String, i32)> = wanted.iter().cloned().collect();

    // What each member still holds and may keep.
    let mut held: Vec<Vec<(String, i32)>> = seats
        .iter()
        .map(|seat| {
            let mut kept: Vec<(String, i32)> = previous
                .get(seat.member)
                .into_iter()
                .flatten()
                .filter(|(topic, _)| seat.topics.contains(*topic))
                .flat_map(|(topic, partitions)| {
                    partitions
                        .iter()
                        .map(move |partition| (topic.clone(), *partition))
                })
                .filter(|owned| wanted_set.contains(owned))
                .collect();
            kept.sort();
            kept
        })
        .collect();

    // Fair shares: the remainder goes to the members already holding most.
    let base = wanted.len() / seats.len();
    let extra = wanted.len() % seats.len();
    let mut by_load: Vec<usize> = (0..seats.len()).collect();
    by_load.sort_by(|left, right| {
        held[*right]
            .len()
            .cmp(&held[*left].len())
            .then(left.cmp(right))
    });
    let mut share = vec![base; seats.len()];
    for seat in by_load.into_iter().take(extra) {
        share[seat] += 1;
    }

    // Release what is over a share, highest partitions first.
    let mut taken: BTreeSet<(String, i32)> = BTreeSet::new();
    for (seat, owned) in held.iter_mut().enumerate() {
        owned.truncate(share[seat]);
        taken.extend(owned.iter().cloned());
    }

    for item in wanted.into_iter().filter(|item| !taken.contains(item)) {
        let eligible = || (0..seats.len()).filter(|seat| seats[*seat].topics.contains(&item.0));
        let under = eligible()
            .filter(|seat| held[*seat].len() < share[*seat])
            .min_by_key(|seat| (held[*seat].len(), *seat));
        let Some(owner) =
            under.or_else(|| eligible().min_by_key(|seat| (held[*seat].len(), *seat)))
        else {
            continue;
        };
        held[owner].push(item);
    }

    let mut target: BTreeMap<String, BTreeMap<String, Vec<i32>>> = BTreeMap::new();
    for (seat, owned) in seats.iter().zip(held) {
        for (topic, partition) in owned {
            target
                .entry(seat.member.to_string())
                .or_default()
                .entry(topic)
                .or_default()
                .push(partition);
        }
    }
    for topics in target.values_mut() {
        for partitions in topics.values_mut() {
            partitions.sort_unstable();
        }
    }
    target
}

fn assign_range(
    seats: &[Seat<'_>],
    counts: &BTreeMap<String, i32>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    struct Quota {
        partitions: i32,
        members: i32,
        min: i32,
        extra: i32,
        next: i32,
    }
    let mut quotas: BTreeMap<String, Quota> = BTreeMap::new();
    for seat in seats {
        for topic in &seat.topics {
            let Some(partitions) = counts.get(topic).copied().filter(|count| *count > 0) else {
                continue;
            };
            quotas
                .entry(topic.clone())
                .or_insert(Quota {
                    partitions,
                    members: 0,
                    min: -1,
                    extra: 0,
                    next: 0,
                })
                .members += 1;
        }
    }
    let mut target: BTreeMap<String, BTreeMap<String, Vec<i32>>> = BTreeMap::new();
    for seat in seats {
        for topic in &seat.topics {
            let Some(quota) = quotas.get_mut(topic) else {
                continue;
            };
            if quota.min < 0 {
                let members = quota.members.max(1);
                quota.min = quota.partitions / members;
                quota.extra = quota.partitions % members;
            }
            let mut share = quota.min;
            if quota.extra > 0 {
                share += 1;
                quota.extra -= 1;
            }
            let start = quota.next;
            let end = (start + share).min(quota.partitions);
            quota.next = end;
            if start < end {
                target
                    .entry(seat.member.to_string())
                    .or_default()
                    .insert(topic.clone(), (start..end).collect());
            }
        }
    }
    target
}

fn reconcile(
    group: &mut Group,
    member_id: &str,
    owned: Option<&[(Uuid, Vec<i32>)]>,
    subscription_changed: bool,
) {
    let Some(member) = group.members.get(member_id) else {
        return;
    };
    let record = member.record.clone();
    let next = match record.member_state.as_str() {
        "unrevoked" if owns_revoked(owned, &record.revoking) && !subscription_changed => return,
        "unrevoked" if owns_revoked(owned, &record.revoking) => {
            shrink_to_subscription(group, &record, record.member_epoch)
        }
        "stable" if record.member_epoch == group.generation && !subscription_changed => return,
        "stable" if record.member_epoch == group.generation => {
            shrink_to_subscription(group, &record, record.member_epoch)
        }
        // `unreleased` retries every heartbeat: a partition may have been revoked
        // since the member moved to the target epoch.
        _ => Some(plan(
            group,
            &record,
            record.member_epoch,
            &record.assigned,
            owned,
        )),
    };
    let Some((state, epoch, assigned, revoking)) = next else {
        return;
    };
    let member = group.members.get_mut(member_id).expect("member");
    if member.record.member_epoch != epoch {
        member.record.previous_member_epoch = member.record.member_epoch;
        member.record.member_epoch = epoch;
    }
    member.record.member_state = state;
    member.record.assigned = assigned;
    member.record.revoking = revoking;
}

fn shrink_to_subscription(
    group: &Group,
    member: &MemberRecord,
    epoch: i32,
) -> Option<(String, i32, Parts, Parts)> {
    let subscribed = topic_ids(group, member);
    let mut assigned = member.assigned.clone();
    let mut revoking = member.revoking.clone();
    let mut moved = false;
    for topic in member.assigned.keys() {
        if subscribed.contains(topic) {
            continue;
        }
        if let Some(parts) = assigned.remove(topic) {
            revoking.entry(topic.clone()).or_default().extend(parts);
            moved = true;
        }
    }
    if !moved {
        return None;
    }
    Some(("unrevoked".into(), epoch, assigned, revoking))
}

/// Next member state. `owned` is what the request says the member still has;
/// `None` means the member did not say, and revocation cannot complete.
fn plan(
    group: &Group,
    member: &MemberRecord,
    member_epoch: i32,
    assigned_now: &Parts,
    owned: Option<&[(Uuid, Vec<i32>)]>,
) -> (String, i32, Parts, Parts) {
    let subscribed = topic_ids(group, member);
    let target = group
        .target
        .get(&member.member_id)
        .cloned()
        .unwrap_or_default();
    let mut topics: BTreeSet<String> = BTreeSet::new();
    topics.extend(target.keys().cloned());
    topics.extend(assigned_now.keys().cloned());

    let mut assigned: Parts = BTreeMap::new();
    let mut revoking: Parts = BTreeMap::new();
    let mut pending: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    let mut unreleased = false;

    for topic in &topics {
        let wanted: BTreeSet<i32> = if subscribed.contains(topic) {
            target
                .get(topic)
                .map(|parts| parts.iter().copied().collect())
                .unwrap_or_default()
        } else {
            BTreeSet::new()
        };
        let mut keep = BTreeMap::new();
        let mut drop = BTreeMap::new();
        if let Some(current) = assigned_now.get(topic) {
            for (partition, epoch) in current {
                if wanted.contains(partition) {
                    keep.insert(*partition, *epoch);
                } else {
                    drop.insert(*partition, *epoch);
                }
            }
        }
        let mut waiting = Vec::new();
        for partition in &wanted {
            if keep.contains_key(partition) {
                continue;
            }
            let held = partition_epoch(group, topic, *partition) != -1
                && !member
                    .revoking
                    .get(topic)
                    .is_some_and(|parts| parts.contains_key(partition));
            if held {
                unreleased = true;
            } else {
                waiting.push(*partition);
            }
        }
        if !keep.is_empty() {
            assigned.insert(topic.clone(), keep);
        }
        if !drop.is_empty() {
            revoking.insert(topic.clone(), drop);
        }
        if !waiting.is_empty() {
            pending.insert(topic.clone(), waiting);
        }
    }

    if !revoking.is_empty() && owns_revoked(owned, &revoking) {
        return ("unrevoked".into(), member_epoch, assigned, revoking);
    }
    if !pending.is_empty() {
        for (topic, parts) in pending {
            let topic_parts = assigned.entry(topic).or_default();
            for partition in parts {
                topic_parts.insert(partition, group.generation);
            }
        }
        let state = if unreleased { "unreleased" } else { "stable" };
        return (state.into(), group.generation, assigned, BTreeMap::new());
    }
    if unreleased {
        return (
            "unreleased".into(),
            group.generation,
            assigned,
            BTreeMap::new(),
        );
    }
    (
        "stable".into(),
        group.generation,
        assigned,
        BTreeMap::new(),
    )
}

fn partition_epoch(group: &Group, topic: &str, partition: i32) -> i32 {
    let mut epoch = -1;
    for member in group.members.values() {
        let holds = member
            .record
            .assigned
            .get(topic)
            .is_some_and(|parts| parts.contains_key(&partition))
            || member
                .record
                .revoking
                .get(topic)
                .is_some_and(|parts| parts.contains_key(&partition));
        // A static member that left with epoch -2 still owns its partitions
        // until it returns or its session ends. Any non-empty hold counts.
        if holds {
            let stamp = member.record.member_epoch.max(0);
            if stamp > epoch {
                epoch = stamp;
            }
        }
    }
    epoch
}

fn owns_revoked(owned: Option<&[(Uuid, Vec<i32>)]>, revoking: &Parts) -> bool {
    let Some(owned) = owned else {
        return true;
    };
    for (topic, partitions) in owned {
        let Some(pending) = revoking.get(&topic.to_string()) else {
            continue;
        };
        if partitions
            .iter()
            .any(|partition| pending.contains_key(partition))
        {
            return true;
        }
    }
    false
}

fn refresh_state(group: &mut Group) {
    group.state = if group.members.is_empty() {
        State::Empty
    } else {
        State::Stable
    };
}

fn assignment_list(assigned: &Parts) -> Vec<(Uuid, Vec<i32>)> {
    let mut out = Vec::new();
    for (topic, parts) in assigned {
        let Ok(id) = Uuid::parse_str(topic) else {
            continue;
        };
        let mut partitions: Vec<i32> = parts.keys().copied().collect();
        partitions.sort();
        out.push((id, partitions));
    }
    out
}

fn millis(duration: std::time::Duration) -> i32 {
    duration.as_millis().min(i32::MAX as u128) as i32
}

fn fail(error: i16, member_id: &str) -> ConsumerHeartbeatResponse {
    ConsumerHeartbeatResponse {
        error,
        message: None,
        member_id: member_id.to_string(),
        member_epoch: -1,
        heartbeat_interval_ms: 0,
        assignment: None,
    }
}

fn done(response: ConsumerHeartbeatResponse) -> Outcome {
    Outcome {
        response,
        save: None,
        restore: Restore::Keep,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::{Coordinator, SaveFuture, Store};

    struct Saved {
        records: Mutex<Vec<GroupRecord>>,
    }

    impl Store for Saved {
        fn save(&self, _group: String, record: GroupRecord) -> SaveFuture {
            self.records.lock().unwrap().push(record);
            Box::pin(async { Ok(()) })
        }
    }

    fn topic() -> TopicMeta {
        TopicMeta {
            name: "orders".into(),
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            partitions: 4,
        }
    }

    fn beat(member: &str, epoch: i32, owned: Option<Vec<i32>>) -> ConsumerHeartbeat {
        ConsumerHeartbeat {
            group: "g".into(),
            member_id: member.into(),
            member_epoch: epoch,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: if epoch == 0 { 10_000 } else { -1 },
            subscribed_topics: (epoch == 0).then(|| vec!["orders".into()]),
            subscribed_regex: None,
            server_assignor: Some("range".into()),
            owned: owned.map(|parts| vec![(topic().id, parts)]),
            client_id: member.into(),
            client_host: "/127.0.0.1".into(),
            version: 1,
        }
    }

    fn parts(response: &ConsumerHeartbeatResponse) -> Vec<i32> {
        response
            .assignment
            .as_ref()
            .and_then(|topics| topics.first())
            .map(|(_, parts)| parts.clone())
            .unwrap_or_default()
    }

    async fn heartbeat(
        coordinator: &Arc<Coordinator>,
        request: ConsumerHeartbeat,
    ) -> ConsumerHeartbeatResponse {
        coordinator
            .consumer_heartbeat(request, &[topic()], Instant::now())
            .await
    }

    #[tokio::test]
    async fn two_members_revoke_then_share() {
        let saved = Arc::new(Saved {
            records: Mutex::new(Vec::new()),
        });
        let coordinator = Coordinator::new(saved, Config::default());
        let first = heartbeat(&coordinator, beat("a", 0, Some(vec![]))).await;
        assert_eq!(first.error, codes::NONE);
        assert_eq!(first.member_epoch, 1);
        assert_eq!(parts(&first), vec![0, 1, 2, 3]);

        let second = heartbeat(&coordinator, beat("b", 0, Some(vec![]))).await;
        assert_eq!(second.error, codes::NONE);
        assert_eq!(second.member_epoch, 2);
        assert!(parts(&second).is_empty(), "b waits for a to revoke");

        let revoke = heartbeat(&coordinator, beat("a", 1, Some(vec![0, 1, 2, 3]))).await;
        assert_eq!(revoke.error, codes::NONE);
        assert_eq!(revoke.member_epoch, 1);
        assert_eq!(parts(&revoke), vec![0, 1]);

        let released = heartbeat(&coordinator, beat("a", 1, Some(vec![0, 1]))).await;
        assert_eq!(released.member_epoch, 2);
        assert_eq!(parts(&released), vec![0, 1]);

        let granted = heartbeat(&coordinator, beat("b", 2, Some(vec![]))).await;
        assert_eq!(granted.error, codes::NONE);
        assert_eq!(parts(&granted), vec![2, 3]);

        assert_eq!(
            coordinator.check_commit("g", "a", None, 2),
            codes::NONE
        );
        assert_eq!(
            coordinator.check_commit("g", "a", None, 1),
            codes::NONE
        );
        assert_eq!(
            coordinator.check_commit("g", "a", None, 9),
            codes::STALE_MEMBER_EPOCH
        );
    }

    #[tokio::test]
    async fn leave_hands_the_partitions_back() {
        let saved = Arc::new(Saved {
            records: Mutex::new(Vec::new()),
        });
        let coordinator = Coordinator::new(saved, Config::default());
        heartbeat(&coordinator, beat("a", 0, Some(vec![]))).await;
        heartbeat(&coordinator, beat("b", 0, Some(vec![]))).await;
        heartbeat(&coordinator, beat("a", 1, Some(vec![0, 1, 2, 3]))).await;
        heartbeat(&coordinator, beat("a", 1, Some(vec![0, 1]))).await;
        heartbeat(&coordinator, beat("b", 2, Some(vec![]))).await;

        let mut leaving = beat("b", -1, None);
        leaving.rebalance_timeout_ms = -1;
        let left = heartbeat(&coordinator, leaving).await;
        assert_eq!(left.error, codes::NONE);
        assert_eq!(left.member_epoch, -1);

        let again = heartbeat(&coordinator, beat("a", 2, Some(vec![0, 1]))).await;
        assert_eq!(parts(&again), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn a_stale_epoch_is_fenced() {
        let saved = Arc::new(Saved {
            records: Mutex::new(Vec::new()),
        });
        let coordinator = Coordinator::new(saved.clone(), Config::default());
        heartbeat(&coordinator, beat("a", 0, Some(vec![]))).await;
        let fenced = heartbeat(&coordinator, beat("a", 9, Some(vec![0]))).await;
        assert_eq!(fenced.error, codes::FENCED_MEMBER_EPOCH);
        assert_eq!(saved.records.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_unknown_assignor_is_rejected() {
        let coordinator = Coordinator::new(
            Arc::new(Saved {
                records: Mutex::new(Vec::new()),
            }),
            Config::default(),
        );
        let mut request = beat("a", 0, Some(vec![]));
        request.server_assignor = Some("sticky".into());
        let response = heartbeat(&coordinator, request).await;
        assert_eq!(response.error, codes::UNSUPPORTED_ASSIGNOR);
    }

    #[tokio::test]
    async fn a_classic_group_rejects_the_consumer_protocol() {
        let coordinator = Coordinator::new(
            Arc::new(Saved {
                records: Mutex::new(Vec::new()),
            }),
            Config {
                initial_rebalance_delay: Duration::ZERO,
                min_session_timeout: Duration::from_millis(1),
                ..Config::default()
            },
        );
        let now = Instant::now();
        let join = crate::JoinRequest {
            group: "g".into(),
            member_id: String::new(),
            protocol_type: "consumer".into(),
            protocols: vec![("range".into(), vec![1])],
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            ..Default::default()
        };
        let pending = coordinator.join(join, now);
        coordinator.tick(now + Duration::from_millis(1));
        let joined = pending
            .wait(|| crate::JoinResponse::error(codes::NOT_COORDINATOR, ""))
            .await;
        assert_eq!(joined.error, codes::NONE);
        let response = heartbeat(&coordinator, beat("a", 0, Some(vec![]))).await;
        assert_eq!(response.error, codes::INCONSISTENT_GROUP_PROTOCOL);
    }

    #[tokio::test]
    async fn a_quiet_member_is_removed() {
        let saved = Arc::new(Saved {
            records: Mutex::new(Vec::new()),
        });
        let coordinator = Coordinator::new(
            saved,
            Config {
                consumer_session_timeout: Duration::from_millis(1_000),
                ..Config::default()
            },
        );
        let start = Instant::now();
        coordinator
            .consumer_heartbeat(beat("a", 0, Some(vec![])), &[topic()], start)
            .await;
        coordinator
            .consumer_heartbeat(beat("b", 0, Some(vec![])), &[topic()], start)
            .await;
        coordinator
            .consumer_heartbeat(
                beat("a", 1, Some(vec![0, 1, 2, 3])),
                &[topic()],
                start + Duration::from_millis(500),
            )
            .await;
        coordinator.tick(start + Duration::from_millis(1_001));
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let groups = coordinator.groups.lock().unwrap();
        assert!(groups["g"].members.contains_key("a"));
        assert!(!groups["g"].members.contains_key("b"));
    }
}

#[cfg(test)]
mod uniform_tests {
    use super::*;

    type Plan = BTreeMap<String, BTreeMap<String, Vec<i32>>>;

    fn seats<'a>(members: &'a [&'a str]) -> Vec<Seat<'a>> {
        members
            .iter()
            .map(|member| Seat {
                member,
                instance: None,
                topics: BTreeSet::from(["t".to_string()]),
            })
            .collect()
    }

    fn plan(entries: &[(&str, &[i32])]) -> Plan {
        entries
            .iter()
            .map(|(member, partitions)| {
                (
                    member.to_string(),
                    BTreeMap::from([("t".to_string(), partitions.to_vec())]),
                )
            })
            .collect()
    }

    fn owned(plan: &Plan, member: &str) -> Vec<i32> {
        plan.get(member)
            .and_then(|topics| topics.get("t"))
            .cloned()
            .unwrap_or_default()
    }

    fn moved(before: &Plan, after: &Plan) -> usize {
        after
            .iter()
            .map(|(member, _)| {
                owned(after, member)
                    .into_iter()
                    .filter(|partition| !owned(before, member).contains(partition))
                    .count()
            })
            .sum()
    }

    fn counts(partitions: i32) -> BTreeMap<String, i32> {
        BTreeMap::from([("t".to_string(), partitions)])
    }

    #[test]
    fn partitions_are_split_evenly() {
        let target = assign_uniform(&seats(&["a", "b"]), &counts(6), &Plan::new());
        assert_eq!(owned(&target, "a").len(), 3);
        assert_eq!(owned(&target, "b").len(), 3);
    }

    #[test]
    fn a_new_member_takes_only_its_share() {
        let before = plan(&[("a", &[0, 1, 2, 3, 4, 5])]);
        let after = assign_uniform(&seats(&["a", "b"]), &counts(6), &before);
        assert_eq!(owned(&after, "a"), vec![0, 1, 2]);
        assert_eq!(owned(&after, "b"), vec![3, 4, 5]);
        assert_eq!(moved(&before, &after), 3);
    }

    #[test]
    fn a_third_member_moves_one_partition_from_each() {
        let before = plan(&[("a", &[0, 1, 2]), ("b", &[3, 4, 5])]);
        let after = assign_uniform(&seats(&["a", "b", "c"]), &counts(6), &before);
        assert_eq!(owned(&after, "a"), vec![0, 1]);
        assert_eq!(owned(&after, "b"), vec![3, 4]);
        assert_eq!(owned(&after, "c"), vec![2, 5]);
        assert_eq!(moved(&before, &after), 2);
    }

    #[test]
    fn a_departed_member_only_frees_its_own_partitions() {
        let before = plan(&[("a", &[0, 1]), ("b", &[2, 3]), ("c", &[4, 5])]);
        let after = assign_uniform(&seats(&["a", "b"]), &counts(6), &before);
        assert_eq!(owned(&after, "a"), vec![0, 1, 4]);
        assert_eq!(owned(&after, "b"), vec![2, 3, 5]);
    }
}
