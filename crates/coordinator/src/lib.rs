//! Classic consumer group coordinator (Phase 3, ADR 0013).
//!
//! The metadata leader coordinates every group. Live membership is kept here
//! in memory; what must survive a failover goes through the metadata group
//! via [`Store`]: the record of each new generation before anyone hears of
//! it, the assignment before any member receives it, and a static member's
//! new member id. A new coordinator loads those records, so members of a
//! stable group keep their generation across a failover and the group does
//! not split.
//!
//! Behavior follows the classic group protocol of Kafka 4.4.0
//! (`GroupMetadataManager` and `ClassicGroup`); `src/tests` ports the cases
//! from its test suite that apply here, and `docs/kafka-coordinator-port.md`
//! lists every case and what happened to it.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use camus_metadata::groups::{GroupRecord, MemberRecord};
use tokio::sync::oneshot;

/// Kafka error codes the coordinator answers with.
pub mod codes {
    pub const NONE: i16 = 0;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
    pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
    pub const FENCED_INSTANCE_ID: i16 = 82;
}

/// A boxed future, for [`Store::save`].
pub type SaveFuture = Pin<Box<dyn Future<Output = Result<(), i16>> + Send>>;

/// Where generations and assignments are made durable. `Err` is a Kafka
/// error code; the coordinator answers waiting members with it.
pub trait Store: Send + Sync + 'static {
    /// Store `record` for `group`. Resolves once the metadata group has
    /// committed it.
    fn save(&self, group: String, record: GroupRecord) -> SaveFuture;
}

/// Coordinator settings, Kafka's broker defaults unless a test changes them.
#[derive(Clone, Debug)]
pub struct Config {
    /// `group.initial.rebalance.delay.ms`.
    pub initial_rebalance_delay: Duration,
    /// `group.min.session.timeout.ms`.
    pub min_session_timeout: Duration,
    /// `group.max.session.timeout.ms`.
    pub max_session_timeout: Duration,
    /// `group.max.size`.
    pub max_size: usize,
    /// How long a member that joined during a rebalance may wait for it
    /// without heartbeating (Kafka's `classicGroupNewMemberJoinTimeoutMs`).
    pub new_member_join_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            initial_rebalance_delay: Duration::from_millis(3_000),
            min_session_timeout: Duration::from_millis(6_000),
            max_session_timeout: Duration::from_millis(1_800_000),
            max_size: usize::MAX,
            new_member_join_timeout: Duration::from_millis(300_000),
        }
    }
}

/// Group state names as DescribeGroups and ListGroups report them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl State {
    pub fn name(self) -> &'static str {
        match self {
            State::Empty => "Empty",
            State::PreparingRebalance => "PreparingRebalance",
            State::CompletingRebalance => "CompletingRebalance",
            State::Stable => "Stable",
            State::Dead => "Dead",
        }
    }
}

/// A JoinGroup request, decoded.
#[derive(Clone, Debug, Default)]
pub struct JoinRequest {
    pub group: String,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub protocol_type: String,
    /// `(name, metadata)` in the member's order of preference.
    pub protocols: Vec<(String, Vec<u8>)>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    /// JoinGroup v4+: an unknown member gets MEMBER_ID_REQUIRED first.
    pub require_known_member_id: bool,
    /// JoinGroup v9+: a static leader that rejoins may skip reassignment.
    pub supports_skip_assignment: bool,
}

/// One member as the leader's JoinGroup response lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
}

/// A JoinGroup response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinResponse {
    pub error: i16,
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub skip_assignment: bool,
    /// Only the leader's response lists the members.
    pub members: Vec<JoinMember>,
}

impl JoinResponse {
    pub fn error(error: i16, member_id: &str) -> Self {
        Self {
            error,
            generation: -1,
            member_id: member_id.to_string(),
            ..Default::default()
        }
    }
}

/// A SyncGroup response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncResponse {
    pub error: i16,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Vec<u8>,
}

impl SyncResponse {
    pub fn error(error: i16) -> Self {
        Self {
            error,
            ..Default::default()
        }
    }
}

/// An answer now, or one that arrives when the group moves.
pub enum Reply<T> {
    Now(T),
    Later(oneshot::Receiver<T>),
}

impl<T> Reply<T> {
    /// Wait for the answer. A dropped sender (the group was unloaded)
    /// becomes `lost()`.
    pub async fn wait(self, lost: impl FnOnce() -> T) -> T {
        match self {
            Reply::Now(value) => value,
            Reply::Later(rx) => rx.await.unwrap_or_else(|_| lost()),
        }
    }

    /// The answer if it is already here.
    pub fn now(&self) -> Option<&T> {
        match self {
            Reply::Now(value) => Some(value),
            Reply::Later(_) => None,
        }
    }
}

/// One member as DescribeGroups reports it.
#[derive(Clone, Debug)]
pub struct DescribedMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
    pub assignment: Vec<u8>,
}

/// A group as DescribeGroups reports it.
#[derive(Clone, Debug)]
pub struct Described {
    pub state: State,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<DescribedMember>,
}

/// One member to remove in a LeaveGroup.
#[derive(Clone, Debug, Default)]
pub struct Leaving {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

struct Member {
    record: MemberRecord,
    protocols: Vec<(String, Vec<u8>)>,
    last_seen: Instant,
    join: Option<oneshot::Sender<JoinResponse>>,
    sync: Option<oneshot::Sender<SyncResponse>>,
    /// Joined during the current rebalance; not yet in a completed one.
    new_since: Option<Instant>,
    join_order: u64,
}

impl Member {
    fn session(&self) -> Duration {
        Duration::from_millis(self.record.session_timeout_ms.max(0) as u64)
    }

    fn is_static(&self) -> bool {
        self.record.group_instance_id.is_some()
    }

    fn metadata(&self, protocol: Option<&String>) -> Vec<u8> {
        protocol
            .and_then(|name| self.protocols.iter().find(|(theirs, _)| theirs == name))
            .map(|(_, metadata)| metadata.clone())
            .unwrap_or_default()
    }
}

/// The initial-rebalance delay of an empty group (Kafka's
/// `InitialDelayedJoin`): when it ends, it extends by one more delay, measured
/// from then, if a new member joined meanwhile and the rebalance timeout has
/// budget left beyond the first delay.
struct InitialDelay {
    remaining: Duration,
    joined: bool,
}

struct Group {
    state: State,
    generation: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    leader: Option<String>,
    members: BTreeMap<String, Member>,
    /// Member ids handed out with MEMBER_ID_REQUIRED, not yet joined, and
    /// the session that expires them.
    pending: HashMap<String, (Instant, Duration)>,
    /// Members of the latest generation that have not sent SyncGroup.
    pending_sync: HashSet<String>,
    sync_deadline: Option<Instant>,
    join_deadline: Option<Instant>,
    initial: Option<InitialDelay>,
    /// A save is in flight; the phase ends when it lands.
    saving: bool,
    next_order: u64,
}

impl Group {
    fn new() -> Self {
        Self {
            state: State::Empty,
            generation: 0,
            protocol_type: None,
            protocol: None,
            leader: None,
            members: BTreeMap::new(),
            pending: HashMap::new(),
            pending_sync: HashSet::new(),
            sync_deadline: None,
            join_deadline: None,
            initial: None,
            saving: false,
            next_order: 0,
        }
    }

    fn from_record(record: GroupRecord, now: Instant) -> Self {
        let mut group = Self::new();
        group.generation = record.generation;
        group.protocol_type = record.protocol_type;
        group.protocol = record.protocol.clone();
        group.leader = record.leader;
        for (order, member) in record.members.into_iter().enumerate() {
            let protocols = group
                .protocol
                .clone()
                .map(|name| vec![(name, member.subscription.clone())])
                .unwrap_or_default();
            group.members.insert(
                member.member_id.clone(),
                Member {
                    protocols,
                    // A loaded member gets a full session to find the new
                    // coordinator.
                    last_seen: now,
                    join: None,
                    sync: None,
                    new_since: None,
                    join_order: order as u64,
                    record: member,
                },
            );
        }
        group.next_order = group.members.len() as u64;
        let assigned = group
            .members
            .values()
            .all(|member| !member.record.assignment.is_empty());
        group.state = if group.members.is_empty() {
            State::Empty
        } else if assigned {
            State::Stable
        } else {
            // The generation started but its assignment was not stored: the
            // old coordinator died between join and sync. Rebalance.
            State::PreparingRebalance
        };
        group
    }

    fn record(&self) -> GroupRecord {
        let mut members: Vec<&Member> = self.members.values().collect();
        members.sort_by_key(|member| member.join_order);
        GroupRecord {
            generation: self.generation,
            protocol_type: self.protocol_type.clone(),
            protocol: self.protocol.clone(),
            leader: self.leader.clone(),
            members: members
                .into_iter()
                .map(|member| {
                    let mut record = member.record.clone();
                    if self.protocol.is_some() {
                        record.subscription = member.metadata(self.protocol.as_ref());
                    }
                    record
                })
                .collect(),
        }
    }

    fn static_member(&self, instance: &str) -> Option<String> {
        self.members
            .values()
            .find(|member| member.record.group_instance_id.as_deref() == Some(instance))
            .map(|member| member.record.member_id.clone())
    }

    /// Kafka's `validateMember`: an instance id must name this member id; a
    /// member id must be in the group.
    fn validate_member(&self, member_id: &str, instance: Option<&str>) -> Result<(), i16> {
        match instance {
            Some(instance) => match self.static_member(instance) {
                None => Err(codes::UNKNOWN_MEMBER_ID),
                Some(existing) if existing != member_id => Err(codes::FENCED_INSTANCE_ID),
                Some(_) => Ok(()),
            },
            None if self.members.contains_key(member_id) => Ok(()),
            None => Err(codes::UNKNOWN_MEMBER_ID),
        }
    }

    fn awaiting_join(&self) -> usize {
        self.members
            .values()
            .filter(|member| member.join.is_some())
            .count()
    }

    /// Kafka's `hasAllMembersJoined`.
    fn all_joined(&self) -> bool {
        self.awaiting_join() == self.members.len() && self.pending.is_empty()
    }

    fn rebalance_timeout(&self) -> Duration {
        let ms = self
            .members
            .values()
            .map(|member| member.record.rebalance_timeout_ms.max(0))
            .max()
            .unwrap_or(0);
        Duration::from_millis(ms as u64)
    }

    /// The protocol every member supports with the most first-choice votes.
    /// Ties go to the earliest joined member's preference.
    fn select_protocol(&self) -> Option<String> {
        let mut candidates: Option<Vec<String>> = None;
        let mut ordered: Vec<&Member> = self.members.values().collect();
        ordered.sort_by_key(|member| member.join_order);
        for member in &ordered {
            let names: Vec<String> = member
                .protocols
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            candidates = Some(match candidates {
                None => names,
                Some(current) => current
                    .into_iter()
                    .filter(|name| names.contains(name))
                    .collect(),
            });
        }
        let candidates = candidates?;
        let mut votes: BTreeMap<String, usize> = BTreeMap::new();
        for member in &ordered {
            if let Some((name, _)) = member
                .protocols
                .iter()
                .find(|(name, _)| candidates.contains(name))
            {
                *votes.entry(name.clone()).or_default() += 1;
            }
        }
        let best = votes.values().copied().max()?;
        ordered
            .first()
            .and_then(|member| {
                member
                    .protocols
                    .iter()
                    .map(|(name, _)| name.clone())
                    .find(|name| votes.get(name) == Some(&best))
            })
            .or_else(|| {
                votes
                    .iter()
                    .find(|(_, count)| **count == best)
                    .map(|(name, _)| name.clone())
            })
    }

    /// Kafka's `supportsProtocols`: the type matches and at least one of the
    /// protocols is one every member supports.
    fn supports(&self, protocol_type: &str, protocols: &[(String, Vec<u8>)]) -> bool {
        if protocol_type.is_empty() || protocols.is_empty() {
            return false;
        }
        if self.members.is_empty() {
            return true;
        }
        if self.protocol_type.as_deref() != Some(protocol_type) {
            return false;
        }
        protocols.iter().any(|(name, _)| {
            self.members
                .values()
                .all(|member| member.protocols.iter().any(|(theirs, _)| theirs == name))
        })
    }

    fn roster(&self) -> Vec<JoinMember> {
        let mut listed: Vec<&Member> = self.members.values().collect();
        listed.sort_by_key(|member| member.join_order);
        listed
            .into_iter()
            .map(|member| JoinMember {
                member_id: member.record.member_id.clone(),
                group_instance_id: member.record.group_instance_id.clone(),
                metadata: member.metadata(self.protocol.as_ref()),
            })
            .collect()
    }

    /// The current generation's answer to a member's JoinGroup.
    fn current_join(&self, member_id: &str) -> JoinResponse {
        let leader = self.leader.clone().unwrap_or_default();
        JoinResponse {
            error: codes::NONE,
            generation: self.generation,
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol.clone(),
            members: if leader == member_id {
                self.roster()
            } else {
                Vec::new()
            },
            leader,
            member_id: member_id.to_string(),
            skip_assignment: false,
        }
    }
}

/// What the caller must do after releasing the lock.
enum After {
    Nothing,
    /// A join phase ended: store the new generation, then answer joins.
    SaveJoin {
        group: String,
        generation: i32,
        record: GroupRecord,
        at: Instant,
    },
    /// The leader synced: store the assignment, then answer syncs.
    SaveSync {
        group: String,
        generation: i32,
        record: GroupRecord,
    },
    /// A static member got a new id in a stable group: store it, then answer
    /// its join, or undo the replacement if the store fails.
    SaveReplace {
        group: String,
        record: GroupRecord,
        instance: String,
        old_id: String,
        new_id: String,
        old_protocols: Vec<(String, Vec<u8>)>,
        response: Box<JoinResponse>,
    },
}

/// The coordinator for every group this broker leads.
pub struct Coordinator {
    groups: Mutex<HashMap<String, Group>>,
    store: Arc<dyn Store>,
    config: Config,
}

impl Coordinator {
    pub fn new(store: Arc<dyn Store>, config: Config) -> Arc<Self> {
        Arc::new(Self {
            groups: Mutex::new(HashMap::new()),
            store,
            config,
        })
    }

    /// Replace every group with `records`, answering waiting members
    /// NOT_COORDINATOR. Called when this broker becomes the metadata leader.
    pub fn load(self: &Arc<Self>, records: Vec<(String, GroupRecord)>, now: Instant) {
        let mut groups = self.groups.lock().expect("groups");
        drop_all(&mut groups);
        for (id, record) in records {
            let mut group = Group::from_record(record, now);
            if group.state == State::PreparingRebalance {
                group.join_deadline = Some(now + group.rebalance_timeout());
            }
            groups.insert(id, group);
        }
    }

    /// Forget every group: this broker stopped leading metadata.
    pub fn unload(&self) {
        let mut groups = self.groups.lock().expect("groups");
        drop_all(&mut groups);
    }

    /// Mark a group Dead, as a deletion does. Waiting members are answered
    /// COORDINATOR_NOT_AVAILABLE.
    pub fn kill(&self, id: &str) {
        let mut groups = self.groups.lock().expect("groups");
        if let Some(group) = groups.get_mut(id) {
            for member in group.members.values_mut() {
                if let Some(tx) = member.join.take() {
                    let _ = tx.send(JoinResponse::error(
                        codes::COORDINATOR_NOT_AVAILABLE,
                        &member.record.member_id,
                    ));
                }
                if let Some(tx) = member.sync.take() {
                    let _ = tx.send(SyncResponse::error(codes::COORDINATOR_NOT_AVAILABLE));
                }
            }
            group.members.clear();
            group.pending.clear();
            group.state = State::Dead;
        }
    }

    /// Kafka's `acceptJoiningMember`: whether `group.max.size` lets this
    /// member in now.
    fn accepts(&self, group: &Group, member_id: &str) -> bool {
        match group.state {
            State::Empty | State::Dead => true,
            State::PreparingRebalance => {
                group
                    .members
                    .get(member_id)
                    .is_some_and(|member| member.join.is_some())
                    || group.awaiting_join() < self.config.max_size
            }
            State::CompletingRebalance | State::Stable => {
                group.members.contains_key(member_id) || group.members.len() < self.config.max_size
            }
        }
    }

    /// JoinGroup.
    pub fn join(self: &Arc<Self>, request: JoinRequest, now: Instant) -> Reply<JoinResponse> {
        if request.group.is_empty() {
            return Reply::Now(JoinResponse::error(
                codes::INVALID_GROUP_ID,
                &request.member_id,
            ));
        }
        let session = Duration::from_millis(request.session_timeout_ms.max(0) as u64);
        if session < self.config.min_session_timeout || session > self.config.max_session_timeout {
            return Reply::Now(JoinResponse::error(
                codes::INVALID_SESSION_TIMEOUT,
                &request.member_id,
            ));
        }
        let mut groups = self.groups.lock().expect("groups");
        let unknown = request.member_id.is_empty();
        if !groups.contains_key(&request.group) {
            if !unknown {
                return Reply::Now(JoinResponse::error(
                    codes::UNKNOWN_MEMBER_ID,
                    &request.member_id,
                ));
            }
            groups.insert(request.group.clone(), Group::new());
        }
        let group = groups.get_mut(&request.group).expect("group");
        if group.state == State::Dead {
            return Reply::Now(JoinResponse::error(
                codes::COORDINATOR_NOT_AVAILABLE,
                &request.member_id,
            ));
        }
        if !self.accepts(group, &request.member_id) {
            group.pending.remove(&request.member_id);
            return Reply::Now(JoinResponse::error(
                codes::GROUP_MAX_SIZE_REACHED,
                &request.member_id,
            ));
        }
        if !group.supports(&request.protocol_type, &request.protocols) {
            return Reply::Now(JoinResponse::error(
                codes::INCONSISTENT_GROUP_PROTOCOL,
                &request.member_id,
            ));
        }
        let id = request.group.clone();
        let (reply, after) = if unknown {
            self.join_new(&id, group, request, now)
        } else {
            self.join_existing(&id, group, request, now)
        };
        drop(groups);
        self.run(after);
        reply
    }

    fn join_new(
        &self,
        id: &str,
        group: &mut Group,
        request: JoinRequest,
        now: Instant,
    ) -> (Reply<JoinResponse>, After) {
        let fresh = format!("{}-{}", request.client_id, uuid::Uuid::new_v4());
        if let Some(instance) = request.group_instance_id.clone() {
            if group.static_member(&instance).is_some() {
                return self.replace_static(id, group, request, &instance, fresh, now);
            }
            return self.add_and_join(id, group, request, fresh, now);
        }
        if request.require_known_member_id {
            group.pending.insert(
                fresh.clone(),
                (
                    now,
                    Duration::from_millis(request.session_timeout_ms.max(0) as u64),
                ),
            );
            return (
                Reply::Now(JoinResponse::error(codes::MEMBER_ID_REQUIRED, &fresh)),
                After::Nothing,
            );
        }
        self.add_and_join(id, group, request, fresh, now)
    }

    fn join_existing(
        &self,
        id: &str,
        group: &mut Group,
        request: JoinRequest,
        now: Instant,
    ) -> (Reply<JoinResponse>, After) {
        let member_id = request.member_id.clone();
        if let Some(instance) = &request.group_instance_id {
            match group.static_member(instance) {
                Some(existing) if existing != member_id => {
                    return (
                        Reply::Now(JoinResponse::error(codes::FENCED_INSTANCE_ID, &member_id)),
                        After::Nothing,
                    );
                }
                None if !group.pending.contains_key(&member_id) => {
                    return (
                        Reply::Now(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &member_id)),
                        After::Nothing,
                    );
                }
                _ => {}
            }
        }
        if group.pending.remove(&member_id).is_some() {
            return self.add_and_join(id, group, request, member_id, now);
        }
        if !group.members.contains_key(&member_id) {
            return (
                Reply::Now(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &member_id)),
                After::Nothing,
            );
        }
        let is_leader = group.leader.as_deref() == Some(member_id.as_str());
        let matches = group.members[&member_id].protocols == request.protocols;
        match group.state {
            State::Stable if matches && !is_leader => {
                group.members.get_mut(&member_id).expect("member").last_seen = now;
                (Reply::Now(group.current_join(&member_id)), After::Nothing)
            }
            State::CompletingRebalance if matches => {
                group.members.get_mut(&member_id).expect("member").last_seen = now;
                (Reply::Now(group.current_join(&member_id)), After::Nothing)
            }
            State::Empty => (
                Reply::Now(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &member_id)),
                After::Nothing,
            ),
            _ => {
                let (tx, rx) = oneshot::channel();
                let member = group.members.get_mut(&member_id).expect("member");
                update_member(member, &request, now);
                if let Some(old) = member.join.replace(tx) {
                    let _ = old.send(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &member_id));
                }
                if group.state != State::PreparingRebalance {
                    self.prepare(group, now);
                }
                (Reply::Later(rx), self.maybe_complete_join(id, group, now))
            }
        }
    }

    /// Add a new member that waits for the join phase.
    fn add_and_join(
        &self,
        id: &str,
        group: &mut Group,
        request: JoinRequest,
        member_id: String,
        now: Instant,
    ) -> (Reply<JoinResponse>, After) {
        let (tx, rx) = oneshot::channel();
        let mut member = Member {
            record: MemberRecord::default(),
            protocols: Vec::new(),
            last_seen: now,
            join: Some(tx),
            sync: None,
            new_since: Some(now),
            join_order: group.next_order,
        };
        group.next_order += 1;
        member.record.member_id = member_id.clone();
        update_member(&mut member, &request, now);
        if group.members.is_empty() {
            group.protocol_type = Some(request.protocol_type.clone());
        }
        group.members.insert(member_id, member);
        if let Some(initial) = group.initial.as_mut() {
            initial.joined = true;
        }
        if group.state != State::PreparingRebalance {
            self.prepare(group, now);
        }
        (Reply::Later(rx), self.maybe_complete_join(id, group, now))
    }

    /// A static member rejoined without a member id: it gets a new one and
    /// the old one is fenced (Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin`).
    fn replace_static(
        &self,
        id: &str,
        group: &mut Group,
        request: JoinRequest,
        instance: &str,
        new_id: String,
        now: Instant,
    ) -> (Reply<JoinResponse>, After) {
        let old_id = group.static_member(instance).expect("static member");
        let mut member = group.members.remove(&old_id).expect("member");
        if let Some(tx) = member.join.take() {
            let _ = tx.send(JoinResponse::error(codes::FENCED_INSTANCE_ID, &old_id));
        }
        if let Some(tx) = member.sync.take() {
            let _ = tx.send(SyncResponse::error(codes::FENCED_INSTANCE_ID));
        }
        if group.pending_sync.remove(&old_id) {
            group.pending_sync.insert(new_id.clone());
        }
        let old_protocols = member.protocols.clone();
        member.record.member_id = new_id.clone();
        update_member(&mut member, &request, now);
        let was_leader = group.leader.as_deref() == Some(old_id.as_str());
        group.members.insert(new_id.clone(), member);

        match group.state {
            State::Stable if group.select_protocol() == group.protocol => {
                let skip = request.supports_skip_assignment && was_leader;
                if skip {
                    group.leader = Some(new_id.clone());
                }
                let mut response = group.current_join(&new_id);
                response.skip_assignment = skip;
                if !skip {
                    // Kafka reports the leader it has; a replaced leader's
                    // old id stays there until the next rebalance.
                    response.members = Vec::new();
                }
                let (tx, rx) = oneshot::channel();
                group.members.get_mut(&new_id).expect("member").join = Some(tx);
                let after = After::SaveReplace {
                    group: id.to_string(),
                    record: group.record(),
                    instance: instance.to_string(),
                    old_id,
                    new_id,
                    old_protocols,
                    response: Box::new(response),
                };
                (Reply::Later(rx), after)
            }
            _ => {
                let (tx, rx) = oneshot::channel();
                group.members.get_mut(&new_id).expect("member").join = Some(tx);
                if group.state != State::PreparingRebalance {
                    self.prepare(group, now);
                }
                (Reply::Later(rx), self.maybe_complete_join(id, group, now))
            }
        }
    }

    /// Start a rebalance (Kafka's `prepareRebalance`).
    fn prepare(&self, group: &mut Group, now: Instant) {
        if group.state == State::CompletingRebalance {
            for member in group.members.values_mut() {
                if let Some(tx) = member.sync.take() {
                    let _ = tx.send(SyncResponse::error(codes::REBALANCE_IN_PROGRESS));
                }
            }
        }
        group.pending_sync.clear();
        group.sync_deadline = None;
        group.saving = false;
        if group.state == State::Empty {
            let remaining = group
                .rebalance_timeout()
                .saturating_sub(self.config.initial_rebalance_delay);
            group.initial = Some(InitialDelay {
                remaining,
                joined: false,
            });
            group.join_deadline = Some(now + self.config.initial_rebalance_delay);
        } else {
            group.initial = None;
            group.join_deadline = Some(now + group.rebalance_timeout());
        }
        group.state = State::PreparingRebalance;
    }

    /// End the join phase early once every member and pending member has
    /// joined (not during the initial delay).
    fn maybe_complete_join(&self, id: &str, group: &mut Group, now: Instant) -> After {
        if group.state != State::PreparingRebalance || group.saving || group.initial.is_some() {
            return After::Nothing;
        }
        if group.all_joined() {
            return self.complete_join(id, group, now);
        }
        After::Nothing
    }

    /// Kafka's `completeClassicGroupJoin`.
    fn complete_join(&self, id: &str, group: &mut Group, now: Instant) -> After {
        // Dynamic members that did not rejoin are dropped; static ones stay.
        group
            .members
            .retain(|_, member| member.join.is_some() || member.is_static());
        if group.members.is_empty() {
            group.generation += 1;
            group.state = State::Empty;
            group.protocol = None;
            group.leader = None;
            group.join_deadline = None;
            return After::SaveJoin {
                group: id.to_string(),
                generation: group.generation,
                record: group.record(),
                at: now,
            };
        }
        if group.awaiting_join() == 0 {
            // Only static members that have not rejoined: wait longer.
            group.join_deadline = Some(now + group.rebalance_timeout());
            return After::Nothing;
        }
        group.generation += 1;
        group.protocol = group.select_protocol();
        let leader_joined = group
            .leader
            .as_ref()
            .and_then(|leader| group.members.get(leader))
            .is_some_and(|member| member.join.is_some());
        if !leader_joined {
            group.leader = group
                .members
                .values()
                .filter(|member| member.join.is_some())
                .min_by_key(|member| member.join_order)
                .map(|member| member.record.member_id.clone());
        }
        group.pending_sync = group
            .members
            .values()
            .filter(|member| member.join.is_some())
            .map(|member| member.record.member_id.clone())
            .collect();
        for member in group.members.values_mut() {
            member.record.assignment.clear();
        }
        group.state = State::CompletingRebalance;
        group.join_deadline = None;
        group.sync_deadline = Some(now + group.rebalance_timeout());
        group.saving = true;
        After::SaveJoin {
            group: id.to_string(),
            generation: group.generation,
            record: group.record(),
            at: now,
        }
    }

    fn run(self: &Arc<Self>, after: After) {
        let this = Arc::clone(self);
        match after {
            After::Nothing => {}
            After::SaveJoin {
                group,
                generation,
                record,
                at,
            } => {
                let save = self.store.save(group.clone(), record);
                tokio::spawn(async move {
                    let result = save.await;
                    this.joined(&group, generation, result, at);
                });
            }
            After::SaveSync {
                group,
                generation,
                record,
            } => {
                let save = self.store.save(group.clone(), record);
                tokio::spawn(async move {
                    let result = save.await;
                    this.synced(&group, generation, result);
                });
            }
            After::SaveReplace {
                group,
                record,
                instance,
                old_id,
                new_id,
                old_protocols,
                response,
            } => {
                let save = self.store.save(group.clone(), record);
                tokio::spawn(async move {
                    let result = save.await;
                    this.replaced(
                        &group,
                        &instance,
                        &old_id,
                        &new_id,
                        old_protocols,
                        *response,
                        result,
                    );
                });
            }
        }
    }

    /// The new generation is stored: answer every waiting JoinGroup.
    /// `at` is when the join phase completed: sessions restart from then.
    fn joined(&self, id: &str, generation: i32, result: Result<(), i16>, at: Instant) {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return;
        };
        if group.generation != generation {
            return;
        }
        group.saving = false;
        if let Err(code) = result {
            for member in group.members.values_mut() {
                if let Some(tx) = member.join.take() {
                    let _ = tx.send(JoinResponse::error(code, &member.record.member_id));
                }
            }
            return;
        }
        let now = at;
        let ids: Vec<String> = group
            .members
            .values()
            .filter(|member| member.join.is_some())
            .map(|member| member.record.member_id.clone())
            .collect();
        for member_id in ids {
            let response = group.current_join(&member_id);
            let member = group.members.get_mut(&member_id).expect("member");
            member.new_since = None;
            member.last_seen = now;
            if let Some(tx) = member.join.take() {
                let _ = tx.send(response);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn replaced(
        &self,
        id: &str,
        instance: &str,
        old_id: &str,
        new_id: &str,
        old_protocols: Vec<(String, Vec<u8>)>,
        response: JoinResponse,
        result: Result<(), i16>,
    ) {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return;
        };
        let Some(mut member) = group.members.remove(new_id) else {
            return;
        };
        let tx = member.join.take();
        match result {
            Ok(()) => {
                group.members.insert(new_id.to_string(), member);
                if let Some(tx) = tx {
                    let _ = tx.send(response);
                }
            }
            Err(code) => {
                // Undo: the old member id and protocols come back.
                member.record.member_id = old_id.to_string();
                member.protocols = old_protocols;
                debug_assert_eq!(member.record.group_instance_id.as_deref(), Some(instance));
                if group.leader.as_deref() == Some(new_id) {
                    group.leader = Some(old_id.to_string());
                }
                if group.pending_sync.remove(new_id) {
                    group.pending_sync.insert(old_id.to_string());
                }
                group.members.insert(old_id.to_string(), member);
                if let Some(tx) = tx {
                    let code = if code == codes::NOT_COORDINATOR {
                        code
                    } else {
                        codes::COORDINATOR_NOT_AVAILABLE
                    };
                    let _ = tx.send(JoinResponse::error(code, new_id));
                }
            }
        }
    }

    /// SyncGroup. `assignments` come from the leader; followers send none.
    #[allow(clippy::too_many_arguments)]
    pub fn sync(
        self: &Arc<Self>,
        id: &str,
        member_id: &str,
        instance: Option<&str>,
        generation: i32,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Vec<(String, Vec<u8>)>,
        now: Instant,
    ) -> Reply<SyncResponse> {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return Reply::Now(SyncResponse::error(codes::UNKNOWN_MEMBER_ID));
        };
        if group.state == State::Dead {
            return Reply::Now(SyncResponse::error(codes::COORDINATOR_NOT_AVAILABLE));
        }
        if let Err(code) = group.validate_member(member_id, instance) {
            return Reply::Now(SyncResponse::error(code));
        }
        if generation != group.generation {
            return Reply::Now(SyncResponse::error(codes::ILLEGAL_GENERATION));
        }
        if protocol_type.is_some_and(|value| Some(value) != group.protocol_type.as_deref())
            || protocol_name.is_some_and(|value| Some(value) != group.protocol.as_deref())
        {
            return Reply::Now(SyncResponse::error(codes::INCONSISTENT_GROUP_PROTOCOL));
        }
        group.pending_sync.remove(member_id);
        if group.pending_sync.is_empty() {
            group.sync_deadline = None;
        }
        let protocol_type = group.protocol_type.clone();
        let protocol = group.protocol.clone();
        match group.state {
            State::Empty | State::Dead => Reply::Now(SyncResponse::error(codes::UNKNOWN_MEMBER_ID)),
            State::PreparingRebalance => {
                Reply::Now(SyncResponse::error(codes::REBALANCE_IN_PROGRESS))
            }
            State::Stable => {
                let member = group.members.get_mut(member_id).expect("member");
                member.last_seen = now;
                Reply::Now(SyncResponse {
                    error: codes::NONE,
                    protocol_type,
                    protocol_name: protocol,
                    assignment: member.record.assignment.clone(),
                })
            }
            State::CompletingRebalance => {
                let (tx, rx) = oneshot::channel();
                let is_leader = group.leader.as_deref() == Some(member_id);
                {
                    let member = group.members.get_mut(member_id).expect("member");
                    member.last_seen = now;
                    if let Some(old) = member.sync.replace(tx) {
                        let _ = old.send(SyncResponse::error(codes::REBALANCE_IN_PROGRESS));
                    }
                }
                let mut after = After::Nothing;
                if is_leader && !group.saving {
                    let given: HashMap<String, Vec<u8>> = assignments.into_iter().collect();
                    for member in group.members.values_mut() {
                        member.record.assignment = given
                            .get(&member.record.member_id)
                            .cloned()
                            .unwrap_or_default();
                    }
                    group.saving = true;
                    after = After::SaveSync {
                        group: id.to_string(),
                        generation: group.generation,
                        record: group.record(),
                    };
                }
                drop(groups);
                self.run(after);
                Reply::Later(rx)
            }
        }
    }

    /// The assignment is stored: the group is stable; answer every SyncGroup.
    fn synced(&self, id: &str, generation: i32, result: Result<(), i16>) {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return;
        };
        if group.generation != generation || group.state != State::CompletingRebalance {
            return;
        }
        group.saving = false;
        if let Err(code) = result {
            let code = if code == codes::NOT_COORDINATOR {
                code
            } else {
                codes::COORDINATOR_NOT_AVAILABLE
            };
            for member in group.members.values_mut() {
                if let Some(tx) = member.sync.take() {
                    let _ = tx.send(SyncResponse::error(code));
                }
            }
            return;
        }
        group.state = State::Stable;
        let protocol_type = group.protocol_type.clone();
        let protocol = group.protocol.clone();
        for member in group.members.values_mut() {
            if let Some(tx) = member.sync.take() {
                let _ = tx.send(SyncResponse {
                    error: codes::NONE,
                    protocol_type: protocol_type.clone(),
                    protocol_name: protocol.clone(),
                    assignment: member.record.assignment.clone(),
                });
            }
        }
    }

    /// Heartbeat (Kafka's `classicGroupHeartbeat`).
    pub fn heartbeat(
        &self,
        id: &str,
        member_id: &str,
        instance: Option<&str>,
        generation: i32,
        now: Instant,
    ) -> i16 {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        if group.state == State::Dead {
            return codes::COORDINATOR_NOT_AVAILABLE;
        }
        if let Err(code) = group.validate_member(member_id, instance) {
            return code;
        }
        if generation != group.generation {
            return codes::ILLEGAL_GENERATION;
        }
        let state = group.state;
        group.members.get_mut(member_id).expect("member").last_seen = now;
        match state {
            State::PreparingRebalance => codes::REBALANCE_IN_PROGRESS,
            State::CompletingRebalance | State::Stable => codes::NONE,
            State::Empty | State::Dead => codes::UNKNOWN_MEMBER_ID,
        }
    }

    /// LeaveGroup. `Err` is a group-level error; otherwise one code per
    /// member, in order.
    pub fn leave(
        self: &Arc<Self>,
        id: &str,
        leaving: &[Leaving],
        now: Instant,
    ) -> Result<Vec<i16>, i16> {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return Err(codes::UNKNOWN_MEMBER_ID);
        };
        if group.state == State::Dead {
            return Err(codes::COORDINATOR_NOT_AVAILABLE);
        }
        let mut answers = Vec::with_capacity(leaving.len());
        let mut removed = false;
        let mut unblocked = false;
        for one in leaving {
            let target = match &one.group_instance_id {
                Some(instance) => match group.static_member(instance) {
                    None => {
                        answers.push(codes::UNKNOWN_MEMBER_ID);
                        continue;
                    }
                    Some(found) if !one.member_id.is_empty() && found != one.member_id => {
                        answers.push(codes::FENCED_INSTANCE_ID);
                        continue;
                    }
                    Some(found) => found,
                },
                None => one.member_id.clone(),
            };
            if group.pending.remove(&target).is_some() {
                // A pending member is not in the generation; it only
                // matters to a join phase waiting on it.
                answers.push(codes::NONE);
                unblocked = true;
                continue;
            }
            let Some(mut member) = group.members.remove(&target) else {
                answers.push(codes::UNKNOWN_MEMBER_ID);
                continue;
            };
            if let Some(tx) = member.join.take() {
                let _ = tx.send(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &target));
            }
            if let Some(tx) = member.sync.take() {
                let _ = tx.send(SyncResponse::error(codes::UNKNOWN_MEMBER_ID));
            }
            group.pending_sync.remove(&target);
            answers.push(codes::NONE);
            removed = true;
        }
        let after = if removed {
            self.members_left(id, group, now)
        } else if unblocked {
            self.maybe_complete_join(id, group, now)
        } else {
            After::Nothing
        };
        drop(groups);
        self.run(after);
        Ok(answers)
    }

    /// After members left or expired (Kafka's
    /// `removeMemberAndUpdateClassicGroup`).
    fn members_left(&self, id: &str, group: &mut Group, now: Instant) -> After {
        match group.state {
            State::Stable | State::CompletingRebalance => {
                self.prepare(group, now);
                self.maybe_complete_join(id, group, now)
            }
            State::PreparingRebalance => self.maybe_complete_join(id, group, now),
            State::Empty | State::Dead => After::Nothing,
        }
    }

    /// Advance timers. Call every ~100ms.
    pub fn tick(self: &Arc<Self>, now: Instant) {
        let mut afters = Vec::new();
        {
            let mut groups = self.groups.lock().expect("groups");
            for (id, group) in groups.iter_mut() {
                if group.state == State::Dead {
                    continue;
                }
                // Pending members expire on their own session.
                let before = group.pending.len();
                group
                    .pending
                    .retain(|_, (since, session)| now.duration_since(*since) <= *session);
                let unblocked = group.pending.len() != before;
                let mut changed = false;

                // Heartbeats (Kafka's `onClassicGroupHeartbeatExpiration`): a
                // member awaiting join or sync is alive, unless it is new and
                // the new-member join timeout passed.
                let expired: Vec<String> = group
                    .members
                    .values()
                    .filter(|member| {
                        if let Some(since) = member.new_since {
                            if member.join.is_some() {
                                return now.duration_since(since)
                                    > self.config.new_member_join_timeout;
                            }
                        }
                        member.join.is_none()
                            && member.sync.is_none()
                            && now.duration_since(member.last_seen) > member.session()
                    })
                    .map(|member| member.record.member_id.clone())
                    .collect();
                for member_id in &expired {
                    if let Some(mut member) = group.members.remove(member_id) {
                        if let Some(tx) = member.join.take() {
                            let _ =
                                tx.send(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, member_id));
                        }
                    }
                    group.pending_sync.remove(member_id);
                    changed = true;
                }

                // Members that never sent SyncGroup for the generation.
                if group.sync_deadline.is_some_and(|deadline| now >= deadline)
                    && matches!(group.state, State::CompletingRebalance | State::Stable)
                    && !group.saving
                {
                    let unsynced: Vec<String> = group.pending_sync.drain().collect();
                    group.sync_deadline = None;
                    for member_id in unsynced {
                        group.members.remove(&member_id);
                        changed = true;
                    }
                }

                if changed {
                    afters.push(self.members_left(id, group, now));
                    continue;
                }
                if unblocked {
                    afters.push(self.maybe_complete_join(id, group, now));
                }

                if group.state == State::PreparingRebalance
                    && !group.saving
                    && group.join_deadline.is_some_and(|deadline| now >= deadline)
                {
                    let delay = self.config.initial_rebalance_delay;
                    if let Some(initial) = group.initial.as_mut() {
                        if initial.joined && !initial.remaining.is_zero() {
                            let next = delay.min(initial.remaining);
                            initial.remaining -= next;
                            initial.joined = false;
                            group.join_deadline = Some(now + next);
                            continue;
                        }
                    }
                    group.initial = None;
                    afters.push(self.complete_join(id, group, now));
                }
            }
        }
        for after in afters {
            self.run(after);
        }
    }

    /// Check an offset commit against live state (Kafka's
    /// `validateOffsetCommit`). The metadata group checks it again against
    /// the stored generation.
    pub fn check_commit(
        &self,
        id: &str,
        member_id: &str,
        instance: Option<&str>,
        generation: i32,
    ) -> i16 {
        let groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get(id) else {
            return if generation < 0 {
                codes::NONE
            } else {
                codes::ILLEGAL_GENERATION
            };
        };
        if group.state == State::Dead {
            return codes::COORDINATOR_NOT_AVAILABLE;
        }
        if generation < 0 && group.state == State::Empty {
            return codes::NONE;
        }
        if let Err(code) = group.validate_member(member_id, instance) {
            return code;
        }
        if group.pending.contains_key(member_id) {
            return codes::UNKNOWN_MEMBER_ID;
        }
        if generation != group.generation {
            return codes::ILLEGAL_GENERATION;
        }
        if group.state == State::CompletingRebalance {
            return codes::REBALANCE_IN_PROGRESS;
        }
        codes::NONE
    }

    /// Groups this coordinator knows: id, state, protocol type.
    pub fn list(&self) -> Vec<(String, State, String)> {
        let groups = self.groups.lock().expect("groups");
        let mut out: Vec<_> = groups
            .iter()
            .filter(|(_, group)| group.state != State::Dead)
            .map(|(id, group)| {
                (
                    id.clone(),
                    group.state,
                    group.protocol_type.clone().unwrap_or_default(),
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// DescribeGroups for one group, or `None` if unknown. Outside Stable,
    /// Kafka reports no protocol and no member metadata.
    pub fn describe(&self, id: &str) -> Option<Described> {
        let groups = self.groups.lock().expect("groups");
        let group = groups.get(id)?;
        let stable = group.state == State::Stable;
        let mut members: Vec<&Member> = group.members.values().collect();
        members.sort_by_key(|member| member.join_order);
        Some(Described {
            state: group.state,
            protocol_type: group.protocol_type.clone().unwrap_or_default(),
            protocol: if stable {
                group.protocol.clone().unwrap_or_default()
            } else {
                String::new()
            },
            members: members
                .into_iter()
                .map(|member| DescribedMember {
                    member_id: member.record.member_id.clone(),
                    group_instance_id: member.record.group_instance_id.clone(),
                    client_id: member.record.client_id.clone(),
                    client_host: member.record.client_host.clone(),
                    metadata: if stable {
                        member.metadata(group.protocol.as_ref())
                    } else {
                        Vec::new()
                    },
                    assignment: member.record.assignment.clone(),
                })
                .collect(),
        })
    }

    /// Whether a group may be deleted: unknown here, or no members.
    pub fn deletable(&self, id: &str) -> bool {
        let groups = self.groups.lock().expect("groups");
        groups.get(id).is_none_or(|group| group.members.is_empty())
    }

    /// Forget a deleted group.
    pub fn forget(&self, id: &str) {
        self.groups.lock().expect("groups").remove(id);
    }

    /// Track a group that only has offsets, so it lists and describes.
    pub fn touch(&self, id: &str) {
        self.groups
            .lock()
            .expect("groups")
            .entry(id.to_string())
            .or_insert_with(Group::new);
    }
}

fn update_member(member: &mut Member, request: &JoinRequest, now: Instant) {
    member.record.group_instance_id = request.group_instance_id.clone();
    member.record.client_id = request.client_id.clone();
    member.record.client_host = request.client_host.clone();
    member.record.session_timeout_ms = request.session_timeout_ms;
    member.record.rebalance_timeout_ms = request.rebalance_timeout_ms;
    member.protocols = request.protocols.clone();
    member.last_seen = now;
}

fn drop_all(groups: &mut HashMap<String, Group>) {
    for group in groups.values_mut() {
        for member in group.members.values_mut() {
            if let Some(tx) = member.join.take() {
                let _ = tx.send(JoinResponse::error(
                    codes::NOT_COORDINATOR,
                    &member.record.member_id,
                ));
            }
            if let Some(tx) = member.sync.take() {
                let _ = tx.send(SyncResponse::error(codes::NOT_COORDINATOR));
            }
        }
    }
    groups.clear();
}

#[cfg(test)]
mod tests;
