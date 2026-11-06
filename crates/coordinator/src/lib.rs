//! Classic consumer group coordinator (Phase 3, ADR 0013).
//!
//! The metadata leader coordinates every group. Live membership is kept here
//! in memory; what must survive a failover goes through the metadata group
//! via [`Store`]: the record of each new generation before anyone hears of
//! it, and the assignment before any member receives it. A new coordinator
//! loads those records, so members of a stable group keep their generation
//! across a failover and the group does not split.
//!
//! Behavior follows the classic group protocol of Kafka 4.4.0: MEMBER_ID_
//! REQUIRED on first join from v4, the initial rebalance delay for an empty
//! group, rebalance and session timeouts, protocol selection by vote, and
//! the error codes in [`codes`].

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            initial_rebalance_delay: Duration::from_millis(3_000),
            min_session_timeout: Duration::from_millis(6_000),
            max_session_timeout: Duration::from_millis(1_800_000),
            max_size: usize::MAX,
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

struct Member {
    record: MemberRecord,
    protocols: Vec<(String, Vec<u8>)>,
    last_seen: Instant,
    join: Option<oneshot::Sender<JoinResponse>>,
    sync: Option<oneshot::Sender<SyncResponse>>,
    /// Joined since the current rebalance began.
    rejoined: bool,
    /// Sent SyncGroup for the current generation.
    synced: bool,
    join_order: u64,
}

struct Group {
    state: State,
    generation: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    leader: Option<String>,
    members: BTreeMap<String, Member>,
    /// Member ids handed out with MEMBER_ID_REQUIRED, not yet joined.
    pending: HashMap<String, Instant>,
    /// When the current join or sync phase gives up on stragglers.
    deadline: Option<Instant>,
    /// The latest point the initial delay may extend to.
    initial_cap: Option<Instant>,
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
            deadline: None,
            initial_cap: None,
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
                    rejoined: false,
                    synced: true,
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
            for member in group.members.values_mut() {
                member.synced = false;
            }
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
                    if let Some(protocol) = &self.protocol {
                        if let Some((_, metadata)) =
                            member.protocols.iter().find(|(name, _)| name == protocol)
                        {
                            record.subscription = metadata.clone();
                        }
                    }
                    record
                })
                .collect(),
        }
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

    /// The protocol every member supports that gets the most first-choice
    /// votes; ties go to the leader's preference order.
    fn select_protocol(&self) -> Option<String> {
        let mut candidates: Option<Vec<String>> = None;
        for member in self.members.values() {
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
        for member in self.members.values() {
            if let Some((name, _)) = member
                .protocols
                .iter()
                .find(|(name, _)| candidates.contains(name))
            {
                *votes.entry(name.clone()).or_default() += 1;
            }
        }
        let best = votes.values().copied().max()?;
        let first = self
            .leader
            .as_ref()
            .and_then(|leader| self.members.get(leader))
            .or_else(|| self.members.values().min_by_key(|member| member.join_order));
        first
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
}

/// What the caller must do after releasing the lock.
enum After {
    Nothing,
    /// A join phase ended: store the new generation, then answer joins.
    SaveJoin {
        group: String,
        generation: i32,
        record: GroupRecord,
    },
    /// The leader synced: store the assignment, then answer syncs.
    SaveSync {
        group: String,
        generation: i32,
        record: GroupRecord,
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
                group.deadline = Some(now + group.rebalance_timeout());
            }
            groups.insert(id, group);
        }
    }

    /// Forget every group: this broker stopped leading metadata.
    pub fn unload(&self) {
        let mut groups = self.groups.lock().expect("groups");
        drop_all(&mut groups);
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
        let group = groups
            .entry(request.group.clone())
            .or_insert_with(Group::new);
        if group.state == State::Dead {
            return Reply::Now(JoinResponse::error(
                codes::COORDINATOR_NOT_AVAILABLE,
                &request.member_id,
            ));
        }
        if !group.supports(&request.protocol_type, &request.protocols) {
            return Reply::Now(JoinResponse::error(
                codes::INCONSISTENT_GROUP_PROTOCOL,
                &request.member_id,
            ));
        }

        let mut member_id = request.member_id.clone();
        // Static membership (KIP-345): a known instance id takes over its
        // member; a different member id for it is fenced.
        if let Some(instance) = &request.group_instance_id {
            let previous = group
                .members
                .values()
                .find(|member| member.record.group_instance_id.as_ref() == Some(instance))
                .map(|member| member.record.member_id.clone());
            if let Some(previous) = previous {
                if !member_id.is_empty() && member_id != previous {
                    return Reply::Now(JoinResponse::error(codes::FENCED_INSTANCE_ID, &member_id));
                }
                member_id = previous;
            }
        }
        if member_id.is_empty() {
            if group.members.len() + group.pending.len() >= self.config.max_size {
                return Reply::Now(JoinResponse::error(codes::GROUP_MAX_SIZE_REACHED, ""));
            }
            let fresh = format!("{}-{}", request.client_id, uuid::Uuid::new_v4());
            if request.require_known_member_id && request.group_instance_id.is_none() {
                group.pending.insert(fresh.clone(), now);
                return Reply::Now(JoinResponse::error(codes::MEMBER_ID_REQUIRED, &fresh));
            }
            member_id = fresh;
        } else if !group.members.contains_key(&member_id)
            && group.pending.remove(&member_id).is_none()
        {
            return Reply::Now(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &member_id));
        }

        let existing = group.members.contains_key(&member_id);
        // A stable follower rejoining with the same subscription gets the
        // current generation back without a rebalance.
        if existing && group.state == State::Stable {
            let is_leader = group.leader.as_deref() == Some(member_id.as_str());
            let member = group.members.get_mut(&member_id).expect("member");
            if member.protocols == request.protocols && !is_leader {
                member.last_seen = now;
                return Reply::Now(JoinResponse {
                    error: codes::NONE,
                    generation: group.generation,
                    protocol_type: group.protocol_type.clone(),
                    protocol_name: group.protocol.clone(),
                    leader: group.leader.clone().unwrap_or_default(),
                    member_id,
                    members: Vec::new(),
                });
            }
        }

        let (tx, rx) = oneshot::channel();
        let order = group.next_order;
        if !existing {
            group.next_order += 1;
        }
        let member = group
            .members
            .entry(member_id.clone())
            .or_insert_with(|| Member {
                record: MemberRecord::default(),
                protocols: Vec::new(),
                last_seen: now,
                join: None,
                sync: None,
                rejoined: false,
                synced: false,
                join_order: order,
            });
        member.record.member_id = member_id.clone();
        member.record.group_instance_id = request.group_instance_id.clone();
        member.record.client_id = request.client_id.clone();
        member.record.client_host = request.client_host.clone();
        member.record.session_timeout_ms = request.session_timeout_ms;
        member.record.rebalance_timeout_ms = request.rebalance_timeout_ms;
        member.protocols = request.protocols.clone();
        member.last_seen = now;
        member.rejoined = true;
        if let Some(old) = member.join.replace(tx) {
            let _ = old.send(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &member_id));
        }
        if group.members.len() == 1 || group.protocol_type.is_none() {
            group.protocol_type = Some(request.protocol_type.clone());
        }

        match group.state {
            State::Empty => self.prepare(group, now, true),
            State::Stable | State::CompletingRebalance => self.prepare(group, now, false),
            State::PreparingRebalance => {
                if let Some(cap) = group.initial_cap {
                    // Initial delay: a new member pushes the end out by one
                    // delay, up to the cap.
                    if !existing {
                        let pushed = now + self.config.initial_rebalance_delay;
                        group.deadline = Some(pushed.min(cap));
                    }
                }
            }
            State::Dead => {}
        }
        let after = self.maybe_complete_join(&request.group, group, now);
        drop(groups);
        self.run(after);
        Reply::Later(rx)
    }

    /// Start a rebalance. Members who do not rejoin before the deadline are
    /// dropped.
    fn prepare(&self, group: &mut Group, now: Instant, initial: bool) {
        if group.state == State::CompletingRebalance {
            // Members waiting on SyncGroup learn the generation is over.
            for member in group.members.values_mut() {
                if let Some(tx) = member.sync.take() {
                    let _ = tx.send(SyncResponse::error(codes::REBALANCE_IN_PROGRESS));
                }
            }
        }
        group.state = State::PreparingRebalance;
        group.saving = false;
        for member in group.members.values_mut() {
            if member.join.is_none() {
                member.rejoined = false;
            }
            member.synced = false;
        }
        if initial {
            let cap = group
                .rebalance_timeout()
                .max(self.config.initial_rebalance_delay);
            group.initial_cap = Some(now + cap);
            group.deadline = Some(now + self.config.initial_rebalance_delay);
        } else {
            group.initial_cap = None;
            group.deadline = Some(now + group.rebalance_timeout());
        }
    }

    /// End the join phase when every member has rejoined (not during the
    /// initial delay) or the deadline passed.
    fn maybe_complete_join(&self, id: &str, group: &mut Group, now: Instant) -> After {
        if group.state != State::PreparingRebalance || group.saving {
            return After::Nothing;
        }
        let all_in = group.members.values().all(|member| member.rejoined);
        let due = group.deadline.is_some_and(|deadline| now >= deadline);
        if !(due || (all_in && group.initial_cap.is_none())) {
            return After::Nothing;
        }
        group.members.retain(|_, member| member.rejoined);
        group.pending.clear();
        group.initial_cap = None;
        group.generation += 1;
        if group.members.is_empty() {
            group.state = State::Empty;
            group.protocol = None;
            group.leader = None;
            group.deadline = None;
            return After::SaveJoin {
                group: id.to_string(),
                generation: group.generation,
                record: group.record(),
            };
        }
        group.protocol = group.select_protocol();
        let leader_kept = group
            .leader
            .as_ref()
            .is_some_and(|leader| group.members.contains_key(leader));
        if !leader_kept {
            group.leader = group
                .members
                .values()
                .min_by_key(|member| member.join_order)
                .map(|member| member.record.member_id.clone());
        }
        for member in group.members.values_mut() {
            member.record.assignment.clear();
            member.last_seen = now;
        }
        group.state = State::CompletingRebalance;
        group.deadline = Some(now + group.rebalance_timeout());
        group.saving = true;
        After::SaveJoin {
            group: id.to_string(),
            generation: group.generation,
            record: group.record(),
        }
    }

    fn run(self: &Arc<Self>, after: After) {
        match after {
            After::Nothing => {}
            After::SaveJoin {
                group,
                generation,
                record,
            } => {
                let this = Arc::clone(self);
                let save = self.store.save(group.clone(), record);
                tokio::spawn(async move {
                    let result = save.await;
                    this.joined(&group, generation, result);
                });
            }
            After::SaveSync {
                group,
                generation,
                record,
            } => {
                let this = Arc::clone(self);
                let save = self.store.save(group.clone(), record);
                tokio::spawn(async move {
                    let result = save.await;
                    this.synced(&group, generation, result);
                });
            }
        }
    }

    /// The new generation is stored: answer every member's JoinGroup.
    fn joined(&self, id: &str, generation: i32, result: Result<(), i16>) {
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
        let leader = group.leader.clone().unwrap_or_default();
        let protocol = group.protocol.clone();
        let mut listed: Vec<&Member> = group.members.values().collect();
        listed.sort_by_key(|member| member.join_order);
        let roster: Vec<JoinMember> = listed
            .iter()
            .map(|member| JoinMember {
                member_id: member.record.member_id.clone(),
                group_instance_id: member.record.group_instance_id.clone(),
                metadata: protocol
                    .as_ref()
                    .and_then(|name| member.protocols.iter().find(|(theirs, _)| theirs == name))
                    .map(|(_, metadata)| metadata.clone())
                    .unwrap_or_default(),
            })
            .collect();
        let protocol_type = group.protocol_type.clone();
        for member in group.members.values_mut() {
            member.rejoined = false;
            if let Some(tx) = member.join.take() {
                let is_leader = member.record.member_id == leader;
                let _ = tx.send(JoinResponse {
                    error: codes::NONE,
                    generation,
                    protocol_type: protocol_type.clone(),
                    protocol_name: protocol.clone(),
                    leader: leader.clone(),
                    member_id: member.record.member_id.clone(),
                    members: if is_leader {
                        roster.clone()
                    } else {
                        Vec::new()
                    },
                });
            }
        }
    }

    /// SyncGroup. `assignments` come from the leader; followers send none.
    #[allow(clippy::too_many_arguments)]
    pub fn sync(
        self: &Arc<Self>,
        id: &str,
        member_id: &str,
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
        if !group.members.contains_key(member_id) {
            return Reply::Now(SyncResponse::error(codes::UNKNOWN_MEMBER_ID));
        }
        if generation != group.generation {
            return Reply::Now(SyncResponse::error(codes::ILLEGAL_GENERATION));
        }
        if protocol_type.is_some_and(|value| Some(value) != group.protocol_type.as_deref())
            || protocol_name.is_some_and(|value| Some(value) != group.protocol.as_deref())
        {
            return Reply::Now(SyncResponse::error(codes::INCONSISTENT_GROUP_PROTOCOL));
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
                    member.synced = true;
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
            for member in group.members.values_mut() {
                if let Some(tx) = member.sync.take() {
                    let _ = tx.send(SyncResponse::error(code));
                }
            }
            return;
        }
        group.state = State::Stable;
        group.deadline = None;
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

    /// Heartbeat.
    pub fn heartbeat(&self, id: &str, member_id: &str, generation: i32, now: Instant) -> i16 {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        if group.state == State::Dead {
            return codes::COORDINATOR_NOT_AVAILABLE;
        }
        let state = group.state;
        let current = group.generation;
        let Some(member) = group.members.get_mut(member_id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        if generation != current {
            return codes::ILLEGAL_GENERATION;
        }
        member.last_seen = now;
        match state {
            State::Stable => codes::NONE,
            State::PreparingRebalance | State::CompletingRebalance => codes::REBALANCE_IN_PROGRESS,
            State::Empty | State::Dead => codes::UNKNOWN_MEMBER_ID,
        }
    }

    /// LeaveGroup for one member, by member id or (v3+) instance id.
    pub fn leave(
        self: &Arc<Self>,
        id: &str,
        member_id: &str,
        instance: Option<&str>,
        now: Instant,
    ) -> i16 {
        let mut groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get_mut(id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        let target = match instance {
            Some(instance) => {
                let found = group
                    .members
                    .values()
                    .find(|member| member.record.group_instance_id.as_deref() == Some(instance))
                    .map(|member| member.record.member_id.clone());
                match found {
                    Some(found) if member_id.is_empty() || found == member_id => found,
                    Some(_) => return codes::FENCED_INSTANCE_ID,
                    None => return codes::UNKNOWN_MEMBER_ID,
                }
            }
            None => member_id.to_string(),
        };
        let Some(mut member) = group.members.remove(&target) else {
            return if group.pending.remove(&target).is_some() {
                codes::NONE
            } else {
                codes::UNKNOWN_MEMBER_ID
            };
        };
        if let Some(tx) = member.join.take() {
            let _ = tx.send(JoinResponse::error(codes::UNKNOWN_MEMBER_ID, &target));
        }
        if let Some(tx) = member.sync.take() {
            let _ = tx.send(SyncResponse::error(codes::UNKNOWN_MEMBER_ID));
        }
        let after = self.member_left(id, group, now);
        drop(groups);
        self.run(after);
        codes::NONE
    }

    fn member_left(&self, id: &str, group: &mut Group, now: Instant) -> After {
        if group
            .leader
            .as_ref()
            .is_some_and(|leader| !group.members.contains_key(leader))
        {
            group.leader = None;
        }
        match group.state {
            State::Stable | State::CompletingRebalance => {
                self.prepare(group, now, false);
                self.maybe_complete_join(id, group, now)
            }
            State::PreparingRebalance => self.maybe_complete_join(id, group, now),
            State::Empty | State::Dead => After::Nothing,
        }
    }

    /// Advance timers: end join phases whose deadline passed, rebalance when
    /// members never synced, and expire members whose session lapsed. Call
    /// every ~100ms.
    pub fn tick(self: &Arc<Self>, now: Instant) {
        let mut afters = Vec::new();
        {
            let mut groups = self.groups.lock().expect("groups");
            for (id, group) in groups.iter_mut() {
                group
                    .pending
                    .retain(|_, since| now.duration_since(*since) < Duration::from_secs(300));
                if group.state != State::PreparingRebalance {
                    // A member waiting on join or sync is alive; one that is
                    // not rejoining is handled by the join deadline instead.
                    let expired: Vec<String> = group
                        .members
                        .values()
                        .filter(|member| {
                            member.join.is_none()
                                && member.sync.is_none()
                                && now.duration_since(member.last_seen)
                                    > Duration::from_millis(
                                        member.record.session_timeout_ms.max(0) as u64
                                    )
                        })
                        .map(|member| member.record.member_id.clone())
                        .collect();
                    if !expired.is_empty() {
                        for member_id in expired {
                            group.members.remove(&member_id);
                        }
                        afters.push(self.member_left(id, group, now));
                        continue;
                    }
                }
                let due = group.deadline.is_some_and(|deadline| now >= deadline);
                match group.state {
                    State::PreparingRebalance if due => {
                        afters.push(self.maybe_complete_join(id, group, now));
                    }
                    State::CompletingRebalance if due && !group.saving => {
                        // Members that never synced are dropped, then the
                        // group rebalances without them.
                        group.members.retain(|_, member| member.synced);
                        afters.push(self.member_left(id, group, now));
                    }
                    _ => {}
                }
            }
        }
        for after in afters {
            self.run(after);
        }
    }

    /// Check an offset commit from a member against live state. The metadata
    /// group checks it again against the stored generation.
    pub fn check_commit(&self, id: &str, member_id: &str, generation: i32) -> i16 {
        let groups = self.groups.lock().expect("groups");
        let Some(group) = groups.get(id) else {
            return if generation < 0 {
                codes::NONE
            } else {
                codes::ILLEGAL_GENERATION
            };
        };
        if generation < 0 {
            return if group.members.is_empty() {
                codes::NONE
            } else {
                codes::UNKNOWN_MEMBER_ID
            };
        }
        if !group.members.contains_key(member_id) {
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

    /// DescribeGroups for one group, or `None` if unknown.
    pub fn describe(&self, id: &str) -> Option<Described> {
        let groups = self.groups.lock().expect("groups");
        let group = groups.get(id)?;
        let mut members: Vec<&Member> = group.members.values().collect();
        members.sort_by_key(|member| member.join_order);
        Some(Described {
            state: group.state,
            protocol_type: group.protocol_type.clone().unwrap_or_default(),
            protocol: if group.state == State::Stable {
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
                    metadata: group
                        .protocol
                        .as_ref()
                        .and_then(|name| member.protocols.iter().find(|(theirs, _)| theirs == name))
                        .map(|(_, metadata)| metadata.clone())
                        .unwrap_or_default(),
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
