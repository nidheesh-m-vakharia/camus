//! Ported from Apache Kafka 4.4.0, `group-coordinator` tests:
//! `classic/ClassicGroupTest.java` and the classic-group cases of
//! `GroupMetadataManagerTest.java`. Each test keeps the Kafka name in snake
//! case. Kafka drives time with a mock clock; these pass `now` explicitly and
//! call `tick`. Where Kafka checks the records it writes, these check the
//! saves made through [`Store`]. `docs/kafka-coordinator-port.md` lists
//! every case, ported or not.

use super::*;

const DELAY: Duration = Duration::from_millis(3_000);
const DELAY_MS: u64 = 3_000;

/// Records every save; fails them while `fail` is set.
#[derive(Default)]
struct Saved {
    records: Mutex<Vec<(String, GroupRecord)>>,
    fail: Mutex<Option<i16>>,
}

impl Store for Saved {
    fn save(&self, group: String, record: GroupRecord) -> SaveFuture {
        let result = match *self.fail.lock().unwrap() {
            Some(code) => Err(code),
            None => {
                self.records.lock().unwrap().push((group, record));
                Ok(())
            }
        };
        Box::pin(async move { result })
    }
}

struct Ctx {
    c: Arc<Coordinator>,
    saved: Arc<Saved>,
    t: Instant,
}

impl Ctx {
    fn new() -> Self {
        Self::with(Config {
            initial_rebalance_delay: DELAY,
            min_session_timeout: Duration::from_millis(10),
            ..Config::default()
        })
    }

    fn with(config: Config) -> Self {
        let saved = Arc::new(Saved::default());
        Self {
            c: Coordinator::new(saved.clone(), config),
            saved,
            t: Instant::now(),
        }
    }

    /// Advance the mock clock and fire timers.
    async fn sleep(&mut self, ms: u64) {
        self.t += Duration::from_millis(ms);
        self.c.tick(self.t);
        settle().await;
    }

    fn state(&self) -> State {
        self.c.groups.lock().unwrap()["group-id"].state
    }

    fn generation(&self) -> i32 {
        self.c.groups.lock().unwrap()["group-id"].generation
    }

    fn members(&self) -> usize {
        self.c.groups.lock().unwrap()["group-id"].members.len()
    }

    fn pending(&self) -> usize {
        self.c.groups.lock().unwrap()["group-id"].pending.len()
    }

    fn has_member(&self, member: &str) -> bool {
        self.c.groups.lock().unwrap()["group-id"]
            .members
            .contains_key(member)
    }

    fn join(&self, request: JoinRequest) -> Pending<JoinResponse> {
        Pending::from(self.c.join(request, self.t))
    }

    fn sync(
        &self,
        member: &str,
        instance: Option<&str>,
        generation: i32,
        assign: Vec<(String, Vec<u8>)>,
    ) -> Pending<SyncResponse> {
        Pending::from(self.c.sync(
            "group-id", member, instance, generation, None, None, assign, self.t,
        ))
    }

    fn heartbeat(&self, member: &str, generation: i32) -> i16 {
        self.c
            .heartbeat("group-id", member, None, generation, self.t)
    }

    fn leave(&self, leaving: &[Leaving]) -> Result<Vec<i16>, i16> {
        self.c.leave("group-id", leaving, self.t)
    }

    /// Join with MEMBER_ID_REQUIRED, then rejoin with the id.
    async fn dynamic_join(&self, mut request: JoinRequest) -> (String, Pending<JoinResponse>) {
        request.require_known_member_id = true;
        let mut first = self.join(request.clone());
        let required = first.done().await.expect("answered");
        assert_eq!(required.error, codes::MEMBER_ID_REQUIRED);
        let member = required.member_id.clone();
        (
            member.clone(),
            self.join(JoinRequest {
                member_id: member,
                ..request
            }),
        )
    }

    /// One member joins and the initial delay passes
    /// (`joinClassicGroupAsDynamicMemberAndCompleteJoin`).
    async fn join_and_complete(&mut self, request: JoinRequest) -> JoinResponse {
        let mut joined = if request.group_instance_id.is_some() {
            self.join(request)
        } else {
            self.dynamic_join(request).await.1
        };
        self.sleep(DELAY_MS).await;
        joined.done().await.expect("join completes")
    }

    /// ... and the leader syncs (`joinClassicGroupAsDynamicMemberAndCompleteRebalance`).
    async fn join_and_rebalance(&mut self, mut request: JoinRequest) -> JoinResponse {
        // Kafka's helper uses these unless the test says otherwise.
        if request.session_timeout_ms == 500 && request.rebalance_timeout_ms == 500 {
            request.session_timeout_ms = 5_000;
            request.rebalance_timeout_ms = 10_000;
        }
        let joined = self.join_and_complete(request).await;
        let mut synced = self.sync(&joined.member_id, None, joined.generation, vec![]);
        assert_eq!(synced.done().await.expect("sync").error, codes::NONE);
        joined
    }

    /// Two static members join and rebalance (`staticMembersJoinAndRebalance`).
    async fn static_rebalance(&mut self) -> Rebalanced {
        self.static_rebalance_with(5_000).await
    }

    async fn static_rebalance_with(&mut self, session: i32) -> Rebalanced {
        let base = || {
            request()
                .protocols(&["range", "roundrobin"])
                .rebalance(10_000)
                .session(session)
        };
        let mut leader = self.join(base().instance("leader-instance-id").build());
        let mut follower = self.join(base().instance("follower-instance-id").build());
        // The follower joined during the delay, so it extends once.
        self.sleep(DELAY_MS).await;
        self.sleep(DELAY_MS).await;
        let leader = leader.done().await.expect("leader join");
        let follower = follower.done().await.expect("follower join");
        assert_eq!(leader.leader, leader.member_id);
        let assign = vec![
            (leader.member_id.clone(), b"leader".to_vec()),
            (follower.member_id.clone(), b"follower".to_vec()),
        ];
        let mut ls = self.sync(
            &leader.member_id,
            Some("leader-instance-id"),
            leader.generation,
            assign,
        );
        let mut fs = self.sync(
            &follower.member_id,
            Some("follower-instance-id"),
            follower.generation,
            vec![],
        );
        assert_eq!(ls.done().await.unwrap().error, 0);
        assert_eq!(fs.done().await.unwrap().error, 0);
        Rebalanced {
            leader: leader.member_id,
            follower: follower.member_id,
            generation: leader.generation,
        }
    }
}

struct Rebalanced {
    leader: String,
    follower: String,
    generation: i32,
}

async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

/// A reply that may arrive later.
struct Pending<T> {
    now: Option<T>,
    rx: Option<oneshot::Receiver<T>>,
}

impl<T> From<Reply<T>> for Pending<T> {
    fn from(reply: Reply<T>) -> Self {
        match reply {
            Reply::Now(value) => Self {
                now: Some(value),
                rx: None,
            },
            Reply::Later(rx) => Self {
                now: None,
                rx: Some(rx),
            },
        }
    }
}

impl<T> Pending<T> {
    /// The answer if it has arrived, after letting saves land.
    async fn done(&mut self) -> Option<T> {
        if let Some(value) = self.now.take() {
            return Some(value);
        }
        settle().await;
        let rx = self.rx.as_mut()?;
        match rx.try_recv() {
            Ok(value) => {
                self.rx = None;
                Some(value)
            }
            Err(_) => None,
        }
    }
}

struct Builder(JoinRequest);

fn request() -> Builder {
    Builder(JoinRequest {
        group: "group-id".into(),
        client_id: "client".into(),
        client_host: "/127.0.0.1".into(),
        protocol_type: "consumer".into(),
        protocols: protocols(&["range"]),
        // Kafka's JoinGroupRequestBuilder defaults.
        session_timeout_ms: 500,
        rebalance_timeout_ms: 500,
        ..Default::default()
    })
}

fn protocols(names: &[&str]) -> Vec<(String, Vec<u8>)> {
    names
        .iter()
        .map(|name| (name.to_string(), Vec::new()))
        .collect()
}

impl Builder {
    fn member(mut self, id: &str) -> Self {
        self.0.member_id = id.into();
        self
    }
    fn instance(mut self, id: &str) -> Self {
        self.0.group_instance_id = Some(id.into());
        self
    }
    fn protocols(mut self, names: &[&str]) -> Self {
        self.0.protocols = protocols(names);
        self
    }
    fn protocol_type(mut self, value: &str) -> Self {
        self.0.protocol_type = value.into();
        self
    }
    fn session(mut self, ms: i32) -> Self {
        self.0.session_timeout_ms = ms;
        self
    }
    fn rebalance(mut self, ms: i32) -> Self {
        self.0.rebalance_timeout_ms = ms;
        self
    }
    fn skip_assignment(mut self) -> Self {
        self.0.supports_skip_assignment = true;
        self
    }
    fn not_required(mut self) -> Self {
        self.0.require_known_member_id = false;
        self
    }
    fn required(mut self) -> Self {
        self.0.require_known_member_id = true;
        self
    }
    fn build(self) -> JoinRequest {
        self.0
    }
}

fn leaving(member: &str, instance: Option<&str>) -> Leaving {
    Leaving {
        member_id: member.into(),
        group_instance_id: instance.map(String::from),
    }
}

// --- ClassicGroupTest ---------------------------------------------------------

fn bare(names: &[&str], order: u64) -> Member {
    Member {
        record: MemberRecord {
            member_id: format!("member{order}"),
            ..Default::default()
        },
        protocols: protocols(names),
        last_seen: Instant::now(),
        join: None,
        sync: None,
        new_since: None,
        join_order: order,
    }
}

#[test]
fn test_select_protocol() {
    let mut group = Group::new();
    group
        .members
        .insert("member1".into(), bare(&["range", "roundrobin"], 1));
    group
        .members
        .insert("member2".into(), bare(&["roundrobin", "range"], 2));
    let selected = group.select_protocol().unwrap();
    assert!(selected == "range" || selected == "roundrobin");
    group
        .members
        .insert("member3".into(), bare(&["roundrobin", "range"], 3));
    assert_eq!(group.select_protocol().as_deref(), Some("roundrobin"));
}

#[test]
fn test_select_protocol_raises_if_no_members() {
    assert_eq!(Group::new().select_protocol(), None);
}

#[test]
fn test_select_protocol_chooses_compatible_protocol() {
    let mut group = Group::new();
    group
        .members
        .insert("member1".into(), bare(&["range", "roundrobin"], 1));
    group
        .members
        .insert("member2".into(), bare(&["roundrobin", "foo"], 2));
    assert_eq!(group.select_protocol().as_deref(), Some("roundrobin"));
}

#[test]
fn test_supports_protocols() {
    let mut group = Group::new();
    assert!(group.supports("consumer", &protocols(&["range", "roundrobin"])));
    group.protocol_type = Some("consumer".into());
    group
        .members
        .insert("member1".into(), bare(&["range", "roundrobin"], 1));
    assert!(group.supports("consumer", &protocols(&["roundrobin", "foo"])));
    assert!(!group.supports("consumer", &protocols(&["foo", "bar"])));
    assert!(!group.supports("invalid_type", &protocols(&["range"])));
    group
        .members
        .insert("member2".into(), bare(&["roundrobin", "foo"], 2));
    assert!(!group.supports("consumer", &protocols(&["range"])));
    assert!(group.supports("consumer", &protocols(&["roundrobin"])));
}

#[test]
fn test_validate_offset_commit() {
    let c = Coordinator::new(Arc::new(Saved::default()), Config::default());
    // A call from the admin client without any parameters should pass.
    assert_eq!(c.check_commit("group-id", "", None, -1), codes::NONE);
    let mut group = Group::new();
    let mut member = bare(&["roundrobin"], 0);
    member.record.member_id = "member-id".into();
    member.record.group_instance_id = Some("instance-id".into());
    group.members.insert("member-id".into(), member);
    group.generation = 1;
    group.state = State::CompletingRebalance;
    c.groups.lock().unwrap().insert("group-id".into(), group);
    // No parameters and the group is not empty.
    assert_eq!(
        c.check_commit("group-id", "", None, -1),
        codes::UNKNOWN_MEMBER_ID
    );
    // The member id does not exist.
    assert_eq!(
        c.check_commit("group-id", "unknown", Some("unknown"), -1),
        codes::UNKNOWN_MEMBER_ID
    );
    // The instance id does not exist.
    assert_eq!(
        c.check_commit("group-id", "member-id", Some("unknown"), -1),
        codes::UNKNOWN_MEMBER_ID
    );
    // The generation id is invalid.
    assert_eq!(
        c.check_commit("group-id", "member-id", Some("instance-id"), 0),
        codes::ILLEGAL_GENERATION
    );
    // The group is completing a rebalance.
    assert_eq!(
        c.check_commit("group-id", "member-id", Some("instance-id"), 1),
        codes::REBALANCE_IN_PROGRESS
    );
    c.groups.lock().unwrap().get_mut("group-id").unwrap().state = State::Stable;
    assert_eq!(
        c.check_commit("group-id", "member-id", Some("instance-id"), 1),
        codes::NONE
    );
    // Replace the static member: the old member id is fenced.
    {
        let mut groups = c.groups.lock().unwrap();
        let group = groups.get_mut("group-id").unwrap();
        let mut member = group.members.remove("member-id").unwrap();
        member.record.member_id = "new-member-id".into();
        group.members.insert("new-member-id".into(), member);
    }
    assert_eq!(
        c.check_commit("group-id", "member-id", Some("instance-id"), 1),
        codes::FENCED_INSTANCE_ID
    );
    c.kill("group-id");
    assert_eq!(
        c.check_commit("group-id", "member-id", Some("new-instance-id"), 1),
        codes::COORDINATOR_NOT_AVAILABLE
    );
}

#[test]
fn test_validate_delete_group() {
    let c = Coordinator::new(Arc::new(Saved::default()), Config::default());
    assert!(c.deletable("unknown"));
    let mut group = Group::new();
    group.members.insert("member1".into(), bare(&["range"], 1));
    c.groups.lock().unwrap().insert("group-id".into(), group);
    assert!(!c.deletable("group-id"));
}

// --- GroupMetadataManagerTest: join --------------------------------------------

#[tokio::test]
async fn test_join_group_unknown_member_new_group() {
    let ctx = Ctx::new();
    let mut joined = ctx.join(request().member("member-id").build());
    assert_eq!(joined.done().await.unwrap().error, codes::UNKNOWN_MEMBER_ID);
    let mut joined = ctx.join(
        request()
            .member("member-id")
            .instance("group-instance-id")
            .build(),
    );
    assert_eq!(joined.done().await.unwrap().error, codes::UNKNOWN_MEMBER_ID);
}

#[tokio::test]
async fn test_classic_group_join_inconsistent_protocol_type() {
    let ctx = Ctx::new();
    let (_, _first) = ctx.dynamic_join(request().build()).await;
    let mut other = ctx.join(request().protocol_type("connect").build());
    assert_eq!(
        other.done().await.unwrap().error,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
}

#[tokio::test]
async fn test_join_group_with_empty_protocol_type() {
    let ctx = Ctx::new();
    let mut joined = ctx.join(request().protocol_type("").build());
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
    let mut joined = ctx.join(
        request()
            .protocol_type("")
            .instance("group-instance-id")
            .build(),
    );
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
}

#[tokio::test]
async fn test_join_group_with_empty_group_protocol() {
    let ctx = Ctx::new();
    let mut joined = ctx.join(request().protocols(&[]).build());
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
}

#[tokio::test]
async fn test_join_group_inconsistent_group_protocol() {
    let mut ctx = Ctx::new();
    let mut first = ctx.join(request().protocols(&["range"]).not_required().build());
    let mut second = ctx.join(request().protocols(&["roundrobin"]).not_required().build());
    assert_eq!(
        second.done().await.unwrap().error,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
    ctx.sleep(DELAY_MS).await;
    assert_eq!(first.done().await.unwrap().error, codes::NONE);
}

#[tokio::test]
async fn test_join_group_second_join_inconsistent_protocol() {
    let mut ctx = Ctx::new();
    let mut first = ctx.join(request().required().build());
    let required = first.done().await.unwrap();
    assert_eq!(required.error, codes::MEMBER_ID_REQUIRED);
    let member = required.member_id;
    let mut empty = ctx.join(request().member(&member).protocols(&[]).build());
    assert_eq!(
        empty.done().await.unwrap().error,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
    let mut ok = ctx.join(request().member(&member).protocols(&["range"]).build());
    assert!(ok.done().await.is_none());
    ctx.sleep(DELAY_MS).await;
    assert_eq!(ok.done().await.unwrap().error, codes::NONE);
}

#[tokio::test]
async fn test_join_group_unknown_consumer_existing_group() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    assert_eq!(joined.error, codes::NONE);
    let mut other = ctx.join(request().member("other-member-id").build());
    assert_eq!(other.done().await.unwrap().error, codes::UNKNOWN_MEMBER_ID);
}

#[tokio::test]
async fn test_join_group_unknown_consumer_new_dead_group() {
    let ctx = Ctx::new();
    ctx.c.touch("group-id");
    ctx.c.kill("group-id");
    let mut joined = ctx.join(request().build());
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::COORDINATOR_NOT_AVAILABLE
    );
}

#[tokio::test]
async fn test_existing_member_join_dead_group() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    assert_eq!(joined.error, codes::NONE);
    ctx.c.kill("group-id");
    let mut again = ctx.join(request().member(&joined.member_id).build());
    assert_eq!(
        again.done().await.unwrap().error,
        codes::COORDINATOR_NOT_AVAILABLE
    );
}

#[tokio::test]
async fn test_join_group_returns_the_protocol_type() {
    let mut ctx = Ctx::new();
    let leader = ctx.join(request().not_required().build());
    let follower = ctx.join(request().not_required().build());
    ctx.sleep(DELAY_MS).await;
    for mut joined in [leader, follower] {
        let response = joined.done().await.unwrap();
        assert_eq!(response.error, codes::NONE);
        assert_eq!(response.protocol_type.as_deref(), Some("consumer"));
        assert_eq!(response.protocol_name.as_deref(), Some("range"));
    }
}

#[tokio::test]
async fn test_join_group_protocol_type_is_not_provided_when_an_error_occurs() {
    let ctx = Ctx::new();
    let mut joined = ctx.join(request().member("member-id").build());
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::UNKNOWN_MEMBER_ID);
    assert_eq!(response.protocol_type, None);
}

#[tokio::test]
async fn test_delay_initial_rebalance_by_group_initial_rebalance_delay_on_empty_group() {
    let mut ctx = Ctx::new();
    let mut joined = ctx.join(request().not_required().build());
    ctx.sleep(DELAY_MS / 2).await;
    assert!(joined.done().await.is_none());
    ctx.sleep(DELAY_MS / 2 + 1).await;
    assert_eq!(joined.done().await.unwrap().error, codes::NONE);
}

#[tokio::test]
async fn test_reset_rebalance_delay_when_new_member_joins_group_during_initial_rebalance() {
    let mut ctx = Ctx::new();
    let rebalance = DELAY_MS as i32 * 3;
    let mut first = ctx.join(request().not_required().rebalance(rebalance).build());
    ctx.sleep(DELAY_MS - 1).await;
    let mut second = ctx.join(request().not_required().rebalance(rebalance).build());
    ctx.sleep(2).await;
    // The second member extended the delay.
    assert!(first.done().await.is_none());
    assert!(second.done().await.is_none());
    ctx.sleep(DELAY_MS - 2).await;
    assert!(first.done().await.is_none());
    ctx.sleep(2).await;
    assert_eq!(first.done().await.unwrap().error, codes::NONE);
    assert_eq!(second.done().await.unwrap().error, codes::NONE);
}

#[tokio::test]
async fn test_delay_rebalance_upto_rebalance_timeout() {
    let mut ctx = Ctx::new();
    let rebalance = DELAY_MS as i32 * 2;
    let mut first = ctx.join(request().not_required().rebalance(rebalance).build());
    let mut second = ctx.join(request().not_required().rebalance(rebalance).build());
    ctx.sleep(DELAY_MS + 1).await;
    let mut third = ctx.join(request().not_required().rebalance(rebalance).build());
    // Right before the rebalance timeout.
    ctx.sleep(DELAY_MS - 1).await;
    assert!(first.done().await.is_none());
    assert!(second.done().await.is_none());
    assert!(third.done().await.is_none());
    ctx.sleep(1).await;
    assert_eq!(first.done().await.unwrap().error, codes::NONE);
    assert_eq!(second.done().await.unwrap().error, codes::NONE);
    assert_eq!(third.done().await.unwrap().error, codes::NONE);
}

async fn two_member_group(ctx: &mut Ctx) -> (JoinResponse, JoinResponse) {
    let base = || request().rebalance(10_000).session(5_000);
    let leader = ctx.join_and_rebalance(base().build()).await;
    let (_follower, mut fj) = ctx.dynamic_join(base().build()).await;
    let mut lj = ctx.join(base().member(&leader.member_id).build());
    let leader = lj.done().await.unwrap();
    let follower = fj.done().await.unwrap();
    assert_eq!(leader.generation, follower.generation);
    assert_eq!(follower.leader, leader.member_id);
    (leader, follower)
}

#[tokio::test]
async fn test_join_group_existing_member_does_not_trigger_rebalance_in_stable_state() {
    let mut ctx = Ctx::new();
    let (leader, follower) = two_member_group(&mut ctx).await;
    let mut ls = ctx.sync(&leader.member_id, None, leader.generation, vec![]);
    let mut fs = ctx.sync(&follower.member_id, None, follower.generation, vec![]);
    assert_eq!(ls.done().await.unwrap().error, 0);
    assert_eq!(fs.done().await.unwrap().error, 0);
    assert_eq!(ctx.state(), State::Stable);
    let mut again = ctx.join(
        request()
            .member(&follower.member_id)
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    let response = again.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, follower.generation);
    assert!(response.members.is_empty());
    assert_eq!(ctx.state(), State::Stable);
}

#[tokio::test]
async fn test_join_group_as_existing_leader_triggers_rebalance_in_stable_state() {
    let mut ctx = Ctx::new();
    let leader = ctx.join_and_rebalance(request().build()).await;
    let mut again = ctx.join(request().member(&leader.member_id).build());
    let response = again.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, leader.generation + 1);
    assert_eq!(ctx.state(), State::CompletingRebalance);
}

#[tokio::test]
async fn test_join_group_existing_member_updated_metadata_triggers_rebalance() {
    let mut ctx = Ctx::new();
    let first = ctx
        .join_and_complete(request().protocols(&["range", "roundrobin"]).build())
        .await;
    let mut again = ctx.join(
        request()
            .member(&first.member_id)
            .protocols(&["roundrobin", "range"])
            .build(),
    );
    let response = again.done().await.unwrap();
    assert_eq!(response.generation, first.generation + 1);
    assert_eq!(response.protocol_name.as_deref(), Some("roundrobin"));
}

#[tokio::test]
async fn test_join_group_existing_member_in_empty_state() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    assert_eq!(
        ctx.leave(&[leaving(&joined.member_id, None)]),
        Ok(vec![codes::NONE])
    );
    settle().await;
    assert_eq!(ctx.state(), State::Empty);
    let mut again = ctx.join(request().member(&joined.member_id).build());
    assert_eq!(again.done().await.unwrap().error, codes::UNKNOWN_MEMBER_ID);
}

#[tokio::test]
async fn test_complete_join_remove_not_yet_rejoined_dynamic_members() {
    let mut ctx = Ctx::new();
    let first = ctx
        .join_and_rebalance(request().rebalance(1_000).session(1_000).build())
        .await;
    let (_second, mut sj) = ctx
        .dynamic_join(request().rebalance(1_000).session(1_000).build())
        .await;
    ctx.sleep(1_000).await;
    let response = sj.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(ctx.members(), 1);
    assert!(!ctx.has_member(&first.member_id));
    assert_eq!(ctx.generation(), 2);
}

#[tokio::test]
async fn test_complete_join_phase_no_members_rejoined_extends_join_phase() {
    let mut ctx = Ctx::new();
    let base = || request().session(30_000).rebalance(10_000);
    let first = ctx
        .join_and_complete(base().instance("first-instance-id").build())
        .await;
    assert_eq!(first.error, codes::NONE);
    let mut second = ctx.join(base().instance("second-instance-id").build());
    assert!(second.done().await.is_none());
    let mut first_again = ctx.join(
        base()
            .instance("first-instance-id")
            .member(&first.member_id)
            .build(),
    );
    assert_eq!(first_again.done().await.unwrap().error, codes::NONE);
    let second = second.done().await.unwrap();
    assert_eq!(ctx.members(), 2);
    assert_eq!(ctx.generation(), 2);
    // Trigger a rebalance; nobody rejoins.
    {
        let mut groups = ctx.c.groups.lock().unwrap();
        let group = groups.get_mut("group-id").unwrap();
        group.saving = false;
        ctx.c.prepare(group, ctx.t);
    }
    ctx.sleep(10_000).await;
    assert_eq!(ctx.state(), State::PreparingRebalance);
    ctx.sleep(10_000).await;
    assert_eq!(ctx.state(), State::PreparingRebalance);
    assert_eq!(ctx.members(), 2);
    assert_eq!(ctx.generation(), 2);
    let mut a = ctx.join(
        base()
            .instance("first-instance-id")
            .member(&first.member_id)
            .build(),
    );
    assert!(a.done().await.is_none());
    let mut b = ctx.join(
        base()
            .instance("second-instance-id")
            .member(&second.member_id)
            .build(),
    );
    assert_eq!(a.done().await.unwrap().error, codes::NONE);
    assert_eq!(b.done().await.unwrap().error, codes::NONE);
    assert_eq!(ctx.state(), State::CompletingRebalance);
    assert_eq!(ctx.generation(), 3);
}

#[tokio::test]
async fn test_second_member_partially_join_and_timeout() {
    let mut ctx = Ctx::new();
    let first = ctx
        .join_and_complete(
            request()
                .instance("leader-instance-id")
                .rebalance(10_000)
                .session(5_000)
                .build(),
        )
        .await;
    assert_eq!(first.generation, 1);
    let mut synced = ctx.sync(&first.member_id, Some("leader-instance-id"), 1, vec![]);
    assert_eq!(synced.done().await.unwrap().error, 0);
    let mut pending = ctx.join(request().required().session(5_000).build());
    assert_eq!(
        pending.done().await.unwrap().error,
        codes::MEMBER_ID_REQUIRED
    );
    assert_eq!(ctx.pending(), 1);
    for _ in 0..2 {
        ctx.sleep(2_500).await;
        assert_eq!(
            ctx.c
                .heartbeat("group-id", &first.member_id, None, 1, ctx.t),
            codes::NONE
        );
    }
    ctx.sleep(1).await;
    assert_eq!(ctx.pending(), 0);
    assert_eq!(ctx.members(), 1);
    assert_eq!(ctx.generation(), 1);
    assert_eq!(ctx.state(), State::Stable);
}

#[tokio::test]
async fn test_heartbeat_expiration_should_remove_member() {
    let mut ctx = Ctx::new();
    ctx.join_and_rebalance(request().build()).await;
    ctx.sleep(5_001).await;
    assert_eq!(ctx.members(), 0);
}

#[tokio::test]
async fn test_generation_id_increments_on_rebalance() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    assert_eq!(joined.generation, 1);
    let mut sync = ctx.sync(&joined.member_id, None, 1, vec![]);
    assert_eq!(sync.done().await.unwrap().error, codes::NONE);
    // The leader rejoins: a rebalance that completes at once.
    let mut again = ctx.join(request().member(&joined.member_id).build());
    let response = again.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, 2);
}

#[tokio::test]
async fn test_join_group_completion_when_pending_member_joins() {
    let mut ctx = Ctx::new();
    let leader = ctx.join_and_rebalance(request().build()).await;
    let base = || request().rebalance(10_000).session(5_000);
    let mut required = ctx.join(base().required().build());
    let pending = required.done().await.unwrap().member_id;
    let mut lj = ctx.join(base().member(&leader.member_id).build());
    assert!(
        lj.done().await.is_none(),
        "a pending member blocks completion"
    );
    let mut pj = ctx.join(base().member(&pending).build());
    assert_eq!(lj.done().await.unwrap().error, codes::NONE);
    assert_eq!(pj.done().await.unwrap().error, codes::NONE);
    assert_eq!(ctx.members(), 2);
    assert_eq!(ctx.pending(), 0);
}

#[tokio::test]
async fn test_join_group_completion_when_pending_member_times_out() {
    let mut ctx = Ctx::new();
    let leader = ctx.join_and_rebalance(request().build()).await;
    let mut required = ctx.join(request().required().session(2_500).build());
    required.done().await.unwrap();
    let mut lj = ctx.join(
        request()
            .member(&leader.member_id)
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert!(lj.done().await.is_none());
    ctx.sleep(2_501).await;
    assert_eq!(lj.done().await.unwrap().error, codes::NONE);
    assert_eq!(ctx.members(), 1);
    assert_eq!(ctx.pending(), 0);
}

fn small_group() -> Ctx {
    Ctx::with(Config {
        max_size: 10,
        initial_rebalance_delay: Duration::from_millis(50),
        min_session_timeout: Duration::from_millis(10),
        ..Config::default()
    })
}

#[tokio::test]
async fn test_dynamic_members_join_group_with_max_size_and_required_known_member() {
    let mut ctx = small_group();
    let mut ids = Vec::new();
    for _ in 0..11 {
        let mut first = ctx.join(request().required().build());
        let response = first.done().await.unwrap();
        assert_eq!(response.error, codes::MEMBER_ID_REQUIRED);
        ids.push(response.member_id);
    }
    assert_eq!(ctx.state(), State::Empty);
    assert_eq!(ctx.pending(), 11);
    let mut second: Vec<_> = ids
        .iter()
        .map(|id| ctx.join(request().member(id).build()))
        .collect();
    ctx.sleep(50).await;
    ctx.sleep(50).await;
    let mut seen = Vec::new();
    for joined in &mut second {
        seen.push(joined.done().await.unwrap().error);
    }
    assert_eq!(seen.iter().filter(|code| **code == codes::NONE).count(), 10);
    assert_eq!(
        seen.iter()
            .filter(|code| **code == codes::GROUP_MAX_SIZE_REACHED)
            .count(),
        1
    );
    assert_eq!(ctx.members(), 10);
    assert_eq!(ctx.pending(), 0);
    assert_eq!(ctx.state(), State::CompletingRebalance);
    let mut third: Vec<_> = ids
        .iter()
        .map(|id| ctx.join(request().member(id).build()))
        .collect();
    let mut rejected = 0;
    for joined in &mut third {
        if let Some(response) = joined.done().await {
            if response.error == codes::GROUP_MAX_SIZE_REACHED {
                rejected += 1;
            }
        }
    }
    assert_eq!(rejected, 1);
}

#[tokio::test]
async fn test_static_members_join_group_with_max_size() {
    let mut ctx = small_group();
    let mut joins: Vec<_> = (0..11)
        .map(|index| ctx.join(request().instance(&format!("instance-id-{index}")).build()))
        .collect();
    ctx.sleep(50).await;
    ctx.sleep(50).await;
    let (mut ok, mut full) = (0, 0);
    for joined in &mut joins {
        match joined.done().await.unwrap().error {
            codes::NONE => ok += 1,
            codes::GROUP_MAX_SIZE_REACHED => full += 1,
            other => panic!("unexpected {other}"),
        }
    }
    assert_eq!((ok, full), (10, 1));
    assert_eq!(ctx.members(), 10);
}

// --- static membership ------------------------------------------------------------

#[tokio::test]
async fn test_static_member_join_as_first_member() {
    let mut ctx = Ctx::new();
    let mut joined = ctx.join(request().instance("group-instance-id").build());
    assert!(
        joined.done().await.is_none(),
        "no MEMBER_ID_REQUIRED for a static member"
    );
    ctx.sleep(DELAY_MS).await;
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.leader, response.member_id);
}

#[tokio::test]
async fn test_get_different_static_member_id_after_each_rejoin() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut last = rebalanced.leader.clone();
    let saves = ctx.saved.records.lock().unwrap().len();
    for _ in 0..5 {
        let mut joined = ctx.join(
            request()
                .instance("leader-instance-id")
                .protocols(&["range", "roundrobin"])
                .build(),
        );
        let response = joined.done().await.unwrap();
        assert_eq!(response.error, codes::NONE);
        assert_ne!(response.member_id, last);
        last = response.member_id;
    }
    assert_eq!(ctx.saved.records.lock().unwrap().len(), saves + 5);
}

#[tokio::test]
async fn test_static_member_rejoin_with_leader_id_and_unknown_member_id() {
    for skip in [false, true] {
        let mut ctx = Ctx::new();
        let rebalanced = ctx.static_rebalance().await;
        let mut builder = request()
            .instance("leader-instance-id")
            .protocols(&["range", "roundrobin"]);
        if skip {
            builder = builder.skip_assignment();
        }
        let mut joined = ctx.join(builder.build());
        let response = joined.done().await.unwrap();
        assert_eq!(response.error, codes::NONE);
        assert_eq!(response.generation, rebalanced.generation, "no rebalance");
        assert_eq!(ctx.state(), State::Stable);
        assert_eq!(response.skip_assignment, skip);
        if skip {
            assert_eq!(response.leader, response.member_id);
            assert_eq!(response.members.len(), 2);
        } else {
            assert_eq!(response.leader, rebalanced.leader);
            assert!(response.members.is_empty());
        }
        let mut old = ctx.join(
            request()
                .instance("leader-instance-id")
                .member(&rebalanced.leader)
                .build(),
        );
        assert_eq!(old.done().await.unwrap().error, codes::FENCED_INSTANCE_ID);
        let mut old_sync = ctx.sync(
            &rebalanced.leader,
            Some("leader-instance-id"),
            rebalanced.generation,
            vec![],
        );
        assert_eq!(
            old_sync.done().await.unwrap().error,
            codes::FENCED_INSTANCE_ID
        );
        let mut no_instance = ctx.sync(&rebalanced.leader, None, rebalanced.generation, vec![]);
        assert_eq!(
            no_instance.done().await.unwrap().error,
            codes::UNKNOWN_MEMBER_ID
        );
    }
}

#[tokio::test]
async fn test_static_member_rejoin_as_follower_with_unknown_member_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("follower-instance-id")
            .protocols(&["range", "roundrobin"])
            .build(),
    );
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, rebalanced.generation);
    assert_eq!(response.leader, rebalanced.leader);
    assert!(!response.skip_assignment);
    assert_ne!(response.member_id, rebalanced.follower);
    let mut synced = ctx.sync(
        &response.member_id,
        Some("follower-instance-id"),
        rebalanced.generation,
        vec![],
    );
    let synced = synced.done().await.unwrap();
    assert_eq!(synced.error, codes::NONE);
    assert_eq!(synced.assignment, b"follower");
}

#[tokio::test]
async fn test_static_member_rejoin_as_follower_with_known_member_id_and_no_protocol_change() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .protocols(&["range", "roundrobin"])
            .rebalance(10_000)
            .session(5_000)
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .build(),
    );
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, rebalanced.generation);
    assert_eq!(response.member_id, rebalanced.follower);
    assert_eq!(ctx.state(), State::Stable);
}

#[tokio::test]
async fn test_static_member_rejoin_as_follower_with_mismatched_instance_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("leader-instance-id")
            .member(&rebalanced.follower)
            .build(),
    );
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::FENCED_INSTANCE_ID
    );
}

#[tokio::test]
async fn test_static_member_rejoin_as_leader_with_mismatched_instance_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("follower-instance-id")
            .member(&rebalanced.leader)
            .build(),
    );
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::FENCED_INSTANCE_ID
    );
}

#[tokio::test]
async fn test_static_member_join_with_unknown_instance_id_and_known_member_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("unknown-instance-id")
            .member(&rebalanced.leader)
            .build(),
    );
    assert_eq!(joined.done().await.unwrap().error, codes::UNKNOWN_MEMBER_ID);
}

#[tokio::test]
async fn test_static_member_sync_as_leader_with_invalid_member_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut synced = ctx.sync(
        "invalid-member-id",
        Some("leader-instance-id"),
        rebalanced.generation,
        vec![],
    );
    assert_eq!(
        synced.done().await.unwrap().error,
        codes::FENCED_INSTANCE_ID
    );
}

#[tokio::test]
async fn test_replace_static_member_in_stable_state_errors() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    *ctx.saved.fail.lock().unwrap() = Some(codes::COORDINATOR_NOT_AVAILABLE);
    let mut joined = ctx.join(request().instance("follower-instance-id").build());
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::COORDINATOR_NOT_AVAILABLE
    );
    // The replacement was undone: the old member id still works.
    assert!(ctx.has_member(&rebalanced.follower));
    assert_eq!(
        ctx.c.heartbeat(
            "group-id",
            &rebalanced.follower,
            Some("follower-instance-id"),
            rebalanced.generation,
            ctx.t
        ),
        codes::NONE
    );
}

#[tokio::test]
async fn test_replace_static_member_in_completing_rebalance_state_triggers_rebalance() {
    let mut ctx = Ctx::new();
    let mut leader = ctx.join(request().instance("leader-instance-id").build());
    let mut follower = ctx.join(request().instance("follower-instance-id").build());
    ctx.sleep(DELAY_MS).await;
    leader.done().await.unwrap();
    follower.done().await.unwrap();
    assert_eq!(ctx.state(), State::CompletingRebalance);
    let mut replaced = ctx.join(request().instance("follower-instance-id").build());
    assert!(replaced.done().await.is_none());
    assert_eq!(ctx.state(), State::PreparingRebalance);
}

// --- sync ---------------------------------------------------------------------------

async fn sync_with(ctx: &mut Ctx, kind: Option<&str>, name: Option<&str>) -> i16 {
    let joined = ctx.join_and_complete(request().build()).await;
    let reply = ctx.c.sync(
        "group-id",
        &joined.member_id,
        None,
        joined.generation,
        kind,
        name,
        vec![],
        ctx.t,
    );
    Pending::from(reply).done().await.unwrap().error
}

#[tokio::test]
async fn test_sync_group_returns_an_error_when_protocol_type_is_inconsistent() {
    let mut ctx = Ctx::new();
    assert_eq!(
        sync_with(&mut ctx, Some("protocol-type"), Some("range")).await,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
}

#[tokio::test]
async fn test_sync_group_returns_an_error_when_protocol_name_is_inconsistent() {
    let mut ctx = Ctx::new();
    assert_eq!(
        sync_with(&mut ctx, Some("consumer"), Some("protocol-name")).await,
        codes::INCONSISTENT_GROUP_PROTOCOL
    );
}

#[tokio::test]
async fn test_sync_group_succeed_when_protocol_type_and_name_are_not_provided() {
    let mut ctx = Ctx::new();
    assert_eq!(sync_with(&mut ctx, None, None).await, codes::NONE);
}

#[tokio::test]
async fn test_sync_group_succeed_when_protocol_type_and_name_are_consistent() {
    let mut ctx = Ctx::new();
    assert_eq!(
        sync_with(&mut ctx, Some("consumer"), Some("range")).await,
        codes::NONE
    );
}

#[tokio::test]
async fn test_sync_group_from_unknown_group() {
    let ctx = Ctx::new();
    let mut synced = ctx.sync("member-id", None, 1, vec![]);
    assert_eq!(synced.done().await.unwrap().error, codes::UNKNOWN_MEMBER_ID);
}

#[tokio::test]
async fn test_sync_group_from_unknown_member() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    let mut unknown = ctx.sync("unknown-member-id", None, joined.generation, vec![]);
    assert_eq!(
        unknown.done().await.unwrap().error,
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn test_sync_group_from_illegal_generation() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    let mut synced = ctx.sync(&joined.member_id, None, joined.generation + 1, vec![]);
    assert_eq!(
        synced.done().await.unwrap().error,
        codes::ILLEGAL_GENERATION
    );
}

#[tokio::test]
async fn test_sync_group_empty_assignment() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    let mut synced = ctx.sync(&joined.member_id, None, joined.generation, vec![]);
    let response = synced.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert!(response.assignment.is_empty());
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::NONE
    );
}

#[tokio::test]
async fn test_join_group_from_unchanged_follower_does_not_rebalance() {
    let mut ctx = Ctx::new();
    let (_leader, follower) = two_member_group(&mut ctx).await;
    assert_eq!(ctx.state(), State::CompletingRebalance);
    let mut again = ctx.join(
        request()
            .member(&follower.member_id)
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert_eq!(again.done().await.unwrap().generation, follower.generation);
    assert_eq!(ctx.state(), State::CompletingRebalance);
}

#[tokio::test]
async fn test_join_group_from_unchanged_leader_should_rebalance() {
    let mut ctx = Ctx::new();
    let leader = ctx.join_and_rebalance(request().build()).await;
    let mut again = ctx.join(request().member(&leader.member_id).build());
    assert_eq!(
        again.done().await.unwrap().generation,
        leader.generation + 1
    );
}

#[tokio::test]
async fn test_leader_failure_in_sync_group() {
    let mut ctx = Ctx::new();
    let (_leader, follower) = two_member_group(&mut ctx).await;
    let mut fs = ctx.sync(&follower.member_id, None, follower.generation, vec![]);
    assert!(fs.done().await.is_none());
    ctx.sleep(10_000).await;
    assert_eq!(fs.done().await.unwrap().error, codes::REBALANCE_IN_PROGRESS);
    assert_eq!(ctx.members(), 1);
    assert!(ctx.has_member(&follower.member_id));
    assert_eq!(ctx.state(), State::PreparingRebalance);
}

#[tokio::test]
async fn test_sync_group_follower_after_leader() {
    let mut ctx = Ctx::new();
    let (leader, follower) = two_member_group(&mut ctx).await;
    let assign = vec![
        (leader.member_id.clone(), b"leader".to_vec()),
        (follower.member_id.clone(), b"follower".to_vec()),
    ];
    let mut ls = ctx.sync(&leader.member_id, None, leader.generation, assign);
    assert_eq!(ls.done().await.unwrap().assignment, b"leader");
    let mut fs = ctx.sync(&follower.member_id, None, follower.generation, vec![]);
    assert_eq!(fs.done().await.unwrap().assignment, b"follower");
}

#[tokio::test]
async fn test_sync_group_leader_after_follower() {
    let mut ctx = Ctx::new();
    let (leader, follower) = two_member_group(&mut ctx).await;
    let mut fs = ctx.sync(&follower.member_id, None, follower.generation, vec![]);
    assert!(fs.done().await.is_none());
    let assign = vec![
        (leader.member_id.clone(), b"leader".to_vec()),
        (follower.member_id.clone(), b"follower".to_vec()),
    ];
    let mut ls = ctx.sync(&leader.member_id, None, leader.generation, assign);
    assert_eq!(ls.done().await.unwrap().assignment, b"leader");
    assert_eq!(fs.done().await.unwrap().assignment, b"follower");
}

// --- heartbeat and sessions ------------------------------------------------------------

#[tokio::test]
async fn test_heartbeat_unknown_group() {
    assert_eq!(
        Ctx::new().heartbeat("member-id", 1),
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn test_heartbeat_dead_group() {
    let ctx = Ctx::new();
    ctx.c.touch("group-id");
    ctx.c.kill("group-id");
    assert_eq!(
        ctx.heartbeat("member-id", 1),
        codes::COORDINATOR_NOT_AVAILABLE
    );
}

#[tokio::test]
async fn test_heartbeat_empty_group() {
    let ctx = Ctx::new();
    ctx.c.touch("group-id");
    assert_eq!(ctx.heartbeat("member-id", 0), codes::UNKNOWN_MEMBER_ID);
}

#[tokio::test]
async fn test_heartbeat_illegal_generation() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation + 1),
        codes::ILLEGAL_GENERATION
    );
}

#[tokio::test]
async fn test_valid_heartbeat() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::NONE
    );
}

#[tokio::test]
async fn test_heartbeat_during_preparing_rebalance() {
    let ctx = Ctx::new();
    let (member, _joined) = ctx.dynamic_join(request().build()).await;
    assert_eq!(ctx.state(), State::PreparingRebalance);
    assert_eq!(ctx.heartbeat(&member, 0), codes::REBALANCE_IN_PROGRESS);
}

#[tokio::test]
async fn test_heartbeat_during_completing_rebalance() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    assert_eq!(joined.generation, 1);
    assert_eq!(ctx.state(), State::CompletingRebalance);
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::NONE
    );
}

#[tokio::test]
async fn test_classic_group_member_session_timeout() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    ctx.sleep(5_001).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn test_classic_group_member_heartbeat_maintains_session() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    ctx.sleep(2_500).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::NONE
    );
    ctx.sleep(2_500).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::NONE
    );
}

#[tokio::test]
async fn test_classic_group_member_session_timeout_during_rebalance() {
    let mut ctx = Ctx::new();
    let leader = ctx.join_and_rebalance(request().build()).await;
    let (_other, mut other) = ctx
        .dynamic_join(request().rebalance(10_000).session(5_000).build())
        .await;
    assert_eq!(ctx.state(), State::PreparingRebalance);
    ctx.sleep(2_500).await;
    assert_eq!(
        ctx.heartbeat(&leader.member_id, leader.generation),
        codes::REBALANCE_IN_PROGRESS
    );
    ctx.sleep(5_001).await;
    assert_eq!(
        ctx.heartbeat(&leader.member_id, leader.generation),
        codes::UNKNOWN_MEMBER_ID
    );
    let response = other.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(ctx.members(), 1);
    assert_eq!(ctx.generation(), 2);
    assert_eq!(ctx.state(), State::CompletingRebalance);
}

/// Kafka's `joinWithNMembers`: ids first, then everyone joins; later
/// joiners extend the initial delay once.
async fn three_joined(ctx: &mut Ctx) -> Vec<JoinResponse> {
    let base = || request().rebalance(5_000).session(5_000);
    let mut ids = Vec::new();
    for _ in 0..3 {
        let mut first = ctx.join(base().required().build());
        ids.push(first.done().await.unwrap().member_id);
    }
    let mut joins: Vec<_> = ids
        .iter()
        .map(|id| ctx.join(base().member(id).build()))
        .collect();
    ctx.sleep(DELAY_MS).await;
    ctx.sleep(DELAY_MS).await;
    let mut out = Vec::new();
    for joined in &mut joins {
        out.push(joined.done().await.unwrap());
    }
    // The leader first.
    out.sort_by_key(|response| response.member_id != response.leader);
    out
}

#[tokio::test]
async fn test_rebalance_times_out_when_sync_request_is_not_received_from_followers() {
    let mut ctx = Ctx::new();
    let joined = three_joined(&mut ctx).await;
    ctx.sleep(2_500).await;
    for response in &joined {
        assert_eq!(
            ctx.heartbeat(&response.member_id, response.generation),
            codes::NONE
        );
    }
    let mut ls = ctx.sync(&joined[0].member_id, None, joined[0].generation, vec![]);
    assert_eq!(ls.done().await.unwrap().error, codes::NONE);
    assert_eq!(ctx.state(), State::Stable);
    assert_eq!(
        ctx.heartbeat(&joined[0].member_id, joined[0].generation),
        codes::NONE
    );
    ctx.sleep(2_500).await;
    assert_eq!(
        ctx.heartbeat(&joined[0].member_id, joined[0].generation),
        codes::REBALANCE_IN_PROGRESS
    );
    for response in &joined[1..] {
        assert_eq!(
            ctx.heartbeat(&response.member_id, response.generation),
            codes::UNKNOWN_MEMBER_ID
        );
    }
    assert_eq!(ctx.state(), State::PreparingRebalance);
}

#[tokio::test]
async fn test_rebalance_times_out_when_sync_request_is_not_received_from_leaders() {
    let mut ctx = Ctx::new();
    let joined = three_joined(&mut ctx).await;
    let mut followers: Vec<_> = joined[1..]
        .iter()
        .map(|response| ctx.sync(&response.member_id, None, response.generation, vec![]))
        .collect();
    ctx.sleep(2_500).await;
    for response in &joined {
        assert_eq!(
            ctx.heartbeat(&response.member_id, response.generation),
            codes::NONE
        );
    }
    ctx.sleep(2_500).await;
    for synced in &mut followers {
        assert_eq!(
            synced.done().await.unwrap().error,
            codes::REBALANCE_IN_PROGRESS
        );
    }
    assert_eq!(
        ctx.heartbeat(&joined[0].member_id, joined[0].generation),
        codes::UNKNOWN_MEMBER_ID
    );
    assert_eq!(ctx.state(), State::PreparingRebalance);
}

#[tokio::test]
async fn test_rebalance_does_not_time_out_when_all_sync_are_received() {
    let mut ctx = Ctx::new();
    let joined = three_joined(&mut ctx).await;
    let mut syncs: Vec<_> = joined
        .iter()
        .map(|response| ctx.sync(&response.member_id, None, response.generation, vec![]))
        .collect();
    for synced in &mut syncs {
        assert_eq!(synced.done().await.unwrap().error, codes::NONE);
    }
    ctx.sleep(2_500).await;
    for response in &joined {
        assert_eq!(
            ctx.heartbeat(&response.member_id, response.generation),
            codes::NONE
        );
    }
    ctx.sleep(2_500).await;
    assert_eq!(ctx.state(), State::Stable);
}

// --- leave ----------------------------------------------------------------------------

#[tokio::test]
async fn test_leave_group_unknown_group() {
    assert_eq!(
        Ctx::new().leave(&[leaving("member-id", None)]),
        Err(codes::UNKNOWN_MEMBER_ID)
    );
}

#[tokio::test]
async fn test_leave_dead_group() {
    let ctx = Ctx::new();
    ctx.c.touch("group-id");
    ctx.c.kill("group-id");
    assert_eq!(
        ctx.leave(&[leaving("member-id", None)]),
        Err(codes::COORDINATOR_NOT_AVAILABLE)
    );
}

#[tokio::test]
async fn test_leave_group_unknown_member_id_existing_group() {
    let mut ctx = Ctx::new();
    ctx.join_and_rebalance(request().build()).await;
    assert_eq!(
        ctx.leave(&[leaving("other-member-id", None)]),
        Ok(vec![codes::UNKNOWN_MEMBER_ID])
    );
}

#[tokio::test]
async fn test_valid_leave_group() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    assert_eq!(
        ctx.leave(&[leaving(&joined.member_id, None)]),
        Ok(vec![codes::NONE])
    );
    settle().await;
    assert_eq!(ctx.state(), State::Empty);
}

#[tokio::test]
async fn test_leave_group_with_fenced_instance_id() {
    let mut ctx = Ctx::new();
    ctx.join_and_complete(request().instance("group-instance-id").build())
        .await;
    assert_eq!(
        ctx.leave(&[leaving("other-member-id", Some("group-instance-id"))]),
        Ok(vec![codes::FENCED_INSTANCE_ID])
    );
}

#[tokio::test]
async fn test_leave_group_static_member_with_unknown_member_id() {
    let mut ctx = Ctx::new();
    ctx.join_and_complete(request().instance("group-instance-id").build())
        .await;
    assert_eq!(
        ctx.leave(&[leaving("", Some("group-instance-id"))]),
        Ok(vec![codes::NONE])
    );
}

#[tokio::test]
async fn test_static_members_unknown_instance_batch_leave_group() {
    let mut ctx = Ctx::new();
    ctx.static_rebalance().await;
    assert_eq!(
        ctx.leave(&[
            leaving("", Some("unknown-instance-id")),
            leaving("", Some("leader-instance-id"))
        ]),
        Ok(vec![codes::UNKNOWN_MEMBER_ID, codes::NONE])
    );
}

#[tokio::test]
async fn test_static_members_fenced_instance_batch_leave_group() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    assert_eq!(
        ctx.leave(&[
            leaving("invalid-member-id", Some("leader-instance-id")),
            leaving(&rebalanced.follower, Some("follower-instance-id"))
        ]),
        Ok(vec![codes::FENCED_INSTANCE_ID, codes::NONE])
    );
}

#[tokio::test]
async fn test_static_members_valid_batch_leave_group() {
    let mut ctx = Ctx::new();
    ctx.static_rebalance().await;
    assert_eq!(
        ctx.leave(&[
            leaving("", Some("leader-instance-id")),
            leaving("", Some("follower-instance-id"))
        ]),
        Ok(vec![codes::NONE, codes::NONE])
    );
    settle().await;
    assert_eq!(ctx.state(), State::Empty);
}

#[tokio::test]
async fn test_pending_members_leave_group() {
    let mut ctx = Ctx::new();
    two_member_group(&mut ctx).await;
    let mut required = ctx.join(
        request()
            .required()
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    let pending = required.done().await.unwrap().member_id;
    assert_eq!(ctx.pending(), 1);
    assert_eq!(ctx.leave(&[leaving(&pending, None)]), Ok(vec![codes::NONE]));
    assert_eq!(ctx.pending(), 0);
    assert_eq!(ctx.state(), State::CompletingRebalance);
    assert_eq!(ctx.members(), 2);
}

#[tokio::test]
async fn test_joined_member_pending_member_batch_leave_group_with_unknown_member() {
    let mut ctx = Ctx::new();
    let (leader, follower) = two_member_group(&mut ctx).await;
    let mut required = ctx.join(
        request()
            .required()
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    let pending = required.done().await.unwrap().member_id;
    assert_eq!(
        ctx.leave(&[
            leaving(&leader.member_id, None),
            leaving(&follower.member_id, None),
            leaving(&pending, None),
            leaving("unknown-member-id", None)
        ]),
        Ok(vec![
            codes::NONE,
            codes::NONE,
            codes::NONE,
            codes::UNKNOWN_MEMBER_ID
        ])
    );
}

// --- list, describe ---------------------------------------------------------------------

#[tokio::test]
async fn test_list_groups() {
    let mut ctx = Ctx::new();
    ctx.join_and_rebalance(request().build()).await;
    ctx.c.touch("simple-group");
    let listed = ctx.c.list();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        listed[0],
        ("group-id".into(), State::Stable, "consumer".into())
    );
    assert_eq!(
        listed[1],
        ("simple-group".into(), State::Empty, String::new())
    );
}

#[tokio::test]
async fn test_describe_group_stable() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    let mut synced = ctx.sync(
        &joined.member_id,
        None,
        joined.generation,
        vec![(joined.member_id.clone(), b"a".to_vec())],
    );
    assert_eq!(synced.done().await.unwrap().error, 0);
    let described = ctx.c.describe("group-id").unwrap();
    assert_eq!(described.state, State::Stable);
    assert_eq!(described.protocol, "range");
    assert_eq!(described.members[0].assignment, b"a");
}

#[tokio::test]
async fn test_describe_group_rebalancing() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_complete(request().build()).await;
    let described = ctx.c.describe("group-id").unwrap();
    assert_eq!(described.state, State::CompletingRebalance);
    assert_eq!(described.protocol, "");
    assert!(described.members[0].metadata.is_empty());
    assert_eq!(described.members[0].member_id, joined.member_id);
}

#[tokio::test]
async fn test_describe_groups_group_id_not_found_exception() {
    assert!(Ctx::new().c.describe("group-id").is_none());
}

// --- Camus: failover and storage --------------------------------------------------------

#[tokio::test]
async fn a_new_coordinator_keeps_the_stored_generation() {
    let mut first = Ctx::new();
    let (leader, follower) = two_member_group(&mut first).await;
    let assign = vec![
        (leader.member_id.clone(), b"p0".to_vec()),
        (follower.member_id.clone(), b"p1".to_vec()),
    ];
    let mut ls = first.sync(&leader.member_id, None, leader.generation, assign);
    let mut fs = first.sync(&follower.member_id, None, follower.generation, vec![]);
    assert_eq!(ls.done().await.unwrap().error, 0);
    assert_eq!(fs.done().await.unwrap().error, 0);
    let (group, record) = first.saved.records.lock().unwrap().last().unwrap().clone();
    let second = Ctx::new();
    second.c.load(vec![(group, record)], second.t);
    assert_eq!(
        second.heartbeat(&leader.member_id, leader.generation),
        codes::NONE
    );
    assert_eq!(
        second.heartbeat(&follower.member_id, follower.generation),
        codes::NONE
    );
    assert_eq!(
        second
            .c
            .check_commit("group-id", &follower.member_id, None, follower.generation),
        codes::NONE
    );
    assert_eq!(second.c.describe("group-id").unwrap().state, State::Stable);
    first.c.unload();
    assert_eq!(
        first.heartbeat(&leader.member_id, leader.generation),
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn a_failed_save_answers_the_joiners_with_its_code() {
    let mut ctx = Ctx::new();
    *ctx.saved.fail.lock().unwrap() = Some(codes::NOT_COORDINATOR);
    let (_, mut joined) = ctx.dynamic_join(request().build()).await;
    ctx.sleep(DELAY_MS).await;
    assert_eq!(joined.done().await.unwrap().error, codes::NOT_COORDINATOR);
}

#[tokio::test]
async fn joins_are_checked_before_anything_changes() {
    let ctx = Ctx::with(Config::default());
    let mut short = ctx.join(request().session(100).build());
    assert_eq!(
        short.done().await.unwrap().error,
        codes::INVALID_SESSION_TIMEOUT
    );
    let mut nameless = request().build();
    nameless.group = String::new();
    let mut nameless = ctx.join(nameless);
    assert_eq!(
        nameless.done().await.unwrap().error,
        codes::INVALID_GROUP_ID
    );
}

// --- more GroupMetadataManagerTest cases --------------------------------------------

#[tokio::test]
async fn test_join_group_should_receive_error_if_group_over_max_size() {
    let ctx = Ctx::with(Config {
        max_size: 1,
        initial_rebalance_delay: DELAY,
        min_session_timeout: Duration::from_millis(10),
        ..Config::default()
    });
    let mut first = ctx.join(request().build());
    assert!(first.done().await.is_none());
    let mut second = ctx.join(request().build());
    assert_eq!(
        second.done().await.unwrap().error,
        codes::GROUP_MAX_SIZE_REACHED
    );
}

#[tokio::test]
async fn test_dynamic_members_join_group_with_max_size_and_not_required_known_member() {
    let mut ctx = small_group();
    let mut joins: Vec<_> = (0..11).map(|_| ctx.join(request().build())).collect();
    // Later joiners extend the initial delay once.
    ctx.sleep(50).await;
    ctx.sleep(50).await;
    let mut full = 0;
    let mut ok = 0;
    for joined in &mut joins {
        match joined.done().await.map(|response| response.error) {
            Some(codes::GROUP_MAX_SIZE_REACHED) => full += 1,
            Some(codes::NONE) => ok += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!((ok, full), (10, 1));
    assert_eq!(ctx.members(), 10);
}

#[tokio::test]
async fn test_static_member_rejoin_with_explicit_unknown_member_id() {
    let mut ctx = Ctx::new();
    ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("leader-instance-id")
            .member("unknown-member-id")
            .build(),
    );
    assert_eq!(
        joined.done().await.unwrap().error,
        codes::FENCED_INSTANCE_ID
    );
}

#[tokio::test]
async fn test_join_group_replace_static_member() {
    let mut ctx = Ctx::new();
    let mut first = ctx.join(request().instance("group-instance-id").build());
    assert!(first.done().await.is_none());
    // The same instance joins again without a member id: the first join is
    // fenced and the second one takes its place.
    let mut second = ctx.join(request().instance("group-instance-id").build());
    assert_eq!(first.done().await.unwrap().error, codes::FENCED_INSTANCE_ID);
    ctx.sleep(DELAY_MS).await;
    assert_eq!(second.done().await.unwrap().error, codes::NONE);
    assert_eq!(ctx.members(), 1);
}

#[tokio::test]
async fn test_join_group_as_existing_member_with_updated_metadata_triggers_rebalance_in_stable_state(
) {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .protocols(&["roundrobin", "range"])
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert!(joined.done().await.is_none());
    assert_eq!(ctx.state(), State::PreparingRebalance);
}

#[tokio::test]
async fn test_static_member_fence_duplicate_rejoined_follower() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let base = || {
        request()
            .protocols(&["range", "roundrobin"])
            .rebalance(10_000)
            .session(5_000)
    };
    // A new member starts a rebalance; the follower rejoins with its id.
    let _new = ctx.join(base().build());
    assert_eq!(ctx.state(), State::PreparingRebalance);
    let mut old = ctx.join(
        base()
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .build(),
    );
    assert!(old.done().await.is_none());
    // The follower instance rejoins without an id: the old join is fenced.
    let _replacement = ctx.join(base().instance("follower-instance-id").build());
    assert_eq!(old.done().await.unwrap().error, codes::FENCED_INSTANCE_ID);
}

#[tokio::test]
async fn test_static_member_fence_duplicate_syncing_follower_after_member_id_changed() {
    let mut ctx = Ctx::new();
    let mut leader = ctx.join(
        request()
            .instance("leader-instance-id")
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    let mut follower = ctx.join(
        request()
            .instance("follower-instance-id")
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    ctx.sleep(DELAY_MS).await;
    ctx.sleep(DELAY_MS).await;
    let leader = leader.done().await.unwrap();
    let follower = follower.done().await.unwrap();
    assert_eq!((leader.error, follower.error), (codes::NONE, codes::NONE));
    let mut syncing = ctx.sync(
        &follower.member_id,
        Some("follower-instance-id"),
        follower.generation,
        vec![],
    );
    assert!(syncing.done().await.is_none());
    // The instance rejoins without an id while its old id waits on sync.
    let _replacement = ctx.join(
        request()
            .instance("follower-instance-id")
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert_eq!(
        syncing.done().await.unwrap().error,
        codes::FENCED_INSTANCE_ID
    );
}

#[tokio::test]
async fn test_static_member_rejoin_with_known_member_id() {
    let mut ctx = Ctx::new();
    let mut joined = ctx.join(request().instance("group-instance-id").build());
    ctx.sleep(DELAY_MS).await;
    let first = joined.done().await.unwrap();
    assert_eq!(first.error, codes::NONE);
    // Rejoining with the known id during CompletingRebalance and the same
    // protocols returns the current generation.
    let mut again = ctx.join(
        request()
            .instance("group-instance-id")
            .member(&first.member_id)
            .build(),
    );
    let again = again.done().await.unwrap();
    assert_eq!(again.error, codes::NONE);
    assert_eq!(again.generation, first.generation);
    let mut synced = ctx.sync(
        &first.member_id,
        Some("group-instance-id"),
        first.generation,
        vec![],
    );
    assert_eq!(synced.done().await.unwrap().error, codes::NONE);
}

#[tokio::test]
async fn test_static_member_rejoin_with_unknown_member_id_and_change_of_protocol_with_selected_protocol_changed(
) {
    let mut ctx = Ctx::new();
    ctx.static_rebalance().await;
    // The leader's new protocols would select a different protocol.
    let mut joined = ctx.join(
        request()
            .instance("leader-instance-id")
            .protocols(&["roundrobin"])
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert!(joined.done().await.is_none());
    assert_eq!(ctx.state(), State::PreparingRebalance);
}

#[tokio::test]
async fn test_static_member_follower_fail_to_rejoin_before_rebalance_timeout() {
    // Adapted: Kafka's mock timer, advanced 10 s at once, completes the join
    // phase before the member's 5 s session expiry fires. In real time the
    // session expires first, so these use a session longer than the
    // rebalance timeout to test what Kafka's case means to test.
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance_with(30_000).await;
    let base = || {
        request()
            .protocols(&["range", "roundrobin"])
            .rebalance(10_000)
            .session(30_000)
    };
    let mut newcomer = ctx.join(base().instance("new-member-instance-id").build());
    assert!(newcomer.done().await.is_none());
    assert_eq!(ctx.state(), State::PreparingRebalance);
    let mut leader = ctx.join(
        base()
            .instance("leader-instance-id")
            .member(&rebalanced.leader)
            .build(),
    );
    assert!(leader.done().await.is_none());
    ctx.sleep(10_000).await;
    let leader = leader.done().await.unwrap();
    let newcomer = newcomer.done().await.unwrap();
    assert_eq!((leader.error, newcomer.error), (codes::NONE, codes::NONE));
    assert_eq!(leader.generation, rebalanced.generation + 1);
    // The static follower that did not rejoin is still a member.
    assert_eq!(ctx.members(), 3);
    assert!(ctx.has_member(&rebalanced.follower));
}

#[tokio::test]
async fn test_static_member_leader_fail_to_rejoin_before_rebalance_timeout() {
    // Adapted: Kafka's mock timer, advanced 10 s at once, completes the join
    // phase before the member's 5 s session expiry fires. In real time the
    // session expires first, so these use a session longer than the
    // rebalance timeout to test what Kafka's case means to test.
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance_with(30_000).await;
    let base = || {
        request()
            .protocols(&["range", "roundrobin"])
            .rebalance(10_000)
            .session(30_000)
    };
    let mut follower = ctx.join(
        base()
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .protocols(&["roundrobin"])
            .build(),
    );
    assert!(follower.done().await.is_none());
    ctx.sleep(10_000).await;
    let follower = follower.done().await.unwrap();
    assert_eq!(follower.error, codes::NONE);
    // The leader did not rejoin: a member that did leads.
    assert_eq!(follower.leader, rebalanced.follower);
    assert!(ctx.has_member(&rebalanced.leader));
}

#[tokio::test]
async fn test_new_member_timeout_completion() {
    let mut ctx = Ctx::new();
    let long = (Config::default().new_member_join_timeout.as_millis() + 5_000) as i32;
    let mut joined = ctx.join(request().session(long).build());
    assert!(joined.done().await.is_none());
    ctx.sleep(DELAY_MS).await;
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, 1);
    assert_eq!(ctx.state(), State::CompletingRebalance);
    let mut synced = ctx.sync(&response.member_id, None, 1, vec![]);
    assert_eq!(synced.done().await.unwrap().error, codes::NONE);
    // No longer new: the member's own session applies, not the join timeout.
    ctx.sleep(Config::default().new_member_join_timeout.as_millis() as u64)
        .await;
    assert_eq!(ctx.members(), 1);
    ctx.sleep(5_001).await;
    assert_eq!(ctx.members(), 0);
    assert_eq!(ctx.state(), State::Empty);
}

#[tokio::test]
async fn test_new_member_failure_after_join_group_completion() {
    let mut ctx = Ctx::new();
    let base = || request().session(5_000).rebalance(10_000);
    let first = ctx.join_and_rebalance(base().build()).await;
    assert_eq!(ctx.state(), State::Stable);
    assert_eq!(first.generation, 1);
    let mut other = ctx.join(base().build());
    let mut again = ctx.join(base().member(&first.member_id).build());
    assert_eq!(again.done().await.unwrap().error, codes::NONE);
    assert_eq!(other.done().await.unwrap().error, codes::NONE);
}

#[tokio::test]
async fn test_static_member_heartbeat_leader_with_invalid_member_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    assert_eq!(
        ctx.c.heartbeat(
            "group-id",
            &rebalanced.leader,
            Some("leader-instance-id"),
            rebalanced.generation,
            ctx.t
        ),
        codes::NONE
    );
    assert_eq!(
        ctx.c.heartbeat(
            "group-id",
            "invalid-member-id",
            Some("leader-instance-id"),
            rebalanced.generation,
            ctx.t
        ),
        codes::FENCED_INSTANCE_ID
    );
}

#[tokio::test]
async fn test_heartbeat_unknown_member_existing_group() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    assert_eq!(
        ctx.heartbeat("unknown-member-id", joined.generation),
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn test_heartbeat_during_rebalance_causes_rebalance_in_progress() {
    let mut ctx = Ctx::new();
    let joined = ctx.join_and_rebalance(request().build()).await;
    let _new = ctx.join(request().build());
    assert_eq!(ctx.state(), State::PreparingRebalance);
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::REBALANCE_IN_PROGRESS
    );
}

#[tokio::test]
async fn test_rebalance_times_out_when_sync_request_is_not_received() {
    let mut ctx = Ctx::new();
    let joined = ctx
        .join_and_complete(request().rebalance(5_000).session(5_000).build())
        .await;
    ctx.sleep(2_500).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::NONE
    );
    ctx.sleep(2_500).await;
    assert_eq!(
        ctx.heartbeat(&joined.member_id, joined.generation),
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn test_leave_group_invalid_group() {
    assert_eq!(
        Ctx::new()
            .c
            .leave("", &[leaving("member-id", None)], Instant::now()),
        Err(codes::UNKNOWN_MEMBER_ID)
    );
}

#[tokio::test]
async fn test_static_members_leave_unknown_group() {
    assert_eq!(
        Ctx::new().leave(&[
            leaving("", Some("leader-instance-id")),
            leaving("", Some("follower-instance-id"))
        ]),
        Err(codes::UNKNOWN_MEMBER_ID)
    );
}

#[tokio::test]
async fn test_static_member_rejoin_with_leader_id_and_known_member_id() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("leader-instance-id")
            .member(&rebalanced.leader)
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert!(joined.done().await.is_none());
    ctx.sleep(10_000).await;
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, rebalanced.generation + 1);
    assert_eq!(response.leader, rebalanced.leader);
    // The follower's session expired while the leader rejoined.
    assert!(!ctx.has_member(&rebalanced.follower));
    assert_eq!(response.members.len(), 1);
    assert_eq!(ctx.state(), State::CompletingRebalance);
}

#[tokio::test]
async fn test_static_member_rejoin_with_follower_id_and_change_of_protocol() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let mut joined = ctx.join(
        request()
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .protocols(&["roundrobin"])
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    assert!(joined.done().await.is_none());
    ctx.sleep(10_000).await;
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(
        response.leader, rebalanced.follower,
        "the follower now leads"
    );
    assert_eq!(response.protocol_name.as_deref(), Some("roundrobin"));
}

#[tokio::test]
async fn test_static_member_rejoin_with_unknown_member_id_and_change_of_protocol_while_select_protocol_unchanged(
) {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let selected = ctx.c.groups.lock().unwrap()["group-id"]
        .protocol
        .clone()
        .unwrap();
    let mut joined = ctx.join(
        request()
            .instance("follower-instance-id")
            .protocols(&[selected.as_str()])
            .rebalance(10_000)
            .session(5_000)
            .build(),
    );
    let response = joined.done().await.unwrap();
    assert_eq!(response.error, codes::NONE);
    assert_eq!(response.generation, rebalanced.generation);
    let new_id = response.member_id;
    assert_ne!(new_id, rebalanced.follower);
    let mut old = ctx.join(
        request()
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .build(),
    );
    assert_eq!(old.done().await.unwrap().error, codes::FENCED_INSTANCE_ID);
    let mut old_sync = ctx.sync(
        &rebalanced.follower,
        Some("follower-instance-id"),
        rebalanced.generation,
        vec![],
    );
    assert_eq!(
        old_sync.done().await.unwrap().error,
        codes::FENCED_INSTANCE_ID
    );
    let mut new_sync = ctx.sync(
        &new_id,
        Some("follower-instance-id"),
        rebalanced.generation,
        vec![],
    );
    let synced = new_sync.done().await.unwrap();
    assert_eq!(synced.error, codes::NONE);
    assert_eq!(synced.assignment, b"follower");
}

#[tokio::test]
async fn test_static_member_fence_duplicate_rejoining_follower_after_member_id_changed() {
    let mut ctx = Ctx::new();
    let rebalanced = ctx.static_rebalance().await;
    let base = || request().rebalance(10_000).session(5_000);
    let mut leader = ctx.join(
        base()
            .instance("leader-instance-id")
            .member(&rebalanced.leader)
            .build(),
    );
    assert!(leader.done().await.is_none());
    assert_eq!(ctx.state(), State::PreparingRebalance);
    // The follower instance rejoins without an id: everyone has joined.
    let mut duplicate = ctx.join(base().instance("follower-instance-id").build());
    let duplicate = duplicate.done().await.unwrap();
    assert_eq!(duplicate.error, codes::NONE);
    let leader = leader.done().await.unwrap();
    assert_eq!(leader.error, codes::NONE);
    assert_eq!(leader.member_id, rebalanced.leader);
    // The old follower id is fenced.
    let mut old = ctx.join(
        base()
            .instance("follower-instance-id")
            .member(&rebalanced.follower)
            .build(),
    );
    assert_eq!(old.done().await.unwrap().error, codes::FENCED_INSTANCE_ID);
}
