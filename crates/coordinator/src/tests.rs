use super::*;

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

fn coordinator() -> (Arc<Coordinator>, Arc<Saved>) {
    let saved = Arc::new(Saved::default());
    let config = Config {
        initial_rebalance_delay: Duration::from_millis(50),
        ..Config::default()
    };
    (Coordinator::new(saved.clone(), config), saved)
}

fn request(member_id: &str, client: &str) -> JoinRequest {
    JoinRequest {
        group: "g".into(),
        member_id: member_id.into(),
        client_id: client.into(),
        client_host: "/127.0.0.1".into(),
        protocol_type: "consumer".into(),
        protocols: vec![("range".into(), client.as_bytes().to_vec())],
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 2_000,
        require_known_member_id: true,
        ..Default::default()
    }
}

/// First JoinGroup gets MEMBER_ID_REQUIRED and an id; return the id.
fn member_id(coordinator: &Arc<Coordinator>, client: &str) -> String {
    match coordinator.join(request("", client), Instant::now()) {
        Reply::Now(response) => {
            assert_eq!(response.error, codes::MEMBER_ID_REQUIRED);
            assert!(response.member_id.starts_with(client));
            response.member_id
        }
        Reply::Later(_) => panic!("first join should ask for a member id"),
    }
}

async fn tick_until<T>(coordinator: &Arc<Coordinator>, reply: Reply<T>) -> T {
    match reply {
        Reply::Now(value) => value,
        Reply::Later(mut rx) => loop {
            coordinator.tick(Instant::now());
            tokio::select! {
                value = &mut rx => return value.expect("answered"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        },
    }
}

async fn form_group(coordinator: &Arc<Coordinator>) -> (JoinResponse, JoinResponse) {
    let a = member_id(coordinator, "a");
    let b = member_id(coordinator, "b");
    let ja = coordinator.join(request(&a, "a"), Instant::now());
    let jb = coordinator.join(request(&b, "b"), Instant::now());
    let (ra, rb) = tokio::join!(tick_until(coordinator, ja), tick_until(coordinator, jb));
    (ra, rb)
}

#[tokio::test]
async fn two_members_join_sync_and_heartbeat() {
    let (coordinator, saved) = coordinator();
    let (ra, rb) = form_group(&coordinator).await;
    assert_eq!((ra.error, rb.error), (0, 0));
    assert_eq!(ra.generation, 1);
    assert_eq!(ra.generation, rb.generation);
    assert_eq!(ra.protocol_name.as_deref(), Some("range"));
    let (leader, follower) = if ra.leader == ra.member_id {
        (ra, rb)
    } else {
        (rb, ra)
    };
    assert_eq!(leader.members.len(), 2, "the leader sees every member");
    assert!(follower.members.is_empty());

    let fs = coordinator.sync(
        "g",
        &follower.member_id,
        1,
        None,
        None,
        vec![],
        Instant::now(),
    );
    let assignments = vec![
        (leader.member_id.clone(), b"p0".to_vec()),
        (follower.member_id.clone(), b"p1".to_vec()),
    ];
    let ls = coordinator.sync(
        "g",
        &leader.member_id,
        1,
        Some("consumer"),
        Some("range"),
        assignments,
        Instant::now(),
    );
    let (fs, ls) = tokio::join!(tick_until(&coordinator, fs), tick_until(&coordinator, ls));
    assert_eq!((fs.error, fs.assignment.as_slice()), (0, b"p1".as_slice()));
    assert_eq!((ls.error, ls.assignment.as_slice()), (0, b"p0".as_slice()));
    assert_eq!(
        coordinator.heartbeat("g", &follower.member_id, 1, Instant::now()),
        0
    );
    assert_eq!(
        coordinator.heartbeat("g", &follower.member_id, 0, Instant::now()),
        codes::ILLEGAL_GENERATION
    );
    assert_eq!(
        coordinator.heartbeat("g", "nobody", 1, Instant::now()),
        codes::UNKNOWN_MEMBER_ID
    );
    assert_eq!(coordinator.check_commit("g", &leader.member_id, 1), 0);

    // Generation 1 was stored at join, then again with its assignment.
    let records = saved.records.lock().unwrap();
    let last = &records.last().unwrap().1;
    assert_eq!(last.generation, 1);
    assert!(last
        .members
        .iter()
        .all(|member| !member.assignment.is_empty()));
}

async fn stable(coordinator: &Arc<Coordinator>) -> (String, String) {
    let (ra, rb) = form_group(coordinator).await;
    let (leader, follower) = if ra.leader == ra.member_id {
        (ra, rb)
    } else {
        (rb, ra)
    };
    let fs = coordinator.sync(
        "g",
        &follower.member_id,
        1,
        None,
        None,
        vec![],
        Instant::now(),
    );
    let assignments = vec![
        (leader.member_id.clone(), b"p0".to_vec()),
        (follower.member_id.clone(), b"p1".to_vec()),
    ];
    let ls = coordinator.sync(
        "g",
        &leader.member_id,
        1,
        None,
        None,
        assignments,
        Instant::now(),
    );
    let _ = tokio::join!(tick_until(coordinator, fs), tick_until(coordinator, ls));
    (leader.member_id, follower.member_id)
}

#[tokio::test]
async fn a_new_member_starts_a_rebalance_and_everyone_rejoins() {
    let (coordinator, _) = coordinator();
    let (leader, follower) = stable(&coordinator).await;
    let c = member_id(&coordinator, "c");
    let jc = coordinator.join(request(&c, "c"), Instant::now());
    assert_eq!(
        coordinator.heartbeat("g", &follower, 1, Instant::now()),
        codes::REBALANCE_IN_PROGRESS
    );
    let jl = coordinator.join(request(&leader, "a"), Instant::now());
    let jf = coordinator.join(request(&follower, "b"), Instant::now());
    let (rc, rl, rf) = tokio::join!(
        tick_until(&coordinator, jc),
        tick_until(&coordinator, jl),
        tick_until(&coordinator, jf)
    );
    assert_eq!((rc.generation, rl.generation, rf.generation), (2, 2, 2));
    assert_eq!(rl.leader, leader, "the leader keeps its role");
    assert_eq!(rl.members.len(), 3);
}

#[tokio::test]
async fn a_silent_member_is_expired_and_the_rest_rebalance() {
    let (coordinator, _) = coordinator();
    let (leader, follower) = stable(&coordinator).await;
    // The follower stops heartbeating; the leader keeps going.
    let later = Instant::now() + Duration::from_millis(10_500);
    assert_eq!(coordinator.heartbeat("g", &leader, 1, later), 0);
    coordinator.tick(later + Duration::from_millis(1));
    assert_eq!(
        coordinator.heartbeat("g", &follower, 1, later),
        codes::UNKNOWN_MEMBER_ID
    );
    assert_eq!(
        coordinator.heartbeat("g", &leader, 1, later),
        codes::REBALANCE_IN_PROGRESS
    );
    let rejoin = coordinator.join(request(&leader, "a"), later);
    let response = tick_until(&coordinator, rejoin).await;
    assert_eq!(response.generation, 2);
    assert_eq!(response.members.len(), 1);
}

#[tokio::test]
async fn a_new_coordinator_keeps_the_stored_generation() {
    let (first, saved) = coordinator();
    let (leader, follower) = stable(&first).await;
    let records = saved.records.lock().unwrap().clone();
    let (group, record) = records.last().unwrap().clone();

    // Failover: another broker loads what the metadata group stored.
    let (second, _) = coordinator();
    second.load(vec![(group, record)], Instant::now());
    assert_eq!(second.heartbeat("g", &leader, 1, Instant::now()), 0);
    assert_eq!(second.heartbeat("g", &follower, 1, Instant::now()), 0);
    assert_eq!(second.check_commit("g", &follower, 1), 0);
    let described = second.describe("g").unwrap();
    assert_eq!(described.state, State::Stable);
    assert_eq!(described.members.len(), 2);

    // The old coordinator, unloaded, answers waiting members NOT_COORDINATOR.
    first.unload();
    assert_eq!(
        first.heartbeat("g", &leader, 1, Instant::now()),
        codes::UNKNOWN_MEMBER_ID
    );
}

#[tokio::test]
async fn a_failed_save_answers_the_joiners_with_its_code() {
    let (coordinator, saved) = coordinator();
    *saved.fail.lock().unwrap() = Some(codes::NOT_COORDINATOR);
    let a = member_id(&coordinator, "a");
    let join = coordinator.join(request(&a, "a"), Instant::now());
    let response = tick_until(&coordinator, join).await;
    assert_eq!(response.error, codes::NOT_COORDINATOR);
}

#[tokio::test]
async fn joins_are_checked_before_anything_changes() {
    let (coordinator, _) = coordinator();
    let mut bad = request("", "a");
    bad.session_timeout_ms = 100;
    assert!(matches!(
        coordinator.join(bad, Instant::now()),
        Reply::Now(JoinResponse {
            error: codes::INVALID_SESSION_TIMEOUT,
            ..
        })
    ));
    assert!(matches!(
        coordinator.join(request("made-up", "a"), Instant::now()),
        Reply::Now(JoinResponse {
            error: codes::UNKNOWN_MEMBER_ID,
            ..
        })
    ));
    let mut nameless = request("", "a");
    nameless.group = String::new();
    assert!(matches!(
        coordinator.join(nameless, Instant::now()),
        Reply::Now(JoinResponse {
            error: codes::INVALID_GROUP_ID,
            ..
        })
    ));
}
