//! Phase 3: a coordinator failover does not split-brain a group (ADR 0013).
//!
//! Three brokers. Two members form a group and commit. The coordinator (the
//! metadata leader) is killed. The new coordinator keeps the stored
//! generation, so heartbeats and commits from it still work and the
//! committed offset is still there. After a rebalance, a commit carrying the
//! old generation is rejected; the old coordinator, back as a follower,
//! refuses group requests. Raw protocol, so every step is observable.

mod support;

use std::net::TcpStream;
use std::time::{Duration, Instant};

use bytes::Bytes;
use camus_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use camus_protocol::messages::leave_group_request::MemberIdentity;
use camus_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use camus_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use camus_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use camus_protocol::messages::{
    ApiKey, DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, SyncGroupRequest,
    SyncGroupResponse, TopicName,
};
use camus_protocol::protocol::StrBytes;
use support::*;

const GROUP: &str = "failover";

fn group() -> GroupId {
    GroupId(StrBytes::from_static_str(GROUP))
}

fn coordinator_of(via: &str) -> (i32, String) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "no coordinator");
        if let Ok(mut stream) = TcpStream::connect(via) {
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            send(
                &mut stream,
                ApiKey::FindCoordinator,
                3,
                &FindCoordinatorRequest::default().with_key(StrBytes::from_static_str(GROUP)),
            );
            let found = recv::<FindCoordinatorResponse>(&mut stream, 3);
            let addr = format!("{}:{}", found.host.as_str(), found.port);
            if found.error_code == 0 && TcpStream::connect(&addr).is_ok() {
                return (found.node_id.0, addr);
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn join(addr: &str, member_id: &str) -> JoinGroupResponse {
    let mut stream = connect(addr);
    let protocol = JoinGroupRequestProtocol::default()
        .with_name(StrBytes::from_static_str("range"))
        .with_metadata(Bytes::from_static(b"sub"));
    let request = JoinGroupRequest::default()
        .with_group_id(group())
        .with_session_timeout_ms(6_000)
        .with_rebalance_timeout_ms(6_000)
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![protocol]);
    send(&mut stream, ApiKey::JoinGroup, 5, &request);
    recv::<JoinGroupResponse>(&mut stream, 5)
}

/// First join returns MEMBER_ID_REQUIRED and an id; return it.
fn member_id(addr: &str) -> String {
    let first = join(addr, "");
    assert_eq!(first.error_code, 79, "{first:?}");
    first.member_id.to_string()
}

fn sync(
    addr: &str,
    member: &str,
    generation: i32,
    assign: Vec<(String, &'static [u8])>,
) -> SyncGroupResponse {
    let mut stream = connect(addr);
    let request = SyncGroupRequest::default()
        .with_group_id(group())
        .with_generation_id(generation)
        .with_member_id(StrBytes::from_string(member.to_string()))
        .with_assignments(
            assign
                .into_iter()
                .map(|(id, bytes)| {
                    SyncGroupRequestAssignment::default()
                        .with_member_id(StrBytes::from_string(id))
                        .with_assignment(Bytes::from_static(bytes))
                })
                .collect(),
        );
    send(&mut stream, ApiKey::SyncGroup, 3, &request);
    recv::<SyncGroupResponse>(&mut stream, 3)
}

fn heartbeat(addr: &str, member: &str, generation: i32) -> i16 {
    let mut stream = connect(addr);
    let request = HeartbeatRequest::default()
        .with_group_id(group())
        .with_generation_id(generation)
        .with_member_id(StrBytes::from_string(member.to_string()));
    send(&mut stream, ApiKey::Heartbeat, 3, &request);
    recv::<HeartbeatResponse>(&mut stream, 3).error_code
}

fn commit(addr: &str, member: &str, generation: i32, offset: i64) -> i16 {
    let mut stream = connect(addr);
    let partition = OffsetCommitRequestPartition::default()
        .with_partition_index(0)
        .with_committed_offset(offset);
    let topic = OffsetCommitRequestTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
        .with_partitions(vec![partition]);
    let request = OffsetCommitRequest::default()
        .with_group_id(group())
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(StrBytes::from_string(member.to_string()))
        .with_topics(vec![topic]);
    send(&mut stream, ApiKey::OffsetCommit, 8, &request);
    recv::<OffsetCommitResponse>(&mut stream, 8).topics[0].partitions[0].error_code
}

fn fetched(addr: &str) -> i64 {
    let mut stream = connect(addr);
    let topic = OffsetFetchRequestTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
        .with_partition_indexes(vec![0]);
    let request = OffsetFetchRequest::default()
        .with_group_id(group())
        .with_topics(Some(vec![topic]));
    send(&mut stream, ApiKey::OffsetFetch, 7, &request);
    let response = recv::<OffsetFetchResponse>(&mut stream, 7);
    assert_eq!(response.error_code, 0, "{response:?}");
    response.topics[0].partitions[0].committed_offset
}

fn leave(addr: &str, member: &str) -> i16 {
    let mut stream = connect(addr);
    let request = LeaveGroupRequest::default()
        .with_group_id(group())
        .with_members(vec![
            MemberIdentity::default().with_member_id(StrBytes::from_string(member.to_string()))
        ]);
    send(&mut stream, ApiKey::LeaveGroup, 3, &request);
    let response = recv::<LeaveGroupResponse>(&mut stream, 3);
    response.members[0].error_code
}

#[test]
fn coordinator_failover_keeps_the_generation_and_fences_the_old_one() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping; set CAMUS_MINIO=1");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let opts = Opts::default()
        .env("CAMUS_GROUP_INITIAL_DELAY_MS", "200")
        .env("CAMUS_GROUP_MIN_SESSION_MS", "1000");
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| Node::with(id, &root.path().join(format!("node-{id}")), opts.clone()))
        .collect();
    init(&nodes, &[1, 2, 3]);
    let meta_leader = wait_settled(&nodes.iter().collect::<Vec<_>>());
    create_wal_topic(&kafka_addr(
        nodes.iter().find(|node| node.id == meta_leader).unwrap(),
    ));

    let (old_id, old) = coordinator_of(&kafka_addr(&nodes[0]));
    let a = member_id(&old);
    let b = member_id(&old);
    let (ja, jb) = std::thread::scope(|scope| {
        let ja = scope.spawn(|| join(&old, &a));
        let jb = scope.spawn(|| join(&old, &b));
        (ja.join().unwrap(), jb.join().unwrap())
    });
    assert_eq!((ja.error_code, jb.error_code), (0, 0));
    let generation = ja.generation_id;
    let leader = ja.leader.to_string();
    let follower = if leader == a { b.clone() } else { a.clone() };
    let (sl, sf) = std::thread::scope(|scope| {
        let assign = vec![(leader.clone(), &b"p0"[..]), (follower.clone(), &b"p1"[..])];
        let sl = scope.spawn(|| sync(&old, &leader, generation, assign));
        let sf = scope.spawn(|| sync(&old, &follower, generation, vec![]));
        (sl.join().unwrap(), sf.join().unwrap())
    });
    assert_eq!((sl.error_code, sf.error_code), (0, 0));
    assert_eq!(sf.assignment.as_ref(), b"p1");
    assert_eq!(commit(&old, &leader, generation, 5), 0);

    // Kill the coordinator. A survivor leads metadata and coordinates.
    let gone = nodes.iter().position(|node| node.id == old_id).unwrap();
    nodes[gone].kill();
    let survivor = nodes.iter().find(|node| node.id != old_id).unwrap();
    let (new_id, new) = coordinator_of(&kafka_addr(survivor));
    assert_ne!(new_id, old_id);

    // Same generation, same members, same committed offset.
    let beat = Instant::now() + Duration::from_secs(10);
    loop {
        let code = heartbeat(&new, &follower, generation);
        if code == 0 {
            break;
        }
        // Loading after the election can briefly answer a retriable code.
        assert!(
            [14, 15, 16].contains(&code) && Instant::now() < beat,
            "heartbeat {code}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(heartbeat(&new, &leader, generation), 0);
    assert_eq!(fetched(&new), 5);
    assert_eq!(commit(&new, &leader, generation, 7), 0);
    assert_eq!(
        commit(&new, &leader, generation - 1, 8),
        22,
        "an older generation is fenced"
    );
    assert_eq!(fetched(&new), 7);

    // The old coordinator comes back as a follower and coordinates nothing.
    nodes[gone].restart();
    let back = kafka_addr(&nodes[gone]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while heartbeat(&back, &leader, generation) != 16 {
        assert!(
            Instant::now() < deadline,
            "the old coordinator still answers"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // A rebalance moves the generation on; the old one can no longer commit.
    assert_eq!(leave(&new, &follower), 0);
    assert_eq!(
        heartbeat(&new, &leader, generation),
        27,
        "REBALANCE_IN_PROGRESS"
    );
    let rejoined = join(&new, &leader);
    assert_eq!(rejoined.error_code, 0);
    assert_eq!(rejoined.generation_id, generation + 1);
    assert_eq!(commit(&new, &leader, generation, 9), 22);
    let synced = sync(
        &new,
        &leader,
        generation + 1,
        vec![(leader.clone(), &b"p0p1"[..])],
    );
    assert_eq!(synced.error_code, 0);
    assert_eq!(commit(&new, &leader, generation + 1, 9), 0);
    assert_eq!(fetched(&new), 9);

    // ListGroups, DescribeGroups, DeleteGroups on the new coordinator.
    let mut stream = connect(&new);
    send(
        &mut stream,
        ApiKey::ListGroups,
        4,
        &ListGroupsRequest::default(),
    );
    let listed = recv::<ListGroupsResponse>(&mut stream, 4);
    let row = listed
        .groups
        .iter()
        .find(|row| row.group_id.0.as_str() == GROUP)
        .expect("listed");
    assert_eq!(row.group_state.as_str(), "Stable");
    assert_eq!(row.protocol_type.as_str(), "consumer");
    send(
        &mut stream,
        ApiKey::DescribeGroups,
        5,
        &DescribeGroupsRequest::default().with_groups(vec![group()]),
    );
    let described = recv::<DescribeGroupsResponse>(&mut stream, 5);
    assert_eq!(described.groups[0].group_state.as_str(), "Stable");
    assert_eq!(described.groups[0].protocol_data.as_str(), "range");
    assert_eq!(described.groups[0].members.len(), 1);
    assert_eq!(
        described.groups[0].members[0].member_assignment.as_ref(),
        b"p0p1"
    );
    let delete = |stream: &mut TcpStream| {
        send(
            stream,
            ApiKey::DeleteGroups,
            2,
            &DeleteGroupsRequest::default().with_groups_names(vec![group()]),
        );
        recv::<DeleteGroupsResponse>(stream, 2).results[0].error_code
    };
    assert_eq!(delete(&mut stream), 68, "NON_EMPTY_GROUP");
    assert_eq!(leave(&new, &leader), 0);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let code = delete(&mut stream);
        if code == 0 {
            break;
        }
        // The last member's leave empties the group once its save lands.
        assert!(code == 68 && Instant::now() < deadline, "delete {code}");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(fetched(&new), -1, "offsets went with the group");
    assert_eq!(delete(&mut stream), 69, "GROUP_ID_NOT_FOUND");
}
