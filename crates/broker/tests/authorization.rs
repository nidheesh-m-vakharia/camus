//! Kafka's authorizer on every API: a resource no ACL names follows
//! `allow.everyone.if.no.acl.found`, an ACL for someone else makes a resource
//! deny the caller, hosts other than `*` match only that client, and super
//! users skip the ACLs. Clients connect from 127.0.0.1 as `User:ANONYMOUS`.

mod support;

use camus_protocol::messages::create_acls_request::AclCreation;
use camus_protocol::messages::create_acls_response::CreateAclsResponse;
use camus_protocol::messages::create_topics_request::CreatableTopic;
use camus_protocol::messages::create_topics_response::CreateTopicsResponse;
use camus_protocol::messages::describe_cluster_response::DescribeClusterResponse;
use camus_protocol::messages::describe_configs_request::DescribeConfigsResource;
use camus_protocol::messages::describe_configs_response::DescribeConfigsResponse;
use camus_protocol::messages::describe_groups_response::DescribeGroupsResponse;
use camus_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use camus_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use camus_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use camus_protocol::messages::join_group_response::JoinGroupResponse;
use camus_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use camus_protocol::messages::list_offsets_response::ListOffsetsResponse;
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::{
    ApiKey, CreateAclsRequest, CreateTopicsRequest, DescribeClusterRequest, DescribeConfigsRequest,
    DescribeGroupsRequest, FindCoordinatorRequest, GroupId, InitProducerIdRequest,
    JoinGroupRequest, ListOffsetsRequest, MetadataRequest, TopicName, TransactionalId,
};
use camus_protocol::protocol::StrBytes;
use support::{connect, init, kafka_addr, recv, send, single, wait_settled, Node, Opts, TOPIC};

const TOPIC_TYPE: i8 = 2;
const GROUP_TYPE: i8 = 3;
const TXN_TYPE: i8 = 5;
const ALL: i8 = 2;
const ALLOW: i8 = 3;

fn acl(
    resource_type: i8,
    name: &'static str,
    principal: &'static str,
    host: &'static str,
) -> AclCreation {
    AclCreation::default()
        .with_resource_type(resource_type)
        .with_resource_name(StrBytes::from_static_str(name))
        .with_resource_pattern_type(3)
        .with_principal(StrBytes::from_static_str(principal))
        .with_host(StrBytes::from_static_str(host))
        .with_operation(ALL)
        .with_permission_type(ALLOW)
}

fn create_acls(addr: &str, creations: Vec<AclCreation>) -> Vec<i16> {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::CreateAcls,
        3,
        &CreateAclsRequest::default().with_creations(creations),
    );
    recv::<CreateAclsResponse>(&mut stream, 3)
        .results
        .iter()
        .map(|result| result.error_code)
        .collect()
}

fn create_topic(addr: &str, name: &'static str) -> i16 {
    let mut stream = connect(addr);
    let mut topic = CreatableTopic::default();
    topic.name = TopicName(StrBytes::from_static_str(name));
    topic.num_partitions = 1;
    topic.replication_factor = -1;
    send(
        &mut stream,
        ApiKey::CreateTopics,
        7,
        &CreateTopicsRequest::default().with_topics(vec![topic]),
    );
    recv::<CreateTopicsResponse>(&mut stream, 7).topics[0].error_code
}

fn metadata(addr: &str, names: Option<&[&'static str]>) -> Vec<(String, i16)> {
    let mut stream = connect(addr);
    let topics = names.map(|names| {
        names
            .iter()
            .map(|name| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_static_str(name))))
            })
            .collect()
    });
    send(
        &mut stream,
        ApiKey::Metadata,
        12,
        &MetadataRequest::default().with_topics(topics),
    );
    recv::<MetadataResponse>(&mut stream, 12)
        .topics
        .iter()
        .map(|topic| {
            (
                topic
                    .name
                    .as_ref()
                    .map(|name| name.0.to_string())
                    .unwrap_or_default(),
                topic.error_code,
            )
        })
        .collect()
}

fn list_offsets(addr: &str, name: &'static str) -> i16 {
    let mut stream = connect(addr);
    let topic = ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(name)))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(0)
            .with_timestamp(-1)]);
    send(
        &mut stream,
        ApiKey::ListOffsets,
        7,
        &ListOffsetsRequest::default().with_topics(vec![topic]),
    );
    recv::<ListOffsetsResponse>(&mut stream, 7).topics[0].partitions[0].error_code
}

fn describe_configs(addr: &str, name: &'static str) -> i16 {
    let mut stream = connect(addr);
    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str(name);
    send(
        &mut stream,
        ApiKey::DescribeConfigs,
        4,
        &DescribeConfigsRequest::default().with_resources(vec![resource]),
    );
    recv::<DescribeConfigsResponse>(&mut stream, 4).results[0].error_code
}

fn join(addr: &str, group: &'static str) -> JoinGroupResponse {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::JoinGroup,
        5,
        &JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str(group)))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(10_000)
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default().with_name(StrBytes::from_static_str("range"))
            ]),
    );
    recv::<JoinGroupResponse>(&mut stream, 5)
}

fn find_group(addr: &str, group: &'static str) -> i16 {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::FindCoordinator,
        3,
        &FindCoordinatorRequest::default().with_key(StrBytes::from_static_str(group)),
    );
    recv::<FindCoordinatorResponse>(&mut stream, 3).error_code
}

fn init_transactional(addr: &str, id: &'static str) -> i16 {
    let mut stream = connect(addr);
    send(
        &mut stream,
        ApiKey::InitProducerId,
        4,
        &InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(id))))
            .with_transaction_timeout_ms(60_000),
    );
    recv::<InitProducerIdResponse>(&mut stream, 4).error_code
}

#[test]
fn an_acl_for_someone_else_denies_the_caller_on_every_api() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    assert_eq!(create_topic(&addr, "open"), 0);

    // `orders`, group `owned`, and transactional id `theirs` now belong to
    // someone else; `open` and everything unnamed stays allowed.
    let created = create_acls(
        &addr,
        vec![
            acl(TOPIC_TYPE, TOPIC, "User:other", "*"),
            acl(GROUP_TYPE, "owned", "User:other", "*"),
            acl(TXN_TYPE, "theirs", "User:other", "*"),
        ],
    );
    assert_eq!(created, vec![0, 0, 0]);

    let listed = metadata(&addr, None);
    assert!(listed.iter().any(|(name, _)| name == "open"), "{listed:?}");
    assert!(!listed.iter().any(|(name, _)| name == TOPIC), "{listed:?}");
    assert_eq!(metadata(&addr, Some(&[TOPIC]))[0].1, 29);
    assert_eq!(metadata(&addr, Some(&["open"]))[0].1, 0);
    assert_eq!(list_offsets(&addr, TOPIC), 29);
    assert_eq!(list_offsets(&addr, "open"), 0);
    assert_eq!(describe_configs(&addr, TOPIC), 29);
    assert_eq!(describe_configs(&addr, "open"), 0);

    assert_eq!(find_group(&addr, "owned"), 30);
    assert_eq!(join(&addr, "owned").error_code, 30);
    assert_eq!(find_group(&addr, "mine"), 0);

    assert_eq!(init_transactional(&addr, "theirs"), 53);
    assert_eq!(init_transactional(&addr, "ours"), 0);
}

#[test]
fn a_host_acl_matches_only_that_client() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    assert_eq!(create_topic(&addr, "local"), 0);
    assert_eq!(create_topic(&addr, "remote"), 0);
    let created = create_acls(
        &addr,
        vec![
            acl(TOPIC_TYPE, "local", "User:ANONYMOUS", "127.0.0.1"),
            acl(TOPIC_TYPE, "remote", "User:ANONYMOUS", "10.0.0.9"),
        ],
    );
    assert_eq!(created, vec![0, 0]);
    assert_eq!(list_offsets(&addr, "local"), 0);
    assert_eq!(list_offsets(&addr, "remote"), 29);
}

#[test]
fn join_group_records_the_client_host() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let joined = join(&addr, "hosts");
    // A new member is told to rejoin with its id; the second join enters.
    let rejoined = {
        let mut stream = connect(&addr);
        send(
            &mut stream,
            ApiKey::JoinGroup,
            5,
            &JoinGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from_static_str("hosts")))
                .with_member_id(joined.member_id.clone())
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(10_000)
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))]),
        );
        recv::<JoinGroupResponse>(&mut stream, 5)
    };
    assert_eq!(rejoined.error_code, 0, "{rejoined:?}");
    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::DescribeGroups,
        5,
        &DescribeGroupsRequest::default()
            .with_groups(vec![GroupId(StrBytes::from_static_str("hosts"))]),
    );
    let described = recv::<DescribeGroupsResponse>(&mut stream, 5);
    let member = &described.groups[0].members[0];
    assert_eq!(member.client_host.as_str(), "/127.0.0.1");
}

#[test]
fn without_allow_if_none_an_unnamed_resource_is_denied() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::with(
        1,
        dir.path(),
        Opts::default()
            .env("CAMUS_ACL_ALLOW_IF_NO_ACL", "false")
            .env("CAMUS_SUPER_USERS", "User:admin"),
    );
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    let addr = kafka_addr(&node);
    assert_eq!(create_topic(&addr, "denied"), 29);
    assert!(metadata(&addr, None).is_empty());
    assert_eq!(metadata(&addr, Some(&["denied"]))[0].1, 29);
    assert_eq!(
        create_acls(&addr, vec![acl(TOPIC_TYPE, TOPIC, "User:ANONYMOUS", "*")]),
        vec![31]
    );
    let mut stream = connect(&addr);
    send(
        &mut stream,
        ApiKey::DescribeCluster,
        0,
        &DescribeClusterRequest::default(),
    );
    assert_eq!(
        recv::<DescribeClusterResponse>(&mut stream, 0).error_code,
        31
    );
    assert_eq!(find_group(&addr, "any"), 30);
    assert_eq!(init_transactional(&addr, "any"), 53);
}
