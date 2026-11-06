//! Error responses for API keys this process does not serve.
//!
//! The body is the schema's response for the requested version, with the
//! error code on the response and on one nested result when the error lives
//! there. A version outside the schema cannot be encoded, and the caller
//! closes the connection.

use bytes::Bytes;

use crate::messages::ApiKey;

/// Encode an error response for `key` at `version`.
///
/// Returns `None` when that version has no schema, or when the response type
/// has no place to put an error code.
pub fn error_response(key: ApiKey, version: i16, correlation_id: i32, code: i16) -> Option<Bytes> {
    let versions = key.valid_versions();
    if version < versions.min || version > versions.max {
        return None;
    }
    match key {
        ApiKey::Produce => {
            let mut response = crate::messages::produce_response::ProduceResponse::default();
            let mut response_item =
                crate::messages::produce_response::TopicProduceResponse::default();
            let mut response_item_item =
                crate::messages::produce_response::PartitionProduceResponse::default();
            response_item_item.error_code = code;
            response_item.partition_responses.push(response_item_item);
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::Fetch => {
            let mut response = crate::messages::fetch_response::FetchResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::fetch_response::FetchableTopicResponse::default();
            let mut response_item_item = crate::messages::fetch_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ListOffsets => {
            let mut response =
                crate::messages::list_offsets_response::ListOffsetsResponse::default();
            let mut response_item =
                crate::messages::list_offsets_response::ListOffsetsTopicResponse::default();
            let mut response_item_item =
                crate::messages::list_offsets_response::ListOffsetsPartitionResponse::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::Metadata => {
            let mut response = crate::messages::metadata_response::MetadataResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::metadata_response::MetadataResponseTopic::default();
            response_item.error_code = code;
            let mut response_item_item =
                crate::messages::metadata_response::MetadataResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::OffsetCommit => {
            let mut response =
                crate::messages::offset_commit_response::OffsetCommitResponse::default();
            let mut response_item =
                crate::messages::offset_commit_response::OffsetCommitResponseTopic::default();
            let mut response_item_item =
                crate::messages::offset_commit_response::OffsetCommitResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::OffsetFetch => {
            let mut response =
                crate::messages::offset_fetch_response::OffsetFetchResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::offset_fetch_response::OffsetFetchResponseTopic::default();
            let mut response_item_item =
                crate::messages::offset_fetch_response::OffsetFetchResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::FindCoordinator => {
            let mut response =
                crate::messages::find_coordinator_response::FindCoordinatorResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::find_coordinator_response::Coordinator::default();
            response_item.error_code = code;
            response.coordinators.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::JoinGroup => {
            let mut response = crate::messages::join_group_response::JoinGroupResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::Heartbeat => {
            let mut response = crate::messages::heartbeat_response::HeartbeatResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::LeaveGroup => {
            let mut response = crate::messages::leave_group_response::LeaveGroupResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::leave_group_response::MemberResponse::default();
            response_item.error_code = code;
            response.members.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::SyncGroup => {
            let mut response = crate::messages::sync_group_response::SyncGroupResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeGroups => {
            let mut response =
                crate::messages::describe_groups_response::DescribeGroupsResponse::default();
            let mut response_item =
                crate::messages::describe_groups_response::DescribedGroup::default();
            response_item.error_code = code;
            response.groups.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ListGroups => {
            let mut response = crate::messages::list_groups_response::ListGroupsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::SaslHandshake => {
            let mut response =
                crate::messages::sasl_handshake_response::SaslHandshakeResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ApiVersions => {
            let mut response =
                crate::messages::api_versions_response::ApiVersionsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::CreateTopics => {
            let mut response =
                crate::messages::create_topics_response::CreateTopicsResponse::default();
            let mut response_item =
                crate::messages::create_topics_response::CreatableTopicResult::default();
            response_item.error_code = code;
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DeleteTopics => {
            let mut response =
                crate::messages::delete_topics_response::DeleteTopicsResponse::default();
            let mut response_item =
                crate::messages::delete_topics_response::DeletableTopicResult::default();
            response_item.error_code = code;
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DeleteRecords => {
            let mut response =
                crate::messages::delete_records_response::DeleteRecordsResponse::default();
            let mut response_item =
                crate::messages::delete_records_response::DeleteRecordsTopicResult::default();
            let mut response_item_item =
                crate::messages::delete_records_response::DeleteRecordsPartitionResult::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::InitProducerId => {
            let mut response =
                crate::messages::init_producer_id_response::InitProducerIdResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::OffsetForLeaderEpoch => {
            let mut response = crate::messages::offset_for_leader_epoch_response::OffsetForLeaderEpochResponse::default();
            let mut response_item = crate::messages::offset_for_leader_epoch_response::OffsetForLeaderTopicResult::default();
            let mut response_item_item =
                crate::messages::offset_for_leader_epoch_response::EpochEndOffset::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AddPartitionsToTxn => {
            let mut response = crate::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AddOffsetsToTxn => {
            let mut response =
                crate::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::EndTxn => {
            let mut response = crate::messages::end_txn_response::EndTxnResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::WriteTxnMarkers => {
            let mut response =
                crate::messages::write_txn_markers_response::WriteTxnMarkersResponse::default();
            let mut response_item =
                crate::messages::write_txn_markers_response::WritableTxnMarkerResult::default();
            let response_item_item =
                crate::messages::write_txn_markers_response::WritableTxnMarkerTopicResult::default(
                );
            response_item.topics.push(response_item_item);
            response.markers.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::TxnOffsetCommit => {
            let mut response =
                crate::messages::txn_offset_commit_response::TxnOffsetCommitResponse::default();
            let mut response_item =
                crate::messages::txn_offset_commit_response::TxnOffsetCommitResponseTopic::default(
                );
            let mut response_item_item = crate::messages::txn_offset_commit_response::TxnOffsetCommitResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeAcls => {
            let mut response =
                crate::messages::describe_acls_response::DescribeAclsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::CreateAcls => {
            let mut response = crate::messages::create_acls_response::CreateAclsResponse::default();
            let mut response_item =
                crate::messages::create_acls_response::AclCreationResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DeleteAcls => {
            let mut response = crate::messages::delete_acls_response::DeleteAclsResponse::default();
            let mut response_item =
                crate::messages::delete_acls_response::DeleteAclsFilterResult::default();
            response_item.error_code = code;
            let mut response_item_item =
                crate::messages::delete_acls_response::DeleteAclsMatchingAcl::default();
            response_item_item.error_code = code;
            response_item.matching_acls.push(response_item_item);
            response.filter_results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeConfigs => {
            let mut response =
                crate::messages::describe_configs_response::DescribeConfigsResponse::default();
            let mut response_item =
                crate::messages::describe_configs_response::DescribeConfigsResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterConfigs => {
            let mut response =
                crate::messages::alter_configs_response::AlterConfigsResponse::default();
            let mut response_item =
                crate::messages::alter_configs_response::AlterConfigsResourceResponse::default();
            response_item.error_code = code;
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterReplicaLogDirs => {
            let mut response = crate::messages::alter_replica_log_dirs_response::AlterReplicaLogDirsResponse::default();
            let mut response_item = crate::messages::alter_replica_log_dirs_response::AlterReplicaLogDirTopicResult::default();
            let mut response_item_item = crate::messages::alter_replica_log_dirs_response::AlterReplicaLogDirPartitionResult::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeLogDirs => {
            let mut response =
                crate::messages::describe_log_dirs_response::DescribeLogDirsResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::describe_log_dirs_response::DescribeLogDirsResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::SaslAuthenticate => {
            let mut response =
                crate::messages::sasl_authenticate_response::SaslAuthenticateResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::CreatePartitions => {
            let mut response =
                crate::messages::create_partitions_response::CreatePartitionsResponse::default();
            let mut response_item =
                crate::messages::create_partitions_response::CreatePartitionsTopicResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::CreateDelegationToken => {
            let mut response = crate::messages::create_delegation_token_response::CreateDelegationTokenResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::RenewDelegationToken => {
            let mut response = crate::messages::renew_delegation_token_response::RenewDelegationTokenResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ExpireDelegationToken => {
            let mut response = crate::messages::expire_delegation_token_response::ExpireDelegationTokenResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeDelegationToken => {
            let mut response = crate::messages::describe_delegation_token_response::DescribeDelegationTokenResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DeleteGroups => {
            let mut response =
                crate::messages::delete_groups_response::DeleteGroupsResponse::default();
            let mut response_item =
                crate::messages::delete_groups_response::DeletableGroupResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ElectLeaders => {
            let mut response =
                crate::messages::elect_leaders_response::ElectLeadersResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::elect_leaders_response::ReplicaElectionResult::default();
            let mut response_item_item =
                crate::messages::elect_leaders_response::PartitionResult::default();
            response_item_item.error_code = code;
            response_item.partition_result.push(response_item_item);
            response.replica_election_results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::IncrementalAlterConfigs => {
            let mut response = crate::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse::default();
            let mut response_item = crate::messages::incremental_alter_configs_response::AlterConfigsResourceResponse::default();
            response_item.error_code = code;
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterPartitionReassignments => {
            let mut response = crate::messages::alter_partition_reassignments_response::AlterPartitionReassignmentsResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::alter_partition_reassignments_response::ReassignableTopicResponse::default();
            let mut response_item_item = crate::messages::alter_partition_reassignments_response::ReassignablePartitionResponse::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ListPartitionReassignments => {
            let mut response = crate::messages::list_partition_reassignments_response::ListPartitionReassignmentsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::OffsetDelete => {
            let mut response =
                crate::messages::offset_delete_response::OffsetDeleteResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::offset_delete_response::OffsetDeleteResponseTopic::default();
            let mut response_item_item =
                crate::messages::offset_delete_response::OffsetDeleteResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeClientQuotas => {
            let mut response = crate::messages::describe_client_quotas_response::DescribeClientQuotasResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterClientQuotas => {
            let mut response =
                crate::messages::alter_client_quotas_response::AlterClientQuotasResponse::default();
            let mut response_item =
                crate::messages::alter_client_quotas_response::EntryData::default();
            response_item.error_code = code;
            response.entries.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeUserScramCredentials => {
            let mut response = crate::messages::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::describe_user_scram_credentials_response::DescribeUserScramCredentialsResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterUserScramCredentials => {
            let mut response = crate::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResponse::default();
            let mut response_item = crate::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::Vote => {
            let mut response = crate::messages::vote_response::VoteResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::vote_response::TopicData::default();
            let mut response_item_item = crate::messages::vote_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::BeginQuorumEpoch => {
            let mut response =
                crate::messages::begin_quorum_epoch_response::BeginQuorumEpochResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::begin_quorum_epoch_response::TopicData::default();
            let mut response_item_item =
                crate::messages::begin_quorum_epoch_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::EndQuorumEpoch => {
            let mut response =
                crate::messages::end_quorum_epoch_response::EndQuorumEpochResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::end_quorum_epoch_response::TopicData::default();
            let mut response_item_item =
                crate::messages::end_quorum_epoch_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeQuorum => {
            let mut response =
                crate::messages::describe_quorum_response::DescribeQuorumResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::describe_quorum_response::TopicData::default();
            let mut response_item_item =
                crate::messages::describe_quorum_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterPartition => {
            let mut response =
                crate::messages::alter_partition_response::AlterPartitionResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::alter_partition_response::TopicData::default();
            let mut response_item_item =
                crate::messages::alter_partition_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::UpdateFeatures => {
            let mut response =
                crate::messages::update_features_response::UpdateFeaturesResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::update_features_response::UpdatableFeatureResult::default();
            response_item.error_code = code;
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::Envelope => {
            let mut response = crate::messages::envelope_response::EnvelopeResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::FetchSnapshot => {
            let mut response =
                crate::messages::fetch_snapshot_response::FetchSnapshotResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::fetch_snapshot_response::TopicSnapshot::default();
            let mut response_item_item =
                crate::messages::fetch_snapshot_response::PartitionSnapshot::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeCluster => {
            let mut response =
                crate::messages::describe_cluster_response::DescribeClusterResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeProducers => {
            let mut response =
                crate::messages::describe_producers_response::DescribeProducersResponse::default();
            let mut response_item =
                crate::messages::describe_producers_response::TopicResponse::default();
            let mut response_item_item =
                crate::messages::describe_producers_response::PartitionResponse::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::BrokerRegistration => {
            let mut response =
                crate::messages::broker_registration_response::BrokerRegistrationResponse::default(
                );
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::BrokerHeartbeat => {
            let mut response =
                crate::messages::broker_heartbeat_response::BrokerHeartbeatResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::UnregisterBroker => {
            let mut response =
                crate::messages::unregister_broker_response::UnregisterBrokerResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeTransactions => {
            let mut response = crate::messages::describe_transactions_response::DescribeTransactionsResponse::default();
            let mut response_item =
                crate::messages::describe_transactions_response::TransactionState::default();
            response_item.error_code = code;
            response.transaction_states.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ListTransactions => {
            let mut response =
                crate::messages::list_transactions_response::ListTransactionsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AllocateProducerIds => {
            let mut response = crate::messages::allocate_producer_ids_response::AllocateProducerIdsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ConsumerGroupHeartbeat => {
            let mut response = crate::messages::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ConsumerGroupDescribe => {
            let mut response = crate::messages::consumer_group_describe_response::ConsumerGroupDescribeResponse::default();
            let mut response_item =
                crate::messages::consumer_group_describe_response::DescribedGroup::default();
            response_item.error_code = code;
            response.groups.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ControllerRegistration => {
            let mut response = crate::messages::controller_registration_response::ControllerRegistrationResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::GetTelemetrySubscriptions => {
            let mut response = crate::messages::get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::PushTelemetry => {
            let mut response =
                crate::messages::push_telemetry_response::PushTelemetryResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AssignReplicasToDirs => {
            let mut response = crate::messages::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::assign_replicas_to_dirs_response::DirectoryData::default();
            let response_item_item =
                crate::messages::assign_replicas_to_dirs_response::TopicData::default();
            response_item.topics.push(response_item_item);
            response.directories.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ListConfigResources => {
            let mut response = crate::messages::list_config_resources_response::ListConfigResourcesResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeTopicPartitions => {
            let mut response = crate::messages::describe_topic_partitions_response::DescribeTopicPartitionsResponse::default();
            let mut response_item = crate::messages::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic::default();
            response_item.error_code = code;
            let mut response_item_item = crate::messages::describe_topic_partitions_response::DescribeTopicPartitionsResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.topics.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ShareGroupHeartbeat => {
            let mut response = crate::messages::share_group_heartbeat_response::ShareGroupHeartbeatResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ShareGroupDescribe => {
            let mut response =
                crate::messages::share_group_describe_response::ShareGroupDescribeResponse::default(
                );
            let mut response_item =
                crate::messages::share_group_describe_response::DescribedGroup::default();
            response_item.error_code = code;
            response.groups.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ShareFetch => {
            let mut response = crate::messages::share_fetch_response::ShareFetchResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::share_fetch_response::ShareFetchableTopicResponse::default();
            let mut response_item_item =
                crate::messages::share_fetch_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ShareAcknowledge => {
            let mut response =
                crate::messages::share_acknowledge_response::ShareAcknowledgeResponse::default();
            response.error_code = code;
            let mut response_item =
                crate::messages::share_acknowledge_response::ShareAcknowledgeTopicResponse::default(
                );
            let mut response_item_item =
                crate::messages::share_acknowledge_response::PartitionData::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AddRaftVoter => {
            let mut response =
                crate::messages::add_raft_voter_response::AddRaftVoterResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::RemoveRaftVoter => {
            let mut response =
                crate::messages::remove_raft_voter_response::RemoveRaftVoterResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::UpdateRaftVoter => {
            let mut response =
                crate::messages::update_raft_voter_response::UpdateRaftVoterResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::InitializeShareGroupState => {
            let mut response = crate::messages::initialize_share_group_state_response::InitializeShareGroupStateResponse::default();
            let mut response_item = crate::messages::initialize_share_group_state_response::InitializeStateResult::default();
            let mut response_item_item =
                crate::messages::initialize_share_group_state_response::PartitionResult::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ReadShareGroupState => {
            let mut response = crate::messages::read_share_group_state_response::ReadShareGroupStateResponse::default();
            let mut response_item =
                crate::messages::read_share_group_state_response::ReadStateResult::default();
            let mut response_item_item =
                crate::messages::read_share_group_state_response::PartitionResult::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::WriteShareGroupState => {
            let mut response = crate::messages::write_share_group_state_response::WriteShareGroupStateResponse::default();
            let mut response_item =
                crate::messages::write_share_group_state_response::WriteStateResult::default();
            let mut response_item_item =
                crate::messages::write_share_group_state_response::PartitionResult::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DeleteShareGroupState => {
            let mut response = crate::messages::delete_share_group_state_response::DeleteShareGroupStateResponse::default();
            let mut response_item =
                crate::messages::delete_share_group_state_response::DeleteStateResult::default();
            let mut response_item_item =
                crate::messages::delete_share_group_state_response::PartitionResult::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::ReadShareGroupStateSummary => {
            let mut response = crate::messages::read_share_group_state_summary_response::ReadShareGroupStateSummaryResponse::default();
            let mut response_item = crate::messages::read_share_group_state_summary_response::ReadStateSummaryResult::default();
            let mut response_item_item =
                crate::messages::read_share_group_state_summary_response::PartitionResult::default(
                );
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.results.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::StreamsGroupHeartbeat => {
            let mut response = crate::messages::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::StreamsGroupDescribe => {
            let mut response = crate::messages::streams_group_describe_response::StreamsGroupDescribeResponse::default();
            let mut response_item =
                crate::messages::streams_group_describe_response::DescribedGroup::default();
            response_item.error_code = code;
            response.groups.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DescribeShareGroupOffsets => {
            let mut response = crate::messages::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponse::default();
            let mut response_item = crate::messages::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup::default();
            response_item.error_code = code;
            let response_item_item = crate::messages::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseTopic::default();
            response_item.topics.push(response_item_item);
            response.groups.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::AlterShareGroupOffsets => {
            let mut response = crate::messages::alter_share_group_offsets_response::AlterShareGroupOffsetsResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::alter_share_group_offsets_response::AlterShareGroupOffsetsResponseTopic::default();
            let mut response_item_item = crate::messages::alter_share_group_offsets_response::AlterShareGroupOffsetsResponsePartition::default();
            response_item_item.error_code = code;
            response_item.partitions.push(response_item_item);
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::DeleteShareGroupOffsets => {
            let mut response = crate::messages::delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse::default();
            response.error_code = code;
            let mut response_item = crate::messages::delete_share_group_offsets_response::DeleteShareGroupOffsetsResponseTopic::default();
            response_item.error_code = code;
            response.responses.push(response_item);
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::StreamsGroupTopologyDescriptionUpdate => {
            let mut response = crate::messages::streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
        ApiKey::UnregisterController => {
            let mut response = crate::messages::unregister_controller_response::UnregisterControllerResponse::default();
            response.error_code = code;
            crate::frame::encode_response(version, correlation_id, &response).ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::error_response;
    use crate::messages::{ApiKey, HeartbeatResponse, JoinGroupResponse, ResponseHeader};
    use crate::protocol::{Decodable, HeaderVersion};
    use bytes::Bytes;

    fn body<M: Decodable + HeaderVersion>(frame: Bytes, version: i16) -> M {
        let mut buf = frame;
        let size = i32::from_be_bytes(buf.split_to(4).as_ref().try_into().unwrap());
        assert_eq!(size as usize, buf.len());
        let _ = ResponseHeader::decode(&mut buf, M::header_version(version)).unwrap();
        M::decode(&mut buf, version).unwrap()
    }

    #[test]
    fn known_unimplemented_keys_carry_unsupported_version() {
        let heartbeat = error_response(ApiKey::Heartbeat, 4, 9, 35).unwrap();
        let decoded = body::<HeartbeatResponse>(heartbeat, 4);
        assert_eq!(decoded.error_code, 35);

        let joined = error_response(ApiKey::JoinGroup, 5, 3, 35).unwrap();
        let decoded = body::<JoinGroupResponse>(joined, 5);
        assert_eq!(decoded.error_code, 35);

        assert!(error_response(ApiKey::Heartbeat, 99, 1, 35).is_none());
        assert!(error_response(ApiKey::Produce, 2, 1, 35).is_none());
    }
}
