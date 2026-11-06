# Kafka 4.4.0 group coordinator tests, ported

Phase 3 acceptance (ADR 0013) asks for the 4.4.0 group coordinator test
cases that apply to the classic protocol. Source:
`group-coordinator/src/test/java/org/apache/kafka/coordinator/group/` at
tag `4.4.0`. The ports are in `crates/coordinator/src/tests.rs`, named after
the Kafka test in snake case (two adapt a timer-ordering artifact; see
their comments), and use Kafka's test defaults (500 ms session
and rebalance timeouts on the request builder, 3 s initial delay, 10 s / 5 s
in the helpers that form groups).

A case counts as classic here when its body drives the classic group API
and not the consumer, share, or streams protocols or a conversion between
them. Porting changed the coordinator in these ways, each now matching
Kafka:

- Static members that rejoin without a member id get a new id; the old one
  is fenced (`FENCED_INSTANCE_ID`) in join, sync, heartbeat, leave, and
  offset commit, and the replacement is stored before the member hears.
- The initial rebalance delay extends while new members arrive, from a
  budget of the rebalance timeout minus one delay.
- Sessions expire during a rebalance; members awaiting join or sync are
  alive. Members that never sync are removed after the rebalance timeout,
  even once the group is stable.
- A join phase with only static members that have not rejoined is
  extended instead of completed; dynamic stragglers are removed.
- `group.max.size` follows `acceptJoiningMember`, and a heartbeat while the
  rebalance completes answers `NONE`.
- Pending members (after `MEMBER_ID_REQUIRED`) expire on their own session
  and block completion of a join phase until they join or expire.
- DescribeGroups outside `Stable` reports no protocol and no member
  metadata.

## `classic/ClassicGroupTest.java`

6 of 58 ported.

| Kafka test | Status |
| --- | --- |
| `testCanRebalanceWhenStable` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testCanRebalanceWhenCompletingRebalance` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testCannotRebalanceWhenPreparingRebalance` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testCannotRebalanceWhenDead` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testStableToPreparingRebalanceTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testStableToDeadTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testAwaitingRebalanceToPreparingRebalanceTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testPreparingRebalanceToDeadTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testPreparingRebalanceToEmptyTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testEmptyToDeadTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testAwaitingRebalanceToStableTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testEmptyToStableIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testStableToStableIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testEmptyToAwaitingRebalanceIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testPreparingRebalanceToPreparingRebalanceIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testPreparingRebalanceToStableIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testAwaitingRebalanceToAwaitingRebalanceIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testDeadToDeadIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testDeadToStableIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testDeadToPreparingRebalanceIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testDeadToAwaitingRebalanceIllegalTransition` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testSelectProtocol` | ported |
| `testSelectProtocolRaisesIfNoMembers` | ported |
| `testSelectProtocolChoosesCompatibleProtocol` | ported |
| `testSupportsProtocols` | ported |
| `testSubscribedTopics` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testSubscribedTopicsNonConsumerGroup` | not ported: conversion between classic and KIP-848 groups; KIP-848 is not built |
| `testInitNextGeneration` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testInitNextGenerationEmptyGroup` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testUpdateMember` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testReplaceGroupInstanceWithNonExistingMember` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testReplaceGroupInstance` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testCompleteJoinFuture` | not ported: tests Kafka future plumbing; replies are covered by the join and sync ports |
| `testNotCompleteJoinFuture` | not ported: tests Kafka future plumbing; replies are covered by the join and sync ports |
| `testCompleteSyncFuture` | not ported: tests Kafka future plumbing; replies are covered by the join and sync ports |
| `testNotCompleteSyncFuture` | not ported: tests Kafka future plumbing; replies are covered by the join and sync ports |
| `testCannotAddPendingMemberIfStable` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testRemovalFromPendingAfterMemberIsStable` | not ported: `ClassicGroup` bookkeeping; covered by `test_join_group_completion_when_pending_member_joins` |
| `testRemovalFromPendingWhenMemberIsRemoved` | not ported: `ClassicGroup` bookkeeping; covered by `test_pending_members_leave_group` |
| `testCannotAddStaticMemberIfAlreadyPresent` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testCannotAddPendingSyncOfUnknownMember` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testCannotRemovePendingSyncOfUnknownMember` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testCanAddAndRemovePendingSyncMember` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testRemovalFromPendingSyncWhenMemberIsRemoved` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testNewGenerationClearsPendingSyncMembers` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testElectNewJoinedLeader` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testMaybeElectNewJoinedLeaderChooseExisting` | not ported: unit test of a `ClassicGroup` method; the behavior is covered by the join and static-member ports |
| `testValidateOffsetCommit` | ported |
| `testValidateOffsetDelete` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testValidateDeleteGroup` | ported |
| `testOffsetExpirationCondition` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testIsSubscribedToTopic` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testComputeSubscribedTopicsHandlesMalformedMemberMetadata` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testIsInStates` | not ported: Kafka-internal state guard of `ClassicGroup`; the state machine here has no free transition API, and the transitions are covered by the join, sync and leave ports |
| `testCompleteAllJoinFutures` | not ported: tests Kafka future plumbing; replies are covered by the join and sync ports |
| `testCompleteAllSyncFutures` | not ported: tests Kafka future plumbing; replies are covered by the join and sync ports |
| `testFromConsumerGroupWithJoiningMember` | not ported: conversion between classic and KIP-848 groups; KIP-848 is not built |
| `testFromConsumerGroupWithoutJoiningMember` | not ported: conversion between classic and KIP-848 groups; KIP-848 is not built |

## `GroupMetadataManagerTest.java`, classic-group cases

95 of 125 ported.

| Kafka test | Status |
| --- | --- |
| `testGenerateRecordsOnNewClassicGroup` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testGenerateRecordsOnNewClassicGroupFailureTransformsError` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testReplayGroupMetadataRecords` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testOnLoadedExceedGroupMaxSizeTriggersRebalance` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testOnLoadedSchedulesClassicGroupMemberHeartbeats` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testJoinGroupShouldReceiveErrorIfGroupOverMaxSize` | ported |
| `testDynamicMembersJoinGroupWithMaxSizeAndRequiredKnownMember` | ported |
| `testDynamicMembersJoinGroupWithMaxSizeAndNotRequiredKnownMember` | ported |
| `testStaticMembersJoinGroupWithMaxSize` | ported |
| `testDynamicMembersCanRejoinGroupWithMaxSizeWhileRebalancing` | not ported: covered by the third round of `test_dynamic_members_join_group_with_max_size_and_required_known_member` |
| `testLastJoiningMembersAreKickedOutWhenRejoiningGroupWithMaxSize` | not ported: needs a loaded group already over `group.max.size`; Camus never loads one (the size is checked on join) |
| `testJoinGroupUnknownMemberNewGroup` | ported |
| `testClassicGroupJoinInconsistentProtocolType` | ported |
| `testJoinGroupWithEmptyProtocolType` | ported |
| `testJoinGroupWithEmptyGroupProtocol` | ported |
| `testNewMemberJoinExpiration` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testJoinGroupInconsistentGroupProtocol` | ported |
| `testJoinGroupSecondJoinInconsistentProtocol` | ported |
| `testStaticMemberJoinAsFirstMember` | ported |
| `testStaticMemberRejoinWithExplicitUnknownMemberId` | ported |
| `testJoinGroupUnknownConsumerExistingGroup` | ported |
| `testJoinGroupUnknownConsumerNewDeadGroup` | ported |
| `testJoinGroupProtocolTypeIsNotProvidedWhenAnErrorOccurs` | ported |
| `testJoinGroupReturnsTheProtocolType` | ported |
| `testDelayInitialRebalanceByGroupInitialRebalanceDelayOnEmptyGroup` | ported |
| `testResetRebalanceDelayWhenNewMemberJoinsGroupDuringInitialRebalance` | ported |
| `testDelayRebalanceUptoRebalanceTimeout` | ported |
| `testJoinGroupReplaceStaticMember` | ported |
| `testHeartbeatExpirationShouldRemovePendingMember` | not ported: offset expiration, OffsetDelete, and subscription tracking are not built (Phase 4) |
| `testHeartbeatExpirationShouldRemoveMember` | ported |
| `testExistingMemberJoinDeadGroup` | ported |
| `testJoinGroupExistingPendingMemberWithGroupInstanceIdThrowsException` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testJoinGroupExistingMemberUpdatedMetadataTriggersRebalance` | ported |
| `testJoinGroupAsExistingLeaderTriggersRebalanceInStableState` | ported |
| `testJoinGroupAsExistingMemberWithUpdatedMetadataTriggersRebalanceInStableState` | ported |
| `testJoinGroupExistingMemberDoesNotTriggerRebalanceInStableState` | ported |
| `testJoinGroupExistingMemberInEmptyState` | ported |
| `testCompleteJoinRemoveNotYetRejoinedDynamicMembers` | ported |
| `testCompleteJoinPhaseInEmptyStateSkipsRebalance` | not ported: calls the join-completion callback directly in Empty; not reachable here |
| `testCompleteJoinPhaseNoMembersRejoinedExtendsJoinPhase` | ported |
| `testReplaceStaticMemberInStableStateNoError` | not ported: covered by `test_static_member_rejoin_as_follower_with_unknown_member_id` |
| `testReplaceStaticMemberInStableStateWithUpdatedProtocolTriggersRebalance` | not ported: covered by `test_static_member_rejoin_with_unknown_member_id_and_change_of_protocol_with_selected_protocol_changed` |
| `testReplaceStaticMemberInStableStateErrors` | ported |
| `testReplaceStaticMemberInStableStateSucceeds` | not ported: covered by `test_get_different_static_member_id_after_each_rejoin` |
| `testReplaceStaticMemberInCompletingRebalanceStateTriggersRebalance` | ported |
| `testNewMemberTimeoutCompletion` | ported |
| `testNewMemberFailureAfterJoinGroupCompletion` | ported |
| `testStaticMemberFenceDuplicateRejoinedFollower` | ported |
| `testStaticMemberFenceDuplicateSyncingFollowerAfterMemberIdChanged` | ported |
| `testStaticMemberFenceDuplicateRejoiningFollowerAfterMemberIdChanged` | ported |
| `testStaticMemberRejoinWithKnownMemberId` | ported |
| `testStaticMemberRejoinWithLeaderIdAndUnknownMemberId` | ported |
| `testStaticMemberRejoinWithLeaderIdAndKnownMemberId` | ported |
| `testStaticMemberRejoinWithLeaderIdAndUnexpectedDeadGroup` | not ported: forces a Dead group that still has members; `kill` here removes them |
| `testStaticMemberRejoinWithLeaderIdAndUnexpectedEmptyGroup` | not ported: forces an Empty group that still has a static member, a state the state machine here cannot reach |
| `testStaticMemberRejoinWithFollowerIdAndChangeOfProtocol` | ported |
| `testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWithSelectedProtocolChanged` | ported |
| `testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchangedPersistenceFailure` | not ported: checks Kafka log-append error mapping (MESSAGE_TOO_LARGE, UNKNOWN_SERVER_ERROR); the failed-save path is covered by `test_replace_static_member_in_stable_state_errors` |
| `testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchanged` | ported |
| `testStaticMemberRejoinWithKnownLeaderIdToTriggerRebalanceAndFollowerWithChangeOfProtocol` | not ported: its two halves are `test_static_member_rejoin_with_leader_id_and_known_member_id` and `test_static_member_rejoin_with_follower_id_and_change_of_protocol` |
| `testStaticMemberRejoinAsFollowerWithUnknownMemberId` | ported |
| `testStaticMemberRejoinAsFollowerWithKnownMemberIdAndNoProtocolChange` | ported |
| `testStaticMemberRejoinAsFollowerWithMismatchedInstanceId` | ported |
| `testStaticMemberRejoinAsLeaderWithMismatchedInstanceId` | ported |
| `testStaticMemberSyncAsLeaderWithInvalidMemberId` | ported |
| `testGetDifferentStaticMemberIdAfterEachRejoin` | ported |
| `testStaticMemberJoinWithUnknownInstanceIdAndKnownMemberId` | ported |
| `testStaticMemberReJoinWithIllegalStateAsUnknownMember` | not ported: forces an illegal internal state to check Kafka raises IllegalStateException |
| `testStaticMemberFollowerFailToRejoinBeforeRebalanceTimeout` | ported |
| `testStaticMemberLeaderFailToRejoinBeforeRebalanceTimeout` | ported |
| `testSyncGroupFromUnknownGroup` | ported |
| `testSyncGroupFromUnknownMember` | ported |
| `testSyncGroupFromIllegalGeneration` | ported |
| `testSyncGroupAsLeaderAppendFailureTransformsError` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testJoinGroupFromUnchangedFollowerDoesNotRebalance` | ported |
| `testLeaderFailureInSyncGroup` | ported |
| `testSyncGroupFollowerAfterLeader` | ported |
| `testSyncGroupLeaderAfterFollower` | ported |
| `testJoinGroupFromUnchangedLeaderShouldRebalance` | ported |
| `testJoinGroupCompletionWhenPendingMemberJoins` | ported |
| `testJoinGroupCompletionWhenPendingMemberTimesOut` | ported |
| `testGenerationIdIncrementsOnRebalance` | ported |
| `testStaticMemberHeartbeatLeaderWithInvalidMemberId` | ported |
| `testHeartbeatUnknownGroup` | ported |
| `testHeartbeatDeadGroup` | ported |
| `testHeartbeatEmptyGroup` | ported |
| `testHeartbeatUnknownMemberExistingGroup` | ported |
| `testHeartbeatDuringPreparingRebalance` | ported |
| `testHeartbeatDuringCompletingRebalance` | ported |
| `testHeartbeatIllegalGeneration` | ported |
| `testValidHeartbeat` | ported |
| `testClassicGroupMemberSessionTimeout` | ported |
| `testClassicGroupMemberHeartbeatMaintainsSession` | ported |
| `testClassicGroupMemberSessionTimeoutDuringRebalance` | ported |
| `testRebalanceCompletesBeforeMemberJoins` | not ported: an eleven-step sequence; its steps (rebalance before a member joins, stale generation heartbeat, rejoin) are each covered by the heartbeat and join ports |
| `testSyncGroupEmptyAssignment` | ported |
| `testSecondMemberPartiallyJoinAndTimeout` | ported |
| `testRebalanceTimesOutWhenSyncRequestIsNotReceived` | ported |
| `testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromFollowers` | ported |
| `testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromLeaders` | ported |
| `testRebalanceDoesNotTimeOutWhenAllSyncAreReceived` | ported |
| `testHeartbeatDuringRebalanceCausesRebalanceInProgress` | ported |
| `testDescribeGroupRebalancing` | ported |
| `testGroupStuckInRebalanceTimeoutDueToNonjoinedStaticMember` | not ported: a static member with a long session left out of a rebalance and then leaving; the parts are covered by `test_complete_join_phase_no_members_rejoined_extends_join_phase` and the leave ports |
| `testPendingMembersLeaveGroup` | ported |
| `testLeaveGroupInvalidGroup` | ported |
| `testLeaveGroupUnknownGroup` | ported |
| `testLeaveGroupUnknownMemberIdExistingGroup` | ported |
| `testLeaveDeadGroup` | ported |
| `testValidLeaveGroup` | ported |
| `testLeaveGroupWithFencedInstanceId` | ported |
| `testLeaveGroupStaticMemberWithUnknownMemberId` | ported |
| `testStaticMembersValidBatchLeaveGroup` | ported |
| `testStaticMembersLeaveUnknownGroup` | ported |
| `testStaticMembersFencedInstanceBatchLeaveGroup` | ported |
| `testStaticMembersUnknownInstanceBatchLeaveGroup` | ported |
| `testPendingMemberBatchLeaveGroup` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testJoinedMemberPendingMemberBatchLeaveGroup` | not ported: bookkeeping of `ClassicGroup`; behavior covered by the pending-member ports |
| `testJoinedMemberPendingMemberBatchLeaveGroupWithUnknownMember` | ported |
| `testClassicGroupDelete` | not ported: checks the delete records; deletion is covered by `broker/tests/group_failover.rs` |
| `testClassicGroupMaybeDelete` | not ported: checks the delete records; deletion is covered by `broker/tests/group_failover.rs` |
| `testClassicGroupJoinWithEmptyConsumerGroup` | not ported: conversion between classic and KIP-848 groups; KIP-848 is not built |
| `testClassicGroupOnUnloadedEmptyAndPreparingRebalance` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testClassicGroupOnUnloadedCompletingRebalance` | not ported: checks Kafka records, replay, metrics, or log appends; storage here is the metadata Raft group, covered by its own tests |
| `testClassicGroupHeartbeatToConsumerWithUnknownMember` | not ported: conversion between classic and KIP-848 groups; KIP-848 is not built |
