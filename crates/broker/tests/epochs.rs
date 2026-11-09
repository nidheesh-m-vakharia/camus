//! Leader epochs on one broker: Metadata reports the shard's epoch, Fetch
//! and ListOffsets check a client's `current_leader_epoch`, and
//! `OffsetForLeaderEpoch` answers from the epoch history, unsealed records
//! included.

mod support;

use support::{epoch_end, fetch_in_epoch, kafka_addr, partition_epoch, produce, single, Opts};

#[test]
fn epochs_are_reported_checked_and_answered() {
    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    let epoch = partition_epoch(&addr);
    assert!(epoch >= 1, "{epoch}");
    for value in ["a", "b", "c"] {
        assert_eq!(produce(&addr, value).0, 0);
    }
    assert_eq!(fetch_in_epoch(&addr, -1, 0), 0, "-1 skips the check");
    assert_eq!(fetch_in_epoch(&addr, epoch, 0), 0);
    assert_eq!(
        fetch_in_epoch(&addr, epoch - 1, 0),
        74,
        "FENCED_LEADER_EPOCH"
    );
    assert_eq!(
        fetch_in_epoch(&addr, epoch + 1, 0),
        76,
        "UNKNOWN_LEADER_EPOCH"
    );

    assert_eq!(epoch_end(&addr, epoch, epoch), (0, epoch, 3));
    // Every record is in the current epoch, so an earlier one ended at 0.
    assert_eq!(epoch_end(&addr, epoch, epoch - 1), (0, epoch - 1, 0));
    assert_eq!(epoch_end(&addr, epoch - 1, epoch).0, 74);
}

/// ListOffsets has `leader_epoch` only from v4. An older client must still
/// get an answer rather than a closed connection.
#[test]
fn list_offsets_answers_versions_without_an_epoch() {
    use camus_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use camus_protocol::messages::list_offsets_response::ListOffsetsResponse;
    use camus_protocol::messages::{ApiKey, BrokerId, ListOffsetsRequest, TopicName};
    use camus_protocol::protocol::StrBytes;

    let dir = tempfile::tempdir().unwrap();
    let node = single(dir.path(), Opts::default());
    let addr = kafka_addr(&node);
    for version in [1i16, 3, 4, 11] {
        let mut stream = support::connect(&addr);
        let topic = ListOffsetsTopic::default()
            .with_name(TopicName(StrBytes::from_static_str(support::TOPIC)))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)]);
        support::send(
            &mut stream,
            ApiKey::ListOffsets,
            version,
            &ListOffsetsRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_topics(vec![topic]),
        );
        let response = support::recv::<ListOffsetsResponse>(&mut stream, version);
        let row = &response.topics[0].partitions[0];
        assert_eq!(row.error_code, 0, "v{version}");
        if version >= 4 {
            assert!(row.leader_epoch >= 1, "v{version}");
        }
    }
}
