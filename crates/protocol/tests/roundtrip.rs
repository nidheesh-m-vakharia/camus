use std::collections::BTreeMap;

use bytes::Bytes;
use camus_protocol::frame::{self, Unimplemented};
use camus_protocol::messages::metadata_request::MetadataRequestTopic;
use camus_protocol::messages::{
    ApiKey, ApiVersionsRequest, MetadataRequest, RequestHeader, TopicName,
};
use camus_protocol::protocol::{Message, StrBytes};
use camus_protocol::ResponseError;

#[test]
fn api_versions_v5_round_trips_with_the_4_4_0_fields() {
    // Version 5 added ClusterId and NodeId (KIP-1242) in the pinned 4.4.0 schema.
    let schema = include_str!("../testdata/ApiVersionsRequest.json");
    assert!(schema.contains("\"validVersions\": \"0-5\""));
    assert!(schema.contains("\"apiKey\": 18"));
    assert_eq!(ApiVersionsRequest::VERSIONS.max, 5);
    assert_eq!(ApiVersionsRequest::VERSIONS.min, 0);

    let version = ApiVersionsRequest::VERSIONS.max;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(version)
        .with_correlation_id(42)
        .with_client_id(Some(StrBytes::from_static_str("camus-test")));
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("camus"))
        .with_client_software_version(StrBytes::from_static_str("0.1.0"))
        .with_cluster_id(Some(StrBytes::from_static_str("cluster-1")))
        .with_node_id(7);

    let frame = frame::encode_request(&header, &request, version).unwrap();
    let (decoded_header, decoded) = frame::decode_request::<ApiVersionsRequest>(&frame).unwrap();
    assert_eq!(decoded_header.correlation_id, 42);
    assert_eq!(decoded.client_software_name.as_str(), "camus");
    assert_eq!(decoded.cluster_id.unwrap().as_str(), "cluster-1");
    assert_eq!(decoded.node_id, 7);
}

#[test]
fn metadata_v13_preserves_unknown_tagged_fields() {
    let schema = include_str!("../testdata/MetadataRequest.json");
    assert!(schema.contains("\"validVersions\": \"0-13\""));
    assert_eq!(MetadataRequest::VERSIONS.max, 13);

    let version = MetadataRequest::VERSIONS.max;
    let mut tagged = BTreeMap::new();
    tagged.insert(7, Bytes::from_static(b"kept"));
    let topic = MetadataRequestTopic::default()
        .with_name(Some(TopicName(StrBytes::from_static_str("events"))))
        .with_unknown_tagged_fields(tagged);
    let request = MetadataRequest::default().with_topics(Some(vec![topic]));
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(version)
        .with_correlation_id(9)
        .with_client_id(Some(StrBytes::from_static_str("camus-test")));

    let frame = frame::encode_request(&header, &request, version).unwrap();
    let (_, decoded) = frame::decode_request::<MetadataRequest>(&frame).unwrap();
    let topics = decoded.topics.expect("topics");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name.as_ref().unwrap().0.as_str(), "events");
    assert_eq!(
        topics[0].unknown_tagged_fields.get(&7).unwrap().as_ref(),
        b"kept"
    );
}

#[test]
fn known_unimplemented_keys_are_unsupported_version() {
    assert!(frame::is_implemented(ApiKey::Produce, 9));
    assert!(!frame::is_implemented(ApiKey::Produce, 2));
    assert!(frame::reject_unimplemented(ApiKey::Produce as i16).is_ok());
    assert!(frame::reject_unimplemented(ApiKey::JoinGroup as i16).is_ok());
    assert_eq!(
        frame::reject_unimplemented(ApiKey::DescribeClientQuotas as i16).unwrap_err(),
        Unimplemented::UnsupportedVersion
    );
    assert_eq!(
        frame::reject_unimplemented(ApiKey::DescribeClientQuotas as i16)
            .unwrap_err()
            .code(),
        ResponseError::UnsupportedVersion.code()
    );
    assert_eq!(
        frame::reject_unimplemented(9_999).unwrap_err(),
        Unimplemented::InvalidRequest
    );
    assert_eq!(Unimplemented::InvalidRequest.code(), 42);
    assert_eq!(Unimplemented::UnsupportedVersion.code(), 35);
}

#[test]
fn correlation_id_is_readable_without_knowing_the_key() {
    let body = {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&9999i16.to_be_bytes());
        bytes.extend_from_slice(&0i16.to_be_bytes());
        bytes.extend_from_slice(&77i32.to_be_bytes());
        bytes
    };
    assert_eq!(frame::correlation_id(&body), Some(77));
    assert_eq!(frame::correlation_id(&[0, 1, 2]), None);
}
