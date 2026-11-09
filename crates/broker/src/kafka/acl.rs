//! Create, describe, and delete ACLs. The metadata leader stores them.
//! Creating or deleting one requires `ALTER` on the cluster. Describing
//! requires `DESCRIBE`. With no cluster ACL, both are allowed.

use std::collections::BTreeMap;

use bytes::Bytes;
use camus_metadata::{AclBinding, AclFilter, Command};
use camus_protocol::messages::delete_acls_response::{
    DeleteAclsFilterResult, DeleteAclsMatchingAcl,
};
use camus_protocol::messages::describe_acls_response::{AclDescription, DescribeAclsResource};
use camus_protocol::messages::{
    CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse, RequestHeader,
};
use camus_protocol::protocol::{Decodable, StrBytes};

use super::{frame_of, Caller, Kafka, Reply};

const NOT_LEADER: i16 = 6;
const CLUSTER_AUTH: i16 = 31;
const CLUSTER: i8 = 4;
const ALTER: i8 = 7;
const DESCRIBE: i8 = 8;

impl Kafka {
    pub(super) async fn create_acls(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = CreateAclsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = CreateAclsResponse::default();
        if !self.allows(caller, CLUSTER, "kafka-cluster", ALTER) {
            response.results = request
                .creations
                .iter()
                .map(|_| acl_result(CLUSTER_AUTH))
                .collect();
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let bindings = request
            .creations
            .iter()
            .map(|creation| AclBinding {
                resource_type: creation.resource_type,
                name: creation.resource_name.to_string(),
                pattern: creation.resource_pattern_type,
                principal: creation.principal.to_string(),
                host: creation.host.to_string(),
                operation: creation.operation,
                permission: creation.permission_type,
            })
            .collect();
        let applied = self.meta_apply(Command::CreateAcls { bindings }).await;
        let codes = match applied {
            Ok(result) if result.error_code == 0 => result
                .value
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Vec<i16>>(raw).ok())
                .unwrap_or_default(),
            _ => vec![NOT_LEADER; request.creations.len()],
        };
        let codes = if codes.len() == request.creations.len() {
            codes
        } else {
            vec![NOT_LEADER; request.creations.len()]
        };
        response.results = codes.into_iter().map(acl_result).collect();
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn describe_acls(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DescribeAclsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = DescribeAclsResponse::default();
        if !self.allows(caller, CLUSTER, "kafka-cluster", DESCRIBE) {
            response.error_code = CLUSTER_AUTH;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        if !self.leader() {
            response.error_code = NOT_LEADER;
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let raw = self.node.read_local("acls").ok().flatten();
        let found = camus_metadata::acl_describe(raw.as_deref(), &filter_of(&request));
        let mut grouped: BTreeMap<(i8, String, i8), Vec<AclBinding>> = BTreeMap::new();
        for binding in found {
            grouped
                .entry((binding.resource_type, binding.name.clone(), binding.pattern))
                .or_default()
                .push(binding);
        }
        for ((resource_type, name, pattern), bindings) in grouped {
            let mut resource = DescribeAclsResource::default();
            resource.resource_type = resource_type;
            resource.resource_name = StrBytes::from_string(name);
            resource.pattern_type = pattern;
            resource.acls = bindings
                .into_iter()
                .map(|binding| {
                    let mut acl = AclDescription::default();
                    acl.principal = StrBytes::from_string(binding.principal);
                    acl.host = StrBytes::from_string(binding.host);
                    acl.operation = binding.operation;
                    acl.permission_type = binding.permission;
                    acl
                })
                .collect();
            response.resources.push(resource);
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }

    pub(super) async fn delete_acls(
        &self,
        header: &RequestHeader,
        buf: &mut Bytes,
        caller: &Caller,
    ) -> Reply {
        let Ok(request) = DeleteAclsRequest::decode(buf, header.request_api_version) else {
            return Reply::Close;
        };
        let mut response = DeleteAclsResponse::default();
        if !self.allows(caller, CLUSTER, "kafka-cluster", ALTER) {
            response.filter_results = request
                .filters
                .iter()
                .map(|_| filter_result(CLUSTER_AUTH, Vec::new()))
                .collect();
            return frame_of(header.request_api_version, header.correlation_id, &response);
        }
        let filters = request.filters.iter().map(delete_filter).collect();
        let applied = self.meta_apply(Command::DeleteAcls { filters }).await;
        let removed = match applied {
            Ok(result) if result.error_code == 0 => result
                .value
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Vec<Vec<AclBinding>>>(raw).ok())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        if removed.len() != request.filters.len() {
            response.filter_results = request
                .filters
                .iter()
                .map(|_| filter_result(NOT_LEADER, Vec::new()))
                .collect();
        } else {
            response.filter_results = removed
                .into_iter()
                .map(|bindings| filter_result(0, bindings))
                .collect();
        }
        frame_of(header.request_api_version, header.correlation_id, &response)
    }
}

fn acl_result(code: i16) -> camus_protocol::messages::create_acls_response::AclCreationResult {
    let mut result = camus_protocol::messages::create_acls_response::AclCreationResult::default();
    result.error_code = code;
    result
}

fn filter_of(request: &DescribeAclsRequest) -> AclFilter {
    AclFilter {
        resource_type: request.resource_type_filter,
        name: request
            .resource_name_filter
            .as_ref()
            .map(|value| value.to_string()),
        pattern: request.pattern_type_filter,
        principal: request
            .principal_filter
            .as_ref()
            .map(|value| value.to_string()),
        host: request.host_filter.as_ref().map(|value| value.to_string()),
        operation: request.operation,
        permission: request.permission_type,
    }
}

fn delete_filter(
    filter: &camus_protocol::messages::delete_acls_request::DeleteAclsFilter,
) -> AclFilter {
    AclFilter {
        resource_type: filter.resource_type_filter,
        name: filter
            .resource_name_filter
            .as_ref()
            .map(|value| value.to_string()),
        pattern: filter.pattern_type_filter,
        principal: filter
            .principal_filter
            .as_ref()
            .map(|value| value.to_string()),
        host: filter.host_filter.as_ref().map(|value| value.to_string()),
        operation: filter.operation,
        permission: filter.permission_type,
    }
}

fn filter_result(code: i16, bindings: Vec<AclBinding>) -> DeleteAclsFilterResult {
    let mut result = DeleteAclsFilterResult::default();
    result.error_code = code;
    result.matching_acls = bindings
        .into_iter()
        .map(|binding| {
            let mut acl = DeleteAclsMatchingAcl::default();
            acl.resource_type = binding.resource_type;
            acl.resource_name = StrBytes::from_string(binding.name);
            acl.pattern_type = binding.pattern;
            acl.principal = StrBytes::from_string(binding.principal);
            acl.host = StrBytes::from_string(binding.host);
            acl.operation = binding.operation;
            acl.permission_type = binding.permission;
            acl
        })
        .collect();
    result
}
