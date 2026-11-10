//! Topic, group, and cluster ACLs, evaluated as Kafka's authorizer does.
//!
//! A `DENY` that matches the principal, the client host, and the operation
//! (or `ALL`) wins. A resource that no ACL names at all is allowed only when
//! `allow_if_none` is set, which is Kafka's `allow.everyone.if.no.acl.found`.
//! Otherwise an `ALLOW` must match the principal, the host, and the
//! operation or one that implies it: `READ`, `WRITE`, `DELETE`, and `ALTER`
//! imply `DESCRIBE`, and `ALTER_CONFIGS` implies `DESCRIBE_CONFIGS`. Super
//! users are the broker's decision and never reach this module.

use serde::{Deserialize, Serialize};

use crate::catalog::Outcome;

const ANY: i8 = 1;
const ALL: i8 = 2;
const READ: i8 = 3;
const WRITE: i8 = 4;
const DELETE: i8 = 6;
const ALTER: i8 = 7;
const DESCRIBE: i8 = 8;
const DESCRIBE_CONFIGS: i8 = 10;
const ALTER_CONFIGS: i8 = 11;
const DENY: i8 = 2;
const ALLOW: i8 = 3;
const LITERAL: i8 = 3;
const PREFIXED: i8 = 4;
const MATCH: i8 = 2;
const INVALID: i16 = 42;

/// One stored ACL binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Binding {
    /// 2 topic, 3 group, 4 cluster, 5 transactional id.
    pub resource_type: i8,
    /// Resource name. Literal `*` matches every name of that type.
    pub name: String,
    /// 3 literal, 4 prefixed.
    pub pattern: i8,
    /// `User:name` or `User:*`.
    pub principal: String,
    /// `*` matches every client.
    pub host: String,
    /// Operation this binding grants or denies.
    pub operation: i8,
    /// 2 deny, 3 allow.
    pub permission: i8,
}

/// A describe or delete filter. `None` names and `1` (ANY) match anything.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Filter {
    /// Resource type, or 1 for any.
    pub resource_type: i8,
    /// Resource name, or absent for any.
    pub name: Option<String>,
    /// 1 any, 2 match, 3 literal, 4 prefixed.
    pub pattern: i8,
    /// Principal, or absent for any.
    pub principal: Option<String>,
    /// Host, or absent for any.
    pub host: Option<String>,
    /// Operation, or 1 for any.
    pub operation: i8,
    /// Permission, or 1 for any.
    pub permission: i8,
}

pub(crate) const KEY: &str = "acls";

fn load(raw: Option<String>) -> Result<Vec<Binding>, String> {
    match raw {
        None => Ok(Vec::new()),
        Some(text) => serde_json::from_str(&text).map_err(|err| err.to_string()),
    }
}

fn valid(binding: &Binding) -> bool {
    matches!(binding.resource_type, 2 | 3 | 4 | 5)
        && matches!(binding.pattern, 3 | 4)
        && !binding.name.is_empty()
        && !binding.principal.is_empty()
        && !binding.host.is_empty()
        && binding.operation > ANY
        && matches!(binding.permission, 2 | 3)
}

fn resource_matches(binding: &Binding, resource_type: i8, name: &str) -> bool {
    if binding.resource_type != resource_type {
        return false;
    }
    match binding.pattern {
        LITERAL => binding.name == name || binding.name == "*",
        PREFIXED => name.starts_with(&binding.name),
        _ => false,
    }
}

fn grants(acl_operation: i8, needed: i8) -> bool {
    if acl_operation == ALL || acl_operation == needed {
        return true;
    }
    if needed == DESCRIBE {
        return matches!(acl_operation, READ | WRITE | DELETE | ALTER);
    }
    if needed == DESCRIBE_CONFIGS {
        return acl_operation == ALTER_CONFIGS;
    }
    false
}

fn principal_matches(binding: &Binding, principal: &str) -> bool {
    binding.principal == principal || binding.principal == "User:*"
}

fn host_matches(binding: &Binding, host: &str) -> bool {
    binding.host == "*" || binding.host == host
}

/// Stored ACLs parsed once, for many checks. Parse again when they change.
#[derive(Debug)]
pub struct Compiled {
    /// `None` when the stored ACLs could not be read: every check denies.
    bindings: Option<Vec<Binding>>,
}

impl Compiled {
    /// Parse the stored JSON array, or `None` when nothing has been written.
    pub fn parse(raw: Option<&str>) -> Self {
        Self {
            bindings: load(raw.map(str::to_string)).ok(),
        }
    }

    /// `true` when `principal` may perform `operation` on the resource.
    /// `host` is the client's IP address as text.
    pub fn allows(
        &self,
        principal: &str,
        host: &str,
        resource_type: i8,
        name: &str,
        operation: i8,
        allow_if_none: bool,
    ) -> bool {
        let Some(acls) = &self.bindings else {
            return false;
        };
        let named: Vec<_> = acls
            .iter()
            .filter(|binding| resource_matches(binding, resource_type, name))
            .collect();
        if named.is_empty() {
            return allow_if_none;
        }
        let mine: Vec<_> = named
            .into_iter()
            .filter(|binding| principal_matches(binding, principal) && host_matches(binding, host))
            .collect();
        if mine.iter().any(|binding| {
            binding.permission == DENY
                && (binding.operation == ALL || binding.operation == operation)
        }) {
            return false;
        }
        mine.iter()
            .any(|binding| binding.permission == ALLOW && grants(binding.operation, operation))
    }
}

/// `true` when `principal` may perform `operation` on the resource.
///
/// `raw` is the stored JSON array, or `None` when nothing has been written.
/// `host` is the client's IP address as text.
pub fn allows(
    raw: Option<&str>,
    principal: &str,
    host: &str,
    resource_type: i8,
    name: &str,
    operation: i8,
    allow_if_none: bool,
) -> bool {
    Compiled::parse(raw).allows(
        principal,
        host,
        resource_type,
        name,
        operation,
        allow_if_none,
    )
}

fn same(left: &Binding, right: &Binding) -> bool {
    left == right
}

/// Add bindings. One code per input. Identical bindings are not stored twice.
pub fn create(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    bindings: &[Binding],
) -> Result<Outcome, String> {
    let mut acls = load(get(KEY)?)?;
    let mut codes = Vec::with_capacity(bindings.len());
    for binding in bindings {
        if !valid(binding) {
            codes.push(INVALID);
            continue;
        }
        if !acls.iter().any(|stored| same(stored, binding)) {
            acls.push(binding.clone());
        }
        codes.push(0);
    }
    Ok(Outcome::Change {
        put: vec![(
            KEY.to_string(),
            serde_json::to_string(&acls).map_err(|err| err.to_string())?,
        )],
        del: Vec::new(),
        value: Some(serde_json::to_string(&codes).map_err(|err| err.to_string())?),
    })
}

fn filter_matches(binding: &Binding, filter: &Filter) -> bool {
    if filter.resource_type != ANY && filter.resource_type != binding.resource_type {
        return false;
    }
    if filter.pattern != ANY && filter.pattern != MATCH && filter.pattern != binding.pattern {
        return false;
    }
    if let Some(name) = &filter.name {
        let name_ok = match filter.pattern {
            MATCH => match binding.pattern {
                LITERAL => binding.name == *name || binding.name == "*",
                PREFIXED => name.starts_with(&binding.name),
                _ => false,
            },
            _ => binding.name == *name,
        };
        if !name_ok {
            return false;
        }
    }
    if let Some(principal) = &filter.principal {
        if binding.principal != *principal {
            return false;
        }
    }
    if let Some(host) = &filter.host {
        if binding.host != *host {
            return false;
        }
    }
    if filter.operation != ANY && filter.operation != binding.operation {
        return false;
    }
    if filter.permission != ANY && filter.permission != binding.permission {
        return false;
    }
    true
}

/// Bindings selected by `filter`, in stored order.
pub fn describe(raw: Option<&str>, filter: &Filter) -> Vec<Binding> {
    let Ok(acls) = load(raw.map(str::to_string)) else {
        return Vec::new();
    };
    acls.into_iter()
        .filter(|binding| filter_matches(binding, filter))
        .collect()
}

/// Remove every binding selected by each filter. The value is the removed
/// bindings, one list per filter, in the order they were stored.
pub fn delete(
    get: &dyn Fn(&str) -> Result<Option<String>, String>,
    filters: &[Filter],
) -> Result<Outcome, String> {
    let mut acls = load(get(KEY)?)?;
    let mut removed = Vec::with_capacity(filters.len());
    for filter in filters {
        let mut gone = Vec::new();
        acls.retain(|binding| {
            if filter_matches(binding, filter) {
                gone.push(binding.clone());
                false
            } else {
                true
            }
        });
        removed.push(gone);
    }
    Ok(Outcome::Change {
        put: vec![(
            KEY.to_string(),
            serde_json::to_string(&acls).map_err(|err| err.to_string())?,
        )],
        del: Vec::new(),
        value: Some(serde_json::to_string(&removed).map_err(|err| err.to_string())?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow_write() -> Binding {
        Binding {
            resource_type: 2,
            name: "orders".into(),
            pattern: LITERAL,
            principal: "User:ANONYMOUS".into(),
            host: "*".into(),
            operation: WRITE,
            permission: ALLOW,
        }
    }

    const HOST: &str = "10.0.0.1";
    const CLUSTER_ACTION: i8 = 9;

    fn check(raw: Option<&str>, principal: &str, name: &str, operation: i8) -> bool {
        allows(raw, principal, HOST, 2, name, operation, false)
    }

    #[test]
    fn no_acl_follows_allow_if_none() {
        assert!(allows(
            None,
            "User:ANONYMOUS",
            HOST,
            2,
            "orders",
            WRITE,
            true
        ));
        assert!(!allows(
            None,
            "User:ANONYMOUS",
            HOST,
            2,
            "orders",
            WRITE,
            false
        ));
    }

    #[test]
    fn a_deny_beats_an_allow() {
        let mut stored = vec![allow_write()];
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(check(Some(&raw), "User:ANONYMOUS", "orders", WRITE));
        assert!(check(Some(&raw), "User:ANONYMOUS", "orders", DESCRIBE));
        assert!(!check(Some(&raw), "User:ANONYMOUS", "orders", READ));
        assert!(!check(Some(&raw), "User:ANONYMOUS", "other", READ));
        stored.push(Binding {
            permission: DENY,
            ..allow_write()
        });
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(!check(Some(&raw), "User:ANONYMOUS", "orders", WRITE));
    }

    #[test]
    fn a_deny_does_not_spread_to_implied_operations() {
        let stored = vec![
            Binding {
                operation: ALL,
                ..allow_write()
            },
            Binding {
                operation: READ,
                permission: DENY,
                ..allow_write()
            },
        ];
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(!check(Some(&raw), "User:ANONYMOUS", "orders", READ));
        assert!(check(Some(&raw), "User:ANONYMOUS", "orders", DESCRIBE));
    }

    #[test]
    fn a_host_acl_matches_only_that_client() {
        let stored = vec![Binding {
            host: HOST.into(),
            ..allow_write()
        }];
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(allows(
            Some(&raw),
            "User:ANONYMOUS",
            HOST,
            2,
            "orders",
            WRITE,
            true
        ));
        assert!(!allows(
            Some(&raw),
            "User:ANONYMOUS",
            "10.0.0.2",
            2,
            "orders",
            WRITE,
            true
        ));
    }

    #[test]
    fn implied_describe_operations_follow_kafka() {
        for (granted, needed, allowed) in [
            (READ, DESCRIBE, true),
            (ALTER, DESCRIBE, true),
            (CLUSTER_ACTION, DESCRIBE, false),
            (ALTER_CONFIGS, DESCRIBE_CONFIGS, true),
            (DESCRIBE, DESCRIBE_CONFIGS, false),
        ] {
            let stored = vec![Binding {
                operation: granted,
                ..allow_write()
            }];
            let raw = serde_json::to_string(&stored).unwrap();
            assert_eq!(
                check(Some(&raw), "User:ANONYMOUS", "orders", needed),
                allowed,
                "{granted} for {needed}"
            );
        }
    }

    #[test]
    fn a_prefix_allow_covers_the_name() {
        let stored = vec![Binding {
            name: "ord".into(),
            pattern: PREFIXED,
            ..allow_write()
        }];
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(check(Some(&raw), "User:ANONYMOUS", "orders", WRITE));
        assert!(!check(Some(&raw), "User:bob", "orders", WRITE));
    }
}
