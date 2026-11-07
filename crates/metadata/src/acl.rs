//! Topic, group, and cluster ACLs.
//!
//! A resource with no matching ACL is allowed. That is Kafka's
//! `allow.everyone.if.no.acl.found`. Once any ACL names the resource, a
//! matching `DENY` wins, then a matching `ALLOW`, and anything else is
//! denied. Hosts other than `*` are stored but do not match; the broker
//! does not know the client address.

use serde::{Deserialize, Serialize};

use crate::catalog::Outcome;

const ANY: i8 = 1;
const ALL: i8 = 2;
const READ: i8 = 3;
const WRITE: i8 = 4;
const DELETE: i8 = 6;
const ALTER: i8 = 7;
const DESCRIBE: i8 = 8;
const CLUSTER_ACTION: i8 = 9;
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

const KEY: &str = "acls";

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
        return matches!(
            acl_operation,
            READ | WRITE | DELETE | ALTER | CLUSTER_ACTION
        );
    }
    if needed == DESCRIBE_CONFIGS {
        return matches!(acl_operation, DESCRIBE | ALTER_CONFIGS);
    }
    false
}

fn principal_matches(binding: &Binding, principal: &str) -> bool {
    binding.principal == principal || binding.principal == "User:*"
}

/// `true` when `principal` may perform `operation` on the resource.
///
/// `raw` is the stored JSON array, or `None` when nothing has been written.
pub fn allows(
    raw: Option<&str>,
    principal: &str,
    resource_type: i8,
    name: &str,
    operation: i8,
) -> bool {
    let Ok(acls) = load(raw.map(str::to_string)) else {
        return false;
    };
    let matched: Vec<_> = acls
        .iter()
        .filter(|binding| resource_matches(binding, resource_type, name) && binding.host == "*")
        .collect();
    if matched.is_empty() {
        return true;
    }
    let mine: Vec<_> = matched
        .into_iter()
        .filter(|binding| principal_matches(binding, principal))
        .collect();
    if mine
        .iter()
        .any(|binding| binding.permission == DENY && grants(binding.operation, operation))
    {
        return false;
    }
    mine.iter()
        .any(|binding| binding.permission == ALLOW && grants(binding.operation, operation))
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

    #[test]
    fn no_acl_allows_and_a_deny_beats_an_allow() {
        assert!(allows(None, "User:ANONYMOUS", 2, "orders", WRITE));
        let mut stored = vec![allow_write()];
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(allows(Some(&raw), "User:ANONYMOUS", 2, "orders", WRITE));
        assert!(allows(Some(&raw), "User:ANONYMOUS", 2, "orders", DESCRIBE));
        assert!(!allows(Some(&raw), "User:ANONYMOUS", 2, "orders", READ));
        assert!(allows(Some(&raw), "User:ANONYMOUS", 2, "other", READ));
        stored.push(Binding {
            permission: DENY,
            ..allow_write()
        });
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(!allows(Some(&raw), "User:ANONYMOUS", 2, "orders", WRITE));
    }

    #[test]
    fn a_prefix_allow_covers_the_name() {
        let stored = vec![Binding {
            name: "ord".into(),
            pattern: PREFIXED,
            ..allow_write()
        }];
        let raw = serde_json::to_string(&stored).unwrap();
        assert!(allows(Some(&raw), "User:ANONYMOUS", 2, "orders", WRITE));
        assert!(!allows(Some(&raw), "User:bob", 2, "orders", WRITE));
    }
}
