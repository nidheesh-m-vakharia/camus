//! Topic configs: the names Camus accepts, their Kafka 4.4.0 defaults, and
//! how a value is checked.
//!
//! A config is either applied by Camus or accepted with no effect, because the
//! Kafka behavior it tunes does not exist here (segments, local flushes,
//! follower replicas). Accepted configs are stored and returned by
//! `DescribeConfigs`, so tools that set them keep working. Any other name, and
//! any value Camus cannot honor, is `INVALID_CONFIG` (40).

use crate::catalog::Topic;

/// Kafka `INVALID_CONFIG`.
pub const INVALID_CONFIG: i16 = 40;

pub use camus_protocol::batch::MAX_BATCH_CEILING;

/// Kafka's `message.max.bytes` default: 1 MiB plus the log overhead.
pub const DEFAULT_MESSAGE_MAX_BYTES: i64 = 1_048_588;

/// Whether Camus acts on a config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// The value changes broker behavior.
    Applied,
    /// Stored and described, with no effect.
    Accepted,
}

/// How a value is parsed.
#[derive(Debug, Clone, Copy)]
pub enum Kind {
    /// 64-bit integer at or above `min`.
    Long { min: i64 },
    /// 32-bit integer at or above `min`, at most `max`.
    Int { min: i64, max: i64 },
    /// `true` or `false`.
    Bool,
    /// A number from 0 to 1.
    Ratio,
    /// `delete`, `compact`, or both, comma separated.
    Policy,
    /// One of these exact values.
    Only(&'static [&'static str]),
    /// Any text. Used for list-valued configs Camus does not read.
    Text,
}

/// One accepted topic config.
#[derive(Debug, Clone, Copy)]
pub struct Spec {
    /// Kafka config name.
    pub name: &'static str,
    /// Kafka 4.4.0 default.
    pub default: &'static str,
    /// Value check.
    pub kind: Kind,
    /// Applied or accepted.
    pub effect: Effect,
    /// Set only when the topic is created.
    pub create_only: bool,
}

impl Spec {
    /// Kafka's `ConfigType` code for `DescribeConfigs` v3+.
    pub fn config_type(&self) -> i8 {
        match self.kind {
            Kind::Bool => 1,
            Kind::Int { .. } => 3,
            Kind::Long { .. } => 5,
            Kind::Ratio => 6,
            Kind::Policy | Kind::Text => 7,
            Kind::Only(values) if values == ["false"] => 1,
            Kind::Only(_) => 2,
        }
    }
}

const fn applied(name: &'static str, default: &'static str, kind: Kind) -> Spec {
    Spec {
        name,
        default,
        kind,
        effect: Effect::Applied,
        create_only: false,
    }
}

const fn accepted(name: &'static str, default: &'static str, kind: Kind) -> Spec {
    Spec {
        name,
        default,
        kind,
        effect: Effect::Accepted,
        create_only: false,
    }
}

const I32: i64 = i32::MAX as i64;

/// Every topic config Camus accepts, in `DescribeConfigs` order.
pub const SPECS: &[Spec] = &[
    applied("cleanup.policy", "delete", Kind::Policy),
    applied("retention.ms", "604800000", Kind::Long { min: -1 }),
    applied("retention.bytes", "-1", Kind::Long { min: -1 }),
    applied(
        "max.message.bytes",
        "1048588",
        Kind::Int {
            min: 0,
            max: MAX_BATCH_CEILING as i64,
        },
    ),
    applied("delete.retention.ms", "86400000", Kind::Long { min: 0 }),
    applied("min.compaction.lag.ms", "0", Kind::Long { min: 0 }),
    Spec {
        name: "durability.mode",
        default: "wal",
        kind: Kind::Only(&["wal", "r2"]),
        effect: Effect::Applied,
        create_only: true,
    },
    accepted("compression.type", "producer", Kind::Only(&["producer"])),
    accepted("file.delete.delay.ms", "60000", Kind::Long { min: 0 }),
    accepted(
        "flush.messages",
        "9223372036854775807",
        Kind::Long { min: 1 },
    ),
    accepted("flush.ms", "9223372036854775807", Kind::Long { min: 0 }),
    accepted("follower.replication.throttled.replicas", "", Kind::Text),
    accepted(
        "index.interval.bytes",
        "4096",
        Kind::Int { min: 0, max: I32 },
    ),
    accepted("leader.replication.throttled.replicas", "", Kind::Text),
    accepted("local.retention.bytes", "-2", Kind::Long { min: -2 }),
    accepted("local.retention.ms", "-2", Kind::Long { min: -2 }),
    accepted(
        "max.compaction.lag.ms",
        "9223372036854775807",
        Kind::Long { min: 1 },
    ),
    accepted(
        "message.timestamp.after.max.ms",
        "3600000",
        Kind::Long { min: 0 },
    ),
    accepted(
        "message.timestamp.before.max.ms",
        "9223372036854775807",
        Kind::Long { min: 0 },
    ),
    accepted(
        "message.timestamp.type",
        "CreateTime",
        Kind::Only(&["CreateTime"]),
    ),
    accepted("min.cleanable.dirty.ratio", "0.5", Kind::Ratio),
    accepted("min.insync.replicas", "1", Kind::Int { min: 1, max: I32 }),
    accepted("preallocate", "false", Kind::Bool),
    accepted("remote.storage.enable", "false", Kind::Only(&["false"])),
    accepted(
        "segment.bytes",
        "1073741824",
        Kind::Int { min: 14, max: I32 },
    ),
    accepted(
        "segment.index.bytes",
        "10485760",
        Kind::Int { min: 4, max: I32 },
    ),
    accepted("segment.jitter.ms", "0", Kind::Long { min: 0 }),
    accepted("segment.ms", "604800000", Kind::Long { min: 1 }),
    accepted(
        "unclean.leader.election.enable",
        "false",
        Kind::Only(&["false"]),
    ),
];

/// The spec for `name`, if Camus accepts it.
pub fn spec(name: &str) -> Option<&'static Spec> {
    SPECS.iter().find(|spec| spec.name == name)
}

/// A rejected config, with the message Kafka clients show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    /// Kafka error code.
    pub code: i16,
    /// Human-readable reason.
    pub message: String,
}

fn reject(message: String) -> Rejected {
    Rejected {
        code: INVALID_CONFIG,
        message,
    }
}

/// Check `value` for `spec` and return it in stored form.
pub fn check(spec: &Spec, value: &str) -> Result<String, Rejected> {
    let bad = |why: &str| {
        reject(format!(
            "Invalid value {value} for configuration {}: {why}",
            spec.name
        ))
    };
    match spec.kind {
        Kind::Long { min } => {
            let parsed: i64 = value.trim().parse().map_err(|_| bad("not a number"))?;
            if parsed < min {
                return Err(bad(&format!("value must be at least {min}")));
            }
            Ok(parsed.to_string())
        }
        Kind::Int { min, max } => {
            let parsed: i64 = value.trim().parse().map_err(|_| bad("not a number"))?;
            if parsed < min {
                return Err(bad(&format!("value must be at least {min}")));
            }
            if parsed > max {
                return Err(bad(&format!("value must be at most {max}")));
            }
            Ok(parsed.to_string())
        }
        Kind::Bool => match value.trim().to_ascii_lowercase().as_str() {
            "true" => Ok("true".into()),
            "false" => Ok("false".into()),
            _ => Err(bad("expected true or false")),
        },
        Kind::Ratio => {
            let parsed: f64 = value.trim().parse().map_err(|_| bad("not a number"))?;
            if !(0.0..=1.0).contains(&parsed) {
                return Err(bad("value must be between 0 and 1"));
            }
            Ok(value.trim().to_string())
        }
        Kind::Policy => normalize_policy(value)
            .ok_or_else(|| bad("expected delete, compact, or delete,compact")),
        Kind::Only(values) => {
            let trimmed = value.trim();
            let matched = values.iter().find(|allowed| {
                if **allowed == "false" {
                    trimmed.eq_ignore_ascii_case(allowed)
                } else {
                    trimmed == **allowed
                }
            });
            match matched {
                Some(allowed) => Ok((*allowed).to_string()),
                None => Err(bad(&format!("Camus supports only {}", values.join(", ")))),
            }
        }
        Kind::Text => Ok(value.to_string()),
    }
}

fn normalize_policy(value: &str) -> Option<String> {
    let mut delete = false;
    let mut compact = false;
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        match part {
            "delete" => delete = true,
            "compact" => compact = true,
            _ => return None,
        }
    }
    match (delete, compact) {
        (true, true) => Some("delete,compact".into()),
        (true, false) => Some("delete".into()),
        (false, true) => Some("compact".into()),
        (false, false) => None,
    }
}

/// Kafka `IncrementalAlterConfigs` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Replace the value.
    Set,
    /// Return to the default.
    Delete,
    /// Add list items.
    Append,
    /// Remove list items.
    Subtract,
}

impl Op {
    /// Kafka's wire code: 0 set, 1 delete, 2 append, 3 subtract.
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Set),
            1 => Some(Self::Delete),
            2 => Some(Self::Append),
            3 => Some(Self::Subtract),
            _ => None,
        }
    }
}

/// Apply one edit to `topic`. `creating` allows create-only configs.
pub fn edit(
    topic: &mut Topic,
    name: &str,
    op: Op,
    value: Option<&str>,
    creating: bool,
) -> Result<(), Rejected> {
    let Some(spec) = spec(name) else {
        return Err(reject(format!("Unknown topic config name: {name}")));
    };
    if spec.create_only && !creating {
        return Err(reject(format!(
            "{name} can only be set when the topic is created"
        )));
    }
    let next = match op {
        Op::Set => Some(check(spec, value.unwrap_or(""))?),
        Op::Delete => None,
        Op::Append | Op::Subtract => {
            if !matches!(spec.kind, Kind::Policy | Kind::Text) {
                return Err(reject(format!(
                    "Config {name} is not a list, so it cannot be appended to or subtracted from"
                )));
            }
            let joined = join_list(&topic.config(name), value.unwrap_or(""), op == Op::Append);
            Some(check(spec, &joined)?)
        }
    };
    topic.set_config(name, next);
    Ok(())
}

fn join_list(current: &str, extra: &str, append: bool) -> String {
    let mut parts: Vec<&str> = current
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    for part in extra
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if append {
            if !parts.contains(&part) {
                parts.push(part);
            }
        } else {
            parts.retain(|have| *have != part);
        }
    }
    parts.join(",")
}

/// Apply AlterConfigs edits to a copy of `topic`. Nothing changes unless every
/// edit is valid. With `replace`, as in legacy `AlterConfigs`, every config
/// not named in `edits` returns to its default first.
pub fn apply_change(
    topic: &Topic,
    edits: &[crate::admin::ConfigEdit],
    replace: bool,
) -> Result<Topic, Rejected> {
    let mut next = topic.clone();
    if replace {
        let names: Vec<String> = next
            .overrides
            .keys()
            .filter(|name| spec(name).is_some_and(|spec| !spec.create_only))
            .cloned()
            .collect();
        for name in names {
            next.set_config(&name, None);
        }
    }
    for change in edits {
        let op = Op::from_code(change.op)
            .ok_or_else(|| reject(format!("Unknown config operation {}", change.op)))?;
        edit(&mut next, &change.name, op, change.value.as_deref(), false)?;
    }
    Ok(next)
}

/// Check that a full config set is valid for a new topic, without a topic.
pub fn check_create(configs: &[(String, Option<String>)]) -> Result<(), Rejected> {
    let mut topic = Topic::new("", "", 1, "wal");
    for (name, value) in configs {
        edit(&mut topic, name, Op::Set, value.as_deref(), true)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic() -> Topic {
        Topic::new("t", "id", 1, "wal")
    }

    #[test]
    fn every_default_passes_its_own_check() {
        for spec in SPECS {
            if !spec.default.is_empty() || matches!(spec.kind, Kind::Text) {
                assert!(check(spec, spec.default).is_ok(), "{}", spec.name);
            }
        }
    }

    #[test]
    fn set_then_delete_returns_to_the_default() {
        let mut topic = topic();
        edit(&mut topic, "retention.ms", Op::Set, Some("60000"), false).unwrap();
        assert_eq!(topic.retention_ms, 60_000);
        assert!(topic.is_set("retention.ms"));
        edit(&mut topic, "retention.ms", Op::Delete, None, false).unwrap();
        assert_eq!(topic.retention_ms, 604_800_000);
        assert!(!topic.is_set("retention.ms"));
    }

    #[test]
    fn unknown_names_and_unsupported_values_are_rejected() {
        let mut topic = topic();
        let unknown = edit(&mut topic, "no.such.config", Op::Set, Some("1"), false);
        assert_eq!(unknown.unwrap_err().code, INVALID_CONFIG);
        for (name, value) in [
            ("message.timestamp.type", "LogAppendTime"),
            ("unclean.leader.election.enable", "true"),
            ("compression.type", "zstd"),
            ("retention.ms", "-2"),
            ("max.message.bytes", "9000000"),
            ("cleanup.policy", "archive"),
        ] {
            assert!(
                edit(&mut topic, name, Op::Set, Some(value), false).is_err(),
                "{name}={value}"
            );
        }
    }

    #[test]
    fn accepted_configs_are_stored_and_described() {
        let mut topic = topic();
        edit(&mut topic, "segment.bytes", Op::Set, Some("1048576"), false).unwrap();
        edit(&mut topic, "min.insync.replicas", Op::Set, Some("2"), false).unwrap();
        assert_eq!(topic.config("segment.bytes"), "1048576");
        assert_eq!(topic.config("min.insync.replicas"), "2");
        assert_eq!(topic.config("segment.ms"), "604800000");
    }

    #[test]
    fn durability_is_create_only() {
        let mut topic = topic();
        assert!(edit(&mut topic, "durability.mode", Op::Set, Some("r2"), false).is_err());
        edit(&mut topic, "durability.mode", Op::Set, Some("r2"), true).unwrap();
        assert_eq!(topic.durability, "r2");
    }

    #[test]
    fn policy_append_and_subtract() {
        let mut topic = topic();
        edit(
            &mut topic,
            "cleanup.policy",
            Op::Append,
            Some("compact"),
            false,
        )
        .unwrap();
        assert_eq!(topic.cleanup_policy, "delete,compact");
        edit(
            &mut topic,
            "cleanup.policy",
            Op::Subtract,
            Some("delete"),
            false,
        )
        .unwrap();
        assert_eq!(topic.cleanup_policy, "compact");
        assert!(edit(
            &mut topic,
            "cleanup.policy",
            Op::Subtract,
            Some("compact"),
            false
        )
        .is_err());
        assert!(edit(&mut topic, "retention.ms", Op::Append, Some("1"), false).is_err());
    }

    #[test]
    fn create_checks_the_whole_set() {
        let ok = vec![
            ("cleanup.policy".to_string(), Some("compact".to_string())),
            ("segment.bytes".to_string(), Some("1048576".to_string())),
        ];
        assert!(check_create(&ok).is_ok());
        let bad = vec![("segment.bytes".to_string(), Some("1".to_string()))];
        assert!(check_create(&bad).is_err());
    }
}
