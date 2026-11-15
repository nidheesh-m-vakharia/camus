//! Broker settings. A `camus.toml` file, then `CAMUS_*`, then flags.
//!
//! The names are the Kafka names in `docs/compatibility.md`, plus the
//! Camus-only ones (`durability.mode`, `wal.disk.limit`, the `s3` table).
//! Restarting is how a broker setting changes. `DescribeConfigs` stays
//! read-only.

use std::collections::BTreeMap;
use std::path::Path;

/// Where the value that is in effect came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The compiled default.
    Default,
    /// `camus.toml`.
    File,
    /// A `CAMUS_*` variable.
    Env,
    /// A process flag.
    Flag,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Source::Default => "default",
            Source::File => "file",
            Source::Env => "env",
            Source::Flag => "flag",
        }
    }
}

struct Spec {
    key: &'static str,
    env: &'static str,
    default: &'static str,
}

/// One resolved setting.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Item {
    key: &'static str,
    env: &'static str,
    value: String,
    source: Source,
}

/// The settings this process is running with.
#[derive(Debug, Clone)]
pub struct Settings {
    items: Vec<Item>,
}

const SPECS: &[Spec] = &[
    Spec {
        key: "node.id",
        env: "",
        default: "",
    },
    Spec {
        key: "http.addr",
        env: "",
        default: "",
    },
    Spec {
        key: "data.dir",
        env: "",
        default: "",
    },
    Spec {
        key: "wal.dir",
        env: "",
        default: "",
    },
    Spec {
        key: "kafka.addr",
        env: "",
        default: "",
    },
    Spec {
        key: "advertised.listeners",
        env: "",
        default: "",
    },
    Spec {
        key: "dev.plaintext",
        env: "",
        default: "false",
    },
    Spec {
        key: "allow.non.loopback",
        env: "",
        default: "false",
    },
    Spec {
        key: "require.sasl",
        env: "",
        default: "false",
    },
    Spec {
        key: "allow.everyone.if.no.acl.found",
        env: "CAMUS_ACL_ALLOW_IF_NO_ACL",
        default: "",
    },
    Spec {
        key: "super.users",
        env: "CAMUS_SUPER_USERS",
        default: "",
    },
    Spec {
        key: "auto.create.topics.enable",
        env: "CAMUS_AUTO_CREATE_TOPICS",
        default: "true",
    },
    Spec {
        key: "num.partitions",
        env: "CAMUS_NUM_PARTITIONS",
        default: "1",
    },
    Spec {
        key: "message.max.bytes",
        env: "CAMUS_MESSAGE_MAX_BYTES",
        default: "1048588",
    },
    Spec {
        key: "log.retention.check.interval.ms",
        env: "CAMUS_RETENTION_CHECK_MS",
        default: "300000",
    },
    Spec {
        key: "log.cleaner.backoff.ms",
        env: "CAMUS_COMPACT_MS",
        default: "300000",
    },
    Spec {
        key: "file.delete.delay.ms",
        env: "CAMUS_FILE_DELETE_DELAY_MS",
        default: "60000",
    },
    Spec {
        key: "transaction.max.timeout.ms",
        env: "CAMUS_TXN_MAX_TIMEOUT_MS",
        default: "900000",
    },
    Spec {
        key: "transaction.abort.interval.ms",
        env: "CAMUS_TXN_ABORT_INTERVAL_MS",
        default: "10000",
    },
    Spec {
        key: "transactional.id.expiration.ms",
        env: "CAMUS_TRANSACTIONAL_ID_EXPIRATION_MS",
        default: "604800000",
    },
    Spec {
        key: "transaction.cleanup.interval.ms",
        env: "CAMUS_TXN_CLEANUP_INTERVAL_MS",
        default: "3600000",
    },
    Spec {
        key: "producer.id.expiration.ms",
        env: "CAMUS_PRODUCER_ID_EXPIRATION_MS",
        default: "86400000",
    },
    Spec {
        key: "producer.id.check.interval.ms",
        env: "CAMUS_PRODUCER_ID_CHECK_INTERVAL_MS",
        default: "600000",
    },
    Spec {
        key: "offsets.retention.minutes",
        env: "CAMUS_OFFSETS_RETENTION_MINUTES",
        default: "10080",
    },
    Spec {
        key: "offsets.retention.ms",
        env: "CAMUS_OFFSETS_RETENTION_MS",
        default: "",
    },
    Spec {
        key: "offsets.retention.check.interval.ms",
        env: "CAMUS_OFFSETS_RETENTION_CHECK_INTERVAL_MS",
        default: "600000",
    },
    Spec {
        key: "group.initial.rebalance.delay.ms",
        env: "CAMUS_GROUP_INITIAL_DELAY_MS",
        default: "3000",
    },
    Spec {
        key: "group.min.session.timeout.ms",
        env: "CAMUS_GROUP_MIN_SESSION_MS",
        default: "6000",
    },
    Spec {
        key: "group.consumer.session.timeout.ms",
        env: "CAMUS_CONSUMER_SESSION_MS",
        default: "45000",
    },
    Spec {
        key: "group.consumer.heartbeat.interval.ms",
        env: "CAMUS_CONSUMER_HEARTBEAT_MS",
        default: "5000",
    },
    Spec {
        key: "auto.leader.rebalance.enable",
        env: "CAMUS_AUTO_LEADER_REBALANCE",
        default: "true",
    },
    Spec {
        key: "leader.imbalance.check.interval.ms",
        env: "CAMUS_LEADER_IMBALANCE_CHECK_MS",
        default: "300000",
    },
    Spec {
        key: "broker.rack",
        env: "CAMUS_RACK",
        default: "",
    },
    Spec {
        key: "durability.mode",
        env: "CAMUS_DEFAULT_DURABILITY",
        default: "wal",
    },
    Spec {
        key: "wal.disk.limit",
        env: "CAMUS_WAL_DISK_LIMIT",
        default: "8589934592",
    },
    Spec {
        key: "wal.shards",
        env: "CAMUS_WAL_SHARDS",
        default: "",
    },
    Spec {
        key: "log.flush.interval.ms",
        env: "CAMUS_FLUSH_MS",
        default: "250",
    },
    Spec {
        key: "log.quiet.flush.ms",
        env: "CAMUS_QUIET_FLUSH_MS",
        default: "30000",
    },
    Spec {
        key: "log.sweep.interval.ms",
        env: "CAMUS_SWEEP_MS",
        default: "300000",
    },
    Spec {
        key: "log.sweep.min.age.ms",
        env: "CAMUS_SWEEP_MIN_AGE_MS",
        default: "3600000",
    },
    Spec {
        key: "read.cache.bytes",
        env: "CAMUS_READ_CACHE_BYTES",
        default: "67108864",
    },
    Spec {
        key: "read.disk.cache.bytes",
        env: "CAMUS_READ_DISK_CACHE_BYTES",
        default: "4294967296",
    },
    Spec {
        key: "read.disk.cache.entries",
        env: "CAMUS_READ_DISK_CACHE_ENTRIES",
        default: "65536",
    },
    Spec {
        key: "sasl.username",
        env: "CAMUS_SASL_USER",
        default: "",
    },
    Spec {
        key: "sasl.password",
        env: "CAMUS_SASL_PASSWORD",
        default: "",
    },
    Spec {
        key: "s3.endpoint",
        env: "CAMUS_S3_ENDPOINT",
        default: "http://127.0.0.1:9100",
    },
    Spec {
        key: "s3.bucket",
        env: "CAMUS_S3_BUCKET",
        default: "camus",
    },
    Spec {
        key: "s3.region",
        env: "CAMUS_S3_REGION",
        default: "us-east-1",
    },
    Spec {
        key: "s3.access_key",
        env: "CAMUS_S3_ACCESS_KEY",
        default: "camus",
    },
    Spec {
        key: "s3.secret_key",
        env: "CAMUS_S3_SECRET_KEY",
        default: "camus-dev-secret",
    },
    Spec {
        key: "s3.prefix",
        env: "CAMUS_S3_PREFIX",
        default: "",
    },
    Spec {
        key: "s3.virtual_host",
        env: "CAMUS_S3_VIRTUAL_HOST",
        default: "false",
    },
];

impl Settings {
    /// Read `--config`, or `camus.toml` in the working directory when that
    /// file exists. `flags` already won: they are applied last.
    pub fn load(config: Option<&Path>, flags: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let path = match config {
            Some(path) => Some(path.to_path_buf()),
            None => {
                let found = Path::new("camus.toml");
                if found.is_file() {
                    Some(found.to_path_buf())
                } else {
                    None
                }
            }
        };
        let file = match path {
            Some(path) => flatten_file(&path)?,
            None => BTreeMap::new(),
        };
        let mut env = BTreeMap::new();
        for spec in SPECS {
            if spec.env.is_empty() {
                continue;
            }
            if let Ok(value) = std::env::var(spec.env) {
                env.insert(spec.key.to_string(), value);
            }
        }
        Ok(Self::resolve(&file, &env, flags))
    }

    /// `flags` override `env`, which overrides `file`, which overrides the default.
    pub fn resolve(
        file: &BTreeMap<String, String>,
        env: &BTreeMap<String, String>,
        flags: &BTreeMap<String, String>,
    ) -> Self {
        let items = SPECS
            .iter()
            .map(|spec| {
                let (value, source) = if let Some(value) = flags.get(spec.key) {
                    (value.clone(), Source::Flag)
                } else if let Some(value) = env.get(spec.key) {
                    (value.clone(), Source::Env)
                } else if let Some(value) = file.get(spec.key) {
                    (value.clone(), Source::File)
                } else {
                    (spec.default.to_string(), Source::Default)
                };
                Item {
                    key: spec.key,
                    env: spec.env,
                    value,
                    source,
                }
            })
            .collect();
        Self { items }
    }

    fn item(&self, key: &str) -> Option<&Item> {
        self.items.iter().find(|item| item.key == key)
    }

    /// The effective value. Empty when the name is unknown.
    pub fn get(&self, key: &str) -> &str {
        self.item(key).map(|item| item.value.as_str()).unwrap_or("")
    }

    /// Where `key` came from.
    pub fn source_of(&self, key: &str) -> Source {
        self.item(key)
            .map(|item| item.source)
            .unwrap_or(Source::Default)
    }

    /// Replace a value after load. Used for the ACL default, which depends
    /// on the listener.
    pub fn set(&mut self, key: &str, value: impl Into<String>, source: Source) {
        if let Some(item) = self.items.iter_mut().find(|item| item.key == key) {
            item.value = value.into();
            item.source = source;
        }
    }

    /// The truthy strings the `CAMUS_*` flags already accept.
    pub fn is_true(&self, key: &str) -> bool {
        matches!(self.get(key), "1" | "true" | "TRUE" | "yes" | "YES")
    }

    /// A parsed integer, when the value is one.
    pub fn u64(&self, key: &str) -> Option<u64> {
        let value = self.get(key);
        if value.is_empty() {
            return None;
        }
        value.parse().ok()
    }

    /// The `CAMUS_*` name for a setting, when it has one.
    pub fn env_name(&self, key: &str) -> &str {
        self.item(key).map(|item| item.env).unwrap_or("")
    }

    /// `key=value source` for each setting. Secrets print as `(set)`.
    pub fn report_lines(&self) -> Vec<String> {
        self.items
            .iter()
            .map(|item| {
                let shown = if secret(item.key) && item.source != Source::Default {
                    if item.value.is_empty() {
                        "(unset)".to_string()
                    } else {
                        "(set)".to_string()
                    }
                } else {
                    item.value.clone()
                };
                format!("{}={} {}", item.key, shown, item.source.as_str())
            })
            .collect()
    }
}

fn secret(key: &str) -> bool {
    key.ends_with("secret_key") || key.ends_with("password")
}

fn flatten_file(path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(path)?;
    let value: toml::Value = toml::from_str(&text)
        .map_err(|err| anyhow::anyhow!("reading {}: {err}", path.display()))?;
    let mut out = BTreeMap::new();
    flatten("", &value, &mut out);
    Ok(out)
}

fn flatten(prefix: &str, value: &toml::Value, out: &mut BTreeMap<String, String>) {
    match value {
        toml::Value::Table(table) => {
            for (key, child) in table {
                let name = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(&name, child, out);
            }
        }
        toml::Value::String(text) => {
            out.insert(prefix.to_string(), text.clone());
        }
        toml::Value::Integer(number) => {
            out.insert(prefix.to_string(), number.to_string());
        }
        toml::Value::Boolean(flag) => {
            out.insert(prefix.to_string(), flag.to_string());
        }
        toml::Value::Float(number) => {
            out.insert(prefix.to_string(), number.to_string());
        }
        toml::Value::Array(_) | toml::Value::Datetime(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_keys_and_the_s3_table_flatten() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        std::fs::write(
            file.path(),
            "num.partitions = 3\ndurability.mode = \"r2\"\nwal.disk.limit = 1024\n\n[s3]\nendpoint = \"http://minio:9000\"\n",
        )?;
        let map = flatten_file(file.path())?;
        assert_eq!(map.get("num.partitions").map(String::as_str), Some("3"));
        assert_eq!(map.get("durability.mode").map(String::as_str), Some("r2"));
        assert_eq!(map.get("wal.disk.limit").map(String::as_str), Some("1024"));
        assert_eq!(
            map.get("s3.endpoint").map(String::as_str),
            Some("http://minio:9000")
        );
        Ok(())
    }

    #[test]
    fn a_flag_beats_the_environment_and_the_file() {
        let mut file = BTreeMap::new();
        file.insert("num.partitions".to_string(), "2".to_string());
        file.insert("durability.mode".to_string(), "r2".to_string());
        let mut env = BTreeMap::new();
        env.insert("num.partitions".to_string(), "4".to_string());
        env.insert("broker.rack".to_string(), "a".to_string());
        let mut flags = BTreeMap::new();
        flags.insert("num.partitions".to_string(), "8".to_string());
        flags.insert("sasl.password".to_string(), "s3cret".to_string());
        let settings = Settings::resolve(&file, &env, &flags);
        assert_eq!(settings.get("num.partitions"), "8");
        assert_eq!(settings.source_of("num.partitions"), Source::Flag);
        assert_eq!(settings.get("durability.mode"), "r2");
        assert_eq!(settings.source_of("durability.mode"), Source::File);
        assert_eq!(settings.get("broker.rack"), "a");
        assert_eq!(settings.source_of("broker.rack"), Source::Env);
        assert_eq!(settings.get("wal.disk.limit"), "8589934592");
        assert_eq!(settings.source_of("wal.disk.limit"), Source::Default);
        let lines = settings.report_lines();
        assert!(lines.iter().any(|line| line == "num.partitions=8 flag"));
        assert!(lines.iter().any(|line| line == "sasl.password=(set) flag"));
        assert!(lines
            .iter()
            .any(|line| line == "s3.secret_key=camus-dev-secret default"));
    }
}
