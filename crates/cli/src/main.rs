//! `camus` admin binary. Each command calls one broker's HTTP admin route.
//! Produce, consume, topics, groups, and ACLs stay on Kafka's own tools.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{CommandFactory, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "camus", version)]
struct Cli {
    #[command(subcommand)]
    command: Top,
}

#[derive(Subcommand)]
enum Top {
    /// Cluster operations Kafka's tools do not cover.
    #[command(subcommand)]
    Admin(Admin),
    /// Print a completion script for bash, zsh, fish, or another shell clap knows.
    Completions {
        /// The shell to print a script for.
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand)]
enum Admin {
    /// Form the Raft group from an explicit peer list. A second run against
    /// a group that already exists succeeds and changes nothing.
    Init {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// `id=host:port` for one broker. Repeat for each peer.
        #[arg(long = "peer", required = true)]
        peers: Vec<String>,
        /// WAL shard count. Omitted, the broker chooses from its core count.
        #[arg(long)]
        shards: Option<u32>,
    },
    /// Health, the metadata leader, and each WAL shard.
    Status {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
    },
    /// Effective broker settings and which source set each one, or one
    /// topic's stored config next to the value that is enforced.
    Config {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// Print that topic's stored config.
        #[arg(long)]
        topic: Option<String>,
    },
    /// After a WAL shard has lost a majority of its voters for good, make
    /// this surviving voter a one-voter shard. Every partition resumes at
    /// its sealed end. Acknowledged offsets that were not sealed are lost.
    /// See docs/adr/0008-failure-and-recovery.md.
    TruncateToSealed {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// Required in a script. On a terminal the command asks instead.
        #[arg(long, default_value_t = false)]
        i_accept_data_loss: bool,
    },
    /// After every broker disk is gone, load the newest metadata snapshot
    /// and rebuild sealed extents from object footers. The broker must be a
    /// fresh metadata leader. Unsealed WAL entries and metadata newer than
    /// the snapshot do not come back. See docs/adr/0008-failure-and-recovery.md.
    RestoreCluster {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// Required in a script. On a terminal the command asks instead.
        #[arg(long, default_value_t = false)]
        i_accept_data_loss: bool,
    },
    /// Restore drill. Rebuild the newest metadata snapshot and the object
    /// footers in a scratch directory on the broker and report what
    /// `restore-cluster` would serve. Reads the bucket; the cluster is not
    /// changed. Exits 1 when a partition would be offline or the snapshot is
    /// older than `--max-age-secs`.
    VerifySnapshot {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// Fail when the snapshot is older than this many seconds.
        #[arg(long)]
        max_age_secs: Option<u64>,
        /// Print the broker's full JSON report, every partition included.
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Mirror topics from another cluster's bucket (ADR 0018).
    #[command(subcommand)]
    Mirror(Mirror),
}

#[derive(Subcommand)]
enum Mirror {
    /// Links to source clusters' buckets.
    #[command(subcommand)]
    Link(Link),
    /// Create a read-only mirror of a source topic, under the same name, with
    /// the source's partition count. Reads the source's newest snapshot.
    Create {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// The link to the source cluster.
        #[arg(long)]
        alias: String,
        /// The source topic.
        #[arg(long)]
        topic: String,
    },
    /// Set the consumer groups whose committed offsets are synced from the
    /// link's source into its mirrors, while a group has no members here.
    /// Pass no `--group` to stop syncing.
    Groups {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// The link to the source cluster.
        #[arg(long)]
        alias: String,
        /// A group id. Repeat for each group.
        #[arg(long = "group")]
        groups: Vec<String>,
    },
    /// Drop a stalled mirror partition's copy. Copying goes on from the later
    /// of its old end and the source's log start. For a partition the
    /// source's retention passed.
    Reset {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// The mirror topic.
        #[arg(long)]
        topic: String,
        /// The partition.
        #[arg(long)]
        partition: i32,
        /// Required: the mirror's copy of the partition is deleted.
        #[arg(long, default_value_t = false)]
        i_accept_data_loss: bool,
    },
}

#[derive(Subcommand)]
enum Link {
    /// Add a link. Each broker reads its keys from
    /// `CAMUS_MIRROR_<ALIAS>_ACCESS_KEY` and `CAMUS_MIRROR_<ALIAS>_SECRET_KEY`,
    /// the alias upper-cased with `-` as `_`. Adding the same link again
    /// changes nothing.
    Add {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// 1-32 characters of `[a-z0-9-]`.
        #[arg(long)]
        alias: String,
        /// The source bucket's S3 endpoint URL.
        #[arg(long)]
        endpoint: String,
        /// S3 region. R2 uses `auto`.
        #[arg(long, default_value = "auto")]
        region: String,
        /// The source bucket.
        #[arg(long)]
        bucket: String,
        /// The source's `s3.prefix`.
        #[arg(long, default_value = "")]
        prefix: String,
        /// Use virtual-host addressing, as the source's `s3.virtual_host`.
        #[arg(long, default_value_t = false)]
        virtual_host: bool,
    },
    /// Remove a link. Refused while a mirror topic uses it.
    Remove {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
        /// The link's alias.
        #[arg(long)]
        alias: String,
    },
    /// Print every link as JSON.
    List {
        /// HTTP `host:port`. `CAMUS_HTTP` or `~/.camus/context` when omitted.
        #[arg(long)]
        broker: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Top::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "camus", &mut std::io::stdout());
        }
        Top::Admin(Admin::Init {
            broker,
            peers,
            shards,
        }) => {
            let broker = broker_addr(broker)?;
            let peers = peers
                .iter()
                .map(|peer| parse_peer(peer))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let body = match shards {
                Some(shards) => serde_json::json!({ "peers": peers, "shards": shards }),
                None => serde_json::Value::Array(peers),
            };
            wait_until_up(&broker)?;
            let (status, text) = post(&broker, "/admin/init", &body)?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {text}");
                std::process::exit(1);
            }
            if text.is_empty() {
                println!("ok");
            } else {
                println!("{text}");
            }
        }
        Top::Admin(Admin::Status { broker }) => {
            let broker = broker_addr(broker)?;
            let health = reqwest::blocking::Client::new()
                .get(format!("http://{broker}/health"))
                .send()?;
            let health_status = health.status();
            let health_body = health.text()?;
            if !health_status.is_success() {
                eprintln!("{broker} health {health_status}: {health_body}");
                std::process::exit(1);
            }
            let metrics = get_json(&broker, "/admin/metrics")?;
            println!("health: {health_body}");
            println!(
                "metadata leader: {}",
                metrics["current_leader"]
                    .as_u64()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "none".to_string())
            );
            let shards = metrics["wal_shards"].as_array();
            if let Some(shards) = shards {
                for shard in shards {
                    let id = shard["id"].as_u64().unwrap_or(0);
                    let leader = shard["leader"]
                        .as_u64()
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "none".to_string());
                    let voters = shard["voters"]
                        .as_array()
                        .map(|voters| {
                            voters
                                .iter()
                                .filter_map(|id| id.as_u64())
                                .map(|id| id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default();
                    println!("wal shard {id}: leader {leader} voters {voters}");
                }
            }
        }
        Top::Admin(Admin::Mirror(Mirror::Link(Link::Add {
            broker,
            alias,
            endpoint,
            region,
            bucket,
            prefix,
            virtual_host,
        }))) => {
            let broker = broker_addr(broker)?;
            let link = serde_json::json!({
                "alias": alias,
                "endpoint": endpoint,
                "region": region,
                "bucket": bucket,
                "prefix": prefix,
                "virtual_host": virtual_host,
            });
            let (status, body) = post(&broker, "/admin/mirror/links", &link)?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {body}");
                std::process::exit(1);
            }
            println!("linked {alias}");
        }
        Top::Admin(Admin::Mirror(Mirror::Link(Link::Remove { broker, alias }))) => {
            // The alias goes into the URL path; refuse what a link can't be named.
            anyhow::ensure!(
                !alias.is_empty()
                    && alias.bytes().all(|byte| byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'-'),
                "alias must be 1-32 characters of [a-z0-9-]"
            );
            let broker = broker_addr(broker)?;
            let response = reqwest::blocking::Client::new()
                .delete(format!("http://{broker}/admin/mirror/links/{alias}"))
                .send()?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text()?;
                eprintln!("{broker} refused ({status}): {body}");
                std::process::exit(1);
            }
            println!("removed {alias}");
        }
        Top::Admin(Admin::Mirror(Mirror::Create {
            broker,
            alias,
            topic,
        })) => {
            let broker = broker_addr(broker)?;
            let body = serde_json::json!({ "alias": alias, "topic": topic });
            let (status, text) = post(&broker, "/admin/mirror/topics", &body)?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {text}");
                std::process::exit(1);
            }
            println!("mirroring {topic} from {alias}");
        }
        Top::Admin(Admin::Mirror(Mirror::Groups {
            broker,
            alias,
            groups,
        })) => {
            let broker = broker_addr(broker)?;
            let body = serde_json::json!({ "alias": alias, "groups": groups });
            let (status, text) = post(&broker, "/admin/mirror/groups", &body)?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {text}");
                std::process::exit(1);
            }
            println!("{alias} syncs {} groups", groups.len());
        }
        Top::Admin(Admin::Mirror(Mirror::Reset {
            broker,
            topic,
            partition,
            i_accept_data_loss,
        })) => {
            confirm(
                i_accept_data_loss,
                "reset",
                "reset deletes this mirror's copy of the partition; copying goes on from \
                 the later of its old end and the source's log start.",
            )?;
            let broker = broker_addr(broker)?;
            let body = serde_json::json!({
                "topic": topic,
                "partition": partition,
                "i_accept_data_loss": true,
            });
            let (status, text) = post(&broker, "/admin/mirror/reset", &body)?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {text}");
                std::process::exit(1);
            }
            println!("reset {topic} partition {partition}");
        }
        Top::Admin(Admin::Mirror(Mirror::Link(Link::List { broker }))) => {
            let broker = broker_addr(broker)?;
            let links = get_json(&broker, "/admin/mirror/links")?;
            println!("{}", serde_json::to_string_pretty(&links)?);
        }
        Top::Admin(Admin::Config { broker, topic }) => {
            let broker = broker_addr(broker)?;
            let path = match topic {
                Some(topic) => format!("/admin/config?topic={topic}"),
                None => "/admin/config".to_string(),
            };
            let response = reqwest::blocking::Client::new()
                .get(format!("http://{broker}{path}"))
                .send()?;
            let status = response.status();
            let body = response.text()?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {body}");
                std::process::exit(1);
            }
            print!("{body}");
        }
        Top::Admin(Admin::TruncateToSealed {
            broker,
            i_accept_data_loss,
        }) => {
            confirm(
                i_accept_data_loss,
                "truncate-to-sealed",
                "truncate-to-sealed drops acknowledged offsets that are not in R2. \
                 Restore a lost voter's disk if you can.",
            )?;
            let broker = broker_addr(broker)?;
            post_loss(&broker, "/admin/truncate-to-sealed")?;
        }
        Top::Admin(Admin::RestoreCluster {
            broker,
            i_accept_data_loss,
        }) => {
            confirm(
                i_accept_data_loss,
                "restore-cluster",
                "restore-cluster loses unsealed WAL entries and metadata newer than the \
                 last snapshot. Group offsets can move back by one snapshot interval.",
            )?;
            let broker = broker_addr(broker)?;
            post_loss(&broker, "/admin/restore-cluster")?;
        }
        Top::Admin(Admin::VerifySnapshot {
            broker,
            max_age_secs,
            json,
        }) => {
            let broker = broker_addr(broker)?;
            // A rebuild reads every object footer, which can outlast the
            // client's default 30-second timeout on a large bucket.
            let response = reqwest::blocking::Client::builder()
                .timeout(None)
                .build()?
                .get(format!("http://{broker}/admin/verify-snapshot"))
                .send()?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text()?;
                eprintln!("{broker} refused ({status}): {body}");
                std::process::exit(1);
            }
            let report: serde_json::Value = response.json()?;
            if json {
                println!("{report}");
            }
            let age_ms = report["snapshot_age_ms"].as_u64().unwrap_or(u64::MAX);
            let offline = report["offline"].as_u64().unwrap_or(0);
            if !json {
                println!(
                    "snapshot: {} (index {}, {}s old)",
                    report["snapshot"].as_str().unwrap_or("none"),
                    report["snapshot_index"],
                    age_ms / 1000
                );
                println!(
                    "topics: {}, partitions: {}, rebuilt extents: {}, offline partitions: {offline}",
                    report["topics"], report["partitions"], report["extents"]
                );
            }
            let stale = max_age_secs.is_some_and(|max| age_ms > max.saturating_mul(1000));
            if stale {
                eprintln!("the snapshot is older than --max-age-secs");
            }
            if offline > 0 {
                eprintln!("{offline} partitions would be offline after a restore");
            }
            if stale || offline > 0 {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

fn confirm(accepted: bool, name: &str, why: &str) -> anyhow::Result<()> {
    if accepted {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        eprintln!("{why} To proceed, pass --i-accept-data-loss.");
        std::process::exit(2);
    }
    eprintln!("{why} Type {name} to continue:");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    if line.trim() != name {
        anyhow::bail!("confirmation did not match {name}");
    }
    Ok(())
}

fn broker_addr(flag: Option<String>) -> anyhow::Result<String> {
    if let Some(broker) = flag {
        if !broker.is_empty() {
            return Ok(broker);
        }
    }
    if let Ok(broker) = std::env::var("CAMUS_HTTP") {
        if !broker.is_empty() {
            return Ok(broker);
        }
    }
    let path = context_path()?;
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Some(line) = text.lines().next() {
            let line = line.trim();
            if !line.is_empty() {
                return Ok(line.to_string());
            }
        }
    }
    anyhow::bail!(
        "pass --broker, set CAMUS_HTTP, or write the address in {}",
        path.display()
    )
}

fn context_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE"))?;
    Ok(PathBuf::from(home).join(".camus").join("context"))
}

fn parse_peer(text: &str) -> anyhow::Result<serde_json::Value> {
    let (id, addr) = text
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("--peer must be id=host:port"))?;
    let id: u64 = id
        .parse()
        .map_err(|_| anyhow::anyhow!("--peer id is not a number: {text}"))?;
    if !addr.contains(':') {
        anyhow::bail!("--peer address must be host:port: {text}");
    }
    Ok(serde_json::json!({ "id": id, "addr": addr }))
}

fn wait_until_up(broker: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    loop {
        let up = client
            .get(format!("http://{broker}/admin/metrics"))
            .send()
            .ok()
            .is_some_and(|response| response.status().is_success());
        if up {
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!("{broker} did not answer /admin/metrics");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn get_json(broker: &str, path: &str) -> anyhow::Result<serde_json::Value> {
    let response = reqwest::blocking::Client::new()
        .get(format!("http://{broker}{path}"))
        .send()?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text()?;
        anyhow::bail!("{broker} refused ({status}): {body}");
    }
    Ok(response.json()?)
}

fn post(
    broker: &str,
    path: &str,
    body: &serde_json::Value,
) -> anyhow::Result<(reqwest::StatusCode, String)> {
    let response = reqwest::blocking::Client::new()
        .post(format!("http://{broker}{path}"))
        .json(body)
        .send()?;
    let status = response.status();
    let text = response.text()?;
    Ok((status, text))
}

fn post_loss(broker: &str, path: &str) -> anyhow::Result<()> {
    let (status, body) = post(
        broker,
        path,
        &serde_json::json!({ "i_accept_data_loss": true }),
    )?;
    if !status.is_success() {
        eprintln!("{broker} refused ({status}): {body}");
        std::process::exit(1);
    }
    println!("{body}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_peer;

    #[test]
    fn a_peer_is_an_id_and_an_address() -> anyhow::Result<()> {
        let peer = parse_peer("2=broker-2:18002")?;
        assert_eq!(peer["id"], 2);
        assert_eq!(peer["addr"], "broker-2:18002");
        assert!(parse_peer("broker-2:18002").is_err());
        Ok(())
    }
}
