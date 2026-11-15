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
