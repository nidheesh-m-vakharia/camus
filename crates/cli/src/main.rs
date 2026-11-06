//! `camus` admin binary. Each command calls one broker's HTTP admin route.
//! It exits non-zero when the broker refuses, and prints the broker's reason.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "camus", version)]
struct Cli {
    #[command(subcommand)]
    command: Top,
}

#[derive(Subcommand)]
enum Top {
    /// Cluster operations.
    #[command(subcommand)]
    Admin(Admin),
}

#[derive(Subcommand)]
enum Admin {
    /// After a WAL shard has lost a majority of its voters for good, make
    /// this surviving voter a one-voter shard. Every partition resumes at
    /// its sealed end. Acknowledged offsets that were not sealed are lost.
    /// See docs/adr/0008-failure-and-recovery.md.
    TruncateToSealed {
        /// HTTP `host:port` of a surviving shard voter.
        #[arg(long)]
        broker: String,
        /// Required. The command loses acknowledged data.
        #[arg(long, default_value_t = false)]
        i_accept_data_loss: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Top::Admin(Admin::TruncateToSealed {
            broker,
            i_accept_data_loss,
        }) => {
            if !i_accept_data_loss {
                eprintln!(
                    "truncate-to-sealed drops acknowledged offsets that are not in R2. \
                     Restore a lost voter's disk if you can. To proceed, pass --i-accept-data-loss."
                );
                std::process::exit(2);
            }
            let response = reqwest::blocking::Client::new()
                .post(format!("http://{broker}/admin/truncate-to-sealed"))
                .json(&serde_json::json!({ "i_accept_data_loss": true }))
                .send()?;
            let status = response.status();
            let body = response.text()?;
            if !status.is_success() {
                eprintln!("{broker} refused ({status}): {body}");
                std::process::exit(1);
            }
            println!("{body}");
        }
    }
    Ok(())
}
