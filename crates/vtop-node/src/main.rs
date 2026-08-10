//! `vtop-node` — live-cluster node runner and load client (#215).
//!
//! This binary exists so the chaos validation scripts have real OS processes
//! to kill, freeze, partition, and starve. It assembles ONLY existing library
//! pieces (`vtop-meta` Raft node, `vtop-broker` replication/serving); every
//! correctness mechanism lives in those crates, none here.

mod client;
mod colocated;
mod config;
mod data_node;
mod lease_agent;
mod lease_watcher;
mod meta_node;
mod observe;
mod promotion;
mod tls;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use vtop_protocol::Durability;

#[derive(Parser, Debug)]
#[command(
    name = "vtop-node",
    version,
    about = "Live 3-node chaos-validation harness for the native VTOP broker (#215)."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a metadata Raft node (peer + admin endpoints, durable real disk).
    Meta {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run a data-plane node (leader, follower, or standalone re-open).
    Data {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run BOTH roles in one process — a metadata voter and a data-plane
    /// replica sharing a runtime, one observability endpoint, and a fate
    /// (#215).
    Node {
        #[arg(long)]
        config: PathBuf,
    },
    /// Recover and seal one quiesced active segment for offline verification.
    SealActive {
        #[arg(long)]
        path: PathBuf,
    },
    /// Produce byte-deterministic records against a leader.
    Produce {
        #[arg(long)]
        client_config: PathBuf,
        #[arg(long)]
        addr: String,
        #[arg(long)]
        records: u64,
        #[arg(long, default_value_t = 128, value_parser = parse_batch)]
        batch: u32,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        /// First producer sequence (continue a stream after recovery).
        #[arg(long, default_value_t = 0)]
        first_sequence: u64,
        /// local-fsync | quorum
        #[arg(long, default_value = "quorum")]
        durability: String,
        /// Persist the acknowledged floor here after every batch.
        #[arg(long)]
        acked_file: Option<PathBuf>,
    },
    /// Fetch from offset 0 and byte-verify everything below the committed HWM.
    Verify {
        #[arg(long)]
        client_config: PathBuf,
        #[arg(long)]
        addr: String,
        /// Fail if the committed HWM is below this acknowledged floor.
        #[arg(long, default_value_t = 0)]
        expect_at_least: u64,
        /// Byte-verify record CONTENT below this offset only; above it check
        /// structure alone (contiguity, high watermark).
        ///
        /// Content is reconstructed from the offset, which is only predictable
        /// for records this producer wrote contiguously from sequence 0. A
        /// range that also holds records from another producer — or from this
        /// one after a producer-epoch bump, whose sequences restart at 0 — has
        /// a suffix whose content no reader can derive. Defaults to unbounded,
        /// which is every existing caller.
        #[arg(long, default_value_t = u64::MAX)]
        verify_content_through: u64,
        #[arg(long, default_value_t = 512, value_parser = parse_batch)]
        batch: u32,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
    },
    /// Query a follower's local committed offset over the replica plane.
    ReplicaStatus {
        #[arg(long)]
        client_config: PathBuf,
        #[arg(long)]
        addr: String,
        #[arg(long, default_value = "localhost")]
        server_name: String,
    },
}

fn parse_durability(value: &str) -> Result<Durability, String> {
    match value {
        "local-fsync" => Ok(Durability::LocalFsync),
        "quorum" => Ok(Durability::Quorum),
        other => Err(format!(
            "unknown durability {other:?}; expected local-fsync|quorum"
        )),
    }
}

fn parse_batch(value: &str) -> Result<u32, String> {
    let batch = value
        .parse::<u32>()
        .map_err(|_| format!("{value:?} is not a valid batch size"))?;
    if batch == 0 || batch > data_node::MAX_RECORDS {
        return Err(format!(
            "batch must be between 1 and {}",
            data_node::MAX_RECORDS
        ));
    }
    Ok(batch)
}

#[tokio::main]
async fn main() {
    // Structured logs use the same `VTOP_LOG_FORMAT` contract as `vtopctl`, so
    // node and engine lines land in Loki with one shape. Stderr only: stdout
    // carries the ready markers the chaos harness parses.
    vtop_observe::logging::init("info", false);
    let cli = Cli::parse();
    let shutdown = shutdown_signal();
    let outcome = run(cli, shutdown).await;
    match outcome {
        Ok(code) => std::process::exit(code),
        Err(message) => {
            eprintln!("error: {message}");
            std::process::exit(1);
        }
    }
}

/// SIGTERM/SIGINT flip one process-wide flag every serving loop observes
/// (#280). One flag rather than per-role signal plumbing: the roles differ in
/// what DRAINING means, not in what "stop" means.
fn shutdown_signal() -> tokio::sync::watch::Receiver<bool> {
    let (sender, receiver) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        #[cfg(unix)]
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut term) => {
                    term.recv().await;
                }
                Err(error) => {
                    eprintln!("SIGTERM handler unavailable ({error}); only SIGINT is handled");
                    std::future::pending::<()>().await;
                }
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();
        tokio::select! {
            () = terminate => {}
            result = tokio::signal::ctrl_c() => {
                if result.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
        // Stdout, like the ready markers: the chaos harness parses this line
        // to distinguish a drain from a hang.
        println!("shutdown_signal received, draining");
        use std::io::Write;
        std::io::stdout().flush().ok();
        let _ = sender.send(true);
        // Hold the sender for the rest of the process lifetime so receivers
        // never observe a closed channel as an implicit shutdown.
        std::future::pending::<()>().await;
    });
    receiver
}

async fn run(cli: Cli, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<i32, String> {
    match cli.command {
        Command::Meta { config } => {
            meta_node::run(config::load(&config)?, shutdown).await?;
            Ok(0)
        }
        Command::Data { config } => {
            data_node::run(config::load(&config)?, shutdown).await?;
            Ok(0)
        }
        Command::Node { config } => {
            colocated::run(config::load(&config)?, shutdown).await?;
            Ok(0)
        }
        Command::SealActive { path } => {
            data_node::seal_active(&path)?;
            Ok(0)
        }
        Command::Produce {
            client_config,
            addr,
            records,
            batch,
            value_bytes,
            first_sequence,
            durability,
            acked_file,
        } => {
            let config = client::ClientConfig::load(&client_config)?;
            client::produce(
                &config,
                client::ProduceArgs {
                    addr,
                    records,
                    batch,
                    value_bytes,
                    first_sequence,
                    durability: parse_durability(&durability)?,
                    acked_file,
                },
            )
            .await
        }
        Command::Verify {
            client_config,
            addr,
            expect_at_least,
            verify_content_through,
            batch,
            value_bytes,
        } => {
            let config = client::ClientConfig::load(&client_config)?;
            client::verify(
                &config,
                client::VerifyArgs {
                    addr,
                    expect_at_least,
                    verify_content_through,
                    batch,
                    value_bytes,
                },
            )
            .await
        }
        Command::ReplicaStatus {
            client_config,
            addr,
            server_name,
        } => {
            let config = client::ClientConfig::load(&client_config)?;
            client::replica_status(&config.tls, &server_name, &addr, &config.range).await
        }
    }
}
