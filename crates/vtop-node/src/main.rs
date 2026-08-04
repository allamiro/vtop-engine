//! `vtop-node` — live-cluster node runner and load client (#215).
//!
//! This binary exists so the chaos validation scripts have real OS processes
//! to kill, freeze, partition, and starve. It assembles ONLY existing library
//! pieces (`vtop-meta` Raft node, `vtop-broker` replication/serving); every
//! correctness mechanism lives in those crates, none here.

mod client;
mod config;
mod data_node;
mod meta_node;
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
    let cli = Cli::parse();
    let outcome = run(cli).await;
    match outcome {
        Ok(code) => std::process::exit(code),
        Err(message) => {
            eprintln!("error: {message}");
            std::process::exit(1);
        }
    }
}

async fn run(cli: Cli) -> Result<i32, String> {
    match cli.command {
        Command::Meta { config } => {
            meta_node::run(config::load(&config)?).await?;
            Ok(0)
        }
        Command::Data { config } => {
            data_node::run(config::load(&config)?).await?;
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
            batch,
            value_bytes,
        } => {
            let config = client::ClientConfig::load(&client_config)?;
            client::verify(
                &config,
                client::VerifyArgs {
                    addr,
                    expect_at_least,
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
