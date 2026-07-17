//! `vtopctl` command-line interface.

use crate::engine::Engine;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;

#[derive(Parser, Debug)]
#[command(
    name = "vtopctl",
    version,
    about = "VTOP Engine — replay-safe, manifest-driven telemetry object transfer (prototype)."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Emit machine-readable JSON instead of human-readable tables.
    #[arg(long, global = true)]
    pub json: bool,

    /// Override the log level (trace|debug|info|warn|error).
    #[arg(long, global = true)]
    pub log_level: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the engine continuously (recover, discover, process).
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    /// Discover available sources and print them.
    Discover {
        #[arg(long)]
        config: PathBuf,
    },
    /// Process a single cycle for one source type.
    ProcessOnce {
        #[arg(long, value_enum)]
        source: SourceKind,
        #[arg(long)]
        config: PathBuf,
    },
    /// Run recovery / replay for incomplete batches (optionally one batch).
    Replay {
        #[arg(long)]
        batch_id: Option<String>,
        #[arg(long)]
        config: PathBuf,
    },
    /// Show a summary of batch states.
    Status {
        #[arg(long)]
        config: PathBuf,
    },
    /// List all batches in the state store.
    ListBatches {
        #[arg(long)]
        config: PathBuf,
    },
    /// Verify a manifest object in storage.
    VerifyManifest {
        #[arg(long)]
        manifest: String,
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SourceKind {
    Kafka,
    File,
    Syslog,
}

impl From<SourceKind> for SourceType {
    fn from(s: SourceKind) -> Self {
        match s {
            SourceKind::Kafka => SourceType::Kafka,
            SourceKind::File => SourceType::File,
            SourceKind::Syslog => SourceType::SyslogSpool,
        }
    }
}

/// Initialize structured logging. Honors `--log-level`, then config, then env.
pub fn init_tracing(level: &str, json: bool) {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_lowercase()));
    // Logs go to STDERR so they never collide with command output on STDOUT
    // (notably the machine-readable `--json` payloads).
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().with_current_span(false).init();
    } else {
        builder.init();
    }
}

fn load(config: &Path) -> Result<(VtopConfig, StreamsConfig), VtopError> {
    let cfg = VtopConfig::from_path(config)?;
    // streams.yaml is optional and looked up next to config.yaml.
    let streams_path = config.with_file_name("streams.yaml");
    let streams = if streams_path.exists() {
        StreamsConfig::from_path(&streams_path)?
    } else {
        StreamsConfig { streams: vec![] }
    };
    Ok((cfg, streams))
}

/// Dispatch a parsed CLI invocation. Returns a process exit code.
pub async fn dispatch(cli: Cli) -> i32 {
    match run_command(&cli).await {
        Ok(()) => 0,
        Err(e) => {
            // Errors are reported on stderr without leaking secrets.
            tracing::error!(error = %e, "command failed");
            eprintln!("error: {e}");
            1
        }
    }
}

async fn run_command(cli: &Cli) -> Result<(), VtopError> {
    match &cli.command {
        Command::Run { config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            // Metrics are opt-in via VTOP_METRICS_ADDR and only for the
            // long-running `run` action: one-shot commands would expose a port
            // for a few milliseconds and never be scraped. Failure to start is
            // logged, never fatal - telemetry must not block archiving.
            crate::metrics_server::maybe_start().await;
            let mut engine = Engine::new(cfg, streams).await?;
            engine.run().await
        }
        Command::Discover { config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let engine = Engine::new(cfg, streams).await?;
            let sources = engine.discover().await?;
            if cli.json {
                let v: Vec<_> = sources
                    .iter()
                    .map(|s| {
                        serde_json::json!({
                            "source_type": s.source_type.as_str(),
                            "source_name": s.source_name,
                            "format": s.format.as_str(),
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                println!("{:<14} {:<8} SOURCE", "TYPE", "FORMAT");
                for s in sources {
                    println!("{:<14} {:<8} {}", s.source_type, s.format, s.source_name);
                }
            }
            Ok(())
        }
        Command::ProcessOnce { source, config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let mut engine = Engine::new(cfg, streams).await?;
            engine.recover().await?;
            let outcomes = engine.process_once((*source).into()).await?;
            if cli.json {
                let v: Vec<_> = outcomes
                    .iter()
                    .map(|o| {
                        serde_json::json!({
                            "batch_id": o.batch_id,
                            "final_state": o.final_state.as_str(),
                            "committed": o.committed,
                            "record_count": o.record_count,
                            "object_uri": o.object_uri,
                            "metrics": o.metrics,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else if outcomes.is_empty() {
                println!("no records available for source type {:?}", source);
            } else {
                for o in outcomes {
                    println!(
                        "{}  state={}  committed={}  records={}",
                        o.batch_id, o.final_state, o.committed, o.record_count
                    );
                    if let Some(m) = &o.metrics {
                        println!("    metrics: {}", m.summary());
                    }
                }
            }
            Ok(())
        }
        Command::Replay { batch_id, config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let mut engine = Engine::new(cfg, streams).await?;
            if let Some(id) = batch_id {
                match engine.store.get_batch(id).await? {
                    Some(rec) => println!(
                        "batch {} is in state {} — recovery will {}",
                        id,
                        rec.state,
                        describe_recovery(rec.state)
                    ),
                    None => return Err(VtopError::NotFound(format!("batch {id}"))),
                }
            }
            let summary = engine.recover().await?;
            println!(
                "recovery complete: committed={} replay_required={} still_pending={}",
                summary.committed, summary.replay_required, summary.still_pending
            );
            Ok(())
        }
        Command::Status { config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let engine = Engine::new(cfg, streams).await?;
            let batches = engine.store.list_batches().await?;
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for b in &batches {
                *counts.entry(b.state.to_string()).or_default() += 1;
            }
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&counts)?);
            } else {
                println!("total batches: {}", batches.len());
                for (state, n) in counts {
                    println!("  {state:<18} {n}");
                }
            }
            Ok(())
        }
        Command::ListBatches { config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let engine = Engine::new(cfg, streams).await?;
            let batches = engine.store.list_batches().await?;
            if cli.json {
                let v: Vec<_> = batches
                    .iter()
                    .map(|b| {
                        serde_json::json!({
                            "batch_id": b.batch_id,
                            "state": b.state.as_str(),
                            "source_type": b.source_type.as_str(),
                            "source_name": b.source_name,
                            "object_uri": b.object_uri,
                            "record_count": b.record_count,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                println!("{:<18} {:<10} {:<14} SOURCE", "STATE", "RECORDS", "TYPE");
                for b in batches {
                    println!(
                        "{:<18} {:<10} {:<14} {}",
                        b.state,
                        b.record_count.unwrap_or(0),
                        b.source_type,
                        b.source_name
                    );
                }
            }
            Ok(())
        }
        Command::VerifyManifest { manifest, config } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let engine = Engine::new(cfg, streams).await?;
            let head = engine.backend.head_object(manifest).await?;
            let exists = head.size_bytes.is_some();
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "manifest": manifest,
                        "exists": exists,
                        "size_bytes": head.size_bytes,
                        "stored_sha256": head.checksum_sha256,
                        "backend": engine.backend.backend_name(),
                    }))?
                );
            } else if exists {
                println!(
                    "manifest present: {} ({} bytes){}",
                    manifest,
                    head.size_bytes.unwrap_or(0),
                    head.checksum_sha256
                        .map(|s| format!(", sha256={s}"))
                        .unwrap_or_default()
                );
            } else {
                println!("manifest NOT found: {manifest}");
            }
            Ok(())
        }
    }
}

fn describe_recovery(state: BatchState) -> &'static str {
    use vtop_core::replay::{next_recovery_action, RecoveryAction};
    match next_recovery_action(state) {
        RecoveryAction::RetrySourceCommit => "retry the source commit",
        RecoveryAction::None => "do nothing (already committed)",
        _ => "mark REPLAY_REQUIRED (source progress preserved)",
    }
}
