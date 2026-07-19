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
///
/// JSON is selected by `--json` OR `VTOP_LOG_FORMAT=json`. The env form lets a
/// container opt into structured logs without changing the entrypoint, so a log
/// pipeline (Alloy -> Loki) gets parseable `{"level":...,"records":...}` lines
/// instead of pretty text. In pretty mode ANSI colour is emitted ONLY to a real
/// terminal: writing escape codes to a pipe (a container's captured stderr)
/// corrupts every downstream parser — a `level=~"WARN"` filter or `| logfmt`
/// then matches nothing because the field names are wrapped in `\e[3m…\e[0m`.
pub fn init_tracing(level: &str, json: bool) {
    use std::io::IsTerminal;
    use tracing_subscriber::EnvFilter;
    let json = json
        || std::env::var("VTOP_LOG_FORMAT")
            .map(|v| v.eq_ignore_ascii_case("json"))
            .unwrap_or(false);
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
        builder.with_ansi(std::io::stderr().is_terminal()).init();
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
            let report =
                verify_manifest_deep(engine.store.as_ref(), engine.backend.as_ref(), manifest)
                    .await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report.json)?);
            } else {
                for line in &report.lines {
                    println!("{line}");
                }
            }
            if report.passed {
                Ok(())
            } else {
                Err(VtopError::Other(format!(
                    "verification FAILED for {manifest} (see report above)"
                )))
            }
        }
    }
}

/// Outcome of a deep manifest verification, in both output shapes.
pub struct VerifyReport {
    pub passed: bool,
    pub json: serde_json::Value,
    pub lines: Vec<String>,
}

/// Deep verification of a stored manifest (#68). A HEAD proves only that
/// *something* with that name exists; every check here works on downloaded
/// CONTENT, because metadata can lie about a replaced object and content
/// cannot:
///
/// 1. the manifest downloads and parses as a VTOP manifest;
/// 2. its self-hash verifies (the manifest itself is untampered/uncorrupted);
/// 3. the referenced object's size AND content digest (recomputed from the
///    downloaded bytes, using the manifest's algorithm) match the manifest;
/// 4. the ledger row for the batch agrees (state and recorded digest).
///
/// A missing ledger row is reported but is not a failure: the manifest may
/// have been written by a different engine with its own state store.
pub async fn verify_manifest_deep(
    store: &dyn vtop_state::StateStore,
    backend: &dyn vtop_upload::UploadBackend,
    manifest_uri: &str,
) -> Result<VerifyReport, VtopError> {
    use vtop_core::checksum::digest_bytes;
    use vtop_core::types::ChecksumAlgorithm;

    let mut lines = Vec::new();
    let mut failed = false;

    // 1. Content, not metadata.
    let manifest_bytes = backend.get_object(manifest_uri).await?;
    let parsed: vtop_core::manifest::VtopManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| VtopError::Other(format!("manifest {manifest_uri} does not parse: {e}")))?;
    lines.push(format!(
        "manifest        : {} ({} bytes, batch {})",
        manifest_uri,
        manifest_bytes.len(),
        parsed.batch_id
    ));

    // 2. Self-hash: is the manifest itself intact?
    let self_hash_ok = parsed.verify_self_hash().is_ok();
    failed |= !self_hash_ok;
    lines.push(format!(
        "self-hash       : {}",
        if self_hash_ok {
            "OK"
        } else {
            "FAILED (manifest content does not match its embedded sha256)"
        }
    ));

    // 3. The stored object's actual content.
    let object_bytes = backend.get_object(&parsed.object.uri).await?;
    let size_ok = object_bytes.len() as u64 == parsed.object.size_bytes;
    failed |= !size_ok;
    lines.push(format!(
        "object size     : {} ({} bytes stored, {} expected)",
        if size_ok { "OK" } else { "FAILED" },
        object_bytes.len(),
        parsed.object.size_bytes
    ));

    let algo = parsed
        .object
        .checksum_algorithm
        .parse::<ChecksumAlgorithm>()
        .ok();
    let recomputed = algo.and_then(|a| digest_bytes(a, &object_bytes));
    let checksum_status = match &recomputed {
        Some(actual) if parsed.object.checksum.is_empty() => {
            // Manifest says an algorithm but carries no digest: nothing to
            // compare against; surface it rather than calling it a pass.
            format!("LIMITED (no digest recorded; computed {actual})")
        }
        Some(actual) => {
            let ok = actual.eq_ignore_ascii_case(&parsed.object.checksum);
            failed |= !ok;
            if ok {
                format!("OK ({} {})", parsed.object.checksum_algorithm, actual)
            } else {
                format!(
                    "FAILED (stored content hashes to {actual}, manifest says {})",
                    parsed.object.checksum
                )
            }
        }
        None => "LIMITED (checksums disabled for this batch)".to_string(),
    };
    lines.push(format!("object content  : {checksum_status}"));

    // 4. Ledger reconciliation.
    let row = store.get_batch(&parsed.batch_id).await?;
    let ledger_status = match &row {
        None => "ABSENT (not a failure: manifest may belong to another engine's store)".to_string(),
        Some(r) => {
            let state_ok = matches!(r.state, BatchState::Verified | BatchState::SourceCommitted);
            let digest_ok = match (&r.object_sha256, &recomputed, algo) {
                (Some(ledger), Some(actual), Some(ChecksumAlgorithm::Sha256)) => {
                    ledger.eq_ignore_ascii_case(actual)
                }
                _ => true, // nothing comparable recorded
            };
            failed |= !state_ok || !digest_ok;
            match (state_ok, digest_ok) {
                (true, true) => format!("OK (state {})", r.state),
                (false, _) => format!(
                    "FAILED (state {} — the ledger never saw this batch verified)",
                    r.state
                ),
                (_, false) => "FAILED (ledger digest differs from stored content)".to_string(),
            }
        }
    };
    lines.push(format!("ledger          : {ledger_status}"));

    let verdict = if failed { "FAILED" } else { "PASSED" };
    lines.push(format!("verdict         : {verdict}"));

    let json = serde_json::json!({
        "manifest": manifest_uri,
        "batch_id": parsed.batch_id,
        "object_uri": parsed.object.uri,
        "self_hash_ok": self_hash_ok,
        "object_size_ok": size_ok,
        "object_content": checksum_status,
        "ledger": ledger_status,
        "backend": backend.backend_name(),
        "passed": !failed,
    });

    Ok(VerifyReport {
        passed: !failed,
        json,
        lines,
    })
}

fn describe_recovery(state: BatchState) -> &'static str {
    use vtop_core::replay::{next_recovery_action, RecoveryAction};
    match next_recovery_action(state) {
        RecoveryAction::RetrySourceCommit => "retry the source commit",
        RecoveryAction::None => "do nothing (already committed)",
        _ => "mark REPLAY_REQUIRED (source progress preserved)",
    }
}
