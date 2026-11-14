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
    /// Run the engine continuously (recover, discover, process). SIGINT
    /// (Ctrl-C) or, on Unix, SIGTERM stops it after the current source pass
    /// and a shutdown flush (exit 0; exit 1 if the flush could not archive
    /// what it had buffered); a second signal during that flush abandons it
    /// (exit 1).
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
    /// Check a config file (and a `streams.yaml` beside it) without starting
    /// anything: no state store, object store or source is contacted. Exits
    /// non-zero only for an invalid config; a valid but risky setting, such
    /// as `upload.require_strong_verification: false`, is printed as a
    /// warning and still passes.
    Validate {
        #[arg(long)]
        config: PathBuf,
        /// Explain effective per-stream settings and their origin, without
        /// resolving secrets or contacting any service.
        #[arg(long)]
        print_effective: bool,
    },
    /// Apply state-store schema migrations with a deployment identity.
    Migrate {
        #[arg(long)]
        config: PathBuf,
    },
    /// Check that the archive covers every source without holes (#560):
    /// committed ranges must tile each Kafka partition, file and spool file,
    /// and a range that failed must have been archived again since. Reads
    /// the ledger and the digests `prune-ledger` wrote for the rows it
    /// deleted. Exits non-zero when it finds lost data.
    AuditCoverage {
        #[arg(long)]
        config: PathBuf,
        /// Where prune digests are read from [default: engine.ledger_digest_dir,
        /// else <database file>.ledger-digests beside a SQLite database file, else
        /// <engine.work_dir>/ledger-digests].
        #[arg(long)]
        digest_dir: Option<PathBuf>,
        /// Also ask the object store whether every committed object and
        /// manifest is present at its recorded size (one HEAD each).
        #[arg(long)]
        check_storage: bool,
        /// Also write the results as a Prometheus text file (for a node
        /// exporter's textfile collector).
        #[arg(long)]
        metrics_file: Option<PathBuf>,
    },
    /// Read committed objects back and verify them again (#628). For each
    /// committed batch, in the ledger or in a prune digest: download the
    /// manifest at its pinned version, check its self-hash and MAC and that
    /// it binds to the ledger row, verify the object's stored content against
    /// the manifest's digest, and check the manifest's current key still
    /// holds the pinned version. Reports missing, mismatched, unauthenticated
    /// and unreadable batches, and manifests from a newer release
    /// (unsupported_version), and exits non-zero on any. Read-only; it can
    /// run beside a live engine, bounded by the limits below.
    Scrub {
        #[arg(long)]
        config: PathBuf,
        /// Where prune digests are read from [default: as audit-coverage].
        #[arg(long)]
        digest_dir: Option<PathBuf>,
        /// Batches checked at once.
        #[arg(long, default_value_t = crate::scrub::DEFAULT_CONCURRENCY as u32,
              value_parser = clap::value_parser!(u32).range(1..=256))]
        concurrency: u32,
        /// Start at most this many batches per second (fractions allowed, down
        /// to one a day).
        #[arg(long, value_parser = positive_rate)]
        max_objects_per_sec: Option<f64>,
        /// Read at most this many object bytes per second, by each object's
        /// recorded size.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        max_bytes_per_sec: Option<u64>,
        /// Check at most this many batches this run.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        limit: Option<u64>,
        /// Resume after the batch this file names, and record progress in it
        /// as each batch completes. With --limit, successive runs walk the
        /// archive a slice at a time; the run that reaches the end completes
        /// the pass and the next one starts again from the beginning.
        #[arg(long, conflicts_with = "sample")]
        cursor_file: Option<PathBuf>,
        /// Check this many batches chosen at random instead of walking in
        /// order.
        #[arg(long, conflicts_with = "limit",
              value_parser = clap::value_parser!(u64).range(1..))]
        sample: Option<u64>,
        /// The sample's seed (random when not given; printed either way).
        #[arg(long, requires = "sample")]
        seed: Option<u64>,
        /// Also write the results as a Prometheus text file (for a node
        /// exporter's textfile collector). Totals carry forward from the file
        /// it replaces.
        #[arg(long)]
        metrics_file: Option<PathBuf>,
    },
    /// Incrementally delete old SOURCE_COMMITTED ledger rows (#128). The
    /// newest committed row per source path always survives, so recovery
    /// cursor seeding is unchanged; rows in any other state are untouched.
    /// On PostgreSQL this needs a maintenance identity with DELETE on the
    /// ledger — the runtime role is denied DELETE by design. The CANDIDATE
    /// rows are written to a digest BEFORE they are deleted (#560), so a
    /// coverage audit can still account for them; each is re-checked at
    /// deletion, and one that has gained a reason to stay is digested but
    /// kept.
    PruneLedger {
        #[arg(long)]
        config: PathBuf,
        /// Where the digest of the deleted rows is written [default:
        /// engine.ledger_digest_dir, else <database file>.ledger-digests beside
        /// a SQLite database file, else <engine.work_dir>/ledger-digests -
        /// scratch: name one for PostgreSQL].
        #[arg(long)]
        digest_dir: Option<PathBuf>,
        /// Delete committed rows whose last transition is older than this
        /// many days.
        #[arg(long)]
        older_than_days: u32,
        /// Maximum rows deleted per pass; run repeatedly to drain. Zero is
        /// rejected: LIMIT 0 would report a "successful" prune that can never
        /// delete anything.
        #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u32).range(1..))]
        limit: u32,
    },
    /// Print a dual-write / shadow-read receipt file (#458). One JSON object
    /// per line, as the gateway wrote it: which native offset a Kafka offset
    /// became, and the hash of the records both backends served.
    Receipt {
        /// JSONL written by the gateway when a topic is `dual` or `shadow`.
        #[arg(long)]
        file: PathBuf,
    },
    /// Offline verification tools for sealed native segments. Unlike every
    /// other subcommand these take no --config: an independent verifier must
    /// run from the sealed artifacts and explicit pins alone.
    Segment {
        #[command(subcommand)]
        command: crate::segment_tools::SegmentCommand,
    },
    /// Metadata Raft admin client (status, membership, propose). Uses a
    /// dedicated meta admin YAML via --config (endpoint + PEM paths).
    Meta {
        #[command(subcommand)]
        command: crate::meta_tools::MetaCommand,
    },
    /// Native data-plane operator client. Admin parity with `meta`: where
    /// `meta status` reports what the metadata group believes, `node status`
    /// reports where each replica's disk has actually got to, over the
    /// replication plane rather than the observability endpoint.
    Node {
        #[command(subcommand)]
        command: crate::node_tools::NodeCommand,
    },
    /// Cold-tier tools for sealed native segments: resumable upload + verify,
    /// rehydrate from a version pin, and abandoned multipart cleanup. Evidence
    /// commit still goes through the meta admin endpoint.
    Tier {
        #[command(subcommand)]
        command: crate::tier_tools::TierCommand,
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

/// Initialize structured logging.
///
/// Filter precedence is `RUST_LOG`, then `--log-level`, then the config file's
/// `engine.log_level`. `RUST_LOG` wins deliberately: it is the conventional
/// Rust escape hatch, and an operator debugging a running deployment needs to
/// raise verbosity without editing the config or the entrypoint that supplied
/// the flag.
///
/// JSON is selected by `--json` OR `VTOP_LOG_FORMAT=json`. The encoding rules
/// and the reasoning behind them live in [`vtop_observe::logging`], shared with
/// the cluster nodes so engine and node lines land in Loki with one shape.
///
/// With the `otel` feature, the OTLP trace exporter is installed beside the
/// logs when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces-only variable) is
/// set, and not otherwise; see [`crate::otel`]. [`dispatch`] flushes it before
/// the process exits.
pub fn init_tracing(level: &str, json: bool) {
    #[cfg(feature = "otel")]
    crate::otel::init_tracing(level, json);
    #[cfg(not(feature = "otel"))]
    vtop_observe::logging::init(level, json);
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
    streams.validate(&cfg)?;
    Ok((cfg, streams))
}

/// Render only the dedicated allowlist; never walk the configuration model.
fn print_effective_block(
    name: &str,
    stream: &vtop_core::effective_config::EffectiveStream,
) -> Result<(), VtopError> {
    println!("{name}:");
    let value = serde_json::to_value(stream)?;
    if let Some(fields) = value.as_object() {
        for (key, setting) in fields {
            let value = &setting["value"];
            // Strings bare, except an empty one, which would otherwise read
            // as a missing value; anything else as its JSON literal.
            let rendered = match value.as_str() {
                Some("") => "\"\"".to_owned(),
                Some(text) => text.to_owned(),
                None => value.to_string(),
            };
            let origin = setting["origin"].as_str().unwrap_or("built-in default");
            println!("  {key}: {rendered} ({origin})");
        }
    }
    Ok(())
}

/// A rate that is a finite number above zero.
/// An object rate: finite, and at least one a day (a slower rate's interval
/// would exceed the pacer's one-day cap anyway, so it is refused here rather
/// than silently raised).
fn positive_rate(value: &str) -> Result<f64, String> {
    const MIN: f64 = 1.0 / 86_400.0;
    match value.parse::<f64>() {
        Ok(rate) if rate.is_finite() && rate >= MIN => Ok(rate),
        Ok(_) => Err(format!("must be a number of at least {MIN} (one a day)")),
        Err(e) => Err(e.to_string()),
    }
}

fn print_receipts(file: &Path, json: bool) -> i32 {
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) => {
            eprintln!("read {}: {error}", file.display());
            return 1;
        }
    };
    let mut any = false;
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => {
                any = true;
                if json {
                    println!("{value}");
                } else {
                    println!("{}", serde_json::to_string_pretty(&value).unwrap());
                }
            }
            Err(error) => {
                eprintln!("receipt line {}: {error}", i + 1);
                return 1;
            }
        }
    }
    if !any {
        eprintln!("{} contains no receipt rows", file.display());
        return 1;
    }
    0
}

/// Dispatch a parsed CLI invocation. Returns a process exit code.
pub async fn dispatch(cli: Cli) -> i32 {
    // Segment tools are synchronous, config-free, and use their own exit-code
    // contract (see segment_tools), so they bypass the engine dispatch path.
    if let Command::Receipt { file } = &cli.command {
        return print_receipts(file, cli.json);
    }
    if let Command::Segment { command } = &cli.command {
        return crate::segment_tools::run(command, cli.json);
    }
    // Meta admin tools talk to the VTPM admin endpoint; they bypass the
    // archive engine entirely.
    if let Command::Meta { command } = cli.command {
        // Re-bind so we can move `command` into the async helper.
        return crate::meta_tools::run(command, cli.json).await;
    }
    // Node tools speak the replication plane directly; they too bypass the
    // archive engine.
    if let Command::Node { command } = cli.command {
        return crate::node_tools::run(command, cli.json).await;
    }
    // Tier tools drive an upload backend plus the meta admin endpoint; they
    // also bypass the archive engine.
    if let Command::Tier { command } = cli.command {
        return crate::tier_tools::run(command, cli.json).await;
    }
    let code = match run_command(&cli).await {
        Ok(()) => 0,
        Err(e) => {
            // Errors are reported on stderr without leaking secrets.
            tracing::error!(error = %e, "command failed");
            eprintln!("error: {e}");
            1
        }
    };
    // The last spans of a stop (the shutdown flush's batches) are still
    // queued; send them before exiting, within a bounded wait (#683).
    #[cfg(feature = "otel")]
    crate::otel::shutdown().await;
    code
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
            let mut engine = Engine::new_exclusive(cfg, streams).await?;
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
            let mut engine = Engine::new_exclusive(cfg, streams).await?;
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
            let mut engine = Engine::new_exclusive(cfg, streams).await?;
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
                "recovery complete: committed={} replay_required={} superseded={} still_pending={}",
                summary.committed,
                summary.replay_required,
                summary.superseded,
                summary.still_pending
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
            let report = verify_manifest_deep(
                engine.store.as_ref(),
                engine.backend.as_ref(),
                manifest,
                engine.manifest_mac_key(),
            )
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
        Command::Validate {
            config,
            print_effective,
        } => {
            // `load` parses and validates both files; an error is the
            // non-zero exit. Warnings never fail the check (#629): each is a
            // documented opt-out, and refusing it here would only push
            // operators to skip the check.
            let (cfg, streams) = load(config)?;
            let warnings = streams.warnings(&cfg);
            if *print_effective {
                let effective =
                    vtop_core::effective_config::effective_streams(&cfg, Some(&streams));
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"status": "ok", "warnings": warnings, "effective_version": 1, "effective": effective})
                    );
                } else {
                    for warning in &warnings {
                        println!("WARNING: {warning}");
                    }
                    for shadowed in &effective.shadowed {
                        println!(
                            "WARNING: streams[{}] repeats source_name {}; the engine applies only \
                             the first entry with that name, so this one is never used",
                            shadowed.index, shadowed.source_name
                        );
                    }
                    println!("effective_version: 1");
                    print_effective_block("default", &effective.default)?;
                    for stream in &effective.streams {
                        print_effective_block(
                            &format!("stream {}", stream.source_name.value),
                            stream,
                        )?;
                    }
                }
                return Ok(());
            }
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"status": "ok", "warnings": warnings})
                );
            } else {
                for warning in &warnings {
                    println!("WARNING: {warning}");
                }
                println!(
                    "{}: config is valid ({} warning(s))",
                    config.display(),
                    warnings.len()
                );
            }
            Ok(())
        }
        Command::Migrate { config } => {
            let cfg = VtopConfig::from_path(config)?;
            cfg.validate()?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let state_store = cfg.engine.state_store.resolve()?;
            let backend = if state_store.is_postgres() {
                "postgresql"
            } else {
                "sqlite"
            };
            vtop_state::migrate_state_store(state_store.expose_secret()).await?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"status": "ok", "backend": backend})
                );
            } else {
                println!("state-store migration complete ({backend})");
            }
            Ok(())
        }
        Command::AuditCoverage {
            config,
            digest_dir,
            check_storage,
            metrics_file,
        } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let digest_dir = digest_dir
                .clone()
                .unwrap_or_else(|| crate::coverage::default_digest_dir(&cfg.engine));
            // Read-only construction: an audit may run beside a live engine.
            let engine = Engine::new(cfg, streams).await?;
            let ledger = engine.store.list_batches().await?;
            let (digested, digest_files) = crate::coverage::read_digests(&digest_dir)?;
            let (ledger_rows, digest_rows) = (ledger.len(), digested.len());
            let all: Vec<_> = ledger.into_iter().chain(digested).collect();
            let storage = if *check_storage {
                Some(crate::coverage::check_storage(engine.backend.as_ref(), &all).await)
            } else {
                None
            };
            let report = crate::coverage::AuditReport {
                ledger_rows,
                digest_rows,
                digest_files,
                units: crate::coverage::audit(all),
                storage,
            };
            if let Some(path) = metrics_file {
                crate::coverage::write_textfile(
                    path,
                    &report.prometheus_text(chrono::Utc::now().timestamp()),
                )?;
            }
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                for line in report.lines() {
                    println!("{line}");
                }
            }
            if report.passed() {
                Ok(())
            } else {
                Err(VtopError::Other(
                    "coverage audit FAILED (see the report above)".into(),
                ))
            }
        }
        Command::Scrub {
            config,
            digest_dir,
            concurrency,
            max_objects_per_sec,
            max_bytes_per_sec,
            limit,
            cursor_file,
            sample,
            seed,
            metrics_file,
        } => {
            let (cfg, streams) = load(config)?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let digest_dir = digest_dir
                .clone()
                .unwrap_or_else(|| crate::coverage::default_digest_dir(&cfg.engine));
            let retention_days = cfg.engine.ledger_retention_days;
            // Read-only construction, as for audit-coverage: a scrub may run
            // beside a live engine.
            let engine = Engine::new(cfg, streams).await?;
            let ledger = engine.store.list_batches().await?;
            let (digested, digest_files) = crate::coverage::read_digests(&digest_dir)?;
            let digest_warning =
                crate::scrub::missing_digests_warning(retention_days, &digest_dir, digest_files);
            if let Some(warning) = &digest_warning {
                tracing::warn!("{warning}");
            }
            let sources = crate::scrub::RowSources {
                ledger_rows: ledger.len(),
                digest_rows: digested.len(),
                digest_files,
            };
            let to_usize = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
            let opts = crate::scrub::ScrubOptions {
                concurrency: *concurrency as usize,
                max_objects_per_sec: *max_objects_per_sec,
                max_bytes_per_sec: *max_bytes_per_sec,
                limit: limit.map(to_usize),
                cursor_file: cursor_file.clone(),
                sample: sample.map(to_usize),
                seed: *seed,
            };
            // The run's locks, from before anything it will replace is read
            // until it has published or rolled back (review of #652): an
            // overlapping run that published in between would otherwise be
            // overwritten from this run's older snapshot. Held to the end of
            // this arm.
            let lock = crate::scrub::lock_run(cursor_file.as_deref(), metrics_file.as_deref())?;
            // Read BEFORE the scrub, and only an absent file is a first run:
            // the totals and last-success time carry forward from it, so a
            // file that exists and cannot be read would otherwise be replaced
            // with counters reset and a success time of "never" (review).
            let previous = match metrics_file {
                Some(path) => match std::fs::read_to_string(path) {
                    Ok(text) => Some(text),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => {
                        return Err(VtopError::Other(format!(
                            "metrics file {} cannot be read ({e}); the scrub's totals carry \
                             forward from it",
                            path.display()
                        )))
                    }
                },
                None => None,
            };
            // Taken before the run moves the cursor: with --metrics-file, a run
            // counts only once its metrics are published (review of #652).
            let snapshot = match (cursor_file, metrics_file) {
                (Some(cursor), Some(_)) => Some(crate::scrub::CursorSnapshot::take(cursor)?),
                _ => None,
            };
            let mut report = engine
                .scrub_held(crate::scrub::committed_rows(ledger, digested), &opts, &lock)
                .await?;
            report.sources = sources;
            report.warnings.extend(digest_warning);
            // The report first, so the findings reach stdout whatever happens
            // to the metrics file.
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                for line in report.lines() {
                    println!("{line}");
                }
            }
            if let Some(path) = metrics_file {
                let written = crate::coverage::write_textfile(
                    path,
                    &report.prometheus_text(previous.as_deref(), chrono::Utc::now().timestamp()),
                );
                if let Err(error) = written {
                    // Unpublished, the run's findings are in no metric the
                    // alerts read. Put the cursor back, so the next run checks
                    // this slice again and publishes what it finds.
                    let undone = match &snapshot {
                        Some(snapshot) => match snapshot.restore(&lock) {
                            Ok(()) => "; the cursor was put back, so the next run checks \
                                       this slice again"
                                .to_string(),
                            Err(e) => format!("; the cursor could NOT be put back ({e})"),
                        },
                        None => String::new(),
                    };
                    return Err(VtopError::Other(format!(
                        "scrub metrics file {} could not be written ({error}){undone}",
                        path.display()
                    )));
                }
            }
            // Published (the report, and the metrics file when asked for):
            // only now may the cursor pass this run's findings.
            if let Some(pending) = report.pending_cursor.take() {
                pending.commit(&lock)?;
            }
            drop(lock);
            if report.passed {
                Ok(())
            } else {
                Err(VtopError::Other(format!(
                    "scrub FAILED: {} finding(s) (see the report above)",
                    report.findings.len()
                )))
            }
        }
        Command::PruneLedger {
            config,
            digest_dir,
            older_than_days,
            limit,
        } => {
            let cfg = VtopConfig::from_path(config)?;
            cfg.validate()?;
            init_tracing(
                cli.log_level.as_deref().unwrap_or(&cfg.engine.log_level),
                cli.json,
            );
            let state_store = cfg.engine.state_store.resolve()?;
            // The configured bound, as the engine uses (review of #568).
            let store = crate::engine::connect_configured_store(&cfg.engine, &state_store).await?;
            let cutoff = (chrono::Utc::now() - chrono::Duration::days(i64::from(*older_than_days)))
                .to_rfc3339();
            let digest_dir = digest_dir
                .clone()
                .unwrap_or_else(|| crate::coverage::default_digest_dir(&cfg.engine));
            let (deleted, digest) =
                crate::coverage::prune_with_digest(store.as_ref(), &cutoff, *limit, &digest_dir)
                    .await?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"status": "ok", "deleted": deleted, "digest": digest})
                );
            } else {
                match digest {
                    Some(path) => println!(
                        "pruned {deleted} committed ledger row(s); digest {}",
                        path.display()
                    ),
                    None => println!("pruned 0 committed ledger row(s)"),
                }
            }
            Ok(())
        }
        Command::Receipt { .. } => unreachable!("dispatched before run_command"),
        Command::Segment { .. } => unreachable!("dispatched before run_command"),
        Command::Meta { .. } => unreachable!("dispatched before run_command"),
        Command::Node { .. } => unreachable!("dispatched before run_command"),
        Command::Tier { .. } => unreachable!("dispatched before run_command"),
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
/// 2. its self-hash verifies and, when configured, its keyed MAC authenticates;
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
    manifest_mac_key: Option<&vtop_core::manifest::ManifestMacKey>,
) -> Result<VerifyReport, VtopError> {
    use vtop_core::checksum::digest_bytes;
    use vtop_core::types::ChecksumAlgorithm;

    let mut lines = Vec::new();
    let mut failed = false;

    // 1. Content, not metadata.
    let manifest_bytes = backend
        .get_object_bounded(manifest_uri, vtop_core::manifest::MAX_MANIFEST_BYTES)
        .await?;
    // The version is checked before anything else: a manifest from a newer
    // release is reported as that, never as a failed self-hash (#671).
    let parsed = match vtop_core::manifest::ParsedManifest::from_slice(&manifest_bytes) {
        Ok(parsed) => parsed,
        // Standalone, with no ledger to bind the bytes to, the claim is all
        // there is: reported as a claim this build cannot authenticate.
        Err(e @ VtopError::UnsupportedManifestVersion { .. }) => {
            return Err(e.at_manifest_uri(manifest_uri))
        }
        Err(e) => {
            return Err(VtopError::Other(format!(
                "manifest {manifest_uri} does not parse: {e}"
            )))
        }
    };
    lines.push(format!(
        "manifest        : {} ({} bytes, batch {})",
        manifest_uri,
        manifest_bytes.len(),
        parsed.fields().batch_id
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

    // A configured key upgrades the verdict from corruption detection to
    // authentication and deliberately rejects pre-key manifests. Without a
    // key, retain legacy behavior but do not claim an unverified MAC is valid.
    let authentication_ok = parsed.verify_authentication(manifest_mac_key).is_ok();
    failed |= !authentication_ok;
    let authentication_status = match (manifest_mac_key, parsed.fields().manifest.mac.as_deref()) {
        (Some(_), _) if authentication_ok => "OK (keyed BLAKE3)".to_string(),
        (Some(_), _) => "FAILED (MAC missing or invalid)".to_string(),
        (None, Some(_)) => "PRESENT (not checked: no key configured)".to_string(),
        (None, None) => "NOT CONFIGURED (self-hash only)".to_string(),
    };
    lines.push(format!("authentication  : {authentication_status}"));

    // 3. The stored object's actual content.
    let object_bytes = backend.get_object(&parsed.fields().object.uri).await?;
    let size_ok = object_bytes.len() as u64 == parsed.fields().object.size_bytes;
    failed |= !size_ok;
    lines.push(format!(
        "object size     : {} ({} bytes stored, {} expected)",
        if size_ok { "OK" } else { "FAILED" },
        object_bytes.len(),
        parsed.fields().object.size_bytes
    ));

    // LIMITED is reserved for an EXPLICIT `none` algorithm (the operator chose
    // to run without checksums). Anything else that prevents a content
    // comparison — an algorithm this build cannot compute, or a declared
    // algorithm with no recorded digest — is a hard failure: reporting it as
    // "limited" would hand a PASSED verdict to a same-sized replacement.
    let algo = parsed
        .fields()
        .object
        .checksum_algorithm
        .parse::<ChecksumAlgorithm>();
    let recomputed = algo
        .as_ref()
        .ok()
        .and_then(|a| digest_bytes(*a, &object_bytes));
    let checksum_status = match (&algo, &recomputed) {
        (Err(e), _) => {
            failed = true;
            format!("FAILED (cannot verify content: {e})")
        }
        (Ok(_), Some(actual)) if parsed.fields().object.checksum.is_empty() => {
            failed = true;
            format!(
                "FAILED (algorithm declared but no digest recorded; content hashes to {actual})"
            )
        }
        (Ok(_), Some(actual)) => {
            let ok = actual.eq_ignore_ascii_case(&parsed.fields().object.checksum);
            failed |= !ok;
            if ok {
                format!(
                    "OK ({} {})",
                    parsed.fields().object.checksum_algorithm,
                    actual
                )
            } else {
                format!(
                    "FAILED (stored content hashes to {actual}, manifest says {})",
                    parsed.fields().object.checksum
                )
            }
        }
        (Ok(_), None) => "LIMITED (checksums disabled for this batch)".to_string(),
    };
    lines.push(format!("object content  : {checksum_status}"));

    // 4. Ledger reconciliation.
    let row = store.get_batch(&parsed.fields().batch_id).await?;
    let ledger_status = match &row {
        None => "ABSENT (not a failure: manifest may belong to another engine's store)".to_string(),
        Some(r) => {
            let state_ok = matches!(r.state, BatchState::Verified | BatchState::SourceCommitted);
            // The ledger column is named object_sha256 but records the digest
            // of the CONFIGURED algorithm (engine.rs stores blake3 there too),
            // so compare whenever both digests exist — restricting this to
            // SHA-256 would let a coherently-replaced blake3 object+manifest
            // pass with a ledger that disagrees.
            let digest_ok = match (&r.object_sha256, &recomputed) {
                (Some(ledger), Some(actual)) => ledger.eq_ignore_ascii_case(actual),
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
        "batch_id": parsed.fields().batch_id,
        "object_uri": parsed.fields().object.uri,
        "self_hash_ok": self_hash_ok,
        "manifest_authentication": authentication_status,
        "manifest_authentication_required": manifest_mac_key.is_some(),
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
        RecoveryAction::None => "do nothing (committed, or superseded by a later commit)",
        _ => {
            "mark REPLAY_REQUIRED (source progress preserved), or SUPERSEDED if its source \
             has already committed past it"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `prune-ledger --limit 0` must fail at argument parsing: the store would
    // execute LIMIT 0 and report a "successful" zero-row prune, so a scheduled
    // maintenance run could silently retain all history (PR #213 review).
    #[test]
    fn prune_ledger_rejects_a_zero_limit() {
        let err = Cli::try_parse_from([
            "vtopctl",
            "prune-ledger",
            "--config",
            "vtop.yaml",
            "--older-than-days",
            "30",
            "--limit",
            "0",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn prune_ledger_accepts_the_default_limit() {
        let cli = Cli::try_parse_from([
            "vtopctl",
            "prune-ledger",
            "--config",
            "vtop.yaml",
            "--older-than-days",
            "30",
        ])
        .unwrap();
        match cli.command {
            Command::PruneLedger { limit, .. } => assert_eq!(limit, 500),
            other => panic!("parsed wrong command: {other:?}"),
        }
    }

    /// The scrub's bounds are refused at parsing when they could not bound
    /// anything (zero concurrency would never run a check; a zero or
    /// negative rate never admits one), and a cursor walk and a random
    /// sample are different runs, not one.
    #[test]
    fn scrub_rejects_bounds_that_bound_nothing_and_conflicting_modes() {
        let parse = |extra: &[&str]| {
            let mut args = vec!["vtopctl", "scrub", "--config", "vtop.yaml"];
            args.extend_from_slice(extra);
            Cli::try_parse_from(args)
        };
        match parse(&[]).unwrap().command {
            Command::Scrub {
                concurrency,
                limit,
                cursor_file,
                sample,
                ..
            } => {
                assert_eq!(concurrency as usize, crate::scrub::DEFAULT_CONCURRENCY);
                assert!(limit.is_none() && cursor_file.is_none() && sample.is_none());
            }
            other => panic!("parsed wrong command: {other:?}"),
        }
        assert!(parse(&["--cursor-file", "c", "--limit", "500"]).is_ok());
        assert!(parse(&["--sample", "10", "--seed", "7"]).is_ok());
        assert!(parse(&["--max-objects-per-sec", "0.5"]).is_ok());
        for bad in [
            &["--concurrency", "0"][..],
            &["--limit", "0"],
            &["--sample", "0"],
            &["--max-objects-per-sec", "0"],
            &["--max-objects-per-sec", "-1"],
            &["--max-objects-per-sec", "NaN"],
            &["--max-objects-per-sec", "1e-300"],
            &["--max-bytes-per-sec", "0"],
            &["--sample", "5", "--cursor-file", "c"],
            &["--sample", "5", "--limit", "5"],
            &["--seed", "7"],
        ] {
            assert!(parse(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn receipt_parses_and_prints_jsonl_and_rejects_a_bad_line() {
        let cli = Cli::try_parse_from(["vtopctl", "receipt", "--file", "r.jsonl"]).unwrap();
        match cli.command {
            Command::Receipt { file } => assert_eq!(file, std::path::PathBuf::from("r.jsonl")),
            other => panic!("parsed wrong command: {other:?}"),
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.jsonl");
        std::fs::write(
            &path,
            "{\"topic\":\"events\",\"kind\":\"dual_write\",\"primary_offset\":0,\"shadow_offset\":4,\"records\":2,\"sha256\":\"ab\"}\n",
        )
        .unwrap();
        assert_eq!(print_receipts(&path, true), 0);
        std::fs::write(&path, "not-json\n").unwrap();
        assert_eq!(print_receipts(&path, true), 1);
        std::fs::write(&path, "\n").unwrap();
        assert_eq!(print_receipts(&path, true), 1);
        assert_eq!(
            print_receipts(std::path::Path::new("/no/such/receipt.jsonl"), true),
            1
        );
    }
}
