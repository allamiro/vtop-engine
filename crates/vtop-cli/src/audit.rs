//! The engine's audit records (#694), and `vtopctl audit`.
//!
//! The record and its sinks live in `vtop_observe::audit`; the table in
//! `vtop_state::audit`. What is here is the engine's use of them:
//!
//! * the ledger commands (`prune-ledger`, `replay`, `migrate`) write an
//!   `attempt` record to every sink before they act and refuse to act when
//!   it cannot be written ([`LedgerAudit`]), then a terminal record naming
//!   the attempt;
//! * the cluster and node clients (`vtopctl meta`, `vtopctl node`) write the
//!   client's record of what they asked ([`client_record`]);
//! * `vtopctl audit list` and `vtopctl audit verify` read the records back.

use std::io::BufRead as _;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use vtop_core::errors::VtopError;
use vtop_core::manifest::ManifestKeyring;
use vtop_observe::audit::{
    self as audit, Action, Actor, AuditEvent, Outcome, ReadRecord, Sink, Stage, Target, LOG_ENV,
};
use vtop_state::{AuditInsert, StateStore};

/// Open the audit file `VTOP_AUDIT_LOG` names, if any. A set but unwritable
/// path is a startup error naming it.
pub fn init_sink() -> Result<(), VtopError> {
    audit::init_file_sink_from_env()
        .map(|_| ())
        .map_err(|e| VtopError::Config(e.to_string()))
}

/// Count audit records in the engine's registry.
pub fn bind_counters(metrics: &vtop_core::telemetry::Metrics) {
    audit::set_counters(
        metrics.audit_events_total.clone(),
        metrics.audit_write_failures_total.clone(),
    );
}

/// The records of one ledger command: the log stream and the audit file
/// through `vtop_observe::audit`, and the state store's `audit_events` table
/// when a store is given. The chain is keyed with the keyring's signing key
/// (#685) when one is configured.
pub struct LedgerAudit {
    /// The command as the operator typed it, for messages.
    pub command: &'static str,
    pub keyring: ManifestKeyring,
}

impl LedgerAudit {
    /// Write `attempt` to every sink before the command acts. Any sink that
    /// cannot take it refuses the command: `Err` says so, nothing was run,
    /// and a `refused` record goes to whichever sinks still work.
    pub async fn attempt(
        &self,
        store: Option<&dyn StateStore>,
        attempt: &AuditEvent,
    ) -> Result<(), VtopError> {
        let failure = match self.write(store, attempt).await {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        let refused = attempt.terminal(Outcome::Refused).reason(format!(
            "the attempt record could not be written: {failure}"
        ));
        // Best effort: the sink that failed may fail again.
        let _ = self.write(store, &refused).await;
        Err(VtopError::Other(format!(
            "could not record the audit attempt ({failure}); {} was not run and the ledger was \
             not changed. Fix the audit sink that failed (the state store's audit_events \
             table: run `vtopctl migrate` on PostgreSQL and check the runtime role's grants; \
             or the file {LOG_ENV} names) and run it again",
            self.command
        )))
    }

    /// Write the command's terminal record. `Err` means the command ran and
    /// its outcome could not be recorded in every sink.
    pub async fn finish(
        &self,
        store: Option<&dyn StateStore>,
        terminal: &AuditEvent,
    ) -> Result<(), VtopError> {
        self.write(store, terminal).await.map_err(|failure| {
            VtopError::Other(format!(
                "{} ran, but its outcome record {} could not be written ({failure}); the \
                 attempt record and the log stream (target {}) hold what was asked. Fix the \
                 audit sink that failed",
                self.command,
                terminal.audit_id,
                audit::TARGET
            ))
        })
    }

    /// Append a record already written to the log and the file to the
    /// ledger's table as well: `migrate`'s attempt, once the table exists.
    pub async fn append_to_ledger(
        &self,
        store: &dyn StateStore,
        event: &AuditEvent,
    ) -> Result<(), VtopError> {
        let row = AuditInsert::from_record(&event.to_json()).map_err(VtopError::Other)?;
        store
            .append_audit(&row, &self.keyring)
            .await
            .map(|_| ())
            .map_err(|e| {
                audit::count_failure(Sink::Ledger);
                VtopError::Other(format!(
                    "{} ran, but its record {} could not be written to the state store's \
                     audit_events table ({e}); the log stream (target {}) holds it",
                    self.command,
                    event.audit_id,
                    audit::TARGET
                ))
            })
    }

    async fn write(
        &self,
        store: Option<&dyn StateStore>,
        event: &AuditEvent,
    ) -> Result<(), String> {
        let recorded = audit::record(event).map_err(|e| e.to_string());
        let json = match &recorded {
            Ok(recorded) => recorded.json.clone(),
            Err(_) => event.to_json(),
        };
        let ledger = match store {
            None => Ok(()),
            Some(store) => {
                let row = AuditInsert::from_record(&json).map_err(|e| e.to_string())?;
                store
                    .append_audit(&row, &self.keyring)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        audit::count_failure(Sink::Ledger);
                        tracing::error!(
                            target: "vtop::audit",
                            audit_id = %event.audit_id,
                            action = event.action.as_str(),
                            error = %e,
                            "the audit record could not be written to the state store's \
                             audit_events table; it is in the log stream"
                        );
                        format!("state store audit_events: {e}")
                    })
            }
        };
        match (recorded, ledger) {
            (Ok(_), Ok(())) => Ok(()),
            (Err(file), Ok(())) => Err(file),
            (Ok(_), Err(ledger)) => Err(ledger),
            (Err(file), Err(ledger)) => Err(format!("{file}; {ledger}")),
        }
    }
}

/// A local ledger command's attempt record.
pub fn ledger_attempt(action: Action, target: Target, engine: &str) -> AuditEvent {
    AuditEvent::new(
        action,
        Stage::Local,
        Actor::local(),
        target,
        Outcome::Attempt,
    )
    .engine(engine)
    .request_id(uuid::Uuid::new_v4())
}

/// Write a record to the log stream and the audit file. A file that cannot
/// take it is reported on stderr (and counted by the sink); the caller's
/// command is not one that is refused for it.
pub fn record_best_effort(event: &AuditEvent) {
    if let Err(error) = audit::record(event) {
        eprintln!("warning: {error}");
    }
}

fn action_of(action: vtop_meta::audit::AuditAction) -> Action {
    use vtop_meta::audit::AuditAction;
    match action {
        AuditAction::ClusterInit => Action::ClusterInit,
        AuditAction::ClusterMembership => Action::ClusterMembership,
        AuditAction::ClusterEnableFeatures => Action::ClusterEnableFeatures,
        AuditAction::ClusterCreateTopic => Action::ClusterCreateTopic,
        AuditAction::ClusterPropose => Action::ClusterPropose,
        AuditAction::PromotionAttest => Action::PromotionAttest,
    }
}

/// The client's record of a metadata command it sent (`vtopctl meta`,
/// `vtopctl node promote-unproven`, `vtopctl tier`): the local actor, what
/// was asked, and what came back. `None` for a command that is not audited.
pub fn client_record(command: &vtop_meta::MetadataCommand, outcome: Outcome) -> Option<AuditEvent> {
    let described = vtop_meta::audit::command_audit(command)?;
    let action = action_of(described.action);
    let mut event = AuditEvent::new(
        action,
        Stage::Client,
        Actor::local(),
        Target::new(described.target_kind, described.target_id),
        outcome,
    )
    .request_id(command.envelope().request_id)
    .param("command", described.command);
    for (name, value) in described.params {
        if action.params().contains(&name) {
            event = event.param(name, value);
        }
    }
    Some(event)
}

/// The client's record of a bootstrap or membership request.
pub fn membership_record(
    action: Action,
    params: &[(&str, String)],
    outcome: Outcome,
) -> AuditEvent {
    let mut event = AuditEvent::new(
        action,
        Stage::Client,
        Actor::local(),
        Target::new("cluster", None),
        outcome,
    );
    for (name, value) in params {
        event = event.param(name, value);
    }
    event
}

// ---- vtopctl audit ----------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum AuditCommand {
    /// List audit records, oldest first: from the state store's
    /// audit_events table (--config), or from an audit file an engine or a
    /// node wrote (--file, the JSONL file VTOP_AUDIT_LOG names). A record
    /// from a newer release is printed raw with a warning, never dropped.
    List(ListArgs),
    /// Recompute the audit table's hash chain (--config). An edited,
    /// deleted or reordered row breaks it; rows keyed with the manifest MAC
    /// key need that key configured. Prints the row count and the last
    /// link: keep them elsewhere to detect rows removed from the end, which
    /// no chain can show. Exits non-zero on any break.
    Verify {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Args, Debug)]
#[command(group(clap::ArgGroup::new("source").required(true).args(["config", "file"])))]
pub struct ListArgs {
    /// Read the state store's audit_events table.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Read a JSONL audit file.
    #[arg(long)]
    pub file: Option<PathBuf>,
    /// Records at or after this time (RFC 3339).
    #[arg(long, value_parser = rfc3339)]
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// Records at or before this time (RFC 3339).
    #[arg(long, value_parser = rfc3339)]
    pub until: Option<chrono::DateTime<chrono::Utc>>,
    /// Records whose actor has this name, uid, or sudo user.
    #[arg(long)]
    pub actor: Option<String>,
    /// Records of this action (for example ledger.prune).
    #[arg(long)]
    pub action: Option<String>,
    /// Records whose target has this id or kind.
    #[arg(long)]
    pub target: Option<String>,
    /// Records with this outcome (attempt, success, failure, refused).
    #[arg(long)]
    pub outcome: Option<String>,
    /// Print at most this many records: the newest that match.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    pub limit: u64,
}

fn rfc3339(value: &str) -> Result<chrono::DateTime<chrono::Utc>, String> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|t| t.with_timezone(&chrono::Utc))
        .map_err(|e| format!("not an RFC 3339 time ({e}); for example 2026-11-14T09:00:00Z"))
}

impl ListArgs {
    fn matches(&self, raw: &serde_json::Value) -> bool {
        let text = |path: &[&str]| -> Option<String> {
            let mut at = raw;
            for key in path {
                at = at.get(*key)?;
            }
            match at {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            }
        };
        if let Some(action) = &self.action {
            if text(&["action"]).as_deref() != Some(action) {
                return false;
            }
        }
        if let Some(outcome) = &self.outcome {
            if text(&["outcome"]).as_deref() != Some(outcome) {
                return false;
            }
        }
        if let Some(actor) = &self.actor {
            let names = [
                text(&["actor", "name"]),
                text(&["actor", "uid"]),
                text(&["actor", "sudo_user"]),
            ];
            if !names.iter().any(|n| n.as_deref() == Some(actor)) {
                return false;
            }
        }
        if let Some(target) = &self.target {
            let ids = [text(&["target", "id"]), text(&["target", "kind"])];
            if !ids.iter().any(|n| n.as_deref() == Some(target)) {
                return false;
            }
        }
        if self.since.is_some() || self.until.is_some() {
            let Some(time) = text(&["time"])
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t).ok())
                .map(|t| t.with_timezone(&chrono::Utc))
            else {
                return false;
            };
            if self.since.is_some_and(|since| time < since)
                || self.until.is_some_and(|until| time > until)
            {
                return false;
            }
        }
        true
    }
}

/// `vtopctl audit`. Returns the exit code.
pub async fn run(command: AuditCommand, json: bool) -> i32 {
    match run_inner(command, json).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error}");
            1
        }
    }
}

async fn run_inner(command: AuditCommand, json: bool) -> Result<i32, VtopError> {
    match command {
        AuditCommand::List(args) => {
            let lines = match (&args.config, &args.file) {
                (Some(config), _) => {
                    let (store, _) = open_store(config).await?;
                    all_rows(store.as_ref())
                        .await?
                        .into_iter()
                        .map(|row| row.record)
                        .collect()
                }
                (None, Some(file)) => read_file(file)?,
                (None, None) => unreachable!("clap requires --config or --file"),
            };
            print_records(&args, &lines, json, &mut std::io::stdout().lock());
            Ok(0)
        }
        AuditCommand::Verify { config } => {
            let (store, key) = open_store(&config).await?;
            let rows = all_rows(store.as_ref()).await?;
            let report = vtop_state::verify_chain(&rows, &key);
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "passed": report.passed(),
                        "rows": report.rows,
                        "keyed_rows": report.keyed_rows,
                        "last_seq": report.last_seq,
                        "last_hash": report.last_hash,
                        "problems": report.problems,
                    })
                );
            } else {
                println!(
                    "audit chain: {} row(s), {} keyed; last row {} link {}",
                    report.rows,
                    report.keyed_rows,
                    report
                        .last_seq
                        .map_or_else(|| "-".to_owned(), |s| s.to_string()),
                    report.last_hash.as_deref().unwrap_or("-")
                );
                for problem in &report.problems {
                    println!("BROKEN: {problem}");
                }
                println!(
                    "{}",
                    if report.passed() {
                        "verdict: PASSED (keep the row count and last link elsewhere: a chain \
                         cannot show rows removed from its end)"
                    } else {
                        "verdict: FAILED"
                    }
                );
            }
            Ok(i32::from(!report.passed()))
        }
    }
}

async fn open_store(config: &Path) -> Result<(Box<dyn StateStore>, ManifestKeyring), VtopError> {
    let cfg = vtop_core::config::VtopConfig::from_path(config)?;
    cfg.validate()?;
    let key = cfg.resolve_manifest_keyring()?;
    let state_store = cfg.engine.state_store.resolve()?;
    let store = crate::engine::connect_configured_store(&cfg.engine, &state_store).await?;
    Ok((store, key))
}

async fn all_rows(store: &dyn StateStore) -> Result<Vec<vtop_state::AuditRow>, VtopError> {
    let mut rows = Vec::new();
    let mut after = 0;
    loop {
        let page = store.list_audit(after, 1000).await?;
        let Some(last) = page.last() else { break };
        after = last.seq;
        rows.extend(page);
    }
    Ok(rows)
}

fn read_file(path: &Path) -> Result<Vec<String>, VtopError> {
    let file = std::fs::File::open(path)
        .map_err(|e| VtopError::Other(format!("read {}: {e}", path.display())))?;
    let mut lines = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line.map_err(|e| VtopError::Other(format!("read {}: {e}", path.display())))?;
        if !line.trim().is_empty() {
            lines.push(line);
        }
    }
    Ok(lines)
}

/// Print the newest `args.limit` records that match, oldest first. A
/// record this build cannot fully read is printed raw, with a warning on
/// stderr naming why; a line that is not JSON at all is reported and
/// skipped, never silently.
pub fn print_records(args: &ListArgs, lines: &[String], json: bool, out: &mut dyn std::io::Write) {
    let mut matched = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        match audit::read_record(line) {
            ReadRecord::Malformed { error, .. } => {
                eprintln!(
                    "warning: record {} is not JSON ({error}); skipped",
                    index + 1
                );
            }
            ReadRecord::V1(event) => {
                let raw = serde_json::to_value(&*event).unwrap_or_default();
                if args.matches(&raw) {
                    matched.push((line, Some(event), None));
                }
            }
            ReadRecord::Unknown { raw, why } => {
                if args.matches(&raw) {
                    matched.push((line, None, Some(why)));
                }
            }
        }
    }
    let skip = matched
        .len()
        .saturating_sub(usize::try_from(args.limit).unwrap_or(usize::MAX));
    if !json {
        let _ = writeln!(
            out,
            "{:<24} {:<24} {:<9} {:<8} {:<32} {:<24} AUDIT_ID",
            "TIME", "ACTION", "OUTCOME", "STAGE", "ACTOR", "TARGET"
        );
    }
    for (line, event, why) in matched.into_iter().skip(skip) {
        if let Some(why) = why {
            eprintln!("warning: {why}; printed as stored");
            let _ = writeln!(out, "{line}");
            continue;
        }
        let Some(event) = event else { continue };
        if json {
            // The record exactly as stored: the stable audit_version 1 form.
            let _ = writeln!(out, "{line}");
        } else {
            let target = match &event.target.id {
                Some(id) => format!("{}:{id}", event.target.kind),
                None => event.target.kind.clone(),
            };
            let _ = writeln!(
                out,
                "{:<24} {:<24} {:<9} {:<8} {:<32} {:<24} {}{}",
                event.time,
                event.action.as_str(),
                event.outcome.as_str(),
                serde_json::to_value(event.stage)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default(),
                event.actor.describe(),
                target,
                event.audit_id,
                event
                    .reason
                    .as_deref()
                    .map(|r| format!("  ({r})"))
                    .unwrap_or_default()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct Wrapper {
        #[command(flatten)]
        args: ListArgs,
    }

    fn args(extra: &[&str]) -> ListArgs {
        let mut argv = vec!["audit", "--file", "x.jsonl"];
        argv.extend_from_slice(extra);
        Wrapper::try_parse_from(argv).unwrap().args
    }

    fn fixture() -> Vec<String> {
        include_str!("../tests/fixtures/audit/audit-v1.jsonl")
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn listed(args: &ListArgs, json: bool) -> String {
        let mut out = Vec::new();
        print_records(args, &fixture(), json, &mut out);
        String::from_utf8(out).unwrap()
    }

    /// The fixture is records as v0.10.0 writes them; a later release must
    /// keep reading `audit_version` 1 (ddia-encoding-evolution rule 4).
    #[test]
    fn the_committed_version_1_records_still_read() {
        let lines = fixture();
        let v1 = lines
            .iter()
            .filter(|l| matches!(audit::read_record(l), ReadRecord::V1(_)))
            .count();
        assert_eq!(
            v1,
            lines.len() - 1,
            "every record but the newer one reads as v1"
        );
    }

    #[test]
    fn list_filters_by_action_actor_target_outcome_and_time() {
        let out = listed(&args(&["--action", "ledger.prune"]), false);
        assert_eq!(out.lines().count(), 1 + 2, "{out}");
        let out = listed(&args(&["--actor", "ops-alice"]), true);
        assert!(out.lines().all(|l| l.contains("ops-alice")), "{out}");
        assert_eq!(out.lines().count(), 2, "{out}");
        let out = listed(&args(&["--outcome", "refused"]), true);
        assert_eq!(out.lines().count(), 1, "{out}");
        let out = listed(&args(&["--target", "ledger", "--outcome", "success"]), true);
        assert_eq!(out.lines().count(), 1, "{out}");
        let out = listed(
            &args(&[
                "--since",
                "2026-11-14T10:00:00Z",
                "--until",
                "2026-11-14T10:00:01Z",
            ]),
            true,
        );
        assert_eq!(out.lines().count(), 2, "{out}");
        let out = listed(&args(&["--limit", "1"]), true);
        assert_eq!(out.lines().count(), 1);
        assert!(out.contains("audit_version\":2"), "the newest: {out}");
    }

    /// `--json` prints each record exactly as stored, so a script reads the
    /// stable schema; a newer record is printed raw, never dropped.
    #[test]
    fn json_output_is_the_stored_record_and_a_newer_one_is_kept() {
        let out = listed(&args(&[]), true);
        let lines = fixture();
        assert_eq!(
            out.lines().collect::<Vec<_>>(),
            lines.iter().map(String::as_str).collect::<Vec<_>>()
        );
        let text = listed(&args(&[]), false);
        assert!(
            text.contains(r#""audit_version":2"#),
            "a newer record is printed raw: {text}"
        );
        assert!(text.contains("ledger.prune"));
    }
}
