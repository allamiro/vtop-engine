//! The audit trail of the ledger commands (#694), through the real binary:
//! what an operator and an auditor read is the binary's output, its log
//! stream, the audit file and the state store's `audit_events` table.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use vtop_core::state_machine::BatchState;
use vtop_state::{BatchPatch, SqliteStateStore, StateStore, VerificationClass};

fn write_config(dir: &Path) -> PathBuf {
    let config = dir.join("config.yaml");
    let input = dir.join("in.log");
    std::fs::write(&input, "").unwrap();
    let yaml = format!(
        r#"engine:
  name: vtop-audit-test
  tenant: default
  state_store: "sqlite://{state}"
  work_dir: {work}
  log_level: info
batching:
  max_records: 200
  max_bytes: 104857600
  max_batch_age_seconds: 60
  idle_poll_interval_ms: 2000
compression:
  type: gzip
  level: 1
checksum:
  algorithm: sha256
sources:
  file:
    enabled: true
    paths:
      - "{input}"
upload:
  backend: mock
  bucket: "telemetry-data"
  prefix: "audit-test"
  region: us-east-1
"#,
        state = dir.join("state.db").display(),
        work = dir.join("work").display(),
        input = input.display(),
    );
    std::fs::write(&config, yaml).unwrap();
    config
}

fn state_url(dir: &Path) -> String {
    format!("sqlite://{}", dir.join("state.db").display())
}

/// Run vtopctl with `VTOP_AUDIT_LOG` set to `audit_log` (when given), JSON
/// logs, and `RUST_LOG=error`: every audit record must still reach the log.
fn vtopctl(args: &[&str], audit_log: Option<&Path>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_vtopctl"));
    command
        .args(args)
        .env("VTOP_LOG_FORMAT", "json")
        .env("RUST_LOG", "error")
        .env_remove("VTOP_AUDIT_LOG")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .stdin(Stdio::null());
    if let Some(path) = audit_log {
        command.env("VTOP_AUDIT_LOG", path);
    }
    command.output().expect("spawn vtopctl")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn records(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// The audit records on the log stream (stderr, JSON lines).
fn logged(stderr: &[u8]) -> Vec<serde_json::Value> {
    text(stderr)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|l| l["target"] == "vtop::audit" && l["fields"]["record"].is_string())
        .collect()
}

/// Three committed rows for one Kafka partition: `prune-ledger
/// --older-than-days 0` may delete the two older ones (the newest is the
/// restart cursor and always survives).
async fn seed_committed(dir: &Path) {
    let store = SqliteStateStore::connect(&state_url(dir)).await.unwrap();
    for i in 0..3 {
        let id = format!("committed-{i}");
        store
            .save_batch_state(&vtop_state::test_battery::sample_record(&id))
            .await
            .unwrap();
        for state in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state(&id, state, &BatchPatch::default())
                .await
                .unwrap();
        }
        store
            .mark_verified(&id, VerificationClass::Strong)
            .await
            .unwrap();
        store.mark_source_committed(&id).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}

async fn batch_count(dir: &Path) -> usize {
    let store = SqliteStateStore::connect(&state_url(dir)).await.unwrap();
    store.list_batches().await.unwrap().len()
}

fn ledger_records(config: &Path) -> Vec<serde_json::Value> {
    let out = vtopctl(
        &[
            "--json",
            "audit",
            "list",
            "--config",
            config.to_str().unwrap(),
        ],
        None,
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    text(&out.stdout)
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// `prune-ledger` writes its attempt before it deletes and its outcome
/// after, to the log stream (under RUST_LOG=error), the audit file and the
/// ledger's audit table, and names the record in its output.
#[tokio::test]
async fn prune_ledger_records_an_attempt_then_its_outcome_in_every_sink() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    seed_committed(dir.path()).await;
    let audit_log = dir.path().join("audit.jsonl");
    let digests = dir.path().join("digests");
    let out = vtopctl(
        &[
            "prune-ledger",
            "--config",
            config.to_str().unwrap(),
            "--older-than-days",
            "0",
            "--digest-dir",
            digests.to_str().unwrap(),
        ],
        Some(&audit_log),
    );
    let stdout = text(&out.stdout);
    assert!(out.status.success(), "{stdout}\n{}", text(&out.stderr));
    assert_eq!(
        batch_count(dir.path()).await,
        1,
        "the two older rows are pruned"
    );

    let file = records(&audit_log);
    assert_eq!(file.len(), 2, "one attempt and one outcome: {file:#?}");
    let (attempt, done) = (&file[0], &file[1]);
    assert_eq!(attempt["audit_version"], 1);
    assert_eq!(attempt["action"], "ledger.prune");
    assert_eq!(attempt["outcome"], "attempt");
    assert_eq!(attempt["stage"], "local");
    assert_eq!(attempt["actor"]["kind"], "os");
    assert_eq!(attempt["actor"]["source"], "os");
    assert!(attempt["actor"]["uid"].is_u64());
    assert_eq!(attempt["params"]["older_than_days"], "0");
    assert_eq!(attempt["params"]["limit"], "500");
    assert_eq!(attempt["engine"], "vtop-audit-test");
    assert_eq!(done["outcome"], "success");
    assert_eq!(done["after"]["deleted"], 2);
    assert_eq!(done["correlation"]["attempt_id"], attempt["audit_id"]);
    assert_eq!(
        done["correlation"]["request_id"],
        attempt["correlation"]["request_id"]
    );
    let audit_id = done["audit_id"].as_str().unwrap();
    assert!(
        stdout.contains(&format!("audit record {audit_id}")),
        "the operator is told where the record is: {stdout}"
    );

    let log = logged(&out.stderr);
    assert_eq!(
        log.len(),
        2,
        "both records reach the log stream under RUST_LOG=error: {}",
        text(&out.stderr)
    );
    assert_eq!(log[1]["fields"]["audit_id"], audit_id);

    let ledger = ledger_records(&config);
    assert_eq!(
        ledger, file,
        "the ledger rows are the records, byte for byte"
    );
    // Pruning never deletes audit rows: a second prune adds two more.
    let again = vtopctl(
        &[
            "prune-ledger",
            "--config",
            config.to_str().unwrap(),
            "--older-than-days",
            "0",
            "--digest-dir",
            digests.to_str().unwrap(),
        ],
        Some(&audit_log),
    );
    assert!(again.status.success(), "{}", text(&again.stderr));
    assert_eq!(ledger_records(&config).len(), 4);
    let verify = vtopctl(
        &["audit", "verify", "--config", config.to_str().unwrap()],
        None,
    );
    assert!(
        verify.status.success() && text(&verify.stdout).contains("4 row(s)"),
        "{}\n{}",
        text(&verify.stdout),
        text(&verify.stderr)
    );
}

/// The plan's refusal: when the attempt cannot be written to the ledger's
/// audit table, nothing is pruned, the operator is told so and what to do,
/// and the refusal itself is recorded where it can be.
#[tokio::test]
async fn prune_ledger_is_refused_when_its_attempt_cannot_be_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    seed_committed(dir.path()).await;
    // A store whose audit insert fails (as a full disk or a missing grant
    // would make it fail); the trigger survives the schema step on open.
    sql(
        dir.path(),
        "CREATE TRIGGER fail_audit BEFORE INSERT ON audit_events \
         BEGIN SELECT RAISE(ABORT, 'disk I/O error (test)'); END",
    )
    .await;
    let audit_log = dir.path().join("audit.jsonl");
    let out = vtopctl(
        &[
            "prune-ledger",
            "--config",
            config.to_str().unwrap(),
            "--older-than-days",
            "0",
            "--digest-dir",
            dir.path().join("digests").to_str().unwrap(),
        ],
        Some(&audit_log),
    );
    let stderr = text(&out.stderr);
    assert!(!out.status.success(), "the prune must be refused: {stderr}");
    assert!(
        stderr.contains("could not record the audit attempt")
            && stderr.contains("prune-ledger was not run and the ledger was not changed")
            && stderr.contains("disk I/O error (test)"),
        "what happened, what the engine did, what to do: {stderr}"
    );
    assert_eq!(batch_count(dir.path()).await, 3, "nothing was pruned");
    let file = records(&audit_log);
    let outcomes: Vec<_> = file.iter().map(|r| r["outcome"].clone()).collect();
    assert_eq!(outcomes, vec!["attempt", "refused"], "{file:#?}");
    assert!(file[1]["reason"]
        .as_str()
        .unwrap()
        .contains("attempt record could not be written"));
}

/// Raw SQL on the test's ledger file, as someone with the file could run it.
async fn sql(dir: &Path, statement: &'static str) {
    SqliteStateStore::connect(&state_url(dir))
        .await
        .unwrap()
        .execute_raw(statement)
        .await
        .unwrap();
}

/// `VTOP_AUDIT_LOG` naming a file that cannot be opened stops the command
/// at startup, naming the path, before anything is changed.
#[tokio::test]
async fn an_unwritable_audit_file_fails_startup_naming_the_path() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    seed_committed(dir.path()).await;
    let missing = dir.path().join("no-such-dir").join("audit.jsonl");
    let out = vtopctl(
        &[
            "prune-ledger",
            "--config",
            config.to_str().unwrap(),
            "--older-than-days",
            "0",
            "--digest-dir",
            dir.path().join("digests").to_str().unwrap(),
        ],
        Some(&missing),
    );
    let stderr = text(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains(&format!("VTOP_AUDIT_LOG={}", missing.display()))
            && stderr.contains("Nothing was run"),
        "{stderr}"
    );
    assert_eq!(batch_count(dir.path()).await, 3);
}

/// `replay` records its attempt before recovery moves a row, and its
/// outcome with what recovery did; `--batch-id` names the batch.
#[tokio::test]
async fn replay_records_what_recovery_did() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    {
        let store = SqliteStateStore::connect(&state_url(dir.path()))
            .await
            .unwrap();
        store
            .save_batch_state(&vtop_state::test_battery::sample_record("stuck-1"))
            .await
            .unwrap();
    }
    let audit_log = dir.path().join("audit.jsonl");
    let out = vtopctl(
        &[
            "replay",
            "--config",
            config.to_str().unwrap(),
            "--batch-id",
            "stuck-1",
        ],
        Some(&audit_log),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let file = records(&audit_log);
    assert_eq!(file.len(), 2, "{file:#?}");
    assert_eq!(file[0]["action"], "ledger.replay");
    assert_eq!(file[0]["outcome"], "attempt");
    // Recovery covers every incomplete batch, whatever --batch-id names
    // (it only prints that batch's state): the target is the ledger
    // (review of #705).
    assert_eq!(file[0]["target"]["kind"], "ledger");
    assert_eq!(file[0]["params"]["batch_id"], "stuck-1");
    assert_eq!(file[1]["outcome"], "success");
    let after = &file[1]["after"];
    assert_eq!(
        after["replay_required"], 1,
        "the incomplete row was sent back to be re-read: {after}"
    );
    for key in ["committed", "superseded", "still_pending"] {
        assert!(after[key].is_u64(), "{key} is recorded: {after}");
    }
    assert_eq!(ledger_records(&config).len(), 2);
}

/// `migrate` cannot write its attempt row into the table it creates: the
/// attempt goes to the log and the file first, and both rows follow.
#[tokio::test]
async fn migrate_records_its_attempt_before_and_its_rows_after() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    let audit_log = dir.path().join("audit.jsonl");
    let out = vtopctl(
        &["migrate", "--config", config.to_str().unwrap()],
        Some(&audit_log),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let file = records(&audit_log);
    assert_eq!(file.len(), 2, "{file:#?}");
    assert_eq!(file[0]["action"], "ledger.migrate");
    assert_eq!(file[0]["params"]["backend"], "sqlite");
    assert_eq!(file[1]["outcome"], "success");
    assert!(
        !serde_json::to_string(&file).unwrap().contains("sqlite://"),
        "the connection string is never recorded"
    );
    assert_eq!(ledger_records(&config), file);
}

/// `audit list --file` reads a node's audit file, and the table's chain
/// breaks when a row is edited.
#[tokio::test]
async fn audit_verify_finds_an_edited_row() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    let out = vtopctl(&["migrate", "--config", config.to_str().unwrap()], None);
    assert!(out.status.success(), "{}", text(&out.stderr));
    let verify = vtopctl(
        &["audit", "verify", "--config", config.to_str().unwrap()],
        None,
    );
    assert!(verify.status.success(), "{}", text(&verify.stdout));
    sql(
        dir.path(),
        "DROP TRIGGER trg_audit_events_no_update; \
         UPDATE audit_events SET record = replace(record, 'sqlite', 'postgresql') WHERE seq = 1",
    )
    .await;
    let verify = vtopctl(
        &["audit", "verify", "--config", config.to_str().unwrap()],
        None,
    );
    let stdout = text(&verify.stdout);
    assert!(!verify.status.success(), "{stdout}");
    assert!(
        stdout.contains("BROKEN: row 1") && stdout.contains("edited"),
        "{stdout}"
    );
}

/// A scrub that keeps a cursor records the run: whether the cursor moved,
/// and the cursor file it keeps.
#[tokio::test]
async fn a_scrub_that_keeps_a_cursor_is_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    let audit_log = dir.path().join("audit.jsonl");
    let cursor = dir.path().join("scrub.cursor");
    let out = vtopctl(
        &[
            "scrub",
            "--config",
            config.to_str().unwrap(),
            "--cursor-file",
            cursor.to_str().unwrap(),
            "--limit",
            "10",
            "--digest-dir",
            dir.path().join("digests").to_str().unwrap(),
        ],
        Some(&audit_log),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let file = records(&audit_log);
    assert_eq!(file.len(), 1, "{file:#?}");
    assert_eq!(file[0]["action"], "scrub.cursor");
    assert_eq!(file[0]["outcome"], "success");
    assert_eq!(
        file[0]["params"]["cursor_file"],
        cursor.display().to_string()
    );
    assert_eq!(file[0]["params"]["limit"], "10");
    assert!(file[0]["after"]["cursor_moved"].is_boolean());
    // Without a cursor the scrub changes nothing and writes no record.
    let out = vtopctl(
        &[
            "scrub",
            "--config",
            config.to_str().unwrap(),
            "--digest-dir",
            dir.path().join("digests").to_str().unwrap(),
        ],
        Some(&audit_log),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert_eq!(records(&audit_log).len(), 1);
}

/// `node repair` and `node reconfigure-range` record the local actor and
/// their outcome, a failure included.
#[tokio::test]
async fn node_commands_record_the_local_actor_and_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let audit_log = dir.path().join("audit.jsonl");
    let node_config = dir.path().join("node.yaml");
    std::fs::write(&node_config, "not: [a node config").unwrap();
    let out = vtopctl(
        &[
            "node",
            "repair",
            "--config",
            node_config.to_str().unwrap(),
            "--from",
            "00000000-0000-0000-0000-000000000bee",
            "--into",
            dir.path().join("replica").to_str().unwrap(),
            "--fencing-epoch",
            "7",
        ],
        Some(&audit_log),
    );
    assert!(!out.status.success());
    let out = vtopctl(
        &[
            "node",
            "reconfigure-range",
            "--data-dir",
            dir.path().join("no-such-range").to_str().unwrap(),
            "--max-segment-bytes",
            "1048576",
        ],
        Some(&audit_log),
    );
    assert!(!out.status.success());
    let file = records(&audit_log);
    assert_eq!(file.len(), 2, "{file:#?}");
    assert_eq!(file[0]["action"], "node.repair");
    assert_eq!(file[0]["outcome"], "failure");
    assert_eq!(file[0]["actor"]["kind"], "os");
    assert_eq!(file[0]["params"]["side"], "client");
    assert_eq!(file[0]["params"]["fencing_epoch"], "7");
    assert!(file[0]["reason"].is_string());
    assert_eq!(file[1]["action"], "node.reconfigure_range");
    assert_eq!(file[1]["outcome"], "failure");
    assert_eq!(file[1]["params"]["max_segment_bytes"], "1048576");
}

/// Review of #705: a config that names a manifest MAC key keys the chain, so
/// `prune-ledger` needs the key and says why when it is missing; reading the
/// trail does not need it.
#[tokio::test]
async fn reading_the_trail_needs_no_key_and_a_keyed_prune_names_the_missing_one() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    let yaml = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("manifest_mac_key_env: VTOP_AUDIT_TEST_KEY_NOT_SET\n{yaml}"),
    )
    .unwrap();
    seed_committed(dir.path()).await;
    let out = vtopctl(
        &["audit", "list", "--config", config.to_str().unwrap()],
        None,
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let out = vtopctl(
        &[
            "prune-ledger",
            "--config",
            config.to_str().unwrap(),
            "--older-than-days",
            "0",
            "--digest-dir",
            dir.path().join("digests").to_str().unwrap(),
        ],
        None,
    );
    let stderr = text(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains("VTOP_AUDIT_TEST_KEY_NOT_SET") && stderr.contains("audit"),
        "{stderr}"
    );
    assert_eq!(batch_count(dir.path()).await, 3);
}
