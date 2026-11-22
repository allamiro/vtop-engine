//! The ledger's own tamper evidence, with no object store: the audit trail's
//! hash chain (`audit_events`, `vtopctl audit verify`, #694) and the audit of
//! coverage (`audit-coverage`, #560).
//!
//! Every case is a pair: someone with the ledger file changes it, then the
//! defence reports or refuses it. Where the defence cannot see the change,
//! the test says so by asserting the verdict that is wrong. The cases that
//! need a store are in `live_s3_tamper.rs`; the document that lists both is
//! `docs/reference/tamper-evidence.md`.
//!
//! The trail is written by the real binary (`prune-ledger`, which records an
//! attempt and an outcome in `audit_events`), read by the real binary
//! (`audit verify`), and changed with SQL, as someone who can write the
//! SQLite file could.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use sqlx::ConnectOptions;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::manifest::{audit_chain_link, ManifestKeyring, ManifestMacKey};
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_state::{BatchPatch, SqliteStateStore, StateStore, VerificationClass};
use vtop_upload::{MockBackend, UploadBackend};

const KEY_ENV: &str = "VTOP_TAMPER_AUDIT_KEY";
const KEY_HEX: &str = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e";

fn write_config(dir: &Path, keyed: bool) -> PathBuf {
    let config = dir.join("config.yaml");
    let input = dir.join("in.log");
    std::fs::write(&input, "").unwrap();
    let key = if keyed {
        format!("manifest_mac_key_env: {KEY_ENV}\n")
    } else {
        String::new()
    };
    let yaml = format!(
        r#"{key}engine:
  name: vtop-tamper-test
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
  prefix: "tamper-test"
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

fn vtopctl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(args)
        .env("VTOP_LOG_FORMAT", "json")
        .env("RUST_LOG", "error")
        .env(KEY_ENV, KEY_HEX)
        .env_remove("VTOP_AUDIT_LOG")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .stdin(Stdio::null())
        .output()
        .expect("spawn vtopctl")
}

/// `vtopctl audit verify` without the key: only what the chain itself shows.
fn vtopctl_without_key(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(args)
        .env("VTOP_LOG_FORMAT", "json")
        .env("RUST_LOG", "error")
        .env_remove(KEY_ENV)
        .env_remove("VTOP_AUDIT_LOG")
        .stdin(Stdio::null())
        .output()
        .expect("spawn vtopctl")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Committed rows for `prune-ledger` to work on.
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

/// A ledger with `3 * 2` audit rows, written by three runs of
/// `prune-ledger`, and the config that reads them.
async fn trail(dir: &Path, keyed: bool) -> PathBuf {
    let config = write_config(dir, keyed);
    seed_committed(dir).await;
    let digests = dir.join("digests");
    for run in 0..3 {
        let out = vtopctl(&[
            "prune-ledger",
            "--config",
            config.to_str().unwrap(),
            "--older-than-days",
            "0",
            "--digest-dir",
            digests.to_str().unwrap(),
        ]);
        assert!(out.status.success(), "run {run}: {}", text(&out.stderr));
    }
    config
}

/// What `audit verify` says, as JSON: (exit success, report).
fn verify(config: &Path, with_key: bool) -> (bool, serde_json::Value) {
    let args = [
        "--json",
        "audit",
        "verify",
        "--config",
        config.to_str().unwrap(),
    ];
    let out = if with_key {
        vtopctl(&args)
    } else {
        vtopctl_without_key(&args)
    };
    let report: serde_json::Value = serde_json::from_str(text(&out.stdout).trim())
        .unwrap_or_else(|e| panic!("{e}: {}\n{}", text(&out.stdout), text(&out.stderr)));
    (out.status.success(), report)
}

fn problems(report: &serde_json::Value) -> Vec<String> {
    report["problems"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap().to_owned())
        .collect()
}

async fn connection(dir: &Path) -> sqlx::SqliteConnection {
    sqlx::sqlite::SqliteConnectOptions::new()
        .filename(dir.join("state.db"))
        .connect()
        .await
        .unwrap()
}

/// The append-only triggers, dropped: someone who can write the file can.
async fn drop_the_triggers(dir: &Path) {
    let mut conn = connection(dir).await;
    sqlx::raw_sql(
        "DROP TRIGGER trg_audit_events_no_update; DROP TRIGGER trg_audit_events_no_delete",
    )
    .execute(&mut conn)
    .await
    .unwrap();
}

async fn rows(dir: &Path) -> Vec<vtop_state::AuditRow> {
    SqliteStateStore::connect(&state_url(dir))
        .await
        .unwrap()
        .list_audit(0, 1000)
        .await
        .unwrap()
}

// ---- the audit trail -----------------------------------------------------------------

/// The ledger itself refuses an edit or a delete of an audit row (a trigger
/// on the table), so the first defence is that the engine and anyone using
/// its connection cannot change the trail. The chain is the second.
#[tokio::test]
async fn an_audit_row_cannot_be_updated_or_deleted_through_the_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), false).await;
    let (ok, before) = verify(&config, false);
    assert!(ok, "{before}");
    assert_eq!(before["rows"], 6, "{before}");

    let mut conn = connection(dir.path()).await;
    let edit = sqlx::query("UPDATE audit_events SET outcome = 'success' WHERE seq = 1")
        .execute(&mut conn)
        .await;
    assert!(edit.is_err(), "the ledger let an audit row be edited");
    let delete = sqlx::query("DELETE FROM audit_events WHERE seq = 2")
        .execute(&mut conn)
        .await;
    assert!(delete.is_err(), "the ledger let an audit row be deleted");
    let (ok, after) = verify(&config, false);
    assert!(ok, "{after}");
    assert_eq!(after["rows"], 6, "{after}");
    assert_eq!(after["last_hash"], before["last_hash"], "{after}");
}

/// Someone who can write the file drops the trigger and edits a record: the
/// chain breaks at that row, and `audit verify` exits non-zero.
#[tokio::test]
async fn an_edited_audit_record_breaks_the_hash_chain() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), false).await;
    drop_the_triggers(dir.path()).await;
    let mut conn = connection(dir.path()).await;
    sqlx::query("UPDATE audit_events SET record = replace(record, 'ledger.prune', 'ledger.noop') WHERE seq = 3")
        .execute(&mut conn)
        .await
        .unwrap();
    let (ok, report) = verify(&config, false);
    assert!(!ok, "{report}");
    assert_eq!(report["passed"], false, "{report}");
    let found = problems(&report);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].starts_with("row 3 ") && found[0].contains("the row was edited"),
        "{found:?}"
    );
}

/// A deleted row in the middle breaks the link of the row after it.
#[tokio::test]
async fn a_deleted_audit_record_breaks_the_hash_chain() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), false).await;
    drop_the_triggers(dir.path()).await;
    let mut conn = connection(dir.path()).await;
    sqlx::query("DELETE FROM audit_events WHERE seq = 3")
        .execute(&mut conn)
        .await
        .unwrap();
    let (ok, report) = verify(&config, false);
    assert!(!ok, "{report}");
    assert_eq!(report["rows"], 5, "{report}");
    let found = problems(&report);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].starts_with("row 4 ") && found[0].contains("deleted, inserted or reordered"),
        "{found:?}"
    );
}

/// Two rows swapped are as visible as a deleted one.
#[tokio::test]
async fn a_reordered_audit_record_breaks_the_hash_chain() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), false).await;
    drop_the_triggers(dir.path()).await;
    let mut conn = connection(dir.path()).await;
    // Rows 3 and 4 exchange their sequence numbers.
    sqlx::raw_sql(
        "UPDATE audit_events SET seq = -3 WHERE seq = 3; \
         UPDATE audit_events SET seq = 3 WHERE seq = 4; \
         UPDATE audit_events SET seq = 4 WHERE seq = -3",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let (ok, report) = verify(&config, false);
    assert!(!ok, "{report}");
    assert!(
        problems(&report)
            .iter()
            .any(|p| p.contains("deleted, inserted or reordered")),
        "{report}"
    );
}

/// The limit of any chain: rows removed from the END leave a chain that
/// verifies. `audit verify` says so in its verdict and prints the row count
/// and the last link so an operator can keep them elsewhere. Against that
/// anchor the truncation shows: fewer rows, another last link.
#[tokio::test]
async fn a_truncated_audit_tail_passes_the_chain_and_shows_against_the_kept_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), false).await;
    let (ok, anchor) = verify(&config, false);
    assert!(ok, "{anchor}");
    assert_eq!(anchor["rows"], 6, "{anchor}");

    drop_the_triggers(dir.path()).await;
    let mut conn = connection(dir.path()).await;
    sqlx::query("DELETE FROM audit_events WHERE seq > 4")
        .execute(&mut conn)
        .await
        .unwrap();
    let (ok, report) = verify(&config, false);
    assert!(
        ok,
        "NOT CAUGHT by the chain alone: a truncated tail verifies: {report}"
    );
    assert!(problems(&report).is_empty(), "{report}");
    // What does show it: the anchor the operator kept.
    assert_eq!(report["rows"], 4, "{report}");
    assert_ne!(report["rows"], anchor["rows"], "{report}");
    assert_ne!(report["last_hash"], anchor["last_hash"], "{report}");
    assert_ne!(report["last_seq"], anchor["last_seq"], "{report}");
    // The human verdict tells the operator to keep it.
    let out = vtopctl_without_key(&["audit", "verify", "--config", config.to_str().unwrap()]);
    assert!(
        text(&out.stdout).contains("a chain cannot show rows removed from its end"),
        "{}",
        text(&out.stdout)
    );
}

/// Without a key, an attacker who can write the file can edit a record and
/// recompute every link after it: the chain only proves the rows agree with
/// each other, not who wrote them. That is the limit the keyed chain removes.
#[tokio::test]
async fn an_unkeyed_chain_recomputed_after_an_edit_is_not_caught() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), false).await;
    rewrite_the_chain_unkeyed(dir.path(), 3).await;
    let (ok, report) = verify(&config, false);
    assert!(
        ok,
        "NOT CAUGHT: an unkeyed chain recomputed after an edit verifies: {report}"
    );
    assert!(problems(&report).is_empty(), "{report}");
    assert_eq!(report["keyed_rows"], 0, "{report}");
    let edited = rows(dir.path()).await;
    assert!(edited[2].record.contains("ledger.noop"), "the edit stayed");
}

/// With the manifest MAC key configured, the chain is keyed: the same
/// rewrite, made without the key, leaves unkeyed links after keyed ones, and
/// the chain is reported broken.
#[tokio::test]
async fn a_keyed_chain_rewritten_without_the_key_is_caught() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), true).await;
    let (ok, report) = verify(&config, true);
    assert!(ok, "{report}");
    assert_eq!(report["keyed_rows"], 6, "{report}");

    rewrite_the_chain_unkeyed(dir.path(), 3).await;
    let (ok, report) = verify(&config, true);
    assert!(!ok, "{report}");
    let found = problems(&report);
    assert!(
        found
            .iter()
            .any(|p| p.contains("an unkeyed link after a keyed one")),
        "{found:?}"
    );
}

/// The edit of record `seq` and the links of every row from it on, as
/// someone without the key would compute them (the unkeyed chain).
async fn rewrite_the_chain_unkeyed(dir: &Path, seq: i64) {
    let unkeyed = ManifestKeyring::default();
    rewrite_the_chain(dir, seq, &unkeyed).await;
}

/// Edit record `seq` and recompute every link from it on with `keyring`.
/// Opening the store puts its append-only triggers back, so the rows are
/// read first and the triggers dropped last, as an attacker working on the
/// file with its own tools would.
async fn rewrite_the_chain(dir: &Path, seq: i64, keyring: &ManifestKeyring) {
    let all = rows(dir).await;
    drop_the_triggers(dir).await;
    let mut conn = connection(dir).await;
    let mut previous: Option<String> = None;
    for row in &all {
        if row.seq < seq {
            previous = Some(row.hash.clone());
            continue;
        }
        let record = if row.seq == seq {
            row.record.replace("ledger.prune", "ledger.noop")
        } else {
            row.record.clone()
        };
        let (hash, chain) = audit_chain_link(previous.as_deref(), &record, keyring);
        sqlx::query(
            "UPDATE audit_events SET record = ?, prev_hash = ?, hash = ?, chain = ? WHERE seq = ?",
        )
        .bind(&record)
        .bind(previous.as_deref())
        .bind(&hash)
        .bind(&chain)
        .bind(row.seq)
        .execute(&mut conn)
        .await
        .unwrap();
        previous = Some(hash);
    }
}

/// A forgery made with the key (an attacker who holds it) is the limit of
/// the keyed chain, stated here so it is not read as a guarantee.
#[tokio::test]
async fn a_keyed_chain_rewritten_with_a_leaked_key_is_not_caught() {
    let dir = tempfile::tempdir().unwrap();
    let config = trail(dir.path(), true).await;
    let key = ManifestMacKey::from_hex(KEY_HEX).unwrap();
    rewrite_the_chain(dir.path(), 3, &ManifestKeyring::from_key(Some(&key))).await;
    let (ok, report) = verify(&config, true);
    assert!(
        ok,
        "NOT CAUGHT: a chain rewritten with the key verifies: {report}"
    );
    assert!(problems(&report).is_empty(), "{report}");
}

// ---- the audit of coverage ----------------------------------------------------------------

/// Three committed batches of one file, in a ledger the test can edit.
async fn three_ranges() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    let mut cfg = file_config(
        dir.path().join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.path().join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.upload.prefix = "tamper-coverage".into();
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = Arc::new(MockBackend::new()) as Arc<dyn UploadBackend>;
    for round in 0..3 {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&input)
            .unwrap();
        for line in 0..4 {
            writeln!(f, "coverage round {round} record {line}").unwrap();
        }
        drop(f);
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(out.iter().all(|o| o.committed), "{out:?}");
    }
    (dir, engine)
}

/// Where in its file a committed row starts.
fn start_of(row: &vtop_state::BatchRecord) -> u64 {
    vtop_cli::coverage::unit_ranges(row)[0].1
}

async fn delete_batch(dir: &Path, batch_id: &str) {
    let mut conn = connection(dir).await;
    let deleted = sqlx::query("DELETE FROM batches WHERE batch_id = ?")
        .bind(batch_id)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(deleted.rows_affected(), 1);
}

/// A committed row deleted from the middle of a file's history leaves a
/// stretch of bytes no committed range covers: `audit-coverage` reports the
/// hole as lost data, naming where it is.
#[tokio::test]
async fn a_committed_ledger_row_deleted_from_the_middle_is_a_coverage_hole() {
    let (dir, engine) = three_ranges().await;
    let mut ledger = engine.store.list_batches().await.unwrap();
    ledger.sort_by_key(start_of);
    assert_eq!(ledger.len(), 3, "{ledger:#?}");
    let clean = vtop_cli::coverage::audit(ledger.clone());
    assert!(clean.iter().all(|u| !u.has_findings()), "{clean:#?}");

    delete_batch(dir.path(), &ledger[1].batch_id).await;
    let after = engine.store.list_batches().await.unwrap();
    let units = vtop_cli::coverage::audit(after);
    assert_eq!(units.len(), 1, "{units:#?}");
    assert!(units[0].has_findings(), "{units:#?}");
    assert_eq!(units[0].unexplained_holes.len(), 1, "{units:#?}");
}

/// The limit of that audit: a committed row deleted from the END leaves no
/// hole below the last committed range, so nothing is reported. (The
/// source's progress is not lost: the next run reads from the earlier
/// committed point and archives the bytes again.)
#[tokio::test]
async fn a_committed_ledger_row_deleted_from_the_end_is_not_a_coverage_hole() {
    let (dir, engine) = three_ranges().await;
    let mut ledger = engine.store.list_batches().await.unwrap();
    ledger.sort_by_key(start_of);
    delete_batch(dir.path(), &ledger[2].batch_id).await;
    let units = vtop_cli::coverage::audit(engine.store.list_batches().await.unwrap());
    assert!(
        units.iter().all(|u| !u.has_findings()),
        "NOT CAUGHT: a deleted last row is invisible to the audit: {units:#?}"
    );
}
