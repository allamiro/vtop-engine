//! #743: a SQLite ledger and a prune digest that a RELEASED `vtopctl` wrote
//! open under this build, and nothing a release committed is lost.
//!
//! Each directory under `tests/fixtures/ledger-sqlite/<tag>/` was written by
//! that release's published binary, not by the code under test
//! (`scripts/compat/regenerate-fixtures.sh` says how, and its `PROVENANCE`
//! records which asset). So these tests prove the format of a release against
//! the code that reads it today, which a fixture built by this code could not
//! (`ddia-encoding-evolution` rule 4).
//!
//! Per fixture, this build must:
//! - open the ledger (the SQLite migration runs on open) and read every row,
//!   in the state the release left it;
//! - leave `PRAGMA user_version` at the version this build writes (1): opening
//!   an older ledger is the additive migration, and no one-way step;
//! - keep every batch the release committed committed, through recovery and
//!   a further `process_once` over new input;
//! - read the release's prune digest byte for byte as it was written, and
//!   find that the ledger's committed ranges and the digest's tile the source
//!   with no hole (`vtopctl audit-coverage`'s check).
//!
//! The fixtures' markers name the path and inode of the input where the
//! release wrote it. The test writes the same input where it can, and
//! re-roots the markers (and the digest rows, in memory: a digest is hashed,
//! so its bytes stay as written) to it. Nothing else in the file is touched
//! before this build opens it.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::ConnectOptions;
use vtop_cli::coverage;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType};
use vtop_state::BatchRecord;
use vtop_upload::{MockBackend, UploadBackend};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/ledger-sqlite");

/// The releases whose ledgers are fixtures. A fixture deleted without a
/// decision fails here, so the coverage cannot shrink unnoticed.
const FLOOR: &[&str] = &["v0.7.0", "v0.8.0", "v0.9.0", "v0.10.1"];

fn tags() -> Vec<String> {
    let mut tags: Vec<String> = std::fs::read_dir(FIXTURES)
        .expect("the fixtures directory")
        .map(|e| e.unwrap())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();
    tags.sort();
    for tag in FLOOR {
        assert!(
            tags.iter().any(|t| t == tag),
            "no fixture for {tag}: a release's ledger format stays covered until a decision \
             retires it (found {tags:?})"
        );
    }
    tags
}

async fn sqlite(path: &Path) -> sqlx::SqliteConnection {
    sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .connect()
        .await
        .unwrap()
}

async fn user_version(db: &Path) -> i64 {
    let mut conn = sqlite(db).await;
    sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut conn)
        .await
        .unwrap()
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

fn reroot(record: &mut BatchRecord, old: &(String, i64), new: &(String, u64)) {
    if record.source_name == old.0 {
        record.source_name = new.0.clone();
    }
    for marker in [&mut record.progress_start, &mut record.progress_end] {
        if let ProgressMarker::File { path, inode, .. } = marker {
            if *path == old.0 {
                *path = new.0.clone();
                *inode = Some(new.1);
            }
        }
    }
}

fn states(rows: &[BatchRecord]) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.state.as_str()).or_insert(0) += 1;
    }
    counts
}

fn append(path: &Path, lines: &[&str]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    for line in lines {
        writeln!(f, "{line}").unwrap();
    }
}

struct Opened {
    _dir: tempfile::TempDir,
    db: PathBuf,
    digests: PathBuf,
    input: PathBuf,
    engine: Engine,
    /// The input's path and inode where this test wrote it.
    new: (String, u64),
    /// The input's path and inode where the release wrote it.
    old: (String, i64),
    /// Each row's id and state as the release left it, read with SQL before
    /// any code of this build opened the file.
    raw: Vec<(String, String)>,
}

async fn open(tag: &str) -> Opened {
    let fixture = Path::new(FIXTURES).join(tag);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let input = dir.path().join("app.log");
    let digests = dir.path().join("state.db.ledger-digests");
    std::fs::copy(fixture.join("state.db"), &db).unwrap();
    // The copy keeps the checkout's 0644, in a directory other users can
    // enter, which the engine refuses as a ledger (#733): make it owner-only,
    // as the engine creates one. This test is about the format, not the mode.
    std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::copy(fixture.join("app.log"), &input).unwrap();
    copy_dir(&fixture.join("ledger-digests"), &digests);
    let inode = std::fs::metadata(&input).unwrap().ino();

    let mut conn = sqlite(&db).await;
    let raw: Vec<(String, String)> =
        sqlx::query_as("SELECT batch_id, state FROM batches ORDER BY created_at, batch_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert!(!raw.is_empty(), "{tag}: the fixture holds rows");
    // The first row's marker names the input's old path and inode.
    let first: String = sqlx::query_scalar(
        "SELECT progress_start_json FROM batches ORDER BY created_at, batch_id LIMIT 1",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let marker: serde_json::Value = serde_json::from_str(&first).unwrap();
    let old = (
        marker["path"].as_str().unwrap().to_owned(),
        marker["inode"].as_i64().unwrap(),
    );
    for statement in [
        "UPDATE batches SET progress_start_json = \
         replace(replace(progress_start_json, ?1, ?2), ?3, ?4)",
        "UPDATE batches SET progress_end_json = \
         replace(replace(progress_end_json, ?1, ?2), ?3, ?4)",
    ] {
        sqlx::query(statement)
            .bind(&old.0)
            .bind(input.to_string_lossy().into_owned())
            .bind(format!("\"inode\":{}", old.1))
            .bind(format!("\"inode\":{inode}"))
            .execute(&mut conn)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE batches SET source_name = ?1 WHERE source_name = ?2")
        .bind(input.to_string_lossy().into_owned())
        .bind(&old.0)
        .execute(&mut conn)
        .await
        .unwrap();
    drop(conn);

    let mut cfg = file_config(
        dir.path().join("work").to_str().unwrap(),
        &format!("sqlite://{}", db.display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.engine.name = "compat-ledger".into();
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap_or_else(|e| panic!("{tag}: this build opens the release's ledger: {e}"));
    engine.backend = Arc::new(MockBackend::new()) as Arc<dyn UploadBackend>;
    Opened {
        _dir: dir,
        db,
        digests,
        new: (input.to_string_lossy().into_owned(), inode),
        input,
        engine,
        old,
        raw,
    }
}

#[tokio::test]
async fn every_release_ledger_opens_and_every_row_reads_as_it_was_left() {
    for tag in tags() {
        let o = open(&tag).await;
        let rows = o.engine.store.list_batches().await.unwrap();
        assert_eq!(
            rows.len(),
            o.raw.len(),
            "{tag}: a row is unreadable or lost: {rows:?}"
        );
        for (id, state) in &o.raw {
            let row = rows
                .iter()
                .find(|r| &r.batch_id == id)
                .unwrap_or_else(|| panic!("{tag}: row {id} is gone"));
            assert_eq!(
                row.state.as_str(),
                state,
                "{tag}: {id} reads in another state than the release left it"
            );
        }
        let kinds = states(&rows);
        assert!(
            kinds.get("source_committed").copied().unwrap_or(0) >= 1
                && kinds.get("replay_required").copied().unwrap_or(0) >= 1,
            "{tag}: the scenario leaves committed and replay-required rows: {kinds:?}"
        );
        assert_eq!(
            user_version(&o.db).await,
            1,
            "{tag}: opening a release's ledger leaves the version this build writes, and is no \
             one-way step"
        );
    }
}

#[tokio::test]
async fn a_release_ledger_keeps_what_it_committed_and_archives_new_input() {
    for tag in tags() {
        let mut o = open(&tag).await;
        let committed_before: Vec<String> = o
            .raw
            .iter()
            .filter(|(_, s)| s == "source_committed")
            .map(|(id, _)| id.clone())
            .collect();
        assert!(!committed_before.is_empty(), "{tag}");

        o.engine
            .recover()
            .await
            .unwrap_or_else(|e| panic!("{tag}: recovery: {e}"));
        append(&o.input, &["compat head one", "compat head two"]);
        let out = o
            .engine
            .process_once(SourceType::File)
            .await
            .unwrap_or_else(|e| panic!("{tag}: process_once over new input: {e}"));
        assert!(
            !out.is_empty() && out.iter().all(|r| r.committed),
            "{tag}: {out:?}"
        );

        // A later recovery retires the handed-back row the new batch covered.
        o.engine
            .recover()
            .await
            .unwrap_or_else(|e| panic!("{tag}: the second recovery: {e}"));
        let rows = o.engine.store.list_batches().await.unwrap();
        for id in &committed_before {
            let row = rows
                .iter()
                .find(|r| &r.batch_id == id)
                .unwrap_or_else(|| panic!("{tag}: committed batch {id} is gone"));
            assert_eq!(
                row.state,
                BatchState::SourceCommitted,
                "{tag}: {id} was committed by the release and must stay so"
            );
        }
        assert!(
            rows.iter()
                .all(|r| !matches!(r.state, BatchState::Failed | BatchState::ReplayRequired)),
            "{tag}: every handed-back range is archived again or retired: {:?}",
            states(&rows)
        );
        assert_eq!(user_version(&o.db).await, 1, "{tag}");
    }
}

#[tokio::test]
async fn a_release_digest_reads_and_tiles_the_source_with_its_ledger() {
    for tag in tags() {
        let mut o = open(&tag).await;
        let (digest_rows, files) = coverage::read_digests(&o.digests)
            .unwrap_or_else(|e| panic!("{tag}: this build reads the release's digest: {e}"));
        assert!(
            files >= 1 && !digest_rows.is_empty(),
            "{tag}: the scenario prunes a committed row into a digest"
        );
        assert!(
            digest_rows
                .iter()
                .all(|r| r.state == BatchState::SourceCommitted),
            "{tag}: a digest holds committed rows only"
        );

        o.engine.recover().await.unwrap();
        append(&o.input, &["compat head audit"]);
        let out = o.engine.process_once(SourceType::File).await.unwrap();
        assert!(out.iter().all(|r| r.committed), "{tag}: {out:?}");

        let mut records = o.engine.store.list_batches().await.unwrap();
        for mut row in digest_rows {
            reroot(&mut row, &o.old, &o.new);
            records.push(row);
        }
        let len = std::fs::metadata(&o.input).unwrap().len();
        let reports = coverage::audit(records);
        assert_eq!(reports.len(), 1, "{tag}: one source: {reports:?}");
        let report = &reports[0];
        assert!(
            !report.has_findings(),
            "{tag}: the ledger and the release's digest leave a hole: {report:?}"
        );
        assert_eq!(report.audited_from, 0, "{tag}: the digest covers the start");
        assert_eq!(
            report.committed_through, len,
            "{tag}: the archive stops short: {report:?}"
        );
    }
}
