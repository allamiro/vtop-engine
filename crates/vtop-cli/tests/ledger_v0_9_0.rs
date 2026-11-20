//! #626: a SQLite ledger the RELEASED v0.9.0 wrote opens under this build,
//! and its failed ranges are replayed.
//!
//! The fixture (`tests/fixtures/ledger-v0.9.0`) was made by the v0.9.0
//! `vtopctl`, not by the code under test; its `generate.sh` says how. It is at
//! schema version 1, with no `sealed_at` column: a committed range [0, 48)
//! and two REPLAY_REQUIRED rows from failed uploads, [48, 82) and [48, 114)
//! (the second read to the end of the file after it grew). The test writes
//! the same input where it can, and re-roots the markers' path and inode to
//! it with SQL; nothing else in the file is touched before this build opens
//! it.
#![cfg(unix)]

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use sqlx::ConnectOptions;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType};
use vtop_upload::{MockBackend, UploadBackend};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/ledger-v0.9.0");
/// Where the fixture's input lived, and its inode there.
const OLD_PATH: &str = "/vtop-fixture/app.log";
const OLD_INODE: &str = "\"inode\":1043702";

async fn sqlite(path: &Path) -> sqlx::SqliteConnection {
    sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .connect()
        .await
        .unwrap()
}

fn range(marker: &ProgressMarker) -> (u64, u64) {
    match marker {
        ProgressMarker::File {
            start_byte,
            end_byte,
            ..
        } => (*start_byte, *end_byte),
        other => panic!("a file marker: {other:?}"),
    }
}

#[tokio::test]
async fn a_v0_9_0_ledger_opens_and_its_failed_ranges_are_replayed_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let input = dir.path().join("app.log");
    std::fs::copy(format!("{FIXTURE}/state.db"), &db).unwrap();
    // The copy keeps the checkout's 0644, in a directory other users can
    // enter, which the engine refuses as a ledger (#733): make it owner-only,
    // as the engine creates one. This test is about the schema, not the mode.
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    std::fs::copy(format!("{FIXTURE}/app.log"), &input).unwrap();
    let inode = std::fs::metadata(&input).unwrap().ino();
    {
        let mut conn = sqlite(&db).await;
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(version, 1, "the fixture is v0.9.0's schema");
        for statement in [
            "UPDATE batches SET progress_start_json = \
             replace(replace(progress_start_json, ?1, ?2), ?3, ?4)",
            "UPDATE batches SET progress_end_json = \
             replace(replace(progress_end_json, ?1, ?2), ?3, ?4)",
        ] {
            sqlx::query(statement)
                .bind(OLD_PATH)
                .bind(input.to_string_lossy().into_owned())
                .bind(OLD_INODE)
                .bind(format!("\"inode\":{inode}"))
                .execute(&mut conn)
                .await
                .unwrap();
        }
    }

    let mut cfg = file_config(
        dir.path().join("work").to_str().unwrap(),
        &format!("sqlite://{}", db.display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.engine.name = "fixture-090".into();
    cfg.engine.tenant = "ledger-v090-626".into();
    let backend = Arc::new(MockBackend::new());
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .expect("a v0.9.0 ledger opens");
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;

    let rows = engine.store.list_batches().await.unwrap();
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(
        rows.iter().all(|r| r.sealed_at.is_none()),
        "v0.9.0 recorded no seal instant: {rows:?}"
    );
    let exact = || {
        vtop_core::telemetry::init()
            .unwrap()
            .replay_rebuilds_total
            .with_label_values(&["ledger-v090-626", "file", "raw", "exact"])
            .get()
    };
    let exact_before = exact();
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.replay_required, 2, "{summary:?}");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        !out.is_empty() && out.iter().all(|o| o.committed),
        "{out:?}"
    );

    let mut committed: Vec<(u64, u64)> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.state == BatchState::SourceCommitted)
        .map(|r| (range(&r.progress_start).0, range(&r.progress_end).1))
        .collect();
    committed.sort_unstable();
    let len = std::fs::metadata(&input).unwrap().len();
    let mut next = 0;
    for (start, end) in &committed {
        assert_eq!(*start, next, "a gap or a repeat at {next}: {committed:?}");
        next = *end;
    }
    assert_eq!(next, len, "the archive stops short: {committed:?}");

    assert_eq!(
        exact(),
        exact_before,
        "v0.9.0's rows recorded no seal instant: their replay is not claimed as rebuilt in \
         its first hour"
    );
    let mut conn = sqlite(&db).await;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        version, 1,
        "opening it is no one-way step: v0.9.0 can still open the file"
    );
    let replay = engine
        .store
        .get_batch(&out[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        replay.sealed_at.is_some(),
        "a row this build writes records its seal instant"
    );
}
