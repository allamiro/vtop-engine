//! #619: after a restart, the engine re-arms a deferred `delete_after_commit`
//! only for a file that still has the mtime AND the ctime the committed
//! batch read (`CommittedCursor::observed_ctime`, passed by the engine's
//! seed to `FileSource::arm_delete_from_seed`).
//!
//! The adapter's own tests call `arm_delete_from_seed` directly, and the
//! state battery checks only the column. This test goes through the ledger
//! and the engine's recovery, so the seed dropping the ctime on the way -
//! which reverts the fix with every other test still green - fails here.

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::types::SourceType;

/// Three pages of lines, so a rewrite in the middle touches neither the
/// first page nor the page below the end, which the fingerprints cover.
fn write_input(dir: &Path) -> PathBuf {
    let path = dir.join("app.log");
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..130 {
        writeln!(f, "{:0>99}", i).unwrap();
    }
    path
}

fn config(dir: &Path, input: &Path, quiet: u64) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "localfs",
    );
    cfg.upload.local_path = Some(dir.join("store").to_string_lossy().into_owned());
    let file = cfg.sources.file.as_mut().unwrap();
    file.delete_after_commit = true;
    file.delete_quiet_seconds = quiet;
    // Off Linux there is no open-file check, so the opt-in is the only way
    // a delete happens; this test is about the seed, not the lease.
    file.delete_without_open_check = !cfg!(target_os = "linux");
    cfg
}

#[tokio::test]
async fn a_rewrite_that_sets_the_mtime_back_while_the_engine_is_down_is_kept_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path());
    {
        // Archived and committed; the quiet period is far off, so the
        // delete waits in memory, and the engine stops.
        let mut engine = Engine::new(
            config(dir.path(), &input, 3600),
            StreamsConfig { streams: vec![] },
        )
        .await
        .unwrap();
        assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);
    }
    assert!(input.exists());

    // While the engine is down: a same-length rewrite between the two
    // pages, with the mtime set back to the one the batch read.
    let mtime = std::fs::metadata(&input).unwrap().modified().unwrap();
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&input)
            .unwrap();
        f.seek(SeekFrom::Start(6000)).unwrap();
        f.write_all(b"REWRITTEN").unwrap();
        f.set_modified(mtime).unwrap();
    }
    let rewritten = std::fs::read(&input).unwrap();

    // Restarted with no quiet period: a delete armed by the seed runs on
    // the first read pass.
    let mut engine = Engine::new(
        config(dir.path(), &input, 0),
        StreamsConfig { streams: vec![] },
    )
    .await
    .unwrap();
    engine.recover().await.unwrap();
    engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(
        std::fs::read(&input).ok(),
        Some(rewritten),
        "rewritten between the pages while the engine was down, mtime set back, and deleted \
         after recovery: the seed did not hold the file to the ctime the batch read"
    );
}
