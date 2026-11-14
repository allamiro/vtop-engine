//! `vtopctl scrub` (#628): committed objects are read back and verified again.
//!
//! Once a batch is SOURCE_COMMITTED nothing used to read its object again, so
//! an object replaced with same-size bytes, deleted by a lifecycle rule, or a
//! manifest rewritten in place went unnoticed until someone fetched the data.
//! These tests archive real batches through the pipeline into the in-memory
//! mock store (or onto disk with `localfs`, for the command end to end), damage
//! the store the ways the issue names, and check that a scrub reports exactly
//! the damaged batch, with the right result, and nothing else.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use vtop_cli::scrub::{self, ScrubOptions, ScrubReport, ScrubResult, ScrubRow};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::manifest::VtopManifest;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_state::BatchRecord;
use vtop_upload::{
    MockBackend, ObjectChecksum, ObjectHead, StoredManifest, StoredObject, UploadBackend,
    VerificationResult,
};

/// An archive of `n` committed batches, one per input file, in a mock store
/// the test keeps a concrete handle to so it can damage it.
struct Archive {
    dir: tempfile::TempDir,
    engine: Engine,
    mock: Arc<MockBackend>,
}

fn write_input(dir: &Path, name: &str, lines: usize) -> PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..lines {
        writeln!(f, "{name} record {i}").unwrap();
    }
    path
}

fn config(dir: &Path, inputs: &[PathBuf], backend: &str) -> VtopConfig {
    file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        inputs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        backend,
    )
}

async fn archive_with(n: usize, mac_env: Option<&str>) -> Archive {
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<PathBuf> = (0..n)
        .map(|i| write_input(dir.path(), &format!("app-{i}.log"), 3 + i))
        .collect();
    let mut cfg = config(dir.path(), &inputs, "mock");
    cfg.manifest_mac_key_env = mac_env.map(str::to_owned);
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let mock = Arc::new(MockBackend::new());
    engine.backend = mock.clone();
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(outcomes.len(), n, "one batch per input file");
    assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");
    Archive { dir, engine, mock }
}

async fn archive(n: usize) -> Archive {
    archive_with(n, None).await
}

impl Archive {
    async fn ledger(&self) -> Vec<BatchRecord> {
        let mut rows = self.engine.store.list_batches().await.unwrap();
        rows.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
        rows
    }

    async fn rows(&self) -> Vec<ScrubRow> {
        scrub::committed_rows(self.engine.store.list_batches().await.unwrap(), Vec::new())
    }

    async fn scrub(&self, opts: &ScrubOptions) -> ScrubReport {
        self.scrub_rows(self.rows().await, opts).await
    }

    async fn scrub_rows(&self, rows: Vec<ScrubRow>, opts: &ScrubOptions) -> ScrubReport {
        self.engine.scrub(rows, opts).await.unwrap()
    }

    /// Store `bytes` at `uri` as the CURRENT object, the way an overwrite or a
    /// misbehaving writer would.
    async fn overwrite(&self, uri: &str, bytes: &[u8], manifest: bool) {
        let staged = self.dir.path().join("overwrite.bin");
        std::fs::write(&staged, bytes).unwrap();
        if manifest {
            self.mock.put_manifest(&staged, uri, None).await.unwrap();
        } else {
            self.mock.put_object(&staged, uri, None).await.unwrap();
        }
    }

    async fn stored_manifest(&self, row: &BatchRecord) -> VtopManifest {
        let bytes = self
            .mock
            .get_manifest_pinned(
                row.manifest_uri.as_deref().unwrap(),
                row.manifest_version_id.as_deref().unwrap(),
                1 << 20,
            )
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}

fn opts() -> ScrubOptions {
    ScrubOptions::default()
}

/// The findings as `(batch_id, result)`, for exact comparison.
fn findings(report: &ScrubReport) -> Vec<(String, ScrubResult)> {
    report
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.result))
        .collect()
}

fn count(report: &ScrubReport, result: ScrubResult) -> u64 {
    report.results.get(result.as_str()).copied().unwrap_or(0)
}

#[tokio::test]
async fn an_intact_archive_scrubs_clean() {
    let a = archive(3).await;
    let report = a.scrub(&opts()).await;
    assert!(report.passed, "{report:#?}");
    assert_eq!(report.committed_rows, 3);
    assert_eq!(report.checked, 3);
    assert_eq!(count(&report, ScrubResult::Ok), 3, "{report:#?}");
    assert!(report.findings.is_empty(), "{report:#?}");
    // Every result is reported, at zero when nothing had it, so a consumer
    // of the JSON never confuses an absent key with a clean one.
    for result in ScrubResult::ALL {
        assert!(report.results.contains_key(result.as_str()), "{result:?}");
    }
}

/// The scrub's `limited` is recovery's size-only (#629, #651): the backend
/// passed the object with `backend_limited`. Batches committed that way under
/// the `require_strong_verification: false` opt-out (as the testkit config
/// sets it) are counted as `limited` and are not findings.
#[tokio::test]
async fn a_size_only_pass_is_limited_and_not_a_finding() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = vec![write_input(dir.path(), "a.log", 3)];
    let cfg = config(dir.path(), &inputs, "mock");
    assert!(!cfg.upload.require_strong_verification);
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = Arc::new(MockBackend::limited());
    assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);

    let rows = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(rows, &opts()).await.unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(count(&report, ScrubResult::Limited), 1, "{report:#?}");
    assert_eq!(count(&report, ScrubResult::Ok), 0);
}

/// One flipped byte, size unchanged: the shape a HEAD cannot see.
#[tokio::test]
async fn a_corrupted_object_byte_is_reported_as_mismatched() {
    let a = archive(3).await;
    let ledger = a.ledger().await;
    let victim = &ledger[1];
    a.mock.corrupt(victim.object_uri.as_deref().unwrap());

    let report = a.scrub(&opts()).await;
    assert!(!report.passed);
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Mismatched)],
        "{report:#?}"
    );
    assert_eq!(count(&report, ScrubResult::Ok), 2);
    assert_eq!(count(&report, ScrubResult::Mismatched), 1);
}

/// The issue's "done when": an object replaced with same-size bytes. The
/// existing HEAD-only `audit-coverage --check-storage` passes it; the scrub
/// must not.
#[tokio::test]
async fn an_object_replaced_with_same_size_bytes_is_reported_where_a_head_check_is_not() {
    let a = archive(2).await;
    let ledger = a.ledger().await;
    let victim = &ledger[0];
    let uri = victim.object_uri.as_deref().unwrap();
    let size = victim.object_size_bytes.unwrap() as usize;
    a.overwrite(uri, &vec![0x42; size], false).await;

    let head_only = vtop_cli::coverage::check_storage(a.mock.as_ref(), &ledger).await;
    assert!(
        head_only.is_empty(),
        "a HEAD sees the right size and cannot tell: {head_only:?}"
    );

    let report = a.scrub(&opts()).await;
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Mismatched)],
        "{report:#?}"
    );
}

#[tokio::test]
async fn a_deleted_object_is_reported_as_missing() {
    let a = archive(3).await;
    let ledger = a.ledger().await;
    let victim = &ledger[2];
    a.mock
        .delete_object(victim.object_uri.as_deref().unwrap())
        .await
        .unwrap();

    let report = a.scrub(&opts()).await;
    assert!(!report.passed);
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Missing)],
        "{report:#?}"
    );
    let detail = &report.findings[0].detail;
    assert!(
        detail.contains("object"),
        "the finding names what is missing: {detail}"
    );
}

/// A deleted pinned manifest version (retention expiry, a privileged
/// versioned delete) fails closed: no falling back to the current key.
#[tokio::test]
async fn a_deleted_pinned_manifest_version_is_reported_as_missing() {
    let a = archive(2).await;
    let ledger = a.ledger().await;
    let victim = &ledger[0];
    a.mock.delete_version(
        victim.manifest_uri.as_deref().unwrap(),
        victim.manifest_version_id.as_deref().unwrap(),
    );

    let report = a.scrub(&opts()).await;
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Missing)],
        "{report:#?}"
    );
}

/// The manifest's current key rewritten in place while the pinned version
/// survives: anyone reading the archive by key now reads the forgery, so the
/// scrub reports it even though the version the ledger pinned is intact.
#[tokio::test]
async fn a_manifest_overwritten_at_its_current_key_is_reported() {
    let a = archive(2).await;
    let ledger = a.ledger().await;
    let victim = &ledger[1];
    let mut forged = a.stored_manifest(victim).await;
    forged.record_count += 1;
    forged.manifest.sha256 = forged.self_hash().unwrap();
    a.overwrite(
        victim.manifest_uri.as_deref().unwrap(),
        &forged.to_json_bytes().unwrap(),
        true,
    )
    .await;

    let report = a.scrub(&opts()).await;
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Mismatched)],
        "{report:#?}"
    );
    let detail = &report.findings[0].detail;
    assert!(detail.contains("current key"), "{detail}");
}

/// A row with no pinned version (a legacy row, or a store without versions)
/// is read at the current key, so a tampered manifest there is the one the
/// scrub authenticates: an edited body fails its self-hash, and a forgery with
/// a recomputed self-hash still fails the ledger's binding.
#[tokio::test]
async fn a_tampered_manifest_on_an_unpinned_row_is_reported_as_unauthenticated() {
    let a = archive(3).await;
    let ledger = a.ledger().await;

    // Edited in place: the embedded self-hash no longer matches.
    let edited = &ledger[0];
    let mut body = a.stored_manifest(edited).await;
    body.record_count += 7;
    a.overwrite(
        edited.manifest_uri.as_deref().unwrap(),
        &body.to_json_bytes().unwrap(),
        true,
    )
    .await;

    // Forged: self-hash recomputed, so only the ledger's manifest digest
    // tells it from the real one.
    let forged_row = &ledger[1];
    let mut forged = a.stored_manifest(forged_row).await;
    forged.record_count += 1;
    forged.manifest.sha256 = forged.self_hash().unwrap();
    a.overwrite(
        forged_row.manifest_uri.as_deref().unwrap(),
        &forged.to_json_bytes().unwrap(),
        true,
    )
    .await;

    let rows: Vec<ScrubRow> = a
        .rows()
        .await
        .into_iter()
        .map(|mut row| {
            row.record.manifest_version_id = None;
            row
        })
        .collect();
    let report = a.scrub_rows(rows, &opts()).await;
    let mut got = findings(&report);
    got.sort();
    let mut want = vec![
        (edited.batch_id.clone(), ScrubResult::Unauthenticated),
        (forged_row.batch_id.clone(), ScrubResult::Unauthenticated),
    ];
    want.sort();
    assert_eq!(got, want, "{report:#?}");
    assert_eq!(count(&report, ScrubResult::Ok), 1);
}

/// With a manifest key configured, a manifest whose MAC does not verify is
/// unauthenticated even when its self-hash and everything else agree.
#[tokio::test]
async fn a_manifest_with_a_bad_mac_is_reported_as_unauthenticated() {
    let env = format!("VTOP_TEST_SCRUB_MAC_{}", std::process::id());
    std::env::set_var(&env, "5a".repeat(32));
    let a = archive_with(2, Some(&env)).await;
    std::env::remove_var(&env);
    assert!(a.scrub(&opts()).await.passed, "signed archive scrubs clean");

    let ledger = a.ledger().await;
    let victim = &ledger[0];
    // The MAC swapped for a well-formed wrong one: the self-hash and the
    // ledger's manifest digest both exclude the MAC, so only the key tells.
    let mut rebuilt = a.stored_manifest(victim).await;
    assert!(rebuilt.manifest.mac.is_some(), "the archive is signed");
    rebuilt.manifest.mac = Some("00".repeat(32));
    a.overwrite(
        victim.manifest_uri.as_deref().unwrap(),
        &rebuilt.to_json_bytes().unwrap(),
        true,
    )
    .await;
    let rows: Vec<ScrubRow> = a
        .rows()
        .await
        .into_iter()
        .map(|mut row| {
            row.record.manifest_version_id = None;
            row
        })
        .collect();
    let report = a.scrub_rows(rows, &opts()).await;
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        report.findings[0]
            .detail
            .to_ascii_lowercase()
            .contains("mac"),
        "{:?}",
        report.findings[0].detail
    );
}

/// Rows `prune-ledger` deleted are known only by its digests; the scrub
/// checks them from there, and says where each finding's row came from.
#[tokio::test]
async fn a_pruned_row_is_scrubbed_from_its_digest() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path(), "app.log", 3);
    let cfg = config(dir.path(), std::slice::from_ref(&input), "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let mock = Arc::new(MockBackend::new());
    engine.backend = mock.clone();
    assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&input)
            .unwrap();
        writeln!(f, "a later record").unwrap();
    }
    assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);

    let digest_dir = dir.path().join("digests");
    let cutoff = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
    let (deleted, _) =
        vtop_cli::coverage::prune_with_digest(engine.store.as_ref(), &cutoff, 100, &digest_dir)
            .await
            .unwrap();
    assert_eq!(
        deleted, 1,
        "the older of the two rows for one path is pruned"
    );
    let (digested, _) = vtop_cli::coverage::read_digests(&digest_dir).unwrap();
    let pruned = digested[0].clone();
    mock.delete_object(pruned.object_uri.as_deref().unwrap())
        .await
        .unwrap();

    let rows = scrub::committed_rows(engine.store.list_batches().await.unwrap(), digested);
    assert_eq!(rows.len(), 2, "the ledger's row and the digest's");
    let report = scrub::scrub(engine.backend.as_ref(), &Default::default(), rows, &opts())
        .await
        .unwrap();
    assert_eq!(
        findings(&report),
        vec![(pruned.batch_id.clone(), ScrubResult::Missing)],
        "{report:#?}"
    );
    assert_eq!(report.findings[0].origin, scrub::RowOrigin::Digest);
}

/// A row both digested and still in the ledger (a prune that crashed between
/// writing its digest and deleting) is checked once, and rows that are not
/// committed are not checked at all.
#[tokio::test]
async fn committed_rows_are_deduplicated_and_uncommitted_rows_skipped() {
    let a = archive(2).await;
    let ledger = a.ledger().await;
    let mut uncommitted = ledger[0].clone();
    uncommitted.batch_id = "vtop-zz-in-flight".into();
    uncommitted.state = BatchState::Verified;
    let mut digested = vec![ledger[1].clone(), uncommitted.clone()];
    digested[0].object_uri = Some("s3://elsewhere/never-read".into());
    let rows = scrub::committed_rows(
        ledger.iter().cloned().chain([uncommitted]).collect(),
        digested,
    );
    let ids: Vec<&str> = rows.iter().map(|r| r.record.batch_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![ledger[0].batch_id.as_str(), ledger[1].batch_id.as_str()]
    );
    assert!(rows.iter().all(|r| r.origin == scrub::RowOrigin::Ledger));
    assert!(a.scrub_rows(rows, &opts()).await.passed);
}

// ---- bounds and resume -----------------------------------------------------

/// `--limit` with a cursor file: each run checks the next slice, the cursor
/// survives between runs, and the run that reaches the end completes the pass
/// and starts the next one from the beginning.
#[tokio::test]
async fn a_cursor_resumes_where_the_last_run_stopped_and_wraps_at_the_end() {
    let a = archive(5).await;
    let ids: Vec<String> = a.ledger().await.into_iter().map(|r| r.batch_id).collect();
    let cursor = a.dir.path().join("scrub.cursor");
    let opts = ScrubOptions {
        limit: Some(2),
        cursor_file: Some(cursor.clone()),
        ..ScrubOptions::default()
    };

    let mut seen = Vec::new();
    for (run, want_complete) in [(0, false), (1, false), (2, true)] {
        let report = a.scrub(&opts).await;
        assert!(report.passed, "run {run}: {report:#?}");
        let progress = report.cursor.as_ref().expect("cursor mode reports it");
        assert_eq!(
            progress.pass_complete, want_complete,
            "run {run}: {progress:?}"
        );
        seen.extend(report.checked_ids.clone());
    }
    assert_eq!(seen, ids, "three runs check every row once, in order");

    let wrapped = a.scrub(&opts).await;
    assert_eq!(
        wrapped.checked_ids,
        ids[..2].to_vec(),
        "the next pass starts at the beginning"
    );
    assert_eq!(wrapped.cursor.as_ref().unwrap().passes_completed, 1);
}

/// A run that dies part-way leaves the cursor at the last object it finished,
/// so the next run neither skips the rest nor re-reads the whole slice.
#[tokio::test]
async fn an_interrupted_run_resumes_after_the_last_object_it_finished() {
    let a = archive(4).await;
    let ids: Vec<String> = a.ledger().await.into_iter().map(|r| r.batch_id).collect();
    let cursor = a.dir.path().join("scrub.cursor");
    let gate = Arc::new(Gate::new(a.mock.clone(), 2));
    let opts = ScrubOptions {
        concurrency: 1,
        cursor_file: Some(cursor.clone()),
        ..ScrubOptions::default()
    };
    let rows = a.rows().await;
    let no_keys = vtop_core::manifest::ManifestKeyring::default();
    let run = scrub::scrub(gate.as_ref(), &no_keys, rows, &opts);
    // The third object's verification never returns: the run is cut off there.
    let cut = tokio::time::timeout(Duration::from_millis(500), run).await;
    assert!(cut.is_err(), "the gated run must still be waiting");

    let report = a
        .scrub(&ScrubOptions {
            cursor_file: Some(cursor),
            ..ScrubOptions::default()
        })
        .await;
    assert_eq!(report.checked_ids, ids[2..].to_vec(), "{report:#?}");
    assert_eq!(
        report.cursor.as_ref().unwrap().resumed_after.as_deref(),
        Some(ids[1].as_str())
    );
}

/// A run killed after it found damage, before its results were published,
/// resumes AT the damage (review of #652). The cursor used to be saved past
/// every finished batch, findings included, so the resumed run started after
/// the damaged batch. The finding reached no report and no metric, and the
/// findings alert stayed silent until the next pass. The cursor now stops
/// advancing at the first finding.
#[tokio::test]
async fn a_run_killed_after_a_finding_resumes_at_the_finding() {
    let a = archive(4).await;
    let ids: Vec<String> = a.ledger().await.into_iter().map(|r| r.batch_id).collect();
    let ledger = a.ledger().await;
    a.mock.corrupt(ledger[1].object_uri.as_deref().unwrap());
    let cursor = a.dir.path().join("scrub.cursor");
    // ids[0] passes, ids[1] is the finding, ids[2] never returns: killed there.
    let gate = Arc::new(Gate::new(a.mock.clone(), 2));
    let opts = ScrubOptions {
        concurrency: 1,
        cursor_file: Some(cursor.clone()),
        ..ScrubOptions::default()
    };
    let no_keys = vtop_core::manifest::ManifestKeyring::default();
    let run = scrub::scrub(gate.as_ref(), &no_keys, a.rows().await, &opts);
    assert!(tokio::time::timeout(Duration::from_millis(500), run)
        .await
        .is_err());

    let resumed = a
        .scrub(&ScrubOptions {
            cursor_file: Some(cursor),
            ..ScrubOptions::default()
        })
        .await;
    assert_eq!(
        resumed.cursor.as_ref().unwrap().resumed_after.as_deref(),
        Some(ids[0].as_str()),
        "the cursor stopped before the finding: {resumed:#?}"
    );
    assert_eq!(
        findings(&resumed),
        vec![(ids[1].clone(), ScrubResult::Mismatched)],
        "the resumed run finds the damage again"
    );
}

/// A run holds its locks from before it reads the previous metrics until it
/// has published (review of #652). Otherwise an overlapping run could publish
/// in between, and the first would overwrite it from its older snapshot,
/// losing counts and the second's finding time. The metrics file is locked
/// too, so overlapping runs without a cursor are covered. A run that finds
/// the lock held is refused and leaves the file alone.
#[tokio::test]
async fn a_run_is_refused_while_another_holds_its_metrics_file() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = vec![write_input(dir.path(), "a.log", 3)];
    let mut cfg = config(dir.path(), &inputs, "localfs");
    cfg.upload.local_path = Some(dir.path().join("store").to_string_lossy().into_owned());
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();
    {
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);
    }
    let metrics = dir.path().join("scrub.prom");
    std::fs::write(&metrics, "vtop_scrub_objects_total{result=\"ok\"} 41\n").unwrap();
    // Another run, mid-publication, holds the metrics file's lock.
    let held = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(format!("{}.lock", metrics.display()))
        .unwrap();
    held.try_lock().unwrap();

    let cli = vtop_cli::commands::Cli::try_parse_from([
        "vtopctl",
        "scrub",
        "--config",
        config_path.to_str().unwrap(),
        "--metrics-file",
        metrics.to_str().unwrap(),
    ])
    .unwrap();
    assert_eq!(vtop_cli::commands::dispatch(cli).await, 1, "refused");
    assert_eq!(
        std::fs::read_to_string(&metrics).unwrap(),
        "vtop_scrub_objects_total{result=\"ok\"} 41\n",
        "the other run's file is not overwritten"
    );
    drop(held);
}

#[tokio::test]
async fn a_sample_checks_that_many_rows_and_the_same_seed_picks_the_same_rows() {
    let a = archive(6).await;
    let sample = |seed| ScrubOptions {
        sample: Some(2),
        seed: Some(seed),
        ..ScrubOptions::default()
    };
    let first = a.scrub(&sample(7)).await;
    assert_eq!(first.checked, 2, "{first:#?}");
    assert_eq!(first.sample_seed, Some(7));
    assert_eq!(a.scrub(&sample(7)).await.checked_ids, first.checked_ids);
    // Across seeds the sample moves: over a handful of seeds it reaches more
    // than one pair of rows.
    let mut reached = BTreeSet::new();
    for seed in 0..8 {
        reached.extend(a.scrub(&sample(seed)).await.checked_ids);
    }
    assert!(reached.len() > 2, "{reached:?}");
}

#[tokio::test]
async fn concurrency_never_exceeds_its_bound() {
    let a = archive(8).await;
    let probe = Arc::new(Probe::new(a.mock.clone()));
    let report = scrub::scrub(
        probe.as_ref(),
        &Default::default(),
        a.rows().await,
        &ScrubOptions {
            concurrency: 3,
            ..ScrubOptions::default()
        },
    )
    .await
    .unwrap();
    assert!(report.passed, "{report:#?}");
    let peak = probe.peak.load(Ordering::SeqCst);
    assert!(peak <= 3, "at most 3 objects in flight, saw {peak}");
    assert!(peak >= 2, "and it does run them concurrently, saw {peak}");
}

#[tokio::test]
async fn the_object_rate_limit_spaces_the_reads() {
    let a = archive(5).await;
    let started = std::time::Instant::now();
    let report = a
        .scrub(&ScrubOptions {
            concurrency: 5,
            max_objects_per_sec: Some(20.0),
            ..ScrubOptions::default()
        })
        .await;
    assert!(report.passed);
    // Five objects at 20/s: the first at once, the fifth 200 ms later.
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(190),
        "five objects at 20/s took only {took:?}"
    );
}

// ---- the command, end to end ------------------------------------------------

/// `vtopctl scrub` against an on-disk (`localfs`) archive: exit 0 on a clean
/// archive, non-zero once an object is damaged, and the metrics file counts
/// both runs and keeps the last success.
#[tokio::test]
async fn the_command_exits_non_zero_on_findings_and_writes_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = vec![
        write_input(dir.path(), "a.log", 3),
        write_input(dir.path(), "b.log", 4),
    ];
    let mut cfg = config(dir.path(), &inputs, "localfs");
    cfg.upload.local_path = Some(dir.path().join("store").to_string_lossy().into_owned());
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();
    {
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let done = engine.process_once(SourceType::File).await.unwrap();
        assert!(done.iter().all(|o| o.committed) && done.len() == 2);
    }
    let metrics = dir.path().join("scrub.prom");
    let run = || {
        let cli = vtop_cli::commands::Cli::try_parse_from([
            "vtopctl",
            "scrub",
            "--config",
            config_path.to_str().unwrap(),
            "--metrics-file",
            metrics.to_str().unwrap(),
            "--json",
        ])
        .unwrap();
        vtop_cli::commands::dispatch(cli)
    };

    assert_eq!(run().await, 0, "a clean archive exits 0");
    let text = std::fs::read_to_string(&metrics).unwrap();
    assert!(
        text.contains("vtop_scrub_objects_total{result=\"ok\"} 2"),
        "{text}"
    );
    let success = metric_value(&text, "vtop_scrub_last_success_timestamp_seconds");
    assert!(success > 0.0, "{text}");
    assert_eq!(
        metric_value(&text, "vtop_scrub_last_finding_timestamp_seconds"),
        0.0,
        "no run has found anything yet"
    );

    // Damage one object on disk: same size, different bytes.
    let store = dir.path().join("store");
    let object = find_object(&store);
    let len = std::fs::metadata(&object).unwrap().len() as usize;
    std::fs::write(&object, vec![0u8; len]).unwrap();

    assert_eq!(run().await, 1, "a damaged archive exits non-zero");
    let text = std::fs::read_to_string(&metrics).unwrap();
    assert!(
        text.contains("vtop_scrub_objects_total{result=\"ok\"} 3"),
        "the counter carries across runs: {text}"
    );
    assert!(
        text.contains("vtop_scrub_objects_total{result=\"mismatched\"} 1"),
        "{text}"
    );
    assert_eq!(metric_value(&text, "vtop_scrub_last_run_findings"), 1.0);
    assert_eq!(
        metric_value(&text, "vtop_scrub_last_finding_timestamp_seconds"),
        metric_value(&text, "vtop_scrub_last_run_timestamp_seconds"),
        "the run that found damage stamps it: {text}"
    );
    assert_eq!(
        metric_value(&text, "vtop_scrub_last_success_timestamp_seconds"),
        success,
        "a failed run keeps the last success where it was: {text}"
    );
}

/// A metrics file that exists and cannot be read is an error, not a first
/// run (review of #652): read as absent, it was overwritten with the totals
/// reset and the last success set to "never". Invalid UTF-8 cannot be read as
/// text even by root, where a permission bit would not stop the test.
#[tokio::test]
async fn an_unreadable_metrics_file_fails_the_run_and_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = vec![write_input(dir.path(), "a.log", 3)];
    let mut cfg = config(dir.path(), &inputs, "localfs");
    cfg.upload.local_path = Some(dir.path().join("store").to_string_lossy().into_owned());
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();
    {
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);
    }
    let metrics = dir.path().join("scrub.prom");
    let garbage = vec![0xff, 0xfe, 0x00, 0x80];
    std::fs::write(&metrics, &garbage).unwrap();
    let cli = vtop_cli::commands::Cli::try_parse_from([
        "vtopctl",
        "scrub",
        "--config",
        config_path.to_str().unwrap(),
        "--metrics-file",
        metrics.to_str().unwrap(),
    ])
    .unwrap();
    assert_eq!(
        vtop_cli::commands::dispatch(cli).await,
        1,
        "an intact archive, but the metrics file cannot be carried forward"
    );
    assert_eq!(
        std::fs::read(&metrics).unwrap(),
        garbage,
        "the file the totals live in is not replaced"
    );
}

/// A run whose metrics cannot be published does not count (review of #652).
/// The cursor is saved batch by batch as the run goes. If the metrics file
/// then fails to write, a finding from that slice used to be in neither
/// place: not in the metrics, and the cursor had already moved past it, so
/// the next runs checked later slices and the findings alert stayed silent
/// until the next pass. The cursor is now put back where the run started, so
/// the next run checks the same slice again and publishes what it finds.
#[tokio::test]
async fn a_finding_whose_metrics_could_not_be_published_is_found_again() {
    let dir = tempfile::tempdir().unwrap();
    let inputs = vec![
        write_input(dir.path(), "a.log", 3),
        write_input(dir.path(), "b.log", 4),
        write_input(dir.path(), "c.log", 5),
    ];
    let store = dir.path().join("store");
    let mut cfg = config(dir.path(), &inputs, "localfs");
    cfg.upload.local_path = Some(store.to_string_lossy().into_owned());
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();
    let first = {
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let done = engine.process_once(SourceType::File).await.unwrap();
        assert!(done.iter().all(|o| o.committed) && done.len() == 3);
        let mut rows = engine.store.list_batches().await.unwrap();
        rows.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
        rows.remove(0)
    };
    // Damage the batch the first slice checks: same size, different bytes.
    let (bucket, key) = first
        .object_uri
        .as_deref()
        .unwrap()
        .strip_prefix("s3://")
        .unwrap()
        .split_once('/')
        .unwrap();
    let object = store.join(bucket).join(key);
    let len = std::fs::metadata(&object).unwrap().len() as usize;
    std::fs::write(&object, vec![0u8; len]).unwrap();

    let cursor = dir.path().join("scrub.cursor");
    let metrics_dir = dir.path().join("textfile");
    let metrics = metrics_dir.join("scrub.prom");
    let run = || {
        let cli = vtop_cli::commands::Cli::try_parse_from([
            "vtopctl",
            "scrub",
            "--config",
            config_path.to_str().unwrap(),
            "--cursor-file",
            cursor.to_str().unwrap(),
            "--limit",
            "1",
            "--metrics-file",
            metrics.to_str().unwrap(),
        ])
        .unwrap();
        vtop_cli::commands::dispatch(cli)
    };

    // The write fails AFTER the scrub: a directory where the metrics file's
    // temporary file goes. (A missing metrics directory is refused before
    // any reads, at the lock.) The run finds the damage and cannot publish
    // it.
    std::fs::create_dir_all(&metrics_dir).unwrap();
    let blocker = metrics_dir.join(".scrub.prom.tmp");
    std::fs::create_dir(&blocker).unwrap();
    assert_eq!(run().await, 1);
    assert!(!metrics.exists());
    assert!(
        !cursor.exists(),
        "the cursor is put back where the run started (no cursor yet)"
    );

    // Publication repaired: the next slice is the same one, so the finding
    // is published.
    std::fs::remove_dir(&blocker).unwrap();
    assert_eq!(run().await, 1, "the damaged batch is checked again");
    let text = std::fs::read_to_string(&metrics).unwrap();
    assert!(
        text.contains("vtop_scrub_objects_total{result=\"mismatched\"} 1\n"),
        "{text}"
    );
    assert!(
        metric_value(&text, "vtop_scrub_last_finding_timestamp_seconds") > 0.0,
        "the findings alert can see it: {text}"
    );
}

/// The scrub reads the archive, never the sources, and arms no
/// `delete_after_commit` (#631). A source file is deleted only from the file
/// source's read and commit paths, and a deferred delete is re-armed only by
/// recovery's seeding. The scrub's read-only engine runs none of them, even
/// under a config that deletes committed files at once.
#[tokio::test]
async fn a_scrub_never_deletes_or_rearms_a_committed_source_file() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path(), "a.log", 3);
    let original = std::fs::read(&input).unwrap();
    let mut cfg = config(dir.path(), std::slice::from_ref(&input), "localfs");
    cfg.upload.local_path = Some(dir.path().join("store").to_string_lossy().into_owned());
    {
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);
    }
    // Now a config under which the engine WOULD delete it, at once.
    let file = cfg.sources.file.as_mut().unwrap();
    file.delete_after_commit = true;
    file.delete_quiet_seconds = 0;
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();

    let cli = vtop_cli::commands::Cli::try_parse_from([
        "vtopctl",
        "scrub",
        "--config",
        config_path.to_str().unwrap(),
    ])
    .unwrap();
    assert_eq!(vtop_cli::commands::dispatch(cli).await, 0);
    assert_eq!(
        std::fs::read(&input).unwrap(),
        original,
        "the committed source is where it was, unchanged"
    );
}

fn metric_value(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} not in {text}"))
        .trim()
        .parse()
        .unwrap()
}

/// The first telemetry object (not a manifest, not a sidecar) under `root`.
fn find_object(root: &Path) -> PathBuf {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "gz") {
                return path;
            }
        }
    }
    panic!("no object under {}", root.display());
}

// ---- test backends -----------------------------------------------------------

/// Delegates to the mock; implements every method the scrub can call.
macro_rules! delegate_backend {
    ($ty:ty, $inner:ident, $verify:item) => {
        #[async_trait]
        impl UploadBackend for $ty {
            async fn put_object(
                &self,
                local_path: &Path,
                object_uri: &str,
                checksum: Option<ObjectChecksum<'_>>,
            ) -> Result<StoredObject, VtopError> {
                self.$inner.put_object(local_path, object_uri, checksum).await
            }
            async fn put_manifest(
                &self,
                local_path: &Path,
                manifest_uri: &str,
                checksum: Option<ObjectChecksum<'_>>,
            ) -> Result<StoredManifest, VtopError> {
                self.$inner
                    .put_manifest(local_path, manifest_uri, checksum)
                    .await
            }
            async fn get_manifest_pinned(
                &self,
                manifest_uri: &str,
                version_id: &str,
                max_bytes: usize,
            ) -> Result<Vec<u8>, VtopError> {
                self.$inner
                    .get_manifest_pinned(manifest_uri, version_id, max_bytes)
                    .await
            }
            fn supports_object_versions(&self) -> bool {
                true
            }
            async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
                self.$inner.head_object(object_uri).await
            }
            async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
                self.$inner.get_object(object_uri).await
            }
            async fn get_object_bounded(
                &self,
                object_uri: &str,
                max_bytes: usize,
            ) -> Result<Vec<u8>, VtopError> {
                self.$inner.get_object_bounded(object_uri, max_bytes).await
            }
            $verify
            async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
                self.$inner.delete_object(object_uri).await
            }
            fn backend_name(&self) -> &'static str {
                "probe"
            }
            fn supports_checksum_verification(&self) -> bool {
                true
            }
            fn supports_multipart(&self) -> bool {
                false
            }
        }
    };
}

/// Counts object verifications in flight, holding each briefly so overlap
/// is observable.
struct Probe {
    inner: Arc<MockBackend>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl Probe {
    fn new(inner: Arc<MockBackend>) -> Self {
        Self {
            inner,
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}

delegate_backend!(
    Probe,
    inner,
    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let result = self
            .inner
            .verify_object(object_uri, expected_size, expected)
            .await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }
);

/// Lets `open` object verifications through, then hangs every later one: a
/// process killed in the middle of its run.
struct Gate {
    inner: Arc<MockBackend>,
    open: AtomicUsize,
}

impl Gate {
    fn new(inner: Arc<MockBackend>, open: usize) -> Self {
        Self {
            inner,
            open: AtomicUsize::new(open),
        }
    }
}

delegate_backend!(
    Gate,
    inner,
    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let left = self.open.load(Ordering::SeqCst);
        if left == 0 {
            std::future::pending::<()>().await;
        }
        self.open.store(left - 1, Ordering::SeqCst);
        self.inner
            .verify_object(object_uri, expected_size, expected)
            .await
    }
);

/// A manifest from a newer release (#671) is its own finding, not
/// `unauthenticated` - when its bytes are proven to be what this fleet wrote:
/// the row's pinned, immutable version, carrying the self-hash the ledger
/// recorded.
#[tokio::test]
async fn a_pinned_newer_manifest_is_unsupported_not_unauthenticated() {
    let a = archive(1).await;
    let mut rows = a.rows().await;
    let uri = rows[0].record.manifest_uri.clone().unwrap();
    let recorded = rows[0].record.manifest_sha256.clone().unwrap();
    let doc = serde_json::json!({
        "protocol": "VTOP",
        "version": "0.9",
        "batch_id": rows[0].record.batch_id,
        "manifest": {"uri": uri, "sha256": recorded},
        "future_enum": "unrecognized",
    });
    let staged = a.dir.path().join("newer-671.json");
    std::fs::write(&staged, serde_json::to_vec(&doc).unwrap()).unwrap();
    let stored = a.mock.put_manifest(&staged, &uri, None).await.unwrap();
    rows[0].record.manifest_version_id = stored.version_id;
    let report = a.scrub_rows(rows, &opts()).await;
    assert_eq!(report.findings.len(), 1);
    assert_eq!(report.findings[0].result.as_str(), "unsupported_version");
    assert!(
        report.findings[0].detail.contains("claims version 0.9"),
        "{}",
        report.findings[0].detail
    );
}

/// Review of #674: `version` is read before anything is authenticated, so a
/// tamperer who overwrites a manifest with `{"version": "0.9"}` must not turn
/// an integrity finding into "upgrade the scrub". At a key no version pins,
/// the bytes are not proven to be this fleet's: unauthenticated, even when
/// the forgery copies the self-hash the ledger recorded.
#[tokio::test]
async fn an_unpinned_newer_manifest_is_unauthenticated() {
    let a = archive(1).await;
    let mut rows = a.rows().await;
    let uri = rows[0].record.manifest_uri.clone().unwrap();
    let recorded = rows[0].record.manifest_sha256.clone().unwrap();
    for forged in [
        serde_json::json!({"protocol": "VTOP", "version": "0.9"}),
        serde_json::json!({
            "protocol": "VTOP",
            "version": "0.9",
            "batch_id": rows[0].record.batch_id,
            "manifest": {"uri": uri, "sha256": recorded},
        }),
    ] {
        a.overwrite(&uri, &serde_json::to_vec(&forged).unwrap(), true)
            .await;
        rows[0].record.manifest_version_id = None;
        let report = a.scrub_rows(rows.clone(), &opts()).await;
        assert_eq!(report.findings.len(), 1, "{forged}");
        assert_eq!(
            report.findings[0].result.as_str(),
            "unauthenticated",
            "{forged}: {}",
            report.findings[0].detail
        );
    }
}

/// A pin alone is not the proof: the pinned version must also carry the
/// batch id and the self-hash the ledger recorded, or it is not this batch's.
#[tokio::test]
async fn a_pinned_newer_manifest_not_bound_to_the_ledger_is_unauthenticated() {
    let a = archive(1).await;
    let mut rows = a.rows().await;
    let uri = rows[0].record.manifest_uri.clone().unwrap();
    let recorded = rows[0].record.manifest_sha256.clone().unwrap();
    for (batch_id, sha256) in [
        (rows[0].record.batch_id.clone(), "cd".repeat(32)),
        ("another-batch".to_string(), recorded.clone()),
    ] {
        let doc = serde_json::json!({
            "protocol": "VTOP",
            "version": "0.9",
            "batch_id": batch_id,
            "manifest": {"uri": uri, "sha256": sha256},
        });
        let staged = a.dir.path().join("unbound-671.json");
        std::fs::write(&staged, serde_json::to_vec(&doc).unwrap()).unwrap();
        let stored = a.mock.put_manifest(&staged, &uri, None).await.unwrap();
        rows[0].record.manifest_version_id = stored.version_id;
        let report = a.scrub_rows(rows.clone(), &opts()).await;
        assert_eq!(report.findings.len(), 1, "{doc}");
        assert_eq!(
            report.findings[0].result.as_str(),
            "unauthenticated",
            "{doc}: {}",
            report.findings[0].detail
        );
    }
}

/// #671 release B: one archive holding manifests written at 0.3 and, after
/// `manifest.write_version` was switched, at 0.4 scrubs clean, signed or not.
/// A tampered 0.4 manifest is reported like a tampered 0.3 one, and only it.
#[tokio::test]
async fn a_mixed_03_and_04_archive_scrubs_clean_and_a_tampered_04_manifest_is_reported() {
    use vtop_core::manifest::{ManifestWriteVersion, ParsedManifest};
    for signed in [false, true] {
        let env = format!("VTOP_TEST_SCRUB_MIXED_MAC_{}", std::process::id());
        let dir = tempfile::tempdir().unwrap();
        let inputs: Vec<PathBuf> = (0..2)
            .map(|i| write_input(dir.path(), &format!("mixed-{i}.log"), 3))
            .collect();
        let mut cfg = config(dir.path(), &inputs, "mock");
        if signed {
            std::env::set_var(&env, "7c".repeat(32));
            cfg.manifest_mac_key_env = Some(env.clone());
        }
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        std::env::remove_var(&env);
        let mock = Arc::new(MockBackend::new());
        engine.backend = mock.clone();
        assert_eq!(
            engine.config.manifest.write_version,
            ManifestWriteVersion::V0_3
        );
        let first = engine.process_once(SourceType::File).await.unwrap();
        assert!(first.iter().all(|o| o.committed), "{first:?}");
        for input in &inputs {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(input)
                .unwrap();
            writeln!(f, "appended after the switch").unwrap();
        }
        engine.config.manifest.write_version = ManifestWriteVersion::V0_4;
        let second = engine.process_once(SourceType::File).await.unwrap();
        assert_eq!(second.len(), 2, "{second:?}");
        assert!(second.iter().all(|o| o.committed), "{second:?}");
        let a = Archive { dir, engine, mock };

        let ledger = a.ledger().await;
        assert_eq!(ledger.len(), 4);
        let mut by_version = std::collections::BTreeMap::<String, Vec<BatchRecord>>::new();
        for row in &ledger {
            let bytes = a
                .mock
                .get_manifest_pinned(
                    row.manifest_uri.as_deref().unwrap(),
                    row.manifest_version_id.as_deref().unwrap(),
                    1 << 20,
                )
                .await
                .unwrap();
            let parsed = ParsedManifest::from_slice(&bytes).unwrap();
            assert_eq!(parsed.fields().manifest.mac.is_some(), signed);
            by_version
                .entry(parsed.fields().version.clone())
                .or_default()
                .push(row.clone());
        }
        assert_eq!(
            by_version
                .iter()
                .map(|(v, r)| (v.as_str(), r.len()))
                .collect::<Vec<_>>(),
            vec![("0.3", 2), ("0.4", 2)],
            "a mixed archive"
        );
        let report = a.scrub(&opts()).await;
        assert!(report.passed, "signed={signed}: {report:#?}");
        assert_eq!(count(&report, ScrubResult::Ok), 4, "{report:#?}");

        // Tamper with one 0.4 manifest at its (unpinned) key: an edited
        // field without the key, or a well-formed wrong MAC with it.
        let victim = &by_version["0.4"][0];
        let uri = victim.manifest_uri.as_deref().unwrap();
        let stored = a
            .mock
            .get_manifest_pinned(uri, victim.manifest_version_id.as_deref().unwrap(), 1 << 20)
            .await
            .unwrap();
        let mut doc: serde_json::Value = serde_json::from_slice(&stored).unwrap();
        if signed {
            doc["manifest"]["mac"] = serde_json::json!("00".repeat(32));
        } else {
            doc["verification_plan"]["method"] = serde_json::json!("store_checksum");
        }
        a.overwrite(uri, &serde_json::to_vec_pretty(&doc).unwrap(), true)
            .await;
        let rows: Vec<ScrubRow> = a
            .rows()
            .await
            .into_iter()
            .map(|mut row| {
                row.record.manifest_version_id = None;
                row
            })
            .collect();
        let report = a.scrub_rows(rows, &opts()).await;
        assert_eq!(
            findings(&report),
            vec![(victim.batch_id.clone(), ScrubResult::Unauthenticated)],
            "signed={signed}: {report:#?}"
        );
        assert_eq!(count(&report, ScrubResult::Ok), 3, "{report:#?}");
    }
}

// ---- #685: manifest MAC key rotation -----------------------------------------

/// Key files for the rotation tests, outside the archive: `q3` (`3c` x 32,
/// the key the fleet signed with before key ids) and `q4` (`4d` x 32).
const Q3_HEX: &str = "3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c";
const Q4_HEX: &str = "4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d";

/// A `manifest.mac` over key files in `dir`: `keys` as (id, legacy).
fn keyring_config(
    dir: &Path,
    active: &str,
    keys: &[(&str, bool)],
    retired: &[&str],
) -> vtop_core::config::ManifestMacConfig {
    use vtop_core::config::{ManifestKeyConfig, ManifestMacConfig};
    let key_dir = dir.join("keys");
    std::fs::create_dir_all(&key_dir).unwrap();
    for (id, hex) in [("q3", Q3_HEX), ("q4", Q4_HEX)] {
        std::fs::write(key_dir.join(id), format!("{hex}\n")).unwrap();
    }
    ManifestMacConfig {
        active: active.into(),
        keys: keys
            .iter()
            .map(|(id, legacy)| ManifestKeyConfig {
                id: (*id).into(),
                env: None,
                file: Some(key_dir.join(id).to_string_lossy().into_owned()),
                legacy: *legacy,
            })
            .collect(),
        retired: retired.iter().map(|r| r.to_string()).collect(),
    }
}

/// An engine over the archive's ledger and mock store, recovered (so the
/// file source resumes after what is committed) and ready to scrub.
async fn engine_over(mock: &Arc<MockBackend>, cfg: VtopConfig) -> Engine {
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = mock.clone();
    engine.recover().await.unwrap();
    engine
}

fn append(inputs: &[PathBuf], line: &str) {
    for input in inputs {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(input)
            .unwrap();
        writeln!(f, "{line}").unwrap();
    }
}

/// The key id each committed batch's pinned manifest names, by batch id.
async fn key_ids(engine: &Engine, mock: &MockBackend) -> Vec<Option<String>> {
    let mut rows = engine.store.list_batches().await.unwrap();
    rows.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
    let mut ids = Vec::new();
    for row in rows {
        let bytes = mock
            .get_manifest_pinned(
                row.manifest_uri.as_deref().unwrap(),
                row.manifest_version_id.as_deref().unwrap(),
                1 << 20,
            )
            .await
            .unwrap();
        ids.push(
            vtop_core::manifest::ParsedManifest::from_slice(&bytes)
                .unwrap()
                .fields()
                .manifest
                .mac_key_id
                .clone(),
        );
    }
    ids
}

/// #685, the rotation procedure end to end, as the operate page documents
/// it. A fleet signing with `manifest_mac_key_env` (0.3, no key id) moves to
/// a keyring holding its key as `q3` (legacy) and a new `q4`, verify-only;
/// then switches the active key to `q4`; then scrubs. Every manifest, from
/// every phase, verifies with the key it names, and the scrub counts them by
/// key. Afterwards: a keyring that dropped `q3` reports its manifests as
/// unauthenticated naming the key (never mismatched or corrupt), and one
/// that retired `q3` says "retired".
#[tokio::test]
async fn a_key_rotation_verify_only_then_switch_then_scrub_keeps_every_manifest_verified() {
    use vtop_core::manifest::{KeyResult, ManifestWriteVersion};
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<PathBuf> = (0..2)
        .map(|i| write_input(dir.path(), &format!("rotate-{i}.log"), 2))
        .collect();
    let mock = Arc::new(MockBackend::new());

    // Phase 0: before key ids. One key, from manifest_mac_key_env.
    let env = format!("VTOP_TEST_ROTATION_MAC_{}", std::process::id());
    std::env::set_var(&env, Q3_HEX);
    let mut cfg = config(dir.path(), &inputs, "mock");
    cfg.manifest_mac_key_env = Some(env.clone());
    let mut engine = engine_over(&mock, cfg).await;
    std::env::remove_var(&env);
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.len() == 2 && out.iter().all(|o| o.committed), "{out:?}");
    drop(engine);

    // Step 1: the keyring, with the new key verify-only. The old key is
    // active (and legacy, for the manifests that name no id).
    let mut cfg = config(dir.path(), &inputs, "mock");
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.manifest.mac = Some(keyring_config(
        dir.path(),
        "q3",
        &[("q3", true), ("q4", false)],
        &[],
    ));
    let mut engine = engine_over(&mock, cfg).await;
    append(&inputs, "written after q4 was added verify-only");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.len() == 2 && out.iter().all(|o| o.committed), "{out:?}");
    drop(engine);

    // Step 3: switch the active key.
    let mut cfg = config(dir.path(), &inputs, "mock");
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.manifest.mac = Some(keyring_config(
        dir.path(),
        "q4",
        &[("q3", true), ("q4", false)],
        &[],
    ));
    let mut engine = engine_over(&mock, cfg).await;
    append(&inputs, "written after the switch");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.len() == 2 && out.iter().all(|o| o.committed), "{out:?}");

    let mut ids = key_ids(&engine, &mock).await;
    ids.sort();
    assert_eq!(
        ids,
        [
            None,
            None,
            Some("q3".into()),
            Some("q3".into()),
            Some("q4".into()),
            Some("q4".into())
        ],
        "two manifests per phase, each naming the key that signed it"
    );

    // Step 4: scrub. Everything verifies; the counts say by which key.
    let report = engine
        .scrub(
            scrub::committed_rows(engine.store.list_batches().await.unwrap(), vec![]),
            &opts(),
        )
        .await
        .unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(count(&report, ScrubResult::Ok), 6);
    assert_eq!(
        report.manifest_keys,
        [("q3".to_string(), 4), ("q4".to_string(), 2)].into(),
        "the legacy key verified the four manifests before the switch"
    );
    let text = report.lines().join("\n");
    assert!(
        text.contains("manifests authenticated by key: q3=4 q4=2"),
        "{text}"
    );
    let prom = report.prometheus_text(None, 1000);
    for series in [
        "vtop_scrub_manifest_keys_total{key_id=\"q3\"} 4\n",
        "vtop_scrub_manifest_keys_total{key_id=\"q4\"} 2\n",
    ] {
        assert!(prom.contains(series), "{series}: {prom}");
    }
    // Carried across runs like the per-result counter.
    let again = report.prometheus_text(Some(&prom), 2000);
    assert!(
        again.contains("vtop_scrub_manifest_keys_total{key_id=\"q3\"} 8\n"),
        "{again}"
    );
    drop(engine);

    // Afterwards, the two ways to lose the old key. Dropped: its manifests
    // name a key this keyring does not hold. Retired: they name a withdrawn
    // key. Either way unauthenticated, named, and never mismatched.
    for (retired, needle, result) in [
        (&[][..], "not in this keyring", KeyResult::UnknownKey),
        (&["q3"][..], "retired", KeyResult::RetiredKey),
    ] {
        let mut cfg = config(dir.path(), &inputs, "mock");
        cfg.manifest.write_version = ManifestWriteVersion::V0_4;
        cfg.manifest.mac = Some(keyring_config(dir.path(), "q4", &[("q4", false)], retired));
        let engine = engine_over(&mock, cfg).await;
        let rows = scrub::committed_rows(engine.store.list_batches().await.unwrap(), vec![]);
        // The engine's keyring stays inside it; the same config resolves
        // the same keys for the per-batch check.
        let keyring = engine.config.resolve_manifest_keyring().unwrap();
        let mut checked = Vec::new();
        for row in &rows {
            checked.push(
                scrub::check_stored_batch(engine.backend.as_ref(), &keyring, &row.record).await,
            );
        }
        let report = engine.scrub(rows, &opts()).await.unwrap();
        assert_eq!(count(&report, ScrubResult::Ok), 2, "{report:#?}");
        assert_eq!(
            count(&report, ScrubResult::Unauthenticated),
            4,
            "{report:#?}"
        );
        assert_eq!(count(&report, ScrubResult::Mismatched), 0);
        assert_eq!(report.manifest_keys, [("q4".to_string(), 2)].into());
        let details: Vec<&str> = report.findings.iter().map(|f| f.detail.as_str()).collect();
        assert_eq!(
            details
                .iter()
                .filter(|d| d.contains(needle) && d.contains("key id \"q3\""))
                .count(),
            2,
            "{details:#?}"
        );
        assert_eq!(
            details
                .iter()
                .filter(|d| d.contains("names no key id"))
                .count(),
            2,
            "the manifests from before key ids have no legacy key now: {details:#?}"
        );
        for detail in &details {
            assert!(detail.contains("unauthenticated"), "{detail}");
            let lower = detail.to_ascii_lowercase();
            assert!(
                !lower.contains("corrupt") && !lower.contains("tamper"),
                "{detail}"
            );
            assert!(
                !detail.contains(Q3_HEX) && !detail.contains(Q4_HEX),
                "{detail}"
            );
        }
        let mut seen: Vec<(String, KeyResult)> = checked
            .iter()
            .filter_map(|c| c.key.clone())
            .map(|k| (k.key_id, k.result))
            .collect();
        seen.sort();
        let label = if result == KeyResult::RetiredKey {
            "q3"
        } else {
            "other"
        };
        let mut want = vec![
            ("none".to_string(), KeyResult::UnknownKey),
            ("none".to_string(), KeyResult::UnknownKey),
            (label.to_string(), result),
            (label.to_string(), result),
            ("q4".to_string(), KeyResult::Ok),
            ("q4".to_string(), KeyResult::Ok),
        ];
        want.sort();
        assert_eq!(seen, want, "the check says which key, by bounded label");
    }
}

/// Review of #703: an engine still on `manifest_mac_key_env` during a
/// rotation's rollout reads what keyring engines write with the same key.
/// The shorthand holds one key and ignores the id, as v0.8.0 does, so the
/// runbook's step 1 holds: an engine not yet moved to the keyring keeps
/// verifying. A different single key still fails, and `verify-manifest`
/// keeps its single-key wording.
#[tokio::test]
async fn the_single_key_setting_verifies_a_keyring_manifest_signed_with_its_key() {
    use vtop_core::manifest::ManifestWriteVersion;
    let dir = tempfile::tempdir().unwrap();
    let inputs = vec![write_input(dir.path(), "shorthand.log", 2)];
    let mock = Arc::new(MockBackend::new());
    let mut cfg = config(dir.path(), &inputs, "mock");
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.manifest.mac = Some(keyring_config(
        dir.path(),
        "q3",
        &[("q3", true), ("q4", false)],
        &[],
    ));
    let mut engine = engine_over(&mock, cfg).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.len() == 1 && out[0].committed, "{out:?}");
    assert_eq!(key_ids(&engine, &mock).await, [Some("q3".to_string())]);
    drop(engine);

    for (hex, passes) in [(Q3_HEX, true), (Q4_HEX, false)] {
        let env = format!("VTOP_TEST_SHORTHAND_READS_{}_{passes}", std::process::id());
        std::env::set_var(&env, hex);
        let mut cfg = config(dir.path(), &inputs, "mock");
        cfg.manifest_mac_key_env = Some(env.clone());
        let engine = engine_over(&mock, cfg).await;
        let keyring = engine.config.resolve_manifest_keyring().unwrap();
        std::env::remove_var(&env);
        let rows = scrub::committed_rows(engine.store.list_batches().await.unwrap(), vec![]);
        let muri = rows[0].record.manifest_uri.clone().unwrap();
        let report = engine.scrub(rows, &opts()).await.unwrap();
        assert_eq!(report.passed, passes, "{report:#?}");
        let verify = vtop_cli::commands::verify_manifest_deep(
            engine.store.as_ref(),
            engine.backend.as_ref(),
            &muri,
            &keyring,
        )
        .await
        .unwrap();
        let line = verify
            .lines
            .iter()
            .find(|l| l.starts_with("authentication"))
            .unwrap()
            .clone();
        if passes {
            assert_eq!(report.manifest_keys, [("none".to_string(), 1)].into());
            assert!(line.ends_with(": OK (keyed BLAKE3)"), "{line}");
        } else {
            assert_eq!(count(&report, ScrubResult::Unauthenticated), 1);
            assert!(
                report.findings[0].detail.contains("key id \"q3\""),
                "{}",
                report.findings[0].detail
            );
            assert!(
                line.ends_with(": FAILED (MAC missing or invalid)"),
                "{line}"
            );
        }
    }
}
