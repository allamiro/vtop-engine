//! #774: `audit-coverage` reports the objects failed attempts left that no
//! committed row names.
//!
//! An attempt that uploaded its object and then failed leaves it in the store
//! under its own key. When the replay writes another key (a per-attempt key,
//! different bytes, a fallback) the object is named by no committed row:
//! storage cost, never loss, and under Object Lock retained until its date.
//! The report reads the ledger and the prune digests only, so it needs no LIST
//! permission and makes no store request. These tests pin:
//!
//! - a failed attempt's object that no committed row names is reported;
//! - after a replay that reuses the object, it is not;
//! - a committed object is never reported, and the page limit never changes
//!   the counts or the gauge;
//! - the command lists it, writes the gauge, deletes nothing and keeps its
//!   exit status for lost data.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use vtop_cli::coverage::{self, AuditReport, OrphanKind, OrphanOptions};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ChecksumAlgorithm, SourceType};
use vtop_state::BatchRecord;
use vtop_upload::{MockBackend, UploadBackend};

fn write_lines(path: &Path, lines: usize) {
    let mut f = std::fs::File::create(path).unwrap();
    for i in 0..lines {
        writeln!(f, "app record {i}").unwrap();
    }
}

fn config(dir: &Path, input: &Path, backend: &str) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        backend,
    );
    cfg.partitioning.template = "tenant={tenant}/source={source}/format={format}/".into();
    cfg
}

async fn mock_engine(cfg: VtopConfig, backend: &Arc<MockBackend>) -> Engine {
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;
    engine
}

async fn ledger(engine: &Engine) -> Vec<BatchRecord> {
    engine.store.list_batches().await.unwrap()
}

fn report_of(rows: &[BatchRecord], options: &OrphanOptions) -> AuditReport {
    AuditReport {
        ledger_rows: rows.len(),
        digest_rows: 0,
        digest_files: 0,
        units: coverage::audit(rows.to_vec()),
        storage: None,
        orphans: coverage::orphans(rows, options),
    }
}

fn gauge(text: &str, name: &str, source_type: &str) -> Option<f64> {
    let prefix = format!("{name}{{source_type=\"{source_type}\"}} ");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .map(|v| v.parse().unwrap())
}

/// With checksums off every attempt has its own key. The first attempt's
/// manifest is throttled, so it fails after its object landed; the replay
/// writes a second object and commits. The first object is named by no
/// committed row: the report lists it, classified as a failed attempt's, and
/// the gauge counts it. The audit still passes: nothing was lost.
#[tokio::test]
async fn a_failed_attempts_object_that_no_committed_row_names_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let mut cfg = config(dir.path(), &input, "mock");
    cfg.checksum.algorithm = ChecksumAlgorithm::None;
    let mut engine = mock_engine(cfg, &backend).await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed, "the throttled manifest fails it");
    let failed = engine
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    let orphan = failed.object_uri.clone().expect("the object landed");
    assert!(backend.contains(&orphan));

    // Before the replay: the object is the only thing the range has.
    let rows = ledger(&engine).await;
    assert_eq!(
        coverage::orphans(&rows, &OrphanOptions::default()).count,
        1,
        "named by the failed row alone"
    );

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    let committed = second[0].object_uri.clone().unwrap();
    assert_ne!(committed, orphan, "a per-attempt key is another object");

    let rows = ledger(&engine).await;
    let report = report_of(&rows, &OrphanOptions::default());
    assert_eq!(report.orphans.count, 1);
    let source = &report.orphans.sources[0];
    assert_eq!(source.source_type, SourceType::File);
    assert_eq!(source.objects.len(), 1);
    let object = &source.objects[0];
    assert_eq!(object.uri, orphan, "the failed attempt's object");
    assert_ne!(object.uri, committed, "never the committed one");
    assert!(matches!(
        object.kind,
        OrphanKind::Failed | OrphanKind::ReplayRequired
    ));
    assert_eq!(object.batch_ids, [first[0].batch_id.clone()]);
    assert_eq!(
        object.size_bytes,
        failed.object_size_bytes.map(|s| s as u64),
        "the size the ledger recorded"
    );
    assert_eq!(source.size_bytes, object.size_bytes.unwrap());
    assert!(report.passed(), "an orphan is not lost data");

    let text = report.prometheus_text(1);
    assert_eq!(
        gauge(&text, "vtop_coverage_audit_unreferenced_objects", "file"),
        Some(1.0),
        "{text}"
    );
    assert_eq!(
        gauge(
            &text,
            "vtop_coverage_audit_unreferenced_object_bytes",
            "file"
        ),
        Some(object.size_bytes.unwrap() as f64),
        "{text}"
    );
    assert!(!text.contains(&orphan), "a key is never a label");
    // The report read the ledger only: the store holds both objects still.
    assert!(backend.contains(&orphan) && backend.contains(&committed));
}

/// The same failure with a derived key: the replay finds the failed
/// attempt's object and names it in its own row. After it, nothing is
/// reported; before it, the object was.
#[tokio::test]
async fn after_a_replay_that_reuses_the_object_it_is_not_reported() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let mut engine = mock_engine(config(dir.path(), &input, "mock"), &backend).await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let uploaded = engine
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap()
        .object_uri
        .unwrap();
    let before = coverage::orphans(&ledger(&engine).await, &OrphanOptions::default());
    assert_eq!(before.count, 1, "reported while no row commits it");
    assert_eq!(before.sources[0].objects[0].uri, uploaded);

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    assert_eq!(
        second[0].object_uri.as_deref(),
        Some(uploaded.as_str()),
        "the replay reused the object"
    );
    let after = report_of(&ledger(&engine).await, &OrphanOptions::default());
    assert_eq!(after.orphans.count, 0, "{:?}", after.orphans);
    assert!(after
        .lines()
        .iter()
        .any(|l| l == "unreferenced objects: none"));
    let text = after.prometheus_text(1);
    assert_eq!(
        gauge(&text, "vtop_coverage_audit_unreferenced_objects", "file"),
        Some(0.0),
        "{text}"
    );
}

/// A failed row cloned from a committed one, naming `uri`.
fn failed_naming(committed: &BatchRecord, id: &str, uri: &str, size: i64) -> BatchRecord {
    let mut row = committed.clone();
    row.batch_id = id.into();
    // Inserted as a new batch, then failed, as the engine writes it.
    row.state = BatchState::Batching;
    row.object_uri = Some(uri.into());
    row.object_size_bytes = Some(size);
    row.manifest_uri = None;
    row
}

/// The command, on the localfs backend: it lists the objects, writes the
/// gauge, keeps its exit status for lost data, pages its keys, and touches
/// nothing in the store.
#[tokio::test]
async fn the_command_lists_pages_and_counts_without_deleting_or_failing() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, 4);
    let store = dir.path().join("store");
    let mut cfg = config(dir.path(), &input, "localfs");
    cfg.upload.local_path = Some(store.to_string_lossy().into_owned());
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();
    {
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let done = engine.process_once(SourceType::File).await.unwrap();
        assert!(done.iter().all(|o| o.committed), "{done:?}");
        let rows = engine.store.list_batches().await.unwrap();
        let committed = rows
            .into_iter()
            .find(|r| r.state == BatchState::SourceCommitted)
            .unwrap();
        for (id, key, size) in [("f1", "orphan-a", 11), ("f2", "orphan-b", 22)] {
            let uri = format!("{}/{key}", store.display());
            // The object really is in the store, so the check that nothing is
            // deleted means something.
            std::fs::write(&uri, vec![0u8; size as usize]).unwrap();
            engine
                .store
                .save_batch_state(&failed_naming(&committed, id, &uri, size))
                .await
                .unwrap();
            engine
                .store
                .mark_failed(id, "manifest upload failed")
                .await
                .unwrap();
        }
    }
    let before: Vec<PathBuf> = walk(&store);

    let metrics = dir.path().join("coverage.prom");
    let run = |extra: &[&str]| {
        let mut args = vec![
            "vtopctl",
            "audit-coverage",
            "--config",
            config_path.to_str().unwrap(),
            "--metrics-file",
            metrics.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        let cli = vtop_cli::commands::Cli::try_parse_from(args).unwrap();
        Box::pin(vtop_cli::commands::dispatch(cli))
    };

    assert_eq!(run(&[]).await, 0, "an orphan does not fail the audit");
    let text = std::fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        gauge(&text, "vtop_coverage_audit_unreferenced_objects", "file"),
        Some(2.0),
        "{text}"
    );
    assert_eq!(
        gauge(
            &text,
            "vtop_coverage_audit_unreferenced_object_bytes",
            "file"
        ),
        Some(33.0),
        "{text}"
    );

    // A page of one key keeps the counts exact; so does listing none.
    assert_eq!(run(&["--orphans-limit", "1"]).await, 0);
    let text = std::fs::read_to_string(&metrics).unwrap();
    assert_eq!(
        gauge(&text, "vtop_coverage_audit_unreferenced_objects", "file"),
        Some(2.0),
        "the gauge does not depend on the page: {text}"
    );
    assert_eq!(run(&["--orphans-limit", "0"]).await, 0);
    assert_eq!(
        run(&["--orphans-limit", "1", "--orphans-after", "x"]).await,
        0
    );

    assert_eq!(walk(&store), before, "the report deleted nothing");
}

fn walk(root: &Path) -> Vec<PathBuf> {
    fn go(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                go(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    go(root, &mut out);
    out.sort();
    out
}
