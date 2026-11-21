//! A source under a long directory drains.
//!
//! The object name, the batch id and so the manifest and receipt names all
//! embedded the whole source path, and the partition prefix did too. Under a
//! directory of a few hundred characters a name passed 255 bytes, the
//! filesystem refused it ("File name too long"), the range was handed back,
//! and the next pass built the same name and failed again: the engine never
//! drained, and the offsets growing with the file made it worse.
//!
//! These tests archive a file under such a directory through the localfs
//! backend, range by range, and check the whole archive is there under names
//! the filesystem accepts.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};

use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::types::SourceType;

/// A directory under `base` whose path is at least `chars` long, built of
/// components no longer than 100 characters, with a file in it.
fn long_input(base: &Path, chars: usize) -> PathBuf {
    let mut dir = base.to_path_buf();
    let mut n = 0;
    while dir.to_string_lossy().len() - base.to_string_lossy().len() < chars {
        dir = dir.join(format!("customer-data-archive-{n}-{}", "x".repeat(70)));
        n += 1;
    }
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("application-service-events.log");
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..12 {
        writeln!(f, "record {i:04} {}", "payload ".repeat(8)).unwrap();
    }
    path
}

fn config(dir: &Path, input: &Path, template: &str) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "localfs",
    );
    cfg.upload.local_path = Some(dir.join("store").to_string_lossy().into_owned());
    cfg.partitioning.template = template.into();
    // Four records a batch: three ranges, the later ones at larger offsets.
    cfg.batching.max_records = 4;
    cfg
}

/// Every file under `root`, with its path.
fn files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

async fn drain_and_check(template: &str, path_chars: usize) {
    let dir = tempfile::tempdir().unwrap();
    let input = long_input(&dir.path().join("in"), path_chars);
    let source = input.to_string_lossy().into_owned();
    assert!(
        source.len() > 255,
        "the source path must be long enough to matter: {} bytes",
        source.len()
    );
    let cfg = config(dir.path(), &input, template);
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    assert_eq!(engine.backend.backend_name(), "localfs");

    let mut committed = 0;
    for pass in 0..10 {
        let out = engine.process_once(SourceType::File).await.unwrap();
        if out.is_empty() {
            break;
        }
        assert!(
            out.iter().all(|o| o.committed),
            "pass {pass} did not commit (before the fix the same range failed on every pass): {out:?}"
        );
        committed += out.len();
    }
    assert_eq!(committed, 3, "twelve records at four a batch");

    let rows = engine.store.list_batches().await.unwrap();
    assert_eq!(rows.len(), 3, "{rows:#?}");
    let len = std::fs::metadata(&input).unwrap().len();
    let mut ranges: Vec<(u64, u64)> = rows
        .iter()
        .map(|r| match &r.progress_end {
            vtop_core::types::ProgressMarker::File {
                start_byte,
                end_byte,
                ..
            } => (*start_byte, *end_byte),
            other => panic!("not a file range: {other:?}"),
        })
        .collect();
    ranges.sort_unstable();
    assert_eq!(ranges.first().map(|r| r.0), Some(0), "{ranges:?}");
    assert_eq!(ranges.last().map(|r| r.1), Some(len), "{ranges:?}");

    let store = dir.path().join("store");
    let archived = files(&store);
    let manifests = archived
        .iter()
        .filter(|p| p.to_string_lossy().ends_with(".manifest.json"))
        .count();
    assert_eq!(manifests, 3, "{archived:?}");
    for path in &archived {
        for component in path.strip_prefix(&store).unwrap().components() {
            let name = component.as_os_str().to_string_lossy();
            assert!(name.len() <= 255, "{} bytes: {name}", name.len());
        }
    }
    // Nothing is left half-written, and the ledger rows name objects that
    // are there.
    assert!(
        archived
            .iter()
            .all(|p| !p.to_string_lossy().ends_with(".partial")),
        "{archived:?}"
    );
    for row in &rows {
        let uri = row.object_uri.as_deref().unwrap();
        let (bucket, key) = uri.strip_prefix("s3://").unwrap().split_once('/').unwrap();
        assert!(store.join(bucket).join(key).is_file(), "{uri}");
    }
}

/// The object name and the manifest name carry the path: a template with no
/// `{source}` leaves them the only place it shows up.
#[tokio::test]
async fn a_file_under_a_long_directory_drains_when_only_the_names_carry_the_path() {
    drain_and_check("tenant={tenant}/format={format}/", 300).await;
}

/// The partition prefix carries the path too, as `source=<path>`.
#[tokio::test]
async fn a_file_under_a_long_directory_drains_with_the_default_partition_template() {
    drain_and_check(vtop_core::partitioning::DEFAULT_TEMPLATE, 300).await;
}
