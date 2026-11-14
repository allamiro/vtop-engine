//! #668: a failed multipart part must not discard the parts that already
//! completed.
//!
//! The engine used to abort the whole multipart session whenever an upload
//! failed, so the replay of the batch sent every part again from the first
//! byte. On a policed 10 Mbit/s link that is minutes per 8 MiB part. Since
//! #626 a replay of the same bytes names the same object, and so the same
//! session; these tests pin that the engine keeps the session when an upload
//! fails, and that the replay, in the same process, after a restart or in the
//! next engine to hold the work directory, resumes it and sends only what is
//! missing. The commit rule is unchanged: a resumed object is read back and
//! verified like any other before its source progress is committed.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::state_machine::BatchState;
use vtop_core::types::{CompressionType, SourceType};
use vtop_upload::multipart::{load_session, MultipartSession};
use vtop_upload::{MockBackend, PartFault, UploadBackend};

const TEMPLATE: &str = "tenant={tenant}/source={source}/format={format}/";

/// The tests in this file take turns. One captures the engine's log through
/// a thread-local subscriber, and `tracing` caches whether a callsite is
/// enabled: an engine running on another test thread, with no subscriber,
/// can race that cache and the captured log comes back empty.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// About 3.6 KB of records: with 1000-byte parts and no compression, four
/// parts.
fn write_records(path: &Path, tag: &str) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for i in 0..40 {
        writeln!(
            f,
            "{tag} record {i:04} payload abcdefghijklmnopqrstuvwxyz0123456789 ABCDEFGHIJKLMNOP"
        )
        .unwrap();
    }
}

fn config(dir: &Path, input: &Path) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.partitioning.template = TEMPLATE.into();
    cfg.compression.kind = CompressionType::None;
    cfg.upload.multipart_part_size_bytes = 1_000;
    cfg.upload.multipart_threshold_bytes = 1_000;
    cfg.upload.multipart_max_parallelism = 4;
    cfg
}

/// A config with two attempts per part, set the way an operator sets it, so
/// this file compiles against a tree without the key.
fn with_part_attempts(cfg: VtopConfig, attempts: u32) -> VtopConfig {
    let mut value = serde_json::to_value(&cfg).unwrap();
    value["upload"]["multipart_part_attempts"] = attempts.into();
    serde_json::from_value(value).unwrap()
}

async fn engine(cfg: VtopConfig, backend: &Arc<MockBackend>) -> Engine {
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;
    engine
}

/// The multipart sessions in the work directory.
fn sessions(dir: &Path) -> Vec<(PathBuf, MultipartSession)> {
    let Ok(entries) = std::fs::read_dir(dir.join("work")) else {
        return Vec::new();
    };
    entries
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".multipart.json"))
        .map(|p| {
            let s = load_session(&p).unwrap();
            (p, s)
        })
        .collect()
}

fn sends(backend: &MockBackend) -> [usize; 4] {
    [1, 2, 3, 4].map(|n| backend.part_sends(n))
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> LogWriter {
        LogWriter(self.0.clone())
    }
}

impl Logs {
    /// Capture this thread's events until the guard drops; the tests run on
    /// the current-thread runtime, where the upload is driven.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        (logs.clone(), tracing::subscriber::set_default(subscriber))
    }

    fn events(&self, event: &str) -> Vec<Value> {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|v| {
                v.get("fields")
                    .and_then(|f| f.get("event"))
                    .and_then(Value::as_str)
                    == Some(event)
            })
            .map(|v| {
                let mut fields = v["fields"].clone();
                fields["level"] = v["level"].clone();
                fields
            })
            .collect()
    }

    fn one(&self, event: &str) -> Value {
        let found = self.events(event);
        assert_eq!(
            found.len(),
            1,
            "one {event}, found {found:?}; all: {}",
            String::from_utf8_lossy(&self.0.lock().unwrap())
        );
        found[0].clone()
    }
}

fn says(fields: &Value, parts: &[&str]) {
    let message = fields["message"].as_str().unwrap_or_default();
    for part in parts {
        assert!(message.contains(part), "{message:?} should say {part:?}");
    }
}

/// Part 2 fails on every attempt: the batch fails, the session and the three
/// parts that completed are kept, and the replay of the same bytes resumes
/// it, sends part 2 alone, verifies the whole object by read-back and
/// commits. Before #668 the engine aborted the session on failure and the
/// replay sent all four parts again.
#[tokio::test]
async fn a_failed_part_keeps_the_session_and_the_replay_sends_only_that_part() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_records(&input, "app");
    let backend = Arc::new(MockBackend::new().with_part_fault(2, PartFault::Reset, 2));
    let mut engine = engine(with_part_attempts(config(dir.path(), &input), 2), &backend).await;
    let (logs, _guard) = Logs::capture();

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(first[0].final_state, BatchState::Failed, "{first:?}");
    assert_eq!(sends(&backend), [1, 2, 1, 1], "part 2 had two attempts");

    let kept = sessions(dir.path());
    assert_eq!(kept.len(), 1, "the failed upload's session is kept");
    let (_, session) = &kept[0];
    assert_eq!(
        session.parts.keys().copied().collect::<Vec<_>>(),
        vec![1, 3, 4]
    );
    assert!(
        backend.has_pending_multipart(&session.object_uri, &session.upload_id),
        "the store still holds the upload and its parts"
    );
    let notice = logs.one("multipart_session_kept");
    assert_eq!(notice["level"], "WARN");
    assert_eq!(notice["batch_id"], first[0].batch_id.as_str());
    assert_eq!(notice["key"], session.object_uri.as_str());
    says(
        &notice,
        &[
            "part 2 of 4",
            "after 2 attempts",
            "connection reset by peer",
            "the upload session keeps 3 of 4 parts",
            "the batch was marked failed and will be replayed",
            "the replay resumes the session if its bytes are unchanged",
            "upload.multipart_abandon_after_secs (86400 s)",
        ],
    );

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "{second:?}");
    assert_eq!(
        second[0].object_uri.as_deref(),
        Some(session.object_uri.as_str()),
        "the replay names the same object"
    );
    assert_eq!(
        sends(&backend),
        [1, 3, 1, 1],
        "the replay sent part 2 once more and nothing else"
    );
    assert!(
        sessions(dir.path()).is_empty(),
        "a completed session is removed"
    );
    let resumed = logs.one("multipart_session_resumed");
    assert_eq!(resumed["level"], "INFO");
    says(
        &resumed,
        &[
            &format!("resuming multipart session for {}", session.object_uri),
            "3 of 4 parts already uploaded",
            "not re-sent",
        ],
    );
}

/// The process stops between the failed attempt and its replay. The session
/// is on disk in the work directory, so the next process resumes it.
#[tokio::test]
async fn a_restart_between_attempts_resumes_the_session() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_records(&input, "restart");
    let backend = Arc::new(MockBackend::new().with_part_fault(2, PartFault::Reset, 2));
    let cfg = with_part_attempts(config(dir.path(), &input), 2);

    {
        let mut first = engine(cfg.clone(), &backend).await;
        let out = first.process_once(SourceType::File).await.unwrap();
        assert_eq!(out[0].final_state, BatchState::Failed, "{out:?}");
    }
    assert_eq!(sessions(dir.path()).len(), 1);

    let mut second = engine(cfg, &backend).await;
    second.recover().await.unwrap();
    let out = second.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(
        sends(&backend),
        [1, 3, 1, 1],
        "the restarted engine sent part 2 alone"
    );
    assert!(sessions(dir.path()).is_empty());
}

/// Two engines and one session: the work directory, and the sessions in it,
/// belong to one engine at a time. A second engine is refused while the
/// first holds the directory, so the session is never driven by two engines
/// at once; once the first is gone the next one resumes it.
#[tokio::test]
async fn a_second_engine_waits_for_the_work_directory_and_then_resumes_the_session() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_records(&input, "two-engines");
    let backend = Arc::new(MockBackend::new().with_part_fault(2, PartFault::Reset, 2));
    let cfg = with_part_attempts(config(dir.path(), &input), 2);
    let streams = || StreamsConfig { streams: vec![] };

    let mut first = Engine::new_exclusive(cfg.clone(), streams()).await.unwrap();
    first.backend = backend.clone();
    let out = first.process_once(SourceType::File).await.unwrap();
    assert_eq!(out[0].final_state, BatchState::Failed, "{out:?}");
    assert_eq!(sessions(dir.path()).len(), 1);

    let refused = match Engine::new_exclusive(cfg.clone(), streams()).await {
        Ok(_) => panic!("a second engine must not share the work directory and its sessions"),
        Err(e) => e.to_string(),
    };
    assert!(
        refused.contains("another VTOP engine already holds"),
        "{refused}"
    );
    assert_eq!(sessions(dir.path()).len(), 1, "the refusal touched nothing");

    drop(first);
    let mut second = Engine::new_exclusive(cfg, streams()).await.unwrap();
    assert_eq!(
        sessions(dir.path()).len(),
        1,
        "the startup sweep leaves a session younger than the abandon window"
    );
    second.backend = backend.clone();
    second.recover().await.unwrap();
    let out = second.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(sends(&backend), [1, 3, 1, 1]);
}

/// The source grew before the replay, so the replay's bytes differ: they
/// name another object and start their own session. The old one is not
/// resumed; it stays, with its upload, for the abandoned-session sweep.
#[tokio::test]
async fn other_bytes_on_the_replay_start_their_own_session() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_records(&input, "grown");
    let backend = Arc::new(MockBackend::new().with_part_fault(2, PartFault::Reset, 2));
    let mut engine = engine(with_part_attempts(config(dir.path(), &input), 2), &backend).await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first[0].final_state, BatchState::Failed, "{first:?}");
    let old = sessions(dir.path());
    assert_eq!(old.len(), 1);
    let (old_path, old_session) = old[0].clone();

    write_records(&input, "more");
    let before = backend.part_sends_total();
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second.iter().all(|o| o.committed), "{second:?}");
    assert_ne!(
        second[0].object_uri.as_deref(),
        Some(old_session.object_uri.as_str()),
        "other bytes name another object"
    );
    let row = engine
        .store
        .get_batch(&second[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    let size = row.object_size_bytes.unwrap() as usize;
    assert_eq!(
        backend.part_sends_total() - before,
        size.div_ceil(1_000),
        "every part of the grown range ({size} bytes) was sent, none reused"
    );
    assert!(old_path.exists(), "the old session was not adopted");
    assert!(
        backend.has_pending_multipart(&old_session.object_uri, &old_session.upload_id),
        "its upload is left for the sweep"
    );
}

/// A resumed object is verified exactly like a fresh one. A part the store
/// damaged after acknowledging it completes into an object whose read-back
/// fails, so the batch is never committed: the commit rule does not trust
/// what a session remembers.
#[tokio::test]
async fn a_resumed_object_that_does_not_read_back_is_never_committed() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_records(&input, "damaged");
    let backend = Arc::new(MockBackend::new().with_part_fault(2, PartFault::Reset, 2));
    let mut engine = engine(with_part_attempts(config(dir.path(), &input), 2), &backend).await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first[0].final_state, BatchState::Failed, "{first:?}");
    backend.corrupt_pending_part(3);

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(
        !second[0].committed,
        "a resumed object whose bytes do not verify must not commit: {second:?}"
    );
    let rows = engine.store.list_batches().await.unwrap();
    assert!(
        rows.iter().all(|r| r.state != BatchState::SourceCommitted),
        "{rows:?}"
    );
}

/// The store refuses the complete on both of an attempt's uploads (#668,
/// review): the batch fails, and nothing is kept that every replay would
/// complete into the same refusal. The replay starts afresh and commits.
#[tokio::test]
async fn a_complete_the_store_keeps_refusing_is_not_kept_for_the_replay() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_records(&input, "refused");
    let backend = Arc::new(MockBackend::new().with_complete_faults(2));
    let mut engine = engine(config(dir.path(), &input), &backend).await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first[0].final_state, BatchState::Failed, "{first:?}");
    assert!(sessions(dir.path()).is_empty(), "no session is kept");
    assert_eq!(
        backend.pending_multipart_count(),
        0,
        "no upload is left billed"
    );

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second.iter().all(|o| o.committed), "{second:?}");
}
