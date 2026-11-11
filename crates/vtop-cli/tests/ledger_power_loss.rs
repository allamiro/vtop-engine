//! #626: how many archived ranges a power loss makes the engine archive again.
//!
//! **System model.** One engine process on one SQLite ledger in WAL mode. A
//! power loss keeps the database file and a prefix of the WAL: the frames
//! that were synced. SQLite syncs the WAL when a commit runs at
//! `synchronous=FULL`, and at a checkpoint. Under `NORMAL` no commit syncs,
//! so in the worst case only what was synced at the last checkpoint survives.
//! The test holds the run below the auto-checkpoint size and checks that no
//! checkpoint ran, so a synced length is a WAL prefix.
//!
//! **What is simulated.** Engine A archives four rounds of appends to three
//! files. Every ledger write goes through a wrapper that records the WAL
//! length after it. For a crash after write `k`, three survivors are built:
//!
//! - `kill`: a process crash, no power loss. The whole WAL up to `k` survives.
//! - `normal`: power loss with every write at NORMAL, as on `main` before
//!   #626. Only the WAL from before the run survives.
//! - `critical`: power loss with VERIFIED and SOURCE_COMMITTED at FULL, as
//!   now. The WAL up to the last of those at or before `k` survives.
//!
//! Engine B restarts on each survivor, recovers, and archives the rest of the
//! files. Counted: the batches A had archived by `k` (its object put and its
//! OBJECT_UPLOADED written) whose bytes B archives again.
//!
//! The SQLite sync behaviour itself is the model's input, not something this
//! test observes: `sqlite_store`'s
//! `verified_and_source_committed_are_written_at_synchronous_full` pins that
//! those two transitions are written by the FULL connection.
//!
//! **Invariants.**
//! - Safety, every survivor: B ends with every byte of every file committed.
//! - `critical`: what is archived twice is at most the batches A had archived
//!   but not durably VERIFIED at the crash, as after a process crash.
#![cfg(unix)]

use async_trait::async_trait;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType};
use vtop_state::{BatchPatch, BatchRecord, CommittedCursor, CommittedKafkaOffset, StateStore};
use vtop_upload::{MockBackend, UploadBackend};

const FILES: usize = 3;
const ROUNDS: usize = 4;
const LINES_PER_ROUND: usize = 5;

/// A file range: path, inode, start byte, end byte.
type Range = (String, Option<u64>, u64, u64);

fn range_of(rec: &BatchRecord) -> Range {
    let (
        ProgressMarker::File {
            path,
            inode,
            start_byte,
            ..
        },
        ProgressMarker::File { end_byte, .. },
    ) = (&rec.progress_start, &rec.progress_end)
    else {
        panic!("a file batch carries file markers: {rec:?}");
    };
    (path.clone(), *inode, *start_byte, *end_byte)
}

fn overlaps(a: &Range, b: &Range) -> bool {
    a.0 == b.0 && a.1 == b.1 && a.2 < b.3 && b.2 < a.3
}

#[derive(Default)]
struct Log {
    /// WAL length after each write, in write order.
    wal_after: Vec<u64>,
    /// Whether that write was VERIFIED or SOURCE_COMMITTED.
    critical: Vec<bool>,
    /// Batch id -> its range.
    ranges: HashMap<String, Range>,
    /// Batch id -> index of its OBJECT_UPLOADED write.
    uploaded_at: HashMap<String, usize>,
    /// Batch id -> index of its VERIFIED write.
    verified_at: HashMap<String, usize>,
}

/// Forwards to the real store and records the WAL length after every write.
/// Writes are serialized, so the length after write `k` is the WAL holding
/// exactly writes `1..=k`.
struct Recorder {
    inner: Box<dyn StateStore>,
    wal: PathBuf,
    log: Arc<tokio::sync::Mutex<Log>>,
}

impl Recorder {
    async fn write<T>(
        &self,
        what: impl std::future::Future<Output = Result<T, VtopError>>,
        note: impl FnOnce(&mut Log, usize),
    ) -> Result<T, VtopError> {
        let mut log = self.log.lock().await;
        let out = what.await;
        let index = log.wal_after.len();
        let len = std::fs::metadata(&self.wal).map(|m| m.len()).unwrap_or(0);
        log.wal_after.push(len);
        log.critical.push(false);
        note(&mut log, index);
        out
    }
}

fn note_transition(log: &mut Log, index: usize, batch_id: &str, to: BatchState) {
    match to {
        BatchState::ObjectUploaded => {
            log.uploaded_at.insert(batch_id.to_owned(), index);
        }
        BatchState::Verified => {
            log.verified_at.insert(batch_id.to_owned(), index);
            log.critical[index] = true;
        }
        BatchState::SourceCommitted => log.critical[index] = true,
        _ => {}
    }
}

#[async_trait]
impl StateStore for Recorder {
    async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
        let range = range_of(rec);
        let id = rec.batch_id.clone();
        self.write(self.inner.save_batch_state(rec), |log, _| {
            log.ranges.insert(id, range);
        })
        .await
    }
    async fn update_batch_state(
        &self,
        batch_id: &str,
        to: BatchState,
        patch: &BatchPatch,
    ) -> Result<(), VtopError> {
        self.write(
            self.inner.update_batch_state(batch_id, to, patch),
            |log, i| note_transition(log, i, batch_id, to),
        )
        .await
    }
    async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
        self.inner.get_batch(batch_id).await
    }
    async fn claim_incomplete_batches(
        &self,
        owner: &str,
        now: &str,
        lease_until: &str,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        self.write(
            self.inner.claim_incomplete_batches(owner, now, lease_until),
            |_, _| {},
        )
        .await
    }
    async fn max_committed_end_bytes(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<(String, u64)>, VtopError> {
        self.inner.max_committed_end_bytes(source_type).await
    }
    async fn committed_cursors(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<CommittedCursor>, VtopError> {
        self.inner.committed_cursors(source_type).await
    }
    async fn committed_cursors_at(
        &self,
        source_type: SourceType,
        path: &str,
    ) -> Result<Vec<CommittedCursor>, VtopError> {
        self.inner.committed_cursors_at(source_type, path).await
    }
    async fn committed_kafka_offsets(&self) -> Result<Vec<CommittedKafkaOffset>, VtopError> {
        self.inner.committed_kafka_offsets().await
    }
    async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.inner.list_batches().await
    }
    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.inner.list_incomplete_batches().await
    }
    async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.inner.list_failed_batches().await
    }
    async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError> {
        self.write(self.inner.mark_failed(batch_id, message), |_, _| {})
            .await
    }
    async fn mark_verified(&self, batch_id: &str) -> Result<(), VtopError> {
        self.write(self.inner.mark_verified(batch_id), |log, i| {
            note_transition(log, i, batch_id, BatchState::Verified)
        })
        .await
    }
    async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError> {
        self.write(self.inner.mark_source_committed(batch_id), |log, i| {
            note_transition(log, i, batch_id, BatchState::SourceCommitted)
        })
        .await
    }
    async fn prune_committed_history(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<u64, VtopError> {
        self.write(
            self.inner.prune_committed_history(older_than, limit),
            |_, _| {},
        )
        .await
    }
    async fn prune_candidates(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        self.inner.prune_candidates(older_than, limit).await
    }
    async fn prune_listed(&self, older_than: &str, batch_ids: &[String]) -> Result<u64, VtopError> {
        self.write(self.inner.prune_listed(older_than, batch_ids), |_, _| {})
            .await
    }
}

fn wal_of(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push("-wal");
    PathBuf::from(name)
}

fn engine_config(
    dir: &Path,
    work: &str,
    db: &Path,
    inputs: &[PathBuf],
) -> vtop_core::config::VtopConfig {
    let mut cfg = file_config(
        dir.join(work).to_str().unwrap(),
        &format!("sqlite://{}", db.display()),
        inputs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        "mock",
    );
    cfg.batching.max_batch_age_seconds = 3_600;
    cfg
}

/// Engine A's run, as a crash leaves it: the database file, the whole WAL,
/// the input files and the objects A put.
struct Crashed<'a> {
    dir: &'a Path,
    db_bytes: Vec<u8>,
    wal_bytes: Vec<u8>,
    inputs: &'a [PathBuf],
    backend: Arc<MockBackend>,
    /// Every batch id A wrote: B's own batches are the others.
    written_by_a: HashSet<String>,
}

impl Crashed<'_> {
    /// What survives: the database file and the first `wal_len` bytes of the
    /// WAL. Engine B restarts on it and archives the rest of the files.
    /// Returns the ranges of the batches B wrote, and checks that B left every
    /// byte committed.
    async fn restart(&self, wal_len: u64) -> Vec<Range> {
        let (dir, inputs, tag) = (self.dir, self.inputs, wal_len);
        let db = dir.join(format!("survivor-{tag}.db"));
        std::fs::write(&db, &self.db_bytes).unwrap();
        std::fs::write(wal_of(&db), &self.wal_bytes[..wal_len as usize]).unwrap();

        let mut b = Engine::new(
            engine_config(dir, &format!("work-{tag}"), &db, inputs),
            StreamsConfig { streams: vec![] },
        )
        .await
        .unwrap();
        b.backend = self.backend.clone() as Arc<dyn UploadBackend>;
        // A VERIFIED row held back behind a pending range below it commits on a
        // later pass, so recover and read until nothing is left.
        for _ in 0..4 {
            b.recover().await.unwrap();
            let outcomes = b.process_once(SourceType::File).await.unwrap();
            assert!(
                outcomes.iter().all(|o| o.committed),
                "{tag}: every batch B archives commits: {outcomes:?}"
            );
            if outcomes.is_empty() && b.store.list_incomplete_batches().await.unwrap().is_empty() {
                break;
            }
        }

        let cursors = b.store.committed_cursors(SourceType::File).await.unwrap();
        for input in inputs {
            let path = input.to_string_lossy();
            let len = std::fs::metadata(input).unwrap().len();
            let cursor = cursors.iter().find(|c| c.path == path);
            assert_eq!(
                cursor.map(|c| c.end_byte),
                Some(len),
                "{tag}: every byte of {path} is committed after the restart"
            );
        }
        b.store
            .list_batches()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| !self.written_by_a.contains(&r.batch_id))
            .map(|r| range_of(&r))
            .collect()
    }
}

#[derive(Default)]
struct Tally {
    /// Per crash point: A's archived batches B archived again.
    twice: Vec<usize>,
}

impl Tally {
    fn summary(&self) -> String {
        let mut v = self.twice.clone();
        v.sort_unstable();
        let total: usize = v.iter().sum();
        format!(
            "crash points {}, median {}, max {}, total {}",
            v.len(),
            v[v.len() / 2],
            v[v.len() - 1],
            total
        )
    }
}

#[tokio::test]
async fn a_power_loss_archives_again_only_the_batches_still_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let db = dir.join("state.db");
    let inputs: Vec<PathBuf> = (0..FILES)
        .map(|f| dir.join(format!("app-{f}.log")))
        .collect();
    for input in &inputs {
        std::fs::write(input, "").unwrap();
    }
    let backend = Arc::new(MockBackend::new());

    // ---- Engine A: archive ROUNDS rounds of appends -----------------------
    let mut a = Engine::new(
        engine_config(dir, "work-a", &db, &inputs),
        StreamsConfig { streams: vec![] },
    )
    .await
    .unwrap();
    a.backend = backend.clone() as Arc<dyn UploadBackend>;
    let log = Arc::new(tokio::sync::Mutex::new(Log::default()));
    let inner = vtop_state::connect_state_store(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    a.store = Box::new(Recorder {
        inner,
        wal: wal_of(&db),
        log: log.clone(),
    });
    let db_at_start = std::fs::read(&db).unwrap();
    let wal_at_start = std::fs::metadata(wal_of(&db)).map(|m| m.len()).unwrap_or(0);

    for round in 0..ROUNDS {
        for (f, input) in inputs.iter().enumerate() {
            let mut out = std::fs::OpenOptions::new()
                .append(true)
                .open(input)
                .unwrap();
            for line in 0..LINES_PER_ROUND {
                writeln!(out, "file {f} round {round} line {line}").unwrap();
            }
        }
        let outcomes = a.process_once(SourceType::File).await.unwrap();
        assert_eq!(outcomes.len(), FILES, "one batch per file per round");
        assert!(outcomes.iter().all(|o| o.committed));
    }

    // The survivors are cut from the files as they are now, while A still
    // holds the database open: closing it would checkpoint.
    let db_bytes = std::fs::read(&db).unwrap();
    let wal_bytes = std::fs::read(wal_of(&db)).unwrap();
    assert_eq!(
        db_bytes, db_at_start,
        "no checkpoint ran during the run, so every synced length is a WAL prefix"
    );
    let log = std::mem::take(&mut *log.lock().await);
    assert_eq!(log.uploaded_at.len(), FILES * ROUNDS);
    assert!(
        log.wal_after.windows(2).all(|w| w[0] <= w[1]),
        "the WAL only grew"
    );

    // ---- Crash after every write; restart on each survivor ----------------
    let crashed = Crashed {
        dir,
        db_bytes,
        wal_bytes,
        inputs: &inputs,
        backend,
        written_by_a: log.ranges.keys().cloned().collect(),
    };
    let mut by_len: BTreeMap<u64, Vec<Range>> = BTreeMap::new();
    let (mut kill, mut normal, mut critical) =
        (Tally::default(), Tally::default(), Tally::default());
    let mut last_critical: Option<usize> = None;
    for k in 0..log.wal_after.len() {
        if log.critical[k] {
            last_critical = Some(k);
        }
        let archived: Vec<(&String, &Range)> = log
            .uploaded_at
            .iter()
            .filter(|(_, &at)| at <= k)
            .map(|(id, _)| (id, &log.ranges[id]))
            .collect();
        let durable_critical = last_critical.map_or(wal_at_start, |c| log.wal_after[c]);
        for (tally, wal_len) in [
            (&mut kill, log.wal_after[k]),
            (&mut normal, wal_at_start),
            (&mut critical, durable_critical),
        ] {
            if let Entry::Vacant(slot) = by_len.entry(wal_len) {
                slot.insert(crashed.restart(wal_len).await);
            }
            let again = &by_len[&wal_len];
            let twice = archived
                .iter()
                .filter(|(_, r)| again.iter().any(|b| overlaps(r, b)))
                .count();
            tally.twice.push(twice);
        }

        // In flight: archived by k, but not VERIFIED by the last synced write.
        let in_flight = archived
            .iter()
            .filter(|(id, _)| {
                log.verified_at
                    .get(*id)
                    .is_none_or(|&v| last_critical.is_none_or(|c| v > c))
            })
            .count();
        assert!(
            *critical.twice.last().unwrap() <= in_flight,
            "crash after write {k}: {} archived twice, but only {in_flight} were in flight",
            critical.twice.last().unwrap()
        );
    }

    // A WAL frame is a 24-byte header and one page; the WAL header holds the
    // page size at bytes 8..12.
    let wal = &crashed.wal_bytes;
    let page = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as u64;
    let frames = (wal.len() as u64 - wal_at_start) / (24 + page);
    println!(
        "WAL frames per batch: {:.1} (SQLite checkpoints at 1000 frames)",
        frames as f64 / (FILES * ROUNDS) as f64
    );
    println!("archived batches written again after a crash (#626):");
    println!("  kill (process crash) : {}", kill.summary());
    println!("  power loss, NORMAL   : {}", normal.summary());
    println!("  power loss, critical : {}", critical.summary());
    // After the last write everything was committed: a process crash and the
    // critical-FULL power loss archive nothing again, while NORMAL loses the
    // whole run and archives all of it again.
    assert_eq!(*kill.twice.last().unwrap(), 0);
    assert_eq!(*critical.twice.last().unwrap(), 0);
    assert_eq!(*normal.twice.last().unwrap(), FILES * ROUNDS);
}
