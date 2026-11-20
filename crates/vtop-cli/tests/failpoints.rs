//! Every failpoint kills the real `vtopctl` where it says it does (#725).
//!
//! The live sweep (`scripts/live-chaos/engine/run-sweep.sh`) kills the engine
//! at each point against real Kafka and MinIO and checks what a restart makes
//! of it. This file pins the cheaper half on every build of the feature: the
//! process dies by SIGKILL (not an exit code), at the point it names, with
//! the ledger exactly one write short of or past the transition the point
//! sits beside, and a restart without the failpoint archives every record.
//! An unknown point refuses to start and lists every valid one.
//!
//! System model: one crash-recovery process, a local file source, a local
//! filesystem store and a SQLite ledger. Safety: no record of the input is
//! missing from the store after the restart. Liveness: the restart reaches
//! SOURCE_COMMITTED within a bounded number of passes.
#![cfg(unix)]

use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::str::FromStr;

use vtop_cli::failpoints::{self, POINTS};

const RECORDS: usize = 20;

/// The passes a restart may take to commit everything: one to recover and
/// replay, one more in case the replay's window is read on the next cycle.
const MAX_RESTART_PASSES: usize = 3;

struct Lab {
    _dir: tempfile::TempDir,
    root: PathBuf,
    config: PathBuf,
}

impl Lab {
    /// A file source over `RECORDS` lines, a SQLite ledger, and a store:
    /// `localfs` (survives the kill), or `mock` with every object sent as a
    /// multipart upload, for the point that sits inside one.
    fn new(multipart: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let input = root.join("in.log");
        let lines: String = (0..RECORDS)
            .map(|i| format!("fp-record-{i:04} lorem ipsum dolor sit amet\n"))
            .collect();
        std::fs::write(&input, lines).unwrap();
        let upload = if multipart {
            // The mock takes parts of any size; a threshold of one byte makes
            // every object a one-part multipart upload.
            "  backend: mock\n  multipart_threshold_bytes: 1\n".to_string()
        } else {
            format!(
                "  backend: localfs\n  local_path: \"{}\"\n",
                root.join("store").display()
            )
        };
        let config = root.join("config.yaml");
        std::fs::write(
            &config,
            format!(
                r#"engine:
  name: vtop-failpoint-test
  tenant: default
  state_store: "sqlite://{state}"
  work_dir: "{work}"
  log_level: info
batching:
  max_records: 1000
  max_bytes: 104857600
  max_batch_age_seconds: 60
compression:
  type: none
checksum:
  algorithm: sha256
sources:
  file:
    enabled: true
    paths:
      - "{input}"
upload:
{upload}  bucket: "telemetry-data"
  prefix: "failpoints"
  region: us-east-1
"#,
                state = root.join("state.db").display(),
                work = root.join("work").display(),
                input = input.display(),
            ),
        )
        .unwrap();
        Self {
            _dir: dir,
            root,
            config,
        }
    }

    fn process_once(&self, failpoint: Option<&str>) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_vtopctl"));
        cmd.args(["process-once", "--source", "file", "--config"])
            .arg(&self.config)
            .env_remove(failpoints::ENV)
            .env_remove("RUST_LOG");
        if let Some(point) = failpoint {
            cmd.env(failpoints::ENV, point);
        }
        cmd.output().unwrap()
    }

    /// The ledger's rows as `(state, batch_id)`, read from a read-only
    /// connection: a read-write one would checkpoint the WAL the kill left
    /// when it closes, and the restart would not see what the crash left.
    fn ledger(&self) -> Vec<String> {
        let db = self.root.join("state.db");
        if !db.exists() {
            return Vec::new();
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            use sqlx::sqlite::SqliteConnectOptions;
            use sqlx::Connection;
            let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", db.display()))
                .unwrap()
                .read_only(true);
            let mut conn = sqlx::SqliteConnection::connect_with(&opts).await.unwrap();
            let rows: Vec<(String,)> =
                sqlx::query_as("SELECT state FROM batches ORDER BY created_at, batch_id")
                    .fetch_all(&mut conn)
                    .await
                    .unwrap();
            // The ledger stores states in lower case.
            rows.into_iter().map(|(s,)| s.to_uppercase()).collect()
        })
    }

    /// Every input record id found in the store's objects, counted. Read from
    /// the files, not from the engine's report of what it wrote.
    fn archived_records(&self) -> std::collections::BTreeMap<String, usize> {
        let mut found = std::collections::BTreeMap::new();
        let mut stack = vec![self.root.join("store")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                if name.contains("manifest") || name.contains("receipt") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                for line in text.lines() {
                    if let Some(id) = line.split_whitespace().next() {
                        if id.starts_with("fp-record-") {
                            *found.entry(id.to_string()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
        found
    }
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The ledger state each point leaves its one batch in when it fires, or
/// `None` where the row's first write has not happened.
fn state_at_kill(point: &str) -> Option<&'static str> {
    Some(match point {
        failpoints::BEFORE_INSERT => return None,
        failpoints::AFTER_INSERT | failpoints::BEFORE_LEDGER_SEALED => "BATCHING",
        failpoints::AFTER_LEDGER_SEALED | failpoints::BEFORE_LEDGER_COMPRESSED => "SEALED",
        failpoints::AFTER_LEDGER_COMPRESSED | failpoints::BEFORE_LEDGER_CHECKSUMMED => "COMPRESSED",
        failpoints::AFTER_LEDGER_CHECKSUMMED
        | failpoints::MID_OBJECT_PUT
        | failpoints::AFTER_OBJECT_PUT_BEFORE_LEDGER
        | failpoints::BEFORE_LEDGER_OBJECT_UPLOADED => "CHECKSUMMED",
        failpoints::AFTER_LEDGER_OBJECT_UPLOADED
        | failpoints::AFTER_MANIFEST_PUT_BEFORE_LEDGER
        | failpoints::BEFORE_LEDGER_MANIFEST_UPLOADED => "OBJECT_UPLOADED",
        failpoints::AFTER_LEDGER_MANIFEST_UPLOADED | failpoints::BEFORE_LEDGER_VERIFIED => {
            "MANIFEST_UPLOADED"
        }
        failpoints::AFTER_LEDGER_VERIFIED
        | failpoints::BEFORE_SOURCE_COMMIT
        | failpoints::AFTER_SOURCE_COMMIT_BEFORE_LEDGER
        | failpoints::BEFORE_LEDGER_SOURCE_COMMITTED => "VERIFIED",
        failpoints::AFTER_LEDGER_SOURCE_COMMITTED => "SOURCE_COMMITTED",
        other => panic!("{other} has no expected ledger state: add it to this table"),
    })
}

/// F2: a misspelt point must not start an engine that is never killed.
#[test]
fn an_unknown_point_refuses_to_start_and_lists_every_point() {
    let lab = Lab::new(false);
    let out = lab.process_once(Some("before_sorce_commit"));
    assert_eq!(
        out.status.signal(),
        None,
        "an unknown point killed the process: {}",
        stderr(&out)
    );
    assert!(!out.status.success(), "an unknown point started the engine");
    let err = stderr(&out);
    // The first indented block: the CLI may print the error more than once.
    let listed: Vec<&str> = err
        .lines()
        .skip_while(|l| !l.starts_with("  "))
        .map_while(|l| l.strip_prefix("  "))
        .map(str::trim_end)
        .collect();
    assert_eq!(listed, POINTS, "the refusal must list every point:\n{err}");
    assert!(
        lab.ledger().is_empty(),
        "the refused engine wrote to the ledger"
    );
}

/// F4 and the cheap half of F5, for every point: SIGKILL at the point, the
/// ledger where the point says it is, and a restart that archives every
/// record.
#[test]
fn every_point_kills_the_engine_where_it_says_and_a_restart_archives_everything() {
    for point in POINTS {
        let multipart = *point == failpoints::MID_OBJECT_PUT;
        let lab = Lab::new(multipart);
        let out = lab.process_once(Some(point));
        assert_eq!(
            out.status.signal(),
            Some(9),
            "{point}: expected death by SIGKILL, got {:?}; stderr:\n{}",
            out.status,
            stderr(&out)
        );
        assert!(
            stderr(&out).contains(&format!("failpoint {point} fired")),
            "{point}: the kill was not announced; stderr:\n{}",
            stderr(&out)
        );
        let at_kill = lab.ledger();
        match state_at_kill(point) {
            None => assert!(at_kill.is_empty(), "{point}: rows at the kill: {at_kill:?}"),
            Some(state) => assert_eq!(
                at_kill,
                vec![state.to_string()],
                "{point}: the ledger at the kill"
            ),
        }

        // The mock store dies with the process, so there is nothing to read
        // back for the multipart point; the live sweep covers its restart.
        if multipart {
            continue;
        }
        let mut passes = 0;
        loop {
            passes += 1;
            let out = lab.process_once(None);
            assert!(
                out.status.success(),
                "{point}: restart pass {passes} failed: {:?}\n{}",
                out.status,
                stderr(&out)
            );
            let rows = lab.ledger();
            let open: Vec<&String> = rows
                .iter()
                .filter(|s| !matches!(s.as_str(), "SOURCE_COMMITTED" | "SUPERSEDED"))
                .collect();
            let found = lab.archived_records();
            if open.is_empty() && found.len() == RECORDS {
                break;
            }
            assert!(
                passes < MAX_RESTART_PASSES,
                "{point}: after {passes} restart passes the ledger is {rows:?} and the store \
                 holds {} of {RECORDS} records",
                found.len()
            );
        }
        let found = lab.archived_records();
        for i in 0..RECORDS {
            let id = format!("fp-record-{i:04}");
            assert!(
                found.contains_key(&id),
                "{point}: {id} is missing from the store after the restart (a hole)"
            );
        }
    }
}
