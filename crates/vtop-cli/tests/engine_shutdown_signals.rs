//! `vtopctl run` stops in order when a supervisor asks it to (#526).
//!
//! The unit tests in `engine.rs` pin the loop's handling of a stop request
//! through an injected source. This file pins the part they cannot reach: the
//! real process, with real signals, busy. It is the shape `docker stop`, a
//! Kubernetes pod termination and a benchmark harness all use: the engine is
//! archiving a file that keeps growing, a signal arrives while cycles are in
//! flight, and the process must run its shutdown flush and exit 0 within a
//! bound — not ignore the signal until SIGKILL, which is what a loaded engine
//! did before #526 for SIGINT, and always did for SIGTERM.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long a loaded engine may take to finish its cycle, flush and exit.
/// Generous for a debug build on a shared CI runner; the defect it guards
/// against never exits at all.
const STOP_BOUND: Duration = Duration::from_secs(15);

/// How long the engine may take to show it is under load before the signal.
const LOAD_BOUND: Duration = Duration::from_secs(60);

fn write_config(dir: &Path, input: &Path) -> PathBuf {
    let config = dir.join("config.yaml");
    // Small batches so threshold flushes keep every cycle productive: the
    // zero-backoff loop is where the lost signal hid.
    let yaml = format!(
        r#"engine:
  name: vtop-shutdown-test
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
  prefix: "shutdown-test"
  region: us-east-1
"#,
        state = dir.join("state.db").display(),
        work = dir.join("work").display(),
        input = input.display(),
    );
    std::fs::write(&config, yaml).unwrap();
    config
}

/// Appends lines to the input for as long as it lives, so the engine never
/// runs out of work while the test signals it.
struct Seeder {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Seeder {
    fn start(input: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&input)
                .unwrap();
            let mut n: u64 = 0;
            while !flag.load(Ordering::Relaxed) {
                for _ in 0..500 {
                    writeln!(file, "event {n} lorem ipsum dolor sit amet").unwrap();
                    n += 1;
                }
                file.flush().unwrap();
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Seeder {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Kills the engine if a failed assertion unwinds past it, so a regression
/// fails the test instead of leaking a process that runs forever.
struct Engine(Child);

impl Drop for Engine {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn log_text(log: &Path) -> String {
    std::fs::read_to_string(log).unwrap_or_default()
}

/// The end of the engine's log, for failure messages: a loaded engine that
/// ignored its signal logs thousands of batches, and the part that explains
/// the failure is the last few lines.
fn log_tail(log: &Path) -> String {
    let text = log_text(log);
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(40)..].join("\n")
}

fn signal_a_busy_engine(signal: &str) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.log");
    std::fs::write(&input, "").unwrap();
    let config = write_config(dir.path(), &input);
    let log = dir.path().join("engine.log");
    let _seeder = Seeder::start(input);

    let log_file = std::fs::File::create(&log).unwrap();
    let mut engine = Engine(
        Command::new(env!("CARGO_BIN_EXE_vtopctl"))
            .arg("--log-level")
            .arg("info")
            .arg("run")
            .arg("--config")
            .arg(&config)
            // The assertions read the plain-text log format.
            .env_remove("VTOP_LOG_FORMAT")
            .stdin(Stdio::null())
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .expect("spawn vtopctl run"),
    );

    // Deadline-poll for real load rather than sleeping: several committed
    // batches mean cycles are running back to back when the signal lands.
    let started = Instant::now();
    loop {
        if log_text(&log).matches("source_committed").count() >= 3 {
            break;
        }
        if let Some(status) = engine.0.try_wait().unwrap() {
            panic!(
                "vtopctl run exited ({status}) before taking load:\n{}",
                log_tail(&log)
            );
        }
        assert!(
            started.elapsed() < LOAD_BOUND,
            "the engine committed fewer than three batches in {LOAD_BOUND:?}:\n{}",
            log_tail(&log)
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let sent = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(engine.0.id().to_string())
        .status()
        .expect("run kill");
    assert!(sent.success(), "kill -{signal} failed");
    let signalled = Instant::now();

    let status = loop {
        if let Some(status) = engine.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            signalled.elapsed() < STOP_BOUND,
            "vtopctl run was still running {STOP_BOUND:?} after SIG{signal} under \
             load: the signal was lost, and a supervisor would have to SIGKILL it \
             and skip the shutdown flush (#526):\n{}",
            log_tail(&log)
        );
        std::thread::sleep(Duration::from_millis(20));
    };

    let text = log_text(&log);
    let tail = log_tail(&log);
    assert_eq!(
        status.code(),
        Some(0),
        "an orderly SIG{signal} stop must exit 0, or a supervisor records a \
         failure for every clean shutdown:\n{tail}"
    );
    assert!(
        text.lines()
            .any(|line| line.contains("shutdown signal received")
                && line.contains(&format!("signal=\"SIG{signal}\""))),
        "the log must say which signal started the shutdown:\n{tail}"
    );
    assert!(
        text.contains("shutdown flush complete"),
        "exit 0 must mean the shutdown flush ran to completion, not that the \
         process merely ended:\n{tail}"
    );
}

#[test]
fn sigterm_under_load_flushes_and_exits_zero() {
    signal_a_busy_engine("TERM");
}

#[test]
fn sigint_under_load_flushes_and_exits_zero() {
    signal_a_busy_engine("INT");
}
