//! `upload.require_strong_verification: false` is said out loud (#629).
//!
//! The opt-out lets a batch commit on a size/existence check, so a corrupt or
//! replaced object of the recorded size is archived as verified. Before #629
//! the only sign of it was a WARN per batch: nothing at startup, nothing from
//! a config check. These tests drive the real binary, because what they pin is
//! what an operator reads: the config check's output and the engine's log.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

/// Words every warning must carry: the consequence and the key to change.
const CONSEQUENCE: &str = "accepts same-size corruption";
const KEY: &str = "upload.require_strong_verification";

fn write_config(dir: &Path, require_strong: Option<bool>) -> PathBuf {
    let config = dir.join("config.yaml");
    let input = dir.join("in.log");
    std::fs::write(&input, "").unwrap();
    let strong = match require_strong {
        Some(v) => format!("  require_strong_verification: {v}\n"),
        None => String::new(),
    };
    let yaml = format!(
        r#"engine:
  name: vtop-size-only-test
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
  prefix: "size-only-test"
  region: us-east-1
{strong}"#,
        state = dir.join("state.db").display(),
        work = dir.join("work").display(),
        input = input.display(),
    );
    std::fs::write(&config, yaml).unwrap();
    config
}

fn vtopctl(args: &[&str], config: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(args)
        .arg("--config")
        .arg(config)
        // The assertions read the plain-text log format, at the level the
        // test passes: RUST_LOG overrides --log-level (vtop_observe::logging),
        // so an inherited RUST_LOG=error would hide the WARN under test.
        .env_remove("VTOP_LOG_FORMAT")
        .env_remove("RUST_LOG")
        .stdin(Stdio::null())
        .output()
        .expect("spawn vtopctl")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `vtopctl validate` names the risk and the key, and still passes: the
/// opt-out is a legitimate (lab, digest-less backend) setting, not an error.
#[test]
fn validate_reports_size_only_verification_as_a_warning_and_passes() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path(), Some(false));

    let out = vtopctl(&["validate"], &config);
    let stdout = text(&out.stdout);
    assert!(
        out.status.success(),
        "validate must not fail on the opt-out; exit {:?}\nstdout:\n{stdout}\nstderr:\n{}",
        out.status.code(),
        text(&out.stderr)
    );
    assert!(
        stdout.contains(CONSEQUENCE) && stdout.contains(KEY),
        "validate must warn that the engine {CONSEQUENCE:?} and name {KEY:?}; stdout:\n{stdout}"
    );

    let out = vtopctl(&["--json", "validate"], &config);
    assert!(out.status.success(), "{}", text(&out.stderr));
    let report: serde_json::Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("--json validate is not JSON ({e}): {}", text(&out.stdout)));
    let warnings = report["warnings"]
        .as_array()
        .unwrap_or_else(|| panic!("no warnings array in {report}"));
    assert_eq!(report["status"], "ok", "{report}");
    assert_eq!(
        warnings
            .iter()
            .filter(|w| w.as_str().is_some_and(|w| w.contains(CONSEQUENCE)))
            .count(),
        1,
        "exactly one size-only warning expected in {report}"
    );
}

/// The default (strong verification) is not warned about.
#[test]
fn validate_is_quiet_under_the_strong_default() {
    let dir = tempfile::tempdir().unwrap();
    for setting in [None, Some(true)] {
        let config = write_config(dir.path(), setting);
        let out = vtopctl(&["validate"], &config);
        let stdout = text(&out.stdout);
        assert!(
            out.status.success(),
            "require_strong_verification={setting:?}: exit {:?}\n{stdout}{}",
            out.status.code(),
            text(&out.stderr)
        );
        assert!(
            !stdout.contains(CONSEQUENCE),
            "require_strong_verification={setting:?} must not warn; stdout:\n{stdout}"
        );
    }
}

/// An engine started for mutating work (`run`, `process-once`, `replay` all
/// start the same way) logs the risk once at startup, before any batch.
#[test]
fn startup_warns_once_when_size_only_verification_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path(), Some(false));

    // An empty ledger: recovery has nothing to do, so no per-batch line can
    // stand in for the startup one.
    let out = vtopctl(&["--log-level", "warn", "replay"], &config);
    let stderr = text(&out.stderr);
    assert!(
        out.status.success(),
        "replay exit {:?}\nstderr:\n{stderr}",
        out.status.code()
    );
    let lines: Vec<&str> = stderr.lines().filter(|l| l.contains(CONSEQUENCE)).collect();
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one startup line saying the engine {CONSEQUENCE:?}; stderr:\n{stderr}"
    );
    assert!(
        lines[0].contains("WARN") && lines[0].contains(KEY),
        "the startup line must be a WARN naming {KEY:?}: {}",
        lines[0]
    );

    let strong = tempfile::tempdir().unwrap();
    let config = write_config(strong.path(), None);
    let out = vtopctl(&["--log-level", "warn", "replay"], &config);
    let stderr = text(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(
        !stderr.contains(CONSEQUENCE),
        "the strong default must not warn at startup; stderr:\n{stderr}"
    );
}

/// The value of `vtop_verification_mode{tenant,mode}` in an encoded scrape,
/// whatever order the encoder puts the labels in.
fn mode_gauge(scrape: &str, tenant: &str, mode: &str) -> Option<i64> {
    scrape
        .lines()
        .filter(|l| l.starts_with("vtop_verification_mode{"))
        .filter(|l| l.contains(&format!("tenant=\"{tenant}\"")))
        .find(|l| l.contains(&format!("mode=\"{mode}\"")))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

/// Startup also sets a gauge, so the mode can be alerted on before (or
/// without) any traffic: the per-batch counter only moves when a batch does.
///
/// The gauge is set wherever a metrics registry exists. Only `vtopctl run`
/// builds one, and only when `VTOP_METRICS_ADDR` is set (`metrics_server`):
/// the one-shot `process-once` and `replay` serve no endpoint, so they log
/// the startup WARN and export nothing. This pins the library half;
/// `run_serves_the_verification_mode_gauge` pins what `run` exposes.
#[tokio::test]
async fn startup_sets_the_verification_mode_gauge() {
    let m = vtop_core::telemetry::init().expect("metrics registry");
    for (tenant, require_strong, size_only, strong) in [
        ("tenant-629-gauge-size-only", false, 1, 0),
        ("tenant-629-gauge-strong", true, 0, 1),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "").unwrap();
        let store = format!("sqlite://{}", dir.path().join("state.db").display());
        let mut cfg = vtop_cli::testkit::file_config(
            dir.path().join("work").to_str().unwrap(),
            &store,
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        // A tenant no other test uses: the registry is process-wide.
        cfg.engine.tenant = tenant.into();
        cfg.upload.require_strong_verification = require_strong;
        let _engine = vtop_cli::Engine::new_exclusive(
            cfg,
            vtop_core::config::StreamsConfig { streams: vec![] },
        )
        .await
        .unwrap();

        let scrape = m.encode().unwrap();
        assert_eq!(
            (
                mode_gauge(&scrape, tenant, "size_only"),
                mode_gauge(&scrape, tenant, "strong")
            ),
            (Some(size_only), Some(strong)),
            "require_strong_verification={require_strong}: expected \
             vtop_verification_mode{{tenant={tenant:?}}} size_only={size_only}, \
             strong={strong}; scrape lines:\n{}",
            scrape
                .lines()
                .filter(|l| l.contains("verification_mode"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

/// Kills `vtopctl run` if an assertion unwinds past it.
struct Running(std::process::Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A loopback port nothing is listening on right now, avoiding the ports
/// the compose lab publishes.
fn free_port() -> u16 {
    loop {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if ![8083, 9000, 9092, 9100].contains(&port) {
            return port;
        }
    }
}

/// One plain-HTTP GET of `/metrics`; `None` while nothing answers yet.
fn scrape(port: u16) -> Option<String> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// The one command that serves metrics exposes the gauge from startup, on
/// an idle engine: no batch has to move for the alert to fire.
#[test]
fn run_serves_the_verification_mode_gauge() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path(), Some(false));
    let port = free_port();
    let log = dir.path().join("engine.log");
    let log_file = std::fs::File::create(&log).unwrap();
    let _engine = Running(
        Command::new(env!("CARGO_BIN_EXE_vtopctl"))
            .args(["--log-level", "warn", "run", "--config"])
            .arg(&config)
            .env("VTOP_METRICS_ADDR", format!("127.0.0.1:{port}"))
            .env_remove("VTOP_METRICS_TLS_CERT")
            .env_remove("VTOP_METRICS_TLS_KEY")
            .env_remove("VTOP_METRICS_TLS_CLIENT_CA")
            .env_remove("VTOP_LOG_FORMAT")
            .env_remove("RUST_LOG")
            .stdin(Stdio::null())
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .expect("spawn vtopctl run"),
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let body = loop {
        if let Some(body) = scrape(port).filter(|b| b.contains("vtop_verification_mode{")) {
            break body;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "vtopctl run never served vtop_verification_mode on 127.0.0.1:{port}; log:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    };
    assert_eq!(
        (
            mode_gauge(&body, "default", "size_only"),
            mode_gauge(&body, "default", "strong")
        ),
        (Some(1), Some(0)),
        "require_strong_verification=false must read size_only=1, strong=0; scrape:\n{}",
        body.lines()
            .filter(|l| l.contains("verification_mode"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
