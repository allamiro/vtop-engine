//! Key, state-store, ledger and audit files other local users can reach
//! (#733), through the real binary: what an operator sees is the exit code,
//! stderr, and the audit file.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const KEY_HEX: &str = "4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d";

/// A directory other users can enter (0755). The test's own temporary
/// directory is 0755 too under the usual umask 022, so a test that needs a
/// closed directory makes one explicitly.
fn open_dir(root: &Path) -> PathBuf {
    let dir = root.join("open");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

fn write_mode(path: &Path, contents: &str, mode: u32) {
    std::fs::write(path, contents).unwrap();
    chmod(path, mode);
}

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// A config with a one-key keyring read from `key`, the ledger at `db`, and
/// the override when `loose`.
fn write_config(dir: &Path, key: &Path, db: &Path, loose: bool) -> PathBuf {
    let config = dir.join("config.yaml");
    let input = dir.join("in.log");
    std::fs::write(&input, "one\ntwo\n").unwrap();
    let mut yaml = format!(
        r#"engine:
  name: vtop-permission-test
  tenant: default
  state_store: "sqlite://{db}"
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
manifest:
  write_version: "0.4"
  mac:
    active: q4-2026
    keys:
      - {{id: q4-2026, file: {key}}}
sources:
  file:
    enabled: true
    paths:
      - "{input}"
upload:
  backend: mock
  bucket: "telemetry-data"
  prefix: "permission-test"
  region: us-east-1
"#,
        db = db.display(),
        work = dir.join("work").display(),
        key = key.display(),
        input = input.display(),
    );
    if loose {
        yaml.push_str("security:\n  allow_loose_secret_permissions: true\n");
    }
    std::fs::write(&config, yaml).unwrap();
    config
}

fn vtopctl(args: &[&str], config: &Path, audit_log: Option<&Path>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_vtopctl"));
    command
        .args(args)
        .arg("--config")
        .arg(config)
        .env("VTOP_LOG_FORMAT", "json")
        .env_remove("RUST_LOG")
        .env_remove("VTOP_AUDIT_LOG")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .stdin(Stdio::null());
    if let Some(path) = audit_log {
        command.env("VTOP_AUDIT_LOG", path);
    }
    command.output().expect("spawn vtopctl")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A world-readable or group/world-writable key file stops `process-once`
/// (the engine's startup path) with the key, the path, the mode, what was
/// done and the fix, and without reading the key.
#[test]
fn startup_refuses_a_key_file_other_users_can_reach() {
    for (mode, fix) in [
        (0o644, "chmod 0400"),
        (0o640, "chmod 0400"),
        (0o666, "chmod go-w"),
        (0o620, "chmod go-w"),
        (0o602, "chmod go-w"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let key = open_dir(dir.path()).join("q4");
        write_mode(&key, KEY_HEX, mode);
        let config = write_config(dir.path(), &key, &dir.path().join("state.db"), false);
        let out = vtopctl(&["process-once", "--source", "file"], &config, None);
        let stderr = text(&out.stderr);
        assert!(!out.status.success(), "{mode:o} started:\n{stderr}");
        for needle in [
            "manifest key \"q4-2026\"".to_owned(),
            key.display().to_string(),
            format!("{mode:04o}"),
            "the file was not read and nothing was started".to_owned(),
            fix.to_owned(),
        ] {
            assert!(
                stderr.contains(&needle),
                "{mode:o}: {needle} missing:\n{stderr}"
            );
        }
        assert!(!stderr.contains(KEY_HEX), "the key leaked:\n{stderr}");
        // Nothing was started: no ledger was created.
        assert!(!dir.path().join("state.db").exists(), "{mode:o}");
    }
}

/// Owner-only key files start, and so does a SQLite ledger at 0644 inside
/// the test's own 0700 directory: nobody else can reach it.
#[test]
fn owner_only_files_and_a_ledger_in_a_closed_directory_start() {
    for mode in [0o400, 0o600] {
        let dir = tempfile::tempdir().unwrap();
        chmod(dir.path(), 0o700);
        let key = open_dir(dir.path()).join("q4");
        write_mode(&key, KEY_HEX, mode);
        let db = dir.path().join("state.db");
        write_mode(&db, "", 0o644);
        let config = write_config(dir.path(), &key, &db, false);
        let out = vtopctl(&["process-once", "--source", "file"], &config, None);
        assert!(
            out.status.success(),
            "{mode:o}: {}\n{}",
            text(&out.stdout),
            text(&out.stderr)
        );
    }
}

/// A 0644 ledger, or its `-wal`, in a directory others can enter is refused
/// at startup and by `validate`, naming the file.
#[test]
fn a_ledger_others_can_read_is_refused_at_startup_and_by_validate() {
    for suffix in ["", "-wal"] {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("q4");
        write_mode(&key, KEY_HEX, 0o400);
        let db = open_dir(dir.path()).join("state.db");
        write_mode(&db, "", 0o600);
        let offender = PathBuf::from(format!("{}{suffix}", db.display()));
        write_mode(&offender, "", 0o644);
        let config = write_config(dir.path(), &key, &db, false);
        for args in [&["process-once", "--source", "file"][..], &["validate"][..]] {
            let out = vtopctl(args, &config, None);
            let stderr = text(&out.stderr);
            assert!(!out.status.success(), "{args:?} {suffix}:\n{stderr}");
            assert!(
                stderr.contains(&offender.display().to_string())
                    && stderr.contains("0644")
                    && stderr.contains("chmod 0600"),
                "{args:?} {suffix}:\n{stderr}"
            );
        }
    }
}

/// `validate` names every offending file in one run and exits non-zero: a
/// key file, the ledger and the audit file.
#[test]
fn validate_reports_every_offending_file_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let open = open_dir(dir.path());
    let key = open.join("q4");
    write_mode(&key, KEY_HEX, 0o644);
    let db = open.join("state.db");
    write_mode(&db, "", 0o640);
    let audit = open.join("audit.jsonl");
    write_mode(&audit, "", 0o666);
    let config = write_config(dir.path(), &key, &db, false);
    let out = vtopctl(&["validate"], &config, Some(&audit));
    let stderr = text(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains("3 files are refused"), "{stderr}");
    for path in [&key, &db, &audit] {
        assert!(
            stderr.contains(&path.display().to_string()),
            "{} missing:\n{stderr}",
            path.display()
        );
    }
    assert!(!stderr.contains(KEY_HEX), "{stderr}");
    // With the files fixed, the same config validates.
    chmod(&key, 0o400);
    chmod(&db, 0o600);
    chmod(&audit, 0o600);
    let out = vtopctl(&["validate"], &config, Some(&audit));
    assert!(out.status.success(), "{}", text(&out.stderr));
}

/// The state-store secret file follows the same rule, in `validate`.
#[test]
fn validate_refuses_a_state_store_secret_file_others_can_read() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("q4");
    write_mode(&key, KEY_HEX, 0o400);
    let secret = open_dir(dir.path()).join("store");
    write_mode(&secret, "postgres://vtop:hunter2@db/vtop", 0o644);
    let config = write_config(dir.path(), &key, &dir.path().join("state.db"), false);
    let yaml = std::fs::read_to_string(&config).unwrap().replace(
        &format!("\"sqlite://{}\"", dir.path().join("state.db").display()),
        &format!("{{file: {}}}", secret.display()),
    );
    std::fs::write(&config, yaml).unwrap();
    let out = vtopctl(&["validate"], &config, None);
    let stderr = text(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(
        stderr.contains("the state-store secret file") && stderr.contains("0644"),
        "{stderr}"
    );
    assert!(!stderr.contains("hunter2"), "{stderr}");
}

/// The audit file: group-writable or world-readable is refused, group
/// readable is accepted (a log shipper's way in).
#[test]
fn the_audit_file_may_be_group_readable_only() {
    for (mode, accepted) in [(0o666, false), (0o644, false), (0o640, true), (0o600, true)] {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("q4");
        write_mode(&key, KEY_HEX, 0o400);
        let audit = open_dir(dir.path()).join("audit.jsonl");
        write_mode(&audit, "", mode);
        let config = write_config(dir.path(), &key, &dir.path().join("state.db"), false);
        for args in [&["validate"][..], &["migrate"][..]] {
            let out = vtopctl(args, &config, Some(&audit));
            let stderr = text(&out.stderr);
            assert_eq!(
                out.status.success(),
                accepted,
                "{args:?} {mode:o}:\n{stderr}"
            );
            if !accepted {
                assert!(
                    stderr.contains("the audit file (VTOP_AUDIT_LOG)")
                        && stderr.contains(&format!("{mode:04o}")),
                    "{args:?} {mode:o}:\n{stderr}"
                );
            }
        }
    }
}

/// With the override, a world-readable key file starts: a WARN names the
/// file and its mode, an audit record of kind `security.override` is
/// written, and neither carries a key byte. A writable one is still refused.
#[test]
fn the_override_starts_announced_and_audited_but_never_for_a_writable_file() {
    let dir = tempfile::tempdir().unwrap();
    let key = open_dir(dir.path()).join("q4");
    write_mode(&key, KEY_HEX, 0o644);
    let audit = dir.path().join("audit.jsonl");
    let config = write_config(dir.path(), &key, &dir.path().join("state.db"), true);

    let out = vtopctl(&["migrate"], &config, Some(&audit));
    let stderr = text(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    let warned: Vec<_> = stderr
        .lines()
        .filter(|l| l.contains("Accepted because security.allow_loose_secret_permissions"))
        .collect();
    assert_eq!(warned.len(), 1, "one WARN per file:\n{stderr}");
    assert!(
        warned[0].contains("WARN")
            && warned[0].contains(&key.display().to_string())
            && warned[0].contains("0644"),
        "{}",
        warned[0]
    );
    let records: Vec<serde_json::Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let overrides: Vec<_> = records
        .iter()
        .filter(|r| r["action"] == "security.override")
        .collect();
    assert_eq!(overrides.len(), 1, "{records:#?}");
    let record = overrides[0];
    assert_eq!(record["outcome"], "success");
    assert_eq!(record["target"]["kind"], "file");
    assert_eq!(record["target"]["id"], key.display().to_string());
    assert_eq!(record["params"]["file_kind"], "manifest_key");
    assert_eq!(record["params"]["mode"], "0644");
    assert_eq!(record["params"]["exposed_to"], "group,other");
    assert_eq!(record["engine"], "vtop-permission-test");
    let everything = format!("{stderr}{}", std::fs::read_to_string(&audit).unwrap());
    assert!(!everything.contains(KEY_HEX), "the key leaked");

    // validate says the same, as a warning, and records nothing.
    let before = std::fs::read_to_string(&audit).unwrap();
    let out = vtopctl(&["validate"], &config, Some(&audit));
    let stdout = text(&out.stdout);
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        stdout.contains("WARNING:") && stdout.contains("The engine will accept it"),
        "{stdout}"
    );
    assert_eq!(std::fs::read_to_string(&audit).unwrap(), before);

    for mode in [0o646, 0o664] {
        chmod(&key, mode);
        for args in [&["validate"][..], &["migrate"][..]] {
            let out = vtopctl(args, &config, Some(&audit));
            let stderr = text(&out.stderr);
            assert!(!out.status.success(), "{args:?} {mode:o}:\n{stderr}");
            assert!(stderr.contains("no setting accepts it"), "{stderr}");
        }
    }
}

/// A new ledger and a new audit file are created owner-only, whatever the
/// umask allows.
#[test]
fn a_new_ledger_and_a_new_audit_file_are_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("q4");
    write_mode(&key, KEY_HEX, 0o400);
    let open = open_dir(dir.path());
    let db = open.join("state.db");
    let audit = open.join("audit.jsonl");
    let config = write_config(dir.path(), &key, &db, false);
    let out = vtopctl(&["process-once", "--source", "file"], &config, None);
    assert!(out.status.success(), "{}", text(&out.stderr));
    let out = vtopctl(&["migrate"], &config, Some(&audit));
    assert!(out.status.success(), "{}", text(&out.stderr));
    for path in [db.clone(), audit] {
        assert_eq!(mode_of(&path), 0o600, "{}", path.display());
    }
    for suffix in ["-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{suffix}", db.display()));
        if path.exists() {
            assert_eq!(mode_of(&path), 0o600, "{}", path.display());
        }
    }
    // And the engine starts on what it created, in a directory others can
    // enter.
    let out = vtopctl(&["process-once", "--source", "file"], &config, None);
    assert!(out.status.success(), "{}", text(&out.stderr));
}

/// `vtopctl audit list` opens the ledger like any other command: under the
/// override, a ledger other users can read is announced on its stderr and
/// leaves an audit record there, rather than being accepted silently.
#[test]
fn audit_list_announces_an_override_like_any_other_command() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("q4");
    write_mode(&key, KEY_HEX, 0o400);
    let db = open_dir(dir.path()).join("state.db");
    let config = write_config(dir.path(), &key, &db, true);
    let out = vtopctl(&["migrate"], &config, None);
    assert!(out.status.success(), "{}", text(&out.stderr));
    chmod(&db, 0o644);
    let audit = dir.path().join("audit.jsonl");
    let out = vtopctl(&["audit", "list"], &config, Some(&audit));
    let stderr = text(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    let announced: Vec<_> = stderr
        .lines()
        .filter(|l| l.contains("Accepted because security.allow_loose_secret_permissions"))
        .collect();
    assert_eq!(announced.len(), 1, "{stderr}");
    assert!(announced[0].contains(&db.display().to_string()), "{stderr}");
    assert!(
        stderr.contains("security.override"),
        "no audit record on the log stream:\n{stderr}"
    );
    let recorded = std::fs::read_to_string(&audit).unwrap_or_default();
    assert!(
        recorded.contains("\"action\":\"security.override\"")
            && recorded.contains(&db.display().to_string()),
        "no record in VTOP_AUDIT_LOG: {recorded}"
    );
}

/// Review: every command that reads the keys or the ledger opens the audit
/// file first, so a file accepted under the override is recorded there, not
/// only on the log stream. `process-once` is the engine's own start-up path.
#[test]
fn process_once_records_an_override_in_the_audit_file() {
    let dir = tempfile::tempdir().unwrap();
    let key = open_dir(dir.path()).join("q4");
    write_mode(&key, KEY_HEX, 0o644);
    let audit = dir.path().join("audit.jsonl");
    let config = write_config(dir.path(), &key, &dir.path().join("state.db"), true);
    let out = vtopctl(&["process-once", "--source", "file"], &config, Some(&audit));
    assert!(out.status.success(), "{}", text(&out.stderr));
    let recorded = std::fs::read_to_string(&audit).unwrap_or_default();
    let overrides: Vec<_> = recorded
        .lines()
        .filter(|l| l.contains("\"action\":\"security.override\""))
        .collect();
    assert_eq!(overrides.len(), 1, "{recorded}");
    assert!(
        overrides[0].contains(&key.display().to_string()),
        "{recorded}"
    );
}

/// Review: in `vtopctl run`, an override on the audit file itself is counted
/// on `/metrics`: the registry exists before the audit file is opened.
#[test]
fn run_counts_an_override_on_the_audit_file() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("q4");
    write_mode(&key, KEY_HEX, 0o400);
    let audit = open_dir(dir.path()).join("audit.jsonl");
    write_mode(&audit, "", 0o644);
    let config = write_config(dir.path(), &key, &dir.path().join("state.db"), true);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr = format!("127.0.0.1:{port}");
    let mut child = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(["run", "--config"])
        .arg(&config)
        .env("VTOP_METRICS_ADDR", &addr)
        .env("VTOP_AUDIT_LOG", &audit)
        .env_remove("RUST_LOG")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vtopctl run");
    let name = "vtop_secret_file_permission_overrides_total";
    let mut last = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
            let _ = stream.write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n");
            let mut body = String::new();
            let _ = stream.read_to_string(&mut body);
            if body.lines().any(|l| l == format!("{name} 1")) {
                last = body;
                break;
            }
            last = body;
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        last.lines().any(|l| l == format!("{name} 1")),
        "{name} is not 1:\n{}",
        last.lines()
            .filter(|l| l.starts_with(name) || l.starts_with("HTTP"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
