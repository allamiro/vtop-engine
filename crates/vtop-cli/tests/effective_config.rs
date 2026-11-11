use std::path::Path;
use std::process::{Command, Output};

fn config(dir: &Path) {
    std::fs::write(dir.join("config.yaml"), "engine:\n  name: effective-test\n  tenant: global-tenant\n  state_store: {env: SECRET_DATABASE_ENV}\n  work_dir: /unused\nbatching: {}\ncompression: {}\nsources: {}\nupload:\n  backend: localfs\n  bucket: archive\n  local_path: /unused\n").unwrap();
}
fn run(dir: &Path, json: bool, effective: bool) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_vtopctl"));
    if json {
        c.arg("--json");
    }
    c.args(["validate", "--config"])
        .arg(dir.join("config.yaml"));
    if effective {
        c.arg("--print-effective");
    }
    c.env_remove("RUST_LOG")
        .env_remove("VTOP_LOG_FORMAT")
        .output()
        .unwrap()
}
#[test]
fn effective_two_streams_text_and_json_golden() {
    let dir = tempfile::tempdir().unwrap();
    config(dir.path());
    std::fs::write(dir.path().join("streams.yaml"), "streams:\n- source_name: one\n  source_type: file\n  format: jsonl\n  tenant: override\n- source_name: two\n  source_type: kafka\n  format: raw\n").unwrap();
    for (json, golden) in [
        (false, include_str!("fixtures/effective.txt")),
        (true, include_str!("fixtures/effective.json")),
    ] {
        let out = run(dir.path(), json, true);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8(out.stdout).unwrap(), golden);
    }
}
#[test]
fn validate_without_flag_remains_byte_identical_and_default_is_always_present() {
    let dir = tempfile::tempdir().unwrap();
    config(dir.path());
    let out = run(dir.path(), false, false);
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        format!(
            "{}: config is valid (0 warning(s))\n",
            dir.path().join("config.yaml").display()
        )
    );
    assert_eq!(
        run(dir.path(), true, false).stdout,
        b"{\"status\":\"ok\",\"warnings\":[]}\n"
    );
    let out = run(dir.path(), true, true);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(value["effective"]["default"].is_object());
    assert_eq!(value["effective"]["streams"], serde_json::json!([]));
}
#[test]
fn effective_output_never_resolves_or_prints_secret_references() {
    let dir = tempfile::tempdir().unwrap();
    config(dir.path());
    let path = dir.path().join("config.yaml");
    let base = std::fs::read_to_string(&path).unwrap();
    for state in [
        "{env: SECRET_DATABASE_ENV}",
        "{file: /run/secrets/secret-database-file}",
    ] {
        let yaml = base.replace("{env: SECRET_DATABASE_ENV}", state).replace("sources: {}", "manifest_mac_key_env: SECRET_MAC_ENV\nsources:\n  kafka:\n    enabled: false\n    bootstrap_servers: [nowhere]\n    topics: []\n    group_id: test\n    sasl_password_env: SECRET_KAFKA_ENV");
        std::fs::write(&path, yaml).unwrap();
        for json in [false, true] {
            let mut command = Command::new(env!("CARGO_BIN_EXE_vtopctl"));
            if json {
                command.arg("--json");
            }
            let out = command
                .args(["validate", "--print-effective", "--config"])
                .arg(&path)
                .env(
                    "SECRET_DATABASE_ENV",
                    "postgres://secret-user:secret-password@secret-host/database",
                )
                .env("SECRET_MAC_ENV", "secret-mac-value")
                .env("SECRET_KAFKA_ENV", "secret-kafka-value")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let output = String::from_utf8(out.stdout).unwrap();
            for secret in [
                "SECRET_DATABASE_ENV",
                "SECRET_MAC_ENV",
                "SECRET_KAFKA_ENV",
                "/run/secrets/secret-database-file",
                "secret-user",
                "secret-password",
                "secret-host",
                "postgres://",
                "secret-mac-value",
                "secret-kafka-value",
            ] {
                assert!(!output.contains(secret), "leaked {secret}: {output}");
            }
            assert!(output.contains("enabled"));
        }
    }
}
