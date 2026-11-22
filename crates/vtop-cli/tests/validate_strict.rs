//! `vtopctl validate --strict` and `config_version` (#743, decision 7).

use std::path::Path;
use std::process::{Command, Output};

const BASE: &str = "engine:\n  name: strict-test\n  state_store: {env: SECRET_DATABASE_ENV}\n  work_dir: /unused\n\
batching: {}\ncompression: {}\nsources: {}\nupload:\n  backend: localfs\n  bucket: archive\n  local_path: /unused\n";

fn write(dir: &Path, config: &str, streams: Option<&str>) {
    std::fs::write(dir.join("config.yaml"), config).unwrap();
    if let Some(streams) = streams {
        std::fs::write(dir.join("streams.yaml"), streams).unwrap();
    }
}

fn validate(config: &Path, strict: bool, json: bool) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_vtopctl"));
    if json {
        c.arg("--json");
    }
    c.args(["validate", "--config"]).arg(config);
    if strict {
        c.arg("--strict");
    }
    c.env_remove("RUST_LOG")
        .env_remove("VTOP_LOG_FORMAT")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .env_remove("OTEL_SDK_DISABLED")
        .output()
        .unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn an_unknown_key_is_refused_only_under_strict() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &format!("typo_section: {{}}\n{BASE}"), None);
    let config = dir.path().join("config.yaml");

    let lenient = validate(&config, false, false);
    assert!(lenient.status.success(), "{}", stderr(&lenient));
    assert!(String::from_utf8_lossy(&lenient.stdout).contains("config is valid"));

    let strict = validate(&config, true, false);
    assert!(!strict.status.success());
    let err = stderr(&strict);
    assert!(err.contains("config.yaml: typo_section"), "{err}");
    assert!(err.contains("--strict"), "{err}");
}

#[test]
fn every_unknown_key_is_listed_with_its_path_and_a_suggestion() {
    let dir = tempfile::tempdir().unwrap();
    let config = BASE
        .replace("name: strict-test", "nmae: x\n  name: strict-test")
        .replace("batching: {}", "batching:\n  max_bytez: 5");
    write(
        dir.path(),
        &config,
        Some("streams:\n- source_name: a\n  source_type: file\n  format: jsonl\n  tenent: t\n"),
    );
    let out = validate(&dir.path().join("config.yaml"), true, false);
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(
        err.contains("config.yaml: engine.nmae (did you mean `engine.name`?)"),
        "{err}"
    );
    assert!(err.contains("config.yaml: batching.max_bytez"), "{err}");
    assert!(
        err.contains("streams.yaml: streams[0].tenent (did you mean `streams[0].tenant`?)"),
        "{err}"
    );
    assert!(err.contains("3 key(s)"), "{err}");
}

#[test]
fn strict_passes_a_clean_config_and_leaves_the_output_alone() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), BASE, None);
    let config = dir.path().join("config.yaml");
    let lenient = validate(&config, false, false);
    let strict = validate(&config, true, false);
    assert!(strict.status.success(), "{}", stderr(&strict));
    assert_eq!(strict.stdout, lenient.stdout);
    assert_eq!(
        validate(&config, true, true).stdout,
        b"{\"status\":\"ok\",\"warnings\":[]}\n"
    );
}

#[test]
fn a_newer_config_version_is_refused_with_or_without_strict() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &format!("config_version: 2\n{BASE}"), None);
    let config = dir.path().join("config.yaml");
    for strict in [false, true] {
        let out = validate(&config, strict, false);
        assert!(!out.status.success());
        let err = stderr(&out);
        assert!(err.contains("config_version: 2"), "{err}");
        assert!(err.contains("up to 1"), "{err}");
    }
}

#[test]
fn an_absent_or_current_config_version_is_accepted_under_strict() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.yaml");
    for text in [BASE.to_owned(), format!("config_version: 1\n{BASE}")] {
        write(dir.path(), &text, None);
        let out = validate(&config, true, false);
        assert!(out.status.success(), "{}", stderr(&out));
    }
}

/// The configs and stream mappings this repository ships must pass strict
/// validation: a stale key in an example is one an operator copies.
#[test]
fn every_shipped_example_config_passes_strict() {
    // `examples/streams.yaml` sits beside it and is read too.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let out = validate(&root.join("examples/config.yaml"), true, false);
    assert!(out.status.success(), "{}", stderr(&out));
}
