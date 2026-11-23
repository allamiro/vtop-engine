//! `config_version` and the strict key check (#743, decision 7).

use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::config_strict::{unknown_config_keys, unknown_streams_keys, CONFIG_VERSION};

const BASE: &str = "engine:\n  name: strict-test\n  state_store: {env: DB}\n  work_dir: /unused\n\
batching: {}\ncompression: {}\nsources: {}\nupload:\n  backend: localfs\n  bucket: archive\n  local_path: /unused\n";

fn with(extra: &str) -> String {
    format!("{extra}{BASE}")
}

#[test]
fn a_clean_config_has_no_unknown_keys() {
    assert!(unknown_config_keys(BASE).unwrap().is_empty());
}

#[test]
fn an_unknown_key_is_still_loaded_leniently() {
    // The default through 0.x: loading ignores the key.
    VtopConfig::from_yaml_str(&with("extra_top_level: 1\n")).unwrap();
    let keys = unknown_config_keys(&with("extra_top_level: 1\n")).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].path, "extra_top_level");
}

#[test]
fn nested_unknown_keys_are_found_with_their_path() {
    let text = BASE
        .replace("name: strict-test", "nmae: x\n  name: strict-test")
        .replace("batching: {}", "batching:\n  max_bytez: 5")
        .replace(
            "sources: {}",
            "sources:\n  file:\n    paths: [a]\n    pathz: [a]",
        );
    let paths: Vec<String> = unknown_config_keys(&text)
        .unwrap()
        .into_iter()
        .map(|k| k.path)
        .collect();
    assert_eq!(
        paths,
        ["batching.max_bytez", "engine.nmae", "sources.file.pathz"]
    );
}

#[test]
fn a_misspelling_gets_the_nearest_known_key_at_its_level() {
    let text = BASE.replace("name: strict-test", "nmae: x\n  name: strict-test");
    let keys = unknown_config_keys(&text).unwrap();
    assert_eq!(keys[0].path, "engine.nmae");
    assert_eq!(keys[0].suggestion.as_deref(), Some("engine.name"));
    assert_eq!(
        keys[0].to_string(),
        "engine.nmae (did you mean `engine.name`?)"
    );
    // Nothing close: no suggestion rather than a wrong one.
    let far = unknown_config_keys(&with("zzzzzzzz: 1\n")).unwrap();
    assert_eq!(far[0].suggestion, None);
}

#[test]
fn a_top_level_misspelling_is_matched_against_the_sections() {
    let keys = unknown_config_keys(&with("batchin: {}\n")).unwrap();
    assert_eq!(keys[0].path, "batchin");
    assert_eq!(keys[0].suggestion.as_deref(), Some("batching"));
}

#[test]
fn streams_entries_are_walked_by_index() {
    let text = "streams:\n- source_name: a\n  source_type: file\n  format: jsonl\n\
- source_name: b\n  source_type: file\n  format: jsonl\n  tenent: t\n";
    StreamsConfig::from_yaml_str(text).unwrap();
    let keys = unknown_streams_keys(text).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].path, "streams[1].tenent");
    assert_eq!(keys[0].suggestion.as_deref(), Some("streams[1].tenant"));
}

#[test]
fn a_config_that_does_not_load_reports_its_own_error() {
    let err = unknown_config_keys("engine: 1\n").unwrap_err().to_string();
    assert!(err.contains("engine"), "{err}");
}

#[test]
fn config_version_absent_and_current_are_accepted() {
    let absent = VtopConfig::from_yaml_str(BASE).unwrap();
    assert_eq!(absent.config_version, None);
    let current =
        VtopConfig::from_yaml_str(&with(&format!("config_version: {CONFIG_VERSION}\n"))).unwrap();
    assert_eq!(current.config_version, Some(CONFIG_VERSION));
    // The key is known, so strict mode does not report it.
    assert!(
        unknown_config_keys(&with(&format!("config_version: {CONFIG_VERSION}\n")))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_newer_config_version_is_refused_naming_both() {
    let newer = CONFIG_VERSION + 1;
    let err = VtopConfig::from_yaml_str(&with(&format!("config_version: {newer}\n")))
        .unwrap_err()
        .to_string();
    assert!(err.contains(&format!("config_version: {newer}")), "{err}");
    assert!(err.contains(&format!("up to {CONFIG_VERSION}")), "{err}");
}

#[test]
fn a_newer_config_version_wins_over_a_key_this_build_cannot_parse() {
    // `manifest.mac` refuses unknown keys, so a newer file's new key there
    // would fail the typed parse first; the version is the cause.
    let text = with(&format!(
        "config_version: {}\nmanifest:\n  mac:\n    active: k\n    keys: []\n    from_the_future: 1\n",
        CONFIG_VERSION + 1
    ));
    let err = VtopConfig::from_yaml_str(&text).unwrap_err().to_string();
    assert!(err.contains("newer"), "{err}");
    assert!(!err.contains("from_the_future"), "{err}");
}

#[test]
fn a_malformed_config_version_is_refused() {
    for bad in ["0", "\"1\"", "1.5", "-1", "[1]", "true"] {
        let err = VtopConfig::from_yaml_str(&with(&format!("config_version: {bad}\n")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("config_version"), "{bad}: {err}");
    }
}
