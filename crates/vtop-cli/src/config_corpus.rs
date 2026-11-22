//! The config corpus (#743): every example config the repository commits, and
//! every config snippet its documentation shows, parses under this build.
//!
//! An operator copies these. A change that makes `config.yaml` stricter, drops
//! a key, renames one or changes what a value may be breaks a file in the
//! documentation without any other test noticing, because those tests build
//! their configs from scratch. This test reads the files themselves.
//!
//! - `examples/config.yaml` parses as an engine config and passes `validate`;
//!   `examples/streams.yaml` parses as the per-source overrides.
//! - Every fenced `yaml` block in the Markdown (the docs, the README, the Helm
//!   chart's README) is classified by what it is, and each kind is checked:
//!   an engine config fragment (top-level keys are engine sections) is
//!   overlaid on the example config and must then parse and validate; a
//!   `streams` list parses as the overrides; a `vtopctl` client config parses
//!   as one. A block that is not a VTOP config (a Kubernetes manifest, a
//!   Compose file, an alert rule, Helm values) is recognised by its first key
//!   and skipped. A block that is none of these fails the test: classify it
//!   here when it is added, so a snippet is never silently unchecked.
//! - Node configs (`vtop-node`) are checked by that crate's own corpus test,
//!   which has their types.

use serde_yaml::Value;
use std::path::{Path, PathBuf};
use vtop_core::config::{StreamsConfig, VtopConfig};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A fenced YAML block of a Markdown file: where it starts and its text with
/// the fence's indentation removed.
struct Block {
    file: String,
    line: usize,
    text: String,
}

fn blocks_of(path: &Path) -> Vec<Block> {
    let name = path
        .strip_prefix(root())
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned();
    let text = std::fs::read_to_string(path).expect("read markdown");
    let mut blocks = Vec::new();
    let mut open: Option<(usize, usize, Vec<String>)> = None; // (line, indent, body)
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        match &mut open {
            None => {
                if trimmed == "```yaml" || trimmed == "```yml" {
                    open = Some((index + 1, indent, Vec::new()));
                }
            }
            Some((start, fence_indent, body)) => {
                if trimmed == "```" {
                    blocks.push(Block {
                        file: name.clone(),
                        line: *start,
                        text: body.join("\n"),
                    });
                    open = None;
                } else {
                    let cut = line
                        .char_indices()
                        .take_while(|(i, c)| *i < *fence_indent && *c == ' ')
                        .count();
                    body.push(line[cut..].to_owned());
                }
            }
        }
    }
    blocks
}

fn markdown_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("md") {
                out.push(path);
            }
        }
    }
    let mut files = vec![root().join("README.md")];
    for dir in ["docs", "helm"] {
        walk(&root().join(dir), &mut files);
    }
    files.sort();
    files
}

/// Overlay `over` on `base`: mappings merge key by key, anything else replaces.
fn overlay(base: &mut Value, over: &Value) {
    match (base, over) {
        (Value::Mapping(base), Value::Mapping(over)) => {
            for (key, value) in over {
                match base.get_mut(key) {
                    Some(existing) => overlay(existing, value),
                    None => {
                        base.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base, over) => *base = over.clone(),
    }
}

fn example_config_text() -> String {
    std::fs::read_to_string(root().join("examples/config.yaml")).expect("examples/config.yaml")
}

fn parse_engine(text: &str, what: &str) {
    let config = VtopConfig::from_yaml_str(text)
        .unwrap_or_else(|error| panic!("{what} does not parse as a config.yaml: {error}"));
    config
        .validate()
        .unwrap_or_else(|error| panic!("{what} does not validate: {error}"));
}

#[test]
fn the_example_config_parses_and_validates() {
    parse_engine(&example_config_text(), "examples/config.yaml");
}

#[test]
fn the_example_streams_file_parses() {
    let text = std::fs::read_to_string(root().join("examples/streams.yaml")).expect("streams");
    let streams = StreamsConfig::from_yaml_str(&text)
        .unwrap_or_else(|error| panic!("examples/streams.yaml does not parse: {error}"));
    assert!(!streams.streams.is_empty());
}

/// What a block is, by its top-level keys.
#[derive(Debug, PartialEq)]
enum Kind {
    /// Sections of `config.yaml`.
    Engine,
    /// `streams.yaml`.
    Streams,
    /// `vtopctl meta` admin client config.
    AdminClient,
    /// `vtopctl node status` client config.
    NodeClient,
    /// A fragment of a `vtop-node` config (checked in that crate).
    NodeFragment,
    /// Not a VTOP config.
    Foreign(&'static str),
}

const ENGINE_SECTIONS: &[&str] = &[
    "engine",
    "batching",
    "compression",
    "checksum",
    "manifest_mac_key_env",
    "manifest",
    "sources",
    "upload",
    "partitioning",
    "security",
];

fn classify(value: &Value) -> Option<Kind> {
    let keys: Vec<String> = match value {
        Value::Mapping(map) => map
            .keys()
            .filter_map(|key| key.as_str().map(str::to_owned))
            .collect(),
        // A list at the top: an alert-rule list.
        Value::Sequence(_) => return Some(Kind::Foreign("a list (Prometheus alert rules)")),
        _ => return None,
    };
    let has = |name: &str| keys.iter().any(|key| key == name);
    let only = |allowed: &[&str]| keys.iter().all(|key| allowed.contains(&key.as_str()));
    if has("apiVersion") || has("kind") || has("spec") {
        return Some(Kind::Foreign("a Kubernetes manifest or pod spec"));
    }
    if has("services") {
        return Some(Kind::Foreign("a Compose file"));
    }
    if has("extraVolumes") || has("extraVolumeMounts") || has("replicaCount") {
        return Some(Kind::Foreign("Helm values"));
    }
    if has("streams") && only(&["streams"]) {
        return Some(Kind::Streams);
    }
    if has("endpoint")
        && only(&[
            "endpoint",
            "server_name",
            "transport",
            "ca_cert",
            "client_cert",
            "client_key",
            "peers",
        ])
    {
        return Some(Kind::AdminClient);
    }
    if has("range") && has("replicas") {
        return Some(Kind::NodeClient);
    }
    if only(&["observability"]) || only(&["range"]) {
        return Some(Kind::NodeFragment);
    }
    if !keys.is_empty() && only(ENGINE_SECTIONS) {
        return Some(Kind::Engine);
    }
    None
}

/// A placeholder the docs use for an identifier an operator fills in
/// (`8b1f...-...`): replaced by a distinct valid UUID each, so the rest is
/// parsed and two placeholders are never read as the same node.
fn without_placeholders(text: &str) -> String {
    const PLACEHOLDER: &str = "...-...";
    let mut out = String::new();
    for (number, line) in text.lines().enumerate() {
        let mut line = line.to_owned();
        if let Some(at) = line.find(PLACEHOLDER) {
            let start = line[..at]
                .rfind(|c: char| !c.is_ascii_hexdigit())
                .map_or(0, |i| i + 1);
            line.replace_range(
                start..at + PLACEHOLDER.len(),
                &format!("aaaaaaaa-0000-0000-0000-{:012x}", number + 1),
            );
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

fn temp_file(text: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("temp file");
    std::fs::write(file.path(), text).expect("write");
    file
}

#[test]
fn every_yaml_snippet_in_the_docs_is_a_config_this_build_accepts() {
    let base: Value = serde_yaml::from_str(&example_config_text()).expect("example config");
    let mut checked = 0;
    let mut skipped = Vec::new();
    for path in markdown_files() {
        for block in blocks_of(&path) {
            let here = format!("{}:{}", block.file, block.line);
            let text = without_placeholders(&block.text);
            let value: Value = serde_yaml::from_str(&text)
                .unwrap_or_else(|error| panic!("{here}: the snippet is not YAML: {error}"));
            let kind = classify(&value).unwrap_or_else(|| {
                panic!(
                    "{here}: a YAML snippet that is not recognised. Add its kind to \
                     config_corpus.rs (and check it parses), so it is not left unchecked:\n{}",
                    block.text
                )
            });
            match kind {
                Kind::Engine => {
                    let mut merged = base.clone();
                    overlay(&mut merged, &value);
                    parse_engine(
                        &serde_yaml::to_string(&merged).expect("yaml"),
                        &format!("{here} (on the example config)"),
                    );
                    checked += 1;
                }
                Kind::Streams => {
                    StreamsConfig::from_yaml_str(&text).unwrap_or_else(|error| {
                        panic!("{here}: does not parse as streams.yaml: {error}")
                    });
                    checked += 1;
                }
                Kind::AdminClient => {
                    let file = temp_file(&text);
                    crate::meta_tools::load_admin_config(file.path())
                        .unwrap_or_else(|error| panic!("{here}: {error}"));
                    checked += 1;
                }
                Kind::NodeClient => {
                    let file = temp_file(&text);
                    crate::node_tools::load_config(file.path())
                        .unwrap_or_else(|error| panic!("{here}: {error}"));
                    checked += 1;
                }
                Kind::NodeFragment => skipped.push(format!("{here}: a vtop-node fragment")),
                Kind::Foreign(what) => skipped.push(format!("{here}: {what}")),
            }
        }
    }
    // The corpus is not empty: a scanner that finds nothing would pass.
    assert!(
        checked >= 10,
        "only {checked} snippets were checked; skipped: {skipped:#?}"
    );
}
