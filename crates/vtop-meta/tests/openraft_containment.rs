//! Policy: every `openraft` mention under `crates/*/src` must live inside
//! `crates/vtop-meta/src/raft/`. Cargo.toml / lock / tests may mention it.

use std::path::PathBuf;

#[test]
fn openraft_imports_are_confined_to_vtop_meta_raft_module() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root");
    let crates_dir = workspace.join("crates");
    let mut offenders = Vec::new();

    for crate_entry in std::fs::read_dir(&crates_dir).expect("crates/") {
        let crate_entry = crate_entry.expect("crate dir");
        let src = crate_entry.path().join("src");
        if !src.is_dir() {
            continue;
        }
        walk_rs(&src, &mut |path, contents| {
            if !contents.contains("openraft") {
                return;
            }
            let ok = path.components().any(|c| c.as_os_str() == "raft")
                && path.components().any(|c| c.as_os_str() == "vtop-meta");
            if !ok {
                offenders.push(path.display().to_string());
            }
        });
    }

    assert!(
        offenders.is_empty(),
        "openraft must only appear under crates/vtop-meta/src/raft/; found in:\n{}",
        offenders.join("\n")
    );
}

fn walk_rs(dir: &std::path::Path, visit: &mut dyn FnMut(&std::path::Path, &str)) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let entry = entry.expect("dirent");
        let path = entry.path();
        if path.is_dir() {
            walk_rs(&path, visit);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let contents = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            visit(&path, &contents);
        }
    }
}
