//! `docs/reference/tamper-evidence.md` lists every tamper case with the test
//! that proves it. This keeps the list and the tests from drifting: every
//! test in the two tamper suites is in the table, every test the table names
//! exists, and the security model links to the page.

use std::collections::BTreeSet;
use std::path::PathBuf;

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

/// The names of the test functions in `source`: the `fn` that follows a
/// `#[test]` or `#[tokio::test]` attribute.
fn test_names(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut pending = false;
    for line in source.lines() {
        let line = line.trim();
        if line == "#[test]" || line == "#[tokio::test]" {
            pending = true;
        } else if pending && (line.starts_with("fn ") || line.starts_with("async fn ")) {
            let name = line
                .trim_start_matches("async ")
                .trim_start_matches("fn ")
                .split('(')
                .next()
                .unwrap();
            names.insert(name.trim().to_owned());
            pending = false;
        }
    }
    names
}

#[test]
fn every_tamper_test_is_in_the_table_and_every_test_in_the_table_exists() {
    let mut tests = BTreeSet::new();
    for file in [
        "crates/vtop-cli/tests/live_s3_tamper.rs",
        "crates/vtop-cli/tests/tamper_audit_trail.rs",
    ] {
        let source = std::fs::read_to_string(repo(file)).unwrap();
        tests.extend(test_names(&source));
    }
    assert!(tests.len() >= 20, "found only {tests:?}");

    let doc = std::fs::read_to_string(repo("docs/reference/tamper-evidence.md")).unwrap();
    // The last cell of each table row holds the test names, in backticks.
    let mut named = BTreeSet::new();
    for row in doc.lines().filter(|l| l.starts_with('|')) {
        let last = row.trim_end_matches('|').rsplit('|').next().unwrap();
        for (i, part) in last.split('`').enumerate() {
            if i % 2 == 1 {
                named.insert(part.trim().to_owned());
            }
        }
    }
    let missing_from_doc: Vec<_> = tests.difference(&named).collect();
    assert!(
        missing_from_doc.is_empty(),
        "tests with no row in docs/reference/tamper-evidence.md: {missing_from_doc:#?}"
    );
    let missing_test: Vec<_> = named.difference(&tests).collect();
    assert!(
        missing_test.is_empty(),
        "tests the table names that do not exist: {missing_test:#?}"
    );
}

#[test]
fn the_security_model_links_the_tamper_evidence_page() {
    let model = std::fs::read_to_string(repo("docs/SECURITY_MODEL.md")).unwrap();
    assert!(
        model.contains("reference/tamper-evidence.md"),
        "docs/SECURITY_MODEL.md must link reference/tamper-evidence.md"
    );
}
