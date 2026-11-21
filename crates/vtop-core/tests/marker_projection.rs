//! The manifest projection of every progress-marker variant is a committed
//! golden (#619, #743).
//!
//! `ProgressMarker::for_manifest()` strips the ledger-only fields: the
//! content and tail fingerprints (#578, #620), the spool's `file_size`, and
//! the file read's `ctime` (#619). If a ledger-only field reached a
//! manifest, the bytes a released `verify-manifest` re-encodes would change.
//! So each variant is built with EVERY optional field set, projected, and
//! compared byte for byte with `fixtures/markers/<variant>-0.3.json`, the
//! shape manifest version 0.3 carries (see that directory's README).

use vtop_core::types::{ContentFingerprint, ProgressMarker};

fn golden(name: &str) -> String {
    let path = format!(
        "{}/tests/fixtures/markers/{name}-0.3.json",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{path}: {e}"))
        .trim_end()
        .to_owned()
}

fn fingerprint(bytes: &[u8]) -> Option<ContentFingerprint> {
    ContentFingerprint::of(bytes)
}

/// The golden a variant's projection is held to. Exhaustive: a new variant
/// does not compile here until it names its golden (and `every_variant`
/// then has to build one, or `every_variant_has_a_marker` fails).
fn golden_name(marker: &ProgressMarker) -> &'static str {
    match marker {
        ProgressMarker::File { .. } => "file",
        ProgressMarker::SyslogSpool { .. } => "syslog_spool",
        ProgressMarker::Kafka { .. } => "kafka",
    }
}

/// One marker per variant, every optional field set.
fn every_variant() -> Vec<(&'static str, ProgressMarker)> {
    [
        ProgressMarker::File {
            path: "/var/log/app/app.log".into(),
            inode: Some(4242),
            start_byte: 100,
            end_byte: 4196,
            file_size: 5000,
            mtime: "2026-11-14T11:15:06.796566426+00:00".into(),
            content_fingerprint: fingerprint(b"first page"),
            tail_fingerprint: fingerprint(b"page below the end"),
            ctime: Some("2026-11-14T11:15:07.000000123+00:00".into()),
        },
        ProgressMarker::SyslogSpool {
            spool_id: "edge-1".into(),
            path: "/var/spool/vtop/edge-1.spool".into(),
            inode: Some(5151),
            start_byte: 0,
            end_byte: 2048,
            received_time_start: Some("2026-11-14T11:00:00.000000001+00:00".into()),
            received_time_end: Some("2026-11-14T11:05:00.000000002+00:00".into()),
            file_size: Some(4096),
            content_fingerprint: fingerprint(b"spool head"),
        },
        ProgressMarker::Kafka {
            topic: "events".into(),
            partition: 3,
            start_offset: 1000,
            end_offset: 1999,
            consumer_group: "vtop-archive".into(),
            topic_id: None,
        },
    ]
    .into_iter()
    .map(|marker| (golden_name(&marker), marker))
    .collect()
}

/// Every golden file has a marker built for it: a variant added to
/// `golden_name` without one in `every_variant` fails here.
#[test]
fn every_variant_has_a_marker() {
    let dir = format!("{}/tests/fixtures/markers", env!("CARGO_MANIFEST_DIR"));
    let mut goldens: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().into_owned();
            name.strip_suffix("-0.3.json").map(str::to_owned)
        })
        .collect();
    goldens.sort();
    let mut built: Vec<String> = every_variant()
        .into_iter()
        .map(|(name, _)| name.to_owned())
        .collect();
    built.sort();
    assert_eq!(
        built, goldens,
        "a golden with no marker built for it, or the reverse"
    );
}

#[test]
fn every_variant_projects_to_its_committed_0_3_golden() {
    for (name, marker) in every_variant() {
        let full = serde_json::to_string(&marker).unwrap();
        let projected = serde_json::to_string(&marker.for_manifest()).unwrap();
        assert_eq!(
            projected,
            golden(name),
            "{name}: the manifest projection is not the committed 0.3 golden; the ledger \
             marker was {full}"
        );
        for ledger_only in [
            "content_fingerprint",
            "tail_fingerprint",
            "ctime",
            "file_size\":4096",
        ] {
            assert!(
                !projected.contains(ledger_only),
                "{name}: the ledger-only field {ledger_only} reached the manifest: {projected}"
            );
        }
        // The golden reads back as the projection: nothing in it is lost.
        assert_eq!(
            serde_json::from_str::<ProgressMarker>(&golden(name)).unwrap(),
            marker.for_manifest(),
            "{name}"
        );
    }
}

/// The ledger keeps the fields the projection drops: a sanity check that
/// the test above sets them, so its goldens prove something.
#[test]
fn the_ledger_marker_carries_what_the_projection_drops() {
    for (name, marker) in every_variant() {
        let full = serde_json::to_string(&marker).unwrap();
        let expected: &[&str] = match name {
            "file" => &["content_fingerprint", "tail_fingerprint", "\"ctime\""],
            "syslog_spool" => &["content_fingerprint", "\"file_size\""],
            _ => &[],
        };
        for field in expected {
            assert!(full.contains(field), "{name}: {field} not set in {full}");
        }
    }
}

/// The file golden has exactly the keys, in the order, of `source_progress`
/// in the manifests the released v0.8.0 binary wrote (manifest version 0.3).
#[test]
fn the_file_golden_has_the_released_v0_8_0_source_progress_keys() {
    fn keys(json: &str) -> Vec<String> {
        // Key order as written, from the raw text: serde_json's default
        // maps are sorted.
        let mut out = Vec::new();
        let mut rest = json;
        let mut depth = 0usize;
        while let Some(i) = rest.find(['{', '}', '"']) {
            let c = rest.as_bytes()[i];
            rest = &rest[i + 1..];
            match c {
                b'{' => depth += 1,
                b'}' => depth = depth.saturating_sub(1),
                _ => {
                    let end = rest.find('"').unwrap();
                    let token = &rest[..end];
                    rest = &rest[end + 1..];
                    if depth == 1 && rest.trim_start().starts_with(':') {
                        out.push(token.to_owned());
                    }
                }
            }
        }
        out
    }
    for fixture in ["v0.8.0-unsigned.json", "v0.8.0-signed.json"] {
        let path = format!(
            "{}/tests/fixtures/manifests/{fixture}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap();
        let doc: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(doc["version"], "0.3", "{fixture}");
        let start = text.find("\"source_progress\"").expect("source_progress");
        let open = start + text[start..].find('{').unwrap();
        let close = open + text[open..].find('}').unwrap();
        assert_eq!(
            keys(&text[open..=close]),
            keys(&golden("file")),
            "{fixture}: source_progress keys"
        );
    }
}
