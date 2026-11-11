//! Manifest format evolution (#671, release A): readers check `protocol` and
//! `version` first, verify 0.1-0.3 exactly as before, verify 0.4 over the
//! stored document so unknown fields stay authenticated, and report a newer
//! version as newer rather than as tampered.

use serde_json::{json, Value};
use vtop_core::checksum::sha256_bytes;
use vtop_core::errors::VtopError;
use vtop_core::manifest::{ManifestBuilder, ManifestMacKey, ParsedManifest};
use vtop_core::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};

fn legacy() -> Value {
    serde_json::to_value(
        ManifestBuilder {
            batch_id: "evolution".into(),
            tenant: "default".into(),
            source_type: SourceType::File,
            source_name: "input.log".into(),
            format: TelemetryFormat::Raw,
            compression: CompressionType::None,
            record_count: 1,
            first_timestamp: None,
            last_timestamp: None,
            source_progress: ProgressMarker::File {
                path: "input.log".into(),
                inode: None,
                start_byte: 0,
                end_byte: 4,
                file_size: 4,
                mtime: String::new(),
                content_fingerprint: None,
                tail_fingerprint: None,
            },
            object_uri: "s3://archive/object".into(),
            object_size: 4,
            object_checksum_algorithm: "sha256".into(),
            object_checksum: sha256_bytes(b"one\n"),
            manifest_uri: "s3://archive/manifest".into(),
            path_template: "test".into(),
            resolved_prefix: "test".into(),
            upload_backend: "localfs".into(),
            created_at: "2026-11-10T00:00:00Z".into(),
        }
        .build()
        .unwrap(),
    )
    .unwrap()
}

fn future(signed: bool) -> Value {
    let mut doc = legacy();
    doc["version"] = json!("0.4");
    doc["extension"] = json!({"z": [1, {"nested": true}], "a": "unknown"});
    doc["manifest"]["sha256"] = json!("");
    if signed {
        doc["manifest"]["mac"] = json!("");
    }
    // serde_json's default Value maps are sorted; this fixture deliberately
    // does not call the reader's canonicalization implementation.
    let auth = serde_json::to_vec(&doc).unwrap();
    doc["manifest"]["sha256"] = json!(sha256_bytes(&auth));
    if signed {
        doc["manifest"]["mac"] = json!(blake3::keyed_hash(&[0x11; 32], &auth).to_hex().to_string());
    }
    doc
}

#[test]
fn unknown_04_fields_survive_hash_and_mac_verification() {
    for signed in [false, true] {
        let doc = future(signed);
        let bytes = serde_json::to_vec_pretty(&doc).unwrap();
        let parsed = ParsedManifest::from_slice(&bytes).unwrap();
        let key = ManifestMacKey::from_hex(&"11".repeat(32)).unwrap();
        parsed
            .verify_authentication(signed.then_some(&key))
            .unwrap();
        let mut changed = doc;
        changed["extension"]["z"][1]["nested"] = json!(false);
        assert!(
            ParsedManifest::from_slice(&serde_json::to_vec(&changed).unwrap())
                .unwrap()
                .verify_authentication(signed.then_some(&key))
                .is_err(),
            "unknown fields are authenticated"
        );
    }
}

#[test]
fn canonical_04_hash_ignores_whitespace_and_key_order() {
    let doc = future(false);
    let compact = serde_json::to_vec(&doc).unwrap();
    let pretty = serde_json::to_vec_pretty(&doc).unwrap();
    let fields = doc
        .as_object()
        .unwrap()
        .iter()
        .rev()
        .map(|(k, v)| format!("{}:{}", serde_json::to_string(k).unwrap(), v))
        .collect::<Vec<_>>()
        .join(",");
    let reordered = format!("{{{fields}}}");
    for bytes in [&compact[..], &pretty[..], reordered.as_bytes()] {
        ParsedManifest::from_slice(bytes)
            .unwrap()
            .verify_authentication(None)
            .unwrap();
    }
}

#[test]
fn newer_version_is_distinct_before_decoding_or_authentication() {
    for version in [
        "0.5",
        "0.9",
        "1.0",
        "10.0",
        "99999999999999999999999.0",
        "0.99999999999999999999999",
    ] {
        let bytes = serde_json::to_vec(&json!({"protocol":"VTOP", "version":version})).unwrap();
        let err = ParsedManifest::from_slice(&bytes).unwrap_err();
        assert!(matches!(err, VtopError::UnsupportedManifestVersion { .. }));
        assert!(err.to_string().contains("cannot authenticate"));
    }
    for header in [
        json!({"protocol":"other", "version":"0.4"}),
        json!({"protocol":"VTOP", "version":"0.0"}),
        json!({"protocol":"VTOP", "version":"garbage"}),
    ] {
        assert!(matches!(
            ParsedManifest::from_slice(&serde_json::to_vec(&header).unwrap()),
            Err(VtopError::Manifest(_))
        ));
    }
}

#[test]
fn writers_still_emit_03_and_legacy_hash_is_unchanged() {
    let doc = legacy();
    assert_eq!(doc["version"], "0.3");
    ParsedManifest::from_slice(&serde_json::to_vec(&doc).unwrap())
        .unwrap()
        .verify_authentication(None)
        .unwrap();
}

#[test]
fn released_070_manifests_verify_without_reencoding_the_fixture() {
    let key = ManifestMacKey::from_hex(&"11".repeat(32)).unwrap();
    for (bytes, signed) in [
        (
            &include_bytes!("fixtures/manifests/v0.7.0-unsigned.json")[..],
            false,
        ),
        (
            &include_bytes!("fixtures/manifests/v0.7.0-signed.json")[..],
            true,
        ),
    ] {
        let parsed = ParsedManifest::from_slice(bytes).unwrap();
        assert_eq!(parsed.fields().version, "0.2");
        parsed
            .verify_authentication(signed.then_some(&key))
            .unwrap();
    }
}

/// Seal an arbitrary 0.4 document the way the protocol draft specifies,
/// without calling the reader's implementation.
fn seal(mut doc: Value) -> Value {
    doc["manifest"]["sha256"] = json!("");
    let auth = serde_json::to_vec(&doc).unwrap();
    doc["manifest"]["sha256"] = json!(sha256_bytes(&auth));
    doc
}

#[test]
fn changing_a_known_04_field_fails_verification() {
    let mut doc = future(false);
    doc["record_count"] = json!(2);
    let err = ParsedManifest::from_slice(&serde_json::to_vec(&doc).unwrap())
        .unwrap()
        .verify_authentication(None)
        .unwrap_err();
    assert!(matches!(err, VtopError::ChecksumMismatch { .. }), "{err}");
}

/// `serde_json` keeps the last of two equal keys, other parsers the first: a
/// document both accept would authenticate one value and hand another to a
/// first-wins consumer. A 0.4 reader refuses it as malformed.
#[test]
fn a_duplicate_key_in_an_04_manifest_is_malformed() {
    let compact = String::from_utf8(serde_json::to_vec(&future(false)).unwrap()).unwrap();
    let forged = compact.replacen(r#""a":"unknown""#, r#""a":"forged","a":"unknown""#, 1);
    assert_ne!(forged, compact);
    let err = ParsedManifest::from_slice(forged.as_bytes()).unwrap_err();
    assert!(
        !matches!(err, VtopError::UnsupportedManifestVersion { .. }),
        "{err}"
    );
    assert!(err.to_string().contains("duplicate key"), "{err}");
}

/// A fraction re-encodes through `f64`, so two different stored numbers can
/// share one canonical form and an edit between them would still verify.
#[test]
fn a_non_integer_number_in_an_04_manifest_is_malformed() {
    let mut doc = future(false);
    doc["extension"]["ratio"] = json!(0.1);
    let sealed = String::from_utf8(serde_json::to_vec(&seal(doc)).unwrap()).unwrap();
    let edited = sealed.replacen("0.1", "0.10000000000000001", 1);
    for text in [&sealed, &edited] {
        let err = ParsedManifest::from_slice(text.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("integer"), "{err}");
    }
    // Integers across the whole signed and unsigned 64-bit range are exact.
    let mut doc = future(false);
    doc["extension"]["range"] = json!([i64::MIN, u64::MAX, 0]);
    ParsedManifest::from_slice(&serde_json::to_vec(&seal(doc)).unwrap())
        .unwrap()
        .verify_authentication(None)
        .unwrap();
}

/// The legacy scheme is untouched: an unknown field in a 0.3 document is
/// still dropped before hashing, exactly as every released reader does.
#[test]
fn a_legacy_manifest_keeps_the_struct_reencoding_scheme() {
    let mut doc = legacy();
    doc["not_in_03"] = json!("ignored, as before");
    ParsedManifest::from_slice(&serde_json::to_vec_pretty(&doc).unwrap())
        .unwrap()
        .verify_authentication(None)
        .unwrap();
}

#[test]
fn a_malformed_envelope_is_not_reported_as_newer() {
    for doc in [
        json!({"version": "0.4"}),
        json!({"protocol": "VTOP"}),
        json!({"protocol": "VTOP", "version": 4}),
        json!({"protocol": "VTOP", "version": "0.4.1.0"}),
        json!({"protocol": "VTOP", "version": "-1.0"}),
        json!({"protocol": "VTOP", "version": ""}),
        json!(["VTOP", "0.9"]),
    ] {
        let err = ParsedManifest::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
        assert!(
            !matches!(err, VtopError::UnsupportedManifestVersion { .. }),
            "{doc}: {err}"
        );
    }
}

/// The version comes from storage and is echoed into logs and reports.
#[test]
fn a_huge_newer_version_is_reported_bounded() {
    let version = format!("{}.0", "9".repeat(10_000));
    let bytes = serde_json::to_vec(&json!({"protocol": "VTOP", "version": version})).unwrap();
    let err = ParsedManifest::from_slice(&bytes).unwrap_err();
    assert!(matches!(err, VtopError::UnsupportedManifestVersion { .. }));
    assert!(err.to_string().len() < 200, "{err}");
}

/// Review of #674: the header was read into a last-wins map, so a document
/// carrying `version` twice could be reported as newer. A duplicate envelope
/// key is malformed, whatever the version.
#[test]
fn a_duplicate_envelope_key_is_malformed_not_newer() {
    for text in [
        r#"{"protocol":"VTOP","version":"0.4","version":"0.9"}"#,
        r#"{"protocol":"VTOP","version":"0.3","version":"0.9"}"#,
        r#"{"protocol":"other","protocol":"VTOP","version":"0.9"}"#,
        r#"{"protocol":"VTOP","version":"0.9","version":"0.9"}"#,
    ] {
        let err = ParsedManifest::from_slice(text.as_bytes()).unwrap_err();
        assert!(
            !matches!(err, VtopError::UnsupportedManifestVersion { .. }),
            "{text}: {err}"
        );
        assert!(err.to_string().contains("duplicate"), "{text}: {err}");
    }
    // A legacy manifest with a duplicate envelope key is refused too.
    let mut doc = legacy();
    doc["tenant"] = json!("default");
    let text = String::from_utf8(serde_json::to_vec(&doc).unwrap()).unwrap();
    let doubled = text.replacen(
        r#""version":"0.3""#,
        r#""version":"0.3","version":"0.3""#,
        1,
    );
    assert_ne!(doubled, text);
    assert!(ParsedManifest::from_slice(doubled.as_bytes()).is_err());
}

/// The version grammar is fixed now, so later writers are not tied to two
/// parts: dotted digits, two or three parts, no leading zeros. Above the
/// newest readable version is newer; anything else is malformed.
#[test]
fn a_three_part_version_above_the_maximum_is_newer_and_bad_grammar_is_malformed() {
    let classify = |version: &str| {
        let bytes = serde_json::to_vec(&json!({"protocol": "VTOP", "version": version})).unwrap();
        ParsedManifest::from_slice(&bytes).unwrap_err()
    };
    for version in ["1.0.0", "0.5.1", "0.4.1", "0.5", "10.0"] {
        let err = classify(version);
        assert!(
            matches!(&err, VtopError::UnsupportedManifestVersion { supported, .. }
                if *supported == vtop_core::manifest::MAX_READABLE_VERSION),
            "{version}: {err}"
        );
    }
    for version in [
        "00.5", "0.05", "0.4a", "", "0.4.0.1", "1.", ".5", "0.4.0", "0.3.9", "+1.0", " 1.0",
    ] {
        let err = classify(version);
        assert!(
            !matches!(err, VtopError::UnsupportedManifestVersion { .. }),
            "{version}: {err}"
        );
    }
}

/// `-0` re-encodes as `0` (or `-0.0`): two stored forms, one canonical form.
#[test]
fn negative_zero_in_an_04_manifest_is_malformed() {
    let mut doc = future(false);
    doc["extension"]["zero"] = json!(0);
    let sealed = String::from_utf8(serde_json::to_vec(&seal(doc)).unwrap()).unwrap();
    let edited = sealed.replacen(r#""zero":0"#, r#""zero":-0"#, 1);
    assert_ne!(edited, sealed);
    assert!(ParsedManifest::from_slice(edited.as_bytes()).is_err());
}

/// Review of #674: a 0.4 manifest must not hand out the legacy re-encoded
/// hash or bytes - they drop its unknown fields, the way back to "newer
/// looks tampered".
#[test]
fn a_parsed_04_manifest_gives_no_legacy_hash_or_reencoding() {
    let parsed = ParsedManifest::from_slice(&serde_json::to_vec(&future(false)).unwrap()).unwrap();
    assert!(parsed.fields().self_hash().is_err());
    assert!(parsed.fields().to_json_bytes().is_err());
    assert!(parsed.fields().verify_self_hash().is_err());
    // A legacy one still does.
    let parsed = ParsedManifest::from_slice(&serde_json::to_vec(&legacy()).unwrap()).unwrap();
    parsed.fields().self_hash().unwrap();
    parsed.fields().to_json_bytes().unwrap();
}

/// The error says what is known: the document CLAIMS a newer version, which
/// this build cannot authenticate.
#[test]
fn the_newer_version_error_says_the_version_is_claimed() {
    let bytes = serde_json::to_vec(&json!({"protocol": "VTOP", "version": "0.9"})).unwrap();
    let err = ParsedManifest::from_slice(&bytes).unwrap_err();
    assert!(err.to_string().contains("claims version 0.9"), "{err}");
    assert!(err.to_string().contains("cannot authenticate"), "{err}");
}
