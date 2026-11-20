//! The manifest writer (#671, release B): version 0.3 stays byte for byte
//! what v0.8.0 wrote, and version 0.4, written only on opt-in, carries a
//! `verification_plan` and is hashed and MAC'd over the canonical form every
//! v0.8.0 and later reader authenticates.

use serde::Deserialize;
use serde_json::{json, Value};
use vtop_core::checksum::sha256_bytes;
use vtop_core::effective_config::verification_method;
use vtop_core::errors::VtopError;
use vtop_core::manifest::{
    ManifestBuilder, ManifestFormat, ManifestMacKey, ParsedManifest, VerificationPlan, VtopManifest,
};
use vtop_core::types::ChecksumAlgorithm;

const V080_UNSIGNED: &[u8] = include_bytes!("fixtures/manifests/v0.8.0-unsigned.json");
const V080_SIGNED: &[u8] = include_bytes!("fixtures/manifests/v0.8.0-signed.json");
const B04_UNSIGNED: &[u8] = include_bytes!("fixtures/manifests/release-b-0.4-unsigned.json");
const B04_SIGNED: &[u8] = include_bytes!("fixtures/manifests/release-b-0.4-signed.json");

/// The fixtures' non-secret MAC key: `11` repeated 32 times.
fn fixture_key() -> ManifestMacKey {
    ManifestMacKey::from_hex(&"11".repeat(32)).unwrap()
}

/// The builder inputs a stored manifest records, so a document can be
/// written again from what it says.
fn builder_from(m: &VtopManifest) -> ManifestBuilder {
    ManifestBuilder {
        batch_id: m.batch_id.clone(),
        tenant: m.tenant.clone(),
        source_type: m.source_type.clone(),
        source_name: m.source_name.clone(),
        format: m.format.clone(),
        compression: m.compression,
        record_count: m.record_count,
        first_timestamp: m.first_timestamp.clone(),
        last_timestamp: m.last_timestamp.clone(),
        source_progress: m.source_progress.clone(),
        object_uri: m.object.uri.clone(),
        object_size: m.object.size_bytes,
        object_checksum_algorithm: m.object.checksum_algorithm.clone(),
        object_checksum: m.object.checksum.clone(),
        manifest_uri: m.manifest.uri.clone(),
        path_template: m.partitioning.path_template.clone(),
        resolved_prefix: m.partitioning.resolved_prefix.clone(),
        upload_backend: m.upload_backend.clone(),
        created_at: m.created_at.clone(),
    }
}

fn released_builder() -> ManifestBuilder {
    builder_from(ParsedManifest::from_slice(V080_UNSIGNED).unwrap().fields())
}

fn plan() -> VerificationPlan {
    VerificationPlan::for_backend("localfs", ChecksumAlgorithm::Sha256, true).unwrap()
}

fn v04(key: Option<&ManifestMacKey>) -> vtop_core::manifest::WrittenManifest {
    released_builder()
        .build_written(ManifestFormat::V0_4 { plan: plan() }, key)
        .unwrap()
}

/// The 0.4 hash/MAC input as the protocol draft states it, computed here
/// without the reader's implementation: the stored document with
/// `manifest.sha256` (and `manifest.mac` where present) blanked, keys sorted
/// (serde_json's default `Value` maps are sorted), compact.
fn independent_auth_bytes(stored: &[u8]) -> Vec<u8> {
    let mut doc: Value = serde_json::from_slice(stored).unwrap();
    doc["manifest"]["sha256"] = json!("");
    if doc["manifest"].get("mac").is_some() {
        doc["manifest"]["mac"] = json!("");
    }
    serde_json::to_vec(&doc).unwrap()
}

/// The default writer is byte for byte the released v0.8.0 one: both
/// fixtures are unmodified output of the released binary, rebuilt here from
/// the fields they record.
#[test]
fn the_default_03_writer_reproduces_the_released_v080_bytes() {
    for (fixture, key) in [(V080_UNSIGNED, None), (V080_SIGNED, Some(fixture_key()))] {
        let released = ParsedManifest::from_slice(fixture).unwrap();
        assert_eq!(released.fields().version, "0.3");
        released.verify_authentication(key.as_ref()).unwrap();
        let rebuilt = builder_from(released.fields())
            .build_written(ManifestFormat::V0_3, key.as_ref())
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(rebuilt.bytes()),
            String::from_utf8_lossy(fixture),
            "the 0.3 writer must not change a byte"
        );
        // And through the pre-release-B entry points the engine used.
        let legacy = builder_from(released.fields())
            .build_with_mac(key.as_ref())
            .unwrap()
            .to_json_bytes()
            .unwrap();
        assert_eq!(legacy, fixture);
    }
}

#[test]
fn an_opt_in_04_manifest_is_written_and_verifies_with_and_without_a_key() {
    let key = fixture_key();
    let wrong = ManifestMacKey::from_hex(&"22".repeat(32)).unwrap();
    for signed in [false, true] {
        let written = v04(signed.then_some(&key));
        let fields = written.fields();
        assert_eq!(fields.version, "0.4");
        assert_eq!(fields.verification_plan, Some(plan()));
        assert_eq!(fields.manifest.mac.is_some(), signed);

        // The canonical bytes a reader authenticates, computed independently.
        let auth = independent_auth_bytes(written.bytes());
        assert_eq!(fields.manifest.sha256, sha256_bytes(&auth));
        if signed {
            assert_eq!(
                fields.manifest.mac.as_deref(),
                Some(
                    blake3::keyed_hash(&[0x11; 32], &auth)
                        .to_hex()
                        .to_string()
                        .as_str()
                )
            );
        }

        // Round trip through the reader every consumer uses.
        let parsed = ParsedManifest::from_slice(written.bytes()).unwrap();
        parsed.verify_self_hash().unwrap();
        parsed
            .verify_authentication(signed.then_some(&key))
            .unwrap();
        written
            .verify_authentication(signed.then_some(&key))
            .unwrap();
        assert_eq!(parsed.fields().verification_plan, Some(plan()));
        if signed {
            assert!(parsed.verify_authentication(Some(&wrong)).is_err());
        } else {
            // A configured key refuses an unsigned 0.4 manifest, as for 0.3.
            assert!(parsed.verify_authentication(Some(&key)).is_err());
        }
    }
}

/// The plan is inside the hash and the MAC: rewriting `method`, even with
/// the unkeyed hash recomputed, is caught.
#[test]
fn the_verification_plan_is_authenticated() {
    let key = fixture_key();
    for signed in [false, true] {
        let written = v04(signed.then_some(&key));
        let text = String::from_utf8(written.bytes().to_vec()).unwrap();
        let edited = text.replacen(
            r#""method": "read_back""#,
            r#""method": "store_checksum""#,
            1,
        );
        assert_ne!(edited, text);
        let err = ParsedManifest::from_slice(edited.as_bytes())
            .unwrap()
            .verify_authentication(signed.then_some(&key))
            .unwrap_err();
        assert!(matches!(err, VtopError::ChecksumMismatch { .. }), "{err}");

        let edited = text.replacen(r#""require_strong": true"#, r#""require_strong": false"#, 1);
        assert_ne!(edited, text);
        if signed {
            // The attacker recomputes the unkeyed hash; the MAC still fails.
            let mut doc: Value = serde_json::from_str(&edited).unwrap();
            doc["manifest"]["sha256"] =
                json!(sha256_bytes(&independent_auth_bytes(edited.as_bytes())));
            let parsed = ParsedManifest::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap();
            parsed.verify_self_hash().unwrap();
            assert!(parsed.verify_authentication(Some(&key)).is_err());
        } else {
            assert!(ParsedManifest::from_slice(edited.as_bytes())
                .unwrap()
                .verify_authentication(None)
                .is_err());
        }
    }
}

/// v0.8.0's `VtopManifest` requires `state` and `verification_status` (D3 of
/// the plan): a 0.4 manifest keeps both, at their write-time values, so a
/// release-A reader decodes it.
#[test]
fn a_04_manifest_keeps_the_fields_a_v080_reader_requires() {
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct V080Required {
        protocol: String,
        version: String,
        batch_id: String,
        tenant: String,
        source_type: String,
        source_name: String,
        format: String,
        compression: String,
        record_count: u64,
        first_timestamp: Option<String>,
        last_timestamp: Option<String>,
        source_progress: Value,
        object: Value,
        manifest: Value,
        partitioning: Value,
        upload_backend: String,
        state: String,
        verification_status: String,
        created_at: String,
    }
    for bytes in [v04(None).bytes().to_vec(), B04_UNSIGNED.to_vec()] {
        let old: V080Required = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(old.version, "0.4");
        assert_eq!(old.state, "manifest_uploaded");
        assert_eq!(old.verification_status, "not_verified");
    }
}

/// `method` is `verification_method(upload.backend, algorithm)` unchanged,
/// so the manifest, `validate --print-effective` and the startup log agree.
#[test]
fn the_plan_method_follows_the_backend_and_algorithm() {
    let cases = [
        ("s3_native", ChecksumAlgorithm::Sha256, "store_checksum"),
        ("s3_native", ChecksumAlgorithm::Blake3, "read_back"),
        ("s3_native", ChecksumAlgorithm::None, "size_only"),
        ("localfs", ChecksumAlgorithm::Sha256, "read_back"),
        ("localfs", ChecksumAlgorithm::Blake3, "read_back"),
        ("localfs", ChecksumAlgorithm::None, "size_only"),
        ("mock_limited", ChecksumAlgorithm::Sha256, "size_only"),
        ("mock_limited", ChecksumAlgorithm::Blake3, "size_only"),
        ("mock_limited", ChecksumAlgorithm::None, "size_only"),
        ("minio", ChecksumAlgorithm::Sha256, "read_back"),
        ("mock", ChecksumAlgorithm::Sha256, "read_back"),
    ];
    for (backend, algorithm, method) in cases {
        for require_strong in [false, true] {
            let plan = VerificationPlan::for_backend(backend, algorithm, require_strong).unwrap();
            assert_eq!(plan.method, method, "{backend} {algorithm:?}");
            assert_eq!(
                plan.method,
                verification_method(backend, algorithm).as_str()
            );
            assert_eq!(plan.require_strong, require_strong);
        }
    }
    let err =
        VerificationPlan::for_backend("custom_store", ChecksumAlgorithm::Sha256, true).unwrap_err();
    assert!(err.to_string().contains("custom_store"), "{err}");
    assert!(err.to_string().contains("verification method"), "{err}");
}

/// A 0.3 document never carries a plan: v0.8.0 drops one before hashing, so
/// there it would be unauthenticated. The legacy reader still verifies such a
/// document exactly as v0.8.0 did, and does not hand the plan out.
#[test]
fn a_plan_is_never_part_of_a_03_manifest() {
    let mut doc: Value = serde_json::from_slice(V080_UNSIGNED).unwrap();
    doc["verification_plan"] = json!({"method": "store_checksum", "require_strong": true});
    let parsed = ParsedManifest::from_slice(&serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
    parsed.verify_authentication(None).unwrap();
    assert_eq!(parsed.fields().verification_plan, None);

    // Nor can one be written into a 0.3 document.
    let mut manifest = released_builder().build().unwrap();
    manifest.verification_plan = Some(plan());
    assert!(manifest.to_json_bytes().is_err());
    // Its legacy hash is the one without the plan.
    manifest.verify_self_hash().unwrap();
}

/// Review of #698: a legacy reader must treat `verification_plan` as v0.8.0
/// did, as an unknown key, whatever its value. Decoding it first made a 0.3
/// manifest carrying a malformed or duplicated one (which no legacy hash
/// covers) fail to parse here while v0.8.0 verified it: scrub reported it
/// and recovery replayed the batch.
#[test]
fn a_malformed_plan_in_a_03_manifest_is_ignored_as_v080_ignores_it() {
    let key = fixture_key();
    for (fixture, signed) in [(V080_UNSIGNED, false), (V080_SIGNED, true)] {
        let text = String::from_utf8(fixture.to_vec()).unwrap();
        for plan in [
            r#""garbage""#,
            "5",
            "null",
            r#"{"method": "x"}"#,
            r#"{"method": 1, "require_strong": "yes"}"#,
            r#"[1, 2]"#,
        ] {
            let edited = text.replacen(
                r#""created_at""#,
                &format!(r#""verification_plan": {plan}, "created_at""#),
                1,
            );
            assert_ne!(edited, text);
            let parsed = ParsedManifest::from_slice(edited.as_bytes())
                .unwrap_or_else(|e| panic!("{plan}: {e}"));
            parsed
                .verify_authentication(signed.then_some(&key))
                .unwrap_or_else(|e| panic!("{plan}: {e}"));
            assert_eq!(parsed.fields().verification_plan, None, "{plan}");
        }
        // A duplicated key too: v0.8.0's struct decode skipped both.
        let doubled = text.replacen(
            r#""created_at""#,
            r#""verification_plan": 1, "verification_plan": {"method": "read_back", "require_strong": true}, "created_at""#,
            1,
        );
        let parsed = ParsedManifest::from_slice(doubled.as_bytes()).unwrap();
        parsed
            .verify_authentication(signed.then_some(&key))
            .unwrap();
        assert_eq!(parsed.fields().verification_plan, None);
    }
}

/// In 0.4 the field is defined, so a malformed plan is a malformed document,
/// not a newer one.
#[test]
fn a_malformed_plan_in_a_04_manifest_is_malformed() {
    for plan in [json!("garbage"), json!({"method": "x"}), json!(5)] {
        let mut doc: Value = serde_json::from_slice(v04(None).bytes()).unwrap();
        doc["verification_plan"] = plan.clone();
        doc["manifest"]["sha256"] = json!("");
        let auth = serde_json::to_vec(&doc).unwrap();
        doc["manifest"]["sha256"] = json!(sha256_bytes(&auth));
        let err = ParsedManifest::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
        assert!(
            !matches!(err, VtopError::UnsupportedManifestVersion { .. }),
            "{plan}: {err}"
        );
    }
}

/// A later writer's method, or a field added to the plan, still reads and
/// verifies: `method` is a string, not a closed set.
#[test]
fn a_04_plan_with_an_unknown_method_or_field_still_verifies() {
    let mut doc: Value = serde_json::from_slice(v04(None).bytes()).unwrap();
    doc["verification_plan"]["method"] = json!("future_method");
    doc["verification_plan"]["future_field"] = json!([1, 2]);
    doc["manifest"]["sha256"] = json!("");
    let auth = serde_json::to_vec(&doc).unwrap();
    doc["manifest"]["sha256"] = json!(sha256_bytes(&auth));
    let parsed = ParsedManifest::from_slice(&serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
    parsed.verify_authentication(None).unwrap();
    assert_eq!(
        parsed.fields().verification_plan.as_ref().unwrap().method,
        "future_method"
    );
}

/// The committed 0.4 fixtures, written by this release's `vtopctl` (see the
/// fixtures README), verify from their stored bytes: they are the first 0.4
/// documents later readers must keep reading.
#[test]
fn the_committed_release_b_04_manifests_verify_from_their_stored_bytes() {
    let key = fixture_key();
    for (bytes, signed) in [(B04_UNSIGNED, false), (B04_SIGNED, true)] {
        let parsed = ParsedManifest::from_slice(bytes).unwrap();
        assert_eq!(parsed.fields().version, "0.4");
        assert_eq!(
            parsed.fields().verification_plan,
            Some(VerificationPlan {
                method: "read_back".into(),
                require_strong: true,
            })
        );
        parsed
            .verify_authentication(signed.then_some(&key))
            .unwrap();
        // And this build writes them again byte for byte from their fields.
        let rewritten = builder_from(parsed.fields())
            .build_written(
                ManifestFormat::V0_4 {
                    plan: parsed.fields().verification_plan.clone().unwrap(),
                },
                signed.then_some(&key),
            )
            .unwrap();
        assert_eq!(rewritten.bytes(), bytes);
    }
}
