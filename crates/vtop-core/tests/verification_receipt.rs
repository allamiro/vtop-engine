//! Verification receipts (#671 release B, D7): a small JSON object written
//! beside a manifest once its batch is VERIFIED, saying how the object was
//! checked. It is hashed and MAC'd the way a version 0.4 manifest is (the
//! stored document with `receipt.sha256` and `receipt.mac` blanked, keys
//! sorted, compact), names the key that signed it (#685), and binds the
//! manifest's self-hash, which the manifest's own MAC covers, so a receipt
//! cannot be moved to another manifest.

use vtop_core::checksum::sha256_bytes;
use vtop_core::manifest::{
    KeyResult, KeySourceKind, KeyringKey, ManifestKeyring, ManifestMacKey, ParsedReceipt,
    ReceiptBuilder, MAX_RECEIPT_BYTES, RECEIPT_ORIGIN_ENGINE, RECEIPT_VERSION,
};

fn key(byte: &str) -> ManifestMacKey {
    ManifestMacKey::from_hex(&byte.repeat(32)).unwrap()
}

fn builder() -> ReceiptBuilder {
    ReceiptBuilder {
        batch_id: "vtop-20261114T120000Z-app-0-27".into(),
        manifest_uri: "s3://telemetry-data/x/vtop-20261114T120000Z-app-0-27.manifest.json".into(),
        manifest_sha256: "ab".repeat(32),
        manifest_version_id: Some("v1".into()),
        object_uri: "s3://telemetry-data/x/app-0-27-1a2b.raw.gz".into(),
        object_size_bytes: 924_822,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: "cd".repeat(32),
        object_version_id: Some("v7".into()),
        class: "strong".into(),
        method: "read_back".into(),
        verified_at: "2026-11-14T12:00:01Z".into(),
        verified_by: "vtop-test".into(),
        origin: RECEIPT_ORIGIN_ENGINE.into(),
    }
}

fn keyring(active: &str, keys: &[(&str, &str, bool)], retired: &[&str]) -> ManifestKeyring {
    ManifestKeyring::new(
        active,
        keys.iter()
            .map(|(id, byte, legacy)| KeyringKey {
                id: (*id).into(),
                key: key(byte),
                source: KeySourceKind::Env,
                legacy: *legacy,
            })
            .collect(),
        retired.iter().map(|r| (*r).to_owned()).collect(),
    )
    .unwrap()
}

/// The canonical form, computed here independently of the library: the
/// stored document, `receipt.sha256` (and `receipt.mac` where present)
/// blanked, keys sorted at every depth, compact.
fn canonical(bytes: &[u8]) -> Vec<u8> {
    let mut doc: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let block = doc["receipt"].as_object_mut().unwrap();
    block.insert("sha256".into(), "".into());
    if block.contains_key("mac") {
        block.insert("mac".into(), "".into());
    }
    doc.sort_all_objects();
    serde_json::to_vec(&doc).unwrap()
}

#[test]
fn a_receipt_round_trips_and_authenticates_with_and_without_a_key() {
    // Unsigned.
    let written = builder().build_signed(&ManifestKeyring::default()).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(written.bytes()).unwrap();
    assert_eq!(doc["protocol"], "VTOP");
    assert_eq!(doc["receipt_version"], RECEIPT_VERSION);
    assert_eq!(doc["verification"]["class"], "strong");
    assert_eq!(doc["verification"]["method"], "read_back");
    assert_eq!(doc["origin"], "engine");
    assert_eq!(doc["manifest"]["version_id"], "v1");
    assert_eq!(doc["object"]["version_id"], "v7");
    assert!(doc["receipt"].get("mac").is_none(), "{doc}");
    assert_eq!(
        doc["receipt"]["sha256"],
        sha256_bytes(&canonical(written.bytes())),
        "the hash covers the canonical stored document"
    );
    let parsed = ParsedReceipt::from_slice(written.bytes()).unwrap();
    parsed.verify_self_hash().unwrap();
    assert!(parsed.authenticate(&ManifestKeyring::default()).is_ok());
    assert_eq!(parsed.fields(), written.fields());

    // Signed with the manifest_mac_key_env key: a MAC and no key id.
    let k = key("5a");
    let single = ManifestKeyring::single(k.clone());
    let signed = builder().build_signed(&single).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(signed.bytes()).unwrap();
    assert_eq!(doc["receipt"]["mac"].as_str().map(str::len), Some(64));
    assert!(doc["receipt"].get("mac_key_id").is_none(), "{doc}");
    assert_eq!(
        doc["receipt"]["mac"],
        blake3::keyed_hash(&[0x5a; 32], &canonical(signed.bytes()))
            .to_hex()
            .to_string(),
        "the MAC is keyed BLAKE3 over the same canonical form"
    );
    let parsed = ParsedReceipt::from_slice(signed.bytes()).unwrap();
    assert!(parsed.authenticate(&single).is_ok());
    assert!(
        !parsed
            .authenticate(&ManifestKeyring::single(key("5b")))
            .is_ok(),
        "the wrong key fails"
    );
    // An unsigned receipt where a key is configured is refused, as an
    // unsigned manifest is.
    let unsigned = ParsedReceipt::from_slice(written.bytes()).unwrap();
    let refused = unsigned.authenticate(&single);
    assert_eq!(refused.key.map(|k| k.result), Some(KeyResult::Failed));
}

#[test]
fn a_tampered_receipt_fails_its_hash_and_a_rehashed_one_fails_its_mac() {
    let k = key("6a");
    let single = ManifestKeyring::single(k);
    let signed = builder().build_signed(&single).unwrap();
    for (path, value) in [
        (
            vec!["verification", "class"],
            serde_json::json!("size_only"),
        ),
        (
            vec!["verification", "method"],
            serde_json::json!("store_checksum"),
        ),
        (
            vec!["manifest", "sha256"],
            serde_json::json!("ef".repeat(32)),
        ),
        (vec!["origin"], serde_json::json!("recovery")),
        (
            vec!["verified_at"],
            serde_json::json!("2030-01-01T00:00:00Z"),
        ),
    ] {
        let mut doc: serde_json::Value = serde_json::from_slice(signed.bytes()).unwrap();
        let mut slot = &mut doc;
        for p in &path {
            slot = &mut slot[*p];
        }
        *slot = value;
        let edited = serde_json::to_vec_pretty(&doc).unwrap();
        let parsed = ParsedReceipt::from_slice(&edited).unwrap();
        assert!(parsed.verify_self_hash().is_err(), "{path:?}: hash");
        assert!(!parsed.authenticate(&single).is_ok(), "{path:?}");
        // A forger who recomputes the unkeyed hash still fails the MAC.
        doc["receipt"]["sha256"] = serde_json::json!(sha256_bytes(&canonical(&edited)));
        let rehashed = serde_json::to_vec_pretty(&doc).unwrap();
        let parsed = ParsedReceipt::from_slice(&rehashed).unwrap();
        parsed.verify_self_hash().unwrap();
        let outcome = parsed.authenticate(&single);
        assert_eq!(
            outcome.key.as_ref().map(|k| k.result),
            Some(KeyResult::Failed),
            "{path:?}"
        );
        assert!(
            outcome
                .result
                .unwrap_err()
                .to_string()
                .contains("receipt MAC"),
            "{path:?}: the message names the receipt"
        );
    }
}

/// #685: a receipt names the key that signed it and is verified with that
/// key, never by trying every key. An id the keyring does not hold, or a
/// retired one, is unauthenticated, not corrupt.
#[test]
fn a_receipt_names_its_key_id_and_verifies_with_that_key_only() {
    let q4 = keyring("q4", &[("q3", "3c", true), ("q4", "4d", false)], &[]);
    let signed = builder().build_signed(&q4).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(signed.bytes()).unwrap();
    assert_eq!(doc["receipt"]["mac_key_id"], "q4");
    let parsed = ParsedReceipt::from_slice(signed.bytes()).unwrap();
    let ok = parsed.authenticate(&q4);
    assert!(ok.is_ok());
    assert_eq!(ok.key.unwrap().key_id, "q4");

    // The same receipt under a keyring where q4 is verify-only still
    // verifies with q4.
    let rotated = keyring("q5", &[("q4", "4d", false), ("q5", "5e", false)], &[]);
    assert!(parsed.authenticate(&rotated).is_ok());

    // A keyring that holds the bytes of q4 under another id does not search:
    // the receipt names q4, which it does not hold.
    let other = keyring("q9", &[("q9", "4d", true)], &[]);
    let unknown = parsed.authenticate(&other);
    assert_eq!(
        unknown.key.as_ref().map(|k| (k.key_id.as_str(), k.result)),
        Some(("other", KeyResult::UnknownKey))
    );
    assert!(unknown
        .result
        .unwrap_err()
        .to_string()
        .contains("receipt is unauthenticated"));

    let retired = keyring("q5", &[("q5", "5e", false)], &["q4"]);
    let outcome = parsed.authenticate(&retired);
    assert_eq!(
        outcome.key.map(|k| k.result),
        Some(KeyResult::RetiredKey),
        "a retired id is reported as retired"
    );

    // The key id is covered by the hash and the MAC: pointing it at another
    // held key fails.
    let mut doc = doc;
    doc["receipt"]["mac_key_id"] = serde_json::json!("q3");
    let moved = ParsedReceipt::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap();
    assert!(moved.verify_self_hash().is_err());
}

#[test]
fn a_receipt_refuses_duplicate_keys_fractions_oversize_and_says_when_it_is_newer() {
    let written = builder().build_signed(&ManifestKeyring::default()).unwrap();
    let text = String::from_utf8(written.bytes().to_vec()).unwrap();

    let duplicate = text.replacen("\"origin\"", "\"origin\": \"recovery\",\n  \"origin\"", 1);
    let e = ParsedReceipt::from_slice(duplicate.as_bytes()).unwrap_err();
    assert!(e.to_string().contains("duplicate key"), "{e}");
    assert!(e.to_string().contains("receipt"), "{e}");

    let fraction = text.replacen("924822", "924822.0", 1);
    let e = ParsedReceipt::from_slice(fraction.as_bytes()).unwrap_err();
    assert!(e.to_string().contains("integer"), "{e}");

    let big = vec![b' '; MAX_RECEIPT_BYTES + 1];
    let e = ParsedReceipt::from_slice(&big).unwrap_err();
    assert!(e.to_string().contains("limit"), "{e}");

    let mut doc: serde_json::Value = serde_json::from_slice(written.bytes()).unwrap();
    doc["receipt_version"] = serde_json::json!("2");
    let e = ParsedReceipt::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
    assert!(
        e.to_string().contains("newer than this build reads"),
        "a newer receipt is not called corrupt: {e}"
    );
    doc["receipt_version"] = serde_json::json!("1x");
    let e = ParsedReceipt::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
    assert!(e.to_string().contains("malformed"), "{e}");
    doc["receipt_version"] = serde_json::json!(RECEIPT_VERSION);
    doc["protocol"] = serde_json::json!("OTHER");
    assert!(ParsedReceipt::from_slice(&serde_json::to_vec(&doc).unwrap()).is_err());

    // A field a later release adds is kept and authenticated, not dropped.
    let mut doc: serde_json::Value = serde_json::from_slice(written.bytes()).unwrap();
    doc["later_field"] = serde_json::json!({"x": 1});
    let edited = serde_json::to_vec(&doc).unwrap();
    doc["receipt"]["sha256"] = serde_json::json!(sha256_bytes(&canonical(&edited)));
    let parsed = ParsedReceipt::from_slice(&serde_json::to_vec(&doc).unwrap()).unwrap();
    parsed.verify_self_hash().unwrap();
}

/// Redaction (vtop-observability): a receipt carries no key material, and
/// never a URL with a query string or user info, which could carry a signed
/// request's credentials. Such a URI is refused rather than written.
#[test]
fn a_receipt_carries_no_key_and_refuses_a_uri_with_a_query_or_user_info() {
    let hex = "7e".repeat(32);
    let single = ManifestKeyring::single(ManifestMacKey::from_hex(&hex).unwrap());
    let signed = builder().build_signed(&single).unwrap();
    let text = String::from_utf8(signed.bytes().to_vec()).unwrap();
    assert!(!text.contains(&hex), "no key material in the receipt");
    for bad in [
        "s3://telemetry-data/x/o.raw.gz?X-Amz-Signature=abc",
        "https://user:secret@store.example/x/o.raw.gz",
        "s3://telemetry-data/x/o.raw.gz#frag",
    ] {
        let mut b = builder();
        b.object_uri = bad.into();
        let e = b.build_signed(&single).unwrap_err().to_string();
        assert!(
            !e.contains("secret") && !e.contains("X-Amz-Signature"),
            "{e}"
        );
        let mut b = builder();
        b.manifest_uri = bad.into();
        assert!(b.build_signed(&single).is_err(), "{bad}");
    }
}

/// The receipt this build's `vtopctl` wrote (fixtures README): the first
/// receipt later readers must keep reading, byte for byte.
#[test]
fn the_committed_release_b_receipt_verifies_from_its_stored_bytes() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/manifests");
    // The fixture key every signed fixture uses (fixtures README).
    let k = key("11");
    for (name, keyring) in [
        (
            "release-b-receipt-unsigned.json",
            ManifestKeyring::default(),
        ),
        ("release-b-receipt-signed.json", ManifestKeyring::single(k)),
    ] {
        let bytes = std::fs::read(dir.join(name)).unwrap();
        let parsed = ParsedReceipt::from_slice(&bytes).unwrap();
        assert!(
            parsed.authenticate(&keyring).is_ok(),
            "{name}: {:?}",
            parsed.authenticate(&keyring).result
        );
        assert_eq!(parsed.fields().receipt_version, RECEIPT_VERSION, "{name}");
        assert_eq!(parsed.fields().origin, RECEIPT_ORIGIN_ENGINE, "{name}");
        // The manifest it names is the committed release-B manifest of the
        // same run, and it binds that manifest's self-hash.
        let manifest_name = name.replace("receipt", "receipted-0.4");
        let manifest = vtop_core::manifest::ParsedManifest::from_slice(
            &std::fs::read(dir.join(manifest_name)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            parsed.fields().manifest.sha256,
            manifest.fields().manifest.sha256,
            "{name}"
        );
        assert_eq!(parsed.fields().batch_id, manifest.fields().batch_id);
    }
}
