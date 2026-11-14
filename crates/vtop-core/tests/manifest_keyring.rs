//! Manifest MAC key rotation (#685): a version 0.4 manifest names the key
//! that signed it in `manifest.mac_key_id`, and a reader verifies it with
//! that key, never by trying every key it holds.
//!
//! Each test states the outcome an operator relies on during a rotation:
//! an archive signed by several keys verifies; a manifest signed by a key
//! the reader was not given, or by a retired one, is unauthenticated (not
//! corrupt) and says which; a manifest written before key ids is verified by
//! the key marked legacy; and no key or MAC ever appears in a message.

use serde_json::{json, Value};
use vtop_core::checksum::sha256_bytes;
use vtop_core::manifest::{
    KeyResult, KeySourceKind, KeyringKey, ManifestBuilder, ManifestFormat, ManifestKeyring,
    ManifestMacKey, ParsedManifest, VerificationPlan, KEY_LABEL_NONE, KEY_LABEL_OTHER,
};
use vtop_core::types::{
    ChecksumAlgorithm, CompressionType, ProgressMarker, SourceType, TelemetryFormat,
};

const KEYRING_Q3: &[u8] = include_bytes!("fixtures/manifests/keyring-0.4-q3.json");
const KEYRING_Q4: &[u8] = include_bytes!("fixtures/manifests/keyring-0.4-q4.json");
const V080_SIGNED: &[u8] = include_bytes!("fixtures/manifests/v0.8.0-signed.json");
const B04_SIGNED: &[u8] = include_bytes!("fixtures/manifests/release-b-0.4-signed.json");

/// The fixtures' non-secret keys: `11` x 32 (the key every earlier signed
/// fixture uses, here under id `q3`) and `22` x 32 (`q4`).
const Q3_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const Q4_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const Q9_HEX: &str = "9999999999999999999999999999999999999999999999999999999999999999";

fn key(hex: &str) -> ManifestMacKey {
    ManifestMacKey::from_hex(hex).unwrap()
}

fn entry(id: &str, hex: &str, legacy: bool) -> KeyringKey {
    KeyringKey {
        id: id.into(),
        key: key(hex),
        source: KeySourceKind::Env,
        legacy,
    }
}

/// A keyring of `keys` (id, hex, legacy) signing with `active`.
fn keyring(active: &str, keys: &[(&str, &str, bool)], retired: &[&str]) -> ManifestKeyring {
    ManifestKeyring::new(
        active,
        keys.iter()
            .map(|(id, hex, legacy)| entry(id, hex, *legacy))
            .collect(),
        retired.iter().map(|r| r.to_string()).collect(),
    )
    .unwrap()
}

fn builder(batch: &str) -> ManifestBuilder {
    ManifestBuilder {
        batch_id: batch.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: "/data/app.log".into(),
        format: TelemetryFormat::Raw,
        compression: CompressionType::None,
        record_count: 3,
        first_timestamp: None,
        last_timestamp: None,
        source_progress: ProgressMarker::File {
            path: "/data/app.log".into(),
            inode: None,
            start_byte: 0,
            end_byte: 30,
            file_size: 30,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        },
        object_uri: format!("file:///archive/{batch}.raw"),
        object_size: 30,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: "ab".repeat(32),
        manifest_uri: format!("file:///archive/{batch}.manifest.json"),
        path_template: "t".into(),
        resolved_prefix: "p".into(),
        upload_backend: "localfs".into(),
        created_at: "2026-11-14T00:00:00Z".into(),
    }
}

fn v04() -> ManifestFormat {
    ManifestFormat::V0_4 {
        plan: VerificationPlan::for_backend("localfs", ChecksumAlgorithm::Sha256, true).unwrap(),
    }
}

/// Written by `keyring`'s active key, as the engine writes it.
fn signed(batch: &str, keyring: &ManifestKeyring) -> Vec<u8> {
    builder(batch)
        .build_signed(v04(), keyring)
        .unwrap()
        .bytes()
        .to_vec()
}

/// Written with `manifest_mac_key_env` (one key, no id) at `format`.
fn signed_without_id(batch: &str, format: ManifestFormat, hex: &str) -> Vec<u8> {
    builder(batch)
        .build_written(format, Some(&key(hex)))
        .unwrap()
        .bytes()
        .to_vec()
}

fn parse(bytes: &[u8]) -> ParsedManifest {
    ParsedManifest::from_slice(bytes).unwrap()
}

/// (label, result, error text) of authenticating `bytes` with `keyring`.
fn check(bytes: &[u8], keyring: &ManifestKeyring) -> (String, KeyResult, String) {
    let auth = parse(bytes).authenticate(keyring);
    let key = auth.key.clone().expect("a MAC check ran");
    let text = auth.result.err().map(|e| e.to_string()).unwrap_or_default();
    (key.key_id, key.result, text)
}

/// The rotation's end state: a keyring holding the old key (verify-only,
/// legacy) and the new one (active).
fn rotated() -> ManifestKeyring {
    keyring("q4", &[("q3", Q3_HEX, true), ("q4", Q4_HEX, false)], &[])
}

/// Words that would tell an operator the bytes are damaged. An unknown or
/// retired key says nothing about the bytes, so its message must not use
/// them.
fn says_corrupt(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    ["corrupt", "tamper", "mismatch", "damaged"]
        .iter()
        .any(|w| text.contains(w))
}

/// The issue's first condition. One archive holds manifests signed before
/// key ids (0.3 and 0.4, no id), under the old key by id, and under the new
/// key by id. The rotated keyring verifies every one, each with the key it
/// names (or the legacy key), and says which.
#[test]
fn a_mixed_key_archive_verifies_each_manifest_with_the_key_it_names() {
    let before_q3 = keyring("q3", &[("q3", Q3_HEX, true)], &[]);
    let archive = [
        (
            signed_without_id("pre-ids-03", ManifestFormat::V0_3, Q3_HEX),
            "q3",
        ),
        (signed_without_id("pre-ids-04", v04(), Q3_HEX), "q3"),
        (signed("by-id-q3", &before_q3), "q3"),
        (signed("by-id-q4", &rotated()), "q4"),
        (KEYRING_Q3.to_vec(), "q3"),
        (KEYRING_Q4.to_vec(), "q4"),
        (V080_SIGNED.to_vec(), "q3"),
        (B04_SIGNED.to_vec(), "q3"),
    ];
    for (bytes, want) in &archive {
        let (label, result, text) = check(bytes, &rotated());
        assert_eq!(
            (label.as_str(), result),
            (*want, KeyResult::Ok),
            "{text}\n{}",
            String::from_utf8_lossy(bytes)
        );
    }
    // And the ids are what the writer recorded.
    assert_eq!(
        parse(&archive[2].0).fields().manifest.mac_key_id.as_deref(),
        Some("q3")
    );
    assert_eq!(
        parse(&archive[3].0).fields().manifest.mac_key_id.as_deref(),
        Some("q4")
    );
    assert_eq!(parse(&archive[1].0).fields().manifest.mac_key_id, None);
}

/// The writer signs with the active key and records its id; a verify-only
/// key never signs. Verification is by the named key only: the q4 manifest
/// fails under a keyring where the id q4 holds another key, even though that
/// keyring also holds the real q4 key under a different id. No search.
#[test]
fn the_writer_signs_with_the_active_key_and_a_reader_never_searches() {
    let written = builder("w").build_signed(v04(), &rotated()).unwrap();
    assert_eq!(written.fields().manifest.mac_key_id.as_deref(), Some("q4"));
    let stored: Value = serde_json::from_slice(written.bytes()).unwrap();
    assert_eq!(stored["manifest"]["mac_key_id"], json!("q4"));

    // The MAC is q4's: a keyring holding only q4 verifies it. Where the id
    // q4 holds another key, it fails, although that keyring holds the real
    // q4 key under another id: the named key is used, never a search.
    let only_q4 = keyring("q4", &[("q4", Q4_HEX, false)], &[]);
    assert_eq!(check(written.bytes(), &only_q4).1, KeyResult::Ok);
    let q4_is_q3s_key = keyring("q4", &[("q4", Q3_HEX, false), ("q5", Q4_HEX, false)], &[]);
    let (label, result, text) = check(written.bytes(), &q4_is_q3s_key);
    assert_eq!(
        (label.as_str(), result),
        ("q4", KeyResult::Failed),
        "{text}"
    );
    assert!(
        text.contains(r#"MAC verification failed for key id "q4""#),
        "{text}"
    );
}

/// The issue's second condition: an id this keyring does not hold is
/// unauthenticated, names the id, says what to do, is not called corrupt,
/// and is labelled `other` (the id from storage is never a label).
#[test]
fn an_unknown_key_id_is_unauthenticated_not_corrupt() {
    let q9 = keyring("q9", &[("q9", Q9_HEX, false)], &[]);
    let bytes = signed("from-elsewhere", &q9);
    let (label, result, text) = check(&bytes, &rotated());
    assert_eq!(label, KEY_LABEL_OTHER);
    assert_eq!(result, KeyResult::UnknownKey);
    for needle in [
        "unauthenticated",
        r#"key id "q9""#,
        "not in this keyring",
        r#""q3", "q4""#,
        "add it to manifest.mac.keys as a verify-only entry",
    ] {
        assert!(text.contains(needle), "{needle}: {text}");
    }
    assert!(!says_corrupt(&text), "{text}");
    // The bytes themselves are intact: the hash still verifies.
    parse(&bytes).verify_self_hash().unwrap();
}

/// The issue's third condition, read the way #685 settles it: a key rotated
/// out in the normal way stays as a verify-only entry, and everything it
/// signed keeps verifying. "Retire" means stop signing, never re-sign.
#[test]
fn a_retired_from_signing_key_still_verifies() {
    let old = keyring("q3", &[("q3", Q3_HEX, true)], &[]);
    let by_old_key = signed("signed-before-the-switch", &old);
    let by_new_key = signed("signed-after-the-switch", &rotated());
    assert_eq!(check(&by_old_key, &rotated()).1, KeyResult::Ok);
    assert_eq!(check(&by_new_key, &rotated()).1, KeyResult::Ok);
    assert_eq!(rotated().verify_only_ids(), ["q3"]);
    assert_eq!(rotated().active_id(), Some("q4"));
}

/// A tombstoned (withdrawn) key fails clearly: unauthenticated, "retired",
/// named, labelled by the configured tombstone id, and not corrupt.
#[test]
fn a_tombstoned_key_fails_clearly() {
    let old = keyring("q3", &[("q3", Q3_HEX, false)], &[]);
    let bytes = signed("signed-with-a-withdrawn-key", &old);
    let withdrawn = keyring("q4", &[("q4", Q4_HEX, false)], &["q3"]);
    let (label, result, text) = check(&bytes, &withdrawn);
    assert_eq!((label.as_str(), result), ("q3", KeyResult::RetiredKey));
    for needle in [
        "unauthenticated",
        r#"key id "q3""#,
        "retired",
        "manifest.mac.retired",
        "withdrawn",
    ] {
        assert!(text.contains(needle), "{needle}: {text}");
    }
    assert!(!text.contains("not in this keyring"), "{text}");
    assert!(!says_corrupt(&text), "{text}");
}

/// A manifest with no key id (every manifest before #685) is verified with
/// the entry marked legacy; with none marked, it is unauthenticated and the
/// message names that cause.
#[test]
fn a_manifest_with_no_id_uses_the_legacy_key() {
    for bytes in [
        signed_without_id("old-03", ManifestFormat::V0_3, Q3_HEX),
        signed_without_id("old-04", v04(), Q3_HEX),
        V080_SIGNED.to_vec(),
    ] {
        let (label, result, text) = check(&bytes, &rotated());
        assert_eq!((label.as_str(), result), ("q3", KeyResult::Ok), "{text}");

        // The legacy key is not the one that signed it: the failure says
        // which key was used and why.
        let wrong_legacy = keyring("q4", &[("q3", Q9_HEX, true), ("q4", Q4_HEX, false)], &[]);
        let (label, result, text) = check(&bytes, &wrong_legacy);
        assert_eq!((label.as_str(), result), ("q3", KeyResult::Failed));
        assert!(
            text.contains(r#"for the legacy key "q3" (the manifest names no key id)"#),
            "{text}"
        );

        let no_legacy = keyring("q4", &[("q3", Q3_HEX, false), ("q4", Q4_HEX, false)], &[]);
        let (label, result, text) = check(&bytes, &no_legacy);
        assert_eq!(label, KEY_LABEL_NONE);
        assert_eq!(result, KeyResult::UnknownKey);
        for needle in ["unauthenticated", "names no key id", "legacy: true"] {
            assert!(text.contains(needle), "{needle}: {text}");
        }
        assert!(!says_corrupt(&text), "{text}");
    }
}

/// The id is authenticated: changing it in the stored bytes fails the hash,
/// and recomputing the unkeyed hash still fails the MAC, now for the key it
/// was changed to.
#[test]
fn changing_the_key_id_fails_the_hash_and_the_mac() {
    let bytes = signed("pinned-id", &rotated());
    let mut doc: Value = serde_json::from_slice(&bytes).unwrap();
    doc["manifest"]["mac_key_id"] = json!("q3");
    let edited = serde_json::to_vec_pretty(&doc).unwrap();
    let auth = parse(&edited).authenticate(&rotated());
    assert!(
        auth.key.is_none(),
        "the hash fails before any key is chosen"
    );
    assert!(
        auth.result
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"),
        "the self-hash covers the id"
    );

    // Recompute the hash the way a tamperer without the key can.
    let mut canonical = doc.clone();
    canonical["manifest"]["sha256"] = json!("");
    canonical["manifest"]["mac"] = json!("");
    doc["manifest"]["sha256"] = json!(sha256_bytes(&serde_json::to_vec(&canonical).unwrap()));
    let rehashed = serde_json::to_vec_pretty(&doc).unwrap();
    parse(&rehashed).verify_self_hash().unwrap();
    let (label, result, text) = check(&rehashed, &rotated());
    assert_eq!(
        (label.as_str(), result),
        ("q3", KeyResult::Failed),
        "{text}"
    );

    // Removing the id makes the legacy key verify it: also refused.
    let mut doc: Value = serde_json::from_slice(&bytes).unwrap();
    doc["manifest"]
        .as_object_mut()
        .unwrap()
        .remove("mac_key_id");
    let mut canonical = doc.clone();
    canonical["manifest"]["sha256"] = json!("");
    canonical["manifest"]["mac"] = json!("");
    doc["manifest"]["sha256"] = json!(sha256_bytes(&serde_json::to_vec(&canonical).unwrap()));
    let stripped = serde_json::to_vec_pretty(&doc).unwrap();
    assert_eq!(check(&stripped, &rotated()).1, KeyResult::Failed);
}

/// A key id cannot ride in a 0.3 manifest. The writer refuses it, and the
/// legacy reader ignores an id added to one (no legacy hash covers it, as
/// v0.8.0 ignores it): verification of 0.3 stays as it was, with the legacy
/// key, so the added id cannot choose the key.
#[test]
fn a_key_id_in_a_03_manifest_is_refused_and_ignored() {
    let err = builder("no-id-in-03")
        .build_signed(ManifestFormat::V0_3, &rotated())
        .unwrap_err()
        .to_string();
    assert!(err.contains("version 0.4"), "{err}");

    let bytes = signed_without_id("legacy", ManifestFormat::V0_3, Q3_HEX);
    let mut doc: Value = serde_json::from_slice(&bytes).unwrap();
    doc["manifest"]["mac_key_id"] = json!("q4");
    let with_id = serde_json::to_vec_pretty(&doc).unwrap();
    let parsed = parse(&with_id);
    assert_eq!(parsed.fields().manifest.mac_key_id, None);
    assert_eq!(
        check(&with_id, &rotated()),
        ("q3".into(), KeyResult::Ok, String::new()),
        "verified with the legacy key, as before key ids"
    );
    // ... and an encoder never writes one into a legacy document.
    let mut fields = parsed.fields().clone();
    fields.manifest.mac_key_id = Some("q4".into());
    assert!(fields.to_json_bytes().is_err());
    assert!(fields.verify_authentication(Some(&key(Q3_HEX))).is_err());
}

/// A malformed id in a 0.4 manifest is a malformed document; an id that is
/// well formed JSON but no configured id (however long or odd) is an unknown
/// key, shown bounded and escaped, never raw.
#[test]
fn a_hostile_key_id_is_bounded_in_the_message_and_never_a_label() {
    let bytes = signed("hostile", &rotated());
    let mut doc: Value = serde_json::from_slice(&bytes).unwrap();
    doc["manifest"]["mac_key_id"] = json!(7);
    assert!(ParsedManifest::from_slice(&serde_json::to_vec(&doc).unwrap()).is_err());

    let hostile = format!("evil\n{}", "x".repeat(10_000));
    let mut doc: Value = serde_json::from_slice(&bytes).unwrap();
    doc["manifest"]["mac_key_id"] = json!(hostile);
    let mut canonical = doc.clone();
    canonical["manifest"]["sha256"] = json!("");
    canonical["manifest"]["mac"] = json!("");
    doc["manifest"]["sha256"] = json!(sha256_bytes(&serde_json::to_vec(&canonical).unwrap()));
    let (label, result, text) = check(&serde_json::to_vec(&doc).unwrap(), &rotated());
    assert_eq!((label.as_str(), result), ("other", KeyResult::UnknownKey));
    assert!(!text.contains('\n'), "escaped: {text}");
    assert!(text.len() < 600, "bounded: {} bytes", text.len());
}

/// A v0.8.0 reader during a rotation, as a test-local copy of its single-key
/// check: it ignores the id and verifies with its one key. It passes the
/// manifests signed with the key it holds and fails the others, which is why
/// every reader must run the keyring release before the active key changes.
#[test]
fn a_v080_reader_verifies_only_what_its_one_key_signed() {
    fn v080_check(bytes: &[u8], key_hex: &str) -> bool {
        // v0.8.0 verifies a 0.4 manifest over its canonical form with one
        // key and no notion of an id.
        let mut doc: Value = serde_json::from_slice(bytes).unwrap();
        let mac = doc["manifest"]["mac"].as_str().unwrap().to_owned();
        doc["manifest"]["sha256"] = json!("");
        doc["manifest"]["mac"] = json!("");
        let canonical = serde_json::to_vec(&doc).unwrap();
        let mut k = [0u8; 32];
        hex::decode_to_slice(key_hex, &mut k).unwrap();
        blake3::keyed_hash(&k, &canonical).to_hex().as_str() == mac
    }
    let by_q3 = signed("q3", &keyring("q3", &[("q3", Q3_HEX, true)], &[]));
    let by_q4 = signed("q4", &rotated());
    assert!(v080_check(&by_q3, Q3_HEX));
    assert!(
        !v080_check(&by_q4, Q3_HEX),
        "an old reader fails new-key manifests"
    );
    assert!(v080_check(&by_q4, Q4_HEX));
    assert!(!v080_check(&by_q3, Q4_HEX));
    // The keyring release reads both.
    assert_eq!(check(&by_q3, &rotated()).1, KeyResult::Ok);
    assert_eq!(check(&by_q4, &rotated()).1, KeyResult::Ok);
}

/// `manifest_mac_key_env` alone behaves exactly as before #685: it signs
/// with no id, and its messages are unchanged.
#[test]
fn the_single_key_shorthand_behaves_as_before() {
    let single = ManifestKeyring::single(key(Q3_HEX));
    let written = builder("single").build_signed(v04(), &single).unwrap();
    assert_eq!(written.fields().manifest.mac_key_id, None);
    assert_eq!(
        written.bytes(),
        signed_without_id("single", v04(), Q3_HEX),
        "byte for byte the pre-#685 0.4 writer"
    );
    assert_eq!(
        check(written.bytes(), &single),
        (KEY_LABEL_NONE.into(), KeyResult::Ok, String::new())
    );
    let unsigned = builder("unsigned").build_written(v04(), None).unwrap();
    let (_, result, text) = check(unsigned.bytes(), &single);
    assert_eq!(result, KeyResult::Failed);
    assert_eq!(
        text,
        "manifest error: manifest MAC is required because manifest_mac_key_env is configured"
    );
    // A manifest that names a key id, signed with this one key, verifies,
    // as on v0.8.0 (review of #703): the shorthand holds one key and ignores
    // the id, so an engine still on `manifest_mac_key_env` mid-rollout keeps
    // verifying what keyring engines sign with the same key. One key is not
    // a search.
    let same_key = keyring("q3", &[("q3", Q3_HEX, true)], &[]);
    let named_by_its_key = signed("named-q3", &same_key);
    assert_eq!(
        check(&named_by_its_key, &single),
        (KEY_LABEL_NONE.into(), KeyResult::Ok, String::new())
    );
    assert!(parse(&named_by_its_key)
        .verify_authentication(Some(&key(Q3_HEX)))
        .is_ok());
    // Signed with another key: unauthenticated, naming the id and saying
    // the one key does not verify it.
    let named = signed("named", &rotated());
    let (label, result, text) = check(&named, &single);
    assert_eq!(
        (label.as_str(), result),
        (KEY_LABEL_OTHER, KeyResult::UnknownKey)
    );
    for needle in [
        "unauthenticated",
        r#"key id "q4""#,
        "manifest_mac_key_env",
        "manifest.mac",
    ] {
        assert!(text.contains(needle), "{needle}: {text}");
    }
    // No key: no MAC check, as before.
    let none = parse(&named).authenticate(&ManifestKeyring::default());
    assert!(none.key.is_none() && none.result.is_ok());
}

/// The keyring refuses the shapes config load refuses too, so a keyring
/// built in code cannot skip them.
#[test]
fn the_keyring_refuses_inconsistent_shapes() {
    let cases: [(&str, Vec<KeyringKey>, Vec<String>, &str); 7] = [
        ("q5", vec![entry("q4", Q4_HEX, false)], vec![], "not one of"),
        (
            "q4",
            vec![entry("q4", Q4_HEX, false), entry("q4", Q3_HEX, false)],
            vec![],
            "more than once",
        ),
        (
            "q4",
            vec![entry("q3", Q3_HEX, true), entry("q4", Q4_HEX, true)],
            vec![],
            "at most one key",
        ),
        (
            "q4",
            vec![entry("q4", Q4_HEX, false)],
            vec!["q4".into()],
            "more than once",
        ),
        (
            "q3",
            vec![entry("q4", Q4_HEX, false)],
            vec!["q3".into()],
            "is retired",
        ),
        (
            "bad id",
            vec![entry("bad id", Q4_HEX, false)],
            vec![],
            "may only contain",
        ),
        (
            "none",
            vec![entry("none", Q4_HEX, false)],
            vec![],
            "reserved",
        ),
    ];
    for (active, keys, retired, needle) in cases {
        let err = ManifestKeyring::new(active, keys, retired)
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "{needle}: {err}");
    }
}

/// Redaction: no key and no MAC value appears in any message of the
/// outcomes table, in the keyring's Debug, or in its startup summary.
#[test]
fn no_key_or_mac_appears_in_any_message() {
    let ring = keyring(
        "q4",
        &[("q3", Q3_HEX, true), ("q4", Q4_HEX, false)],
        &["q1"],
    );
    let q9 = keyring("q9", &[("q9", Q9_HEX, false)], &[]);
    let q1 = keyring("q1", &[("q1", Q9_HEX, false)], &[]);
    let wrong = keyring("q4", &[("q3", Q9_HEX, true), ("q4", Q9_HEX, false)], &[]);
    let no_legacy = keyring("q4", &[("q4", Q4_HEX, false)], &[]);
    let unsigned = builder("u").build_written(v04(), None).unwrap();
    let rows: Vec<(Vec<u8>, &ManifestKeyring)> = vec![
        (signed("ok", &ring), &ring),
        (signed("wrong", &ring), &wrong),
        (signed("unknown", &q9), &ring),
        (signed("retired", &q1), &ring),
        (signed_without_id("legacy", v04(), Q3_HEX), &wrong),
        (signed_without_id("no-legacy", v04(), Q3_HEX), &no_legacy),
        (unsigned.bytes().to_vec(), &ring),
    ];
    let mut texts = vec![
        format!("{ring:?}"),
        ring.summary(),
        format!("{:?}", key(Q3_HEX)),
    ];
    for (bytes, ring) in &rows {
        let auth = parse(bytes).authenticate(ring);
        texts.push(format!("{auth:?}"));
        if let Err(e) = auth.result {
            texts.push(e.to_string());
        }
    }
    let macs: Vec<String> = rows
        .iter()
        .filter_map(|(b, _)| parse(b).fields().manifest.mac.clone())
        .collect();
    for text in &texts {
        for secret in [Q3_HEX, Q4_HEX, Q9_HEX] {
            assert!(!text.contains(secret), "a key leaked: {text}");
        }
        for mac in &macs {
            assert!(!text.contains(mac.as_str()), "a MAC leaked: {text}");
        }
    }
    assert_eq!(
        ring.summary(),
        r#"manifest MAC keyring: active key q4 (env); verify-only: q3 (env, legacy); retired: q1"#
    );
}

/// The committed fixtures were written by this release's `vtopctl` with a
/// keyring (see the fixtures README). Each names its key, verifies with it,
/// and fails, as unauthenticated, under a keyring that lacks that key.
#[test]
fn the_committed_keyring_fixtures_verify_under_their_key_ids() {
    for (bytes, id) in [(KEYRING_Q3, "q3"), (KEYRING_Q4, "q4")] {
        let parsed = parse(bytes);
        assert_eq!(parsed.fields().version, "0.4");
        assert_eq!(parsed.fields().manifest.mac_key_id.as_deref(), Some(id));
        assert_eq!(
            check(bytes, &rotated()),
            (id.to_string(), KeyResult::Ok, String::new())
        );
    }
    let only_q4 = keyring("q4", &[("q4", Q4_HEX, false)], &[]);
    assert_eq!(check(KEYRING_Q3, &only_q4).1, KeyResult::UnknownKey);
    assert_eq!(check(KEYRING_Q4, &only_q4).1, KeyResult::Ok);
}
