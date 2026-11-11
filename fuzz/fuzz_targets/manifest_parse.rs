//! Fuzz manifest deserialization: a manifest is read back from an object store
//! as JSON during verification and replay, so its parser sees bytes that a
//! tampering or corrupting party may control. Every reader goes through
//! `ParsedManifest::from_slice`, which checks `protocol` and `version` first and
//! then, for a 0.4 document, parses it strictly (no duplicate keys, integer
//! numbers only) and canonicalizes it (#671). Parsing arbitrary bytes must
//! return `Err`, never panic, and neither may the self-hash and MAC checks that
//! run on anything that parses. The legacy decode-and-re-encode helpers are
//! still exercised on anything that decodes into a `VtopManifest`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_core::manifest::{ParsedManifest, VtopManifest};

fuzz_target!(|data: &[u8]| {
    // Read on a newer-version claim, before anything is authenticated.
    let _ = vtop_core::manifest::claimed_binding(data);
    if let Ok(stored) = ParsedManifest::from_slice(data) {
        let _ = stored.verify_self_hash();
        let _ = stored.verify_authentication(None);
        let key = vtop_core::manifest::ManifestMacKey::from_hex(&"11".repeat(32)).unwrap();
        let _ = stored.verify_authentication(Some(&key));
    }
    if let Ok(manifest) = serde_json::from_slice::<VtopManifest>(data) {
        // A parsed-but-hostile manifest still flows through these on the verify
        // and replay paths; none may panic on it.
        let _ = manifest.self_hash();
        let _ = manifest.verify_self_hash();
        let _ = manifest.to_json_bytes();
    }
});
