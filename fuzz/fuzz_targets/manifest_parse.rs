//! Fuzz manifest deserialization: a `VtopManifest` is read back from an object
//! store as JSON during verification and replay, so its parser sees bytes that a
//! tampering or corrupting party may control. Deserializing arbitrary bytes must
//! return `Err`, never panic. And when a byte string DOES parse into a manifest,
//! the self-hash / re-serialization helpers that run next must also not panic —
//! those are the operations the verify path actually performs.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_core::manifest::VtopManifest;

fuzz_target!(|data: &[u8]| {
    if let Ok(manifest) = serde_json::from_slice::<VtopManifest>(data) {
        // A parsed-but-hostile manifest still flows through these on the verify
        // and replay paths; none may panic on it.
        let _ = manifest.self_hash();
        let _ = manifest.verify_self_hash();
        let _ = manifest.to_json_bytes();
    }
});
