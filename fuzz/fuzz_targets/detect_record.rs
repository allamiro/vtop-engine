//! Fuzz `detect_record`: it classifies UNTRUSTED input bytes into a
//! `TelemetryFormat`. The contract this target enforces is simply that it never
//! panics — no slicing past a boundary, no UTF-8 unwrap, no arithmetic overflow
//! — on ANY byte string. A classifier that can be crashed by a crafted record
//! is a denial-of-service surface, since records come straight off Kafka / files
//! / syslog.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_core::detect::detect_record;

fuzz_target!(|data: &[u8]| {
    // The only assertion is "does not panic". The returned format is
    // deliberately not checked: any classification of arbitrary bytes is valid.
    let _ = detect_record(data);
});
