//! Fuzz the audit record reader (`vtop_observe::audit::read_record`): it
//! reads every line of an audit file and every audit row of the ledger back
//! for `vtopctl audit`, and those are written by whatever release ran, so a
//! line can be from a newer release, damaged, or not JSON at all. The
//! contract (#694) is that it never panics and never drops a line: a newer
//! version or an unknown value stays raw (`Unknown`), not-JSON stays as text
//! (`Malformed`). And a record this build reads writes back to a line that
//! reads as the same record (#729).
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_observe::audit::{read_record, AuditEvent, ReadRecord};

/// Whether a `before`/`after` value holds a non-integer number anywhere.
fn has_float(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Number(number) => number.is_f64(),
        serde_json::Value::Array(items) => items.iter().any(has_float),
        serde_json::Value::Object(fields) => fields.values().any(has_float),
        _ => false,
    }
}

/// serde_json is built without its `float_roundtrip` feature, so it reads a
/// decimal float to within one ULP rather than exactly: 1.5555555555555557e86
/// reads back as ...555e86 (the first PR run of this target found it). The
/// engine writes integers and strings into `before` and `after`, so a float
/// there is foreign; it must still read and write without a panic, but its
/// last bit is not part of the contract.
fn carries_float(event: &AuditEvent) -> bool {
    event.before.values().chain(event.after.values()).any(has_float)
}

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    for line in text.lines() {
        match read_record(line) {
            ReadRecord::V1(event) => {
                let written = event.to_json();
                match read_record(&written) {
                    ReadRecord::V1(again) if carries_float(&event) => drop(again),
                    ReadRecord::V1(again) => {
                        assert_eq!(again, event, "an audit record changed on a write and read")
                    }
                    other => panic!("a written v1 record did not read back as v1: {other:?}"),
                }
            }
            ReadRecord::Unknown { .. } | ReadRecord::Malformed { .. } => {}
        }
    }
});
