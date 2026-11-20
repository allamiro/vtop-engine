//! Fuzz the ledger's row formats as they are read back: the prune digest
//! (`ledger-digest-*.jsonl`, `vtop_cli::coverage::parse_digest`), the
//! `BatchRecord` JSON each digest line holds, and the `ProgressMarker` JSON
//! every row carries, including the shapes older releases wrote. A digest
//! sits in a directory an operator or a restore can change, and the coverage
//! audit (`vtopctl ledger audit`) trusts what parses, so no input may panic
//! the parsers or the audit that runs over their rows (#729).
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_cli::coverage::{audit, parse_digest, rotation_unproven, unit_ranges};
use vtop_core::types::ProgressMarker;
use vtop_state::BatchRecord;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(rows) = parse_digest(text, "fuzz") {
        let _ = audit(rows);
    }
    let mut rows = Vec::new();
    for line in text.lines() {
        if let Ok(row) = serde_json::from_str::<BatchRecord>(line) {
            // A row the ledger holds is written back (digests, migrations):
            // what it writes must read again.
            let written = serde_json::to_string(&row).expect("a row serializes");
            serde_json::from_str::<BatchRecord>(&written).expect("a written row reads back");
            let _ = unit_ranges(&row);
            let _ = rotation_unproven(&row);
            rows.push(row);
        }
        if let Ok(marker) = serde_json::from_str::<ProgressMarker>(line) {
            let written = serde_json::to_string(&marker).expect("a marker serializes");
            let again: ProgressMarker =
                serde_json::from_str(&written).expect("a written marker reads back");
            assert_eq!(again, marker, "a marker changed on a write and read");
        }
    }
    if !rows.is_empty() {
        let _ = audit(rows);
    }
});
