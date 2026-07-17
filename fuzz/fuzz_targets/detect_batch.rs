//! Fuzz `detect_batch`: the batch-level format vote over many records. It calls
//! `detect_record` per record and picks a winner, so it must inherit the
//! never-panic contract even when the records disagree, are empty, or number in
//! the thousands. The fuzzer splits the raw input on newlines to synthesise the
//! record vector.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_core::detect::detect_batch;

fuzz_target!(|data: &[u8]| {
    // Newlines partition the input into records; an all-newline or empty input
    // yields empty/zero-length records, which the classifier must also tolerate.
    let records: Vec<Vec<u8>> = data.split(|&b| b == b'\n').map(|r| r.to_vec()).collect();
    let _ = detect_batch(&records);
});
