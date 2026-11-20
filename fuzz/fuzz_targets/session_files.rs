//! Fuzz the state files a run resumes from: the multipart upload session
//! (`vtop_upload::multipart::MultipartSession`, the JSON a resumed upload
//! reads to know which parts are already stored) and the scrub cursor
//! (`vtop_cli::scrub::parse_cursor`, where an interrupted scrub pass picks
//! up). Both are files in the work directory that a crash can tear and an
//! operator can edit, so no content may panic their readers, and a session
//! that reads must write back to one that reads the same (#729).
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_cli::scrub::parse_cursor;
use vtop_upload::multipart::MultipartSession;

fuzz_target!(|data: &[u8]| {
    if let Ok(session) = serde_json::from_slice::<MultipartSession>(data) {
        let written = serde_json::to_vec(&session).expect("a session serializes");
        let again: MultipartSession =
            serde_json::from_slice(&written).expect("a written session reads back");
        assert_eq!(again, session, "a session changed on a write and read");
    }
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = parse_cursor(text, "fuzz");
    }
});
