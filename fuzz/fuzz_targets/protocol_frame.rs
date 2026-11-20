//! Fuzz the VTPW frame decoder (`vtop-protocol`): every byte a producer, a
//! consumer or a replica peer sends a broker goes through `decode_frame` or
//! `read_frame`, before anything about the sender is trusted. The contract
//! is that no input panics, and that a frame that decodes re-encodes to bytes
//! that decode to the same frame (#729).
//!
//! The input is decoded as sent, then again with its checksum repaired, so
//! the per-kind message decoders behind the checksum see hostile payloads.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_fuzz::{run_ready, with_frame_checksum};
use vtop_protocol::{decode_frame, encode_frame, read_frame, ProtocolLimits, HEADER_LEN};

/// The checksum's place in the 64-byte header (`CHECKSUM_OFFSET` in the crate).
const CHECKSUM_OFFSET: usize = 32;

fn check(bytes: &[u8]) {
    let limits = ProtocolLimits::default();
    if let Ok(frame) = decode_frame(bytes, limits) {
        // The encoder may refuse what the decoder accepted (it checks limits
        // the decoder does not need to); what it does encode must read back
        // as the same frame.
        if let Ok(encoded) = encode_frame(&frame, limits) {
            let again = decode_frame(&encoded, limits).expect("a re-encoded frame decodes");
            assert_eq!(again, frame, "decode(encode(frame)) != frame");
        }
    }
    // The streaming reader has its own header path (one byte first, then the
    // rest) and allocates the payload from the declared length.
    let mut reader = bytes;
    let _ = run_ready(read_frame(&mut reader, limits));
}

fuzz_target!(|data: &[u8]| {
    check(data);
    if data.len() >= HEADER_LEN {
        check(&with_frame_checksum(data, CHECKSUM_OFFSET, HEADER_LEN));
    }
});
