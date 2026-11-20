//! Harness code the fuzz targets share (#729).
//!
//! Most of the binary formats this crate fuzzes carry a BLAKE3 checksum over
//! their own bytes. A fuzzer that mutates one byte almost always breaks it,
//! so a target that only fed raw input would spend its night proving the
//! checksum check works. [`with_frame_checksum`] repairs the checksum, so the
//! decoders behind it see hostile bytes too; every target feeds the raw input
//! as well, so the check itself stays covered.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// `bytes` with the checksum at `[checksum_offset, header_len)` replaced by
/// BLAKE3 over the header before the checksum and everything after the
/// header: the layout VTPW (vtop-protocol) and VTPM (vtop-meta transport)
/// frames share. Input shorter than a header is returned unchanged.
pub fn with_frame_checksum(bytes: &[u8], checksum_offset: usize, header_len: usize) -> Vec<u8> {
    let mut out = bytes.to_vec();
    if out.len() >= header_len {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&out[..checksum_offset]);
        hasher.update(&out[header_len..]);
        out[checksum_offset..header_len].copy_from_slice(hasher.finalize().as_bytes());
    }
    out
}

/// Drive a future that never waits on anything external (an async reader
/// over a byte slice) to completion. `None` if it returns `Pending`, which
/// such a future does not do; the harness treats that as "no result" rather
/// than spinning.
pub fn run_ready<F: Future>(future: F) -> Option<F::Output> {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..1024 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return Some(output);
        }
    }
    None
}

/// The first byte, as a selector, and the rest.
pub fn split_selector(data: &[u8]) -> Option<(u8, &[u8])> {
    data.split_first().map(|(first, rest)| (*first, rest))
}
