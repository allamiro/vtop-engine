//! Known-answer tests (KATs) for the checksum implementations.
//!
//! Why this file exists: the unit tests in `checksum.rs` compare
//! `digest_bytes(Sha256, x)` against `sha256_bytes(x)` — **both sides call the
//! same `sha2` crate**. If that crate's output ever changed, or the algorithm
//! were swapped, both sides would change together and the tests would still
//! pass. That gap surfaced during the sha2 0.10 -> 0.11 bump: CI was green, but
//! green meant nothing, and the digest had to be checked by hand.
//!
//! The checksum is the heart of VERIFIED. A wrong digest would be silently
//! accepted and is the worst failure this system can have, so these vectors are
//! **hardcoded literals from the published specs** and must never be derived
//! from the implementation.
//!
//! Sources:
//! * SHA-256 — NIST FIPS 180-4 / RFC 6234 test vectors.
//! * BLAKE3  — official BLAKE3 reference test vectors
//!   (<https://github.com/BLAKE3-team/BLAKE3/blob/master/test_vectors/test_vectors.json>),
//!   whose inputs are the repeating byte pattern 0,1,2,...,250,0,1,... of a
//!   given length.

use vtop_core::checksum::{
    blake3_bytes, blake3_file, digest_bytes, digest_file, sha256_bytes, sha256_file,
};
use vtop_core::types::ChecksumAlgorithm;

// ---------------------------------------------------------------------------
// SHA-256 (NIST FIPS 180-4 / RFC 6234)
// ---------------------------------------------------------------------------

const SHA256_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
/// "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq" (RFC 6234 §8.5)
const SHA256_448BIT: &str = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
/// One million repetitions of 'a' (FIPS 180-4). Exercises multi-block streaming.
const SHA256_MILLION_A: &str = "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0";

#[test]
fn sha256_matches_published_vectors() {
    assert_eq!(sha256_bytes(b""), SHA256_EMPTY, "SHA-256 of empty input");
    assert_eq!(sha256_bytes(b"abc"), SHA256_ABC, "SHA-256 of \"abc\"");
    assert_eq!(
        sha256_bytes(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
        SHA256_448BIT,
        "SHA-256 of the RFC 6234 448-bit message"
    );
}

#[test]
fn sha256_matches_published_vector_for_large_multiblock_input() {
    // 1,000,000 x 'a' — crosses many compression-function blocks, so it catches
    // buffering/streaming errors that short inputs cannot.
    let data = vec![b'a'; 1_000_000];
    assert_eq!(sha256_bytes(&data), SHA256_MILLION_A);
}

// ---------------------------------------------------------------------------
// BLAKE3 (official reference vectors)
// ---------------------------------------------------------------------------

/// The BLAKE3 test-vector input pattern: bytes 0,1,2,...,250 repeating.
fn blake3_vector_input(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

const BLAKE3_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
const BLAKE3_LEN1: &str = "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213";
const BLAKE3_LEN1024: &str = "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7";

#[test]
fn blake3_matches_published_vectors() {
    assert_eq!(
        blake3_bytes(&blake3_vector_input(0)),
        BLAKE3_EMPTY,
        "BLAKE3 of empty input"
    );
    assert_eq!(
        blake3_bytes(&blake3_vector_input(1)),
        BLAKE3_LEN1,
        "BLAKE3 of the 1-byte reference input"
    );
    assert_eq!(
        blake3_bytes(&blake3_vector_input(1024)),
        BLAKE3_LEN1024,
        "BLAKE3 of the 1024-byte reference input (exactly one chunk)"
    );
}

// ---------------------------------------------------------------------------
// File-based digests: the streaming/chunked path must agree with the vectors,
// not just the in-memory path (uploads always hash from a file).
// ---------------------------------------------------------------------------

async fn write_temp(data: &[u8]) -> tempfile::NamedTempFile {
    let f = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(f.path(), data).await.unwrap();
    f
}

#[tokio::test]
async fn sha256_file_matches_published_vectors() {
    let f = write_temp(b"abc").await;
    assert_eq!(sha256_file(f.path()).await.unwrap(), SHA256_ABC);

    let empty = write_temp(b"").await;
    assert_eq!(sha256_file(empty.path()).await.unwrap(), SHA256_EMPTY);
}

#[tokio::test]
async fn sha256_file_streaming_matches_vector_for_large_input() {
    // The file path reads in chunks; a 1 MB input crosses many read boundaries.
    let f = write_temp(&vec![b'a'; 1_000_000]).await;
    assert_eq!(sha256_file(f.path()).await.unwrap(), SHA256_MILLION_A);
}

#[tokio::test]
async fn blake3_file_matches_published_vector() {
    let f = write_temp(&blake3_vector_input(1024)).await;
    assert_eq!(blake3_file(f.path()).await.unwrap(), BLAKE3_LEN1024);
}

// ---------------------------------------------------------------------------
// The dispatchers must route to the algorithm they claim — a mis-wired match
// arm would otherwise hash with the wrong function and still "look" consistent.
// ---------------------------------------------------------------------------

#[test]
fn digest_bytes_dispatches_to_the_named_algorithm() {
    assert_eq!(
        digest_bytes(ChecksumAlgorithm::Sha256, b"abc").as_deref(),
        Some(SHA256_ABC),
        "Sha256 must produce the published SHA-256 vector"
    );
    assert_eq!(
        digest_bytes(ChecksumAlgorithm::Blake3, &blake3_vector_input(1024)).as_deref(),
        Some(BLAKE3_LEN1024),
        "Blake3 must produce the published BLAKE3 vector"
    );
}

#[tokio::test]
async fn digest_file_dispatches_to_the_named_algorithm() {
    let f = write_temp(b"abc").await;
    assert_eq!(
        digest_file(ChecksumAlgorithm::Sha256, f.path())
            .await
            .unwrap()
            .as_deref(),
        Some(SHA256_ABC)
    );

    let b3 = write_temp(&blake3_vector_input(1024)).await;
    assert_eq!(
        digest_file(ChecksumAlgorithm::Blake3, b3.path())
            .await
            .unwrap()
            .as_deref(),
        Some(BLAKE3_LEN1024)
    );
}

/// SHA-256 and BLAKE3 must never coincide: if a refactor accidentally pointed
/// both arms at one implementation, every other assertion here could still pass
/// while the configured algorithm silently did nothing.
#[test]
fn algorithms_are_actually_distinct() {
    assert_ne!(sha256_bytes(b"abc"), blake3_bytes(b"abc"));
    assert_ne!(
        digest_bytes(ChecksumAlgorithm::Sha256, b"abc"),
        digest_bytes(ChecksumAlgorithm::Blake3, b"abc")
    );
}
