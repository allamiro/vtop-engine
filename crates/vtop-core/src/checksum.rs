//! SHA-256 checksum helpers.
//!
//! Rules:
//! * Object checksum MUST be calculated after compression.
//! * Manifest checksum MUST be calculated after manifest serialization.
//! * Source progress MUST NOT be committed unless checksum verification
//!   succeeds.

use crate::errors::VtopError;
use crate::types::ChecksumAlgorithm;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::AsyncReadExt;

/// Compute the lowercase hex SHA-256 of an in-memory byte slice.
pub fn sha256_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Compute the lowercase hex BLAKE3 of an in-memory byte slice.
pub fn blake3_bytes(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Compute the lowercase hex BLAKE3 of a file, streaming in bounded chunks.
pub async fn blake3_file(path: &Path) -> Result<String, VtopError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Compute a file digest with the requested algorithm. Returns `Ok(None)` when
/// checksums are disabled ([`ChecksumAlgorithm::None`]).
pub async fn digest_file(
    algo: ChecksumAlgorithm,
    path: &Path,
) -> Result<Option<String>, VtopError> {
    Ok(match algo {
        ChecksumAlgorithm::Sha256 => Some(sha256_file(path).await?),
        ChecksumAlgorithm::Blake3 => Some(blake3_file(path).await?),
        ChecksumAlgorithm::None => None,
    })
}

/// Compute an in-memory digest with the requested algorithm.
pub fn digest_bytes(algo: ChecksumAlgorithm, data: &[u8]) -> Option<String> {
    match algo {
        ChecksumAlgorithm::Sha256 => Some(sha256_bytes(data)),
        ChecksumAlgorithm::Blake3 => Some(blake3_bytes(data)),
        ChecksumAlgorithm::None => None,
    }
}

/// Compute the lowercase hex SHA-256 of a file, streaming it in bounded chunks
/// so that large objects never need to be fully resident in memory.
pub async fn sha256_file(path: &Path) -> Result<String, VtopError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20]; // 1 MiB streaming window
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Verify that `path` hashes to `expected`. Returns an error on mismatch.
pub async fn verify_file(path: &Path, expected: &str, uri: &str) -> Result<(), VtopError> {
    let actual = sha256_file(path).await?;
    if actual.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(VtopError::ChecksumMismatch {
            uri: uri.to_string(),
            expected: expected.to_string(),
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn known_vector() {
        // SHA-256 of the empty string.
        assert_eq!(
            sha256_bytes(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // SHA-256 of "abc".
        assert_eq!(
            sha256_bytes(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn file_matches_bytes() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"hello vtop").unwrap();
        f.flush().unwrap();
        let from_file = sha256_file(f.path()).await.unwrap();
        assert_eq!(from_file, sha256_bytes(b"hello vtop"));
    }

    #[tokio::test]
    async fn checksum_changes_when_object_changes() {
        let a = sha256_bytes(b"payload-A");
        let b = sha256_bytes(b"payload-B");
        assert_ne!(a, b);
    }

    #[test]
    fn blake3_known_vector() {
        // BLAKE3 of the empty input.
        assert_eq!(
            blake3_bytes(b""),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        assert_ne!(blake3_bytes(b"abc"), sha256_bytes(b"abc"));
    }

    #[tokio::test]
    async fn digest_dispatch_and_disabled() {
        use crate::types::ChecksumAlgorithm::*;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"data").unwrap();
        f.flush().unwrap();
        assert_eq!(
            digest_file(Sha256, f.path()).await.unwrap(),
            Some(sha256_bytes(b"data"))
        );
        assert_eq!(
            digest_file(Blake3, f.path()).await.unwrap(),
            Some(blake3_bytes(b"data"))
        );
        assert_eq!(digest_file(None, f.path()).await.unwrap(), Option::None);
        assert_eq!(digest_bytes(None, b"x"), Option::None);
    }

    #[tokio::test]
    async fn verify_detects_mismatch() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"data").unwrap();
        f.flush().unwrap();
        let err = verify_file(f.path(), &sha256_bytes(b"other"), "s3://x/y")
            .await
            .unwrap_err();
        assert!(matches!(err, VtopError::ChecksumMismatch { .. }));
    }
}
