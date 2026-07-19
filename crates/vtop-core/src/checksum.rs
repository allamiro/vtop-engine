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
use tokio::io::{AsyncRead, AsyncReadExt};

/// Hash an asynchronous byte stream in bounded chunks and return both its
/// lowercase-hex digest and the number of bytes actually read.
///
/// Storage backends use this for read-back verification. Counting the stream
/// itself avoids treating a prior HEAD/stat as proof about bytes that may have
/// changed before the read began.
pub async fn digest_reader<R>(
    algo: ChecksumAlgorithm,
    mut reader: R,
) -> Result<Option<(String, u64)>, VtopError>
where
    R: AsyncRead + Unpin,
{
    enum Hasher {
        Sha256(Sha256),
        Blake3(Box<blake3::Hasher>),
    }

    let mut hasher = match algo {
        ChecksumAlgorithm::Sha256 => Hasher::Sha256(Sha256::new()),
        ChecksumAlgorithm::Blake3 => Hasher::Blake3(Box::new(blake3::Hasher::new())),
        ChecksumAlgorithm::None => return Ok(None),
    };
    let mut total = 0u64;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total = total.checked_add(n as u64).ok_or_else(|| {
            VtopError::Other("content length overflow while computing checksum".into())
        })?;
        match &mut hasher {
            Hasher::Sha256(h) => h.update(&buf[..n]),
            Hasher::Blake3(h) => {
                h.update(&buf[..n]);
            }
        }
    }
    let hex = match hasher {
        Hasher::Sha256(h) => hex::encode(h.finalize()),
        Hasher::Blake3(h) => h.finalize().to_hex().to_string(),
    };
    Ok(Some((hex, total)))
}

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
    if algo == ChecksumAlgorithm::None {
        return Ok(None);
    }
    let file = tokio::fs::File::open(path).await?;
    Ok(digest_reader(algo, file).await?.map(|(hex, _)| hex))
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
    async fn digest_reader_counts_the_hashed_bytes() {
        use crate::types::ChecksumAlgorithm::*;
        let data = b"streamed content";
        let (sha, size) = digest_reader(Sha256, &data[..]).await.unwrap().unwrap();
        assert_eq!(sha, sha256_bytes(data));
        assert_eq!(size, data.len() as u64);

        let (b3, size) = digest_reader(Blake3, &data[..]).await.unwrap().unwrap();
        assert_eq!(b3, blake3_bytes(data));
        assert_eq!(size, data.len() as u64);
        assert!(digest_reader(None, &data[..]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn disabled_digest_does_not_touch_the_path() {
        use crate::types::ChecksumAlgorithm::None;
        let missing = std::path::Path::new("/definitely/not/a/vtop/file");
        assert_eq!(digest_file(None, missing).await.unwrap(), Option::None);
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
