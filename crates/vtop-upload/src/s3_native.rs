//! Native S3 backend built on `aws-sdk-s3` / `aws-config`.
//!
//! Supports AWS S3, MinIO, and Ceph RGW via a custom endpoint and optional
//! path-style addressing. Credentials are read from the environment by the SDK
//! credential chain and are never logged.
//!
//! Integrity: for **SHA-256** the precomputed digest is sent on `PUT`
//! (`x-amz-checksum-sha256`), so the store recomputes the body hash and rejects
//! a corrupted upload (server-validated), and verification reads that
//! store-computed checksum back via `head_object`. For **BLAKE3**, verification
//! streams the stored body through BLAKE3. The uploader-provided digest remains
//! user metadata for inventory tooling only and is never strong evidence.
//! When checksums are disabled, verification falls back to size + existence
//! (backend-limited).

use crate::base::{
    parse_s3_uri, read_bounded, ObjectChecksum, ObjectHead, UploadBackend, VerificationResult,
};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::Client;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use std::path::Path;
use vtop_core::checksum::digest_reader;
use vtop_core::errors::VtopError;
use vtop_core::types::ChecksumAlgorithm;

const CHECKSUM_META_KEY: &str = "vtop-checksum";

/// Convert a lowercase-hex SHA-256 into the base64 form S3 uses for
/// `x-amz-checksum-sha256` (base64 of the raw 32-byte digest).
fn hex_to_b64_sha256(hex_sha: &str) -> Option<String> {
    let raw = hex::decode(hex_sha).ok()?;
    if raw.len() != 32 {
        return None;
    }
    Some(B64.encode(raw))
}

/// Convert S3's base64 `x-amz-checksum-sha256` back into lowercase hex so it
/// compares against the engine's hex SHA-256 representation.
fn b64_to_hex_sha256(b64: &str) -> Option<String> {
    let raw = B64.decode(b64).ok()?;
    if raw.len() != 32 {
        return None;
    }
    Some(hex::encode(raw))
}

/// Connection / addressing settings for the native S3 backend.
#[derive(Debug, Clone)]
pub struct S3NativeConfig {
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub verify_tls: bool,
}

pub struct S3NativeBackend {
    client: Client,
}

/// Enforce the transport policy BEFORE any client is built (#75).
///
/// `verify_tls: true` (the default) means "telemetry must travel encrypted":
/// a plaintext `http://` endpoint under it is a configuration error, not a
/// warning — silently accepting one is exactly the downgrade the flag claims
/// to prevent. `verify_tls: false` is the explicit lab opt-out that permits
/// plaintext endpoints (e.g. the compose lab's `http://minio:9000`).
///
/// Honest scope: this flag does NOT disable certificate verification for
/// `https://` endpoints — the AWS SDK always verifies against the system
/// trust store. A self-signed or private-CA endpoint needs its CA in the
/// system trust store; skipping verification is deliberately unsupported.
fn validate_endpoint_scheme(endpoint_url: Option<&str>, verify_tls: bool) -> Result<(), VtopError> {
    let Some(ep) = endpoint_url else {
        return Ok(()); // default AWS endpoints are always https
    };
    let plaintext = ep.trim().to_ascii_lowercase().starts_with("http://");
    if plaintext && verify_tls {
        return Err(VtopError::Config(format!(
            "endpoint_url {ep} is plaintext http:// while verify_tls is true; refusing to send \
             telemetry unencrypted. Use an https:// endpoint, or set verify_tls: false \
             (VTOP_S3_VERIFY_TLS=false) to explicitly opt into a plaintext LAB endpoint"
        )));
    }
    if plaintext {
        tracing::warn!(
            endpoint = %ep,
            "plaintext S3 endpoint permitted because verify_tls=false (lab use only)"
        );
    }
    Ok(())
}

impl S3NativeBackend {
    /// Build the backend from config, resolving credentials via the standard
    /// AWS credential chain (env vars, profile, instance metadata).
    pub async fn new(cfg: &S3NativeConfig) -> Result<Self, VtopError> {
        validate_endpoint_scheme(cfg.endpoint_url.as_deref(), cfg.verify_tls)?;
        if !cfg.verify_tls {
            tracing::warn!(
                "verify_tls is false: plaintext endpoints are permitted (lab use only). \
                 Certificate verification for https:// endpoints is NOT disabled - \
                 private CAs must be in the system trust store"
            );
        }

        // The SDK resolves endpoints from its OWN configuration too —
        // AWS_ENDPOINT_URL / AWS_ENDPOINT_URL_S3 and the shared config file —
        // and those must not bypass the policy the explicit config obeys.
        // The service-specific variable is checked here because it is applied
        // at service-config construction, where no resolved value is
        // observable; the globally-resolved endpoint is checked on the loaded
        // SdkConfig below.
        for var in ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"] {
            if let Ok(ep) = std::env::var(var) {
                validate_endpoint_scheme(Some(&ep), cfg.verify_tls)
                    .map_err(|e| VtopError::Config(format!("{var}: {e}")))?;
            }
        }

        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cfg.region.clone()));
        if let Some(ep) = &cfg.endpoint_url {
            loader = loader.endpoint_url(ep.clone());
        }
        let shared = loader.load().await;
        // Whatever endpoint actually resolved (explicit config, env, or the
        // shared config file) is what the client will talk to — validate THAT,
        // not only the value we passed in.
        validate_endpoint_scheme(shared.endpoint_url(), cfg.verify_tls)?;

        let s3_conf = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(cfg.force_path_style)
            .build();

        Ok(Self {
            client: Client::from_conf(s3_conf),
        })
    }

    async fn put(
        &self,
        local_path: &Path,
        uri: &str,
        content_type: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        let (bucket, key) = parse_s3_uri(uri)?;
        let body = ByteStream::from_path(local_path)
            .await
            .map_err(|e| VtopError::Upload(format!("reading {}: {e}", local_path.display())))?;

        let mut req = self
            .client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .content_type(content_type)
            .body(body);

        if let Some(c) = checksum {
            // Always retain the hex digest as user metadata (any algorithm),
            // for tooling and verification of objects from older writers.
            req = req.metadata(CHECKSUM_META_KEY, c.hex);
            // For SHA-256 only, also request server-validated integrity: S3
            // recomputes SHA-256 over the body and rejects the upload
            // (BadDigest) if it does not match, so in-transit corruption fails
            // the PUT itself. (BLAKE3 is 32 bytes too, so it MUST NOT be sent
            // here — S3 would recompute SHA-256 and reject it.)
            if c.is_sha256() {
                if let Some(b64) = hex_to_b64_sha256(c.hex) {
                    req = req.checksum_sha256(b64);
                }
            }
        }

        req.send().await.map_err(|e| {
            VtopError::Upload(format!("put_object {uri}: {}", e.into_service_error()))
        })?;
        tracing::info!(uri, "object uploaded via s3_native");
        Ok(())
    }

    /// Recompute a digest from the bytes returned by S3 without buffering the
    /// full object. Used for algorithms S3 does not compute natively.
    async fn digest_stored_body(
        &self,
        object_uri: &str,
        algo: ChecksumAlgorithm,
    ) -> Result<(String, u64), VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                VtopError::Upload(format!(
                    "get_object {object_uri}: {}",
                    e.into_service_error()
                ))
            })?;
        digest_reader(algo, out.body.into_async_read())
            .await?
            .ok_or_else(|| VtopError::Upload("cannot hash with disabled checksum mode".into()))
    }
}

#[async_trait]
impl UploadBackend for S3NativeBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        self.put(local_path, object_uri, "application/octet-stream", checksum)
            .await
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        self.put(local_path, manifest_uri, "application/json", checksum)
            .await
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                VtopError::Upload(format!(
                    "get_object {object_uri}: {}",
                    e.into_service_error()
                ))
            })?;
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| VtopError::Upload(format!("get_object body {object_uri}: {e}")))?;
        Ok(bytes.into_bytes().to_vec())
    }

    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                VtopError::Upload(format!(
                    "get_object {object_uri}: {}",
                    e.into_service_error()
                ))
            })?;
        if out
            .content_length()
            .is_some_and(|size| size < 0 || size as u64 > max_bytes as u64)
        {
            return Err(VtopError::Upload(format!(
                "stored object {object_uri} exceeds the {max_bytes}-byte read limit"
            )));
        }
        read_bounded(out.body.into_async_read(), max_bytes, object_uri).await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .map_err(|e| {
                VtopError::Upload(format!(
                    "head_object {object_uri}: {}",
                    e.into_service_error()
                ))
            })?;

        // Only expose the checksum S3 itself computed over the stored body.
        // x-amz-meta-vtop-checksum is written by the uploader and therefore
        // cannot establish content integrity (#64).
        let checksum_sha256 = out.checksum_sha256().and_then(b64_to_hex_sha256);

        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: out.content_length().map(|v| v as u64),
            etag: out.e_tag().map(|s| s.to_string()),
            checksum_sha256,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let head = self.head_object(object_uri).await?;

        if let Some(sz) = head.size_bytes {
            if sz != expected_size {
                return Ok(VerificationResult::failed(format!(
                    "size mismatch: expected {expected_size}, got {sz}"
                )));
            }
        } else {
            return Ok(VerificationResult::failed("object size unavailable"));
        }

        // Checksums disabled: size + existence is all we can confirm.
        let Some(expected) = expected else {
            return Ok(VerificationResult::limited(
                "object present and size matches (checksums disabled)",
            ));
        };

        let algo = match expected.algorithm.parse::<ChecksumAlgorithm>() {
            Ok(ChecksumAlgorithm::None) => {
                return Ok(VerificationResult::failed(
                    "checksum value supplied with disabled algorithm",
                ))
            }
            Ok(algo) => algo,
            Err(e) => return Ok(VerificationResult::failed(e)),
        };

        match algo {
            ChecksumAlgorithm::Sha256 => match head.checksum_sha256 {
                Some(stored) if stored.eq_ignore_ascii_case(expected.hex) => Ok(
                    VerificationResult::passed("S3 service-computed SHA-256 verified"),
                ),
                Some(_) => Ok(VerificationResult::failed(
                    "S3 service-computed SHA-256 mismatch",
                )),
                None => Ok(VerificationResult::limited(
                    "object size matches; S3 returned no service-computed SHA-256",
                )),
            },
            ChecksumAlgorithm::Blake3 => {
                let (actual, bytes_read) = self.digest_stored_body(object_uri, algo).await?;
                if bytes_read != expected_size {
                    return Ok(VerificationResult::failed(format!(
                        "size mismatch: expected {expected_size}, read {bytes_read} stored bytes"
                    )));
                }
                if actual.eq_ignore_ascii_case(expected.hex) {
                    Ok(VerificationResult::passed("stored content BLAKE3 verified"))
                } else {
                    Ok(VerificationResult::failed("stored content BLAKE3 mismatch"))
                }
            }
            ChecksumAlgorithm::None => unreachable!("handled above"),
        }
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        self.client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                VtopError::Upload(format!(
                    "delete_object {object_uri}: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), VtopError> {
        // Idempotent: treat "already exists / already owned by you" as success.
        match self.client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                tracing::info!(bucket, "bucket created");
                Ok(())
            }
            Err(e) => {
                let se = e.into_service_error();
                let msg = se.to_string().to_lowercase();
                if msg.contains("alreadyexists")
                    || msg.contains("already exists")
                    || msg.contains("alreadyownedbyyou")
                    || msg.contains("already owned")
                    || msg.contains("bucketalreadyownedbyyou")
                {
                    Ok(())
                } else {
                    Err(VtopError::Upload(format!("create_bucket {bucket}: {se}")))
                }
            }
        }
    }

    fn backend_name(&self) -> &'static str {
        "s3_native"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        // Single-part put for the prototype; multipart is a documented follow-up.
        false
    }
}

/// Build an [`S3NativeConfig`] from a [`vtop_core::config::UploadConfig`] and
/// the standard VTOP environment overrides.
pub fn config_from_upload(upload: &vtop_core::config::UploadConfig) -> S3NativeConfig {
    let endpoint_url = std::env::var("VTOP_S3_ENDPOINT_URL")
        .ok()
        .or_else(|| upload.endpoint_url.clone());
    let force_path_style = std::env::var("VTOP_S3_FORCE_PATH_STYLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(upload.force_path_style);
    let verify_tls = std::env::var("VTOP_S3_VERIFY_TLS")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(upload.verify_tls);

    S3NativeConfig {
        region: upload.region.clone(),
        endpoint_url,
        force_path_style,
        verify_tls,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_core::checksum::sha256_bytes;

    #[test]
    fn hex_b64_round_trips() {
        let hex = sha256_bytes(b"vtop object body");
        let b64 = hex_to_b64_sha256(&hex).expect("hex -> b64");
        let back = b64_to_hex_sha256(&b64).expect("b64 -> hex");
        assert_eq!(back, hex);
    }

    #[test]
    fn known_empty_string_vector() {
        // SHA-256("") in hex and the base64 S3 reports for it.
        let hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(
            hex_to_b64_sha256(hex).unwrap(),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
    }

    #[test]
    fn rejects_non_sha256_lengths() {
        // Not 32 bytes once decoded -> no conversion (avoids sending a bogus
        // checksum that S3 would reject opaquely).
        assert!(hex_to_b64_sha256("abcd").is_none());
        assert!(hex_to_b64_sha256("zz").is_none()); // not valid hex
        assert!(b64_to_hex_sha256("not-base64!!").is_none());
        assert!(b64_to_hex_sha256(&B64.encode([0u8; 16])).is_none()); // 16 bytes
    }

    /// #75: verify_tls=true must REJECT plaintext endpoints, not warn past
    /// them; verify_tls=false is the explicit lab opt-out.
    #[test]
    fn plaintext_endpoint_policy() {
        // The hole this closes: verify_tls promised encryption but plaintext
        // was accepted anyway.
        let err = validate_endpoint_scheme(Some("http://minio:9000"), true)
            .expect_err("plaintext + verify_tls=true must fail");
        assert!(matches!(err, VtopError::Config(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("http://minio:9000"),
            "names the endpoint: {msg}"
        );
        assert!(msg.contains("verify_tls"), "names the fix: {msg}");

        // Explicit lab opt-out still works (the compose lab is plaintext).
        assert!(validate_endpoint_scheme(Some("http://minio:9000"), false).is_ok());
        // Scheme check is case-insensitive and trims whitespace.
        assert!(validate_endpoint_scheme(Some("  HTTP://minio:9000"), true).is_err());
        // https endpoints pass under either setting.
        assert!(validate_endpoint_scheme(Some("https://s3.example.com"), true).is_ok());
        assert!(validate_endpoint_scheme(Some("https://s3.example.com"), false).is_ok());
        // No custom endpoint = default AWS https endpoints.
        assert!(validate_endpoint_scheme(None, true).is_ok());
    }
}
