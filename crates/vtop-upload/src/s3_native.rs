//! Native S3 backend built on `aws-sdk-s3` / `aws-config`.
//!
//! Supports AWS S3, MinIO, and Ceph RGW via a custom endpoint and optional
//! path-style addressing. Credentials are read from the environment by the SDK
//! credential chain and are never logged. The object's SHA-256 is stored as
//! user metadata (`x-amz-meta-vtop-sha256`) so verification via `head_object`
//! can confirm integrity without re-downloading.

use crate::base::{parse_s3_uri, ObjectHead, UploadBackend, VerificationResult};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::path::Path;
use vtop_core::errors::VtopError;

const SHA256_META_KEY: &str = "vtop-sha256";

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

impl S3NativeBackend {
    /// Build the backend from config, resolving credentials via the standard
    /// AWS credential chain (env vars, profile, instance metadata).
    pub async fn new(cfg: &S3NativeConfig) -> Result<Self, VtopError> {
        if !cfg.verify_tls {
            tracing::warn!(
                "VTOP_S3_VERIFY_TLS is disabled; TLS verification is OFF (lab use only)"
            );
        }

        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cfg.region.clone()));
        if let Some(ep) = &cfg.endpoint_url {
            loader = loader.endpoint_url(ep.clone());
        }
        let shared = loader.load().await;

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
        sha256: Option<&str>,
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

        if let Some(h) = sha256 {
            req = req.metadata(SHA256_META_KEY, h);
        }

        req.send().await.map_err(|e| {
            VtopError::Upload(format!("put_object {uri}: {}", e.into_service_error()))
        })?;
        tracing::info!(uri, "object uploaded via s3_native");
        Ok(())
    }
}

#[async_trait]
impl UploadBackend for S3NativeBackend {
    async fn put_object(&self, local_path: &Path, object_uri: &str) -> Result<(), VtopError> {
        let sha = vtop_core::checksum::sha256_file(local_path).await?;
        self.put(
            local_path,
            object_uri,
            "application/octet-stream",
            Some(&sha),
        )
        .await
    }

    async fn put_manifest(&self, local_path: &Path, manifest_uri: &str) -> Result<(), VtopError> {
        let sha = vtop_core::checksum::sha256_file(local_path).await?;
        self.put(local_path, manifest_uri, "application/json", Some(&sha))
            .await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                VtopError::Upload(format!(
                    "head_object {object_uri}: {}",
                    e.into_service_error()
                ))
            })?;

        let checksum_sha256 = out.metadata().and_then(|m| m.get(SHA256_META_KEY)).cloned();

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
        expected_sha256: &str,
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

        match head.checksum_sha256 {
            Some(stored) if stored.eq_ignore_ascii_case(expected_sha256) => {
                Ok(VerificationResult::passed("size + sha256 verified"))
            }
            Some(stored) => Ok(VerificationResult::failed(format!(
                "sha256 mismatch: expected {expected_sha256}, stored {stored}"
            ))),
            None => Ok(VerificationResult::limited(
                "object present and size matches; backend did not return sha256 metadata",
            )),
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
