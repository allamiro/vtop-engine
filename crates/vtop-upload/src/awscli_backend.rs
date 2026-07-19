//! Compatibility backend that shells out to the AWS CLI (`aws`).
//!
//! Compatibility mode only. Supports `--endpoint-url` and `AWS_PROFILE`.
//! Credentials come from the environment / profile and are never printed.

use crate::base::{parse_s3_uri, ObjectChecksum, ObjectHead, UploadBackend, VerificationResult};
use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;
use vtop_core::errors::VtopError;

const SHA256_META_KEY: &str = "vtop-sha256";

pub struct AwsCliBackend {
    pub endpoint_url: Option<String>,
    pub profile: Option<String>,
}

impl AwsCliBackend {
    pub fn new(endpoint_url: Option<String>, profile: Option<String>) -> Self {
        Self {
            endpoint_url,
            profile: profile.or_else(|| std::env::var("AWS_PROFILE").ok()),
        }
    }

    fn base_cmd(&self) -> Command {
        let mut c = Command::new("aws");
        if let Some(ep) = &self.endpoint_url {
            c.arg("--endpoint-url").arg(ep);
        }
        if let Some(p) = &self.profile {
            c.arg("--profile").arg(p);
        }
        c
    }
}

#[async_trait]
impl UploadBackend for AwsCliBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        let mut cmd = self.base_cmd();
        cmd.arg("s3").arg("cp").arg(local_path).arg(object_uri);
        if let Some(c) = checksum {
            cmd.arg("--metadata")
                .arg(format!("{SHA256_META_KEY}={}", c.hex));
        }
        run(&mut cmd).await
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        let mut cmd = self.base_cmd();
        cmd.arg("s3").arg("cp").arg(local_path).arg(manifest_uri);
        if let Some(c) = checksum {
            cmd.arg("--metadata")
                .arg(format!("{SHA256_META_KEY}={}", c.hex));
        }
        run(&mut cmd).await
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| VtopError::Upload(format!("temp file for download: {e}")))?;
        run(self
            .base_cmd()
            .arg("s3")
            .arg("cp")
            .arg(object_uri)
            .arg(tmp.path()))
        .await?;
        tokio::fs::read(tmp.path())
            .await
            .map_err(|e| VtopError::Upload(format!("reading downloaded {object_uri}: {e}")))
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = output(
            self.base_cmd()
                .arg("s3api")
                .arg("head-object")
                .arg("--bucket")
                .arg(&bucket)
                .arg("--key")
                .arg(&key),
        )
        .await?;
        let json: serde_json::Value = serde_json::from_str(&out).unwrap_or(serde_json::Value::Null);
        let size = json.get("ContentLength").and_then(|v| v.as_u64());
        let checksum_sha256 = json
            .get("Metadata")
            .and_then(|m| m.get(SHA256_META_KEY))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: size,
            etag: json
                .get("ETag")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
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
        match head.size_bytes {
            Some(sz) if sz != expected_size => {
                return Ok(VerificationResult::failed(format!(
                    "size mismatch: expected {expected_size}, got {sz}"
                )))
            }
            None => return Ok(VerificationResult::failed("object size unavailable")),
            _ => {}
        }
        let Some(expected) = expected else {
            return Ok(VerificationResult::limited(
                "aws cli: size matches (checksums disabled)",
            ));
        };
        match head.checksum_sha256 {
            Some(stored) if stored.eq_ignore_ascii_case(expected.hex) => Ok(
                VerificationResult::passed("aws cli: size + checksum verified"),
            ),
            Some(stored) => Ok(VerificationResult::failed(format!(
                "checksum mismatch: expected {}, stored {stored}",
                expected.hex
            ))),
            None => Ok(VerificationResult::limited(
                "aws cli: size matches; no checksum metadata returned",
            )),
        }
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        run(self.base_cmd().arg("s3").arg("rm").arg(object_uri)).await
    }

    fn backend_name(&self) -> &'static str {
        "awscli"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        true
    }
}

async fn run(cmd: &mut Command) -> Result<(), VtopError> {
    let status = cmd
        .status()
        .await
        .map_err(|e| VtopError::Upload(format!("spawning command failed: {e}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(VtopError::Upload(format!("command exited with {status}")))
    }
}

async fn output(cmd: &mut Command) -> Result<String, VtopError> {
    let out = cmd
        .output()
        .await
        .map_err(|e| VtopError::Upload(format!("spawning command failed: {e}")))?;
    if !out.status.success() {
        return Err(VtopError::Upload(format!(
            "command exited with {}",
            out.status
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}
