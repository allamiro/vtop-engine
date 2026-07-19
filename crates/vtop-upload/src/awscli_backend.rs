//! Compatibility backend that shells out to the AWS CLI (`aws`).
//!
//! Compatibility mode only. Supports `--endpoint-url` and `AWS_PROFILE`.
//! Credentials come from the environment / profile and are never printed.

use crate::base::{
    parse_s3_uri, read_command_bounded, verify_command_content, ObjectChecksum, ObjectHead,
    StoredManifest, UploadBackend, VerificationResult,
};
use crate::command::CommandPolicy;
use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;
use vtop_core::errors::VtopError;

const SHA256_META_KEY: &str = "vtop-sha256";

pub struct AwsCliBackend {
    command: CommandPolicy,
    pub endpoint_url: Option<String>,
    pub profile: Option<String>,
}

impl AwsCliBackend {
    pub(crate) fn new(
        command: CommandPolicy,
        endpoint_url: Option<String>,
        profile: Option<String>,
    ) -> Self {
        Self {
            command,
            endpoint_url,
            profile,
        }
    }

    fn base_cmd(&self) -> Command {
        let mut c = self.command.command();
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
        self.command.run(&mut cmd, "object upload").await
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        let mut cmd = self.base_cmd();
        cmd.arg("s3").arg("cp").arg(local_path).arg(manifest_uri);
        if let Some(c) = checksum {
            cmd.arg("--metadata")
                .arg(format!("{SHA256_META_KEY}={}", c.hex));
        }
        self.command.run(&mut cmd, "manifest upload").await?;
        // The CLI does not surface x-amz-version-id; no version to pin.
        Ok(StoredManifest::default())
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| VtopError::Upload(format!("temp file for download: {e}")))?;
        let mut command = self.base_cmd();
        command.arg("s3").arg("cp").arg(object_uri).arg(tmp.path());
        self.command.run(&mut command, "object download").await?;
        tokio::fs::read(tmp.path())
            .await
            .map_err(|e| VtopError::Upload(format!("reading downloaded {object_uri}: {e}")))
    }

    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let mut cmd = self.base_cmd();
        cmd.arg("s3").arg("cp").arg(object_uri).arg("-");
        read_command_bounded(
            &mut cmd,
            max_bytes,
            object_uri,
            "aws cli",
            self.command.timeout(),
        )
        .await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let mut command = self.base_cmd();
        command
            .arg("s3api")
            .arg("head-object")
            .arg("--bucket")
            .arg(&bucket)
            .arg("--key")
            .arg(&key);
        let out = self.command.output(&mut command, "object metadata").await?;
        let json: serde_json::Value = serde_json::from_str(&out).unwrap_or(serde_json::Value::Null);
        let size = json.get("ContentLength").and_then(|v| v.as_u64());
        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: size,
            etag: json
                .get("ETag")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            // vtop-sha256 is uploader-controlled user metadata, not a digest
            // computed by S3 over the stored body.
            checksum_sha256: None,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let Some(expected) = expected else {
            let head = self.head_object(object_uri).await?;
            return match head.size_bytes {
                Some(size) if size == expected_size => Ok(VerificationResult::limited(
                    "aws cli: object present and size matches (checksums disabled)",
                )),
                Some(size) => Ok(VerificationResult::failed(format!(
                    "size mismatch: expected {expected_size}, got {size}"
                ))),
                None => Ok(VerificationResult::failed("object size unavailable")),
            };
        };
        let mut cmd = self.base_cmd();
        cmd.arg("s3").arg("cp").arg(object_uri).arg("-");
        verify_command_content(
            &mut cmd,
            expected_size,
            expected,
            "aws cli",
            self.command.timeout(),
        )
        .await
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        let mut command = self.base_cmd();
        command.arg("s3").arg("rm").arg(object_uri);
        self.command.run(&mut command, "object delete").await
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
