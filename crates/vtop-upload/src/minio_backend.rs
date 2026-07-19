//! Compatibility backend that shells out to the MinIO client (`mc`).
//!
//! Compatibility mode only. Uses a configured `mc` alias (e.g. `local`) so the
//! URI `s3://bucket/key` maps to `alias/bucket/key`. Credentials live in the
//! `mc` config and are never printed.

use crate::base::{
    parse_s3_uri, read_command_bounded, verify_command_content, ObjectChecksum, ObjectHead,
    StoredManifest, UploadBackend, VerificationResult,
};
use crate::command::CommandPolicy;
use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;
use vtop_core::errors::VtopError;

pub struct MinioBackend {
    command: CommandPolicy,
    /// The `mc` alias that points at the target endpoint + credentials.
    pub alias: String,
}

impl MinioBackend {
    pub(crate) fn new(command: CommandPolicy, alias: impl Into<String>) -> Self {
        Self {
            command,
            alias: alias.into(),
        }
    }

    /// Map `s3://bucket/key` to `alias/bucket/key` for `mc`.
    fn mc_target(&self, uri: &str) -> Result<String, VtopError> {
        let (bucket, key) = parse_s3_uri(uri)?;
        Ok(format!("{}/{}/{}", self.alias, bucket, key))
    }

    fn base_cmd(&self) -> Command {
        self.command.command()
    }
}

#[async_trait]
impl UploadBackend for MinioBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        let target = self.mc_target(object_uri)?;
        let mut command = self.base_cmd();
        command.arg("cp").arg(local_path).arg(target);
        self.command.run(&mut command, "object upload").await
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        let target = self.mc_target(manifest_uri)?;
        let mut command = self.base_cmd();
        command.arg("cp").arg(local_path).arg(target);
        self.command.run(&mut command, "manifest upload").await?;
        Ok(StoredManifest::default())
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let target = self.mc_target(object_uri)?;
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| VtopError::Upload(format!("temp file for download: {e}")))?;
        let mut command = self.base_cmd();
        command.arg("cp").arg(target).arg(tmp.path());
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
        let target = self.mc_target(object_uri)?;
        let mut cmd = self.base_cmd();
        cmd.arg("cat").arg(target);
        read_command_bounded(
            &mut cmd,
            max_bytes,
            object_uri,
            "mc",
            self.command.timeout(),
        )
        .await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let target = self.mc_target(object_uri)?;
        let mut command = self.base_cmd();
        command.arg("stat").arg("--json").arg(target);
        let out = self.command.output(&mut command, "object metadata").await?;
        let json: serde_json::Value = serde_json::from_str(&out).unwrap_or(serde_json::Value::Null);
        let size = json.get("size").and_then(|v| v.as_u64());
        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: size,
            etag: json
                .get("etag")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            checksum_sha256: None,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected_checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let Some(expected) = expected_checksum else {
            let head = self.head_object(object_uri).await?;
            return match head.size_bytes {
                Some(size) if size == expected_size => Ok(VerificationResult::limited(
                    "mc: object present and size matches (checksums disabled)",
                )),
                Some(size) => Ok(VerificationResult::failed(format!(
                    "size mismatch: expected {expected_size}, got {size}"
                ))),
                None => Ok(VerificationResult::failed("could not read object size")),
            };
        };
        let target = self.mc_target(object_uri)?;
        let mut cmd = self.base_cmd();
        cmd.arg("cat").arg(target);
        verify_command_content(
            &mut cmd,
            expected_size,
            expected,
            "mc",
            self.command.timeout(),
        )
        .await
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        let target = self.mc_target(object_uri)?;
        let mut command = self.base_cmd();
        command.arg("rm").arg(target);
        self.command.run(&mut command, "object delete").await
    }

    fn backend_name(&self) -> &'static str {
        "minio"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        true
    }
}
