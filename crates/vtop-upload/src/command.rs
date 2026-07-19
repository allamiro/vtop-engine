//! Hardened process execution for compatibility upload backends.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use vtop_core::config::UploadConfig;
use vtop_core::errors::VtopError;

#[derive(Clone)]
pub(crate) struct CommandPolicy {
    binary: PathBuf,
    environment: Vec<(OsString, OsString)>,
    timeout: Duration,
    max_output_bytes: usize,
    backend: &'static str,
}

impl CommandPolicy {
    pub(crate) fn from_config(
        config: &UploadConfig,
        backend: &'static str,
    ) -> Result<Self, VtopError> {
        if config.command_timeout_seconds == 0 {
            return Err(VtopError::Config(
                "upload.command_timeout_seconds must be > 0".into(),
            ));
        }
        if config.command_max_output_bytes == 0 {
            return Err(VtopError::Config(
                "upload.command_max_output_bytes must be > 0".into(),
            ));
        }
        let configured = config.command_binary.as_deref().ok_or_else(|| {
            VtopError::Config(format!(
                "upload.command_binary is required for the {backend} compatibility backend"
            ))
        })?;
        let path = PathBuf::from(configured);
        if !path.is_absolute() {
            return Err(VtopError::Config(format!(
                "{backend} command path must be absolute; PATH lookup is forbidden"
            )));
        }
        let binary = std::fs::canonicalize(&path).map_err(|error| {
            VtopError::Config(format!(
                "cannot resolve configured {backend} command {}: {error}",
                path.display()
            ))
        })?;
        if !std::fs::metadata(&binary)?.is_file() {
            return Err(VtopError::Config(format!(
                "configured {backend} command is not a regular file: {}",
                binary.display()
            )));
        }

        let mut seen = HashSet::new();
        let mut environment = Vec::new();
        for name in &config.command_env_allowlist {
            if name.trim().is_empty() || name.contains('=') {
                return Err(VtopError::Config(
                    "upload.command_env_allowlist entries must be non-empty environment-variable names"
                        .into(),
                ));
            }
            if !seen.insert(name) {
                return Err(VtopError::Config(format!(
                    "duplicate upload.command_env_allowlist entry: {name}"
                )));
            }
            let value = std::env::var_os(name).ok_or_else(|| {
                VtopError::Config(format!(
                    "allowlisted command environment variable {name} is not set"
                ))
            })?;
            environment.push((OsString::from(name), value));
        }

        Ok(Self {
            binary,
            environment,
            timeout: Duration::from_secs(config.command_timeout_seconds),
            max_output_bytes: config.command_max_output_bytes,
            backend,
        })
    }

    pub(crate) fn command(&self) -> Command {
        self.command_with_environment(true)
    }

    fn command_with_environment(&self, include_allowlist: bool) -> Command {
        let mut command = Command::new(&self.binary);
        command.env_clear().env("LC_ALL", "C");
        if include_allowlist {
            command.envs(self.environment.iter().cloned());
        }
        command.stdin(Stdio::null()).kill_on_drop(true);
        command
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) async fn verify_version(&self, marker: &str) -> Result<(), VtopError> {
        // Tool identity needs no storage credentials. Probe with the minimal
        // fixed locale only, even when runtime operations have an allowlist.
        let mut command = self.command_with_environment(false);
        command.arg("--version");
        let output = self.output(&mut command, "version check").await?;
        if !output.to_ascii_lowercase().contains(marker) {
            return Err(VtopError::Config(format!(
                "configured {} command did not identify as expected during --version",
                self.backend
            )));
        }
        Ok(())
    }

    pub(crate) async fn run(
        &self,
        command: &mut Command,
        operation: &str,
    ) -> Result<(), VtopError> {
        let output = self.capture(command, operation).await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(VtopError::Upload(format!(
                "{} {operation} exited with {}",
                self.backend, output.status
            )))
        }
    }

    pub(crate) async fn output(
        &self,
        command: &mut Command,
        operation: &str,
    ) -> Result<String, VtopError> {
        let output = self.capture(command, operation).await?;
        if !output.status.success() {
            return Err(VtopError::Upload(format!(
                "{} {operation} exited with {}",
                self.backend, output.status
            )));
        }
        let mut combined = output.stdout;
        combined.extend_from_slice(&output.stderr);
        Ok(String::from_utf8_lossy(&combined).into_owned())
    }

    async fn capture(
        &self,
        command: &mut Command,
        operation: &str,
    ) -> Result<CapturedOutput, VtopError> {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|error| {
            VtopError::Upload(format!("spawning {} {operation}: {error}", self.backend))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            VtopError::Upload(format!("{} {operation} stdout unavailable", self.backend))
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            VtopError::Upload(format!("{} {operation} stderr unavailable", self.backend))
        })?;
        let max = self.max_output_bytes;
        let completed = tokio::time::timeout(self.timeout, async {
            let (status, stdout, stderr) = tokio::join!(
                child.wait(),
                capture_bounded(stdout, max),
                capture_bounded(stderr, max)
            );
            (status, stdout, stderr)
        })
        .await;

        let (status, stdout, stderr) = match completed {
            Ok(result) => result,
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(VtopError::Upload(format!(
                    "{} {operation} exceeded the {}s timeout",
                    self.backend,
                    self.timeout.as_secs()
                )));
            }
        };
        let status = status.map_err(|error| {
            VtopError::Upload(format!("waiting for {} {operation}: {error}", self.backend))
        })?;
        let (stdout, stdout_oversized) = stdout?;
        let (stderr, stderr_oversized) = stderr?;
        if stdout_oversized || stderr_oversized {
            return Err(VtopError::Upload(format!(
                "{} {operation} exceeded the {}-byte output limit",
                self.backend, self.max_output_bytes
            )));
        }
        Ok(CapturedOutput {
            status,
            stdout,
            stderr,
        })
    }
}

struct CapturedOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Continue draining after the storage cap is reached so a finite child does
/// not deadlock on a full pipe. Memory remains bounded; an infinite producer
/// is terminated by the policy timeout.
async fn capture_bounded<R>(mut reader: R, max_bytes: usize) -> Result<(Vec<u8>, bool), VtopError>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::with_capacity(max_bytes.min(64 * 1024));
    let mut oversized = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(captured.len());
        let keep = remaining.min(read);
        captured.extend_from_slice(&chunk[..keep]);
        oversized |= keep < read;
    }
    Ok((captured, oversized))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vtop_core::config::UploadConfig;

    static NEXT_ENV: AtomicUsize = AtomicUsize::new(0);

    fn unique_env(prefix: &str) -> String {
        format!(
            "{prefix}_{}_{}",
            std::process::id(),
            NEXT_ENV.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn executable_script(body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        (dir, path)
    }

    fn config(path: &std::path::Path) -> UploadConfig {
        UploadConfig {
            backend: "awscli".into(),
            bucket: "test".into(),
            prefix: String::new(),
            endpoint_url: None,
            region: "us-east-1".into(),
            force_path_style: false,
            verify_tls: true,
            profile: None,
            command_binary: Some(path.to_string_lossy().into_owned()),
            command_timeout_seconds: 2,
            command_max_output_bytes: 1024,
            command_env_allowlist: Vec::new(),
            create_bucket: false,
            local_path: None,
            require_strong_verification: true,
        }
    }

    #[tokio::test]
    async fn clears_environment_and_copies_only_allowlisted_names() {
        let allowed = unique_env("VTOP_ALLOWED");
        let denied = unique_env("VTOP_DENIED");
        std::env::set_var(&allowed, "kept");
        std::env::set_var(&denied, "secret");
        let script = format!("printf '%s|%s' \"${{{allowed}-unset}}\" \"${{{denied}-unset}}\"");
        let (_dir, path) = executable_script(&script);
        let mut cfg = config(&path);
        cfg.command_env_allowlist.push(allowed.clone());
        let policy = CommandPolicy::from_config(&cfg, "test").unwrap();
        let mut command = policy.command();
        let output = policy
            .output(&mut command, "environment test")
            .await
            .unwrap();
        std::env::remove_var(&allowed);
        std::env::remove_var(&denied);
        assert_eq!(output, "kept|unset");
    }

    #[tokio::test]
    async fn version_identity_accepts_stderr_and_rejects_the_wrong_tool() {
        let credential = unique_env("VTOP_VERSION_SECRET");
        std::env::set_var(&credential, "must-not-reach-version-probe");
        let script = format!(
            "test \"${{{credential}-unset}}\" = unset || exit 9\necho 'aws-cli/2.22.0' >&2"
        );
        let (_dir, path) = executable_script(&script);
        let mut cfg = config(&path);
        cfg.command_env_allowlist.push(credential.clone());
        let policy = CommandPolicy::from_config(&cfg, "aws cli").unwrap();
        policy.verify_version("aws-cli/").await.unwrap();
        let error = policy.verify_version("s3cmd version").await.unwrap_err();
        std::env::remove_var(credential);
        assert!(error.to_string().contains("did not identify"));
    }

    #[tokio::test]
    async fn timeout_kills_a_hung_child() {
        let (_dir, path) = executable_script("exec /bin/sleep 5");
        let mut policy = CommandPolicy::from_config(&config(&path), "test").unwrap();
        policy.timeout = Duration::from_millis(50);
        let mut command = policy.command();
        let started = std::time::Instant::now();
        let error = policy.run(&mut command, "hang test").await.unwrap_err();
        assert!(error.to_string().contains("timeout"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn captured_output_is_bounded() {
        let (_dir, path) = executable_script("printf '123456789'");
        let mut cfg = config(&path);
        cfg.command_max_output_bytes = 8;
        let policy = CommandPolicy::from_config(&cfg, "test").unwrap();
        let mut command = policy.command();
        let error = policy
            .output(&mut command, "output test")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("8-byte output limit"));
    }

    #[test]
    fn allowlisted_variables_must_exist_and_be_unique() {
        let (_dir, path) = executable_script("exit 0");
        let missing = unique_env("VTOP_MISSING");
        let mut cfg = config(&path);
        cfg.command_env_allowlist = vec![missing.clone()];
        let error = CommandPolicy::from_config(&cfg, "test").err().unwrap();
        assert!(error.to_string().contains("is not set"));

        std::env::set_var(&missing, "value");
        cfg.command_env_allowlist.push(missing.clone());
        let error = CommandPolicy::from_config(&cfg, "test").err().unwrap();
        assert!(error.to_string().contains("duplicate"));
        std::env::remove_var(missing);
    }
}
