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
            Err(exit_failure(self.backend, operation, &output))
        }
    }

    pub(crate) async fn output(
        &self,
        command: &mut Command,
        operation: &str,
    ) -> Result<String, VtopError> {
        let output = self.capture(command, operation).await?;
        if !output.status.success() {
            return Err(exit_failure(self.backend, operation, &output));
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
        let mut busy_retries = 0_u32;
        let mut child = loop {
            match command.spawn() {
                Ok(child) => break child,
                Err(error) if error.raw_os_error() == Some(26) && busy_retries < 4 => {
                    busy_retries += 1;
                    tokio::time::sleep(Duration::from_millis(10 * u64::from(busy_retries))).await;
                }
                Err(error) => {
                    let hint = if error.kind() == std::io::ErrorKind::PermissionDenied {
                        "; verify that the command is executable and its filesystem is not mounted noexec"
                    } else if error.raw_os_error() == Some(26) {
                        "; the executable remained busy after bounded retries; install/replace it atomically and retry"
                    } else {
                        ""
                    };
                    return Err(VtopError::Upload(format!(
                        "spawning {} {operation}: {error}{hint}",
                        self.backend
                    )));
                }
            }
        };
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

/// The error for a tool that exited non-zero: its exit status, the last
/// line it wrote (stderr first, stdout if stderr was silent — bounded, since
/// a tool can print a whole listing on failure), and the classification the
/// evidence supports (#102). Before this the message carried only the
/// status, so a `SlowDown` and a `NoSuchBucket` read identically.
fn exit_failure(backend: &str, operation: &str, output: &CapturedOutput) -> VtopError {
    const MAX_EVIDENCE_CHARS: usize = 240;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let evidence = if stderr.trim().is_empty() {
        stdout
    } else {
        stderr
    };
    let last_line = evidence
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    let shown: String = last_line.chars().take(MAX_EVIDENCE_CHARS).collect();
    let detail = if shown.is_empty() {
        format!("{backend} {operation} exited with {}", output.status)
    } else {
        format!(
            "{backend} {operation} exited with {}: {shown}",
            output.status
        )
    };
    crate::base::upload_failure(detail, &evidence)
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

    fn write_executable_script(path: &std::path::Path, body: &str) -> std::io::Result<()> {
        use std::io::Write;
        let tmp = path.with_extension("tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            write!(file, "#!/bin/sh\n{body}\n")?;
            file.sync_all()?;
        }
        let mut permissions = std::fs::metadata(&tmp)?.permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&tmp, permissions)?;
        std::fs::rename(&tmp, path)
    }

    fn executable_tempdir() -> tempfile::TempDir {
        // Prefer an explicit override, then the test binary's filesystem, and
        // finally the system temp directory. Hardened builders commonly mount
        // /tmp with noexec; other builders make the target directory read-only.
        let explicit = std::env::var_os("VTOP_TEST_EXEC_TMPDIR").map(PathBuf::from);
        let mut candidates = Vec::new();
        if let Some(path) = explicit.as_ref() {
            candidates.push((path.clone(), true));
        } else {
            if let Ok(binary) = std::env::current_exe() {
                if let Some(parent) = binary.parent() {
                    // The running test binary already proves this mount allows
                    // execution; only writability still needs to be checked.
                    candidates.push((parent.to_path_buf(), false));
                }
            }
            let system_temp = std::env::temp_dir();
            if !candidates.iter().any(|(path, _)| path == &system_temp) {
                candidates.push((system_temp, true));
            }
        }

        let mut failures = Vec::new();
        for (root, needs_exec_probe) in candidates {
            let dir = match tempfile::Builder::new()
                .prefix("vtop-exec-test-")
                .tempdir_in(&root)
            {
                Ok(dir) => dir,
                Err(error) => {
                    failures.push(format!("{}: not writable ({error})", root.display()));
                    continue;
                }
            };
            if !needs_exec_probe {
                return dir;
            }
            let probe = dir.path().join("exec-probe");
            if let Err(error) = write_executable_script(&probe, "exit 0") {
                failures.push(format!("{}: cannot create probe ({error})", root.display()));
                continue;
            }
            let mut status_result = std::process::Command::new(&probe).status();
            for delay_ms in [5, 10, 20, 40] {
                if !matches!(&status_result, Err(error) if error.raw_os_error() == Some(26)) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(delay_ms));
                status_result = std::process::Command::new(&probe).status();
            }
            match status_result {
                Ok(status) if status.success() => {
                    let _ = std::fs::remove_file(probe);
                    return dir;
                }
                Ok(status) => failures.push(format!(
                    "{}: executable probe exited with {status}",
                    root.display()
                )),
                Err(error) => failures.push(format!(
                    "{}: cannot execute probe ({error}); the filesystem may be mounted noexec",
                    root.display()
                )),
            }
        }

        panic!(
            "no writable, exec-enabled directory is available for command-backend test fixtures. \
             Set VTOP_TEST_EXEC_TMPDIR to a writable directory on an exec-enabled filesystem. \
             Checked: {}",
            failures.join("; ")
        );
    }

    fn executable_script(body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = executable_tempdir();
        let path = dir.path().join("tool");
        write_executable_script(&path, body).unwrap();
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
            transport: "tcp_tls".into(),
            profile: None,
            command_binary: Some(path.to_string_lossy().into_owned()),
            command_timeout_seconds: 2,
            command_max_output_bytes: 1024,
            command_env_allowlist: Vec::new(),
            create_bucket: false,
            local_path: None,
            require_strong_verification: true,
            require_object_versioning: false,
            multipart_part_size_bytes: 8 * 1024 * 1024,
            multipart_threshold_bytes: 8 * 1024 * 1024,
            multipart_max_parallelism: 4,
            multipart_abandon_after_secs: 24 * 60 * 60,
            transports: Default::default(),
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
        // The child sleeps 30s against a 50ms timeout: the wall-clock bound
        // only has to prove the timeout fired instead of waiting the child
        // out. Keep the margin wide — a tight bound measures CI scheduling
        // (process spawn + kill under load), not the policy under test.
        let (_dir, path) = executable_script("exec /bin/sleep 30");
        let mut policy = CommandPolicy::from_config(&config(&path), "test").unwrap();
        policy.timeout = Duration::from_millis(50);
        let mut command = policy.command();
        let started = std::time::Instant::now();
        let error = policy.run(&mut command, "hang test").await.unwrap_err();
        assert!(
            error.to_string().contains("timeout"),
            "unexpected error: {error}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "timeout did not fire: elapsed {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn captured_output_is_bounded() {
        // The fixture helper syncs and atomically renames the script, avoiding
        // ETXTBSY races while also exercising the same configured-command path
        // used by the compatibility backends.
        let (_dir, path) = executable_script("printf '123456789'");
        let mut cfg = config(&path);
        cfg.command_max_output_bytes = 8;
        let policy = CommandPolicy::from_config(&cfg, "test").unwrap();
        let mut command = policy.command();
        let error = policy
            .output(&mut command, "output test")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("8-byte output limit"),
            "unexpected error: {error}"
        );
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

    #[tokio::test]
    async fn every_command_backend_reports_a_store_throttle_as_a_throttle_on_every_request_path() {
        use crate::base::{ObjectChecksum, UploadBackend};
        type Build = fn(CommandPolicy) -> Box<dyn UploadBackend>;
        let table: [(&str, Build, &[&str]); 3] = [
            (
                "awscli",
                |policy| Box::new(crate::awscli_backend::AwsCliBackend::new(policy, None, None)),
                &[
                    "An error occurred (SlowDown) when calling the PutObject operation (reached max retries: 4): Please reduce your request rate.",
                    "An error occurred (Throttling) when calling the GetObject operation",
                    "HTTP 429 Too Many Requests",
                ],
            ),
            (
                "s3cmd",
                |policy| Box::new(crate::s3cmd_backend::S3cmdBackend::new(policy, None)),
                &[
                    "ERROR: S3 error: 503 (SlowDown): Please reduce your request rate.",
                    "ERROR: S3 error: 429 (TooManyRequests)",
                ],
            ),
            (
                "minio",
                |policy| Box::new(crate::minio_backend::MinioBackend::new(policy, "local")),
                &[
                    "mc: <ERROR> Failed to copy `x`. Please reduce your request rate.",
                    "503 Service Unavailable",
                ],
            ),
        ];
        let object = tempfile::NamedTempFile::new().unwrap();
        let digest = vtop_core::checksum::sha256_bytes(b"");
        for (backend_name, build, throttles) in table {
            let ordinary = "An error occurred (AccessDenied) when calling the PutObject operation";
            let cases = throttles
                .iter()
                .map(|line| (*line, true))
                .chain(std::iter::once((ordinary, false)));
            for (line, is_throttle) in cases {
                let (_dir, path) =
                    executable_script(&format!("printf '%s\\n' '{line}' >&2\nexit 1"));
                let backend = build(CommandPolicy::from_config(&config(&path), "test").unwrap());
                let uri = "s3://bucket/key";
                let outcomes = [
                    (
                        "put_object",
                        backend.put_object(object.path(), uri, None).await.err(),
                    ),
                    ("head_object", backend.head_object(uri).await.err()),
                    (
                        "get_object_bounded",
                        backend.get_object_bounded(uri, 1024).await.err(),
                    ),
                    (
                        "verify_object",
                        backend
                            .verify_object(uri, 0, Some(ObjectChecksum::new("sha256", &digest)))
                            .await
                            .err(),
                    ),
                ];
                for (operation, error) in outcomes {
                    let error = error.unwrap_or_else(|| {
                        panic!("[{backend_name} {operation}] a failing tool must fail the call")
                    });
                    assert_eq!(
                        error.is_upload_throttle(),
                        is_throttle,
                        "[{backend_name} {operation}] {line:?} was classified wrongly: {error}. \
                         A throttle reported as a plain failure tells the width controller the \
                         store is fine while it asks for less"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn a_grandchild_holding_stderr_cannot_stretch_a_read_back_past_its_command_timeout() {
        // `upload.command_timeout_seconds` bounds the whole invocation. The
        // stderr evidence is awaited after the tool exits, and a grandchild the
        // tool left behind keeps the pipe open; giving that wait a FRESH
        // timeout let a download or verify take nearly twice the configured
        // bound. The tool here spends most of its 2 s budget before failing,
        // so a second full wait is plainly visible, while the throttle line it
        // printed first is already in the kept tail and must still count.
        use crate::base::{ObjectChecksum, UploadBackend};
        let (_dir, path) = executable_script(
            "printf '%s\\n' 'An error occurred (SlowDown) when calling the GetObject operation' >&2\n\
             sleep 10 >/dev/null &\n\
             sleep 1.6\n\
             exit 1",
        );
        let policy = CommandPolicy::from_config(&config(&path), "test").unwrap();
        let bound = policy.timeout();
        let backend = crate::awscli_backend::AwsCliBackend::new(policy, None, None);
        let digest = vtop_core::checksum::sha256_bytes(b"");
        let timed = |operation: &'static str, elapsed: Duration, error: Option<VtopError>| {
            let error = error.unwrap_or_else(|| panic!("[{operation}] a failing tool must fail"));
            assert!(
                elapsed < bound.mul_f64(1.5),
                "[{operation}] took {elapsed:?} against a {bound:?} command timeout: the evidence \
                 wait restarted the clock instead of spending what the command left"
            );
            assert!(
                error.is_upload_throttle(),
                "[{operation}] the SlowDown line was printed before the deadline, yet the \
                 timed-out evidence wait discarded it: {error}"
            );
        };
        let get = async {
            let started = std::time::Instant::now();
            let error = backend
                .get_object_bounded("s3://bucket/key", 1024)
                .await
                .err();
            (started.elapsed(), error)
        };
        let verify = async {
            let started = std::time::Instant::now();
            let error = backend
                .verify_object(
                    "s3://bucket/key",
                    0,
                    Some(ObjectChecksum::new("sha256", &digest)),
                )
                .await
                .err();
            (started.elapsed(), error)
        };
        let ((get_elapsed, get_error), (verify_elapsed, verify_error)) = tokio::join!(get, verify);
        timed("get_object_bounded", get_elapsed, get_error);
        timed("verify_object", verify_elapsed, verify_error);
    }

    #[tokio::test]
    async fn a_throttle_after_a_flood_of_progress_output_is_still_read_as_a_throttle() {
        // The read-back keeps a bounded amount of stderr, and the classifier
        // reads its LAST line. Keeping the first 64 KiB instead would discard
        // exactly that line for a tool that prints progress before it fails,
        // and the throttle would reach the controller as an ordinary failure.
        use crate::base::UploadBackend;
        let (_dir, path) = executable_script(
            "i=0; while [ $i -lt 4000 ]; do printf 'progress %060d\\n' $i >&2; i=$((i+1)); done\n\
             printf '%s\\n' 'An error occurred (SlowDown) when calling the GetObject operation' >&2\n\
             exit 1",
        );
        let backend = crate::awscli_backend::AwsCliBackend::new(
            CommandPolicy::from_config(&config(&path), "test").unwrap(),
            None,
            None,
        );
        let error = backend
            .get_object_bounded("s3://bucket/key", 1024)
            .await
            .expect_err("a failing tool must fail the read-back");
        assert!(
            error.is_upload_throttle(),
            "~290 KiB of progress before the SlowDown line pushed it past the kept stderr, \
             and the throttle was reported as a plain failure: {error}"
        );
    }
}
