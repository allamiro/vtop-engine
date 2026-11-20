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

    /// Run the tool and keep what it wrote.
    ///
    /// The call is over when the TOOL exits, not when every copy of its
    /// pipes closes (#593). This used to join the exit with both drains, so
    /// a descendant that inherited stdout or stderr — a backgrounded helper,
    /// a credential process — kept a tool that had already exited "running"
    /// until `upload.command_timeout_seconds` (300 s by default) ran out,
    /// and the timeout then threw away everything captured, a `SlowDown`
    /// included. Now the exit is watched while the pipes drain; once it is
    /// in, the drains get `EXITED_DRAIN_GRACE` (never past the deadline) to
    /// collect what the tool wrote before exiting, and what they hold then
    /// is the output. A timeout reports that output too.
    ///
    /// The tool runs in its own process group, which is killed on a timeout,
    /// when the call is dropped mid-run, and when something is still holding
    /// the pipes after the tool exited; `kill_on_drop` alone ends only the
    /// tool and left the holder running.
    async fn capture(
        &self,
        command: &mut Command,
        operation: &str,
    ) -> Result<CapturedOutput, VtopError> {
        // Fixed before the spawn: running the tool and draining what it
        // wrote spend from this one deadline (#529 review).
        let deadline = crate::base::command_deadline(self.timeout);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child =
            crate::base::spawn_retrying_text_busy(command, self.backend, operation).await?;
        // Declared after `child`, so on a cancelled call it is dropped first
        // and kills the group while the tool is still unreaped.
        let mut group = ToolGroup::of(&child);
        let stdout = child.stdout.take().ok_or_else(|| {
            VtopError::Upload(format!("{} {operation} stdout unavailable", self.backend))
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            VtopError::Upload(format!("{} {operation} stderr unavailable", self.backend))
        })?;
        let max = self.max_output_bytes;
        let mut kept_stdout = KeptOutput::default();
        let mut kept_stderr = KeptOutput::default();
        let (exit, stdout_end, stderr_end) = {
            let stdout_drain = drain_bounded(stdout, max, &mut kept_stdout);
            let stderr_drain = drain_bounded(stderr, max, &mut kept_stderr);
            tokio::pin!(stdout_drain, stderr_drain);
            let mut stdout_end = None;
            let mut stderr_end = None;
            let exit = tokio::time::timeout_at(deadline, async {
                loop {
                    tokio::select! {
                        end = &mut stdout_drain, if stdout_end.is_none() => stdout_end = Some(end),
                        end = &mut stderr_drain, if stderr_end.is_none() => stderr_end = Some(end),
                        status = child.wait() => break status,
                    }
                }
            })
            .await;
            if exit.is_ok() {
                let until = tokio::time::Instant::now()
                    .checked_add(crate::base::EXITED_DRAIN_GRACE)
                    .map_or(deadline, |grace| grace.min(deadline));
                // Both unfinished drains are polled TOGETHER (review of
                // #615): awaiting one and then the other let a descendant
                // holding stdout spend the whole grace while the tool's last
                // stderr line sat unread in the pipe.
                let _ = tokio::time::timeout_at(until, async {
                    while stdout_end.is_none() || stderr_end.is_none() {
                        tokio::select! {
                            end = &mut stdout_drain, if stdout_end.is_none() => stdout_end = Some(end),
                            end = &mut stderr_drain, if stderr_end.is_none() => stderr_end = Some(end),
                        }
                    }
                })
                .await;
            }
            (exit, stdout_end, stderr_end)
        };

        let status = match exit {
            Ok(status) => {
                // A failed wait proves nothing about the tool, so the group
                // stays armed for the drop below.
                if status.is_ok() {
                    group.reaped();
                }
                status
            }
            Err(_) => {
                // The tool is still running, so its group is certainly the
                // one it was started in: end the tool and everything it
                // started, then report what they had written.
                group.kill();
                let _ = child.kill().await;
                let _ = child.wait().await;
                group.reaped();
                let detail = format!(
                    "{} {operation} exceeded the {}s timeout",
                    self.backend,
                    self.timeout.as_secs()
                );
                return Err(failure_with_evidence(
                    detail,
                    &kept_stdout.bytes,
                    &kept_stderr.bytes,
                ));
            }
        };
        if !reached_eof(&stdout_end) || !reached_eof(&stderr_end) {
            // The tool has exited, yet a pipe was not read to its end: a
            // descendant may still hold it. End the group rather than leave
            // it running with nobody reading its output.
            group.kill();
        }
        let status = status.map_err(|error| {
            VtopError::Upload(format!("waiting for {} {operation}: {error}", self.backend))
        })?;
        for end in [stdout_end, stderr_end].into_iter().flatten() {
            end?;
        }
        if kept_stdout.oversized || kept_stderr.oversized {
            return Err(VtopError::Upload(format!(
                "{} {operation} exceeded the {}-byte output limit",
                self.backend, self.max_output_bytes
            )));
        }
        Ok(CapturedOutput {
            status,
            stdout: kept_stdout.bytes,
            stderr: kept_stderr.bytes,
        })
    }
}

struct CapturedOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// The process group a tool runs in, made by `process_group(0)` so its id is
/// the tool's pid (#593). A tool's descendants inherit it unless they leave
/// on purpose, so killing the group ends what the tool started, including
/// whatever is still holding its stdout or stderr.
///
/// While the tool is unreaped its pid, and so the group id, cannot be
/// reused. After it is reaped the group is killed only when a pipe was not
/// read to EOF — normally because a descendant still holds it, almost always
/// from inside that group, which then still exists and still pins the id.
///
/// Unix only: elsewhere there is no group, and only the tool is killed.
struct ToolGroup {
    #[cfg(unix)]
    leader: Option<rustix::process::Pid>,
    unreaped: bool,
}

impl ToolGroup {
    fn of(child: &tokio::process::Child) -> Self {
        #[cfg(unix)]
        {
            // Never 1: `kill(-1, ..)` signals every process this one may
            // signal. A spawned child's pid never is 1, but the guard is free.
            let leader = child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .filter(|id| *id > 1)
                .and_then(rustix::process::Pid::from_raw);
            Self {
                leader,
                unreaped: true,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = child;
            Self { unreaped: true }
        }
    }

    fn kill(&self) {
        #[cfg(unix)]
        if let Some(leader) = self.leader {
            // ESRCH (the group is already gone) is the expected outcome
            // when nothing was left behind.
            let _ = rustix::process::kill_process_group(leader, rustix::process::Signal::KILL);
        }
    }

    fn reaped(&mut self) {
        self.unreaped = false;
    }
}

impl Drop for ToolGroup {
    fn drop(&mut self) {
        // A call dropped mid-run (shutdown, a caller's own timeout) never
        // reaches the explicit kills; `kill_on_drop` then ends only the tool.
        if self.unreaped {
            self.kill();
        }
    }
}

/// The error for a tool that exited non-zero: its exit status, the last
/// line it wrote (stderr first, stdout if stderr was silent — bounded, since
/// a tool can print a whole listing on failure), and the classification the
/// evidence supports (#102). Before this the message carried only the
/// status, so a `SlowDown` and a `NoSuchBucket` read identically.
fn exit_failure(backend: &str, operation: &str, output: &CapturedOutput) -> VtopError {
    failure_with_evidence(
        format!("{backend} {operation} exited with {}", output.status),
        &output.stdout,
        &output.stderr,
    )
}

/// `detail`, followed by the last line the tool wrote, classified by that
/// evidence. Shared by a non-zero exit and a timeout (#593): a tool that hung
/// after printing a `SlowDown` was reported as a bare timeout, which the
/// width controller read as an ordinary failure.
fn failure_with_evidence(detail: String, stdout: &[u8], stderr: &[u8]) -> VtopError {
    const MAX_EVIDENCE_CHARS: usize = 240;
    let stderr = String::from_utf8_lossy(stderr);
    let stdout = String::from_utf8_lossy(stdout);
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
        detail
    } else {
        format!("{detail}: {shown}")
    };
    crate::base::upload_failure(detail, &evidence)
}

/// Whether a drain read its pipe to EOF, which proves nothing else holds it.
/// A drain cut off by the grace (`None`) proves nothing, and neither does one
/// that FAILED (review of #615): it stopped reading with the pipe possibly
/// still held, and treating it as finished skipped the group kill.
fn reached_eof(end: &Option<std::io::Result<()>>) -> bool {
    matches!(end, Some(Ok(())))
}

/// One pipe's output as far as it has been read: the first `max_bytes`, and
/// whether more arrived. It lives outside the drain so that a drain cut off
/// by the deadline or the post-exit grace still leaves what it read (#593).
#[derive(Default)]
struct KeptOutput {
    bytes: Vec<u8>,
    oversized: bool,
}

/// Continue draining after the storage cap is reached so a finite child does
/// not deadlock on a full pipe. Memory remains bounded; an infinite producer
/// is terminated by the policy timeout.
async fn drain_bounded<R>(
    mut reader: R,
    max_bytes: usize,
    kept: &mut KeptOutput,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    kept.bytes.reserve(max_bytes.min(64 * 1024));
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        let remaining = max_bytes.saturating_sub(kept.bytes.len());
        let keep = remaining.min(read);
        kept.bytes.extend_from_slice(&chunk[..keep]);
        kept.oversized |= keep < read;
    }
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

    /// A fixture that leaves a descendant behind: `{bg}` in `body` is
    /// replaced by a line that starts `command` in the background and records
    /// its pid in the fixture's directory, so `kill_descendants` can end it
    /// once the test has its results. Without that the descendant outlives
    /// the test by up to its whole sleep. The pid file is named from the
    /// script's own path (`${0%/*}`, the tool is run by absolute path), so the
    /// directory never has to be quoted into the script: any path
    /// `VTOP_TEST_EXEC_TMPDIR` can name, apostrophes included, works.
    fn script_with_descendant(body: &str, command: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = executable_tempdir();
        let background = format!("{command} &\necho $! >> \"${{0%/*}}/descendants\"");
        let path = dir.path().join("tool");
        write_executable_script(&path, &body.replace("{bg}", &background)).unwrap();
        (dir, path)
    }

    /// Kill every descendant `script_with_descendant` recorded, returning how
    /// many there were. Called before any assertion, so a failing test does
    /// not leave them running either.
    fn kill_descendants(dir: &tempfile::TempDir) -> usize {
        let recorded = std::fs::read_to_string(dir.path().join("descendants")).unwrap_or_default();
        let pids: Vec<&str> = recorded.split_whitespace().collect();
        if !pids.is_empty() {
            let _ = std::process::Command::new("kill").args(&pids).status();
        }
        pids.len()
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
            request_timeout_seconds: 300,
            request_min_bytes_per_second: 4 * 1024,
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
            multipart_part_attempts: 3,
            transports: Default::default(),
            max_egress_bytes_per_second: None,
            max_concurrency_ceiling: None,
            object_lock: None,
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

    /// Review of #530: a tool that prints a throttle, leaves a descendant
    /// holding its STDOUT open, and exits non-zero is classified from its own
    /// exit and stderr - not timed out waiting for stdout to close.
    #[tokio::test]
    async fn a_failed_read_back_is_classified_while_a_descendant_holds_stdout() {
        use crate::base::{ObjectChecksum, UploadBackend};
        // `sleep 10 &` without a redirect inherits stdout AND stderr. Before
        // the fix the stdout drain waited out the whole deadline and the call
        // came back as a plain timeout; now the exit is classified from the
        // stderr already kept, within the usual evidence budget.
        let (dir, path) = script_with_descendant(
            "printf '%s\\n' 'An error occurred (SlowDown) when calling the GetObject operation' >&2\n\
             {bg}\n\
             exit 1",
            "sleep 10",
        );
        let policy = CommandPolicy::from_config(&config(&path), "test").unwrap();
        let bound = policy.timeout();
        let backend = crate::awscli_backend::AwsCliBackend::new(policy, None, None);
        let digest = vtop_core::checksum::sha256_bytes(b"");
        let timed = |operation: &'static str, elapsed: Duration, error: Option<VtopError>| {
            let error = error.unwrap_or_else(|| panic!("[{operation}] a failing tool must fail"));
            assert!(
                error.is_upload_throttle(),
                "[{operation}] the tool exited with a SlowDown on stderr, yet the call reported: \
                 {error}"
            );
            assert!(
                elapsed < bound.mul_f64(1.5),
                "[{operation}] took {elapsed:?} against a {bound:?} command timeout"
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
        let descendants = kill_descendants(&dir);
        timed("get_object_bounded", get_elapsed, get_error);
        timed("verify_object", verify_elapsed, verify_error);
        assert_eq!(
            descendants, 2,
            "each call's tool must have left its descendant"
        );
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
        let (dir, path) = script_with_descendant(
            "printf '%s\\n' 'An error occurred (SlowDown) when calling the GetObject operation' >&2\n\
             {bg}\n\
             sleep 1.6\n\
             exit 1",
            "sleep 10 >/dev/null",
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
        let descendants = kill_descendants(&dir);
        timed("get_object_bounded", get_elapsed, get_error);
        timed("verify_object", verify_elapsed, verify_error);
        assert_eq!(
            descendants, 2,
            "each call's tool must have left its descendant"
        );
    }

    /// Review of #530: a tool that fails AT ONCE, leaving a descendant that
    /// holds stderr open, is classified once it has exited - not after the
    /// stderr drain has waited out the rest of the command's deadline. With
    /// the default 300 s `upload.command_timeout_seconds` that wait made an
    /// immediately-failed read-back or verify hang for about five minutes,
    /// although its throttle line was already in the kept tail.
    #[tokio::test]
    async fn an_immediately_failed_read_back_returns_promptly_while_a_descendant_holds_stderr() {
        use crate::base::{ObjectChecksum, UploadBackend};
        // `sleep 30 >/dev/null &` keeps stderr only. The command timeout is
        // 20 s, far above the prompt bound, so waiting out the deadline (the
        // old behaviour, ~20 s) and returning on the tool's exit plus a short
        // grace are told apart by an order of magnitude, not by CI jitter.
        let (dir, path) = script_with_descendant(
            "printf '%s\\n' 'An error occurred (SlowDown) when calling the GetObject operation' >&2\n\
             {bg}\n\
             exit 1",
            "sleep 30 >/dev/null",
        );
        let mut cfg = config(&path);
        cfg.command_timeout_seconds = 20;
        let policy = CommandPolicy::from_config(&cfg, "test").unwrap();
        let bound = policy.timeout();
        let prompt = Duration::from_secs(6);
        let backend = crate::awscli_backend::AwsCliBackend::new(policy, None, None);
        let digest = vtop_core::checksum::sha256_bytes(b"");
        let timed = |operation: &'static str, elapsed: Duration, error: Option<VtopError>| {
            let error = error.unwrap_or_else(|| panic!("[{operation}] a failing tool must fail"));
            assert!(
                elapsed < prompt,
                "[{operation}] took {elapsed:?} (command timeout {bound:?}): the tool had already \
                 exited, yet the stderr evidence wait ran on toward the command deadline"
            );
            assert!(
                error.is_upload_throttle(),
                "[{operation}] the tool exited with a SlowDown on stderr, yet the call reported: \
                 {error}"
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
        let descendants = kill_descendants(&dir);
        timed("get_object_bounded", get_elapsed, get_error);
        timed("verify_object", verify_elapsed, verify_error);
        assert_eq!(
            descendants, 2,
            "each call's tool must have left its descendant"
        );
    }

    /// A descendant for `script_with_descendant` that holds the tool's stdout
    /// AND stderr (it redirects neither) and, once the test creates `go` in
    /// the fixture directory, writes `survived` there. Gating on a file the
    /// test controls, rather than on a sleep, keeps the check off the clock:
    /// a descendant still alive when the call has returned leaves `survived`
    /// within one poll of `go`, and one the call killed never does.
    const SURVIVOR: &str = "( while [ ! -e \"${0%/*}/go\" ]; do sleep 0.05; done; \
                            echo survived > \"${0%/*}/survived\" )";

    /// Whether a `SURVIVOR` descendant outlived the call: release it, then
    /// give it far longer than its 50 ms poll to answer. Under a loaded CI
    /// box a slow survivor can only make this read `false` (a missed
    /// detection), never a spurious `true`.
    async fn survivor_outlived_the_call(dir: &tempfile::TempDir) -> bool {
        std::fs::write(dir.path().join("go"), b"").unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        dir.path().join("survived").exists()
    }

    /// #593: the WRITE path (`capture`, behind every upload, manifest write,
    /// delete and `--version` probe) joined the tool's exit with BOTH pipe
    /// drains, so a descendant that inherited stdout/stderr kept the call
    /// open until `upload.command_timeout_seconds` (300 s by default) ran
    /// out, and then reported a successful tool as a timeout. The command
    /// timeout here is 20 s, so returning on the tool's exit plus the short
    /// drain grace and waiting out the deadline differ by an order of
    /// magnitude, not by CI jitter.
    #[tokio::test]
    async fn a_successful_write_returns_on_the_tools_exit_while_a_descendant_holds_its_output() {
        let (dir, path) = script_with_descendant(
            "printf 'upload: ./object to s3://bucket/key\\n'\n{bg}\nexit 0",
            SURVIVOR,
        );
        let mut cfg = config(&path);
        cfg.command_timeout_seconds = 20;
        let policy = CommandPolicy::from_config(&cfg, "test").unwrap();
        let bound = policy.timeout();
        let mut command = policy.command();
        let started = std::time::Instant::now();
        let outcome = policy.output(&mut command, "object upload").await;
        let elapsed = started.elapsed();
        let survived = survivor_outlived_the_call(&dir).await;
        let descendants = kill_descendants(&dir);
        assert_eq!(descendants, 1, "the tool must have left its descendant");
        assert!(
            elapsed < Duration::from_secs(6),
            "took {elapsed:?} (command timeout {bound:?}): the tool had already exited 0, yet \
             the call waited for a descendant to close the pipes it inherited"
        );
        let output = outcome.unwrap_or_else(|error| {
            panic!("the tool exited 0, yet the write was reported as failed: {error}")
        });
        assert!(
            output.contains("upload: ./object to s3://bucket/key"),
            "the tool's own stdout was lost: {output:?}"
        );
        assert!(
            !survived,
            "the descendant still holding the tool's pipes outlived the call: it was not in a \
             process group the call ended"
        );
    }

    /// #593, the evidence half: a write the store THROTTLED, whose tool
    /// exited at once but left a descendant holding stderr, used to come back
    /// after the whole deadline as a bare `exceeded the 20s timeout` - a
    /// plain failure, telling the width controller the store was fine while
    /// it was asking for less (the misclassification #529 fixed for the read
    /// path). The same shape as #530's read-back test, driving an upload.
    #[tokio::test]
    async fn a_throttled_write_is_classified_promptly_while_a_descendant_holds_stderr() {
        use crate::base::UploadBackend;
        let (dir, path) = script_with_descendant(
            "printf '%s\\n' 'An error occurred (SlowDown) when calling the PutObject operation' >&2\n\
             {bg}\n\
             exit 1",
            "sleep 30 >/dev/null",
        );
        let mut cfg = config(&path);
        cfg.command_timeout_seconds = 20;
        let policy = CommandPolicy::from_config(&cfg, "test").unwrap();
        let bound = policy.timeout();
        let backend = crate::awscli_backend::AwsCliBackend::new(policy, None, None);
        let object = tempfile::NamedTempFile::new().unwrap();
        let started = std::time::Instant::now();
        let outcome = backend
            .put_object(object.path(), "s3://bucket/key", None)
            .await;
        let elapsed = started.elapsed();
        let descendants = kill_descendants(&dir);
        assert_eq!(descendants, 1, "the tool must have left its descendant");
        let error = outcome.expect_err("a tool that exited 1 must fail the upload");
        assert!(
            elapsed < Duration::from_secs(6),
            "took {elapsed:?} (command timeout {bound:?}): the tool had already exited, yet the \
             stderr drain ran on toward the command deadline"
        );
        assert!(
            error.is_upload_throttle(),
            "the tool exited with a SlowDown on stderr, yet the upload reported: {error}"
        );
        assert!(
            error.to_string().contains("SlowDown"),
            "the throttle line is the evidence and must be in the message: {error}"
        );
    }

    /// Review of #615: the post-exit grace awaited the stdout drain and THEN
    /// the stderr drain. When the tool's exit won the race against the read
    /// of its last stderr bytes, and a descendant held stdout, the stdout
    /// wait spent the whole grace, stderr was never polled again, and the
    /// throttle line sitting in the pipe was dropped. Whether the exit wins
    /// is a coin flip per call (`select!` picks among ready branches at
    /// random), so one call proves little; many concurrent calls make the
    /// old order fail essentially every run, while each call still costs only
    /// the one shared 1 s grace.
    #[tokio::test]
    async fn a_descendant_holding_stdout_cannot_cost_a_write_its_final_stderr_line() {
        use crate::base::UploadBackend;
        const CALLS: usize = 24;
        let (dir, path) = script_with_descendant(
            "{bg}\n\
             printf '%s\\n' 'An error occurred (SlowDown) when calling the PutObject operation' >&2\n\
             exit 1",
            "sleep 30 2>/dev/null",
        );
        let mut cfg = config(&path);
        cfg.command_timeout_seconds = 20;
        let backend = crate::awscli_backend::AwsCliBackend::new(
            CommandPolicy::from_config(&cfg, "test").unwrap(),
            None,
            None,
        );
        let object = tempfile::NamedTempFile::new().unwrap();
        let started = std::time::Instant::now();
        let outcomes = futures::future::join_all(
            (0..CALLS).map(|_| backend.put_object(object.path(), "s3://bucket/key", None)),
        )
        .await;
        let elapsed = started.elapsed();
        let descendants = kill_descendants(&dir);
        assert_eq!(
            descendants, CALLS,
            "each call's tool must have left its descendant"
        );
        assert!(
            elapsed < Duration::from_secs(10),
            "{CALLS} concurrent calls took {elapsed:?} against a 20 s command timeout"
        );
        let lost: Vec<String> = outcomes
            .into_iter()
            .map(|outcome| outcome.expect_err("a tool that exited 1 must fail the upload"))
            .filter(|error| !error.is_upload_throttle() || !error.to_string().contains("SlowDown"))
            .map(|error| error.to_string())
            .collect();
        assert!(
            lost.is_empty(),
            "{} of {CALLS} calls lost the tool's final stderr line while a descendant held \
             stdout through the grace; first: {:?}",
            lost.len(),
            lost.first()
        );
    }

    /// Review of #615: only a drain that read its pipe to EOF lets the
    /// tool's group live. A failed read used to count as finished, so the
    /// group kill was skipped with the pipe's holder possibly alive. A pipe
    /// read error cannot be provoked from a shell fixture, so the rule is
    /// pinned here, on the predicate `capture` uses.
    #[test]
    fn only_a_drain_that_reached_eof_spares_the_tools_group() {
        assert!(reached_eof(&Some(Ok(()))));
        assert!(
            !reached_eof(&None),
            "a drain cut off by the grace left the pipe held"
        );
        assert!(
            !reached_eof(&Some(Err(std::io::Error::other("read failed")))),
            "a failed drain stopped reading with the pipe possibly still held"
        );
    }

    /// #593: a write that really does time out reported only `exceeded the
    /// Ns timeout`, discarding everything the tool had printed, and killed
    /// only the tool - a descendant it started kept running. Both what each
    /// pipe carried and the descendant's end are checked here.
    #[tokio::test]
    async fn a_timed_out_write_reports_what_it_captured_and_ends_the_tools_descendants() {
        let throttled = script_with_descendant(
            "printf 'upload: 12%% done\\n'\n\
             printf '%s\\n' 'An error occurred (SlowDown) when calling the PutObject operation' >&2\n\
             {bg}\n\
             exec /bin/sleep 30",
            SURVIVOR,
        );
        let quiet_stderr = executable_script("printf 'upload: 47%% done\\n'\nexec /bin/sleep 30");
        let policy_for = |path: &std::path::Path| {
            let mut policy = CommandPolicy::from_config(&config(path), "test").unwrap();
            policy.timeout = Duration::from_secs(1);
            policy
        };
        let throttled_policy = policy_for(&throttled.1);
        let quiet_policy = policy_for(&quiet_stderr.1);
        let mut throttled_command = throttled_policy.command();
        let mut quiet_command = quiet_policy.command();
        let started = std::time::Instant::now();
        let (throttled_outcome, quiet_outcome) = tokio::join!(
            throttled_policy.run(&mut throttled_command, "object upload"),
            quiet_policy.run(&mut quiet_command, "object upload"),
        );
        let elapsed = started.elapsed();
        let survived = survivor_outlived_the_call(&throttled.0).await;
        let descendants = kill_descendants(&throttled.0);
        assert_eq!(descendants, 1, "the tool must have left its descendant");
        assert!(
            elapsed < Duration::from_secs(6),
            "a 1 s command timeout took {elapsed:?} to fire"
        );

        let error = throttled_outcome.expect_err("a hung tool must fail the upload");
        let message = error.to_string();
        assert!(message.contains("timeout"), "not a timeout: {message}");
        assert!(
            message.contains("SlowDown"),
            "the stderr captured before the timeout was discarded: {message}"
        );
        assert!(
            error.is_upload_throttle(),
            "the tool's last word before it hung was a SlowDown, yet the timeout reported: \
             {error}"
        );

        let error = quiet_outcome.expect_err("a hung tool must fail the upload");
        let message = error.to_string();
        assert!(message.contains("timeout"), "not a timeout: {message}");
        assert!(
            message.contains("upload: 47% done"),
            "with stderr silent, the stdout captured before the timeout is the evidence, and \
             it was discarded: {message}"
        );
        assert!(
            !error.is_upload_throttle(),
            "progress output is not a throttle: {error}"
        );

        assert!(
            !survived,
            "the timed-out tool's descendant outlived the call: the timeout killed only the tool"
        );
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
