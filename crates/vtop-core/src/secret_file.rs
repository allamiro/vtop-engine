//! Permission checks on the files that hold secrets or the ledger (#733).
//!
//! A manifest MAC key file, the `engine.state_store` secret file, the SQLite
//! ledger (with its `-wal` and `-shm` files) and the audit file are refused
//! when other local users can reach them:
//!
//! | The file's mode, as the engine sees it | Result |
//! |---|---|
//! | writable by group or other | refused; no setting allows it |
//! | readable by other | refused, unless the parent directory is closed |
//! | readable by group, not other | refused, unless the parent directory is closed, the file is a read-only Kubernetes secret mount, or it is the audit file |
//! | owner only | accepted |
//!
//! "Closed" means the directory that holds the file is owned by the engine's
//! user and has no group or other bits: nobody else can reach the file, so its
//! own read bits expose nothing. That keeps every SQLite ledger created at
//! `0644` inside a `0700` data directory starting after the upgrade.
//!
//! The Kubernetes case exists because the kubelet adds the group read bit to
//! a Secret volume when the pod sets `fsGroup` (which a non-root pod needs to
//! read the file at all), so `defaultMode: 0400` is `0440` on disk. Refusing
//! that would refuse the recommended setup. It is recognised narrowly: the
//! file is reached through the kubelet's atomic-writer layout (a `..data`
//! symlink naming the `..<timestamp>` directory that holds it), lives on
//! `tmpfs`, and the engine cannot write it. It only ever relaxes the group
//! read bit.
//!
//! `security.allow_loose_secret_permissions: true` accepts the readable cases
//! that would otherwise be refused, never the writable one. Each file accepted
//! that way is announced once per process: a WARN, a count on
//! `vtop_secret_file_permission_overrides_total`, and an audit record written
//! by the hook the binary installs ([`set_override_hook`]).
//!
//! The checks run on Unix only. Elsewhere there are no mode bits to read and
//! every file is accepted.
//!
//! The mode rule itself is [`judge`], a pure function of numbers, so the
//! table above is tested without a filesystem.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// The setting that accepts readable files, as an operator writes it.
pub const OVERRIDE_SETTING: &str = "security.allow_loose_secret_permissions";

/// What a file holds, which decides the rule for group read and the advice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretFileKind {
    /// `manifest.mac.keys[].file`: a manifest MAC key.
    ManifestKey,
    /// `engine.state_store: { file: ... }`: a connection string, which for
    /// PostgreSQL carries a password.
    StateStoreSecret,
    /// The SQLite ledger and its `-wal` and `-shm` files.
    StateStoreDatabase,
    /// The file `VTOP_AUDIT_LOG` names. Group read is accepted: a log
    /// shipper reading the trail through a shared group is the documented
    /// pattern, and the file holds no secret.
    AuditLog,
}

impl SecretFileKind {
    /// The value of the audit record's `file_kind` parameter.
    pub fn as_str(self) -> &'static str {
        match self {
            SecretFileKind::ManifestKey => "manifest_key",
            SecretFileKind::StateStoreSecret => "state_store_secret",
            SecretFileKind::StateStoreDatabase => "state_store_database",
            SecretFileKind::AuditLog => "audit_log",
        }
    }

    /// Whether the engine writes the file (so the advice is `0600`, not
    /// `0400`).
    fn engine_writes(self) -> bool {
        matches!(
            self,
            SecretFileKind::StateStoreDatabase | SecretFileKind::AuditLog
        )
    }

    /// What another user who could write the file could do.
    fn write_consequence(self) -> &'static str {
        match self {
            SecretFileKind::ManifestKey => "replace the manifest signing key",
            SecretFileKind::StateStoreSecret => "point the engine at another database",
            SecretFileKind::StateStoreDatabase => "rewrite the ledger",
            SecretFileKind::AuditLog => "rewrite the audit trail",
        }
    }

    /// What another user who could read the file would learn.
    fn read_consequence(self) -> &'static str {
        match self {
            SecretFileKind::ManifestKey => "the manifest signing key",
            SecretFileKind::StateStoreSecret => "the state-store connection string",
            SecretFileKind::StateStoreDatabase => "the ledger",
            SecretFileKind::AuditLog => "the audit trail",
        }
    }
}

/// One file to check, and how messages name it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretFile {
    pub kind: SecretFileKind,
    /// How a message names the file, for example `manifest key "q4-2026"`.
    pub label: String,
    pub path: PathBuf,
}

impl SecretFile {
    pub fn new(kind: SecretFileKind, label: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            label: label.into(),
            path: path.into(),
        }
    }
}

/// The SQLite ledger at `database` and the two files SQLite keeps beside it
/// in WAL mode. SQLite resolves a symlinked database before naming those
/// two, so they are looked for beside the file the link names.
pub fn sqlite_files(database: &Path) -> Vec<SecretFile> {
    let resolved = std::fs::canonicalize(database).unwrap_or_else(|_| database.to_path_buf());
    let mut files = vec![SecretFile::new(
        SecretFileKind::StateStoreDatabase,
        "the state-store database",
        database,
    )];
    for suffix in ["-wal", "-shm"] {
        let mut name = resolved.as_os_str().to_owned();
        name.push(suffix);
        files.push(SecretFile::new(
            SecretFileKind::StateStoreDatabase,
            format!("the state-store database's {suffix} file"),
            PathBuf::from(name),
        ));
    }
    files
}

/// How the checks are applied: the override, and the engine a record names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SecretPolicy {
    /// `security.allow_loose_secret_permissions`.
    pub allow_loose: bool,
    /// The engine's name, for the audit record of an override.
    pub engine: Option<String>,
}

impl SecretPolicy {
    /// The default rule, for a process with no engine config.
    pub fn strict() -> Self {
        Self::default()
    }
}

/// Who besides the owner a mode lets in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Exposure {
    pub group: bool,
    pub other: bool,
}

impl Exposure {
    fn any(self) -> bool {
        self.group || self.other
    }

    /// The value of the audit record's `exposed_to` parameter.
    pub fn as_str(self) -> &'static str {
        match (self.group, self.other) {
            (true, true) => "group,other",
            (true, false) => "group",
            (false, true) => "other",
            (false, false) => "none",
        }
    }

    fn who(self) -> &'static str {
        match (self.group, self.other) {
            (true, true) => "its group and every other user on this host",
            (true, false) => "the members of its group",
            _ => "every other user on this host",
        }
    }
}

/// Where the file sits, which can make its read bits harmless.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Placement {
    /// The directory holding the file is owned by the engine's user and has
    /// no group or other bits.
    pub parent_closed: bool,
    /// The file is a read-only Kubernetes secret mount (see the module
    /// documentation).
    pub orchestrator_mount: bool,
}

/// What [`judge`] decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Accepted,
    /// Readable by others, accepted only because of the override.
    Overridden(Exposure),
    /// Readable by others; the override would accept it.
    RefusedReadable(Exposure),
    /// Writable by others; nothing accepts it.
    RefusedWritable(Exposure),
}

/// The rule, on the permission bits of a file (`mode & 0o777`).
pub fn judge(kind: SecretFileKind, mode: u32, placement: Placement, allow_loose: bool) -> Verdict {
    let writable = Exposure {
        group: mode & 0o020 != 0,
        other: mode & 0o002 != 0,
    };
    if writable.any() {
        return Verdict::RefusedWritable(writable);
    }
    let readable = Exposure {
        group: mode & 0o040 != 0
            && !placement.parent_closed
            && !placement.orchestrator_mount
            && kind != SecretFileKind::AuditLog,
        other: mode & 0o004 != 0 && !placement.parent_closed,
    };
    if !readable.any() {
        Verdict::Accepted
    } else if allow_loose {
        Verdict::Overridden(readable)
    } else {
        Verdict::RefusedReadable(readable)
    }
}

/// Whether a file's directory is the kubelet's atomic-writer payload
/// directory: named `..<something>` (not `..data`), with the mount's `..data`
/// symlink pointing at it. `data_link` is where `..data` points, if it is a
/// symlink.
pub fn is_atomic_writer_dir(dir_name: &str, data_link: Option<&Path>) -> bool {
    dir_name.len() > 2
        && dir_name.starts_with("..")
        && dir_name != "..data"
        && data_link == Some(Path::new(dir_name))
}

/// A file refused for its permissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub file: SecretFile,
    pub mode: u32,
    pub writable: bool,
    pub exposure: Exposure,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = self.file.path.display();
        let label = &self.file.label;
        let mode = self.mode;
        let who = self.exposure.who();
        if self.writable {
            return write!(
                f,
                "{label}: {path} has mode {mode:04o}, which lets {who} write it and so \
                 {consequence}. Refused, and no setting accepts it: the file was not read and \
                 nothing was started. Remove the write bits (`chmod go-w {path}`) and check who \
                 owns the file and its directory (`ls -ln {path}`)",
                consequence = self.file.kind.write_consequence(),
            );
        }
        let wanted = if self.file.kind.engine_writes() {
            "0600"
        } else {
            "0400"
        };
        write!(
            f,
            "{label}: {path} has mode {mode:04o}, which lets {who} read {what}. Refused: the \
             file was not read and nothing was started. Run `chmod {wanted} {path}`, or close \
             its directory to other users (`chmod 0700` on a directory the engine's user owns)",
            what = self.file.kind.read_consequence(),
        )?;
        if !self.file.kind.engine_writes() {
            f.write_str(
                "; in Kubernetes mount the Secret with `defaultMode: 0400` (the kubelet makes it \
                 0440 when the pod sets `fsGroup`, which is accepted)",
            )?;
        }
        write!(
            f,
            ". To accept the exposure knowingly, set `{OVERRIDE_SETTING}: true`"
        )
    }
}

impl std::error::Error for Refusal {}

/// A file accepted only because of the override.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Override {
    pub file: SecretFile,
    pub mode: u32,
    pub exposure: Exposure,
    /// The engine's name, for the audit record.
    pub engine: Option<String>,
}

impl Override {
    /// The mode as an operator types it, `0644`.
    pub fn mode_text(&self) -> String {
        format!("{:04o}", self.mode)
    }

    /// The WARN line: what happened, what the engine did, what to do.
    pub fn message(&self) -> String {
        self.describe("Accepted because {setting} is true; the acceptance is in the audit trail")
    }

    /// The same for `vtopctl validate`, which starts nothing and records
    /// nothing.
    pub fn validate_warning(&self) -> String {
        self.describe(
            "The engine will accept it because {setting} is true, and record that in the audit \
             trail",
        )
    }

    fn describe(&self, accepted: &str) -> String {
        let path = self.file.path.display();
        let wanted = if self.file.kind.engine_writes() {
            "0600"
        } else {
            "0400"
        };
        format!(
            "{label}: {path} has mode {mode}, which lets {who} read {what}. {accepted}. Run \
             `chmod {wanted} {path}` and remove the setting to close the exposure",
            accepted = accepted.replace("{setting}", OVERRIDE_SETTING),
            label = self.file.label,
            mode = self.mode_text(),
            who = self.exposure.who(),
            what = self.file.kind.read_consequence(),
        )
    }
}

/// Check one file. A file that does not exist, or whose metadata cannot be
/// read, is not judged here: whoever reads it reports that, naming the path.
pub fn inspect(file: &SecretFile, policy: &SecretPolicy) -> Result<Option<Override>, Refusal> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Follows symlinks: the mode that matters is the file's, and a
        // Kubernetes secret is a symlink into the payload directory.
        let Ok(meta) = std::fs::metadata(&file.path) else {
            return Ok(None);
        };
        let mode = meta.permissions().mode() & 0o777;
        // Only a readable-but-not-writable mode needs to know where the file
        // sits; skip the extra calls for the common owner-only file.
        let placement = if mode & 0o044 != 0 && mode & 0o022 == 0 {
            placement(&file.path)
        } else {
            Placement::default()
        };
        match judge(file.kind, mode, placement, policy.allow_loose) {
            Verdict::Accepted => Ok(None),
            Verdict::Overridden(exposure) => Ok(Some(Override {
                file: file.clone(),
                mode,
                exposure,
                engine: policy.engine.clone(),
            })),
            Verdict::RefusedReadable(exposure) => Err(Refusal {
                file: file.clone(),
                mode,
                writable: false,
                exposure,
            }),
            Verdict::RefusedWritable(exposure) => Err(Refusal {
                file: file.clone(),
                mode,
                writable: true,
                exposure,
            }),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (file, policy);
        Ok(None)
    }
}

/// Check one file and announce an override ([`announce`]).
pub fn enforce(file: &SecretFile, policy: &SecretPolicy) -> Result<(), Refusal> {
    if let Some(accepted) = inspect(file, policy)? {
        announce(&accepted);
    }
    Ok(())
}

/// Every file at once, so an operator fixes them in one pass.
#[derive(Debug, Default)]
pub struct Report {
    pub overrides: Vec<Override>,
    pub refusals: Vec<Refusal>,
}

impl Report {
    /// The refusals as one message, or `None` when there are none.
    pub fn refusal_message(&self) -> Option<String> {
        match self.refusals.as_slice() {
            [] => None,
            [one] => Some(one.to_string()),
            many => Some(format!(
                "{} files are refused for their permissions:\n{}",
                many.len(),
                many.iter()
                    .map(|r| format!("  - {r}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            )),
        }
    }
}

/// Check every file without announcing anything: for `vtopctl validate`,
/// which changes nothing.
pub fn inspect_all(files: &[SecretFile], policy: &SecretPolicy) -> Report {
    let mut report = Report::default();
    for file in files {
        match inspect(file, policy) {
            Ok(Some(accepted)) => report.overrides.push(accepted),
            Ok(None) => {}
            Err(refused) => report.refusals.push(refused),
        }
    }
    report
}

/// Check every file; refuse with all of the offenders named, or announce
/// every override.
pub fn enforce_all(files: &[SecretFile], policy: &SecretPolicy) -> Result<(), String> {
    let report = inspect_all(files, policy);
    if let Some(message) = report.refusal_message() {
        return Err(message);
    }
    for accepted in &report.overrides {
        announce(accepted);
    }
    Ok(())
}

static ANNOUNCED: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());
static HOOK: OnceLock<fn(&Override)> = OnceLock::new();

/// Install what writes the audit record of an override. The first call wins;
/// the binary installs one before it reads any file.
pub fn set_override_hook(hook: fn(&Override)) {
    let _ = HOOK.set(hook);
}

/// Announce a file accepted under the override, once per file per process: a
/// WARN with the three parts, the counter, and the audit record. Never the
/// file's contents.
pub fn announce(accepted: &Override) {
    let first = ANNOUNCED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(accepted.file.path.clone());
    if !first {
        return;
    }
    tracing::warn!(
        path = %accepted.file.path.display(),
        mode = %accepted.mode_text(),
        file_kind = accepted.file.kind.as_str(),
        exposed_to = accepted.exposure.as_str(),
        "{}",
        accepted.message()
    );
    if let Some(metrics) = crate::telemetry::metrics() {
        metrics.secret_file_permission_overrides_total.inc();
    }
    if let Some(hook) = HOOK.get() {
        hook(accepted);
    }
}

#[cfg(unix)]
fn placement(path: &Path) -> Placement {
    let Ok(canonical) = std::fs::canonicalize(path) else {
        return Placement::default();
    };
    let parent_closed = canonical.parent().is_some_and(|parent| {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(parent)
            .is_ok_and(|m| m.mode() & 0o077 == 0 && m.uid() == rustix::process::geteuid().as_raw())
    });
    Placement {
        parent_closed,
        orchestrator_mount: orchestrator_mount(&canonical),
    }
}

/// The file (by its canonical path) is in the kubelet's atomic-writer
/// layout, on tmpfs, and not writable by this process.
#[cfg(target_os = "linux")]
fn orchestrator_mount(canonical: &Path) -> bool {
    /// `TMPFS_MAGIC` from `linux/magic.h`.
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    let Some(dir) = canonical.parent() else {
        return false;
    };
    let (Some(name), Some(mount)) = (dir.file_name().and_then(|n| n.to_str()), dir.parent()) else {
        return false;
    };
    let link = std::fs::read_link(mount.join("..data")).ok();
    if !is_atomic_writer_dir(name, link.as_deref()) {
        return false;
    }
    #[allow(clippy::unnecessary_cast)] // f_type's width differs by target.
    let on_tmpfs = rustix::fs::statfs(canonical).is_ok_and(|s| s.f_type as i64 == TMPFS_MAGIC);
    on_tmpfs && rustix::fs::access(canonical, rustix::fs::Access::WRITE_OK).is_err()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn orchestrator_mount(_canonical: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use SecretFileKind::*;

    const OPEN: Placement = Placement {
        parent_closed: false,
        orchestrator_mount: false,
    };
    const CLOSED: Placement = Placement {
        parent_closed: true,
        orchestrator_mount: false,
    };
    const MOUNT: Placement = Placement {
        parent_closed: false,
        orchestrator_mount: true,
    };

    fn exposure(group: bool, other: bool) -> Exposure {
        Exposure { group, other }
    }

    /// The table in the module documentation, with plain numbers.
    #[test]
    fn owner_only_modes_are_accepted_everywhere() {
        for kind in [ManifestKey, StateStoreSecret, StateStoreDatabase, AuditLog] {
            for mode in [0o400, 0o600, 0o700, 0o000] {
                for place in [OPEN, CLOSED, MOUNT] {
                    for loose in [false, true] {
                        assert_eq!(
                            judge(kind, mode, place, loose),
                            Verdict::Accepted,
                            "{kind:?} {mode:o} {place:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn writable_by_group_or_other_is_refused_whatever_else_holds() {
        for kind in [ManifestKey, StateStoreSecret, StateStoreDatabase, AuditLog] {
            for (mode, group, other) in [
                (0o620, true, false),
                (0o602, false, true),
                (0o666, true, true),
                (0o660, true, false),
                (0o622, true, true),
            ] {
                for place in [OPEN, CLOSED, MOUNT] {
                    for loose in [false, true] {
                        assert_eq!(
                            judge(kind, mode, place, loose),
                            Verdict::RefusedWritable(exposure(group, other)),
                            "{kind:?} {mode:o} {place:?} override={loose}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn world_readable_is_refused_unless_the_directory_is_closed() {
        for kind in [ManifestKey, StateStoreSecret, StateStoreDatabase, AuditLog] {
            for place in [OPEN, MOUNT] {
                assert!(
                    matches!(
                        judge(kind, 0o604, place, false),
                        Verdict::RefusedReadable(e) if e.other
                    ),
                    "{kind:?} {place:?}"
                );
            }
            assert_eq!(judge(kind, 0o644, CLOSED, false), Verdict::Accepted);
            assert_eq!(judge(kind, 0o604, CLOSED, false), Verdict::Accepted);
        }
        // A Kubernetes Secret at its default mode 0644 is refused: the mount
        // relaxes group read only.
        assert_eq!(
            judge(ManifestKey, 0o644, MOUNT, false),
            Verdict::RefusedReadable(exposure(false, true))
        );
    }

    #[test]
    fn group_readable_is_accepted_only_for_a_mount_a_closed_directory_or_the_audit_file() {
        for kind in [ManifestKey, StateStoreSecret, StateStoreDatabase] {
            assert_eq!(
                judge(kind, 0o640, OPEN, false),
                Verdict::RefusedReadable(exposure(true, false)),
                "{kind:?}"
            );
            assert_eq!(
                judge(kind, 0o440, OPEN, false),
                Verdict::RefusedReadable(exposure(true, false))
            );
            assert_eq!(judge(kind, 0o440, MOUNT, false), Verdict::Accepted);
            assert_eq!(judge(kind, 0o640, CLOSED, false), Verdict::Accepted);
        }
        assert_eq!(judge(AuditLog, 0o640, OPEN, false), Verdict::Accepted);
        assert_eq!(
            judge(AuditLog, 0o644, OPEN, false),
            Verdict::RefusedReadable(exposure(false, true))
        );
    }

    #[test]
    fn the_override_accepts_readable_files_only() {
        assert_eq!(
            judge(ManifestKey, 0o644, OPEN, true),
            Verdict::Overridden(exposure(true, true))
        );
        assert_eq!(
            judge(StateStoreSecret, 0o440, OPEN, true),
            Verdict::Overridden(exposure(true, false))
        );
        assert_eq!(
            judge(AuditLog, 0o644, OPEN, true),
            Verdict::Overridden(exposure(false, true))
        );
        assert!(matches!(
            judge(ManifestKey, 0o646, OPEN, true),
            Verdict::RefusedWritable(_)
        ));
    }

    #[test]
    fn the_atomic_writer_layout_is_recognised_narrowly() {
        let dir = "..2026_11_14_10_00_00.123456789";
        assert!(is_atomic_writer_dir(dir, Some(Path::new(dir))));
        // `..data` must point at this directory, and must be a symlink.
        assert!(!is_atomic_writer_dir(dir, None));
        assert!(!is_atomic_writer_dir(
            dir,
            Some(Path::new("..2026_01_01_00_00_00.1"))
        ));
        // The file's own directory must be the payload, not `..data` or an
        // ordinary directory.
        assert!(!is_atomic_writer_dir("..data", Some(Path::new("..data"))));
        assert!(!is_atomic_writer_dir("keys", Some(Path::new("keys"))));
        assert!(!is_atomic_writer_dir("..", Some(Path::new(".."))));
    }

    #[test]
    fn messages_carry_the_three_parts_and_never_a_value() {
        let file = SecretFile::new(ManifestKey, "manifest key \"q4-2026\"", "/etc/vtop/keys/q4");
        let readable = Refusal {
            file: file.clone(),
            mode: 0o644,
            writable: false,
            exposure: exposure(true, true),
        }
        .to_string();
        for needle in [
            "manifest key \"q4-2026\"",
            "/etc/vtop/keys/q4",
            "0644",
            "the file was not read and nothing was started",
            "chmod 0400 /etc/vtop/keys/q4",
            "defaultMode: 0400",
            "security.allow_loose_secret_permissions: true",
        ] {
            assert!(readable.contains(needle), "{needle}: {readable}");
        }
        let writable = Refusal {
            file: file.clone(),
            mode: 0o666,
            writable: true,
            exposure: exposure(true, true),
        }
        .to_string();
        for needle in [
            "0666",
            "write it",
            "no setting accepts it",
            "chmod go-w /etc/vtop/keys/q4",
            "ls -ln",
        ] {
            assert!(writable.contains(needle), "{needle}: {writable}");
        }
        assert!(!writable.contains("allow_loose_secret_permissions: true"));
        let database = Refusal {
            file: SecretFile::new(StateStoreDatabase, "the state-store database", "/d/s.db"),
            mode: 0o644,
            writable: false,
            exposure: exposure(true, true),
        }
        .to_string();
        assert!(database.contains("chmod 0600 /d/s.db"), "{database}");
        let accepted = Override {
            file,
            mode: 0o644,
            exposure: exposure(true, true),
            engine: None,
        }
        .message();
        for needle in [
            "0644",
            "Accepted because security.allow_loose_secret_permissions is true",
            "audit trail",
            "chmod 0400",
        ] {
            assert!(accepted.contains(needle), "{needle}: {accepted}");
        }
    }

    #[test]
    fn the_sqlite_files_are_the_database_and_its_wal_and_shm() {
        let files = sqlite_files(Path::new("/data/state.db"));
        let paths: Vec<_> = files.iter().map(|f| f.path.clone()).collect();
        assert_eq!(
            paths,
            [
                PathBuf::from("/data/state.db"),
                PathBuf::from("/data/state.db-wal"),
                PathBuf::from("/data/state.db-shm"),
            ]
        );
        assert!(files.iter().all(|f| f.kind == StateStoreDatabase));
    }

    /// The same rule through the filesystem: a real file's mode, in an open
    /// and in a closed directory.
    #[cfg(unix)]
    #[test]
    fn inspect_reads_the_mode_and_the_directory() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let open = root.path().join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let key = open.join("q4");
        std::fs::write(&key, "4d".repeat(32)).unwrap();
        let file = SecretFile::new(ManifestKey, "manifest key \"q4\"", &key);
        let strict = SecretPolicy::strict();
        let set = |mode| std::fs::set_permissions(&key, std::fs::Permissions::from_mode(mode));

        for mode in [0o644, 0o640, 0o440, 0o604] {
            set(mode).unwrap();
            let refused = inspect(&file, &strict).unwrap_err();
            assert!(!refused.writable && refused.mode == mode, "{refused:?}");
        }
        for mode in [0o666, 0o620, 0o602] {
            set(mode).unwrap();
            assert!(inspect(&file, &strict).unwrap_err().writable, "{mode:o}");
        }
        for mode in [0o400, 0o600] {
            set(mode).unwrap();
            assert_eq!(inspect(&file, &strict), Ok(None), "{mode:o}");
        }

        // The override accepts the readable file and reports it.
        set(0o644).unwrap();
        let loose = SecretPolicy {
            allow_loose: true,
            engine: Some("e1".into()),
        };
        let accepted = inspect(&file, &loose).unwrap().unwrap();
        assert_eq!(accepted.mode, 0o644);
        assert_eq!(accepted.engine.as_deref(), Some("e1"));
        set(0o646).unwrap();
        assert!(inspect(&file, &loose).unwrap_err().writable);

        // A directory closed to everyone else makes the read bits harmless,
        // but never the write bits.
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o700)).unwrap();
        set(0o644).unwrap();
        assert_eq!(inspect(&file, &strict), Ok(None));
        set(0o664).unwrap();
        assert!(inspect(&file, &strict).unwrap_err().writable);

        // A symlink is judged by the file it names.
        set(0o600).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&key, &link).unwrap();
        let linked = SecretFile::new(ManifestKey, "manifest key \"q4\"", &link);
        assert_eq!(inspect(&linked, &strict), Ok(None));
        set(0o644).unwrap();
        assert!(inspect(&linked, &strict).is_err());

        // A file that does not exist is left to whoever reads it.
        let missing = SecretFile::new(ManifestKey, "k", root.path().join("absent"));
        assert_eq!(inspect(&missing, &strict), Ok(None));
    }

    /// Review: SQLite keeps `-wal` and `-shm` beside the file a symlinked
    /// database names, so they are checked there, not beside the link.
    #[cfg(unix)]
    #[test]
    fn the_wal_of_a_symlinked_ledger_is_checked_beside_its_target() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let open = root.path().join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let real = open.join("real.db");
        std::fs::write(&real, "").unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o600)).unwrap();
        let wal = open.join("real.db-wal");
        std::fs::write(&wal, "").unwrap();
        std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o644)).unwrap();
        let link = root.path().join("state.db");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let report = inspect_all(&sqlite_files(&link), &SecretPolicy::strict());
        let refused: Vec<_> = report
            .refusals
            .iter()
            .map(|r| r.file.path.clone())
            .collect();
        assert_eq!(refused, [wal.canonicalize().unwrap()], "{report:?}");
    }

    /// The Kubernetes layout, built for real: a `0440` file in a `..data`
    /// payload directory on tmpfs that the process cannot write. Linux only,
    /// and only where `/dev/shm` is a writable tmpfs and the test is not
    /// root (root can write any file, so the mount check rightly fails).
    #[cfg(target_os = "linux")]
    #[test]
    fn a_kubernetes_secret_mount_on_tmpfs_is_accepted_at_0440() {
        use std::os::unix::fs::PermissionsExt;
        if rustix::process::geteuid().is_root() {
            eprintln!("skipped: root can write a 0440 file, so it is not a read-only mount");
            return;
        }
        let Ok(root) = tempfile::tempdir_in("/dev/shm") else {
            eprintln!("skipped: /dev/shm is not available");
            return;
        };
        let on_tmpfs = rustix::fs::statfs(root.path()).is_ok_and(|s| {
            #[allow(clippy::unnecessary_cast)]
            let t = s.f_type as i64;
            t == 0x0102_1994
        });
        if !on_tmpfs {
            eprintln!("skipped: /dev/shm is not tmpfs");
            return;
        }
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let payload = "..2026_11_14_10_00_00.123456789";
        std::fs::create_dir(root.path().join(payload)).unwrap();
        let real = root.path().join(payload).join("q4");
        std::fs::write(&real, "4d".repeat(32)).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o440)).unwrap();
        std::os::unix::fs::symlink(payload, root.path().join("..data")).unwrap();
        std::os::unix::fs::symlink("..data/q4", root.path().join("q4")).unwrap();

        let file = SecretFile::new(ManifestKey, "manifest key \"q4\"", root.path().join("q4"));
        assert_eq!(inspect(&file, &SecretPolicy::strict()), Ok(None));
        // World-readable is still refused in the same layout.
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert!(inspect(&file, &SecretPolicy::strict()).is_err());
        // And so is a 0440 file on the same tmpfs outside the layout.
        let plain = root.path().join("plain");
        std::fs::write(&plain, "4d".repeat(32)).unwrap();
        std::fs::set_permissions(&plain, std::fs::Permissions::from_mode(0o440)).unwrap();
        let plain = SecretFile::new(ManifestKey, "manifest key \"plain\"", plain);
        assert!(inspect(&plain, &SecretPolicy::strict()).is_err());
    }
}
