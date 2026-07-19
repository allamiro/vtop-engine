//! Bounded lifecycle management for local staging artifacts.

use crate::errors::VtopError;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const ARTIFACT_SUFFIXES: &[&str] = &[
    ".manifest.json",
    ".cef.gz",
    ".cef.zst",
    ".cef",
    ".leef.gz",
    ".leef.zst",
    ".leef",
    ".json.gz",
    ".json.zst",
    ".json",
    ".jsonl.gz",
    ".jsonl.zst",
    ".jsonl",
    ".syslog.gz",
    ".syslog.zst",
    ".syslog",
    ".raw.gz",
    ".raw.zst",
    ".raw",
];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkCleanup {
    pub removed_files: usize,
    pub removed_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Debug)]
struct Artifact {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

/// Remove recognized VTOP staging files past `retention`, then evict the
/// oldest remaining artifacts until their aggregate size is at most
/// `max_bytes`. Lock files, directories, symlinks, and unrelated files are
/// never touched.
///
/// Local artifacts are scratch data: current recovery replays from the source
/// or verifies the uploaded object/manifest and never consumes these files.
pub fn cleanup_work_dir(
    work_dir: &Path,
    retention: Duration,
    max_bytes: u64,
) -> Result<WorkCleanup, VtopError> {
    std::fs::create_dir_all(work_dir)?;
    let now = SystemTime::now();
    let mut artifacts = Vec::new();
    for entry in std::fs::read_dir(work_dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_vtop_artifact(name) {
            continue;
        }
        let metadata = entry.metadata()?;
        artifacts.push(Artifact {
            path: entry.path(),
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }

    artifacts.sort_by_key(|artifact| artifact.modified);
    let mut result = WorkCleanup {
        retained_bytes: artifacts.iter().map(|artifact| artifact.size).sum(),
        ..WorkCleanup::default()
    };

    for artifact in artifacts {
        let expired = now.duration_since(artifact.modified).unwrap_or_default() >= retention;
        if expired || result.retained_bytes > max_bytes {
            std::fs::remove_file(&artifact.path)?;
            result.removed_files += 1;
            result.removed_bytes = result.removed_bytes.saturating_add(artifact.size);
            result.retained_bytes = result.retained_bytes.saturating_sub(artifact.size);
        }
    }
    Ok(result)
}

fn is_vtop_artifact(name: &str) -> bool {
    name.starts_with("vtop-")
        && ARTIFACT_SUFFIXES
            .iter()
            .any(|suffix| name.ends_with(suffix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_only_recognized_artifacts_and_enforces_cap() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("vtop-a.raw"), vec![1_u8; 8]).unwrap();
        std::fs::write(dir.path().join("vtop-b.manifest.json"), vec![2_u8; 8]).unwrap();
        std::fs::write(dir.path().join("operator-note.raw"), vec![3_u8; 8]).unwrap();
        std::fs::write(dir.path().join(".vtop.instance.lock"), b"lock").unwrap();

        let result = cleanup_work_dir(dir.path(), Duration::from_secs(3600), 8).unwrap();
        assert_eq!(result.removed_files, 1);
        assert_eq!(result.retained_bytes, 8);
        assert!(dir.path().join("operator-note.raw").exists());
        assert!(dir.path().join(".vtop.instance.lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn never_follows_or_removes_symlinks() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, b"keep").unwrap();
        let link = dir.path().join("vtop-link.raw");
        symlink(&target, &link).unwrap();

        cleanup_work_dir(dir.path(), Duration::ZERO, 0).unwrap();
        assert!(link.exists());
        assert_eq!(std::fs::read(target).unwrap(), b"keep");
    }
}
