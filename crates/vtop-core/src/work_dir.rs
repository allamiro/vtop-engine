//! Bounded lifecycle management for local staging artifacts.

use crate::errors::VtopError;
use std::path::Path;
use std::time::{Duration, SystemTime};

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(not(unix))]
use std::path::PathBuf;

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
    #[cfg(unix)]
    name: OsString,
    #[cfg(not(unix))]
    path: PathBuf,
    size: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

/// Remove recognized VTOP staging files past `retention`, then evict the
/// oldest remaining artifacts until their aggregate size is at most
/// `max_bytes`. Lock files, directories, and unrelated files are never
/// touched. Symlinks are never followed; entries that are non-regular or have
/// changed identity when deletion is revalidated are skipped.
///
/// Local artifacts are scratch data: current recovery replays from the source
/// or verifies the uploaded object/manifest and never consumes these files.
pub fn cleanup_work_dir(
    work_dir: &Path,
    retention: Duration,
    max_bytes: u64,
) -> Result<WorkCleanup, VtopError> {
    std::fs::create_dir_all(work_dir)?;
    #[cfg(unix)]
    let directory = rustix::fs::open(
        work_dir,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    let now = SystemTime::now();
    let mut artifacts = Vec::new();
    for entry in std::fs::read_dir(work_dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !is_vtop_artifact(name) {
            continue;
        }
        // Re-read without following links. The entry may have changed since
        // file_type(); only a regular file with a stable identity is eligible.
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() {
            continue;
        }
        artifacts.push(Artifact {
            #[cfg(unix)]
            name: file_name,
            #[cfg(not(unix))]
            path: entry.path(),
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
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
            #[cfg(unix)]
            let removed = remove_unchanged_artifact(&directory, &artifact)?;
            #[cfg(not(unix))]
            let removed = remove_unchanged_artifact(&artifact)?;
            if removed {
                result.removed_files += 1;
                result.removed_bytes = result.removed_bytes.saturating_add(artifact.size);
                result.retained_bytes = result.retained_bytes.saturating_sub(artifact.size);
            }
        }
    }
    Ok(result)
}

#[cfg(unix)]
fn remove_unchanged_artifact(
    directory: &std::os::fd::OwnedFd,
    artifact: &Artifact,
) -> Result<bool, VtopError> {
    use rustix::fs::{fstat, openat, statat, unlinkat, AtFlags, FileType, Mode, OFlags};

    // Open relative to the already-open directory and refuse symlinks. Using
    // NONBLOCK also prevents a concurrently substituted FIFO/device from
    // stalling startup while it is being rejected.
    let opened = match openat(
        directory,
        artifact.name.as_os_str(),
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(opened) => opened,
        Err(_) => return Ok(false),
    };
    let opened_stat = fstat(&opened).map_err(std::io::Error::from)?;
    if FileType::from_raw_mode(opened_stat.st_mode) != FileType::RegularFile
        || opened_stat.st_dev as u64 != artifact.device
        || opened_stat.st_ino as u64 != artifact.inode
        || opened_stat.st_size < 0
        || opened_stat.st_size as u64 != artifact.size
    {
        return Ok(false);
    }

    // Revalidate the directory entry itself immediately before unlinkat. This
    // catches rename/symlink swaps after enumeration and keeps the operation
    // anchored to the opened directory rather than a mutable absolute path.
    let entry_stat = match statat(
        directory,
        artifact.name.as_os_str(),
        AtFlags::SYMLINK_NOFOLLOW,
    ) {
        Ok(stat) => stat,
        Err(_) => return Ok(false),
    };
    if FileType::from_raw_mode(entry_stat.st_mode) != FileType::RegularFile
        || entry_stat.st_dev as u64 != artifact.device
        || entry_stat.st_ino as u64 != artifact.inode
        || entry_stat.st_size < 0
        || entry_stat.st_size as u64 != artifact.size
    {
        return Ok(false);
    }

    match unlinkat(directory, artifact.name.as_os_str(), AtFlags::empty()) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::NOENT | rustix::io::Errno::ISDIR) => Ok(false),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}

#[cfg(not(unix))]
fn remove_unchanged_artifact(artifact: &Artifact) -> Result<bool, VtopError> {
    let metadata = match std::fs::symlink_metadata(&artifact.path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file()
        || metadata.len() != artifact.size
        || metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH) != artifact.modified
    {
        return Ok(false);
    }
    std::fs::remove_file(&artifact.path)?;
    Ok(true)
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

    #[cfg(unix)]
    #[test]
    fn skips_an_entry_swapped_to_a_symlink_after_enumeration() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vtop-race.raw");
        std::fs::write(&path, b"old artifact").unwrap();
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        let artifact = Artifact {
            name: OsString::from("vtop-race.raw"),
            size: metadata.len(),
            modified: metadata.modified().unwrap(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let directory = rustix::fs::open(
            dir.path(),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::empty(),
        )
        .unwrap();
        let target = dir.path().join("operator-data");
        std::fs::write(&target, b"keep").unwrap();
        std::fs::remove_file(&path).unwrap();
        symlink(&target, &path).unwrap();

        assert!(!remove_unchanged_artifact(&directory, &artifact).unwrap());
        assert!(std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(target).unwrap(), b"keep");
    }
}
