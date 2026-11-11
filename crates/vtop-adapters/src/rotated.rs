//! Re-reading a failed range from a file that has left its path (#621).
//!
//! The file adapter keeps one cursor per PATH. When a batch fails, its range
//! is handed back and re-read from the path - which only works while the file
//! the range was read from is still there. Log rotation moves it: `app.log`
//! becomes `app.log.1` (and later `app.log.1.gz`, or nothing), and a new file
//! takes the path. The rewind used to be refused, loudly, and the range was
//! never archived unless the rotated name happened to match a source pattern.
//!
//! Two ways back to the bytes, tried in this order:
//!
//! 1. **A held descriptor.** While ranges read from a file are not yet
//!    committed, the adapter keeps a descriptor open on it ([`HeldFile`]). A
//!    rename does not affect an open descriptor, and neither does an unlink:
//!    the bytes stay readable through it until it is closed, so this works
//!    even after the rotated file has been compressed and removed. It does not
//!    survive a restart.
//! 2. **The rotated-to path.** Across a restart there is no descriptor, so the
//!    adapter looks for the file where rotation leaves it: the other entries
//!    of the path's own directory, and every file the source's patterns
//!    match ([`find`]). This works as long as the rotated file still exists
//!    under some name.
//!
//! Either way the file must be PROVEN to be the one the range was read from
//! before a byte is read from it ([`proves`]): the same inode, the same first
//! page (the fingerprint the range's marker recorded, #578), and no shorter
//! than it was when the range was read. A `copytruncate` copy fails this by
//! design - it has a new inode, and a fingerprint and a length are not proof
//! of identity - so a range read before a `copytruncate` is still refused.
//!
//! The descriptors are bounded: at most [`MAX_HELD_FILES`] in all, and one on
//! a file that has left its path is closed [`HOLD_FOR`] after it was last used
//! unless a re-read through it is in progress.

use std::path::PathBuf;
use std::time::{Duration, Instant};
use vtop_core::types::ContentFingerprint;

/// The most descriptors the file adapter keeps open for re-reads, counting
/// both the files cursors are reading and the files that have left their
/// paths. Past it, a file is not held and a failed range falls back to the
/// search ([`find`]); a held rotated file with no re-read in progress gives
/// way first, oldest first.
pub(crate) const MAX_HELD_FILES: usize = 128;

/// How long a descriptor on a file that has left its path is kept after it
/// was last used, when no re-read through it is in progress. A descriptor on a
/// removed file keeps its disk space allocated until it is closed, so it is
/// not kept for ever on the chance that a range read from it fails later.
pub(crate) const HOLD_FOR: Duration = Duration::from_secs(60 * 60);

/// How many entries of the path's directory a search looks at, at most.
const MAX_SCAN: usize = 10_000;

/// A descriptor kept open on a file the adapter read ranges from.
#[derive(Debug)]
pub(crate) struct HeldFile {
    pub file: std::fs::File,
    pub inode: u64,
    /// The fingerprint of its first bytes, taken through this descriptor.
    pub content: ContentFingerprint,
    /// The highest byte handed out from it.
    pub read_end: u64,
    /// The highest byte committed from it.
    pub committed: u64,
    /// The longest it has been seen to be. An append-only file never gets
    /// shorter, so a re-read through a descriptor now shorter than this would
    /// read a truncated file as if it were the range (review of #621).
    pub min_len: u64,
    pub touched: Instant,
}

impl HeldFile {
    pub fn new(
        file: std::fs::File,
        inode: u64,
        content: ContentFingerprint,
        read_end: u64,
        committed: u64,
        min_len: u64,
    ) -> Self {
        Self {
            file,
            inode,
            content,
            read_end,
            committed,
            min_len: min_len.max(read_end),
            touched: Instant::now(),
        }
    }

    /// The file was seen at least `len` bytes long.
    pub fn seen_len(&mut self, len: u64) {
        self.min_len = self.min_len.max(len);
    }

    /// Whether a range handed out from it is not committed yet.
    pub fn outstanding(&self) -> bool {
        self.committed < self.read_end
    }

    /// Note a commit of a range of this file ending at `end`.
    pub fn committed_to(&mut self, end: u64) {
        self.committed = self.committed.max(end);
        self.touched = Instant::now();
    }

    /// Whether this descriptor still reaches the file a range was read from.
    pub fn proves(&self, content: &ContentFingerprint, size_when_read: u64, start: u64) -> bool {
        proves(&self.file, self.inode, content, size_when_read, start)
    }
}

/// Where the re-read of a rotated file stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Replay {
    /// The next byte to re-read.
    pub read_byte: u64,
    /// A read found nothing more: what is left is waiting on commits.
    pub drained: bool,
}

/// A file that has left the path its ranges were read under, kept so a range
/// read from it can be read again.
#[derive(Debug)]
pub(crate) struct RotatedFile {
    /// The source path the ranges were read under; re-read ranges carry it.
    pub path: String,
    pub held: HeldFile,
    /// `None` until a range is handed back.
    pub replay: Option<Replay>,
}

impl RotatedFile {
    /// A range of it is being re-read and the read has not reached its end.
    pub fn replaying(&self) -> bool {
        self.replay.is_some_and(|replay| !replay.drained)
    }

    /// Nothing read from it waits on a re-read or a commit any more.
    pub fn resolved(&self) -> bool {
        let handed_out = self
            .replay
            .map_or(self.held.read_end, |replay| replay.read_byte)
            .max(self.held.read_end);
        !self.replaying() && self.held.committed >= handed_out
    }

    /// Hand the range from `start` back: the next re-read starts there - never
    /// below what is committed from this file, and never forward of a re-read
    /// already behind it.
    pub fn rewind(&mut self, start: u64) {
        let from = start.max(self.held.committed);
        let read_byte = self
            .replay
            .map_or(from, |replay| replay.read_byte.min(from));
        self.replay = Some(Replay {
            read_byte,
            drained: false,
        });
        self.held.touched = Instant::now();
    }

    /// Raise what is known to be committed from this file (recovery, from the
    /// ledger): a re-read never starts below it.
    pub fn raise_floor(&mut self, end: u64) {
        self.held.committed = self.held.committed.max(end);
        if let Some(replay) = self.replay.as_mut() {
            replay.read_byte = replay.read_byte.max(self.held.committed);
        }
    }
}

/// Whether `file` is PROVEN to be the file a range was read from: the inode
/// the range's marker recorded, the first bytes its fingerprint covers, at
/// least as long as the file was when the range was read (an append-only file
/// never shrinks), and long enough to hold the range's start. Anything less,
/// and nothing is read from it.
///
/// These are the continuity checks of `crate::continuity` as they stand: the
/// tail fingerprint proposed for #620 belongs here too once it lands, so a
/// rotated file whose first page repeats another file's is not taken for it.
pub(crate) fn proves(
    file: &std::fs::File,
    inode: u64,
    content: &ContentFingerprint,
    size_when_read: u64,
    start: u64,
) -> bool {
    let Ok(metadata) = file.metadata() else {
        return false;
    };
    if inode_of(&metadata) != Some(inode) {
        return false;
    }
    let len = metadata.len();
    if len < size_when_read || start > len {
        return false;
    }
    head_of(file).is_ok_and(|head| content.matches_head(&head))
}

/// The first bytes of `file`, read at an offset so the descriptor's position
/// is left alone.
#[cfg(unix)]
pub(crate) fn head_of(file: &std::fs::File) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut head = vec![0u8; ContentFingerprint::MAX_LEN];
    let mut filled = 0;
    while filled < head.len() {
        match file.read_at(&mut head[filled..], filled as u64) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    head.truncate(filled);
    Ok(head)
}

#[cfg(not(unix))]
pub(crate) fn head_of(_file: &std::fs::File) -> std::io::Result<Vec<u8>> {
    // Without inodes nothing can be proven (`inode_of` is `None`), so no
    // caller gets this far.
    Ok(Vec::new())
}

/// Look for the file a range was read from where rotation leaves it: the
/// entries of `path`'s own directory (`app.log.1`, `app.log-20261111`), then
/// every file the source's `patterns` match (an `olddir` that is configured
/// as a source). Only files on the directory's filesystem with the range's
/// inode are opened, and only one [`proves`] to be the file is returned, with
/// a descriptor that stays on it whatever happens to its name next.
#[cfg(unix)]
pub(crate) fn find(
    path: &str,
    patterns: &[String],
    inode: u64,
    content: &ContentFingerprint,
    size_when_read: u64,
    start: u64,
) -> Option<(PathBuf, std::fs::File)> {
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    let dir = Path::new(path)
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // Inode numbers are per filesystem, and a rename never crosses one.
    let dev = std::fs::metadata(dir).ok()?.dev();
    let same_file = |md: &std::fs::Metadata| md.is_file() && md.ino() == inode && md.dev() == dev;
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.take(MAX_SCAN).flatten() {
            if entry.metadata().is_ok_and(|md| same_file(&md)) {
                candidates.push(entry.path());
            }
        }
    }
    for pattern in patterns {
        let Ok(matches) = glob::glob(pattern) else {
            continue;
        };
        for found in matches.flatten() {
            if std::fs::metadata(&found).is_ok_and(|md| same_file(&md))
                && !candidates.contains(&found)
            {
                candidates.push(found);
            }
        }
    }
    candidates.into_iter().find_map(|candidate| {
        let file = std::fs::File::open(&candidate).ok()?;
        proves(&file, inode, content, size_when_read, start).then_some((candidate, file))
    })
}

#[cfg(not(unix))]
pub(crate) fn find(
    _path: &str,
    _patterns: &[String],
    _inode: u64,
    _content: &ContentFingerprint,
    _size_when_read: u64,
    _start: u64,
) -> Option<(PathBuf, std::fs::File)> {
    None
}

#[cfg(unix)]
fn inode_of(md: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(md.ino())
}

#[cfg(not(unix))]
fn inode_of(_md: &std::fs::Metadata) -> Option<u64> {
    None
}
