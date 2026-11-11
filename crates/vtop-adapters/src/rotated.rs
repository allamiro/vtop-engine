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
//! page (the fingerprint the range's marker recorded, #578), the same page
//! below the marker's position when it recorded one (#620), and no shorter
//! than it was when the range was read. A commit counts towards a held file
//! only by the same content test ([`HeldFile::holds`]). A `copytruncate` copy fails this by
//! design - it has a new inode, and a fingerprint and a length are not proof
//! of identity - so a range read before a `copytruncate` is still refused.
//!
//! The descriptors are bounded: at most [`MAX_HELD_FILES`] in all, and one on
//! a file that has left its path is closed [`HOLD_FOR`] after it was last used
//! unless a re-read through it is in progress.

use crate::continuity::{self, Identity, Tail};
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
    /// The page that ends where a read of it last stopped, or where a range
    /// proven against it ends (#620): a rewrite that keeps the first page
    /// changes it.
    pub tail: Option<Tail>,
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
            tail: None,
            touched: Instant::now(),
        }
    }

    /// Whether a range whose marker recorded `content` (its first page) and
    /// `tail` (the page ending where it ends) was read from the content this
    /// descriptor reaches NOW, not merely from its inode (review of #621): a
    /// file rewritten in place keeps the inode, and a late commit of its old
    /// content must not count as archiving the new. A marker without a
    /// first-page fingerprint proves nothing and answers no.
    pub fn holds(&self, content: Option<&ContentFingerprint>, tail: Option<&Tail>) -> bool {
        let Some(content) = content else {
            return false;
        };
        continuity::proves(
            &self.file,
            &Identity {
                inode: self.inode,
                contents: &[content],
                tails: tail.as_slice(),
                min_len: tail.map_or(0, |tail| tail.end),
            },
        )
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
    pub fn proves(
        &self,
        content: &ContentFingerprint,
        size_when_read: u64,
        start: u64,
        tail: Option<&Tail>,
    ) -> bool {
        proves(&self.file, self.inode, content, size_when_read, start, tail)
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
    /// forward of a re-read already behind it, and never below what is
    /// committed from this file, as a rewind of a file still at its path
    /// never goes below its committed point (review of #621: a re-read left
    /// behind a commit that landed after it re-archived that commit's bytes).
    pub fn rewind(&mut self, start: u64) {
        let read_byte = self
            .replay
            .map_or(start, |replay| replay.read_byte.min(start))
            .max(self.held.committed);
        self.replay = Some(Replay {
            read_byte,
            drained: false,
        });
        self.held.touched = Instant::now();
    }

    /// Raise what is known to be committed from this file - a commit of a
    /// range of it, or what recovery learns from the ledger: a re-read never
    /// starts below it.
    pub fn raise_floor(&mut self, end: u64) {
        self.held.committed_to(end);
        if let Some(replay) = self.replay.as_mut() {
            replay.read_byte = replay.read_byte.max(self.held.committed);
        }
    }
}

/// Whether `file` is PROVEN to be the file a range was read from: the inode
/// the range's marker recorded, the first bytes its fingerprint covers, at
/// least as long as the file was when the range was read (an append-only file
/// never shrinks), and long enough to hold the range's start. When the marker
/// also recorded the page ending at its position (`tail`, #620), the file
/// must still hold those bytes there: neither a rewrite that kept the first
/// page nor a reused inode whose file starts like the old one is the file.
/// Anything less, and nothing is read from it.
///
/// This is [`continuity::proves`], the one positive-identity test, with the
/// range's evidence: the range must also fit, so the length is at least its
/// start.
pub(crate) fn proves(
    file: &std::fs::File,
    inode: u64,
    content: &ContentFingerprint,
    size_when_read: u64,
    start: u64,
    tail: Option<&Tail>,
) -> bool {
    continuity::proves(
        file,
        &Identity {
            inode,
            contents: &[content],
            tails: tail.as_slice(),
            min_len: size_when_read.max(start),
        },
    )
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
    tail: Option<&Tail>,
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
        proves(&file, inode, content, size_when_read, start, tail).then_some((candidate, file))
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
    _tail: Option<&Tail>,
) -> Option<(PathBuf, std::fs::File)> {
    None
}
