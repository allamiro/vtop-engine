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
pub(crate) fn find(
    path: &str,
    patterns: &[String],
    inode: u64,
    content: &ContentFingerprint,
    size_when_read: u64,
    start: u64,
    tail: Option<&Tail>,
) -> Option<(PathBuf, std::fs::File)> {
    #[cfg(unix)]
    {
        search(path, patterns, inode, content, size_when_read, start, tail)
    }
    // Without inode numbers nothing can be proven to be the file.
    #[cfg(not(unix))]
    {
        let _ = (path, patterns, inode, content, size_when_read, start, tail);
        None
    }
}

#[cfg(unix)]
fn search(
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    #[cfg(unix)]
    /// A file of `bytes` in `dir` named `name`: its inode and the fingerprint
    /// of its first page.
    fn file_in(
        dir: &std::path::Path,
        name: &str,
        bytes: &[u8],
    ) -> (PathBuf, u64, ContentFingerprint) {
        let path = dir.join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(bytes).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        (path, inode, ContentFingerprint::of(bytes).unwrap())
    }

    #[cfg(unix)]
    fn fifo_in(dir: &std::path::Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let made = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("mkfifo");
        assert!(made.success());
        path
    }

    #[cfg(unix)]
    /// Run `search` on its own thread and fail instead of hanging when it
    /// does not finish: a search that opens a FIFO waits for a writer.
    fn finishes<T: Send + 'static>(search: impl FnOnce() -> T + Send + 'static) -> T {
        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(search());
        });
        result
            .recv_timeout(Duration::from_secs(10))
            .expect("the search did not finish")
    }

    #[cfg(unix)]
    #[test]
    fn a_length_the_file_was_seen_at_is_kept_and_never_lowered() {
        let dir = tempfile::tempdir().unwrap();
        let (path, inode, content) = file_in(dir.path(), "app.log", b"one\ntwo\n");
        let mut held = HeldFile::new(std::fs::File::open(path).unwrap(), inode, content, 8, 0, 8);
        assert_eq!(held.min_len, 8);
        held.seen_len(100);
        assert_eq!(held.min_len, 100, "seen longer");
        held.seen_len(40);
        assert_eq!(held.min_len, 100, "an append-only file never gets shorter");
    }

    /// The file a range was read from is found under the name rotation gave
    /// it, and only when it is proven to be that file.
    #[cfg(unix)]
    #[test]
    fn a_file_is_found_where_rotation_left_it_and_only_when_proven() {
        let dir = tempfile::tempdir().unwrap();
        let at = dir.path().join("app.log");
        let (_, inode, content) = file_in(dir.path(), "app.log", b"one\ntwo\n");
        let rotated = dir.path().join("app.log.1");
        std::fs::rename(&at, &rotated).unwrap();
        // A new file at the path, and another file nobody asked about.
        std::fs::write(&at, b"fresh\n").unwrap();
        std::fs::write(dir.path().join("other.log"), b"one\ntwo\n").unwrap();

        let path = at.to_string_lossy().into_owned();
        let (found, file) = find(&path, &[], inode, &content, 8, 0, None).expect("found");
        assert_eq!(found, rotated);
        assert_eq!(file.metadata().unwrap().ino(), inode);

        // The name is not what proves it: another first page is not the file.
        let other = ContentFingerprint::of(b"NOPE\n").unwrap();
        assert!(find(&path, &[], inode, &other, 8, 0, None).is_none());
        // Nor is a file shorter than it was when the range was read.
        assert!(find(&path, &[], inode, &content, 9, 0, None).is_none());
        // Another inode is not the file whatever it holds.
        assert!(find(&path, &[], inode + 1_000_000, &content, 8, 0, None).is_none());
    }

    /// An `olddir` that is configured as a source is searched by its pattern.
    #[cfg(unix)]
    #[test]
    fn a_file_rotated_into_another_directory_is_found_by_the_sources_patterns() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old");
        std::fs::create_dir(&old).unwrap();
        let at = dir.path().join("app.log");
        let (_, inode, content) = file_in(dir.path(), "app.log", b"one\ntwo\n");
        std::fs::rename(&at, old.join("app.log.1")).unwrap();
        std::fs::write(&at, b"fresh\n").unwrap();

        let path = at.to_string_lossy().into_owned();
        assert!(
            find(&path, &[], inode, &content, 8, 0, None).is_none(),
            "not in the path's own directory"
        );
        let patterns = [
            "/no/such/dir/*.log".to_string(),
            "[".to_string(), // not a pattern at all
            old.join("*.1").to_string_lossy().into_owned(),
        ];
        let (found, _) = find(&path, &patterns, inode, &content, 8, 0, None).expect("found");
        assert_eq!(found, old.join("app.log.1"));
        // Listed by the directory scan and by a pattern, it is one candidate.
        let again = [dir.path().join("*").to_string_lossy().into_owned()];
        let rotated = dir.path().join("app.log.1");
        std::fs::rename(old.join("app.log.1"), &rotated).unwrap();
        let (found, _) = find(&path, &again, inode, &content, 8, 0, None).expect("found");
        assert_eq!(found, rotated);
    }

    /// Only a regular file with the range's inode is ever opened: opening a
    /// FIFO waits for a writer that never comes, and would stall the search
    /// that goes through every candidate looking for the proven one.
    #[cfg(unix)]
    #[test]
    fn the_search_never_opens_what_is_not_a_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old");
        std::fs::create_dir(&old).unwrap();
        let (_, _, content) = file_in(dir.path(), "app.log", b"one\ntwo\n");
        fifo_in(dir.path(), "a.fifo");
        fifo_in(&old, "b.fifo");

        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let patterns = vec![old.join("*").to_string_lossy().into_owned()];
        // An inode nothing has: every entry is looked at and none is opened.
        let found = finishes(move || {
            find(&path, &patterns, u64::MAX, &content, 8, 0, None).map(|(found, _)| found)
        });
        assert_eq!(found, None);
    }
}
