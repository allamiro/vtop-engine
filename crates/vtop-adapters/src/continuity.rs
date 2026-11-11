//! Has the file under a cursor changed since the cursor last read it?
//!
//! The byte-offset adapters (file, syslog spool) keep one read position per
//! PATH. That position only means something for the file it was taken from.
//! Log rotation breaks the assumption in two ways:
//!
//! - **Rotation by rename or replace.** A different file (a different inode)
//!   now lives at the path. The old position points into a file that is no
//!   longer there, and applying it to the new one skips its first bytes — or,
//!   when the new file is shorter, reads nothing at all until it grows past
//!   the old position, then starts in the middle of a line.
//! - **Truncation** (an in-place truncate, or `copytruncate`). The inode is
//!   the same, but the file is now shorter than the read position.
//!
//! - **Replacement at the same inode** (#578). A filesystem can hand a freed
//!   inode number to the next file created, and a file can be rewritten in
//!   place (a `copytruncate` that regrows before the next read). When the new
//!   content is already at least as long as the old read position, inode and
//!   length both agree with the old file. Its first bytes do not: a read
//!   fingerprints the file's first page ([`ContentFingerprint`]) and the next
//!   read checks it.
//! - **A rewrite that keeps the first page** (#620). A fixed header longer
//!   than a page, a file restored from a copy, or a reused inode whose new
//!   file starts like the old one agrees with the first-page fingerprint
//!   too. The bytes just below the read position do not: a read also
//!   fingerprints the page that ENDS where it stopped ([`Tail`]), and the
//!   next read checks the file still holds it there.
//!
//! In every case the right move for the NEW content is to read it from byte
//! zero. What this module decides is only whether a discontinuity happened;
//! the adapters decide what to do about their cursors.

use std::io::Read;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
use vtop_core::types::ContentFingerprint;

/// A change of identity or length under a read position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discontinuity {
    /// A different file now lives at the path.
    Rotated { from: u64, to: u64 },
    /// The same file, now shorter than the read position (`read_byte`) or than
    /// the length the cursor last observed (`observed`, 0 when it knows none).
    Truncated {
        read_byte: u64,
        observed: u64,
        size: u64,
    },
    /// The same inode, no shorter, but its first `len` bytes are not the ones
    /// the cursor last read (#578).
    Replaced { len: u64 },
    /// The same inode, no shorter, the same first bytes, but the `len` bytes
    /// that end at byte `end` are not the ones the cursor last read there
    /// (#620): rewritten in place, or a reused inode with the same first page.
    Rewritten { end: u64, len: u64 },
}

impl std::fmt::Display for Discontinuity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Discontinuity::Rotated { from, to } => write!(
                f,
                "file replaced (inode {from} -> {to}); the new file is read from byte 0"
            ),
            Discontinuity::Truncated {
                read_byte,
                observed,
                size,
            } => write!(
                f,
                "file truncated (length {size} is below the read position {read_byte} \
                 or the length last observed, {observed}); it is read from byte 0"
            ),
            Discontinuity::Replaced { len } => write!(
                f,
                "file content replaced at the same inode (its first {len} bytes changed); \
                 the new file is read from byte 0"
            ),
            Discontinuity::Rewritten { end, len } => write!(
                f,
                "file content rewritten at the same inode (the {len} bytes before byte {end} \
                 changed, though its first page did not); the new file is read from byte 0"
            ),
        }
    }
}

/// What a cursor last observed of its file: the identity and the length the
/// last read saw. An append-only file never gets shorter, so the length is
/// evidence in its own right, independent of how far the cursor has read.
/// Nor does it change its first bytes, so `content` - the fingerprint the
/// last read took, `None` when it knows none - is evidence too (#578).
///
/// `tail` is the same evidence for the bytes BELOW the read position (#620):
/// an append never changes them either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub inode: Option<u64>,
    pub size: u64,
    pub content: Option<ContentFingerprint>,
    pub tail: Option<Tail>,
}

/// The fingerprint of the bytes that end at `end` (#620): what a read saw
/// just below the position it stopped at. The bytes it covers are
/// `end - fingerprint.len .. end`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tail {
    pub end: u64,
    pub fingerprint: ContentFingerprint,
}

impl Tail {
    /// The tail a marker ending at `end` records, if it records one.
    pub fn at(end: u64, fingerprint: Option<ContentFingerprint>) -> Option<Self> {
        fingerprint.map(|fingerprint| Self { end, fingerprint })
    }
}

/// Decide whether `read_byte` can still be applied to the file now at the
/// path.
///
/// `last_seen` is what the cursor last observed (`None` for a cursor that has
/// not read yet); `current_*` come from the OPEN descriptor the next read will
/// use (`fstat`), never from a second path lookup that could describe a
/// replacement. An unknown inode on either side (`None`: a platform without
/// inodes, a cursor that has not read yet, a legacy marker) can never prove a
/// rotation — it falls through to the length tests, which need no identity.
///
/// Truncation is judged two ways. A file shorter than `read_byte` plainly
/// cannot hold the position. But a cursor that is BEHIND the end of the file
/// (a bounded read of a backlog, or a partial last line) has a `read_byte`
/// much smaller than the length it last saw, and a truncation that leaves the
/// file longer than `read_byte` but shorter than before would pass the first
/// test and resume in the middle of new content. So a file now shorter than
/// the length the cursor last saw is a truncation too.
///
/// Neither test sees a replacement that keeps the inode and is already as
/// long as the old file (#578), so `current_head` - the file's first bytes,
/// read through the same descriptor ([`read_head`]) - is checked against the
/// fingerprint the cursor last took. A cursor with none (a legacy seed, a
/// file that was empty) cannot prove a replacement this way, exactly as an
/// unknown inode cannot prove a rotation.
///
/// Nor does the first page see a rewrite that keeps it (#620): that is
/// [`check_tail`], made only when this finds nothing.
pub fn check(
    last_seen: Option<&Observed>,
    current_inode: Option<u64>,
    current_size: u64,
    current_head: &[u8],
    read_byte: u64,
) -> Option<Discontinuity> {
    if let Some(Observed {
        inode: Some(from), ..
    }) = last_seen
    {
        if let Some(to) = current_inode {
            if *from != to {
                return Some(Discontinuity::Rotated { from: *from, to });
            }
        }
    }
    let shrunk_below = last_seen.map_or(0, |seen| seen.size);
    if current_size < read_byte.max(shrunk_below) {
        return Some(Discontinuity::Truncated {
            read_byte,
            observed: shrunk_below,
            size: current_size,
        });
    }
    if let Some(content) = last_seen.and_then(|seen| seen.content.as_ref()) {
        if !content.matches_head(current_head) {
            return Some(Discontinuity::Replaced { len: content.len });
        }
    }
    None
}

/// The test [`check`] cannot make (#620): a rewrite that kept the first page.
/// `current_tail` is what the file now holds where the cursor's [`Tail`] was
/// taken, read through the descriptor the next read uses
/// ([`read_ending_at`]); a file too short to hold it yields fewer bytes,
/// which never match. A cursor with no tail (a legacy seed, a read that
/// ended at byte 0) proves nothing this way.
///
/// Separate from [`check`] so the tail is read only once the other tests have
/// passed: reading it can fail, and a failure must not take a rotation,
/// truncation or replacement those tests had already found with it (review
/// of #632).
pub fn check_tail(last_seen: Option<&Observed>, current_tail: &[u8]) -> Option<Discontinuity> {
    if let Some(tail) = last_seen.and_then(|seen| seen.tail.as_ref()) {
        if !tail.fingerprint.matches_bytes(current_tail) {
            return Some(Discontinuity::Rewritten {
                end: tail.end,
                len: tail.fingerprint.len,
            });
        }
    }
    None
}

/// Whether a commit's range still ends in the bytes it read (#620): the file
/// now at the path holds, below the range's `end_byte`, the bytes its
/// marker fingerprinted there. `current_tail` is what the file holds there
/// now ([`probe_with_tail`]); a marker with no tail (a legacy row, a range
/// ending at byte 0) is judged by the other tests alone.
pub fn tail_is_current(marker_tail: Option<&ContentFingerprint>, current_tail: &[u8]) -> bool {
    marker_tail.is_none_or(|tail| tail.matches_bytes(current_tail))
}

/// Up to `len` bytes of an open file that end at byte `end` - fewer when the
/// file is shorter than `end`, so a fingerprint of `len` bytes cannot match
/// them. Leaves the descriptor at an arbitrary position; the caller seeks
/// before reading on.
pub async fn read_ending_at(
    file: &mut tokio::fs::File,
    end: u64,
    len: u64,
) -> std::io::Result<Vec<u8>> {
    let from = end.saturating_sub(len);
    file.seek(std::io::SeekFrom::Start(from)).await?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(end - from)
            .unwrap_or(0)
            .min(ContentFingerprint::MAX_LEN),
    );
    (&mut *file)
        .take(end - from)
        .read_to_end(&mut bytes)
        .await?;
    Ok(bytes)
}

/// The first bytes of an open file, up to [`ContentFingerprint::MAX_LEN`]:
/// what [`check`] compares and a marker fingerprints. Leaves the descriptor
/// at an arbitrary position; the caller seeks before reading on.
pub async fn read_head(file: &mut tokio::fs::File) -> std::io::Result<Vec<u8>> {
    file.seek(std::io::SeekFrom::Start(0)).await?;
    let mut head = Vec::with_capacity(ContentFingerprint::MAX_LEN);
    (&mut *file)
        .take(ContentFingerprint::MAX_LEN as u64)
        .read_to_end(&mut head)
        .await?;
    Ok(head)
}

/// The file now at `path` - its metadata and first bytes, through ONE open
/// descriptor - for judging a commit or a rewind against it. `None` when it
/// cannot be opened or read.
pub fn probe(path: &std::path::Path) -> Option<(std::fs::Metadata, Vec<u8>)> {
    probe_with_tail(path, None).map(|(metadata, head, _)| (metadata, head))
}

/// [`probe`], and - through the same descriptor - up to `len` bytes ending at
/// `end` when `tail` is `Some((end, len))`: what a marker's tail fingerprint
/// is checked against (#620). Empty when `tail` is `None`.
pub fn probe_with_tail(
    path: &std::path::Path,
    tail: Option<(u64, u64)>,
) -> Option<(std::fs::Metadata, Vec<u8>, Vec<u8>)> {
    use std::io::{Seek, SeekFrom};
    let mut file = std::fs::File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    let mut head = Vec::with_capacity(ContentFingerprint::MAX_LEN);
    (&mut file)
        .take(ContentFingerprint::MAX_LEN as u64)
        .read_to_end(&mut head)
        .ok()?;
    let mut bytes = Vec::new();
    if let Some((end, len)) = tail {
        let from = end.saturating_sub(len);
        file.seek(SeekFrom::Start(from)).ok()?;
        file.take(end - from).read_to_end(&mut bytes).ok()?;
    }
    Some((metadata, head, bytes))
}

/// What a file must be shown to be, for [`proves`]: the evidence a range's
/// marker, or a cursor, recorded of the file it read.
#[derive(Debug, Clone, Copy)]
pub struct Identity<'a> {
    pub inode: u64,
    /// First-page fingerprints (#578) the file must start with - every one
    /// of them. At least one: without a first page nothing is proven.
    pub contents: &'a [&'a ContentFingerprint],
    /// Pages the file must still hold, each ending at its offset (#620) -
    /// every one recorded.
    pub tails: &'a [&'a Tail],
    /// The length the file must have at least. An append-only file never
    /// gets shorter, so one shorter than a length it was seen at, a position
    /// read up to, or a range's end has been truncated or rewritten since.
    pub min_len: u64,
}

/// POSITIVE identity (#567, #621): whether `file` is proven to be the file
/// `identity` describes - the same inode, no shorter than `min_len`, starting
/// with every first page, and holding every tail where it ends. Not "not
/// proven another file": a decision that cannot be undone, or that skips
/// bytes when it is wrong, rests on this, so anything unknown answers no.
///
/// The one test of its kind, used by every caller that must know which file
/// a recorded position belongs to: the committed point a range is retired
/// against (`committed_through`, #567), the file a failed range is re-read
/// from after a rotation (`rotated::proves`, #621), the file a commit counts
/// towards, and the row a restart is seeded from (`seed_choice`, #621).
/// Reads at offsets, so a descriptor's position is left alone.
pub fn proves(file: &std::fs::File, identity: &Identity<'_>) -> bool {
    let Ok(metadata) = file.metadata() else {
        return false;
    };
    if inode_of(&metadata) != Some(identity.inode) || metadata.len() < identity.min_len {
        return false;
    }
    if identity.contents.is_empty() {
        return false;
    }
    let Ok(head) = read_at_most(file, 0, ContentFingerprint::MAX_LEN as u64) else {
        return false;
    };
    if !identity
        .contents
        .iter()
        .all(|content| content.matches_head(&head))
    {
        return false;
    }
    identity.tails.iter().all(|tail| {
        let from = tail.end.saturating_sub(tail.fingerprint.len);
        read_at_most(file, from, tail.end - from)
            .is_ok_and(|bytes| tail.fingerprint.matches_bytes(&bytes))
    })
}

/// [`proves`] for the file now at `path`. A path that cannot be opened is
/// not proven to be anything.
pub fn proves_at_path(path: &std::path::Path, identity: &Identity<'_>) -> bool {
    std::fs::File::open(path).is_ok_and(|file| proves(&file, identity))
}

/// Up to `len` bytes of `file` from `from`, read at an offset so the
/// descriptor's position is left alone; fewer at the end of the file.
#[cfg(unix)]
pub(crate) fn read_at_most(file: &std::fs::File, from: u64, len: u64) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut bytes = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
    let mut filled = 0;
    while filled < bytes.len() {
        match file.read_at(&mut bytes[filled..], from + filled as u64) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    bytes.truncate(filled);
    Ok(bytes)
}

#[cfg(not(unix))]
pub(crate) fn read_at_most(
    _file: &std::fs::File,
    _from: u64,
    _len: u64,
) -> std::io::Result<Vec<u8>> {
    // Without inodes nothing can be proven (`inode_of` is `None`), so no
    // caller gets this far.
    Ok(Vec::new())
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

/// Whether a commit for a range ending at `end_byte` still describes the file
/// now at the path, so that moving the cursor to it is meaningful.
///
/// A batch that was read before a rotation or truncation commits AFTER the
/// cursor has already restarted on the new content. Applying that commit would
/// carry the cursor to an offset in a file that no longer exists, past data
/// that was never read. A range that ends beyond the current length, or that
/// was read from a different inode, is stale: the batch is archived and its
/// ledger row is committed, but the cursor must not move. So is one whose
/// marker fingerprinted content the file at the path no longer starts with
/// (#578): `current_head` is that file's first bytes, empty when it could not
/// be read - which fails any fingerprint, the conservative answer.
pub fn commit_is_current(
    marker_inode: Option<u64>,
    marker_content: Option<&ContentFingerprint>,
    current_inode: Option<u64>,
    current_size: u64,
    current_head: &[u8],
    end_byte: u64,
) -> bool {
    if let (Some(marker), Some(current)) = (marker_inode, current_inode) {
        if marker != current {
            return false;
        }
    }
    if marker_content.is_some_and(|content| !content.matches_head(current_head)) {
        return false;
    }
    end_byte <= current_size
}

/// Which read each handed-out byte range came from, by cursor generation.
///
/// `commit_is_current` judges a commit by the inode and the length of the file
/// NOW, and cannot tell a pre-truncation range from a post-truncation one: a
/// `copytruncate`d file that has regrown past an old range's end looks the same
/// as it did before. A batch read before the truncation that finishes
/// uploading after the regrowth would then carry the restarted cursor to its
/// old offset, silently skipping the bytes in between (review of #564).
///
/// Every discontinuity the adapter acts on starts a new generation, and every
/// range handed out is remembered with the generation it was read in. A commit
/// is settled against that record: a range from an older generation is stale
/// however the file looks now. The record is in-process: after a restart there
/// is none, and `commit_is_current` with `tail_is_current` is the fallback.
/// That fallback judges content, not order: since #620 a range whose bytes
/// below its end are no longer in the file is stale there too, so what the
/// record alone still catches is a regrowth that repeats the old content in
/// both the first page and the page below the range's end.
#[derive(Debug, Clone, Default)]
pub struct Generations {
    current: u64,
    issued: std::collections::VecDeque<Issued>,
}

#[derive(Debug, Clone, Copy)]
struct Issued {
    start: u64,
    end: u64,
    generation: u64,
}

/// More than this many uncommitted ranges per file means the engine is not
/// committing at all; the oldest records are dropped and their commits fall
/// back to the identity, length and content tests (see [`Generations`]).
const MAX_ISSUED: usize = 256;

/// What a commit's range turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settled {
    /// Read in the current generation.
    Current,
    /// Read before the latest discontinuity: never move the cursor for it.
    Stale,
    /// Not on record (read before a restart, or evicted).
    Unknown,
}

impl Generations {
    /// The file under the cursor was replaced or truncated and the cursor
    /// restarted on the new content.
    pub fn discontinuity(&mut self) {
        self.current += 1;
    }

    /// A range was handed to the engine, read in the current generation.
    pub fn issue(&mut self, start: u64, end: u64) {
        if end <= start {
            return;
        }
        if self.issued.len() >= MAX_ISSUED {
            self.issued.pop_front();
        }
        self.issued.push_back(Issued {
            start,
            end,
            generation: self.current,
        });
    }

    /// A rewind will hand these bytes out again: forget the earlier issue.
    pub fn reissue_from(&mut self, start: u64) {
        self.issued.retain(|issued| issued.start < start);
    }

    /// Settle a commit for `start..end`. Commits arrive in the order the
    /// windows were read, so the OLDEST matching record is the one being
    /// committed; if the same range exists in two generations, the older one
    /// is taken first, which errs towards a duplicate rather than a skip.
    ///
    /// Every record of the SAME generation issued before the matched one is
    /// retired with it. The engine buffers several reads into one window and
    /// commits only the last read's range, so the window's earlier ranges are
    /// never settled by name; left queued, one of them could later match a
    /// range the new content repeats after a truncation, and that valid
    /// commit would be judged stale (review of #564). Within a generation a
    /// record ahead of a committed one belongs to its window or to an earlier
    /// one, and nothing will settle it by name. An OLDER generation's records
    /// stay: a window read before the truncation can still commit after one
    /// read since, and must find its record to be judged stale.
    pub fn settle(&mut self, start: u64, end: u64) -> Settled {
        let Some(at) = self
            .issued
            .iter()
            .position(|issued| issued.start == start && issued.end == end)
        else {
            return Settled::Unknown;
        };
        let issued = self.issued.remove(at).expect("position found above");
        let mut before = 0;
        self.issued.retain(|other| {
            before += 1;
            before > at || other.generation != issued.generation
        });
        if issued.generation == self.current {
            Settled::Current
        } else {
            Settled::Stale
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(inode: Option<u64>, size: u64) -> Option<Observed> {
        Some(Observed {
            inode,
            size,
            content: None,
            tail: None,
        })
    }

    /// The inode and length tests, with no content on either side: what every
    /// judgement before #578 had, and what a legacy marker still has.
    fn check(
        last_seen: Option<Observed>,
        current_inode: Option<u64>,
        current_size: u64,
        read_byte: u64,
    ) -> Option<Discontinuity> {
        super::check(
            last_seen.as_ref(),
            current_inode,
            current_size,
            b"",
            read_byte,
        )
    }

    fn commit_is_current(
        marker_inode: Option<u64>,
        current_inode: Option<u64>,
        current_size: u64,
        end_byte: u64,
    ) -> bool {
        super::commit_is_current(
            marker_inode,
            None,
            current_inode,
            current_size,
            b"",
            end_byte,
        )
    }

    fn seen_with(head: &[u8]) -> Observed {
        Observed {
            inode: Some(7),
            size: head.len() as u64,
            content: ContentFingerprint::of(head),
            tail: None,
        }
    }

    /// #578: a file rewritten at the same inode, already longer than the old
    /// read position when next opened. Inode and length agree with the old
    /// file; only its first bytes tell it apart.
    #[test]
    fn a_same_inode_replacement_that_is_no_shorter_is_a_replacement() {
        let old = b"old line one\nold line two\n";
        let new = b"NEW line one\nNEW line two\nNEW line three\n";
        assert_eq!(
            super::check(
                Some(&seen_with(old)),
                Some(7),
                new.len() as u64,
                new,
                old.len() as u64
            ),
            Some(Discontinuity::Replaced {
                len: old.len() as u64
            })
        );
        // Without a fingerprint the same file is, as before, a continuation.
        assert_eq!(
            check(
                seen(Some(7), old.len() as u64),
                Some(7),
                new.len() as u64,
                old.len() as u64
            ),
            None
        );
        // The same file, appended to, is not a replacement - including a
        // file under one page whose fingerprint covered all of it then.
        let grown = [&old[..], b"old line three\n"].concat();
        assert_eq!(
            super::check(
                Some(&seen_with(old)),
                Some(7),
                grown.len() as u64,
                &grown,
                old.len() as u64
            ),
            None
        );
        // A cursor that saw the file empty took no fingerprint, and proves
        // nothing about what was written since.
        assert_eq!(
            super::check(Some(&seen_with(b"")), Some(7), 5, b"abcd\n", 0),
            None
        );
    }

    /// #620: a file rewritten at the same inode with the same first page -
    /// a fixed header over a page long - or a reused inode whose new file
    /// starts like the old one. Inode, length and first page all agree with
    /// the old file; the bytes below the read position do not.
    #[test]
    fn a_rewrite_that_keeps_the_first_page_is_told_by_the_bytes_below_the_cursor() {
        let header = vec![b'#'; ContentFingerprint::MAX_LEN + 500];
        let old = [&header[..], b"old line one\nold line two\n"].concat();
        let new = [&header[..], b"NEW line one\nNEW line two\nNEW line three\n"].concat();
        let end = old.len() as u64;
        let tail = ContentFingerprint::of_tail(&old).unwrap();
        let seen = Observed {
            inode: Some(7),
            size: end,
            content: ContentFingerprint::of(&old),
            tail: Tail::at(end, Some(tail.clone())),
        };
        let below = |file: &[u8]| -> Vec<u8> {
            let end = (end as usize).min(file.len());
            file[end.saturating_sub(tail.len as usize)..end].to_vec()
        };
        let judge = |file: &[u8]| {
            super::check(
                Some(&seen),
                Some(7),
                file.len() as u64,
                &file[..ContentFingerprint::MAX_LEN],
                end,
            )
            .or_else(|| check_tail(Some(&seen), &below(file)))
        };
        assert_eq!(
            judge(&new),
            Some(Discontinuity::Rewritten {
                end,
                len: ContentFingerprint::MAX_LEN as u64
            })
        );
        // Appended to, the same file is a continuation.
        let grown = [&old[..], b"old line three\n"].concat();
        assert_eq!(judge(&grown), None);
        // Without a tail it is judged as before #620: a continuation.
        let legacy = Observed {
            tail: None,
            ..seen.clone()
        };
        assert_eq!(check_tail(Some(&legacy), b""), None);
        // And a commit whose range ends at `end` is current for the same
        // file only.
        assert!(tail_is_current(Some(&tail), &below(&grown)));
        assert!(!tail_is_current(Some(&tail), &below(&new)));
        assert!(!tail_is_current(Some(&tail), b""), "unreadable");
        assert!(tail_is_current(None, b""), "a legacy marker");
    }

    #[test]
    fn a_commit_is_not_current_once_the_file_starts_with_other_bytes() {
        let read = ContentFingerprint::of(b"old line one\n").unwrap();
        let commit =
            |head: &[u8]| super::commit_is_current(Some(7), Some(&read), Some(7), 5_000, head, 13);
        assert!(commit(b"old line one\nand more"));
        assert!(!commit(b"NEW line one\nand more"), "replaced in place");
        assert!(!commit(b""), "unreadable: the conservative answer");
        assert!(
            super::commit_is_current(Some(7), None, Some(7), 5_000, b"", 13),
            "a legacy marker is judged by inode and length alone"
        );
    }

    #[test]
    fn an_unchanged_file_is_continuous() {
        assert_eq!(check(seen(Some(7), 100), Some(7), 100, 100), None);
        assert_eq!(
            check(seen(Some(7), 100), Some(7), 150, 100),
            None,
            "appended to"
        );
        assert_eq!(check(None, None, 0, 0), None, "a fresh cursor");
        // A cursor behind the end of a file that has only grown is fine.
        assert_eq!(check(seen(Some(7), 100), Some(7), 400, 30), None);
    }

    #[test]
    fn a_different_inode_is_a_rotation_whatever_the_length() {
        for current_size in [5_000, 10, 100] {
            assert_eq!(
                check(seen(Some(7), 100), Some(9), current_size, 100),
                Some(Discontinuity::Rotated { from: 7, to: 9 })
            );
        }
    }

    #[test]
    fn a_shorter_file_with_the_same_inode_is_a_truncation() {
        assert_eq!(
            check(seen(Some(7), 100), Some(7), 40, 100),
            Some(Discontinuity::Truncated {
                read_byte: 100,
                observed: 100,
                size: 40
            })
        );
        // Shorter than the read head is the test; equal is still continuous.
        assert_eq!(check(seen(Some(7), 100), Some(7), 100, 100), None);
    }

    /// The case a read-position-only test misses: the cursor is far behind the
    /// end of the file (a bounded read of a backlog), and a truncation leaves
    /// the file longer than the read position but shorter than it was.
    #[test]
    fn a_truncation_that_stays_above_the_read_position_is_still_a_truncation() {
        // Last read saw a 40 000-byte file; the cursor is at 8. The file is now
        // 20 bytes: above the read position, far below what it was.
        assert_eq!(
            check(seen(Some(7), 40_000), Some(7), 20, 8),
            Some(Discontinuity::Truncated {
                read_byte: 8,
                observed: 40_000,
                size: 20
            })
        );
        // A file merely a little behind where it was is not flagged by luck of
        // the read position: it is shorter than it was, which is the evidence.
        assert!(check(seen(Some(7), 1_000), Some(7), 999, 8).is_some());
        assert_eq!(check(seen(Some(7), 1_000), Some(7), 1_000, 8), None);
    }

    #[test]
    fn a_truncation_reports_both_bounds_and_names_the_one_that_fired() {
        let message = check(seen(Some(7), 1_000), Some(7), 999, 8)
            .unwrap()
            .to_string();
        assert!(
            message.contains("read position 8") && message.contains("last observed, 1000"),
            "{message}"
        );
    }

    #[test]
    fn unknown_identity_cannot_prove_a_rotation_but_length_still_counts() {
        assert_eq!(check(seen(None, 100), Some(9), 500, 100), None);
        assert_eq!(check(seen(Some(7), 100), None, 500, 100), None);
        assert_eq!(
            check(None, None, 10, 100),
            Some(Discontinuity::Truncated {
                read_byte: 100,
                observed: 0,
                size: 10
            }),
            "a file shorter than the read head is a truncation with or without inodes"
        );
        // A cursor seeded without a length (a spool, or a marker that records
        // none) knows an inode but no length (0): the length test cannot fire
        // on it, only the read position can.
        assert_eq!(check(seen(Some(7), 0), Some(7), 5, 3), None);
    }

    #[test]
    fn a_commit_is_current_only_for_a_range_that_still_exists_in_this_file() {
        assert!(commit_is_current(Some(7), Some(7), 100, 100));
        assert!(commit_is_current(Some(7), Some(7), 100, 40));
        assert!(commit_is_current(None, Some(7), 100, 40), "legacy marker");
        assert!(
            !commit_is_current(Some(7), Some(9), 5_000, 40),
            "read from another inode"
        );
        assert!(
            !commit_is_current(Some(7), Some(7), 50, 900),
            "ends beyond the file as it is now: truncated since it was read"
        );
        assert!(!commit_is_current(Some(7), None, 0, 1), "the file is gone");
    }

    #[test]
    fn a_range_from_before_a_truncation_is_stale_however_the_file_looks_now() {
        let mut generations = Generations::default();
        generations.issue(0, 100);
        // copytruncate: the cursor restarts on the new content.
        generations.discontinuity();
        generations.issue(0, 40);
        // The old range finishes uploading after the file has regrown past it.
        // It is commit_is_current by inode and length; the record says stale.
        assert!(commit_is_current(Some(7), Some(7), 5_000, 100));
        assert_eq!(generations.settle(0, 100), Settled::Stale);
        // The new content's range is current.
        assert_eq!(generations.settle(0, 40), Settled::Current);
        // Settled once: a repeat is unknown, not current.
        assert_eq!(generations.settle(0, 40), Settled::Unknown);
    }

    /// Review of #564: a window of several reads commits only its last range.
    /// Its earlier ranges must go with it, or after a truncation the new
    /// content's identical first range matches the leftover and a valid
    /// commit is judged stale.
    #[test]
    fn a_window_of_several_reads_retires_all_of_them() {
        let mut generations = Generations::default();
        // One window: three reads, committed by the last one's range.
        generations.issue(0, 50);
        generations.issue(50, 80);
        generations.issue(80, 100);
        assert_eq!(generations.settle(80, 100), Settled::Current);
        // copytruncate, and the new content's first read is byte for byte the
        // same range as the old window's first read.
        generations.discontinuity();
        generations.issue(0, 50);
        assert_eq!(
            generations.settle(0, 50),
            Settled::Current,
            "matched the old window's leftover first range"
        );
    }

    /// A window read before a truncation can commit after a window read since
    /// it: settling the newer one must not retire the older record, or the
    /// late commit falls back to the length test and moves the cursor.
    #[test]
    fn a_newer_commit_does_not_retire_an_older_generations_record() {
        let mut generations = Generations::default();
        generations.issue(0, 100);
        generations.discontinuity();
        generations.issue(0, 40);
        assert_eq!(generations.settle(0, 40), Settled::Current);
        assert_eq!(generations.settle(0, 100), Settled::Stale);
    }

    #[test]
    fn the_same_range_in_two_generations_settles_oldest_first() {
        let mut generations = Generations::default();
        generations.issue(0, 50);
        generations.discontinuity();
        generations.issue(0, 50);
        assert_eq!(generations.settle(0, 50), Settled::Stale);
        assert_eq!(generations.settle(0, 50), Settled::Current);
    }

    #[test]
    fn a_replayed_range_is_forgotten_and_the_record_is_bounded() {
        let mut generations = Generations::default();
        generations.issue(0, 10);
        generations.issue(10, 20);
        generations.reissue_from(10);
        assert_eq!(generations.settle(10, 20), Settled::Unknown);
        assert_eq!(generations.settle(0, 10), Settled::Current);
        for i in 0..(MAX_ISSUED as u64 + 10) {
            generations.issue(i * 10, i * 10 + 5);
        }
        assert_eq!(generations.settle(0, 5), Settled::Unknown, "evicted");
        assert_eq!(
            generations.settle(
                (MAX_ISSUED as u64 + 9) * 10,
                (MAX_ISSUED as u64 + 9) * 10 + 5
            ),
            Settled::Current
        );
    }
}

/// The file a committed point was confirmed for (#624): the inode, first
/// page and tail of the marker whose commit, checked against the file at the
/// path, set it. A legacy marker without an inode confirms nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedFile {
    pub inode: u64,
    pub content: Option<ContentFingerprint>,
    /// The bytes ending at the committed point (#620), where the marker
    /// recorded them.
    pub tail: Option<Tail>,
}

impl ConfirmedFile {
    pub fn of(
        inode: Option<u64>,
        content: Option<&ContentFingerprint>,
        tail: Option<Tail>,
    ) -> Option<Self> {
        inode.map(|inode| Self {
            inode,
            content: content.cloned(),
            tail,
        })
    }

    /// Whether the file now at `path` is PROVEN to be this confirmed file AND
    /// the file a later, lower commit's marker was read from - so that commit
    /// can be skipped as superseded (#624) without losing a byte.
    ///
    /// Proof, by [`proves_at_path`] (#647), not the absence of disproof
    /// (review of #659, Codex P1). An older generation's commit with no
    /// fingerprint confirmed a point, a newer generation's lower commit was
    /// then skipped because "unknown" could not be shown to differ, and the
    /// read head stayed above bytes the new generation had not archived. So
    /// both first pages must be KNOWN and match the file now, both tails
    /// (where recorded) must hold, the inode must be the marker's, and the
    /// file must be no shorter than the committed point or anything the
    /// marker recorded. Anything unknown answers no, and the commit moves the
    /// cursor as it did before #624.
    pub fn proves(
        &self,
        path: &std::path::Path,
        committed_byte: u64,
        inode: Option<u64>,
        content: Option<&ContentFingerprint>,
        tail: Option<&Tail>,
        min_len: u64,
    ) -> bool {
        let (Some(mine), Some(theirs)) = (self.content.as_ref(), content) else {
            return false;
        };
        if inode != Some(self.inode) {
            return false;
        }
        let tails: Vec<&Tail> = self.tail.iter().chain(tail).collect();
        proves_at_path(
            path,
            &Identity {
                inode: self.inode,
                contents: &[mine, theirs],
                tails: &tails,
                min_len: min_len.max(committed_byte),
            },
        )
    }
}

#[cfg(all(test, unix))]
mod confirmed_file_tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;

    /// Review of #659 (Codex P1): a lower commit is skipped only on PROOF
    /// that it and the confirmed committed point are of the file now at the
    /// path. Unknown content, on either side, never proves.
    #[test]
    fn only_known_matching_content_proves_a_skip() {
        let bytes: Vec<u8> = (0..84u8).map(|i| b'a' + i % 26).collect();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&bytes).unwrap();
        f.flush().unwrap();
        let path = f.path();
        let inode = std::fs::metadata(path).unwrap().ino();
        let fp = |b: &[u8]| ContentFingerprint::of(b);
        let confirmed = |content: Option<ContentFingerprint>, tail: Option<Tail>| ConfirmedFile {
            inode,
            content,
            tail,
        };
        let newer = fp(&bytes[..21]);
        let good = confirmed(fp(&bytes[..60]), Tail::at(60, fp(&bytes[..60])));
        assert!(good.proves(path, 60, Some(inode), newer.as_ref(), None, 21));
        // Unknown content, on either side, proves nothing.
        assert!(!confirmed(None, None).proves(path, 60, Some(inode), newer.as_ref(), None, 21));
        assert!(!good.proves(path, 60, Some(inode), None, None, 21));
        // Known content that is not the file's now.
        assert!(!good.proves(path, 60, Some(inode), fp(&[b'z'; 21]).as_ref(), None, 21));
        // Another inode for the marker.
        assert!(!good.proves(path, 60, Some(inode + 1), newer.as_ref(), None, 21));
        // A file shorter than the committed point or the marker's length.
        assert!(!good.proves(path, 90, Some(inode), newer.as_ref(), None, 21));
        assert!(!good.proves(path, 60, Some(inode), newer.as_ref(), None, 90));
        // A confirmed tail the bytes below the committed point no longer hold.
        let rewritten = confirmed(fp(&bytes[..60]), Tail::at(60, fp(&[b'z'; 60])));
        assert!(!rewritten.proves(path, 60, Some(inode), newer.as_ref(), None, 21));
    }
}
