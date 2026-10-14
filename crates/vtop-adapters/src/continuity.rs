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
//! In both cases the right move for the NEW content is to read it from byte
//! zero. What this module decides is only whether a discontinuity happened;
//! the adapters decide what to do about their cursors.

/// A change of identity or length under a read position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discontinuity {
    /// A different file now lives at the path.
    Rotated { from: u64, to: u64 },
    /// The same file, now shorter than the read position.
    Truncated { read_byte: u64, size: u64 },
}

impl std::fmt::Display for Discontinuity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Discontinuity::Rotated { from, to } => write!(
                f,
                "file replaced (inode {from} -> {to}); the new file is read from byte 0"
            ),
            Discontinuity::Truncated { read_byte, size } => write!(
                f,
                "file truncated (length {size} is below the read position {read_byte}); \
                 it is read from byte 0"
            ),
        }
    }
}

/// What a cursor last observed of its file: the identity and the length the
/// last read saw. An append-only file never gets shorter, so the length is
/// evidence in its own right, independent of how far the cursor has read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    pub inode: Option<u64>,
    pub size: u64,
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
pub fn check(
    last_seen: Option<Observed>,
    current_inode: Option<u64>,
    current_size: u64,
    read_byte: u64,
) -> Option<Discontinuity> {
    if let Some(Observed {
        inode: Some(from), ..
    }) = last_seen
    {
        if let Some(to) = current_inode {
            if from != to {
                return Some(Discontinuity::Rotated { from, to });
            }
        }
    }
    let shrunk_below = last_seen.map_or(0, |seen| seen.size);
    if current_size < read_byte.max(shrunk_below) {
        return Some(Discontinuity::Truncated {
            read_byte: read_byte.max(shrunk_below),
            size: current_size,
        });
    }
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
/// ledger row is committed, but the cursor must not move.
pub fn commit_is_current(
    marker_inode: Option<u64>,
    current_inode: Option<u64>,
    current_size: u64,
    end_byte: u64,
) -> bool {
    if let (Some(marker), Some(current)) = (marker_inode, current_inode) {
        if marker != current {
            return false;
        }
    }
    end_byte <= current_size
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(inode: Option<u64>, size: u64) -> Option<Observed> {
        Some(Observed { inode, size })
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
                read_byte: 40_000,
                size: 20
            })
        );
        // A file merely a little behind where it was is not flagged by luck of
        // the read position: it is shorter than it was, which is the evidence.
        assert!(check(seen(Some(7), 1_000), Some(7), 999, 8).is_some());
        assert_eq!(check(seen(Some(7), 1_000), Some(7), 1_000, 8), None);
    }

    #[test]
    fn unknown_identity_cannot_prove_a_rotation_but_length_still_counts() {
        assert_eq!(check(seen(None, 100), Some(9), 500, 100), None);
        assert_eq!(check(seen(Some(7), 100), None, 500, 100), None);
        assert_eq!(
            check(None, None, 10, 100),
            Some(Discontinuity::Truncated {
                read_byte: 100,
                size: 10
            }),
            "a file shorter than the read head is a truncation with or without inodes"
        );
        // A seeded cursor knows an inode but no length (0): the length test
        // cannot fire on it, only the read position can.
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
}
