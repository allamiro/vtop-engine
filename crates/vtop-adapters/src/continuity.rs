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

/// Decide whether `read_byte` can still be applied to the file now at the
/// path.
///
/// `seen_inode` is the identity the cursor last read; `current_*` come from
/// the OPEN descriptor the next read will use (`fstat`), never from a second
/// path lookup that could describe a replacement. An unknown inode on either
/// side (`None`: a platform without inodes, a cursor that has not read yet, a
/// legacy marker) can never prove a rotation — it falls through to the length
/// test, which needs no identity.
pub fn check(
    seen_inode: Option<u64>,
    current_inode: Option<u64>,
    current_size: u64,
    read_byte: u64,
) -> Option<Discontinuity> {
    if let (Some(from), Some(to)) = (seen_inode, current_inode) {
        if from != to {
            return Some(Discontinuity::Rotated { from, to });
        }
    }
    if current_size < read_byte {
        return Some(Discontinuity::Truncated {
            read_byte,
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

    #[test]
    fn an_unchanged_file_is_continuous() {
        assert_eq!(check(Some(7), Some(7), 100, 100), None);
        assert_eq!(check(Some(7), Some(7), 150, 100), None, "appended to");
        assert_eq!(check(None, None, 0, 0), None, "a fresh cursor");
    }

    #[test]
    fn a_different_inode_is_a_rotation_whatever_the_length() {
        let longer = check(Some(7), Some(9), 5_000, 100);
        let shorter = check(Some(7), Some(9), 10, 100);
        let same = check(Some(7), Some(9), 100, 100);
        for got in [longer, shorter, same] {
            assert_eq!(got, Some(Discontinuity::Rotated { from: 7, to: 9 }));
        }
    }

    #[test]
    fn a_shorter_file_with_the_same_inode_is_a_truncation() {
        assert_eq!(
            check(Some(7), Some(7), 40, 100),
            Some(Discontinuity::Truncated {
                read_byte: 100,
                size: 40
            })
        );
        // Shorter than the read head is the test; equal is still continuous.
        assert_eq!(check(Some(7), Some(7), 100, 100), None);
    }

    #[test]
    fn unknown_identity_cannot_prove_a_rotation_but_length_still_counts() {
        assert_eq!(check(None, Some(9), 500, 100), None);
        assert_eq!(check(Some(7), None, 500, 100), None);
        assert_eq!(
            check(None, None, 10, 100),
            Some(Discontinuity::Truncated {
                read_byte: 100,
                size: 10
            }),
            "a file shorter than the read head is a truncation with or without inodes"
        );
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
