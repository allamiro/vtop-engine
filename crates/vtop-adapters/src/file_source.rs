//! File source adapter.
//!
//! Reads append-only log files line by line, tracking byte offsets, file
//! identity (inode), size, and mtime. Resumes from the last committed byte and
//! never deletes source files unless explicitly configured, only after the
//! batch is committed, and only once the file has gone a quiet period without
//! a write (#619, see `DeleteCandidate`).

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
};
use crate::continuity::{self, Discontinuity, Observed};
use crate::rotated::{self, HeldFile, Replay, RotatedFile};
use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use vtop_core::errors::VtopError;
use vtop_core::types::{ContentFingerprint, ProgressMarker, SourceType, TelemetryFormat};

/// Per-file read/commit cursor.
#[derive(Debug, Clone, Default)]
struct FileCursor {
    /// Next byte to read from (uncommitted read head).
    read_byte: u64,
    /// Highest byte durably committed to object storage.
    committed_byte: u64,
    /// The file a commit this process checked against the file at the path
    /// set `committed_byte` for (#624) - `None` for the restart seed, or after
    /// a discontinuity. Only such a point may supersede a later, lower
    /// commit, and only one of the SAME file (review of #624): the seed can be
    /// the newest committed row of a PREVIOUS generation (a copytruncate),
    /// and a commit below it must still move the cursor down to the new
    /// content (review of #564).
    confirmed_for: Option<crate::continuity::ConfirmedFile>,
    /// What the file looked like when this cursor last caught up with it.
    ///
    /// Kept so a cycle can decide NOT to open the file at all (#100). `None`
    /// until a read has actually observed the file, so an unseen file is
    /// always read rather than skipped on an assumption.
    seen: Option<FileIdentity>,
    /// Which generation of the file each handed-out range was read in.
    generations: continuity::Generations,
    /// `(inode, read_byte)` recorded when a replay rewound this cursor: which
    /// file the rewound position belongs to, valid only while `read_byte` is
    /// still where the rewind put it (a later read supersedes it with `seen`).
    rewound_for: Option<(u64, u64)>,
    /// A committed range that covers the whole file, waiting out the quiet
    /// period before `delete_after_commit` may remove it (#619).
    delete_candidate: Option<DeleteCandidate>,
}

/// A file that a committed range covers end to end, and what it looked like
/// when that was established - the evidence `delete_after_commit` waits on.
///
/// WHY DELETION WAITS (#619). Most appenders open a log once with `O_APPEND`
/// and keep the descriptor until they rotate. Deleting the path the moment the
/// engine caught up did not stop them: every later line went into the
/// unlinked inode, which no path names, until the writer reopened - silently,
/// and every time the engine caught up on a live file. No portable test says
/// "no process holds this open for writing" (`/proc/*/fd` is Linux-only, needs
/// privileges to read other users' descriptors and cannot see writers in
/// other containers' PID namespaces; inotify close events are Linux-only and
/// miss a writer that opened before the watch). What can be observed is
/// whether the file is still being WRITTEN: an append moves both its length
/// and its mtime. So a file is deleted only once it has stayed exactly as the
/// commit saw it - same inode, first page, length and mtime - for the quiet
/// period, timed on the engine's own monotonic clock rather than by the
/// file's mtime, which a writer, an archive extraction or a network
/// filesystem's clock can set to anything.
///
/// THE RESIDUAL RISK, stated plainly: a writer that holds the file open, is
/// silent for longer than the quiet period, and then writes again, writes
/// into a deleted file. So does one whose write through that descriptor lands
/// between the delete's final check and the unlink (see
/// [`DeleteJob::run`]). The quiet period bounds how idle a live
/// writer can be before either happens; it cannot prove there is none.
///
/// The wait is in memory: a restart starts it again, from the seed (see
/// [`FileSource::arm_delete_from_seed`]). With a zero quiet period the commit
/// that covers the file deletes it at once - through the same rename-and-check
/// ([`DeleteJob::run`]), with no quiet-period protection.
#[derive(Debug, Clone)]
struct DeleteCandidate {
    /// The identity the committed range was read from.
    inode: Option<u64>,
    content: Option<ContentFingerprint>,
    /// The fingerprint of the bytes ending at `end_byte` the committed range
    /// was read with (#620); `None` for a legacy marker.
    tail: Option<ContentFingerprint>,
    /// Where the committed range ends: the file's whole length.
    end_byte: u64,
    /// The file's mtime when the candidate was recorded. Unchanged, with the
    /// length, it is the evidence that nothing wrote to the file since.
    mtime: Option<SystemTime>,
    /// When the candidate was recorded, on the engine's monotonic clock.
    since: Instant,
}

/// How the file now open compares with a [`DeleteCandidate`].
enum Compared {
    /// The same file, fully covered, and not touched since: deletable.
    Unchanged,
    /// The same file, fully covered, its first page AND the page below the
    /// committed end still the bytes that were read - but its mtime moved (a
    /// producer stamping a finished drop, a `touch`). Not proof of a write,
    /// so not a reason to give up the delete; but something touched it, so
    /// the wait starts again from now.
    Touched(Option<SystemTime>),
    /// Another file, bytes the committed range does not cover, or a moved
    /// mtime with no tail fingerprint to prove the content unchanged.
    Different,
}

impl DeleteCandidate {
    /// The bytes [`continuity::probe_with_tail`] must read for the tail test:
    /// the page ending at the committed end, when a tail was recorded.
    fn tail_window(&self) -> Option<(u64, u64)> {
        self.tail.as_ref().map(|tail| (self.end_byte, tail.len))
    }

    /// How the file (metadata, first bytes and the bytes ending at the
    /// committed end, from ONE descriptor) compares with what this candidate
    /// recorded.
    ///
    /// A MOVED MTIME is a touch only when the tail proves it (#620, review of
    /// #631). Without a tail, a same-length rewrite past the first page looks
    /// exactly like a touch, and taking it for one deleted the rewritten
    /// bytes unread a quiet period later; so a moved mtime ends the candidate
    /// and the file is kept. With a tail, a rewrite that reaches the first or
    /// the last page is caught, and a moved mtime with both pages and the
    /// length unchanged restarts the wait instead - which is what lets a
    /// touched file go. What neither page sees - a same-length change
    /// confined between them - is the limit #620 documents for every read.
    fn compare(&self, md: &std::fs::Metadata, head: &[u8], tail: &[u8]) -> Compared {
        // EQUALITY on the length: an append-only file under a committed
        // range is never shorter than the range, and a longer one has bytes
        // nobody read.
        if !is_the_file_read(self, md, head, tail) || md.len() != self.end_byte {
            return Compared::Different;
        }
        let mtime = md.modified().ok();
        if mtime == self.mtime {
            Compared::Unchanged
        } else if self.tail.is_some() {
            Compared::Touched(mtime)
        } else {
            Compared::Different
        }
    }
}

/// Whether the file now open is the one a committed batch read: the same
/// inode, the first page (#578) and the bytes below the committed end (#620)
/// that the batch fingerprinted - [`delete_identity_ok`], the identity test
/// the delete path shares with #620. The ONE identity test every
/// `delete_after_commit` decision goes through: the covering commit, the
/// delete decision, and the check after the file is moved aside.
fn is_the_file_read(
    candidate: &DeleteCandidate,
    md: &std::fs::Metadata,
    head: &[u8],
    tail: &[u8],
) -> bool {
    delete_identity_ok(
        candidate.inode,
        candidate.content.as_ref(),
        candidate.tail.as_ref(),
        inode_of(md),
        head,
        tail,
    )
}

/// What became of one attempt to delete a candidate.
enum DeleteAttempt {
    Deleted,
    /// Left in place because the file is no longer what the candidate
    /// recorded (written, rotated, replaced, or touched with no tail to prove
    /// it unchanged), or is gone, so the candidate can never qualify again.
    Changed(&'static str),
    /// Left in place because only its mtime moved, and its first page, tail
    /// and length prove the content unchanged: the candidate stands with the
    /// new mtime, and its wait starts again.
    Touched(Option<SystemTime>),
    /// Left in place because an operation failed (the open, the rename, the
    /// unlink), not because the file changed: the candidate stands and the
    /// delete is tried again on a later pass (review of #631).
    Failed(String),
}

/// One delete, packaged to run off the async executor (review of #631): it
/// opens, reads, renames and unlinks, all blocking calls, and a pass may
/// have many due at once.
struct DeleteJob {
    path: String,
    candidate: DeleteCandidate,
    #[cfg(test)]
    hook: Option<DeleteHook>,
    #[cfg(test)]
    first_aside: Option<PathBuf>,
}

impl DeleteJob {
    /// Run [`Self::run`] on the blocking pool.
    async fn run_blocking(self) -> DeleteAttempt {
        tokio::task::spawn_blocking(move || self.run())
            .await
            .unwrap_or_else(|error| {
                DeleteAttempt::Failed(format!("the delete task failed: {error}"))
            })
    }

    /// Delete the file if it is still exactly what the candidate recorded,
    /// without losing a write that lands while that is decided.
    ///
    /// MOVED ASIDE, THEN CHECKED AGAIN (#619 A). A check followed by an unlink
    /// lost any line appended between the two: it went into the inode being
    /// unlinked. So the file is first renamed to a hidden name beside it, and
    /// the check is repeated on the renamed file. A writer that opens the path
    /// for each write now creates a new file there, which is read as one; a
    /// write that landed before the rename is seen by the second check, and
    /// the file is put back under its name and kept. What no check can see is
    /// a writer still holding the file open that writes after the second
    /// check - the residual [`DeleteCandidate`] states; the quiet period is
    /// what makes that writer one that had already been silent for that long.
    ///
    /// A crash after the rename leaves the file under a hidden name in the
    /// same directory (`.vtop-delete.<nanos>.<n>`), never deleted. It is
    /// fully archived unless a write landed after the decision to delete it:
    /// one the crash kept from being seen and put back, or one through a held
    /// descriptor after the second check (the window above). If a configured
    /// pattern matches that name it is discovered and archived again: a
    /// duplicate of the archived bytes, plus any such write.
    fn run(self) -> DeleteAttempt {
        let path = Path::new(&self.path);
        let candidate = &self.candidate;
        let Some((md, head, tail)) = continuity::probe_with_tail(path, candidate.tail_window())
        else {
            // GONE is not a failure to retry (review of #631): a candidate
            // kept for a vanished file outlives it, and would be held up
            // against whatever is created at that name next.
            return match path.try_exists() {
                Ok(false) => DeleteAttempt::Changed("the file is gone"),
                _ => DeleteAttempt::Failed("the file could not be opened".to_owned()),
            };
        };
        match candidate.compare(&md, &head, &tail) {
            Compared::Unchanged => {}
            Compared::Touched(mtime) => return DeleteAttempt::Touched(mtime),
            Compared::Different => {
                return DeleteAttempt::Changed(
                    "the file changed since the commit that covered it \
                     (written, rotated, truncated, replaced, or touched with no tail to \
                     prove it unchanged)",
                )
            }
        }
        #[cfg(test)]
        if let Some(hook) = &self.hook {
            hook(path);
        }
        if path.file_name().is_none() {
            return DeleteAttempt::Changed("the path names no file to move aside");
        }
        #[cfg(test)]
        let mut forced = self.first_aside.clone();
        let next_name = || {
            #[cfg(test)]
            if let Some(name) = forced.take() {
                return name;
            }
            aside_name(path)
        };
        let aside = match move_aside(path, next_name) {
            Ok(aside) => aside,
            // On Windows, also what a writer that still has it open causes.
            Err(error) => {
                return DeleteAttempt::Failed(format!("the file could not be moved aside: {error}"))
            }
        };
        // The final check before the unlink: the same identity, first page
        // AND tail, on the renamed file.
        let Some((md, head, tail)) = continuity::probe_with_tail(&aside, candidate.tail_window())
        else {
            // Nothing says it changed; nor can it be checked. Kept, as a
            // failed check always is, and tried again.
            put_back(&aside, path);
            return DeleteAttempt::Failed(
                "the file could not be opened again after it was moved aside".to_owned(),
            );
        };
        match candidate.compare(&md, &head, &tail) {
            Compared::Unchanged => match std::fs::remove_file(&aside) {
                Ok(()) => DeleteAttempt::Deleted,
                Err(error) => {
                    put_back(&aside, path);
                    DeleteAttempt::Failed(format!(
                        "the file was moved aside but could not be deleted: {error}"
                    ))
                }
            },
            Compared::Touched(mtime) => {
                put_back(&aside, path);
                DeleteAttempt::Touched(mtime)
            }
            Compared::Different => {
                tracing::warn!(
                    path = %path.display(),
                    "delete-after-commit: the file was written to while it was being deleted; \
                     it is put back and kept, and the new bytes are read as usual"
                );
                put_back(&aside, path);
                DeleteAttempt::Changed("written to while being deleted")
            }
        }
    }
}

/// How long a file must go unwritten before `delete_after_commit` removes it,
/// unless configured otherwise (`sources.file.delete_quiet_seconds`).
const DEFAULT_DELETE_QUIET: Duration =
    Duration::from_secs(vtop_core::config::DEFAULT_DELETE_QUIET_SECONDS);

pub struct FileSource {
    paths: Vec<String>,
    format: TelemetryFormat,
    delete_after_commit: bool,
    /// Read each file as a single whole-file record (for binary / compressed
    /// source files that have no line structure) instead of line by line.
    whole_file: bool,
    cursors: HashMap<String, FileCursor>,
    /// Alias-group spellings already warned about (#378), keyed by
    /// (device, inode): the key is the file, the value every spelling a
    /// warning has named for it while it lived. An unchanged persistent
    /// hard link stays one line per process instead of one per discovery
    /// cycle — but a group carrying a spelling no line has named yet warns
    /// again, because a warning that stops naming what to go look at has
    /// stopped being useful. A group that only SHRANK stays silent: fewer
    /// copies is not new information.
    /// A `BTreeSet` so ordering differences between passes can never
    /// masquerade as a change. A sync Mutex because `discover_sources`
    /// takes `&self`; it is locked briefly and never across an await.
    warned_aliases: std::sync::Mutex<HashMap<(u64, u64), BTreeSet<String>>>,
    /// How long a fully archived file must stay unwritten before
    /// `delete_after_commit` removes it (#619, see [`DeleteCandidate`]).
    delete_quiet: Duration,
    /// Runs after the delete decision and before the file is moved aside, so
    /// a test can append exactly in the gap #619 (A) describes.
    #[cfg(test)]
    before_delete: Option<DeleteHook>,
    /// The first aside name a delete tries, so a test can make it one that
    /// is already taken.
    #[cfg(test)]
    first_aside: Option<PathBuf>,
    /// Per path, a descriptor on the file its cursor reads, kept while a
    /// range read from it is not committed (#621): when the file is rotated
    /// away, it is the way back to those ranges if one fails.
    held: HashMap<String, HeldFile>,
    /// Files that have left the path their ranges were read under, kept to
    /// re-read a failed range from (#621). See [`crate::rotated`].
    rotated: Vec<RotatedFile>,
    /// How many descriptors `held` and `rotated` keep open in all.
    max_held: usize,
    /// How long a descriptor in `rotated` is kept, unused and with no re-read
    /// in progress.
    hold_for: Duration,
    /// Paths whose handed-back range a re-read through a rotated file's
    /// descriptor had to refuse, for the engine to count
    /// ([`Self::take_refused_rereads`]).
    refused_rereads: Vec<String>,
}

/// What recovery knows is already archived of one file at a path (#621): the
/// end of its newest committed range, and which file that was - its inode,
/// its first page, and the page ending at `end_byte` (#620). A rotated file's
/// re-read never starts below the end of a range committed from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedEnd {
    pub inode: Option<u64>,
    pub end_byte: u64,
    pub content: Option<ContentFingerprint>,
    pub tail: Option<ContentFingerprint>,
}

#[cfg(test)]
type DeleteHook = std::sync::Arc<dyn Fn(&Path) + Send + Sync>;

impl FileSource {
    pub fn new(paths: Vec<String>, format: TelemetryFormat, delete_after_commit: bool) -> Self {
        Self::with_mode(paths, format, delete_after_commit, false)
    }

    /// Construct with an explicit whole-file mode.
    pub fn with_mode(
        paths: Vec<String>,
        format: TelemetryFormat,
        delete_after_commit: bool,
        whole_file: bool,
    ) -> Self {
        Self {
            paths,
            format,
            delete_after_commit,
            whole_file,
            cursors: HashMap::new(),
            warned_aliases: std::sync::Mutex::new(HashMap::new()),
            held: HashMap::new(),
            rotated: Vec::new(),
            max_held: rotated::MAX_HELD_FILES,
            hold_for: rotated::HOLD_FOR,
            refused_rereads: Vec::new(),
            delete_quiet: DEFAULT_DELETE_QUIET,
            #[cfg(test)]
            before_delete: None,
            #[cfg(test)]
            first_aside: None,
        }
    }

    /// How long a fully archived file must stay unwritten before
    /// `delete_after_commit` removes it (#619). Zero deletes as soon as a
    /// commit covers the file - still through the rename-and-check, but with
    /// no quiet-period protection, so it is safe only when nothing can still
    /// hold the file open for writing.
    pub fn with_delete_quiet_period(mut self, quiet: Duration) -> Self {
        self.delete_quiet = quiet;
        self
    }

    /// Seed a committed byte offset (used by the replay engine on startup).
    pub fn seed_committed(&mut self, path: &str, committed_byte: u64) {
        let c = self.cursors.entry(path.to_string()).or_default();
        c.committed_byte = committed_byte;
        c.confirmed_for = None;
        c.read_byte = committed_byte;
    }

    /// Seed a committed offset AND the identity of the file it was taken from.
    ///
    /// The offset alone is a position in a file the ledger cannot name. After
    /// a rotation it describes the OLD file, and seeding it blindly resumed
    /// the new one at the old one's high-water mark (#558). With the inode the
    /// first read compares it against the file now at the path and restarts
    /// the new file at byte 0 when they differ. `None` (a legacy ledger row,
    /// or a platform without inodes) behaves as `seed_committed` does.
    ///
    /// `observed_size` is the length the committed batch saw its file at. A
    /// cursor behind the end of the file (a bounded read) must still notice a
    /// `copytruncate` that leaves the file longer than the committed offset but
    /// shorter than it was: without the size, a restart compares the shrunken
    /// file with the offset alone and resumes in the middle of new content
    /// (review of #564). `None` (an unknown size) leaves only the offset test.
    ///
    /// `content` is the fingerprint that batch took of the file's first bytes
    /// (#578): a file replaced at the same inode while the engine was down,
    /// already longer than the offset, agrees with the inode and the size and
    /// is caught only by its content. `None` (a row from before the
    /// fingerprint) leaves the inode and size tests.
    ///
    /// `tail` is the fingerprint that batch took of the bytes ending at
    /// `committed_byte` (#620): a file rewritten at the same inode while the
    /// engine was down with the same first page - or a reused inode whose
    /// new file starts like the old one - agrees with everything else, and
    /// is caught only by what now lies below the offset. `None` (a row from
    /// before the field) leaves the other tests, exactly as before.
    pub fn seed_committed_from(
        &mut self,
        path: &str,
        committed_byte: u64,
        inode: Option<u64>,
        observed_size: Option<u64>,
        content: Option<ContentFingerprint>,
        tail: Option<ContentFingerprint>,
    ) {
        self.seed_committed(path, committed_byte);
        if inode.is_some() || observed_size.is_some() || content.is_some() || tail.is_some() {
            // The size feeds the truncation test; the empty mtime keeps
            // `can_skip` false, so a seeded cursor always reads first.
            self.cursors.get_mut(path).expect("seeded above").seen = Some(FileIdentity {
                inode,
                file_size: observed_size.unwrap_or(0),
                mtime: String::new(),
                content,
                tail: continuity::Tail::at(committed_byte, tail),
            });
        }
    }

    /// Which of `path`'s committed files - the newest committed range of each
    /// file that has lived there, newest first - the restart seed is taken
    /// from (review of #621). An index into `newest_first`.
    ///
    /// Normally the newest, as it always was: the window read last. But a
    /// range re-read from a file rotated away from the path commits under the
    /// path AFTER the batches of the file now there, so the newest row can
    /// name the rotated file. Seeded from it, the restart took the current
    /// file for a rotation and archived it again from byte 0. So when the
    /// newest row's file is not the one at the path, the seed is the row of the
    /// file that is - but only on POSITIVE identity ([`continuity::proves`],
    /// the same test #567's `committed_through` makes): the same inode, the
    /// first page its marker fingerprinted, the page ending at its end (#620),
    /// and at least that long. Overriding the newest row also needs the row
    /// to CARRY the page below its end: a legacy row with a first page alone
    /// is not evidence enough to depart from the default. Without that, or
    /// without a match, the newest row stands: that costs a re-read of the
    /// current file, never a skip.
    pub fn seed_choice(path: &str, newest_first: &[ArchivedEnd]) -> usize {
        let Some(newest) = newest_first.first() else {
            return 0;
        };
        let Some(current_inode) = std::fs::metadata(path).ok().and_then(|md| inode_of(&md)) else {
            return 0;
        };
        if newest.inode.is_none() || newest.inode == Some(current_inode) {
            return 0;
        }
        newest_first
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, row)| {
                let (Some(inode), Some(content), Some(tail)) = (row.inode, &row.content, &row.tail)
                else {
                    return false;
                };
                let tail = continuity::Tail {
                    end: row.end_byte,
                    fingerprint: tail.clone(),
                };
                continuity::proves_at_path(
                    Path::new(path),
                    &continuity::Identity {
                        inode,
                        contents: &[content],
                        tails: &[&tail],
                        min_len: row.end_byte,
                    },
                )
            })
            .map_or(0, |(i, _)| i)
    }

    /// Re-arm the deferred delete of a file the engine had fully archived but
    /// not yet deleted before a restart (#619), from the seed just given to
    /// [`Self::seed_committed_from`] and the mtime the same committed batch
    /// recorded (`archived_mtime`, the marker's `mtime`). The wait was in
    /// memory; without this the file would stay forever, since nothing new
    /// is ever committed for it. The quiet period starts again here, and the
    /// delete itself checks the file against this identity as it would a
    /// commit's.
    ///
    /// Armed only on evidence that the file is still the one archived:
    /// - the seed names both the inode and the first page - a legacy row
    ///   cannot tell this file from one that replaced it while the engine
    ///   was down;
    /// - the batch saw its file END at the committed offset (review of
    ///   #631): one that read part of a longer file archived only that part,
    ///   and a same-inode rewrite cut back to the offset with the first page
    ///   kept passes every other test. The read's continuity check catches
    ///   that by the length the batch saw, but a delete armed now could run
    ///   before any read;
    /// - the file still has the mtime that batch saw (review of #631): a
    ///   same-inode, same-length rewrite past the first page while the
    ///   engine was down agrees on all of the above, and arming with the
    ///   file's CURRENT mtime as the baseline deleted it unread. This is the
    ///   restart side of the rule that a moved mtime ends a candidate.
    ///   `None` or an empty mtime (unknown) leaves the delete unarmed - unless
    ///   the seed carries the tail fingerprint (#620). Then the delete checks
    ///   the page below the committed end as well as the first, which is what
    ///   proves a moved mtime a touch rather than a rewrite (see
    ///   `DeleteCandidate::compare`), so the wait is armed from the file's
    ///   current mtime, as a touch in process restarts it.
    pub fn arm_delete_from_seed(&mut self, path: &str, archived_mtime: Option<&str>) {
        if !self.delete_after_commit {
            return;
        }
        let archived_mtime = archived_mtime.filter(|m| !m.is_empty());
        let Some(cursor) = self.cursors.get_mut(path) else {
            return;
        };
        let committed_byte = cursor.committed_byte;
        let Some(seen) = cursor.seen.as_ref() else {
            return;
        };
        if seen.inode.is_none() || seen.content.is_none() || seen.file_size != committed_byte {
            return;
        }
        let Ok(md) = std::fs::metadata(path) else {
            return;
        };
        // The seed's tail, when it is the one ending at the committed offset.
        let tail = seen
            .tail
            .as_ref()
            .filter(|tail| tail.end == committed_byte)
            .map(|tail| tail.fingerprint.clone());
        let now = Self::identity_of(&md);
        let same_mtime = archived_mtime.is_some_and(|archived| now.mtime == archived);
        if now.inode != seen.inode
            || now.file_size != committed_byte
            || !(same_mtime || tail.is_some())
        {
            return;
        }
        cursor.delete_candidate = Some(DeleteCandidate {
            inode: seen.inode,
            content: seen.content.clone(),
            tail,
            end_byte: committed_byte,
            mtime: md.modified().ok(),
            since: Instant::now(),
        });
    }

    /// The delete `path`'s cursor is due for: a [`DeleteCandidate`] that has
    /// waited out the quiet period (#619). `None` when there is none, or it
    /// is still waiting.
    fn due_delete(&self, path: &str) -> Option<DeleteJob> {
        if !self.delete_after_commit {
            return None;
        }
        let candidate = self.cursors.get(path)?.delete_candidate.clone()?;
        if candidate.since.elapsed() < self.delete_quiet {
            return None;
        }
        Some(DeleteJob {
            path: path.to_owned(),
            candidate,
            #[cfg(test)]
            hook: self.before_delete.clone(),
            #[cfg(test)]
            first_aside: self.first_aside.clone(),
        })
    }

    /// Apply what a delete did to `path`'s cursor. Returns whether the file
    /// was deleted.
    fn settle_delete(&mut self, path: &str, attempt: DeleteAttempt) -> bool {
        match attempt {
            DeleteAttempt::Deleted => {
                // The file this cursor described is gone. A new file at the
                // path may be handed the freed inode number, and an inode
                // check cannot tell it from the one it replaced, so the cursor
                // must not outlive it (#558). Nor may a descriptor kept to
                // re-read its ranges (#621). The commit that recorded this
                // delete usually released it, but not when a later window had
                // been read and the file was then cut back to this commit's
                // end: the later range is still outstanding. With the file and
                // its cursor gone, nothing would read this path again, so
                // nothing else would close it, and it would pin the deleted
                // file's space (a_delete_closes_a_descriptor_the_adapter_still_
                // held_for_that_path). A rotated file being re-read under
                // this path is a different file and stays.
                self.cursors.remove(path);
                self.held.remove(path);
                tracing::info!(
                    path,
                    "deleted after commit: fully archived and unwritten for the quiet period"
                );
                true
            }
            DeleteAttempt::Changed(why) => {
                // The file is no longer what the candidate recorded, so the
                // candidate can never qualify again. A later commit that
                // covers the file records a new one.
                if let Some(c) = self.cursors.get_mut(path) {
                    c.delete_candidate = None;
                }
                tracing::debug!(path, why, "delete-after-commit: file kept");
                false
            }
            DeleteAttempt::Touched(mtime) => {
                // Only the mtime moved, and the first page, the tail and the
                // length prove the content did not. A file at its end yields
                // no batch, so no new covering commit would ever record a
                // candidate again: the wait restarts on this one instead.
                if let Some(candidate) = self
                    .cursors
                    .get_mut(path)
                    .and_then(|c| c.delete_candidate.as_mut())
                {
                    candidate.mtime = mtime;
                    candidate.since = Instant::now();
                }
                tracing::debug!(
                    path,
                    "delete-after-commit: file kept; only its mtime moved (first page, tail \
                     and length unchanged), so the quiet period starts again"
                );
                false
            }
            DeleteAttempt::Failed(why) => {
                // Nothing says the file changed, so the candidate stands and
                // the next pass tries again; dropping it would keep an
                // archived file forever over one transient failure (review of
                // #631). Debug, not warn: it repeats every pass while the
                // cause lasts.
                tracing::debug!(
                    path,
                    why,
                    "delete-after-commit: file kept for now; the delete is retried"
                );
                false
            }
        }
    }

    /// Delete `path` if its cursor holds a [`DeleteCandidate`] that has waited
    /// out the quiet period and the file is still exactly what it recorded
    /// (#619), on the blocking pool. Returns whether the file was deleted;
    /// its cursor goes with it.
    async fn delete_if_quiet(&mut self, path: &str) -> bool {
        let Some(job) = self.due_delete(path) else {
            return false;
        };
        let attempt = job.run_blocking().await;
        self.settle_delete(path, attempt)
    }

    /// Whether this cycle can skip opening `path` entirely (#100).
    ///
    /// A stat is orders of magnitude cheaper than an open-seek-read, and a
    /// log directory is mostly cold: a few files are being appended to and
    /// the rest are rotated history that will never change again. Skipping
    /// those is where the work goes.
    ///
    /// THE CONDITION IS DELIBERATELY NARROW. All four must hold: the file must
    /// be the same INODE this cursor last saw, at the same SIZE, at the same
    /// MTIME, and the cursor must already have read through to that size.
    /// Anything else — an append, a truncation, a rotation that reused the
    /// name, a file this cursor has never observed — falls through to a real
    /// read, where the rotation handling in `read_slice` (#65) decides what
    /// the identity change means.
    ///
    /// WHY THIS CANNOT TRAP, which the Kafka backoff in #99 could. That one
    /// shortened the poll window of a source that had returned nothing, so a
    /// quiet source became LESS able to observe data, which lengthened the
    /// streak that shortened the window — starvation with no way out, measured
    /// at 48x. Nothing here narrows what a later cycle can see: the stat is a
    /// reliable emptiness check rather than a probabilistic one, it happens
    /// every cycle at full strength, and any byte appended to the file changes
    /// both the size and the mtime it is compared against. A file that starts
    /// changing again is read on the very next cycle.
    fn can_skip(cursor: &FileCursor, current: &FileIdentity) -> bool {
        let Some(seen) = cursor.seen.as_ref() else {
            return false;
        };
        seen.inode == current.inode
            && seen.file_size == current.file_size
            // An UNKNOWN mtime never matches (#411): the identity records
            // the empty string when the filesystem cannot report a modified
            // time, and letting "" equal "" would park this file on a skip
            // no evidence justifies. The claim is exactly that narrow
            // (review): falling through opens the file and applies the
            // adapter's append-only rules — it does not re-read content an
            // in-place same-size rewrite replaced, which stat cannot detect
            // WITH an mtime either (the changed-mtime rewrite reads nothing
            // at EOF too). Unknown evidence forfeits the fast path; the
            // append-only contract is unchanged.
            && !seen.mtime.is_empty()
            && seen.mtime == current.mtime
            // EQUALITY, not `>=` (review). A cursor beyond the file's end is
            // not a caught-up cursor, it is an inconsistency — the file shrank
            // under a read head that had already passed the new end — and the
            // right response to an inconsistency is to look, not to skip. `>=`
            // would answer "caught up" to the one state where the cursor
            // provably does not point at valid unread continuity; `==` says
            // what the check actually means.
            && cursor.read_byte == current.file_size
    }

    /// Stat a path without opening it, as the skip decision does (inside each
    /// read's job, through `tokio::fs`, since both read paths share `run_job`).
    #[cfg(test)]
    fn stat_identity(path: &str) -> Option<FileIdentity> {
        std::fs::metadata(path)
            .ok()
            .map(|md| Self::identity_of(&md))
    }

    /// What a stat can say: everything but the content, which takes a read.
    fn identity_of(md: &std::fs::Metadata) -> FileIdentity {
        FileIdentity {
            inode: inode_of(md),
            file_size: md.len(),
            mtime: md
                .modified()
                .ok()
                .map(|t| {
                    let dt: chrono::DateTime<chrono::Utc> = t.into();
                    dt.to_rfc3339()
                })
                .unwrap_or_default(),
            content: None,
            tail: None,
        }
    }

    /// Identity of whatever the PATH names right now - inode, length and
    /// first bytes, through one descriptor. Only for comparing the current
    /// file against a previously recorded marker (the commit and delete
    /// guards in `commit_progress`, a rewind) — never for building a marker,
    /// which must describe the file actually READ (#65): use the open
    /// descriptor's identity. The first bytes tell a replacement at the same
    /// inode from the file a marker was read from (#578). A file that cannot
    /// be opened reads as gone.
    ///
    /// And the bytes the file now holds where a marker ending at `end` took
    /// its tail fingerprint (#620) - empty when it took none, or when the
    /// file cannot be read (which fails any fingerprint).
    fn probe_with_tail(
        path: &Path,
        end: u64,
        tail: Option<&ContentFingerprint>,
    ) -> (Option<u64>, u64, Vec<u8>, Vec<u8>) {
        match continuity::probe_with_tail(path, tail.map(|tail| (end, tail.len))) {
            Some((md, head, tail)) => (inode_of(&md), md.len(), head, tail),
            None => (None, 0, Vec::new(), Vec::new()),
        }
    }

    /// Marker from a descriptor-derived identity: the inode/size/mtime are of
    /// the file whose BYTES were read, so a rotation between read and marker
    /// construction cannot mix the old file's offsets with the new file's
    /// identity (#65) — which previously let `delete_after_commit` match and
    /// delete the replacement.
    ///
    /// `tail` is what the read saw below a position (#620); the marker
    /// carries it only when that position is its own `end`.
    fn marker_from(
        path: &str,
        id: &FileIdentity,
        start: u64,
        end: u64,
        tail: Option<&continuity::Tail>,
    ) -> ProgressMarker {
        ProgressMarker::File {
            path: path.to_string(),
            inode: id.inode,
            start_byte: start,
            end_byte: end,
            file_size: id.file_size,
            mtime: id.mtime.clone(),
            content_fingerprint: id.content.clone(),
            tail_fingerprint: tail
                .filter(|tail| tail.end == end)
                .map(|tail| tail.fingerprint.clone()),
        }
    }

    /// Fold a completed read into the file's cursor and build its result.
    ///
    /// A discontinuity (rotation, truncation) restarts the cursor on the new
    /// content: nothing of THIS file has been committed, so the committed
    /// point returns to zero too — a replay clamps to it, and a stale one
    /// would silently skip the new file's first bytes (#558).
    fn apply_slice(&mut self, path: &str, slice: SliceRead) -> ReadResult {
        let SliceRead {
            records,
            start,
            end,
            verbatim,
            identity,
            discontinuity,
            descriptor,
            start_tail,
        } = slice;
        let cursor = self.cursors.entry(path.to_string()).or_default();
        let read_byte_before = cursor.read_byte;
        if let Some(why) = discontinuity {
            tracing::warn!(path, previous_read_byte = cursor.read_byte, "{why}");
            cursor.committed_byte = 0;
            cursor.confirmed_for = None;
            cursor.generations.discontinuity();
            cursor.delete_candidate = None;
        }
        cursor.generations.issue(start, end);
        cursor.read_byte = end;
        // What the read ACTUALLY observed, not what a stat saw before it: the
        // file can grow between the two, and recording the earlier identity
        // would let the next cycle skip bytes this one never read.
        cursor.seen = Some(identity.clone());
        if let Some(why) = discontinuity {
            self.retire_held(path, read_byte_before, why);
        }
        self.hold(path, descriptor, &identity, start, end);
        ReadResult {
            progress_start: Self::marker_from(path, &identity, start, start, start_tail.as_ref()),
            progress_end: Self::marker_from(path, &identity, start, end, identity.tail.as_ref()),
            records,
            first_timestamp: None,
            last_timestamp: None,
            verbatim,
        }
    }

    /// A read failed. If it had already found the file under the cursor
    /// replaced or truncated, the cursor restarts on the new content exactly
    /// as a successful read would make it (see [`Self::apply_slice`]): the
    /// failure is in reading the new content, not in noticing it.
    fn apply_failed_read(&mut self, path: &str, failed: SliceError) -> VtopError {
        if let Some(restart) = failed.restart {
            let (why, opened) = *restart;
            let cursor = self.cursors.entry(path.to_string()).or_default();
            let read_byte_before = cursor.read_byte;
            tracing::warn!(
                path,
                previous_read_byte = cursor.read_byte,
                "{why}; the read of the new content then failed"
            );
            cursor.committed_byte = 0;
            cursor.confirmed_for = None;
            cursor.read_byte = 0;
            cursor.generations.discontinuity();
            cursor.delete_candidate = None;
            cursor.seen = Some(opened);
            self.retire_held(path, read_byte_before, why);
        }
        failed.error
    }

    // ---- re-reading a range from a file rotated away (#621) ---------------
    //
    // See `crate::rotated` for the why. The cursor's file is held while a
    // range read from it is outstanding; a rotation retires the descriptor to
    // `rotated` instead of closing it; a rewind refused for the file at the
    // path re-reads the range from a rotated file proven to be the one read.

    /// A read restarted the cursor on new content. A descriptor held on the
    /// file that was RENAMED away still reaches its bytes, and is kept while a
    /// range read from it is outstanding - with the re-read already started
    /// when the cursor had been rewound into it, since that rewind was
    /// accepted on the promise that the range would be read again. After a
    /// truncation or a replacement at the same inode the descriptor reaches
    /// the new content, not the old, and is closed.
    fn retire_held(&mut self, path: &str, read_byte_before: u64, why: Discontinuity) {
        let Some(held) = self.held.remove(path) else {
            return;
        };
        if !matches!(why, Discontinuity::Rotated { .. }) {
            return;
        }
        let rewound = read_byte_before < held.read_end;
        if !held.outstanding() && !rewound {
            return;
        }
        let replay = rewound.then(|| Replay {
            read_byte: read_byte_before.max(held.committed),
            drained: false,
        });
        tracing::info!(
            path,
            inode = held.inode,
            read_end = held.read_end,
            committed = held.committed,
            rewound,
            "file rotated away with ranges not yet archived; its descriptor is kept to re-read them"
        );
        // Moving a descriptor from `held` to `rotated` does not change the
        // count, so there is always room.
        self.rotated.push(RotatedFile {
            path: path.to_string(),
            held,
            replay,
        });
    }

    /// Keep (or refresh) the descriptor on the file a read just handed a
    /// range out of. `descriptor` is a duplicate of the read's, taken only
    /// when the read opened another file than the one held.
    fn hold(
        &mut self,
        path: &str,
        descriptor: Option<std::fs::File>,
        identity: &FileIdentity,
        start: u64,
        end: u64,
    ) {
        let (Some(inode), Some(content)) = (identity.inode, identity.content.clone()) else {
            return;
        };
        if end <= start {
            if self
                .held
                .get(path)
                .is_some_and(|held| held.inode == inode && !held.outstanding())
            {
                self.held.remove(path);
            }
            return;
        }
        if let Some(held) = self.held.get_mut(path) {
            if held.inode == inode && descriptor.is_none() {
                held.read_end = held.read_end.max(end);
                held.seen_len(identity.file_size);
                if identity.tail.is_some() {
                    held.tail = identity.tail.clone();
                }
                held.content = content;
                held.touched = std::time::Instant::now();
                return;
            }
        }
        let Some(file) = descriptor else {
            return;
        };
        if !self.held.contains_key(path) && !self.make_room() {
            tracing::debug!(
                path,
                max = self.max_held,
                "not holding a descriptor: the limit is reached; a failed range of this file \
                 can still be found by its inode if the file is rotated"
            );
            return;
        }
        let committed = self.cursors.get(path).map_or(0, |c| c.committed_byte);
        let mut held = HeldFile::new(file, inode, content, end, committed, identity.file_size);
        held.tail = identity.tail.clone();
        self.held.insert(path.to_string(), held);
    }

    /// Whether one more descriptor may be kept. At the limit, the rotated file
    /// unused for longest with no re-read in progress is closed to make room.
    fn make_room(&mut self) -> bool {
        if self.held.len() + self.rotated.len() < self.max_held {
            return true;
        }
        let Some(oldest) = self
            .rotated
            .iter()
            .enumerate()
            .filter(|(_, rotated)| !rotated.replaying())
            .min_by_key(|(_, rotated)| rotated.held.touched)
            .map(|(i, _)| i)
        else {
            return false;
        };
        let closed = self.rotated.remove(oldest);
        tracing::warn!(
            path = closed.path,
            inode = closed.held.inode,
            max = self.max_held,
            "closed the descriptor on a rotated file to stay within the limit; a range read \
             from it that fails is looked for by its inode instead"
        );
        true
    }

    /// Close the descriptors on rotated files unused for longer than
    /// `hold_for` with no re-read in progress.
    fn expire_rotated(&mut self) {
        let hold_for = self.hold_for;
        self.rotated.retain(|rotated| {
            let keep = rotated.replaying() || rotated.held.touched.elapsed() < hold_for;
            if !keep {
                tracing::info!(
                    path = rotated.path,
                    inode = rotated.held.inode,
                    resolved = rotated.resolved(),
                    "closed the descriptor on a rotated file after {hold_for:?} unused"
                );
            }
            keep
        });
    }

    /// Note a commit for the file it was read from, wherever it is held, and
    /// close a descriptor nothing read from it waits on any more.
    ///
    /// A descriptor counts the commit only if the marker's content - its first
    /// page, and the page ending at its end (#620) - is what the descriptor
    /// reaches now (review of #621). The inode alone is not enough: a window
    /// of a file's old content can commit after the file was rewritten in
    /// place, and raising the new content's floor with it made a later
    /// re-read skip the new content's first bytes. A commit that does not
    /// count costs a re-read at most.
    fn note_committed(
        &mut self,
        path: &str,
        inode: Option<u64>,
        end: u64,
        content: Option<&ContentFingerprint>,
        tail: Option<&ContentFingerprint>,
    ) {
        let Some(inode) = inode else {
            return;
        };
        let tail = continuity::Tail::at(end, tail.cloned());
        if let Some(held) = self.held.get_mut(path) {
            if held.inode == inode && held.holds(content, tail.as_ref()) {
                held.committed_to(end);
                if !held.outstanding() {
                    self.held.remove(path);
                }
            }
        }
        for rotated in self.rotated.iter_mut().filter(|rotated| {
            rotated.path == path
                && rotated.held.inode == inode
                && rotated.held.holds(content, tail.as_ref())
        }) {
            rotated.raise_floor(end);
        }
        self.rotated.retain(|rotated| {
            let done = rotated.path == path && rotated.held.inode == inode && rotated.resolved();
            if done {
                tracing::info!(
                    path,
                    inode,
                    "every range read from a rotated file is archived"
                );
            }
            !done
        });
    }

    /// The rotated file whose range is being re-read under `path`, if any.
    fn replaying(&self, path: &str) -> Option<usize> {
        self.rotated
            .iter()
            .position(|rotated| rotated.path == path && rotated.replaying())
    }

    /// A read of a rotated file found nothing more to re-read.
    fn rotated_drained(&mut self, path: &str) {
        let Some(i) = self.replaying(path) else {
            return;
        };
        if let Some(replay) = self.rotated[i].replay.as_mut() {
            replay.drained = true;
        }
        if self.rotated[i].resolved() {
            let done = self.rotated.remove(i);
            tracing::info!(
                path,
                inode = done.held.inode,
                "every range read from a rotated file is archived"
            );
        }
    }

    /// Fold a re-read of a rotated file into its record and build its result.
    /// The markers name the source path, as the range's first read did, and
    /// the inode the bytes are actually in.
    fn apply_rotated(&mut self, path: &str, slice: SliceRead) -> ReadResult {
        if let Some(rotated) = self.rotated.iter_mut().find(|rotated| {
            rotated.path == path
                && rotated.replaying()
                && Some(rotated.held.inode) == slice.identity.inode
        }) {
            rotated.replay = Some(Replay {
                read_byte: slice.end,
                drained: slice.records.is_empty(),
            });
            rotated.held.read_end = rotated.held.read_end.max(slice.end);
            rotated.held.seen_len(slice.identity.file_size);
            if slice.identity.tail.is_some() {
                rotated.held.tail = slice.identity.tail.clone();
            }
            rotated.held.touched = std::time::Instant::now();
        }
        ReadResult {
            progress_start: Self::marker_from(
                path,
                &slice.identity,
                slice.start,
                slice.start,
                slice.start_tail.as_ref(),
            ),
            progress_end: Self::marker_from(
                path,
                &slice.identity,
                slice.start,
                slice.end,
                slice.identity.tail.as_ref(),
            ),
            records: slice.records,
            first_timestamp: None,
            last_timestamp: None,
            verbatim: slice.verbatim,
        }
    }

    /// A re-read of a rotated file failed: the descriptor no longer proves the
    /// file, or the read itself failed. The range will not be re-read in this
    /// process, so this is loud - and handed to the engine to count, as a
    /// refused rewind is ([`Self::take_refused_rereads`]); a restart looks for
    /// the file again.
    fn rotated_read_failed(&mut self, path: &str, error: VtopError) -> VtopError {
        self.refused_rereads.push(path.to_string());
        if let Some(i) = self.replaying(path) {
            let failed = self.rotated.remove(i);
            tracing::error!(
                path,
                inode = failed.held.inode,
                error = %error,
                "could not re-read a failed range from the file it was rotated to; it will not \
                 be re-read until a restart finds that file again"
            );
        }
        error
    }

    /// The cursor was rewound into the file it holds, and that file has left
    /// the path: its rewound range is re-read from the descriptor, before the
    /// new file at the path is read (whose markers would otherwise be newer).
    fn promote_rewound(&mut self, path: &str) {
        let Some(held) = self.held.remove(path) else {
            return;
        };
        let read_byte = self
            .cursors
            .get(path)
            .map_or(0, |c| c.read_byte)
            .max(held.committed);
        self.rotated.push(RotatedFile {
            path: path.to_string(),
            held,
            replay: Some(Replay {
                read_byte,
                drained: false,
            }),
        });
    }

    /// Re-read a range through a descriptor on a rotated file. The descriptor
    /// is proven again first, as [`rotated::proves`] proves a file: its first
    /// bytes must still be the ones it was held for, and it must be no
    /// shorter than it was seen to be. A truncation above the first page
    /// keeps the fingerprint, and reading on would archive a shortened prefix
    /// as the range - or nothing at all, and call the re-read done (review of
    /// #621). Nor may the page below where it was last read have changed
    /// (#620): a rewrite that kept the first page is not the file.
    async fn read_rotated(
        read: RotatedRead,
        path: &str,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
    ) -> Result<SliceRead, VtopError> {
        let mut file = tokio::fs::File::from_std(read.file);
        let mut identity = Self::identity_of(&file.metadata().await?);
        if identity.file_size < read.min_len || read.start > identity.file_size {
            return Err(VtopError::Source(format!(
                "cannot re-read {path} from the file it was rotated to: that file is now {} \
                 bytes, shorter than the {} it was when its ranges were read (truncated since)",
                identity.file_size,
                read.min_len.max(read.start)
            )));
        }
        let head = continuity::read_head(&mut file).await?;
        if identity.inode != Some(read.inode) || !read.content.matches_head(&head) {
            return Err(VtopError::Source(format!(
                "cannot re-read {path} from the file it was rotated to: that file's first bytes \
                 are no longer the ones the range was read after"
            )));
        }
        if let Some(tail) = read.tail.as_ref() {
            let at_tail =
                continuity::read_ending_at(&mut file, tail.end, tail.fingerprint.len).await?;
            if !tail.fingerprint.matches_bytes(&at_tail) {
                return Err(VtopError::Source(format!(
                    "cannot re-read {path} from the file it was rotated to: the bytes before \
                     byte {} are no longer the ones it was read with (rewritten in place)",
                    tail.end
                )));
            }
        }
        identity.content = ContentFingerprint::of(&head);
        let content = identity.content.clone();
        // The page below the re-read's start, for its markers' tails (#620),
        // exactly as a read of the path takes it.
        let below_len = read.start.min(ContentFingerprint::MAX_LEN as u64);
        let below = continuity::read_ending_at(&mut file, read.start, below_len).await?;
        let mut slice = Self::read_opened(
            file,
            path,
            read.start,
            max_records,
            max_bytes,
            whole_file,
            identity.file_size,
            None,
        )
        .await?;
        slice.identity.content = content;
        slice.start_tail = continuity::Tail::at(read.start, ContentFingerprint::of_tail(&below));
        slice.identity.tail = continuity::Tail::at(
            slice.end,
            tail_of_read(&below, &slice.records, !slice.verbatim),
        );
        Ok(slice)
    }

    /// Hand a range back through a rotated file (#621), when the file at the
    /// path refused it. `true` when the range will be re-read: from a rotated
    /// file already kept, from the descriptor the cursor still holds, or from
    /// a file found where rotation left it - each PROVEN to be the file the
    /// range was read from, by its first page and, when the marker recorded
    /// one, the page ending where the range ends (#620). A marker without an
    /// inode or a first-page fingerprint proves nothing, and is refused as
    /// before.
    fn replay_rotated(&mut self, marker: &ProgressMarker) -> bool {
        let ProgressMarker::File {
            path,
            inode: Some(inode),
            start_byte,
            end_byte,
            file_size: size_when_read,
            content_fingerprint: Some(content),
            tail_fingerprint,
            ..
        } = marker
        else {
            return false;
        };
        let (path, inode, start) = (path.as_str(), *inode, *start_byte);
        let tail = continuity::Tail::at(*end_byte, tail_fingerprint.clone());
        if let Some(i) = self
            .rotated
            .iter()
            .position(|rotated| rotated.path == path && rotated.held.inode == inode)
        {
            let held = &mut self.rotated[i].held;
            let min_len = (*size_when_read).max(held.min_len);
            if held.proves(content, min_len, start, tail.as_ref()) {
                held.seen_len(*size_when_read);
                self.rotated[i].rewind(start);
                return true;
            }
            self.rotated.remove(i);
        }
        if self.held.get(path).is_some_and(|held| held.inode == inode) {
            let mut held = self.held.remove(path).expect("checked above");
            let min_len = (*size_when_read).max(held.min_len);
            if held.proves(content, min_len, start, tail.as_ref()) {
                held.seen_len(*size_when_read);
                let mut rotated = RotatedFile {
                    path: path.to_string(),
                    held,
                    replay: None,
                };
                rotated.rewind(start);
                self.rotated.push(rotated);
                return true;
            }
        }
        let Some((found, file)) = rotated::find(
            path,
            &self.paths,
            inode,
            content,
            *size_when_read,
            start,
            tail.as_ref(),
        ) else {
            return false;
        };
        if !self.make_room() {
            tracing::error!(
                path,
                found = %found.display(),
                max = self.max_held,
                "found the file a failed range was rotated to, but every descriptor the adapter \
                 may keep is re-reading another"
            );
            return false;
        }
        tracing::warn!(
            path,
            found = %found.display(),
            inode,
            start,
            "the file a failed range was read from has left its path; re-reading the range from \
             where it was rotated to"
        );
        let mut held = HeldFile::new(file, inode, content.clone(), 0, 0, *size_when_read);
        held.tail = tail;
        let mut rotated = RotatedFile {
            path: path.to_string(),
            held,
            replay: None,
        };
        rotated.rewind(start);
        self.rotated.push(rotated);
        true
    }

    /// The source paths with a rotated file's range being re-read (#621), for
    /// recovery to look up what of those files is already archived.
    pub fn rotated_replay_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .rotated
            .iter()
            .filter(|rotated| rotated.replaying())
            .map(|rotated| rotated.path.clone())
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }

    /// What the ledger says is archived of the files that have lived at
    /// `path` (#621): a rotated file's re-read starts above the newest range
    /// committed from it. Recovery rewinds every FAILED row again at each
    /// start - they stay in the ledger - so without this a range re-read from
    /// a rotated file was archived again at every restart while that file
    /// existed. Only a row naming the file's inode whose fingerprint its first
    /// bytes match counts; anything else costs a re-read, never a skip.
    pub fn raise_rotated_replay_floor(&mut self, path: &str, archived: &[ArchivedEnd]) {
        for rotated in self
            .rotated
            .iter_mut()
            .filter(|rotated| rotated.path == path && rotated.replaying())
        {
            let len = rotated.held.file.metadata().map_or(0, |md| md.len());
            // A row without a fingerprint (written before #578) is not used.
            // It cannot be what a re-read wrote - every re-read row carries a
            // fingerprint, and only a fingerprinted marker is re-read from a
            // rotated file at all - so it predates the failed range and ends
            // at or below its start, where it raises nothing. Trusting inode
            // and length alone would let a file that reused the inode raise
            // the floor and skip bytes; ignoring it costs a re-read at most.
            // A row whose page below its end (#620) is no longer in the file
            // was read from other content at that inode, and does not count.
            let floor = archived
                .iter()
                .filter(|row| row.inode == Some(rotated.held.inode) && row.end_byte <= len)
                .filter(|row| {
                    let tail = continuity::Tail::at(row.end_byte, row.tail.clone());
                    rotated.held.holds(row.content.as_ref(), tail.as_ref())
                })
                .map(|row| row.end_byte)
                .max();
            if let Some(floor) = floor {
                rotated.raise_floor(floor);
                tracing::info!(
                    path,
                    inode = rotated.held.inode,
                    floor,
                    "a rotated file's re-read starts above what is already archived of it"
                );
            }
        }
    }

    /// The source paths whose handed-back range could not be re-read from the
    /// file it was rotated to since the last call (#621): the file was
    /// truncated, or no longer starts with the bytes it was held for. The
    /// engine counts each as a refused rewind; the error alone reached only
    /// the read-error counter, which does not say a range is missing.
    pub fn take_refused_rereads(&mut self) -> Vec<String> {
        std::mem::take(&mut self.refused_rereads)
    }

    #[cfg(test)]
    fn with_hold_limits(mut self, max_held: usize, hold_for: Duration) -> Self {
        self.max_held = max_held;
        self.hold_for = hold_for;
        self
    }

    /// The descriptor held on the file `path`'s cursor was rewound into: a
    /// range of it was handed back and has not been read again yet.
    fn rewound_held(&self, path: &str) -> Option<&HeldFile> {
        let cursor = self.cursors.get(path)?;
        let seen_inode = cursor.seen.as_ref().and_then(|seen| seen.inode);
        self.held
            .get(path)
            .filter(|held| cursor.read_byte < held.read_end && seen_inode == Some(held.inode))
    }

    /// Snapshot what one file's read needs, so the reads of a pass can run
    /// concurrently and be applied afterwards.
    fn read_job(&mut self, path: &str) -> ReadJob {
        let cursor = self.cursors.entry(path.to_string()).or_default();
        let (start, seen) = (cursor.read_byte, cursor.seen.clone());
        let rotated = self.replaying(path).and_then(|i| {
            let rotated = &self.rotated[i];
            Some(RotatedRead {
                file: rotated.held.file.try_clone().ok()?,
                inode: rotated.held.inode,
                content: rotated.held.content.clone(),
                min_len: rotated.held.min_len,
                tail: rotated.held.tail.clone(),
                start: rotated.replay?.read_byte,
            })
        });
        let rewound = self.rewound_held(path).and_then(|held| {
            Some(RotatedRead {
                file: held.file.try_clone().ok()?,
                inode: held.inode,
                content: held.content.clone(),
                min_len: held.min_len,
                tail: held.tail.clone(),
                start: start.max(held.committed),
            })
        });
        let held = self.held.get(path);
        ReadJob {
            path: path.to_string(),
            start,
            seen,
            held_inode: held.map(|held| held.inode),
            rotated,
            rewound,
        }
    }

    /// One file's read. A rotated file's range being re-read comes first;
    /// only when it has nothing more is the path read. `true` alongside means
    /// that re-read reached its end in this pass.
    async fn run_job(
        job: ReadJob,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
    ) -> (PassOutcome, bool) {
        let mut drained = false;
        if let Some(rotated) = job.rotated {
            match Self::read_rotated(rotated, &job.path, max_records, max_bytes, whole_file).await {
                Ok(slice) if slice.records.is_empty() => drained = true,
                other => return (PassOutcome::Rotated(other), false),
            }
        }
        // A path that cannot be stat'd is NOT skipped: it goes to the read,
        // which reports the error properly instead of having it swallowed
        // here as a silent skip.
        let current = tokio::fs::metadata(&job.path)
            .await
            .ok()
            .map(|md| Self::identity_of(&md));
        if let Some(rewound) = job.rewound {
            if current.as_ref().and_then(|current| current.inode) != Some(rewound.inode) {
                let read =
                    Self::read_rotated(rewound, &job.path, max_records, max_bytes, whole_file)
                        .await;
                return (PassOutcome::Rewound(read), drained);
            }
        }
        if drained && current.is_none() {
            return (PassOutcome::Nothing, drained);
        }
        let snapshot = FileCursor {
            read_byte: job.start,
            seen: job.seen,
            ..FileCursor::default()
        };
        let outcome = match current {
            Some(current) if Self::can_skip(&snapshot, &current) => {
                PassOutcome::SkippedUnchanged(current)
            }
            _ => {
                let last_seen = snapshot.seen.as_ref().map(FileIdentity::observed);
                PassOutcome::Read(
                    Self::read_slice(
                        job.path.clone(),
                        job.start,
                        max_records,
                        max_bytes,
                        whole_file,
                        last_seen,
                        job.held_inode,
                    )
                    .await,
                )
            }
        };
        (outcome, drained)
    }

    /// Fold one file's read into the adapter and build what the engine sees.
    fn apply_outcome(
        &mut self,
        path: &str,
        start: u64,
        outcome: PassOutcome,
        drained: bool,
    ) -> Result<Vec<ReadResult>, VtopError> {
        if drained {
            self.rotated_drained(path);
        }
        match outcome {
            // A skipped file reports an empty read at the cursor it already
            // holds: indistinguishable from one that was read and had nothing
            // to give, which is what it is.
            PassOutcome::SkippedUnchanged(id) => Ok(vec![ReadResult {
                progress_start: Self::marker_from(path, &id, start, start, None),
                progress_end: Self::marker_from(path, &id, start, start, None),
                records: Vec::new(),
                first_timestamp: None,
                last_timestamp: None,
                verbatim: false,
            }]),
            PassOutcome::Read(Ok(slice)) => Ok(vec![self.apply_slice(path, slice)]),
            PassOutcome::Read(Err(failed)) => Err(self.apply_failed_read(path, failed)),
            PassOutcome::Rotated(Ok(slice)) => Ok(vec![self.apply_rotated(path, slice)]),
            PassOutcome::Rotated(Err(error)) => Err(self.rotated_read_failed(path, error)),
            PassOutcome::Rewound(Ok(slice)) => {
                self.promote_rewound(path);
                Ok(vec![self.apply_rotated(path, slice)])
            }
            PassOutcome::Rewound(Err(error)) => {
                self.held.remove(path);
                self.refused_rereads.push(path.to_string());
                tracing::error!(
                    path,
                    error = %error,
                    "a range handed back for re-reading cannot be read from its file, which has \
                     left the path since; it will not be re-read until a restart finds that file"
                );
                Err(error)
            }
            PassOutcome::Nothing | PassOutcome::Deleted => Ok(Vec::new()),
        }
    }

    /// Read one file from `start`, honouring the budgets. Pure with respect to
    /// the adapter (no `&self`): takes everything it needs by value so many
    /// files can be read CONCURRENTLY in one pass (#96 B2) — each file's
    /// cursor is snapshotted before and applied after, so concurrent reads
    /// never touch shared state.
    ///
    /// `last_seen` is what the cursor last observed of the file. The open
    /// descriptor is checked against it (and against `start`) before any byte
    /// is read: a different inode, or a file shorter than `start`, means the
    /// position no longer applies and the read restarts at byte 0 (#558).
    ///
    /// `held_inode` is the file the adapter already holds a descriptor on for
    /// this path (#621); a read of any other file hands back a duplicate of
    /// its own descriptor for the adapter to keep.
    async fn read_slice(
        path: String,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
        last_seen: Option<Observed>,
        held_inode: Option<u64>,
    ) -> Result<SliceRead, SliceError> {
        // Read AND fingerprint through one open descriptor: a rotation between
        // "read the bytes" and "stat the path" would otherwise mix the old
        // file's offsets with the NEW file's identity (#65). The descriptor is
        // judged before any byte is read.
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(SliceError::before_judged)?;
        let mut opened =
            Self::identity_of(&file.metadata().await.map_err(SliceError::before_judged)?);
        // The first page, through the same descriptor: what tells a file from
        // a replacement at the same inode that is no shorter (#578), and what
        // the markers of this read record for the next one to check.
        let head = continuity::read_head(&mut file)
            .await
            .map_err(SliceError::before_judged)?;
        opened.content = ContentFingerprint::of(&head);
        let mut discontinuity = continuity::check(
            last_seen.as_ref(),
            opened.inode,
            opened.file_size,
            &head,
            start,
        );
        // Then the bytes the cursor last fingerprinted below its position, as
        // the file holds them now (#620): a rewrite that kept the first page,
        // or a reused inode whose new file starts like the old one, is told
        // apart only by them. Read only when nothing else was found, so a
        // failure here has no finding to lose (review of #632).
        let seen_tail = last_seen.as_ref().and_then(|seen| seen.tail.as_ref());
        let mut at_seen_tail = Vec::new();
        if let (None, Some(tail)) = (discontinuity, seen_tail) {
            at_seen_tail = continuity::read_ending_at(&mut file, tail.end, tail.fingerprint.len)
                .await
                .map_err(SliceError::before_judged)?;
            discontinuity = continuity::check_tail(last_seen.as_ref(), &at_seen_tail);
        }
        let start = if discontinuity.is_some() { 0 } else { start };
        // From here a failure still reports what the judgement found: a read
        // that saw the file truncated and then failed (a first record over
        // max_bytes, say) must restart the cursor all the same, or the file
        // can regrow past its old length before the next read and the
        // truncation is never seen (review of #564).
        let restart = discontinuity.map(|why| Box::new((why, opened.clone())));
        // A descriptor to keep on this file while its ranges are outstanding
        // (#621), duplicated only when the one held is on another file (or
        // on content this read just found replaced). Failing to duplicate is
        // not a failed read: the file is simply not held.
        let descriptor =
            if opened.inode.is_some() && (opened.inode != held_inode || discontinuity.is_some()) {
                match file.try_clone().await {
                    Ok(dup) => Some(dup.into_std().await),
                    Err(_) => None,
                }
            } else {
                None
            };
        // The page below `start`, which this read's markers fingerprint with
        // the bytes it reads (#620). Usually the very bytes just checked.
        let below_len = start.min(ContentFingerprint::MAX_LEN as u64);
        let below = match seen_tail {
            Some(tail)
                if discontinuity.is_none()
                    && tail.end == start
                    && tail.fingerprint.len == below_len =>
            {
                at_seen_tail
            }
            _ => match continuity::read_ending_at(&mut file, start, below_len).await {
                Ok(below) => below,
                Err(error) => {
                    return Err(SliceError {
                        error: error.into(),
                        restart,
                    })
                }
            },
        };
        Self::read_opened(
            file,
            &path,
            start,
            max_records,
            max_bytes,
            whole_file,
            opened.file_size,
            discontinuity,
        )
        .await
        .map(|mut slice| {
            // The size and mtime are the descriptor's after the read; the
            // content is what it held at open, which an append cannot change.
            slice.identity.content = opened.content;
            slice.descriptor = descriptor;
            // The tails are of the bytes this read consumed, not a second
            // read of the file after it: one rewritten in between must not
            // be fingerprinted as what was archived (#620).
            slice.start_tail = continuity::Tail::at(start, ContentFingerprint::of_tail(&below));
            slice.identity.tail = continuity::Tail::at(
                slice.end,
                tail_of_read(&below, &slice.records, !slice.verbatim),
            );
            slice
        })
        .map_err(|error| SliceError { error, restart })
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_opened(
        mut file: tokio::fs::File,
        path: &str,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
        fsize: u64,
        discontinuity: Option<Discontinuity>,
    ) -> Result<SliceRead, VtopError> {
        // Whole-file mode: read the entire remaining file as one opaque record.
        // Used for binary / already-compressed source files with no line
        // structure. The whole file commits as a single byte range.
        if whole_file {
            let remaining = fsize.saturating_sub(start);
            let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
            if remaining > max_bytes_u64 {
                return Err(VtopError::Source(format!(
                    "whole-file record in {path} is {remaining} bytes, exceeding max_bytes={max_bytes}"
                )));
            }
            file.seek(std::io::SeekFrom::Start(start)).await?;
            // fstat happened before the read. Limit to one byte beyond the
            // budget as well, so a concurrently-growing file cannot turn that
            // size check into an unbounded allocation.
            let mut data = Vec::with_capacity(usize::try_from(remaining).unwrap_or(max_bytes));
            let mut limited = (&mut file).take(max_bytes_u64.saturating_add(1));
            limited.read_to_end(&mut data).await?;
            if data.len() > max_bytes {
                return Err(VtopError::Source(format!(
                    "whole-file record in {path} grew beyond max_bytes={max_bytes} while being read"
                )));
            }
            let end = start.saturating_add(data.len() as u64);
            let records = if data.is_empty() {
                Vec::new()
            } else {
                vec![data]
            };
            let identity = Self::identity_of(&file.metadata().await?);
            return Ok(SliceRead {
                records,
                start,
                end,
                verbatim: true,
                identity,
                discontinuity,
                descriptor: None,
                start_tail: None,
            });
        }

        file.seek(std::io::SeekFrom::Start(start)).await?;
        // Bound the descriptor itself for the whole call. An outer Take over
        // BufReader would still allow its default 8 KiB fill to read beyond a
        // small max_bytes budget.
        let read_limit = u64::try_from(max_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let buffer_capacity = max_bytes.saturating_add(1).clamp(1, 8 * 1024);
        let mut reader = BufReader::with_capacity(buffer_capacity, file.take(read_limit));

        let mut records = Vec::new();
        let mut bytes_read: u64 = 0;
        let mut pos = start;

        loop {
            if records.len() >= max_records || bytes_read as usize >= max_bytes {
                break;
            }
            let remaining = max_bytes.saturating_sub(bytes_read as usize);
            let limit = u64::try_from(remaining)
                .unwrap_or(u64::MAX)
                .saturating_add(1);
            let mut line = Vec::new();
            let n = (&mut reader)
                .take(limit)
                .read_until(b'\n', &mut line)
                .await?;
            if n == 0 {
                break; // EOF
            }
            if n > remaining {
                if records.is_empty() {
                    return Err(VtopError::Source(format!(
                        "record in {path} exceeds max_bytes={max_bytes}"
                    )));
                }
                // Return the already-complete prefix. The over-budget bytes
                // were consumed only from this temporary descriptor; `pos`
                // remains at the prior newline, so the next call re-reads the
                // record with a fresh budget.
                break;
            }
            // Only accept complete (newline-terminated) lines so a partially
            // written tail is not committed.
            if !line.ends_with(b"\n") {
                break;
            }
            pos += n as u64;
            bytes_read += n as u64;
            // Strip trailing newline for the stored record.
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            records.push(line);
        }
        // fstat on the descriptor we READ from — the same file even if the
        // path was rotated away mid-read (#65).
        let identity = Self::identity_of(&reader.get_ref().get_ref().metadata().await?);
        Ok(SliceRead {
            records,
            start,
            end: pos,
            verbatim: false,
            identity,
            discontinuity,
            descriptor: None,
            start_tail: None,
        })
    }
}

/// The identity test `delete_after_commit` makes on its own probe of the
/// path, just before the unlink: the batch's inode, and the content its
/// marker fingerprinted - the first page (#578) and the bytes below its end
/// (#620, `current_tail` being what the file holds there now).
fn delete_identity_ok(
    marker_inode: Option<u64>,
    marker_content: Option<&ContentFingerprint>,
    marker_tail: Option<&ContentFingerprint>,
    current_inode: Option<u64>,
    current_head: &[u8],
    current_tail: &[u8],
) -> bool {
    let same_inode = match (marker_inode, current_inode) {
        (Some(m), Some(cur)) => m == cur,
        (None, None) => true, // platform without inode support: size-only
        _ => false,
    };
    same_inode
        && marker_content.is_none_or(|content| content.matches_head(current_head))
        && continuity::tail_is_current(marker_tail, current_tail)
}

/// The fingerprint of the page that ends where a read stopped (#620), from
/// the bytes it actually consumed: `below`, the page before it started, then
/// its records - each followed by the newline it was read with, unless they
/// are verbatim (whole-file mode).
fn tail_of_read(
    below: &[u8],
    records: &[Vec<u8>],
    newline_terminated: bool,
) -> Option<ContentFingerprint> {
    ContentFingerprint::of_tail(&tail_window(below, records, newline_terminated))
}

/// The bytes [`tail_of_read`] hashes: the last page of `below` followed by
/// the records as they were read - only that page, never a whole record
/// (review of #632): a whole-file record or a long line can be as large as
/// the batch limit, and copying it to hash its last 4 KiB doubled the read's
/// peak memory.
fn tail_window(below: &[u8], records: &[Vec<u8>], newline_terminated: bool) -> Vec<u8> {
    // Walk back from the end, taking what each piece contributes to the last
    // page; then lay the pieces out forwards.
    let mut wanted = ContentFingerprint::MAX_LEN;
    let mut pieces: Vec<&[u8]> = Vec::new();
    for record in records.iter().rev() {
        if wanted == 0 {
            break;
        }
        if newline_terminated {
            pieces.push(b"\n");
            wanted -= 1;
        }
        let take = wanted.min(record.len());
        pieces.push(&record[record.len() - take..]);
        wanted -= take;
    }
    pieces.push(&below[below.len().saturating_sub(wanted)..]);
    let mut bytes = Vec::with_capacity(ContentFingerprint::MAX_LEN);
    for piece in pieces.iter().rev() {
        bytes.extend_from_slice(piece);
    }
    bytes
}

/// Inode / size / mtime of the file a read actually consumed, taken from the
/// OPEN descriptor (`fstat`), never from a second path lookup - and the
/// fingerprint of its first bytes, read through it (#578).
#[derive(Debug, Clone)]
struct FileIdentity {
    inode: Option<u64>,
    file_size: u64,
    mtime: String,
    /// `None` from a stat (which reads no content), for an empty file, and
    /// for a cursor seeded from a row that recorded none.
    content: Option<ContentFingerprint>,
    /// What the bytes below the cursor's position were (#620). `None` from a
    /// stat, for a read that ended at byte 0, and for a cursor seeded from a
    /// row that recorded none.
    tail: Option<continuity::Tail>,
}

impl FileIdentity {
    /// What the next read is judged against.
    fn observed(&self) -> Observed {
        Observed {
            inode: self.inode,
            size: self.file_size,
            content: self.content.clone(),
            tail: self.tail.clone(),
        }
    }
}

/// The path with `.` components removed, for comparing two spellings of it.
///
/// `PathBuf` equality already ignores an INTERIOR `.` — `a/./b` and `a/b`
/// compare equal, because `Components` skips it — so this exists for the
/// LEADING one, which it keeps: `./a/b` and `a/b` do not compare equal, and a
/// config carrying one relative pattern written each way would otherwise
/// discover the file twice. Probed rather than assumed; the direct test below
/// is what proves the difference, since the discovery test runs on absolute
/// paths where `PathBuf` alone would have been enough.
///
/// Lexical and total: no filesystem access, so it cannot fail, cannot block,
/// and cannot change its answer between two calls in one discovery pass.
///
/// `..` is preserved rather than resolved. Resolving it lexically is wrong
/// whenever a symlink precedes it — `a/link/../b` is not `a/b` — and the
/// consequence of being wrong in that direction is merging two real sources
/// into one, which loses data. Leaving it costs at most a duplicate, which is
/// the defect this exists to reduce rather than one it creates.
fn without_dot_components(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    if !path
        .components()
        .any(|component| matches!(component, Component::CurDir))
    {
        // The overwhelmingly common case: nothing to strip, nothing to
        // allocate beyond the copy the set needs anyway.
        return path.to_path_buf();
    }
    path.components()
        .filter(|component| !matches!(component, Component::CurDir))
        .collect()
}

/// Surviving directory entries that name the SAME file, grouped (#378): each
/// group is every spelling sharing one (device, inode), with its id, in
/// first-seen order. Entries with no identity never group — on a platform
/// without inode identity (or after a raced deletion) "unknown" must not
/// alias with "unknown", because a false alias warning teaches operators to
/// ignore the true ones.
fn alias_groups(entries: &[(String, Option<(u64, u64)>)]) -> Vec<((u64, u64), Vec<String>)> {
    let mut by_id: HashMap<(u64, u64), Vec<String>> = HashMap::new();
    let mut order: Vec<(u64, u64)> = Vec::new();
    for (path, id) in entries {
        let Some(id) = id else { continue };
        let group = by_id.entry(*id).or_insert_with(|| {
            order.push(*id);
            Vec::new()
        });
        group.push(path.clone());
    }
    order
        .into_iter()
        .filter_map(|id| {
            let group = by_id.remove(&id)?;
            (group.len() > 1).then_some((id, group))
        })
        .collect()
}

/// Which of this pass's alias groups deserve a warning line, given what
/// earlier passes already named — the bookkeeping behind "one line per
/// process" (#388), kept beside `alias_groups` so the decision tests as a
/// pure function, the way `alias_groups` does.
///
/// A group warns when it carries a spelling no earlier line has named for
/// its identity. Keying on bare identity looked equivalent and was not: a
/// group that gained a hard link in a later cycle was silently suppressed,
/// so the one path an operator most needed to go look at was exactly the
/// one never named. Growth is the only trigger — a group that merely
/// shrank stays silent, because fewer copies is not new information, and
/// a stored-set equality check would turn one stat blip on a
/// three-spelling group into two lines of noise (once when a spelling
/// blinked out, again when it returned). The stored value therefore
/// accumulates every spelling named while the identity lives, as a
/// `BTreeSet` so ordering differences between passes can never masquerade
/// as a change; the returned group keeps `alias_groups`' first-seen order,
/// because that order is part of what the warning names.
///
/// Afterwards the map is cut back to identities present anywhere in THIS
/// pass — the FULL identities list, not just the grouped entries. The full
/// list is what makes a transient single-spelling stat failure evict
/// nothing and re-warn nothing (the surviving spelling still carries the
/// identity), while an inode freed by `delete_after_commit` and reused by
/// an unrelated file cannot inherit its dead predecessor's suppression, and
/// the map stays bounded by live files. A pass that discovered no
/// identities at all is exempt: it proves nothing about any one of them.
fn alias_groups_to_warn(
    warned: &mut HashMap<(u64, u64), BTreeSet<String>>,
    groups: Vec<((u64, u64), Vec<String>)>,
    identities: &[(String, Option<(u64, u64)>)],
) -> Vec<Vec<String>> {
    let mut to_warn = Vec::new();
    for (id, group) in groups {
        let composition: BTreeSet<String> = group.iter().cloned().collect();
        let named = warned.entry(id).or_default();
        if !composition.is_subset(named) {
            named.extend(composition);
            to_warn.push(group);
        }
    }
    if !warned.is_empty() {
        // Skipped entirely while the map is empty — the common alias-free
        // process never pays for it, and indexing every file seen this pass
        // would tax back part of what #363 reclaimed.
        let live: std::collections::HashSet<(u64, u64)> =
            identities.iter().filter_map(|(_, id)| *id).collect();
        // A pass that discovered nothing proves nothing about any single
        // identity: an unmounted directory or a burst of stat failures looks
        // exactly like a mass delete, and forgetting on the former turns
        // recovery into a re-warn of every group the process already named.
        // Eviction waits for a pass that saw SOMETHING and not this identity.
        if !live.is_empty() {
            warned.retain(|id, _| live.contains(id));
        }
    }
    to_warn
}

/// The hidden name beside `path` that a file is moved to while it is deleted
/// (#619). The clock and a process-wide counter make it unique per attempt,
/// so a name left by a crash is never overwritten by a later attempt, nor one
/// attempt by another in the same instant.
///
/// BOUNDED, and built from nothing of the file's own name (review of #631):
/// derived from it, a legal name near the filesystem's 255-byte limit gave a
/// hidden name over it, the rename failed with ENAMETOOLONG on every pass,
/// and the archived file was never deleted. Under 80 bytes here, whatever
/// the source is called.
fn aside_name(path: &Path) -> PathBuf {
    static ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.with_file_name(format!(".vtop-delete.{nanos}.{attempt}"))
}

/// How many aside names a delete tries before giving up for this pass.
const ASIDE_ATTEMPTS: usize = 8;

/// Move `path` to the first name `next_name` offers that nothing else has,
/// and return that name. NEVER OVER AN EXISTING FILE (review of #631): a
/// plain rename replaces its destination, and the destination could be a
/// file a crash left mid-delete - which may hold bytes not yet archived -
/// or anyone else's. A name already taken is skipped for the next one.
fn move_aside(path: &Path, mut next_name: impl FnMut() -> PathBuf) -> std::io::Result<PathBuf> {
    let mut taken = None;
    for _ in 0..ASIDE_ATTEMPTS {
        let aside = next_name();
        match rename_noreplace(path, &aside) {
            Ok(()) => return Ok(aside),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                taken = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(taken.unwrap_or_else(|| std::io::Error::other("no aside name to try")))
}

/// Rename that fails with `AlreadyExists` instead of replacing `to`:
/// `renameat2(RENAME_NOREPLACE)` on Linux, `renameatx_np(RENAME_EXCL)` on
/// Apple platforms, both atomic. A filesystem or kernel that does not support
/// the flag falls back to [`rename_unless_exists`].
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags, CWD};
    use rustix::io::Errno;
    match renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE) {
        Ok(()) => Ok(()),
        Err(e) if e == Errno::INVAL || e == Errno::NOSYS || e == Errno::NOTSUP => {
            rename_unless_exists(from, to)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    rename_unless_exists(from, to)
}

/// Where no atomic no-replace rename exists: refuse a destination that is
/// already there, then rename. A file created at `to` between the two would
/// still be replaced; with names unique by clock and counter, only a process
/// guessing the next one could do that.
fn rename_unless_exists(from: &Path, to: &Path) -> std::io::Result<()> {
    if std::fs::symlink_metadata(to).is_ok() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the aside name is taken",
        ));
    }
    std::fs::rename(from, to)
}

/// Give a file moved aside for deletion its name back. A hard link, not a
/// rename: a rename would replace a new file a writer has created at the path
/// meanwhile, and that file has not been read either.
fn put_back(aside: &Path, path: &Path) {
    match std::fs::hard_link(aside, path) {
        Ok(()) => {
            if let Err(error) = std::fs::remove_file(aside) {
                tracing::warn!(
                    path = %path.display(),
                    aside = %aside.display(),
                    %error,
                    "delete-after-commit: the file is back under its name, but the name it \
                     was moved aside to could not be removed; both now name it"
                );
            }
        }
        Err(error) => tracing::error!(
            path = %path.display(),
            aside = %aside.display(),
            %error,
            "delete-after-commit: a file moved aside to be deleted could not be put back \
             (a new file already has its name, or the filesystem has no hard links); it is \
             left under the hidden name and may hold bytes not yet archived - move it back \
             or archive it by hand"
        ),
    }
}

/// The (device, inode) pair that makes two directory entries provably one
/// file. `metadata` follows symlinks, so a link and its target answer the
/// same pair — which is exactly the aliasing #378 asks about.
#[cfg(unix)]
fn dev_ino(md: &std::fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((md.dev(), md.ino()))
}

#[cfg(not(unix))]
fn dev_ino(_md: &std::fs::Metadata) -> Option<(u64, u64)> {
    None
}

/// How many files are read concurrently in one pass. Bounded so a glob that
/// matches thousands of files does not open them all at once.
const FILE_READ_CONCURRENCY: usize = 8;

/// One file's fate in a read pass: skipped because the stat proved the
/// cursor already caught up (#100), carrying the identity the stat saw so
/// the outcome's markers can name it — or read, successfully or not. The
/// decision is made inside the bounded pipeline, so the deciding stat rides
/// the same concurrency as the read it gates instead of serializing on the
/// executor thread ahead of every read (#412).
/// A failed read, and - when it had already judged the open file replaced or
/// truncated - that discontinuity and the identity it observed.
struct SliceError {
    error: VtopError,
    // Boxed: the identity is large and failures are rare.
    restart: Option<Box<(Discontinuity, FileIdentity)>>,
}

impl SliceError {
    /// A failure before the open file was judged (opening it, or its fstat).
    fn before_judged(error: std::io::Error) -> Self {
        Self {
            error: error.into(),
            restart: None,
        }
    }
}

/// What a completed read hands back.
struct SliceRead {
    records: Vec<Vec<u8>>,
    /// The byte the read STARTED at. Normally the cursor's read position; byte
    /// 0 when the file under the cursor was rotated or truncated (#558), so
    /// the markers describe the bytes actually read.
    start: u64,
    /// The byte the read ended at.
    end: u64,
    /// Whether the records are verbatim (whole-file mode).
    verbatim: bool,
    /// The identity the read observed, from the open descriptor.
    identity: FileIdentity,
    /// Set when the read position could not be applied to the file found at
    /// the path, and the read restarted at byte 0.
    discontinuity: Option<Discontinuity>,
    /// What the read saw below `start` (#620), for its start marker; the
    /// tail below `end` is the identity's.
    start_tail: Option<continuity::Tail>,
    /// A duplicate of the read's descriptor, for the adapter to keep while
    /// the ranges read through it are outstanding (#621).
    descriptor: Option<std::fs::File>,
}

/// A descriptor on a rotated file, and where to re-read it from (#621).
struct RotatedRead {
    file: std::fs::File,
    inode: u64,
    /// The first bytes the file must still start with.
    content: ContentFingerprint,
    /// How long it must still be: it was seen this long (review of #621).
    min_len: u64,
    /// The page it must still hold where it was last read up to (#620).
    tail: Option<continuity::Tail>,
    start: u64,
}

/// One file's share of a read pass, snapshotted from the adapter so many
/// files can be read concurrently (#96 B2).
struct ReadJob {
    path: String,
    start: u64,
    seen: Option<FileIdentity>,
    held_inode: Option<u64>,
    /// A rotated file's range being re-read: read before the path (#621).
    rotated: Option<RotatedRead>,
    /// The descriptor held on the file the cursor was rewound into: if that
    /// file has left the path, the rewound range is read from it (#621).
    rewound: Option<RotatedRead>,
}

enum PassOutcome {
    SkippedUnchanged(FileIdentity),
    Read(Result<SliceRead, SliceError>),
    /// A range re-read from a rotated file (#621).
    Rotated(Result<SliceRead, VtopError>),
    /// A rewound range re-read from the file that has left the path since.
    Rewound(Result<SliceRead, VtopError>),
    /// Nothing to read: the path is gone, and a rotated file's re-read has
    /// reached its end.
    Nothing,
    /// Deleted by `delete_after_commit` before the pass (#619): fully
    /// archived, so there was nothing left to read.
    Deleted,
}

#[async_trait]
impl SourceAdapter for FileSource {
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
        let mut out = Vec::new();
        // ONE ENTRY PER FILE, not one per pattern that matched it (#376).
        //
        // Two globs can name the same path — `*.log` and a service prefix is
        // the obvious way it happens, and the shipped example config already
        // carries two `file.paths` entries — and without this the file was
        // discovered twice. Both discoveries carry the same `source_name`, so
        // both jobs snapshot the same starting offset and both outcomes route
        // into the same pending buffer: the records were archived twice, from
        // a configuration nothing warns about.
        //
        // Keyed on the resolved path rather than the pattern, because the
        // pattern is what differs and the file is what matters.
        // KEYED ON THE SPELLING WITH `.` REMOVED, NAMED BY WHAT WAS MATCHED.
        //
        // Two patterns can name one file and `glob` returns each match
        // verbatim, so `logs/app.log` and `logs/./app.log` are two strings for
        // one path — probed, not assumed. Stripping `.` components makes them
        // the same key, with no filesystem access and no ambiguity.
        //
        // `..` IS DELIBERATELY LEFT ALONE. Collapsing it lexically is wrong in
        // the one case that matters: if `link` is a symlink, `a/link/../b` and
        // `a/b` are different files, and merging them would DROP a real source.
        // Failing to merge two spellings costs a duplicate; merging two
        // sources costs data. The asymmetry decides it.
        //
        // Aliases — a symlink beside its target, or two hard links — are not a
        // spelling question, and the RULING (#378) is that they are two
        // sources: each entry is archived under its own name, so the file's
        // records are archived twice, and discovery says so (below). No merge
        // and no knob, because a merge needs a cursor identity that survives
        // one alias being removed, and there is no safe one: keyed by the
        // surviving spelling, `delete_after_commit` removes it and the next
        // discovery re-reads the target from zero (#376 re-entered); keyed by
        // the inode, a freed inode reused by a NEW file inherits the old
        // cursor and that file's opening records are never read. A duplicate
        // costs storage; a merge risks data. `canonicalize` looks like it
        // settles this and does not: it merges symlinks, cannot see hard
        // links at all, and has exactly the first failure above.
        let mut seen = std::collections::HashSet::<std::path::PathBuf>::new();
        // Surviving spellings with their (device, inode), for the alias
        // warning below (#378).
        let mut identities: Vec<(String, Option<(u64, u64)>)> = Vec::new();
        for pattern in &self.paths {
            for entry in glob::glob(pattern)
                .map_err(|e| VtopError::Source(format!("bad glob {pattern}: {e}")))?
            {
                let Ok(p) = entry else { continue };
                // ONE metadata call answers both questions: the is-a-file
                // test this arm always made (`is_file()` is a stat in
                // disguise) and the identity the alias check needs. A second
                // stat per file per cycle would tax back part of what #363
                // reclaimed.
                let Ok(md) = std::fs::metadata(&p) else {
                    continue;
                };
                if !md.is_file() {
                    continue;
                }
                if !seen.insert(without_dot_components(&p)) {
                    continue;
                }
                let name = p.to_string_lossy().into_owned();
                identities.push((name.clone(), dev_ino(&md)));
                out.push(DiscoveredSource {
                    source_type: SourceType::File,
                    source_name: name,
                    format: self.format.clone(),
                });
            }
        }
        // Two surviving entries for ONE inode are archived as two sources,
        // by ruling (#378, above) — and never silently. Warned once per
        // alias-group COMPOSITION per process (#388): the duplication is
        // either intended or a config surprise, and both deserve exactly one
        // line, not one per cycle — until the group changes, when the line
        // that named it no longer says what to go look at.
        let to_warn = {
            let mut warned = self.warned_aliases.lock().expect("alias map lock poisoned");
            alias_groups_to_warn(&mut warned, alias_groups(&identities), &identities)
        };
        for group in to_warn {
            tracing::warn!(
                paths = ?group,
                "two directory entries name one file; each is archived as its own source, so \
                 its records are archived twice — by ruling (#378), since no cursor identity \
                 survives an alias being removed. Narrow the patterns to one spelling if one \
                 source is meant"
            );
        }
        // A range being re-read from a rotated file is read under its source
        // path (#621), and a path with nothing at it right now - rotated away
        // with no new file created yet - matches no pattern. It stays a source
        // until its re-read has reached the end of the rotated file. So does a
        // path whose cursor was rewound into the file it holds a descriptor
        // on (review of #621): if that file was renamed away with nothing
        // created in its place, only a read of the path moves the rewound
        // range onto the rotated list, and no read happens for a path that is
        // not a source.
        let replaying = self
            .rotated
            .iter()
            .filter(|rotated| rotated.replaying())
            .map(|rotated| rotated.path.as_str());
        let rewound = self
            .held
            .keys()
            .map(String::as_str)
            .filter(|path| self.rewound_held(path).is_some());
        for path in replaying.chain(rewound) {
            if seen.insert(without_dot_components(Path::new(path))) {
                out.push(DiscoveredSource {
                    source_type: SourceType::File,
                    source_name: path.to_string(),
                    format: self.format.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn read_batch_candidates(
        &mut self,
        source: &DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        _max_wait: Duration,
    ) -> Result<Vec<ReadResult>, VtopError> {
        let path = source.source_name.clone();
        // A fully archived file that has stayed unwritten for the quiet period
        // goes now (#619); nothing in it is left to read.
        if self.delete_if_quiet(&path).await {
            return Ok(Vec::new());
        }
        // Cheapest signal first (#100): an unchanged, fully-read file is
        // skipped without an open (see `run_job`). The marker still reports
        // the cursor where it stands, so a skipped file is indistinguishable
        // from one that was read and had nothing to give — which is what it is.
        self.expire_rotated();
        let job = self.read_job(&path);
        let start = job.start;
        let (outcome, drained) = Self::run_job(job, max_records, max_bytes, self.whole_file).await;
        self.apply_outcome(&path, start, outcome, drained)
    }

    /// Read every file CONCURRENTLY (#96 B2). Each file's read is independent
    /// — its own handle, its own snapshotted cursor — so disk I/O overlaps
    /// instead of queueing behind the slowest file. The skip-deciding stat
    /// rides inside the same bounded jobs (#412), so no file's metadata
    /// latency is paid on the executor thread or ahead of another file's
    /// read. Cursor updates are applied serially after the joins, keeping
    /// all shared state on this thread.
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[DiscoveredSource],
        max_records: usize,
        max_bytes: usize,
        _max_wait: Duration,
    ) -> Result<AdapterReadReport, VtopError> {
        use futures::StreamExt;
        let started = std::time::Instant::now();

        let whole_file = self.whole_file;
        // STAT BEFORE READ (#100), decided INSIDE the bounded pipeline
        // (#412). The signal is unchanged — a directory of rotated logs is
        // mostly cold, and a stat costs a fraction of an open-seek-read —
        // but the stats used to run as a sequential synchronous pre-pass on
        // the executor thread, so a large directory on a slow or network
        // filesystem serialized every stat before the first read dispatched,
        // while the reads they gated went through tokio::fs. Only the cursor
        // snapshot is taken up front; each job stats and decides for itself
        // under the same bounded concurrency as the read it gates.
        // Fully archived files that have stayed unwritten for the quiet period
        // go first (#619): nothing in them is left to read, and reading one
        // would only recreate the cursor its delete dropped.
        //
        // The deletes are blocking I/O (open, read, rename, unlink), so they
        // run on the blocking pool under the same bound as the reads, never
        // inline on the executor one after another (review of #631): a
        // directory whose quiet periods run out together would otherwise
        // serialize them all ahead of every read, the pre-pass #412 removed.
        // Only the decision of which are due, and the cursor updates after,
        // run here.
        let due: Vec<(usize, DeleteJob)> = sources
            .iter()
            .enumerate()
            .filter_map(|(i, s)| self.due_delete(&s.source_name).map(|job| (i, job)))
            .collect();
        let attempts: Vec<(usize, DeleteAttempt)> = futures::stream::iter(
            due.into_iter()
                .map(|(i, job)| async move { (i, job.run_blocking().await) }),
        )
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .collect()
        .await;
        // A set, so the filter below stays linear however many go at once.
        let mut deleted = std::collections::HashSet::new();
        for (i, attempt) in attempts {
            if self.settle_delete(&sources[i].source_name, attempt) {
                deleted.insert(i);
            }
        }
        self.expire_rotated();
        let jobs: Vec<(usize, ReadJob)> = sources
            .iter()
            .enumerate()
            .filter(|(i, _)| !deleted.contains(i))
            .map(|(i, s)| (i, self.read_job(&s.source_name)))
            .collect();

        let mut results: Vec<(usize, String, u64, PassOutcome, bool)> =
            futures::stream::iter(jobs.into_iter().map(|(i, job)| async move {
                let (path, start) = (job.path.clone(), job.start);
                let (outcome, drained) =
                    Self::run_job(job, max_records, max_bytes, whole_file).await;
                (i, path, start, outcome, drained)
            }))
            .buffer_unordered(FILE_READ_CONCURRENCY)
            .collect()
            .await;
        results.extend(deleted.into_iter().map(|i| {
            (
                i,
                sources[i].source_name.clone(),
                0,
                PassOutcome::Deleted,
                false,
            )
        }));
        // buffer_unordered completes in I/O order; report in source order.
        results.sort_by_key(|(i, ..)| *i);
        // COUNTED, not silent. The issue asks for deferred work to be visible,
        // and a cycle that skipped nine files out of ten is a very different
        // cycle from one that read all ten and found them empty — they look
        // identical in every other metric.
        let skipped_count = results
            .iter()
            .filter(|(_, _, _, outcome, _)| matches!(outcome, PassOutcome::SkippedUnchanged(_)))
            .count();
        if skipped_count > 0 {
            tracing::debug!(
                skipped = skipped_count,
                read = results.len() - skipped_count,
                "file_cycle_skipped_unchanged"
            );
        }

        let mut report = AdapterReadReport {
            outcomes: Vec::with_capacity(results.len()),
            productive_ms: 0,
            empty_ms: 0,
            failed_ms: 0,
            retention_lost: Vec::new(),
        };
        let mut any_records = false;
        let mut any_failed = false;
        // One loop in source order — the sort above already restored it, and
        // a skipped file reports an empty read at the cursor it already
        // holds, so the caller sees one outcome per source however the cycle
        // got there.
        for (source_index, path, start, outcome, drained) in results {
            let result = self.apply_outcome(&path, start, outcome, drained);
            match &result {
                Ok(reads) => any_records |= reads.iter().any(|read| !read.is_empty()),
                Err(_) => any_failed = true,
            }
            report.outcomes.push(SourceReadOutcome {
                source_index,
                result,
            });
        }
        // The reads overlapped, so the pass's wall-clock is one shared bucket
        // (splitting per source would double-count it): productive if ANY file
        // yielded, else failed if ANY file errored, else empty. File reads
        // never block on a poll window, so this is microseconds either way.
        let elapsed = started.elapsed().as_millis() as u64;
        if any_records {
            report.productive_ms = elapsed;
        } else if any_failed {
            report.failed_ms = elapsed;
        } else {
            report.empty_ms = elapsed;
        }
        Ok(report)
    }

    async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::File {
            path,
            start_byte,
            end_byte,
            inode: marker_inode,
            file_size: marker_file_size,
            content_fingerprint: marker_content,
            tail_fingerprint: marker_tail,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        // Whatever the cursor makes of it below, these bytes of that file are
        // archived: a descriptor kept to re-read them can be let go (#621) -
        // if it still reaches the content they were read from.
        self.note_committed(
            path,
            *marker_inode,
            *end_byte,
            marker_content.as_ref(),
            marker_tail.as_ref(),
        );
        // A batch read BEFORE a rotation or truncation commits after the
        // cursor has already restarted on the new content. Moving the cursor
        // to its offset would carry it past bytes of a file that were never
        // read. The batch is archived and its ledger row commits; only the
        // cursor stays put (#558). A replacement at the same inode is told
        // apart by its first bytes (#578), and a rewrite that kept them by
        // the bytes below the range's end (#620) - the only test left once
        // the generation record has none (evicted, or a restart).
        let (current_inode, current_size, current_head, current_tail) =
            Self::probe_with_tail(Path::new(path), *end_byte, marker_tail.as_ref());
        // The length test cannot see a truncation that has since regrown past
        // this range; the record of which generation it was read in can.
        let generation = self
            .cursors
            .entry(path.clone())
            .or_default()
            .generations
            .settle(*start_byte, *end_byte);
        let still_current = generation != continuity::Settled::Stale
            && continuity::commit_is_current(
                *marker_inode,
                marker_content.as_ref(),
                current_inode,
                current_size,
                &current_head,
                *end_byte,
            )
            && continuity::tail_is_current(marker_tail.as_ref(), &current_tail);
        if still_current {
            let c = self.cursors.entry(path.clone()).or_default();
            // A range this process has already committed past, in this file,
            // is superseded (#624): it is archived, and so is everything up
            // to the committed point. A VERIFIED range whose commit failed, or
            // that recovery held back, reaches here after newer ranges have
            // committed; moving the cursor to its end put the committed point
            // back and - for a range read before a restart - the read head
            // too, and everything read since was archived again. Success, as
            // the Kafka adapter reports for a range below its frontier, so the
            // row is recorded SOURCE_COMMITTED: it is VERIFIED, and its source
            // has committed through its end. Only a point this process
            // confirmed, for this marker's file: the seed can be a previous
            // generation's, and a point confirmed for another file says
            // nothing about this range (see `confirmed_for`). Nothing is
            // deleted for it either; the newer commit already decided that.
            if c.committed_byte >= *end_byte
                && c.confirmed_for
                    .as_ref()
                    .is_some_and(|file| file.is(*marker_inode, marker_content.as_ref()))
            {
                tracing::info!(
                    path,
                    end_byte,
                    committed_byte = c.committed_byte,
                    "file commit skipped: the range is superseded by a later committed one, and \
                     committing it would move the cursor back"
                );
                return Ok(());
            }
            c.committed_byte = *end_byte;
            c.confirmed_for =
                crate::continuity::ConfirmedFile::of(*marker_inode, marker_content.as_ref());
            // A commit with no in-process record is recovery after a restart.
            let recovered = generation == continuity::Settled::Unknown;
            if c.read_byte < *end_byte {
                c.read_byte = *end_byte;
            } else if recovered && c.read_byte > *end_byte {
                // A read head ABOVE a recovered commit came from the startup
                // seed - the newest COMMITTED row, which can be the previous
                // generation's when this batch (read after a copytruncate)
                // was only VERIFIED at the crash. Keeping that head, and the
                // longer length it observed, would resume above this range
                // in a file that has since regrown, skipping the bytes in
                // between (review of #564). Restart from this batch's end:
                // a same-generation batch recovered out of order costs a
                // re-read, never a skip. In-process commits keep the head -
                // there it is ahead because later reads are buffered, not
                // because it is stale.
                c.read_byte = *end_byte;
            }
            if recovered {
                // Whatever its end, a recovered commit judged current
                // describes the file now at the path, and the seed may
                // describe the one it replaced at the same inode (review of
                // #578): kept, the seed's fingerprint made the next read take
                // this file for another replacement and archive its
                // committed prefix again. So the cursor adopts the identity
                // this batch saw.
                //
                // A legacy marker names no inode, and a commit without one
                // passes whatever file is at the path - including one rotated
                // in before recovery ran. So the seed's inode stands, and the
                // next read still sees that rotation (review of #578); only
                // without one does the current file's (review of #564), since
                // dropping it would blind the next read to a later rotation.
                // Nor does a legacy marker name content: the seed's
                // fingerprint of that inode is kept, which can cost a
                // re-read, never a skip. The same holds for the bytes below
                // the position (#620): the tail is this batch's, at its end,
                // or the seed's of that inode for a marker that has none.
                let prior = c.seen.as_ref();
                let inode = (*marker_inode)
                    .or(prior.and_then(|seen| seen.inode))
                    .or(current_inode);
                let kept = prior.filter(|seen| seen.inode == inode);
                c.seen = Some(FileIdentity {
                    inode,
                    file_size: *marker_file_size,
                    mtime: String::new(),
                    content: marker_content
                        .clone()
                        .or_else(|| kept.and_then(|seen| seen.content.clone())),
                    tail: continuity::Tail::at(*end_byte, marker_tail.clone())
                        .or_else(|| kept.and_then(|seen| seen.tail.clone())),
                });
            }
            tracing::info!(path, end_byte, "file source progress committed");
        } else {
            tracing::warn!(
                path,
                end_byte,
                "committed batch belongs to a file that was rotated or truncated since it was read; \
                 the archive has it, the read position is left on the current file"
            );
        }

        if self.delete_after_commit {
            // Validate file identity before the irreversible delete: if the path
            // now points to a different inode than the batch's marker (rotation
            // / replacement), deleting would destroy an unrelated/newer file.
            //
            // The same goes for a file that was TRUNCATED since the batch was
            // read: the inode is unchanged and the old range ends beyond the
            // new length, so `end_byte >= size` below would hold for a stale
            // commit and delete the new, unarchived content (#558).
            //
            // And for a replacement that reuses the inode and is no shorter
            // (#578): only its first bytes differ from the batch's file.
            // And for a rewrite that kept the first page (#620): only the
            // bytes below the batch's end differ. One descriptor answers
            // both, and the metadata it carries is what the deferred delete
            // (#619) records as the file's state at this commit.
            let probed = continuity::probe_with_tail(
                Path::new(path),
                marker_tail.as_ref().map(|tail| (*end_byte, tail.len)),
            );
            let size = probed.as_ref().map_or(0, |(md, _, _)| md.len());
            // A file that can no longer be opened is not the one read either.
            let identity_ok = probed.as_ref().is_some_and(|(md, head, tail)| {
                delete_identity_ok(
                    *marker_inode,
                    marker_content.as_ref(),
                    marker_tail.as_ref(),
                    inode_of(md),
                    head,
                    tail,
                )
            });
            if !still_current {
                tracing::warn!(
                    path,
                    "skipping delete-after-commit: the file was rotated or truncated since this batch was read"
                );
            } else if !identity_ok {
                tracing::warn!(
                    path,
                    "skipping delete-after-commit: file identity changed (possible rotation/replacement)"
                );
            } else if *end_byte >= size {
                // Fully archived - but a file the engine has caught up with
                // may still be the one a writer is appending to, and deleting
                // it now sends every later line into an unlinked inode (#619).
                // It is deleted once it has stayed exactly like this for the
                // quiet period; until then it is kept, and read as usual.
                let candidate = DeleteCandidate {
                    inode: *marker_inode,
                    content: marker_content.clone(),
                    tail: marker_tail.clone(),
                    end_byte: *end_byte,
                    mtime: probed.as_ref().and_then(|(md, _, _)| md.modified().ok()),
                    since: Instant::now(),
                };
                self.cursors
                    .entry(path.clone())
                    .or_default()
                    .delete_candidate = Some(candidate);
                if !self.delete_quiet.is_zero() {
                    tracing::debug!(
                        path,
                        quiet_seconds = self.delete_quiet.as_secs(),
                        "fully archived file kept for now: a writer may still hold it open; it is \
                         deleted once it has stayed unwritten for the quiet period"
                    );
                }
                self.delete_if_quiet(path).await;
            }
        }
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::File {
            path,
            start_byte,
            end_byte,
            inode: marker_inode,
            file_size: size_when_read,
            content_fingerprint: marker_content,
            tail_fingerprint: marker_tail,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        // Rewind the read head to the *start* of the uncommitted range so the
        // data is reprocessed. Never moves the committed point forward.
        //
        // The range can only be re-read from the file it was read from. If a
        // rotation or truncation has replaced it since, there is nothing at
        // those offsets to re-read, and rewinding the NEW file there would
        // archive its middle as though it were the lost range. Refuse, so the
        // engine reports that the range will not be re-read (#558) - unless
        // the file it WAS read from can still be reached (#621, below).
        self.expire_rotated();
        let (current_inode, current_size, current_head, current_tail) =
            Self::probe_with_tail(Path::new(path), *end_byte, marker_tail.as_ref());
        let refusal = 'refusal: {
            if marker_inode
                .is_some_and(|marker| current_inode.is_some() && current_inode != Some(marker))
            {
                break 'refusal Some(VtopError::Source(format!(
                    "cannot replay file {path}: file identity changed (rotation/replacement)"
                )));
            }
            // The marker records how long the file was when the range was
            // read, and an append-only file only grows. A file now SHORTER
            // than that has been truncated, even when the range starts early
            // enough to fit in what is left: replaying from byte 0 would
            // archive replacement bytes as though they were the range that
            // was lost.
            if *start_byte > current_size || current_size < *size_when_read {
                break 'refusal Some(VtopError::Source(format!(
                    "cannot replay file {path} at byte {start_byte}: file length is \
                     {current_size} (truncated; it was {size_when_read} when this range was \
                     read)"
                )));
            }
            // The same inode and no shorter, but other first bytes: a
            // replacement that reused the inode (#578). Its offsets are not
            // the range's either.
            if let Some(content) = marker_content
                .as_ref()
                .filter(|content| !content.matches_head(&current_head))
            {
                break 'refusal Some(VtopError::Source(format!(
                    "cannot replay file {path}: its first {} bytes are not the ones this \
                     range was read after (replaced at the same inode)",
                    content.len
                )));
            }
            // The same first bytes, but other bytes below where the range
            // ends: rewritten in place, keeping the first page (#620).
            (!continuity::tail_is_current(marker_tail.as_ref(), &current_tail)).then(|| {
                VtopError::Source(format!(
                    "cannot replay file {path}: the bytes before byte {end_byte} are not the \
                     ones this range was read with (rewritten at the same inode)"
                ))
            })
        };
        if let Some(refusal) = refusal {
            // The file at the path is not the range's, but the range's file
            // may still be reachable - through a descriptor kept on it, or
            // under the name it was rotated to - and PROVEN to be the one the
            // range was read from (#621). Otherwise the refusal stands.
            if self.replay_rotated(marker) {
                tracing::warn!(
                    path,
                    start_byte,
                    "file rotated away since the range was read; the range is re-read from the \
                     file it was read from"
                );
                return Ok(());
            }
            return Err(refusal);
        }
        let c = self.cursors.entry(path.clone()).or_default();
        c.read_byte = (*start_byte).max(c.committed_byte);
        c.rewound_for = current_inode.map(|inode| (inode, c.read_byte));
        // These bytes will be handed out again, with fresh records.
        c.generations.reissue_from(c.read_byte);
        tracing::warn!(
            path,
            read_byte = c.read_byte,
            "file source rewound for replay"
        );
        Ok(())
    }

    /// Where the next read of this marker's file starts - but only when the
    /// cursor is positioned in THAT file. The cursor is per path, so after a
    /// rotation it describes the current file; reporting its position for an
    /// old inode's marker would let recovery treat the old range as handled
    /// (review of #563). An unknown identity on the cursor side answers `None`
    /// too: the caller then keeps the range's own start, the conservative
    /// answer.
    fn read_position(&self, marker: &ProgressMarker) -> Option<i128> {
        let ProgressMarker::File {
            path,
            inode: marker_inode,
            end_byte,
            content_fingerprint: marker_content,
            tail_fingerprint: marker_tail,
            ..
        } = marker
        else {
            return None;
        };
        // A range being re-read from a rotated file (#621) lands where that
        // re-read stands - for a marker of that file's content.
        if let Some(replay) = self
            .rotated
            .iter()
            .filter(|rotated| rotated.path == *path && Some(rotated.held.inode) == *marker_inode)
            .filter(|rotated| {
                marker_content
                    .as_ref()
                    .is_none_or(|marker| rotated.held.content.same_content(marker) != Some(false))
            })
            .find_map(|rotated| rotated.replay)
        {
            return Some(i128::from(replay.read_byte));
        }
        let c = self.cursors.get(path)?;
        let (identity, content) = match c.rewound_for {
            // A rewind checked the marker against the file it rewound.
            Some((inode, at)) if at == c.read_byte => (Some(inode), None),
            _ => (
                c.seen.as_ref().and_then(|seen| seen.inode),
                c.seen.as_ref().and_then(|seen| seen.content.as_ref()),
            ),
        };
        if marker_inode.is_some() && identity != *marker_inode {
            return None;
        }
        // The cursor read other content at that inode since (#578) - or
        // content its fingerprint cannot be compared with (a file under one
        // page fingerprints a different length as it grows): unverified is
        // answered as unknown, the conservative answer (review of #578).
        if content
            .zip(marker_content.as_ref())
            .is_some_and(|(seen, marker)| seen.same_content(marker) != Some(true))
        {
            return None;
        }
        // Nor is it this content when the bytes below the range's end are no
        // longer at the path (#620): a rewrite that kept the first page,
        // whose replay was refused for exactly that (review of #632). Read,
        // not compared with the cursor's own tail, which sits at another
        // offset; unreadable answers unknown.
        if let Some(tail) = marker_tail {
            let (_, _, _, below) = Self::probe_with_tail(Path::new(path), *end_byte, Some(tail));
            if !tail.matches_bytes(&below) {
                return None;
            }
        }
        Some(i128::from(c.read_byte))
    }

    /// The cursor's committed byte - only when the cursor is PROVEN to
    /// describe the marker's file (review of #567). Recovery retires a range
    /// as SUPERSEDED, a terminal state, on this answer, so "not proven to be
    /// another file" is not enough:
    ///
    /// - the marker and the cursor (the file it last read, or the restart
    ///   seed's) name the same inode - a legacy marker with none cannot be
    ///   told from any other file that lived at the path;
    /// - both carry a content fingerprint (#578), and they are the same
    ///   content: equal, or - fingerprints of different lengths, which say
    ///   nothing about each other - both match the first bytes of the file now
    ///   at the path, which still has that inode;
    /// - a marker's tail fingerprint (#620) matches the bytes now below its
    ///   end;
    /// - the file is no shorter than the cursor's position, the length the
    ///   cursor saw, or the length the marker's read saw: a truncation since
    ///   (a rewrite in place that kept the first page) makes the committed
    ///   byte another generation's (review of #567);
    /// - and the cursor's OWN tail, where it recorded one, matches the bytes
    ///   now below the cursor's position. Without it, a seed taken before an
    ///   in-place rewrite that kept the inode and the first page would lend
    ///   the old content's committed byte to a range of the new content
    ///   (review of #567).
    fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
        let ProgressMarker::File {
            path,
            inode: Some(marker_inode),
            end_byte,
            file_size: marker_size,
            content_fingerprint: Some(marker_content),
            tail_fingerprint: marker_tail,
            ..
        } = marker
        else {
            return None;
        };
        let c = self.cursors.get(path)?;
        let seen = c.seen.as_ref()?;
        if seen.inode != Some(*marker_inode) {
            return None;
        }
        let seen_content = seen.content.as_ref()?;
        if seen_content.same_content(marker_content) == Some(false) {
            return None;
        }
        // The one positive-identity test (`continuity::proves`), shared with
        // the restart seed's choice and the rotated-file re-read (#621):
        // - both first pages must match the file now at the path - equal
        //   fingerprints too, which say nothing about what the file holds NOW;
        // - an append-only file never gets shorter: one now shorter than the
        //   cursor's position, the length the cursor saw, the length the
        //   marker's read saw, or the range's end has been truncated since -
        //   a rewrite in place that can keep the inode and the first page -
        //   and the cursor's committed byte belongs to the content before it
        //   (review of #567, Codex);
        // - the marker's tail, and the cursor's own, must still be where they
        //   were taken.
        let marker_tail = continuity::Tail::at(*end_byte, marker_tail.clone());
        let tails: Vec<&continuity::Tail> = marker_tail.iter().chain(seen.tail.as_ref()).collect();
        let proven = continuity::proves_at_path(
            Path::new(path),
            &continuity::Identity {
                inode: *marker_inode,
                contents: &[seen_content, marker_content],
                tails: &tails,
                min_len: c
                    .committed_byte
                    .max(seen.file_size)
                    .max(*marker_size)
                    .max(*end_byte),
            },
        );
        proven.then(|| i128::from(c.committed_byte))
    }

    fn source_type(&self) -> SourceType {
        SourceType::File
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_log(lines: &[&str]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        f.flush().unwrap();
        f
    }

    fn src(path: &str) -> DiscoveredSource {
        DiscoveredSource {
            source_type: SourceType::File,
            source_name: path.to_string(),
            format: TelemetryFormat::Raw,
        }
    }

    /// An unchanged, fully-read file is not opened again (#100), and a file
    /// that starts changing again is picked up on the very NEXT cycle.
    ///
    /// The second half is the important one. A naive "back off sources that
    /// returned nothing" heuristic was tried for Kafka and reverted in #99: it
    /// shrank the poll window of a quiet source, which made the source less
    /// able to return data, which lengthened the quiet streak — a starvation
    /// trap measured at 48x. This skip cannot do that, because the stat is a
    /// reliable emptiness check rather than a probabilistic one and it runs at
    /// full strength every cycle. This test is that claim, executed.
    /// Two globs matching the same file discover it once (#376).
    ///
    /// Both discoveries carried the same `source_name`, so both jobs
    /// snapshotted the same starting offset and both outcomes routed into the
    /// same pending buffer — the records were read twice, acknowledged twice
    /// and archived twice, from a configuration nothing warns about and that
    /// the shipped example already resembles.
    ///
    /// The spellings here are not decoration. `glob` returns each pattern's
    /// match verbatim, so `logs/./app.log` and `logs/app.log` are two strings
    /// for one path, and a set over the rendered names lets the second
    /// through. Probed before this test was written rather than assumed.
    #[tokio::test]
    async fn one_file_spelled_two_ways_is_discovered_once() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("logs")).unwrap();
        std::fs::write(dir.path().join("logs/app.log"), "one\ntwo\n").unwrap();
        // Matched by only one pattern, so this test also pins that
        // deduplication does not swallow distinct sources.
        std::fs::write(dir.path().join("logs/other.log"), "three\n").unwrap();
        let base = dir.path().display().to_string();

        let source = FileSource::new(
            vec![format!("{base}/logs/*.log"), format!("{base}/logs/./app*")],
            TelemetryFormat::Raw,
            false,
        );

        let found: Vec<String> = source
            .discover_sources()
            .await
            .expect("a readable directory discovers")
            .into_iter()
            .map(|s| s.source_name)
            .collect();

        assert_eq!(
            found.len(),
            2,
            "one file spelled two ways is still ONE file — more than that means it is \
             read, acknowledged and archived once per spelling: {found:?}"
        );
        assert_eq!(
            found[0],
            format!("{base}/logs/app.log"),
            "and the surviving name must be the FIRST spelling matched, because it is the \
             cursor key: rewriting it would reset the cursor and re-archive the file"
        );
        assert!(
            found[1].ends_with("other.log"),
            "the file only one pattern matched must still be there: {found:?}"
        );
    }

    /// The name that survives is the one first matched, even when it is not
    /// the canonical spelling of the file (#376).
    ///
    /// This is the half that is easy to get wrong in the tempting direction:
    /// having canonicalised for the KEY, canonicalising the NAME as well looks
    /// tidier and is destructive. `source_name` is the cursor key in
    /// `self.cursors` and the identity in every log line, so rewriting it on
    /// an existing deployment resets that cursor, and the file is read from
    /// the beginning and archived again — which is the very defect this issue
    /// is about, reintroduced by its own fix.
    ///
    /// The non-canonical spelling is listed FIRST on purpose. With the
    /// canonical one first the assertion holds either way and the test proves
    /// nothing, which is how it was written the first time.
    #[tokio::test]
    async fn the_first_spelling_matched_is_the_name_that_survives() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("logs")).unwrap();
        std::fs::write(dir.path().join("logs/app.log"), "one\n").unwrap();
        let base = dir.path().display().to_string();

        let found: Vec<String> = FileSource::new(
            vec![format!("{base}/logs/./app*"), format!("{base}/logs/*.log")],
            TelemetryFormat::Raw,
            false,
        )
        .discover_sources()
        .await
        .expect("a readable directory discovers")
        .into_iter()
        .map(|s| s.source_name)
        .collect();

        assert_eq!(found.len(), 1, "still one file: {found:?}");
        assert_eq!(
            found[0],
            format!("{base}/logs/./app.log"),
            "the name must be the spelling that was matched first, not the canonical \
             form: it is the cursor key, and changing it re-reads and re-archives a file \
             the engine had already committed"
        );
    }

    /// A leading `./` is the spelling `PathBuf` does not normalise away, and
    /// is therefore the whole reason `without_dot_components` exists (#376).
    ///
    /// Tested directly rather than through discovery, because discovery runs
    /// on absolute paths from a temporary directory, where every `.` is
    /// interior and `PathBuf` equality alone would have passed. A helper whose
    /// only test is satisfied by not having the helper is not tested.
    #[test]
    fn a_leading_dot_is_the_spelling_path_equality_keeps() {
        use std::path::PathBuf;

        assert_eq!(
            PathBuf::from("a/./b.log"),
            PathBuf::from("a/b.log"),
            "an interior `.` is already ignored by PathBuf; this is the baseline the \
             helper is measured against"
        );
        assert_ne!(
            PathBuf::from("./a/b.log"),
            PathBuf::from("a/b.log"),
            "a LEADING `.` is not, which is the gap"
        );
        assert_eq!(
            without_dot_components(std::path::Path::new("./a/b.log")),
            without_dot_components(std::path::Path::new("a/b.log")),
            "and closing it is this function's entire job"
        );
        assert_eq!(
            without_dot_components(std::path::Path::new("a/../a/b.log")),
            PathBuf::from("a/../a/b.log"),
            "`..` is left exactly as it was: resolving it lexically is wrong through a \
             symlink, and merging two real sources loses data"
        );
    }

    /// A symlink and its target are TWO sources, and `..` does not merge —
    /// both on purpose (#376), and the first now by ruling (#378).
    ///
    /// This pins a deliberate non-behaviour, which is worth a test precisely
    /// because it looks like an oversight. Deduplicating by inode would merge
    /// them, and `canonicalize` makes it a one-liner — but it is not a
    /// spelling question, it is "are two directory entries for one inode one
    /// source or two", and a merge needs a cursor identity that survives
    /// `delete_after_commit` removing one of the aliases. There is no safe
    /// one: with the symlink deduplicated and deleted, the next discovery
    /// surfaces the target under a different `source_name`, finds no cursor,
    /// and re-reads the file from zero (#376 re-entered through its own fix);
    /// keyed by inode instead, a freed inode reused by a new file inherits
    /// the old cursor and skips that file's opening records. So: two sources,
    /// named once by the alias warning, and no knob.
    ///
    /// `..` is left for the same reason from the other side: `a/link/../b` is
    /// not `a/b` when `link` is a symlink, so collapsing it lexically could
    /// merge two REAL sources. A duplicate costs storage; a merge costs data.
    #[tokio::test]
    async fn aliases_are_not_merged_and_that_is_deliberate() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("logs")).unwrap();
        std::fs::write(dir.path().join("logs/real.log"), "one\n").unwrap();
        std::os::unix::fs::symlink(
            dir.path().join("logs/real.log"),
            dir.path().join("logs/link.log"),
        )
        .unwrap();
        let base = dir.path().display().to_string();

        let symlinked: Vec<String> = FileSource::new(
            vec![format!("{base}/logs/real*"), format!("{base}/logs/link*")],
            TelemetryFormat::Raw,
            false,
        )
        .discover_sources()
        .await
        .expect("a readable directory discovers")
        .into_iter()
        .map(|s| s.source_name)
        .collect();
        assert_eq!(
            symlinked.len(),
            2,
            "a symlink and its target are two directory entries and two sources by ruling \
             (#378): no cursor identity survives an alias being removed: {symlinked:?}"
        );

        let traversed: Vec<String> = FileSource::new(
            vec![
                format!("{base}/logs/real*"),
                format!("{base}/logs/../logs/real*"),
            ],
            TelemetryFormat::Raw,
            false,
        )
        .discover_sources()
        .await
        .expect("a readable directory discovers")
        .into_iter()
        .map(|s| s.source_name)
        .collect();
        assert_eq!(
            traversed.len(),
            2,
            "`..` is not collapsed: resolving it lexically is wrong through a symlink, \
             and merging two real sources loses data where a duplicate only costs \
             storage: {traversed:?}"
        );
    }

    /// The alias DETECTION for #378: two directory entries for one inode are
    /// two sources by ruling, and they must be named, because silent
    /// double-archiving is the wrong way to do even the right thing.
    #[test]
    fn two_entries_for_one_inode_are_grouped_and_distinct_files_are_not() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.log");
        std::fs::write(&real, "one\n").unwrap();
        let link = dir.path().join("link.log");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let hard = dir.path().join("hard.log");
        std::fs::hard_link(&real, &hard).unwrap();
        let other = dir.path().join("other.log");
        std::fs::write(&other, "two\n").unwrap();

        let entries: Vec<(String, Option<(u64, u64)>)> = [&real, &link, &hard, &other]
            .iter()
            .map(|p| {
                let md = std::fs::metadata(p).unwrap();
                (p.display().to_string(), dev_ino(&md))
            })
            .collect();

        let groups = alias_groups(&entries);
        assert_eq!(
            groups.len(),
            1,
            "the symlink, the hard link and the target are ONE file three \
             ways; the distinct file must not join them: {groups:?}"
        );
        assert_eq!(
            groups[0].1,
            vec![
                real.display().to_string(),
                link.display().to_string(),
                hard.display().to_string()
            ],
            "the group carries every spelling, in first-seen order, because \
             the warning is only useful if it names what to go look at"
        );
    }

    /// An entry whose identity is unknown must never alias with another
    /// unknown: a false alias warning teaches operators to ignore true ones.
    #[test]
    fn unknown_identities_never_group() {
        let entries: Vec<(String, Option<(u64, u64)>)> = vec![
            ("a.log".into(), None),
            ("b.log".into(), None),
            ("c.log".into(), Some((1, 42))),
        ];
        assert!(
            alias_groups(&entries).is_empty(),
            "two unknowns are not evidence of one file, and one known entry \
             alone has nothing to alias with"
        );
    }

    /// REGRESSION (#388's own warning): the suppression was keyed on bare
    /// (device, inode), so a group that gained a hard link in a later cycle
    /// stayed suppressed — the one path an operator most needed to go look
    /// at was exactly the one never named.
    #[test]
    fn a_link_added_to_an_already_warned_group_warns_again_naming_the_new_path() {
        let mut warned = HashMap::new();
        let two = vec![
            ("real.log".to_string(), Some((1, 42))),
            ("link.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&two), &two),
            vec![vec!["real.log".to_string(), "link.log".to_string()]],
            "the first sighting of a group warns, naming every spelling"
        );

        let three = vec![
            ("real.log".to_string(), Some((1, 42))),
            ("link.log".to_string(), Some((1, 42))),
            ("hard.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&three), &three),
            vec![vec![
                "real.log".to_string(),
                "link.log".to_string(),
                "hard.log".to_string()
            ]],
            "a group that gained a spelling must warn again and name ALL of it: the old \
             line never said hard.log, and a warning that does not name what to go look \
             at has stopped being useful"
        );
    }

    /// The half #388 already promised, re-pinned across the rekeying: the
    /// duplication is either intended or a config surprise, and both deserve
    /// exactly one line per process, not one per discovery cycle.
    #[test]
    fn an_unchanged_persistent_alias_group_still_warns_exactly_once() {
        let mut warned = HashMap::new();
        let identities = vec![
            ("real.log".to_string(), Some((1, 42))),
            ("link.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&identities), &identities).len(),
            1,
            "the first sighting warns"
        );
        for _ in 0..3 {
            assert!(
                alias_groups_to_warn(&mut warned, alias_groups(&identities), &identities)
                    .is_empty(),
                "an unchanged persistent link is one line per process: a warning that \
                 repeats every cycle trains operators to stop reading it"
            );
        }

        // Same spellings, different first-seen order — a comparison over the
        // ordered group would call this a change, which is why the stored
        // composition is a set.
        let reordered = vec![
            ("link.log".to_string(), Some((1, 42))),
            ("real.log".to_string(), Some((1, 42))),
        ];
        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&reordered), &reordered).is_empty(),
            "an ordering difference between passes is not a new spelling and must not \
             masquerade as one"
        );
    }

    /// REGRESSION (#388's own warning): once `delete_after_commit` freed an
    /// inode, an unrelated new file reusing the number inherited the dead
    /// group's suppression forever — and inode reuse after unlink is
    /// immediate on Linux (#132 CI).
    #[test]
    fn an_inode_reused_by_a_new_group_is_not_suppressed_by_its_dead_predecessor() {
        let mut warned = HashMap::new();
        let old = vec![
            ("old.log".to_string(), Some((1, 42))),
            ("old-link.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&old), &old).len(),
            1,
            "the doomed group warns while it lives"
        );

        // delete_after_commit removed every spelling of the group, while an
        // unrelated file keeps the pass non-empty: a pass that saw something
        // and not this identity must evict its entry. (A pass that saw
        // NOTHING proves nothing — that case is pinned separately below.)
        let gone = vec![("other.log".to_string(), Some((1, 99)))];
        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&gone), &gone).is_empty(),
            "a pass with no alias groups warns about nothing"
        );
        assert!(
            warned.is_empty(),
            "a dead group must not squat in the map: whatever reuses its inode is a \
             different file, and the map would otherwise grow with every \
             delete-and-recreate cycle"
        );

        let new = vec![
            ("new.log".to_string(), Some((1, 42))),
            ("new-link.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&new), &new),
            vec![vec!["new.log".to_string(), "new-link.log".to_string()]],
            "an inode number is not an identity across a delete: this group has never \
             been named, and inheriting the predecessor's suppression would silence it \
             forever"
        );
    }

    /// One spelling failing a stat for one pass (raced rotation, transient
    /// EACCES) must not turn into warning noise when it returns. The retain
    /// runs over the FULL identities list, not just the grouped entries, so
    /// the surviving spelling keeps the entry alive.
    #[test]
    fn a_transiently_missing_spelling_does_not_reset_the_warning() {
        let mut warned = HashMap::new();
        let full = vec![
            ("real.log".to_string(), Some((1, 42))),
            ("link.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&full), &full).len(),
            1,
            "the first sighting warns"
        );

        let one_missing = vec![("real.log".to_string(), Some((1, 42)))];
        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&one_missing), &one_missing).is_empty(),
            "a single surviving spelling is not an alias group"
        );

        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&full), &full).is_empty(),
            "the returned spelling re-forms the exact composition the one line already \
             named: re-warning here would make every transient stat failure a source of \
             noise, which is how warnings get ignored"
        );

        // The same blip on a THREE-spelling group: the survivors still form
        // a group, so a stored-set equality check would warn twice — once
        // when the spelling blinked out, again when it returned. Growth is
        // the only trigger, so the blip costs zero lines.
        let trio = vec![
            ("a.log".to_string(), Some((1, 7))),
            ("b.log".to_string(), Some((1, 7))),
            ("c.log".to_string(), Some((1, 7))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&trio), &trio).len(),
            1,
            "the first sighting of the trio warns"
        );
        let blip = vec![
            ("a.log".to_string(), Some((1, 7))),
            ("c.log".to_string(), Some((1, 7))),
        ];
        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&blip), &blip).is_empty(),
            "two survivors of a three-spelling group are still a group, but every \
             spelling was already named: a shrink is not new information"
        );
        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&trio), &trio).is_empty(),
            "the restored spelling was named by the original line: one stat blip must \
             cost zero warning lines, not two"
        );
    }

    /// An unmounted directory or a burst of stat failures makes one pass
    /// discover nothing, and that pass looks exactly like a mass delete.
    /// Forgetting on it would turn recovery into a re-warn of every group
    /// the process already named.
    #[test]
    fn an_empty_discovery_pass_does_not_forget_what_was_warned() {
        let mut warned = HashMap::new();
        let group = vec![
            ("real.log".to_string(), Some((1, 42))),
            ("link.log".to_string(), Some((1, 42))),
        ];
        assert_eq!(
            alias_groups_to_warn(&mut warned, alias_groups(&group), &group).len(),
            1,
            "the first sighting warns"
        );

        let outage: Vec<(String, Option<(u64, u64)>)> = Vec::new();
        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&outage), &outage).is_empty(),
            "a pass that saw nothing warns about nothing"
        );
        assert!(
            !warned.is_empty(),
            "a pass that discovered nothing proves nothing: eviction on it would make \
             every outage end in a burst of re-warns for groups already named"
        );

        assert!(
            alias_groups_to_warn(&mut warned, alias_groups(&group), &group).is_empty(),
            "the group that rode out the outage was already named: recovery must not \
             repeat it"
        );
    }

    #[tokio::test]
    async fn an_unchanged_file_is_skipped_and_a_changed_one_is_read_at_once() {
        let f = write_log(&["a", "b"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let first = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(first[0].records.len(), 2, "the first cycle reads the file");

        // Nothing has changed, and the cursor is at EOF: the second cycle must
        // decide that without opening the file.
        assert!(
            FileSource::can_skip(
                fs.cursors.get(&path).unwrap(),
                &FileSource::stat_identity(&path).unwrap()
            ),
            "an unchanged, fully-read file is exactly the case worth skipping"
        );
        let second = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            second[0].records.is_empty(),
            "a skipped cycle yields nothing, like the read it stands in for"
        );

        // Append, and the skip must stop applying immediately — no streak to
        // work off, no window to widen back.
        {
            use std::io::Write as _;
            let mut fh = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(fh, "c").unwrap();
            fh.sync_all().unwrap();
        }
        assert!(
            !FileSource::can_skip(
                fs.cursors.get(&path).unwrap(),
                &FileSource::stat_identity(&path).unwrap()
            ),
            "an appended file must never be skippable: this is the anti-starvation \
             property the Kafka backoff in #99 could not offer"
        );
        let third = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            third[0].records,
            vec![b"c".to_vec()],
            "the appended record is read on the very next cycle, not after a backoff"
        );
    }

    /// A file whose mtime the filesystem cannot report is never skipped
    /// (#411): the identity records the empty string there, and letting ""
    /// match "" would skip an in-place rewrite that preserved inode and
    /// size — indefinitely, on exactly the filesystems that give the check
    /// the least to work with. Unknown must fall through to a real read.
    #[test]
    fn a_file_whose_mtime_cannot_be_read_is_never_skipped() {
        let identity = FileIdentity {
            inode: Some(42),
            file_size: 7,
            mtime: String::new(),
            content: None,
            tail: None,
        };
        let cursor = FileCursor {
            read_byte: 7,
            committed_byte: 7,
            seen: Some(identity.clone()),
            ..FileCursor::default()
        };
        assert!(
            !FileSource::can_skip(&cursor, &identity),
            "inode and size agree and the cursor is caught up, but an unknown \
             mtime is not evidence the content is unchanged"
        );
    }

    /// A file this cursor has never observed is read, never skipped. Skipping
    /// on an assumption would lose a file's entire contents the first time it
    /// appeared already-quiet — which is exactly what rotated history looks
    /// like when the engine starts.
    #[tokio::test]
    async fn a_file_never_seen_before_is_never_skipped() {
        let f = write_log(&["x"]);
        let path = f.path().to_string_lossy().into_owned();
        let cursor = FileCursor::default();
        assert!(
            !FileSource::can_skip(&cursor, &FileSource::stat_identity(&path).unwrap()),
            "no prior observation means no basis to skip"
        );
    }

    #[tokio::test]
    async fn reads_lines_and_tracks_offset() {
        let f = write_log(&["a", "b", "c"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let reads = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // A file is a single committable unit, so the Vec is always length 1;
        // assert it rather than indexing blind, so a regression that returns 0
        // or 2 fails here instead of panicking on the index.
        assert_eq!(reads.len(), 1);
        let r = &reads[0];
        assert_eq!(r.records.len(), 3);
        assert_eq!(r.records[0], b"a");
        // end marker byte offset is past the data.
        if let ProgressMarker::File { end_byte, .. } = &r.progress_end {
            assert_eq!(*end_byte, 6); // "a\nb\nc\n"
        } else {
            panic!("expected file marker");
        }
    }

    #[tokio::test]
    async fn a_commit_is_skipped_only_below_a_point_confirmed_for_its_own_file() {
        // #624 (review): the skip is bound to the file whose commit confirmed
        // the committed point, not to the number alone. A point confirmed for
        // another file - a previous one at this path, say - says nothing
        // about this marker's range, which commits as before.
        let f = write_log(&["one", "two"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap()
            .remove(0);
        fs.commit_progress(&read.progress_end).await.unwrap();
        let end = fs.committed_through(&read.progress_end).unwrap();
        // The point is now another file's, and higher.
        {
            let c = fs.cursors.get_mut(&path).unwrap();
            c.committed_byte = 1_000;
            let other = c.confirmed_for.as_mut().expect("the commit confirmed it");
            other.inode += 1;
        }
        fs.commit_progress(&read.progress_end).await.unwrap();
        assert_eq!(
            fs.cursors[&path].committed_byte as i128, end,
            "a commit of this file was skipped below another file's committed point"
        );
    }

    #[tokio::test]
    async fn a_commit_below_this_processs_committed_point_moves_nothing() {
        // #624: a VERIFIED range whose source commit failed (or was held
        // back) is committed later, after a newer range of the same file has
        // committed past it. Moving the cursor to its end would put the
        // committed point BACK and, for a range with no record of its
        // generation, the read head too - the newer range read again.
        let f = write_log(&["one", "two", "three"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = fs
            .read_batch_candidates(&src(&path), 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap()
            .remove(0);
        let rest = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap()
            .remove(0);
        fs.commit_progress(&first.progress_end).await.unwrap();
        fs.commit_progress(&rest.progress_end).await.unwrap();
        let end = fs.committed_through(&rest.progress_end).unwrap();
        // Late, and twice: once while its generation record is gone (as for
        // a row from before a restart), once more.
        for _ in 0..2 {
            fs.commit_progress(&first.progress_end).await.unwrap();
            assert_eq!(
                fs.committed_through(&rest.progress_end),
                Some(end),
                "a superseded commit moved the committed point back"
            );
            let again = fs
                .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            assert!(
                again.iter().all(|r| r.records.is_empty()),
                "a superseded commit moved the read head back: {again:?}"
            );
        }
    }

    #[tokio::test]
    async fn resumes_from_committed_byte() {
        let f = write_log(&["one", "two", "three"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let reads1 = fs
            .read_batch_candidates(&src(&path), 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // One file == one committable unit; see `reads_lines_and_tracks_offset`.
        assert_eq!(reads1.len(), 1);
        let r1 = &reads1[0];
        assert_eq!(r1.records, vec![b"one".to_vec()]);

        // Commit only the first record.
        fs.commit_progress(&r1.progress_end).await.unwrap();

        // Next read resumes after "one\n".
        let reads2 = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads2.len(), 1);
        assert_eq!(reads2[0].records, vec![b"two".to_vec(), b"three".to_vec()]);
    }

    /// #567 (review, Codex): a file rewritten in place with the same first
    /// page but SHORTER agrees with the inode and with fingerprints capped at
    /// one page, and - with no tail recorded - nothing else compared it, so
    /// the old content's committed byte was lent to the new content's range.
    /// The lengths show the truncation: the file is now shorter than the
    /// cursor's position and than the length the cursor saw.
    #[cfg(unix)]
    #[test]
    fn a_truncated_file_with_the_same_first_page_lends_no_committed_point() {
        use std::os::unix::fs::MetadataExt;
        let page: Vec<u8> = (0..4096u32).map(|i| b'a' + (i % 26) as u8).collect();
        let mut bytes = page.clone();
        bytes.extend(std::iter::repeat_n(b'n', 5000 - 4096));
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&bytes).unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let first_page = ContentFingerprint::of(&page);
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        // The old content was 10000 bytes long and committed through 10000.
        fs.seed_committed_from(
            &path,
            10_000,
            Some(inode),
            Some(10_000),
            first_page.clone(),
            None,
        );
        let replacement = ProgressMarker::File {
            path: path.clone(),
            inode: Some(inode),
            start_byte: 4_000,
            end_byte: 5_000,
            file_size: 5_000,
            mtime: String::new(),
            content_fingerprint: first_page,
            tail_fingerprint: None,
        };
        assert_eq!(
            fs.committed_through(&replacement),
            None,
            "the old generation's committed offset was lent to a truncated file's range"
        );
    }

    /// #567 (review): recovery retires a handed-back range as SUPERSEDED on
    /// this answer, and SUPERSEDED is terminal - so the committed point is
    /// answered only when the cursor POSITIVELY describes the marker's file:
    /// the same inode, and content shown to be the same. A marker with no inode
    /// (a legacy row), or with no content fingerprint, cannot show it, and gets
    /// no answer; fingerprints of different lengths are settled against the
    /// file at the path, and so is the tail (#620). Answered, it is the
    /// committed byte, not the read head.
    #[cfg(unix)]
    #[test]
    fn the_committed_point_is_answered_only_for_a_marker_proven_to_be_the_cursors_file() {
        use std::os::unix::fs::MetadataExt;
        let bytes: Vec<u8> = (0..100u8).map(|i| b'a' + i % 26).collect();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&bytes).unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let fp = |b: &[u8]| ContentFingerprint::of(b);
        let marker = |inode: Option<u64>,
                      content: Option<ContentFingerprint>,
                      tail: Option<ContentFingerprint>| ProgressMarker::File {
            path: path.clone(),
            inode,
            start_byte: 0,
            end_byte: 30,
            file_size: 100,
            mtime: String::new(),
            content_fingerprint: content,
            tail_fingerprint: tail,
        };
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let whole = fp(&bytes);
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole.clone(), None)),
            None
        );
        fs.seed_committed_from(&path, 40, Some(inode), Some(100), whole.clone(), None);
        fs.cursors.get_mut(&path).unwrap().read_byte = 55;

        // Proven: the same fingerprint, or a shorter one of the same file's
        // first bytes, settled against the file at the path.
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole.clone(), None)),
            Some(40)
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), fp(&bytes[..50]), None)),
            Some(40)
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole.clone(), fp(&bytes[..30]))),
            Some(40),
            "a tail that matches the bytes below the range's end"
        );
        // Not proven.
        assert_eq!(
            fs.committed_through(&marker(None, whole.clone(), None)),
            None,
            "no inode"
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), None, None)),
            None,
            "no content"
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode + 1), whole.clone(), None)),
            None,
            "another inode"
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), fp(&[b'z'; 100]), None)),
            None,
            "other content of the same length"
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), fp(&[b'z'; 50]), None)),
            None,
            "other content of another length: the file's head says so"
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole.clone(), fp(&[b'z'; 30]))),
            None,
            "the bytes below the range's end are not the ones it was read with"
        );
        // The cursor's own position must still be in this content (review
        // of #567): a seed whose tail - the bytes below its committed byte -
        // is no longer what the file holds there describes content that was
        // rewritten in place, keeping the inode and the first page, and its
        // committed byte is another generation's.
        fs.seed_committed_from(
            &path,
            40,
            Some(inode),
            Some(100),
            whole.clone(),
            fp(&[b'z'; 40]),
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole.clone(), None)),
            None,
            "the seed's tail is not the file's: its committed byte is another generation's"
        );
        fs.seed_committed_from(
            &path,
            40,
            Some(inode),
            Some(100),
            whole.clone(),
            fp(&bytes[..40]),
        );
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole.clone(), None)),
            Some(40),
            "the seed's tail is the file's"
        );
        // A cursor that recorded no content proves nothing either.
        fs.seed_committed_from(&path, 40, Some(inode), Some(100), None, None);
        assert_eq!(
            fs.committed_through(&marker(Some(inode), whole, None)),
            None
        );
    }

    /// The committed point and the restart seed's choice (#621) share one
    /// positive-identity test, which keeps the stricter rule of each: equal
    /// first-page fingerprints in the marker and the cursor say they read the
    /// same content, not that the file at the path STILL holds it. A file
    /// rewritten in place - the same inode, longer, other first bytes - with
    /// no tails recorded passed every other check and lent the old content's
    /// committed byte to a range of it, retiring that range for good.
    #[cfg(unix)]
    #[test]
    fn equal_fingerprints_still_have_to_match_the_file_now_at_the_path() {
        use std::os::unix::fs::MetadataExt;
        let old: Vec<u8> = (0..100u8).map(|i| b'a' + i % 26).collect();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&old).unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let content = ContentFingerprint::of(&old);
        let marker = ProgressMarker::File {
            path: path.clone(),
            inode: Some(inode),
            start_byte: 0,
            end_byte: 30,
            file_size: 100,
            mtime: String::new(),
            content_fingerprint: content.clone(),
            tail_fingerprint: None,
        };
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        fs.seed_committed_from(&path, 40, Some(inode), Some(100), content, None);
        assert_eq!(fs.committed_through(&marker), Some(40), "the file read");

        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(&[b'Z'; 120])
            .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(
            fs.committed_through(&marker),
            None,
            "the file at the path no longer starts with what both recorded"
        );
    }

    /// The batch rewind returns one result per marker, in order, and one that
    /// cannot be applied does not hide or reorder the others.
    #[tokio::test]
    async fn a_batch_rewind_answers_each_marker_in_order() {
        let f = write_log(&["one", "two", "three"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let good = read[0].progress_start.clone();
        let wrong_kind = ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: 0,
            end_offset: 0,
            consumer_group: "g".into(),
        };
        let results = fs
            .replay_from_markers(&[good.clone(), wrong_kind, good])
            .await;
        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(
            results[1].is_err(),
            "a marker of another source type is refused"
        );
        assert!(results[2].is_ok(), "and does not stop the one after it");
    }

    #[tokio::test]
    async fn replay_rewinds_uncommitted_read() {
        let f = write_log(&["x", "y", "z"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let reads1 = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // One file == one committable unit; see `reads_lines_and_tracks_offset`.
        assert_eq!(reads1.len(), 1);
        assert_eq!(reads1[0].records.len(), 3);
        // No commit. Simulate crash + replay from start of range.
        fs.replay_from_marker(&reads1[0].progress_start)
            .await
            .unwrap();
        let reads2 = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads2.len(), 1);
        assert_eq!(
            reads2[0].records.len(),
            3,
            "uncommitted data must be replayable"
        );
    }

    #[tokio::test]
    async fn ignores_partial_trailing_line() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "complete\npartial-no-newline").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let reads = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // One file == one committable unit; see `reads_lines_and_tracks_offset`.
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].records, vec![b"complete".to_vec()]);
    }

    #[tokio::test]
    async fn rejects_a_line_before_it_can_exceed_the_memory_budget() {
        let f = write_log(&["12345678"]); // nine bytes including newline
        let path = f.path().to_string_lossy().into_owned();
        let mut source = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let error = source
            .read_batch_candidates(&src(&path), 10, 8, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds max_bytes=8"));
        assert_eq!(
            source.cursors[&path].read_byte, 0,
            "oversized data is not skipped"
        );
    }

    #[tokio::test]
    async fn whole_file_mode_refuses_an_over_budget_record() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"0123456789").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let mut source =
            FileSource::with_mode(vec![path.clone()], TelemetryFormat::Raw, false, true);

        let error = source
            .read_batch_candidates(&src(&path), 10, 8, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("whole-file record"));
        assert_eq!(
            source.cursors[&path].read_byte, 0,
            "oversized data is not skipped"
        );
    }

    /// #65: the marker's identity must describe the file whose BYTES were
    /// read (fstat on the open descriptor), so that after a rotation the
    /// recorded inode disagrees with the replacement and the delete guard
    /// protects it. A path-stat at marker-build time could fingerprint the
    /// replacement instead.
    #[cfg(unix)]
    #[tokio::test]
    async fn marker_identity_comes_from_the_file_actually_read() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rotating.log");
        std::fs::write(&path, "old-1\nold-2\n").unwrap();
        let read_inode = std::fs::metadata(&path).unwrap().ino();
        let spath = path.to_string_lossy().into_owned();

        // No quiet period (#619), or the wait alone would keep the file and
        // the identity guard under test would never be reached.
        let mut fs = FileSource::new(vec![spath.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let reads = fs
            .read_batch_candidates(&src(&spath), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let ProgressMarker::File { inode, .. } = &reads[0].progress_end else {
            panic!("expected file marker");
        };
        assert_eq!(
            *inode,
            Some(read_inode),
            "marker carries the READ file's inode"
        );

        // Rotate atomically with a replacement that exists at the same time as
        // the original. Unlink-then-create can immediately reuse the freed
        // inode on Linux, making a real replacement look identical (#132 CI).
        let replacement = dir.path().join("replacement.log");
        std::fs::write(&replacement, "new-1\nnew-2\n").unwrap();
        let replacement_inode = std::fs::metadata(&replacement).unwrap().ino();
        assert_ne!(replacement_inode, read_inode);
        std::fs::rename(&replacement, &path).unwrap();

        // Committing the OLD read with delete_after_commit=true must not
        // delete the replacement: the recorded identity disagrees with what
        // the path now names.
        fs.commit_progress(&reads[0].progress_end).await.unwrap();
        assert!(
            path.exists(),
            "rotation replacement must survive delete_after_commit"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new-1\nnew-2\n");
    }

    #[tokio::test]
    async fn delete_after_commit_skips_on_identity_mismatch() {
        let f = write_log(&["a"]);
        let path = f.path().to_string_lossy().into_owned();
        // No quiet period (#619), so only the identity guard can keep it.
        let mut fs = FileSource::with_mode(vec![path.clone()], TelemetryFormat::Raw, true, false)
            .with_delete_quiet_period(Duration::ZERO);
        // Marker carries a stale inode (as if the file rotated since the read);
        // committing must NOT delete the now-different file on disk.
        let marker = ProgressMarker::File {
            path: path.clone(),
            inode: Some(u64::MAX),
            start_byte: 0,
            end_byte: 2,
            file_size: 2,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        fs.commit_progress(&marker).await.unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "must not delete when the file identity no longer matches the marker"
        );
    }

    /// #96 B2: one pass reads MANY files concurrently. Every file's records
    /// must land in its own outcome with its own marker, cursors must advance
    /// per file, and a missing file must fail only its own outcome.
    #[tokio::test]
    async fn read_all_reads_many_files_concurrently_with_isolated_outcomes() {
        let f1 = write_log(&["one-a", "one-b"]);
        let f2 = write_log(&["two-a"]);
        let p1 = f1.path().to_string_lossy().into_owned();
        let p2 = f2.path().to_string_lossy().into_owned();
        let missing = format!("{p1}.does-not-exist");
        let mut fs = FileSource::new(vec![p1.clone(), p2.clone()], TelemetryFormat::Raw, false);

        let sources = vec![src(&p1), src(&p2), src(&missing)];
        let report = fs
            .read_all_batch_candidates(&sources, 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();

        assert_eq!(report.outcomes.len(), 3);
        // Outcomes come back in source order regardless of I/O completion order.
        let r1 = report.outcomes[0].result.as_ref().unwrap();
        assert_eq!(r1[0].records, vec![b"one-a".to_vec(), b"one-b".to_vec()]);
        let ProgressMarker::File { path, .. } = &r1[0].progress_end else {
            panic!("expected file marker")
        };
        assert_eq!(path, &p1, "marker names the outcome's own file");
        let r2 = report.outcomes[1].result.as_ref().unwrap();
        assert_eq!(r2[0].records, vec![b"two-a".to_vec()]);
        // The missing file fails ITS outcome only; the others are unaffected.
        assert!(report.outcomes[2].result.is_err());
        // Any data => the shared wall-clock bucket is productive.
        assert_eq!(report.empty_ms, 0);
        assert_eq!(report.failed_ms, 0);

        // Cursors advanced: a second pass over the good files reads nothing new.
        let report2 = fs
            .read_all_batch_candidates(&sources[..2], 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        for o in &report2.outcomes {
            let reads = o.result.as_ref().unwrap();
            assert!(
                reads[0].records.is_empty(),
                "no re-read after cursor advance"
            );
        }
    }

    // ---- rotation and truncation (#558) -------------------------------------

    /// `logrotate` renames the file and starts a new one at the same path. The
    /// cursor's position belongs to the OLD file; it must not be applied to the
    /// new one. The new file is deliberately LONGER than the old read position,
    /// the case that used to lose data silently: the read resumed in the middle
    /// of it and everything before that byte was never archived.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_file_is_read_from_byte_zero_even_when_it_outgrows_the_old_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\n").unwrap(); // 8 bytes
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let first = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&first[0].progress_end).await.unwrap();
        assert_eq!(first[0].records.len(), 2);

        // Rotate: the old file keeps its inode under another name.
        std::fs::rename(&path, format!("{path}.1")).unwrap();
        let fresh: String = (0..50).map(|i| format!("new-{i}\n")).collect();
        assert!(fresh.len() > 8, "longer than the old read position");
        std::fs::write(&path, &fresh).unwrap();

        let after = fs
            .read_batch_candidates(&src(&path), 1000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            after[0].records.len(),
            50,
            "every line of the new file, not the tail after byte 8"
        );
        assert_eq!(after[0].records[0], b"new-0".to_vec());
        let ProgressMarker::File { start_byte, .. } = after[0].progress_start else {
            panic!("a file marker");
        };
        assert_eq!(
            start_byte, 0,
            "the marker describes the bytes actually read"
        );
    }

    /// `copytruncate` and an in-place truncate keep the inode: only the length
    /// betrays them.
    #[tokio::test]
    async fn a_truncated_file_is_read_from_byte_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap(); // 12 bytes
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&first[0].progress_end).await.unwrap();

        // Truncate in place (same inode) and write something shorter.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"zzz\n")
            .unwrap();

        let after = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(after[0].records, vec![b"zzz".to_vec()]);
        let ProgressMarker::File { start_byte, .. } = after[0].progress_start else {
            panic!("a file marker");
        };
        assert_eq!(start_byte, 0);
    }

    /// A batch read BEFORE the truncation commits after the cursor has already
    /// restarted. Its offset (12) means nothing in the new file (4 bytes); if the
    /// commit moved the cursor there, the next appends to the new file would
    /// be skipped.
    #[tokio::test]
    async fn a_commit_from_before_a_truncation_does_not_move_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let old = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = old[0].progress_end.clone();

        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"zzz\n")
            .unwrap();
        let fresh = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(fresh[0].records, vec![b"zzz".to_vec()]);

        // The pre-truncation batch verifies and commits late.
        fs.commit_progress(&old_end).await.unwrap();
        assert_eq!(
            fs.cursors[&path].read_byte, 4,
            "still at the end of the new file"
        );

        // New appends are read, not skipped.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"yyy\n")
            .unwrap();
        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(next[0].records, vec![b"yyy".to_vec()]);
    }

    /// A range can only be re-read from the file it was read from.
    #[cfg(unix)]
    #[tokio::test]
    async fn replay_refuses_a_range_from_a_file_that_was_rotated_or_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let start = read[0].progress_start.clone();

        // Intact: the rewind is accepted.
        fs.replay_from_marker(&start).await.unwrap();

        // Truncated below the range start's neighbourhood.
        let ProgressMarker::File {
            path: p,
            inode,
            file_size,
            mtime,
            ..
        } = start.clone()
        else {
            panic!("a file marker");
        };
        let beyond = ProgressMarker::File {
            path: p.clone(),
            inode,
            start_byte: 12,
            end_byte: 12,
            file_size,
            mtime: mtime.clone(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        std::fs::write(&path, "z\n").unwrap();
        let err = fs
            .replay_from_marker(&beyond)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("truncated"), "{err}");

        // Rotated: a different file at the same path.
        std::fs::rename(&path, format!("{path}.1")).unwrap();
        std::fs::write(&path, "fresh content here\n").unwrap();
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("identity changed"), "{err}");
    }

    /// A bounded read of a backlog leaves the cursor far behind the end of the
    /// file. A truncation that then leaves the file longer than the read
    /// position but shorter than it was passed a read-position-only test, and
    /// the read resumed in the middle of the new content.
    #[tokio::test]
    async fn a_truncation_to_above_the_read_position_is_still_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let backlog: String = (0..10).map(|i| format!("old-{i}\n")).collect(); // 60 bytes
        std::fs::write(&path, &backlog).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        // Two records only: the cursor stops at byte 12 of 60.
        let first = fs
            .read_batch_candidates(&src(&path), 2, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(first[0].records.len(), 2);

        // Truncated in place and rewritten to 30 bytes: above the read position
        // (12), below the length last seen (60).
        let fresh: String = (0..5).map(|i| format!("new-{i}\n")).collect(); // 30 bytes
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(fresh.as_bytes())
            .unwrap();

        let after = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            after[0].records.len(),
            5,
            "all of the new content, not the tail after byte 12"
        );
        assert_eq!(after[0].records[0], b"new-0".to_vec());
    }

    /// A `copytruncate`d file that has REGROWN past an old range's end looks,
    /// by inode and length, exactly as it did when that range was read. The
    /// batch read before the truncation then finishes uploading, and a commit
    /// judged by the file as it is now carries the restarted cursor to the old
    /// offset, skipping the bytes in between (review of #564). The record of
    /// which generation each range was read in catches it.
    #[tokio::test]
    async fn a_pre_truncation_commit_cannot_move_the_cursor_after_the_file_regrows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let old: String = (0..10).map(|i| format!("old-{i}\n")).collect(); // 60 bytes
        std::fs::write(&path, &old).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = first[0].progress_end.clone();

        // Truncated in place and rewritten: the cursor restarts at byte 0.
        let fresh: String = (0..5).map(|i| format!("new-{i}\n")).collect(); // 30 bytes
        std::fs::write(&path, &fresh).unwrap();
        let second = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(second[0].records.len(), 5);

        // The file regrows to MORE than the old range's end before the old
        // batch's commit lands: by inode and length it now looks current.
        let more: String = (5..12).map(|i| format!("new-{i}\n")).collect(); // +42 bytes
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(more.as_bytes())
            .unwrap();
        fs.commit_progress(&old_end).await.unwrap();

        let third = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            third[0].records.len(),
            7,
            "the bytes appended since the restart are read; the stale commit \
             must not have moved the cursor to byte 60 and skipped them"
        );
        assert_eq!(third[0].records[0], b"new-5".to_vec());
    }

    /// Review of #564: a read that finds the file truncated and then FAILS (the
    /// new content's first line is over `max_bytes`) used to drop what it
    /// found. The cursor kept the old position and the old observed length;
    /// once the file regrew past that length the next read saw no truncation
    /// at all and resumed at the old offset, and a pre-truncation commit
    /// moved the cursor there too.
    #[tokio::test]
    async fn a_truncation_found_by_a_read_that_then_fails_still_restarts_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let old: String = (0..10).map(|i| format!("old-{i}\n")).collect(); // 60 bytes
        std::fs::write(&path, &old).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = first[0].progress_end.clone();

        // Truncated in place; the new content's first line is over budget.
        std::fs::write(&path, format!("{}\n", "x".repeat(30))).unwrap();
        assert!(fs
            .read_batch_candidates(&src(&path), 100, 20, Duration::ZERO)
            .await
            .is_err());

        // Rewritten again, now longer than the old content (84 > 60), and
        // the batch read before the truncation commits.
        let fresh: String = (0..12).map(|i| format!("new-{i:02}\n")).collect();
        std::fs::write(&path, &fresh).unwrap();
        fs.commit_progress(&old_end).await.unwrap();

        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            next[0].records.len(),
            12,
            "the new content is read from byte 0, not from the old offset 60"
        );
        assert_eq!(next[0].records[0], b"new-00".to_vec());
    }

    /// `delete_after_commit` removes a source file once a batch reaching its
    /// end commits. A batch read BEFORE a truncation commits afterwards, with
    /// an end beyond the new length: the inode is unchanged and `end >= size`
    /// holds, so the stale commit deleted the new, unarchived content.
    #[tokio::test]
    async fn a_stale_commit_does_not_delete_a_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap();
        // No quiet period (#619): this test is about which commit may delete,
        // not about how long a fully archived file waits.
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let old = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = old[0].progress_end.clone();

        // Truncated in place, then rewritten shorter.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"zzz\n")
            .unwrap();
        fs.commit_progress(&old_end).await.unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "the new content has not been archived and must not be deleted"
        );

        // The control: a commit that really reaches the end of the file it was
        // read from still deletes it.
        let fresh = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&fresh[0].progress_end).await.unwrap();
        assert!(
            !std::path::Path::new(&path).exists(),
            "archived, so removed"
        );
    }

    /// After `delete_after_commit` removes a fully archived file, a new file at
    /// the same path may be handed the freed inode number, and an inode check
    /// cannot tell it from the one it replaced. The cursor must not outlive
    /// the file: a stale read position applied to the new file would skip its
    /// beginning (#558).
    #[tokio::test]
    async fn deleting_an_archived_file_drops_its_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\n").unwrap();
        // No quiet period (#619): the cursor, not the wait, is under test.
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(fs.cursors.contains_key(&path));
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert!(
            !std::path::Path::new(&path).exists(),
            "archived, so removed"
        );
        assert!(
            !fs.cursors.contains_key(&path),
            "the cursor described a file that no longer exists"
        );

        // A new file at the path is read from its first byte.
        std::fs::write(&path, "ccc\nddd\neee\n").unwrap();
        let fresh = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(fresh[0].records.len(), 3);
    }

    /// A range that starts early enough to fit in what is left of a truncated
    /// file must still not be replayed: the bytes there are not the ones read.
    #[tokio::test]
    async fn replay_refuses_a_file_shorter_than_it_was_when_the_range_was_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\nddd\n").unwrap(); // 16 bytes
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let start = read[0].progress_start.clone();
        let ProgressMarker::File { file_size, .. } = &start else {
            panic!("a file marker");
        };
        assert_eq!(
            *file_size, 16,
            "the start marker records the length the file had when it was read"
        );

        // Truncated to 8 bytes: byte 0 is still inside the file.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"zzz\nyyy\n")
            .unwrap();
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
    }

    /// A restart seeded from a bounded read: the committed offset (12) sits far
    /// behind the end of the 60-byte file the batch saw. If `copytruncate`
    /// rewrote the file to 30 bytes while the engine was down, 30 is above the
    /// offset and passed an offset-only test, so the restart resumed at 12 and
    /// the new file's first 12 bytes were never archived (review of #564). The
    /// size the committed batch observed catches it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_seed_that_remembers_the_observed_size_catches_a_truncation_above_the_offset() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let fresh: String = (0..5).map(|i| format!("new-{i}\n")).collect(); // 30 bytes
        std::fs::write(&path, &fresh).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();

        let mut with_size = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        with_size.seed_committed_from(&path, 12, Some(inode), Some(60), None, None);
        let got = with_size
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            got[0].records.len(),
            5,
            "the file shrank from the 60 bytes the batch saw: it is read from byte 0"
        );

        // Without the size the offset alone cannot tell: this is the old,
        // lossy behaviour, kept for markers that record no size.
        let mut without = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        without.seed_committed_from(&path, 12, Some(inode), None, None, None);
        let got = without
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(got[0].records.len(), 3, "resumes at byte 12");
    }

    /// Review of #564: the process crashes after a post-copytruncate batch
    /// reached VERIFIED but before it committed. Startup seeds the cursor
    /// from the newest COMMITTED row - the previous generation's, at byte
    /// 60 - and recovery then commits the newer batch, which ends at 21. The
    /// file has meanwhile regrown past 60. The next read must resume at 21;
    /// keeping the seeded head would skip 21..60.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_recovered_commit_below_the_seed_restarts_the_read_head_there() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let content: String = (0..12).map(|i| format!("new-{i:02}\n")).collect(); // 84 bytes
        std::fs::write(&path, &content).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();

        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        // The previous generation's committed row: byte 60 of a 60-byte file.
        fs.seed_committed_from(&path, 60, Some(inode), Some(60), None, None);
        // The newer generation's VERIFIED batch: bytes 0..21 of a 21-byte file.
        let recovered = ProgressMarker::File {
            path: path.clone(),
            inode: Some(inode),
            start_byte: 0,
            end_byte: 21,
            file_size: 21,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        fs.commit_progress(&recovered).await.unwrap();

        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            next[0].records.first().map(Vec::as_slice),
            Some(b"new-03".as_slice()),
            "resumed somewhere other than byte 21, the end of the recovered batch"
        );
        assert_eq!(next[0].records.len(), 9);
    }

    /// Review of #564: the recovered batch is a LEGACY marker with no inode.
    /// Restarting the head must not forget the inode the seed knew - the
    /// commit was judged current against that file - or a rotation to a file
    /// at least as long as the batch goes unseen and the new file is read
    /// from byte 21, skipping its first lines.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_legacy_recovered_commit_keeps_the_inode_so_a_rotation_is_still_seen() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let content: String = (0..12).map(|i| format!("old-{i:02}\n")).collect(); // 84 bytes
        std::fs::write(&path, &content).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();

        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        fs.seed_committed_from(&path, 60, Some(inode), Some(60), None, None);
        let recovered = ProgressMarker::File {
            path: path.clone(),
            inode: None,
            start_byte: 0,
            end_byte: 21,
            file_size: 21,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        fs.commit_progress(&recovered).await.unwrap();

        // Rotated: a new file, longer than the batch, at the same path.
        std::fs::rename(&path, format!("{path}.1")).unwrap();
        let fresh: String = (0..12).map(|i| format!("new-{i:02}\n")).collect();
        std::fs::write(&path, &fresh).unwrap();

        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            next[0].records.first().map(Vec::as_slice),
            Some(b"new-00".as_slice()),
            "the rotated file must be read from byte 0"
        );
        assert_eq!(next[0].records.len(), 12);
    }

    /// The restart seed names the file its offset came from. A cursor seeded
    /// from the OLD file's high-water mark must not be applied to the file that
    /// replaced it, and one seeded from the file still there resumes.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_seed_resumes_the_same_file_and_restarts_a_replaced_one() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();

        // Same file: resume after the committed 8 bytes.
        let mut same = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        same.seed_committed_from(&path, 8, Some(inode), None, None, None);
        let got = same
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            got[0].records,
            vec![b"ccc".to_vec()],
            "resumes after byte 8"
        );

        // The ledger remembers a different inode: the file at the path is new.
        let mut replaced = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        replaced.seed_committed_from(&path, 8, Some(inode + 1_000_000), None, None, None);
        let got = replaced
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            got[0].records.len(),
            3,
            "the replaced file is read from byte 0"
        );

        // A legacy row with no inode behaves as before.
        let mut legacy = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        legacy.seed_committed_from(&path, 8, None, None, None, None);
        let got = legacy
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(got[0].records, vec![b"ccc".to_vec()]);
    }
    /// The cursor is per path, so after a rotation it describes the CURRENT
    /// file. Its position must not be reported for an old inode's marker
    /// (review of #563): recovery would treat that old range as handled.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_read_position_is_only_reported_for_the_file_the_cursor_is_in() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let marker = |inode: Option<u64>| ProgressMarker::File {
            path: path.clone(),
            inode,
            start_byte: 4,
            end_byte: 8,
            file_size: 12,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        // Nothing replayed or read yet: no identity, so nothing to report for a
        // marker that names a file.
        assert_eq!(fs.read_position(&marker(Some(inode))), None);
        fs.replay_from_marker(&marker(Some(inode))).await.unwrap();
        assert_eq!(fs.read_position(&marker(Some(inode))), Some(4));
        assert_eq!(
            fs.read_position(&marker(Some(inode + 1))),
            None,
            "another file's range is not placed by this cursor"
        );
        assert_eq!(
            fs.read_position(&marker(None)),
            Some(4),
            "a marker with no inode cannot be told apart, so it is placed"
        );
    }

    // ---- replacement at the same inode (#578) -------------------------------

    fn lines(reads: &[ReadResult]) -> Vec<String> {
        reads
            .iter()
            .flat_map(|read| &read.records)
            .map(|record| String::from_utf8_lossy(record).into_owned())
            .collect()
    }

    /// #578: a file rewritten in place - the same inode, and already as long
    /// as the old read position by the time it is next read - agrees with
    /// the old file by inode and by length. Resuming it at the old offset
    /// skipped its first lines. Its first bytes are what differ.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_same_inode_replacement_that_is_no_shorter_is_read_from_byte_zero() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old one\nold two\n").unwrap(); // 16 bytes
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(lines(&first), ["old one", "old two"]);

        // `std::fs::write` truncates and rewrites the SAME inode; by the next
        // read the new content is longer than the 16 bytes read.
        std::fs::write(&path, "NEW one\nNEW two\nNEW three\n").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().ino(),
            inode,
            "the case under test keeps the inode"
        );
        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            lines(&next),
            ["NEW one", "NEW two", "NEW three"],
            "the replacement must be read from byte 0, not resumed at the old file's \
             offset 16 (which hands out only its third line)"
        );
        assert_eq!(
            next[0].progress_end.content_fingerprint().map(|c| c.len),
            Some(26)
        );
    }

    /// #578, the irreversible half: the old batch commits after the file was
    /// replaced at the same inode by content of the same length. Inode,
    /// length and `end_byte >= size` all agree with the batch, and
    /// `delete_after_commit` deleted the unarchived replacement.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_commit_never_deletes_a_same_inode_replacement() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old one\nold two\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        // No quiet period (#619), so only the content guard can keep it.
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();

        std::fs::write(&path, "NEW one\nNEW two\n").unwrap(); // same length
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).ok().as_deref(),
            Some("NEW one\nNEW two\n"),
            "the replacement was never read; deleting it after the old batch's commit \
             loses it"
        );
    }

    /// #578, across a restart: the seed carries the fingerprint the last
    /// committed batch took, so a file replaced at the same inode while the
    /// engine was down - longer than the offset and than the length that
    /// batch saw - is read from byte 0. A seed from a row that recorded no
    /// fingerprint resumes as before.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_seed_with_a_fingerprint_restarts_a_file_replaced_at_the_same_inode() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "NEW one\nNEW two\nNEW three\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let old = ContentFingerprint::of(b"old one\nold two\n");

        let mut seeded = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        seeded.seed_committed_from(&path, 16, Some(inode), Some(16), old, None);
        let reads = seeded
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(lines(&reads), ["NEW one", "NEW two", "NEW three"]);

        let mut legacy = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        legacy.seed_committed_from(&path, 16, Some(inode), Some(16), None, None);
        let reads = legacy
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            lines(&reads),
            ["NEW three"],
            "a legacy row is judged by inode and length, exactly as before"
        );
    }

    /// A file under one page fingerprints all of itself, so the fingerprint
    /// grows with it; each read checks the last one against the file's first
    /// bytes, and an append is never mistaken for a replacement. A file that
    /// was empty at a read has no fingerprint and proves nothing.
    #[tokio::test]
    async fn a_small_file_that_grows_is_one_file_and_an_empty_one_has_no_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        async fn read(fs: &mut FileSource, path: &str) -> (Vec<String>, ProgressMarker) {
            let reads = fs
                .read_batch_candidates(&src(path), 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            (lines(&reads), reads[0].progress_end.clone())
        }
        let (got, marker) = read(&mut fs, &path).await;
        assert!(got.is_empty());
        assert_eq!(marker.content_fingerprint(), None, "an empty file");
        let mut earlier: Option<ProgressMarker> = None;

        let mut expected = Vec::new();
        for i in 0..300 {
            // Grows past one page over the loop.
            let line = format!("line {i:04} padding padding\n");
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(line.as_bytes())
                .unwrap();
            expected.push(line.trim_end().to_owned());
            let (got, marker) = read(&mut fs, &path).await;
            assert_eq!(got, [expected.last().unwrap().clone()], "append {i}");
            let len = marker.content_fingerprint().unwrap().len;
            let size = std::fs::metadata(&path).unwrap().len();
            assert_eq!(len, size.min(ContentFingerprint::MAX_LEN as u64));
            // Review of #578: an earlier read's fingerprint of another length
            // cannot be compared with the cursor's, so its position is not
            // claimed for it; at full length (both one page) it is.
            if let Some(earlier) = earlier.replace(marker.clone()) {
                let comparable = earlier.content_fingerprint().unwrap().len == len;
                assert_eq!(
                    fs.read_position(&earlier).is_some(),
                    comparable,
                    "append {i}: fingerprint lengths {} and {len}",
                    earlier.content_fingerprint().unwrap().len
                );
            }
        }
    }

    /// The rewind guard also judges content (#578): a range read before an
    /// in-place replacement is never replayed into the new file, and once a
    /// read has seen the replacement the range's position is not reported
    /// for it. Both files are over a page, so their fingerprints cover the
    /// same length and compare; under a page they would not (see
    /// `ContentFingerprint::same_content`) and the position falls back to
    /// the inode, as before.
    #[cfg(unix)]
    #[tokio::test]
    async fn replay_refuses_a_range_from_before_a_same_inode_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let page = |tag: &str, n: usize| -> String {
            (0..n).map(|i| format!("{tag} line {i:05}\n")).collect()
        };
        std::fs::write(&path, page("old", 400)).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let start = read[0].progress_start.clone();

        std::fs::write(&path, page("NEW", 500)).unwrap();
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("replaced at the same inode"), "{err}");

        // Once a read has seen the replacement, the old range's position is
        // not this cursor's to report.
        fs.read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(fs.read_position(&read[0].progress_end), None);
    }

    /// Review of #578 (Codex): startup seeds the cursor from file A, then
    /// recovers a VERIFIED batch read from B, A's replacement at the same
    /// inode, ending AT or ABOVE the seed. The commit is current (B is the
    /// file at the path), but the cursor kept A's fingerprint, so the next
    /// read took B for another replacement and archived its committed
    /// prefix again.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_recovered_commit_at_or_above_the_seed_adopts_the_replacements_fingerprint() {
        use std::os::unix::fs::MetadataExt;
        let a = "old one\nold two\n"; // 16 bytes
        let b = "NEW one\nNEW two\nNEW three\n"; // lines end at 8, 16, 26
        for end in [16u64, 26] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("app.log").to_string_lossy().into_owned();
            std::fs::write(&path, b).unwrap();
            let inode = std::fs::metadata(&path).unwrap().ino();
            let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
            fs.seed_committed_from(
                &path,
                16,
                Some(inode),
                Some(16),
                ContentFingerprint::of(a.as_bytes()),
                None,
            );
            let recovered = ProgressMarker::File {
                path: path.clone(),
                inode: Some(inode),
                start_byte: 0,
                end_byte: end,
                file_size: b.len() as u64,
                mtime: String::new(),
                content_fingerprint: ContentFingerprint::of(b.as_bytes()),
                tail_fingerprint: None,
            };
            fs.commit_progress(&recovered).await.unwrap();
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"NEW four\n")
                .unwrap();
            let reads = fs
                .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            let expected: &[&str] = if end == 16 {
                &["NEW three", "NEW four"]
            } else {
                &["NEW four"]
            };
            assert_eq!(
                lines(&reads),
                expected,
                "recovered end {end}: B's committed prefix [0,{end}) must not be read again"
            );
        }
    }

    /// Review of #578 (cubic): a LEGACY recovered commit - no inode in its
    /// marker - whose file was rotated before recovery ran. The commit cannot
    /// be judged by identity, so it passes; taking the CURRENT file's inode
    /// for the cursor then hid the rotation, and the next read resumed the
    /// new file at the old offset. The seed's inode is kept instead, whether
    /// the recovered end is below the seeded head or at or above it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_legacy_recovered_commit_after_a_rotation_keeps_the_seeds_inode() {
        use std::os::unix::fs::MetadataExt;
        for (seed, end) in [(21u64, 60u64), (60, 21)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("app.log").to_string_lossy().into_owned();
            let old: String = (0..12).map(|i| format!("old-{i:02}\n")).collect(); // 84 bytes
            std::fs::write(&path, &old).unwrap();
            let inode = std::fs::metadata(&path).unwrap().ino();
            let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
            fs.seed_committed_from(&path, seed, Some(inode), Some(84), None, None);

            // Rotated while the engine was down: a new file, longer than the
            // batch, already at the path when recovery commits.
            std::fs::rename(&path, format!("{path}.1")).unwrap();
            let fresh: String = (0..12).map(|i| format!("new-{i:02}\n")).collect();
            std::fs::write(&path, &fresh).unwrap();
            let recovered = ProgressMarker::File {
                path: path.clone(),
                inode: None,
                start_byte: 0,
                end_byte: end,
                file_size: end,
                mtime: String::new(),
                content_fingerprint: None,
                tail_fingerprint: None,
            };
            fs.commit_progress(&recovered).await.unwrap();

            let next = fs
                .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(
                lines(&next).first().map(String::as_str),
                Some("new-00"),
                "seed {seed}, recovered end {end}: the rotated-in file must be read from \
                 byte 0, not resumed at {end}"
            );
            assert_eq!(next[0].records.len(), 12);
        }
    }

    // ---- a failed range from a file rotated away since it was read (#621) ---

    /// One read pass over one path, through the per-source read.
    async fn read_one(fs: &mut FileSource, path: &str) -> Vec<ReadResult> {
        fs.read_batch_candidates(&src(path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap()
    }

    /// One read pass over one path, through the whole-adapter read the engine
    /// uses.
    async fn read_all_one(fs: &mut FileSource, path: &str) -> Vec<ReadResult> {
        let report = fs
            .read_all_batch_candidates(&[src(path)], 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        report
            .outcomes
            .into_iter()
            .flat_map(|outcome| outcome.result.unwrap())
            .collect()
    }

    fn file_marker(marker: &ProgressMarker) -> (Option<u64>, u64, u64) {
        let ProgressMarker::File {
            inode,
            start_byte,
            end_byte,
            ..
        } = marker
        else {
            panic!("a file marker");
        };
        (*inode, *start_byte, *end_byte)
    }

    /// #621: the batch fails after its file has been rotated away - renamed,
    /// then compressed and removed, as logrotate does with `compress` and no
    /// `delaycompress` - and a new file has taken the path. The rewind used to
    /// be refused and the range was never archived. The adapter still holds a
    /// descriptor on the file it read, and re-reads the range through it: the
    /// original bytes, under the original inode, then the new file from byte 0.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_range_is_re_read_through_the_held_descriptor_after_its_file_is_rotated_away()
    {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\nold 2\nold 3\n").unwrap(); // 18 bytes
        let old_inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = read_one(&mut fs, &path).await;
        assert_eq!(lines(&first), ["old 1", "old 2", "old 3"]);

        // The new file is created before the old one is removed, as logrotate
        // does: a filesystem may hand a freed inode to the next file created.
        let rotated = format!("{path}.1");
        std::fs::rename(&path, &rotated).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        std::fs::remove_file(&rotated).unwrap();
        let new_inode = std::fs::metadata(&path).unwrap().ino();
        assert_ne!(old_inode, new_inode, "the test needs a real rotation");

        fs.replay_from_marker(&first[0].progress_start)
            .await
            .expect("the range is re-read through the descriptor still open on the old file");
        let again = read_one(&mut fs, &path).await;
        assert_eq!(
            lines(&again),
            ["old 1", "old 2", "old 3"],
            "the failed range, from the file it was read from"
        );
        assert_eq!(
            file_marker(&again[0].progress_end),
            (Some(old_inode), 0, 18)
        );
        fs.commit_progress(&again[0].progress_end).await.unwrap();

        let next = read_one(&mut fs, &path).await;
        assert_eq!(lines(&next), ["new 1"], "then the new file, from byte 0");
        assert_eq!(file_marker(&next[0].progress_end), (Some(new_inode), 0, 6));
        fs.commit_progress(&next[0].progress_end).await.unwrap();
        assert!(
            lines(&read_one(&mut fs, &path).await).is_empty(),
            "and nothing twice"
        );
    }

    /// #621: the batch failed while its file was still at the path, so the
    /// rewind was accepted - and the file was rotated before the re-read. The
    /// next read found another inode and restarted on the new file, and the
    /// range the engine had been told would be re-read never was, without an
    /// error. The rewound range is now re-read from the old file.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rewind_accepted_before_a_rotation_is_still_re_read_from_the_old_file() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\nold 2\n").unwrap(); // 12 bytes
        let old_inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&first), ["old 1", "old 2"]);
        fs.replay_from_marker(&first[0].progress_start)
            .await
            .expect("the file is still at its path: an ordinary rewind");

        let rotated = format!("{path}.1");
        std::fs::rename(&path, &rotated).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        std::fs::remove_file(&rotated).unwrap();

        let mut old = Vec::new();
        let mut new = Vec::new();
        for _ in 0..4 {
            for read in read_all_one(&mut fs, &path).await {
                if read.is_empty() {
                    continue;
                }
                let (inode, start, end) = file_marker(&read.progress_end);
                let got = lines(std::slice::from_ref(&read));
                if inode == Some(old_inode) {
                    old.push((start, end, got));
                } else {
                    new.push((start, end, got));
                }
                fs.commit_progress(&read.progress_end).await.unwrap();
            }
        }
        assert_eq!(
            old,
            vec![(0, 12, vec!["old 1".to_string(), "old 2".to_string()])],
            "the rewound range is re-read from the old file, once"
        );
        assert_eq!(new, vec![(0, 6, vec!["new 1".to_string()])]);
    }

    /// #621, across a restart: a fresh adapter holds no descriptor, so it
    /// looks for the file the range was read from beside its path - the same
    /// inode with the same first bytes, no shorter than when the range was
    /// read - and re-reads the range from there.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_restarted_adapter_finds_a_failed_range_in_the_file_it_was_rotated_to() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\nold 2\n").unwrap(); // 12 bytes
        let old_inode = std::fs::metadata(&path).unwrap().ino();
        let start = {
            let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
            read_one(&mut fs, &path).await[0].progress_start.clone()
        }; // the engine stops

        std::fs::rename(&path, format!("{path}.1")).unwrap();
        std::fs::write(&path, "new 1\nnew 2\nnew 3\n").unwrap();

        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        fs.replay_from_marker(&start)
            .await
            .expect("the rotated file is found beside the path and proven to be the one read");
        let again = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&again), ["old 1", "old 2"]);
        assert_eq!(
            file_marker(&again[0].progress_end),
            (Some(old_inode), 0, 12)
        );
        fs.commit_progress(&again[0].progress_end).await.unwrap();
        let next = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&next), ["new 1", "new 2", "new 3"]);
    }

    /// #620 in the rotated-file proof: after a restart the search finds the
    /// rotated file by inode, and its first page and length agree with the
    /// marker - but it was rewritten in place below that page, so the page
    /// ending where the failed range starts (what its start marker
    /// fingerprints) is not the one it was read after. It is not the range's
    /// file, and the rewind stays refused.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_file_rewritten_below_its_first_page_is_not_re_read_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let header = "#".repeat(ContentFingerprint::MAX_LEN + 100) + "\n";
        std::fs::write(&path, format!("{header}old 1\n")).unwrap();
        let start = {
            let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
            let first = read_one(&mut fs, &path).await;
            fs.commit_progress(&first[0].progress_end).await.unwrap();
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"old 2\n")
                .unwrap();
            // The window that fails starts above byte 0.
            let second = read_one(&mut fs, &path).await;
            assert_eq!(lines(&second), ["old 2"]);
            second[0].progress_start.clone()
        }; // the engine stops
        let ProgressMarker::File {
            tail_fingerprint, ..
        } = &start
        else {
            panic!("a file marker");
        };
        assert!(
            tail_fingerprint.is_some(),
            "its start marker fingerprints the page below it"
        );

        let rotated = format!("{path}.1");
        std::fs::rename(&path, &rotated).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        // Same inode, same first page, same length - other bytes below the
        // range's start.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&rotated)
            .unwrap();
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(header.len() as u64)).unwrap();
        file.write_all(b"OLD 1\n").unwrap();
        drop(file);

        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("identity changed"), "{err}");
    }

    /// Review of #621: the restart seed is the newest committed row of the
    /// path, unless that row names a file no longer at the path (a re-read of
    /// a rotated file commits after the current file's batches). Then it is
    /// the current file's row - only on positive identity: the same inode,
    /// the first page and the page below its end. Anything less keeps the
    /// newest row, which costs a re-read of the current file, never a skip.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_restart_seed_is_the_file_at_the_path_only_when_it_is_proven() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "new 1\nnew 2\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = read_one(&mut fs, &path).await;
        let ProgressMarker::File {
            content_fingerprint,
            tail_fingerprint,
            ..
        } = &read[0].progress_end
        else {
            panic!("a file marker");
        };
        let current = ArchivedEnd {
            inode: Some(inode),
            end_byte: 12,
            content: content_fingerprint.clone(),
            tail: tail_fingerprint.clone(),
        };
        assert!(current.content.is_some() && current.tail.is_some());
        let rotated = ArchivedEnd {
            inode: Some(inode + 1_000_000),
            end_byte: 30,
            content: ContentFingerprint::of(b"old"),
            tail: ContentFingerprint::of_tail(b"old"),
        };

        assert_eq!(
            FileSource::seed_choice(&path, &[current.clone(), rotated.clone()]),
            0,
            "the newest row is the file at the path: as before"
        );
        assert_eq!(
            FileSource::seed_choice(&path, &[rotated.clone(), current.clone()]),
            1,
            "the newest row is a rotated file's re-read: the file at the path is proven"
        );
        let legacy = ArchivedEnd {
            tail: None,
            ..current.clone()
        };
        assert_eq!(
            FileSource::seed_choice(&path, &[rotated.clone(), legacy]),
            0,
            "a row without the page below its end proves nothing"
        );
        // Rewritten in place below the first page: the same inode and first
        // bytes, other bytes below the end.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(b"new 1\nNEW 2\n")
            .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        let short_head = ArchivedEnd {
            content: ContentFingerprint::of(b"new 1\n"),
            ..current.clone()
        };
        assert_eq!(
            FileSource::seed_choice(&path, &[rotated, short_head]),
            0,
            "a file whose bytes below the end changed is not the row's file"
        );
    }

    /// #621 with `delete_after_commit` (#619): a delete can find the path's
    /// descriptor still held. A later window was read, then the file was cut
    /// back to the end of an earlier one before that window committed. The
    /// earlier window's commit is current and covers the whole (shortened)
    /// file, so it records the delete. It does not release the descriptor,
    /// whose later range is still outstanding. Once the file is deleted and its
    /// cursor dropped, nothing reads that path again, so nothing else would
    /// ever close the descriptor: it pinned the deleted file's space, and a
    /// slot of the limit, for the life of the process.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_delete_closes_a_descriptor_the_adapter_still_held_for_that_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "a 1\n").unwrap(); // 4 bytes
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let first = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&first), ["a 1"]);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"b 2\n")
            .unwrap();
        let second = read_all_one(&mut fs, &path).await;
        assert_eq!(
            lines(&second),
            ["b 2"],
            "a later window, [4, 8), outstanding"
        );
        // Cut back to the first window's end before the second commits.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(4)
            .unwrap();

        fs.commit_progress(&first[0].progress_end).await.unwrap();
        assert!(
            !Path::new(&path).exists(),
            "the first window covers the whole file now, so it is deleted"
        );
        assert!(
            fs.held.is_empty(),
            "and the descriptor held for its outstanding range is closed with it"
        );
    }

    /// #621 with `delete_after_commit` (#619): the re-read of a rotated range
    /// and the deferred delete work on the same path without stepping on each
    /// other. The re-read's commit names the rotated file's inode, which is
    /// not the file at the path, so it records no delete candidate: neither
    /// the rotated file nor the new one is deleted for it. The new file, once
    /// fully archived and unwritten for the quiet period, is deleted as usual,
    /// and nothing the adapter held for that path outlives it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_re_read_and_delete_after_commit_share_a_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let rotated = format!("{path}.1");
        std::fs::write(&path, "old 1\nold 2\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let first = read_all_one(&mut fs, &path).await;
        std::fs::rename(&path, &rotated).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        fs.replay_from_marker(&first[0].progress_start)
            .await
            .expect("re-read through the held descriptor");

        let again = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&again), ["old 1", "old 2"]);
        fs.commit_progress(&again[0].progress_end).await.unwrap();
        assert!(
            Path::new(&rotated).exists() && Path::new(&path).exists(),
            "the re-read's commit deletes neither the rotated file nor the new one"
        );

        let next = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&next), ["new 1"]);
        fs.commit_progress(&next[0].progress_end).await.unwrap();
        assert!(
            !Path::new(&path).exists(),
            "the new file, fully archived and quiet, is deleted as before"
        );
        assert!(
            Path::new(&rotated).exists(),
            "the rotated file is not this adapter's to delete"
        );
        assert!(
            fs.held.is_empty() && fs.rotated.is_empty(),
            "no descriptor outlives what it was kept for"
        );
    }

    /// Review of #621: a commit raises a held file's commit floor only if it
    /// was read from the content that file holds NOW, not merely from its
    /// inode. A window of the old content, committed late after the file was
    /// rewritten in place, raised the floor of the new content's descriptor.
    /// When the new content's range then failed and the file was renamed away
    /// before the re-read, the re-read started at the old window's end and
    /// skipped the new content's first bytes.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_late_commit_of_replaced_content_does_not_raise_the_held_files_floor() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\n").unwrap(); // 6 bytes
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let old = read_all_one(&mut fs, &path).await;
        assert_eq!(lines(&old), ["old 1"]);

        // Rewritten in place, same inode, longer, other first bytes.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"NEW 1\nNEW 2\n")
            .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        let new = read_all_one(&mut fs, &path).await;
        assert_eq!(
            lines(&new),
            ["NEW 1", "NEW 2"],
            "the replacement, from byte 0"
        );

        // The old window commits late; the new one fails and is handed back.
        fs.commit_progress(&old[0].progress_end).await.unwrap();
        fs.replay_from_marker(&new[0].progress_start)
            .await
            .expect("the new content is still at its path");
        // Renamed away before the re-read, nothing in its place.
        std::fs::rename(&path, format!("{path}.1")).unwrap();

        let mut got = Vec::new();
        for _ in 0..3 {
            let sources = fs.discover_sources().await.unwrap();
            let report = fs
                .read_all_batch_candidates(&sources, 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            for outcome in report.outcomes {
                for read in outcome.result.unwrap() {
                    if read.is_empty() {
                        continue;
                    }
                    got.push((
                        file_marker(&read.progress_end),
                        lines(std::slice::from_ref(&read)),
                    ));
                    fs.commit_progress(&read.progress_end).await.unwrap();
                }
            }
        }
        assert_eq!(
            got,
            vec![(
                (Some(inode), 0, 12),
                vec!["NEW 1".to_string(), "NEW 2".to_string()]
            )],
            "the new content's range from its first byte, not from the old window's end"
        );
    }

    /// Review of #621: a rewind of a rotated file's re-read never goes below
    /// what is committed from that file, as a rewind of a file at its path
    /// never goes below its committed point. Recovery rewinds, then commits a
    /// VERIFIED row of the same file that starts where the pending range does
    /// (the object that replaced a stale row for the same bytes), then rewinds
    /// again. Kept at the first rewind's position, the second re-read the
    /// bytes that commit had just archived.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_re_read_is_never_rewound_below_what_is_committed_of_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\nold 2\n").unwrap(); // 12 bytes
        let (start, end) = {
            let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
            let read = read_one(&mut fs, &path).await;
            (read[0].progress_start.clone(), read[0].progress_end.clone())
        };
        std::fs::rename(&path, format!("{path}.1")).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();

        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        fs.replay_from_marker(&start).await.unwrap();
        // The VERIFIED row for the same bytes commits during recovery.
        fs.commit_progress(&end).await.unwrap();
        fs.replay_from_marker(&start).await.unwrap();
        assert_eq!(fs.read_position(&start), Some(12), "landed at the commit");

        let mut got = Vec::new();
        for _ in 0..3 {
            for read in read_all_one(&mut fs, &path).await {
                got.extend(lines(std::slice::from_ref(&read)));
                fs.commit_progress(&read.progress_end).await.unwrap();
            }
        }
        assert_eq!(got, ["new 1"], "the committed bytes are not archived again");
    }

    /// Recovery rewinds every range twice (once to learn where it lands, once
    /// after the commits). A rotated file's range is re-read once, not twice.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_range_rewound_twice_is_re_read_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\nold 2\n").unwrap();
        let start = {
            let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
            read_one(&mut fs, &path).await[0].progress_start.clone()
        };
        std::fs::rename(&path, format!("{path}.1")).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();

        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        for result in fs
            .replay_from_markers(&[start.clone(), start.clone()])
            .await
        {
            result.unwrap();
        }
        fs.replay_from_marker(&start).await.unwrap();
        let mut got = Vec::new();
        for _ in 0..4 {
            for read in read_all_one(&mut fs, &path).await {
                got.extend(lines(std::slice::from_ref(&read)));
                fs.commit_progress(&read.progress_end).await.unwrap();
            }
        }
        assert_eq!(got, ["old 1", "old 2", "new 1"]);
    }

    /// #621 (review): a rewind accepted while the file was at its path, then
    /// the file renamed away with NOTHING created at the path. No pattern
    /// matches the path any more, so discovery dropped it, no read ran, and
    /// the rewound range sat behind a held descriptor that was never read.
    /// The path stays a source until the rewound range is re-read.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rewound_file_renamed_away_with_nothing_at_its_path_is_still_re_read() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old 1\nold 2\n").unwrap(); // 12 bytes
        let old_inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = read_all_one(&mut fs, &path).await;
        fs.replay_from_marker(&first[0].progress_start)
            .await
            .expect("the file is still at its path: an ordinary rewind");
        std::fs::rename(&path, format!("{path}.1")).unwrap();

        let mut got = Vec::new();
        for _ in 0..3 {
            let sources = fs.discover_sources().await.unwrap();
            let report = fs
                .read_all_batch_candidates(&sources, 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            for outcome in report.outcomes {
                for read in outcome.result.unwrap() {
                    if read.is_empty() {
                        continue;
                    }
                    got.push((
                        file_marker(&read.progress_end),
                        lines(std::slice::from_ref(&read)),
                    ));
                    fs.commit_progress(&read.progress_end).await.unwrap();
                }
            }
        }
        assert_eq!(
            got,
            vec![(
                (Some(old_inode), 0, 12),
                vec!["old 1".to_string(), "old 2".to_string()]
            )],
            "the rewound range, once, from the file renamed away"
        );
        assert!(
            fs.discover_sources().await.unwrap().is_empty(),
            "and the path is no longer a source once it is archived"
        );
    }

    /// #621 (review): a rotated file truncated after its rewind was accepted
    /// keeps its first page when the cut is above it, so the fingerprint still
    /// matches. The re-read then archived a shortened prefix of the range and
    /// called it done. The descriptor must still be as long as the file was
    /// when its ranges were read; a shorter one is refused, loudly.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_file_truncated_after_its_rewind_is_refused_not_read_short() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let old: String = (0..200)
            .map(|i| format!("line {i:04} of the old file\n"))
            .collect();
        assert!(old.len() > 4096 + 500, "longer than the fingerprinted page");
        std::fs::write(&path, &old).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let first = fs
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(first[0].records.len(), 200);

        let rotated = format!("{path}.1");
        std::fs::rename(&path, &rotated).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        fs.replay_from_marker(&first[0].progress_start)
            .await
            .expect("the held descriptor proves the file");
        // Cut above the first page: the fingerprint still matches.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&rotated)
            .unwrap()
            .set_len(4_500)
            .unwrap();

        let err = fs
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .expect_err("a shortened rotated file must not be re-read as if it were the range")
            .to_string();
        assert!(err.contains("shorter"), "{err}");
        assert_eq!(
            fs.take_refused_rereads(),
            vec![path.clone()],
            "the refusal is handed to the engine to count"
        );
        let next = fs
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(lines(&next), ["new 1"], "the new file is read as before");
    }

    /// #621: the descriptors are bounded. Past the limit a file is not held,
    /// and a rotated file's descriptor with no re-read in progress is closed
    /// once unused for the hold time - after which a range of a rotated file
    /// that is gone cannot be re-read, and is refused as before.
    #[cfg(unix)]
    #[tokio::test]
    async fn held_descriptors_are_bounded_in_number_and_in_time() {
        fn rotate_away(path: &str) {
            std::fs::rename(path, format!("{path}.1")).unwrap();
            std::fs::write(path, "new 1\n").unwrap();
            std::fs::remove_file(format!("{path}.1")).unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.log").to_string_lossy().into_owned();
        let b = dir.path().join("b.log").to_string_lossy().into_owned();
        std::fs::write(&a, "a 1\n").unwrap();
        std::fs::write(&b, "b 1\n").unwrap();

        // One descriptor at most: the first file read is held, the second not.
        let mut fs = FileSource::new(vec![a.clone(), b.clone()], TelemetryFormat::Raw, false)
            .with_hold_limits(1, Duration::from_secs(3600));
        let start_a = read_one(&mut fs, &a).await[0].progress_start.clone();
        let start_b = read_one(&mut fs, &b).await[0].progress_start.clone();
        rotate_away(&a);
        rotate_away(&b);
        fs.replay_from_marker(&start_a)
            .await
            .expect("held within the limit");
        let err = fs
            .replay_from_marker(&start_b)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("identity changed"), "{err}");

        // No hold time: the rotated file's descriptor is closed at the next
        // pass, before its range fails.
        std::fs::write(&a, "a 2\n").unwrap();
        let mut fs = FileSource::new(vec![a.clone()], TelemetryFormat::Raw, false)
            .with_hold_limits(8, Duration::ZERO);
        let start = read_one(&mut fs, &a).await[0].progress_start.clone();
        rotate_away(&a);
        assert_eq!(lines(&read_one(&mut fs, &a).await), ["new 1"]);
        assert_eq!(fs.rotated.len(), 1, "kept: its range is not committed");
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("identity changed"), "{err}");
        assert!(fs.rotated.is_empty() && fs.held.len() <= 1);
    }

    /// #621: a range is re-read from another file only when that file is
    /// PROVEN to be the one it was read from. Anything else keeps the loud
    /// refusal: a `copytruncate` copy (a new inode holding the same bytes), a
    /// rotated file rewritten in place, a rotated file that is gone.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_range_is_re_read_from_a_rotated_file_only_when_its_identity_is_proven() {
        let old = "old 1\nold 2\n";
        let setup = || {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("app.log").to_string_lossy().into_owned();
            std::fs::write(&path, old).unwrap();
            (dir, path)
        };
        async fn first_start(path: &str) -> ProgressMarker {
            let mut fs = FileSource::new(vec![path.to_string()], TelemetryFormat::Raw, false);
            read_one(&mut fs, path).await[0].progress_start.clone()
        }

        // copytruncate, in the same process: the copy has a new inode, and the
        // descriptor on the original now reads the truncated content.
        let (_dir, path) = setup();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let start = read_one(&mut fs, &path).await[0].progress_start.clone();
        std::fs::copy(&path, format!("{path}.1")).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"n\n")
            .unwrap();
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
        assert_eq!(lines(&read_one(&mut fs, &path).await), ["n"]);

        // Rotated, then rewritten in place: the same inode, other first bytes.
        let (_dir, path) = setup();
        let start = first_start(&path).await;
        let rotated = format!("{path}.1");
        std::fs::rename(&path, &rotated).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&rotated)
            .unwrap()
            .write_all(b"OLD 1\n")
            .unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("identity changed"), "{err}");

        // Rotated, and gone before a restart: nothing holds it any more.
        let (_dir, path) = setup();
        let start = first_start(&path).await;
        std::fs::rename(&path, format!("{path}.1")).unwrap();
        std::fs::write(&path, "new 1\n").unwrap();
        std::fs::remove_file(format!("{path}.1")).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let err = fs.replay_from_marker(&start).await.unwrap_err().to_string();
        assert!(err.contains("identity changed"), "{err}");
        assert_eq!(
            lines(&read_one(&mut fs, &path).await),
            ["new 1"],
            "a refused range leaves the new file's read alone"
        );
    }
    // ---- delete_after_commit on a file still being written (#619) -----------

    /// #619 (A): the delete decision is a check and the unlink an act, with
    /// nothing holding the file in between. A line appended after the check
    /// went into the inode that was then unlinked, with its cursor dropped:
    /// nothing would ever read it. The seam appends exactly there.
    #[tokio::test]
    async fn a_line_appended_between_the_delete_check_and_the_unlink_is_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true);
        // No quiet period, so the commit goes straight to the delete and the
        // only thing under test is the gap between its check and its unlink.
        fs.delete_quiet = Duration::ZERO;
        let appender = path.clone();
        let appended = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let once = appended.clone();
        fs.before_delete = Some(std::sync::Arc::new(move |_| {
            if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&appender)
                    .unwrap()
                    .write_all(b"three\n")
                    .unwrap();
            }
        }));
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(read[0].records.len(), 2);
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert!(
            appended.load(std::sync::atomic::Ordering::SeqCst),
            "the seam never ran, so the delete was never attempted and this test proves nothing"
        );
        assert_eq!(
            std::fs::read_to_string(&path).ok().as_deref(),
            Some("one\ntwo\nthree\n"),
            "the line appended after the delete check was unlinked with the file, unarchived"
        );
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            names,
            vec![std::ffi::OsString::from("app.log")],
            "put back under its own name, with no hidden copy left beside it"
        );
        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            next[0].records,
            vec![b"three".to_vec()],
            "the appended line is handed out for archiving, from where the commit left off"
        );
    }

    /// #619 (B): most appenders open the log once with `O_APPEND` and keep the
    /// descriptor. Catching up to the end and deleting the path does not stop
    /// them: every later line goes into the unlinked inode, which no path
    /// names, until the writer reopens. Not a race - it happened every time
    /// the engine caught up on a live file.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_a_writer_still_holds_open_is_not_deleted_when_the_engine_catches_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let mut writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        writer.write_all(b"one\ntwo\n").unwrap();
        // The default configuration: what an operator who only set
        // `delete_after_commit: true` gets.
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(read[0].records.len(), 2);
        // Caught up: the committed range ends exactly at the end of the file.
        fs.commit_progress(&read[0].progress_end).await.unwrap();

        // The writer goes on through the descriptor it never closed.
        writer.write_all(b"three\n").unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "the file was deleted while a writer held it open; \"three\" went into an \
             unlinked inode that nothing will read"
        );
        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            next[0].records,
            vec![b"three".to_vec()],
            "the line written after the engine caught up is read for archiving"
        );
        fs.commit_progress(&next[0].progress_end).await.unwrap();
        writer.write_all(b"four\n").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).ok().as_deref(),
            Some("one\ntwo\nthree\nfour\n"),
            "still live after the second catch-up, so still not deleted"
        );
    }

    /// The other half of #619: a kept file is not kept forever. Once it has
    /// stayed exactly as its covering commit saw it for the quiet period, the
    /// next read pass - the engine's path, `read_all_batch_candidates` -
    /// deletes it, reports nothing to read for it, and drops its cursor.
    #[tokio::test]
    async fn a_fully_archived_file_is_deleted_once_it_has_stayed_unwritten_for_the_quiet_period() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let quiet = Duration::from_millis(50);
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        let pass = fs
            .read_all_batch_candidates(&[src(&path)], 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let reads = pass.outcomes[0].result.as_ref().unwrap();
        assert_eq!(reads[0].records.len(), 2);
        fs.commit_progress(&reads[0].progress_end).await.unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "kept at the commit: it has not been quiet for {quiet:?} yet"
        );

        std::thread::sleep(quiet * 2);
        let pass = fs
            .read_all_batch_candidates(&[src(&path)], 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            pass.outcomes.len(),
            1,
            "one outcome per source, deleted or not"
        );
        assert!(
            pass.outcomes[0].result.as_ref().unwrap().is_empty(),
            "a deleted file has nothing to read"
        );
        assert!(
            !std::path::Path::new(&path).exists(),
            "unwritten for twice the quiet period after its covering commit, so deleted"
        );
        assert!(
            !fs.cursors.contains_key(&path),
            "the cursor described a file that no longer exists (#558)"
        );
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(
            leftovers.is_empty(),
            "nothing is left under the hidden name it was moved to: {leftovers:?}"
        );
    }

    /// Waiting is not enough: the file must also be unchanged when the wait
    /// is over. A line written after the commit, even one that lands before
    /// the quiet period would have run out, means a writer is still there.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_write_after_the_covering_commit_keeps_the_file_however_long_ago_the_commit_was() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let mut writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        writer.write_all(b"one\n").unwrap();
        let quiet = Duration::from_millis(50);
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        writer.write_all(b"two\n").unwrap();
        std::thread::sleep(quiet * 2);

        let next = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "written to after its covering commit, so a writer may still hold it open"
        );
        assert_eq!(next[0].records, vec![b"two".to_vec()]);
    }

    /// The wait is in memory, so a restart inside it would leave a fully
    /// archived file on disk forever: nothing new is ever committed for it.
    /// The seed restarts the wait - but only from a row that names the inode
    /// and the first page, since a legacy row cannot tell the file from one
    /// that replaced it while the engine was down.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_restart_inside_the_quiet_period_still_deletes_the_file_later() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let mut before = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true);
        let read = before
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        before.commit_progress(&read[0].progress_end).await.unwrap();
        assert!(std::path::Path::new(&path).exists(), "kept: not quiet yet");
        let ProgressMarker::File {
            inode,
            end_byte,
            file_size,
            mtime,
            content_fingerprint,
            tail_fingerprint,
            ..
        } = read[0].progress_end.clone()
        else {
            panic!("expected a file marker");
        };

        let quiet = Duration::from_millis(50);
        let mut legacy = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        legacy.seed_committed_from(&path, end_byte, inode, Some(file_size), None, None);
        legacy.arm_delete_from_seed(&path, Some(&mtime));
        let mut after = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        after.seed_committed_from(
            &path,
            end_byte,
            inode,
            Some(file_size),
            content_fingerprint,
            tail_fingerprint,
        );
        after.arm_delete_from_seed(&path, Some(&mtime));
        std::thread::sleep(quiet * 2);

        legacy
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "a seed without the first page cannot prove this is the file it archived"
        );
        let reads = after
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(reads.is_empty(), "a deleted file has nothing to read");
        assert!(
            !std::path::Path::new(&path).exists(),
            "fully archived and unwritten for the quiet period since the restart, so deleted"
        );
    }

    /// Review of #631: a delete that FAILS (here the rename, because the path
    /// was briefly empty) says nothing about the file, so the candidate must
    /// survive it. Dropping it kept an archived file forever, since nothing
    /// new is ever committed for it to record another.
    #[tokio::test]
    async fn a_delete_that_fails_is_tried_again_on_the_next_pass() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let elsewhere = dir.path().join("elsewhere");
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let (from, to) = (path.clone(), elsewhere.clone());
        let failed_once = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let once = failed_once.clone();
        fs.before_delete = Some(std::sync::Arc::new(move |_| {
            if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                std::fs::rename(&from, &to).unwrap();
            }
        }));
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert!(failed_once.load(std::sync::atomic::Ordering::SeqCst));
        // Back under its name: same inode, same length, same mtime.
        std::fs::rename(&elsewhere, &path).unwrap();
        assert!(
            fs.cursors[&path].delete_candidate.is_some(),
            "a failed rename dropped the candidate"
        );

        let reads = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(reads.is_empty(), "a deleted file has nothing to read");
        assert!(
            !std::path::Path::new(&path).exists(),
            "the delete is retried once the failure has passed"
        );
    }

    /// Review of #631: a seed whose batch read only PART of its file (it saw
    /// the file longer than the committed offset) is not a fully archived
    /// file. If the file was rewritten at the same inode while the engine was
    /// down - cut back to exactly the committed offset, first page unchanged -
    /// inode, first page and length all agree with the seed, and only the
    /// length the batch saw tells it apart. The read's continuity check uses
    /// that length and restarts at byte 0, but a delete armed by the seed ran
    /// before any read, and with no quiet period deleted the unread rewrite.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_seed_from_a_partly_read_file_never_arms_its_delete() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let line = |tag: &str, i: usize| format!("{tag}-{i:04}-{}\n", "x".repeat(90));
        let original: String = (0..164).map(|i| line("old", i)).collect();
        assert_eq!(original.len(), 16_400);
        std::fs::write(&path, &original).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut before = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true);
        let read = before
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let content = read[0].progress_end.content_fingerprint().cloned();
        assert!(
            content.as_ref().is_some_and(|c| c.len < 4_100),
            "the fingerprint covers less than the 41 lines the rewrite keeps"
        );

        // While the engine is down: rewritten in place to the committed
        // offset, the first 41 lines kept and the rest replaced.
        let committed = 8_200u64;
        let rewrite: String = (0..82)
            .map(|i| line(if i < 41 { "old" } else { "NEW" }, i))
            .collect();
        assert_eq!(rewrite.len() as u64, committed);
        let original_mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::fs::write(&path, &rewrite).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        // Its mtime put back, so only the length the batch saw tells it apart.
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(original_mtime)
            .unwrap();
        let ProgressMarker::File { mtime, .. } = read[0].progress_end.clone() else {
            panic!("expected a file marker");
        };

        // The newest committed row read [.., 8200) of a file it saw at 16400.
        let mut after = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        // No tail (a row from before #620): the observed-size guard alone is
        // under test here, and a tail would also catch this rewrite.
        after.seed_committed_from(&path, committed, Some(inode), Some(16_400), content, None);
        after.arm_delete_from_seed(&path, Some(&mtime));
        let reads = after
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "the rewrite was never read; deleting it on the seed's word loses it"
        );
        assert!(std::fs::read_to_string(&path).unwrap() == rewrite);
        assert_eq!(
            reads[0].records.len(),
            82,
            "the shrunken file is read from byte 0, as the continuity check decides"
        );
    }

    /// Review of #631: a file that is GONE is not a transient failure. A
    /// retried candidate for a vanished path outlives the file it described,
    /// and could be matched against whatever is created at that name next.
    #[tokio::test]
    async fn a_candidate_for_a_file_that_is_gone_is_dropped_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let quiet = Duration::from_millis(50);
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert!(fs.cursors[&path].delete_candidate.is_some());

        std::fs::remove_file(&path).unwrap();
        std::thread::sleep(quiet * 2);
        // The read of the missing file fails; the delete before it must
        // still have settled the candidate.
        let _ = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await;
        assert!(
            fs.cursors
                .get(&path)
                .is_none_or(|c| c.delete_candidate.is_none()),
            "the file is gone, yet its delete candidate is kept to be retried"
        );
    }

    /// Commit a fully read file, then rewrite it in place with `after` (same
    /// length) and stamp a new mtime. `with_tail: false` strips the marker's
    /// tail fingerprint first, as for a range from before #620.
    async fn commit_then_restamp(
        path: &str,
        original: &str,
        after: &str,
        with_tail: bool,
        quiet: Duration,
    ) -> FileSource {
        std::fs::write(path, original).unwrap();
        let mut fs = FileSource::new(vec![path.to_owned()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        let read = fs
            .read_batch_candidates(&src(path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let mut end = read[0].progress_end.clone();
        assert!(
            end.content_fingerprint().is_some_and(|c| c.len < 5_000),
            "the first-page fingerprint stops before the rewritten lines"
        );
        if let ProgressMarker::File {
            tail_fingerprint, ..
        } = &mut end
        {
            assert!(
                tail_fingerprint.is_some(),
                "a read to the end records its tail"
            );
            if !with_tail {
                *tail_fingerprint = None;
            }
        }
        fs.commit_progress(&end).await.unwrap();
        let mut file = std::fs::File::options().write(true).open(path).unwrap();
        file.write_all(after.as_bytes()).unwrap();
        file.set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000))
            .unwrap();
        drop(file);
        fs
    }

    /// Lines 50-59 of 60, each 100 bytes: past the first page, inside the
    /// last one.
    fn sixty_lines(rewrite_from: usize) -> String {
        (0..60)
            .map(|i| {
                let tag = if i < rewrite_from { "old" } else { "NEW" };
                format!("{tag}-{i:04}-{}\n", "x".repeat(90))
            })
            .collect()
    }

    /// Review of #631 (P1): a same-length rewrite PAST the first page, with a
    /// new mtime, agrees with the candidate on inode, first page and length.
    /// Treating that as a harmless touch re-armed the delete, and one quiet
    /// period later the rewritten bytes - never read - were deleted. With the
    /// tail fingerprint (#620) the rewrite is seen in the page below the
    /// committed end; without one, a moved mtime ends the candidate. Either
    /// way the file is kept.
    #[tokio::test]
    async fn a_rewrite_past_the_first_page_with_a_new_mtime_is_never_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let (original, rewrite) = (sixty_lines(60), sixty_lines(50));
        assert_eq!(original.len(), rewrite.len());
        let quiet = Duration::from_millis(50);
        for (name, with_tail) in [("tail.log", true), ("no-tail.log", false)] {
            let path = dir.path().join(name).to_string_lossy().into_owned();
            let mut fs = commit_then_restamp(&path, &original, &rewrite, with_tail, quiet).await;
            for _ in 0..3 {
                std::thread::sleep(quiet * 2);
                // A read of a deleted file fails; the assertion below says why.
                let _ = fs
                    .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
                    .await;
            }
            assert!(
                std::fs::read_to_string(&path).ok().as_deref() == Some(rewrite.as_str()),
                "{name}: rewritten past the first page with a new mtime, yet deleted after \
                 the quiet period"
            );
        }
    }

    /// #620 on the delete path: a same-length rewrite past the first page that
    /// puts the old mtime back agrees with the candidate on inode, first
    /// page, length AND mtime, and - unchanged to a stat - is skipped by the
    /// read pass too. Only the tail fingerprint, checked before the unlink,
    /// tells it from the file that was archived.
    #[tokio::test]
    async fn a_rewrite_that_keeps_its_mtime_is_caught_by_the_tail_before_the_unlink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let (original, rewrite) = (sixty_lines(60), sixty_lines(50));
        std::fs::write(&path, &original).unwrap();
        let quiet = Duration::from_millis(50);
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        let read = fs
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        let archived_mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut file = std::fs::File::options().write(true).open(&path).unwrap();
        file.write_all(rewrite.as_bytes()).unwrap();
        file.set_modified(archived_mtime).unwrap();
        drop(file);

        std::thread::sleep(quiet * 2);
        let _ = fs
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await;
        assert!(
            std::fs::read_to_string(&path).ok().as_deref() == Some(rewrite.as_str()),
            "rewritten below the committed end with its mtime put back, yet deleted"
        );
    }

    /// The other side of the moved-mtime rule (#620, review of #631): a file
    /// only TOUCHED - same inode, first page, tail and length, new mtime - is
    /// proven unchanged by the tail, so its wait restarts and it is deleted
    /// once it has stayed quiet. Ending the candidate instead kept it
    /// forever, since a file at its end yields no new covering commit.
    /// Without a tail nothing proves the touch harmless, and it is kept.
    #[tokio::test]
    async fn a_touched_file_whose_tail_still_matches_restarts_the_wait_and_is_deleted_later() {
        let dir = tempfile::tempdir().unwrap();
        let original = sixty_lines(60);
        let quiet = Duration::from_millis(50);

        let path = dir
            .path()
            .join("touched.log")
            .to_string_lossy()
            .into_owned();
        let mut fs = commit_then_restamp(&path, &original, &original, true, quiet).await;
        std::thread::sleep(quiet * 2);
        fs.read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "touched after the commit: the quiet period starts again, so not deleted yet"
        );
        assert!(
            fs.cursors[&path].delete_candidate.is_some(),
            "a proven touch keeps the candidate"
        );
        std::thread::sleep(quiet * 2);
        let reads = fs
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(reads.is_empty(), "a deleted file has nothing to read");
        assert!(
            !std::path::Path::new(&path).exists(),
            "unchanged for the quiet period after the touch, so deleted"
        );

        let legacy = dir.path().join("legacy.log").to_string_lossy().into_owned();
        let mut fs = commit_then_restamp(&legacy, &original, &original, false, quiet).await;
        for _ in 0..3 {
            std::thread::sleep(quiet * 2);
            fs.read_batch_candidates(&src(&legacy), 1_000, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
        }
        assert!(
            std::path::Path::new(&legacy).exists(),
            "without a tail, a moved mtime cannot be told from a rewrite: kept"
        );
    }

    /// Review of #631: the name a file is moved aside to must not grow with
    /// the file's own name. Built from it, a source name near the 255-byte
    /// limit made a hidden name over it; the rename failed with
    /// ENAMETOOLONG on every pass and the archived file was never deleted.
    #[tokio::test]
    async fn a_file_with_a_name_near_the_length_limit_is_still_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let name = format!("{}.log", "n".repeat(246));
        assert_eq!(name.len(), 250);
        let path = dir.path().join(&name).to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert!(
            !std::path::Path::new(&path).exists(),
            "fully archived, no quiet period, and a legal name: deleted"
        );
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(
            leftovers.is_empty(),
            "nothing left beside it: {leftovers:?}"
        );
    }

    /// Review of #631: the restart counterpart of the moved-mtime rule. A
    /// file rewritten past its first page while the engine was down, at the
    /// same inode and length, agrees with the seed on everything the seed
    /// recorded before #631 - and arming took the file's CURRENT mtime as the
    /// baseline, so the rewrite was deleted one quiet period later, unread.
    /// The delete is armed only when the file still has the mtime the
    /// committed batch saw.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_restart_never_arms_the_delete_of_a_file_rewritten_while_the_engine_was_down() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let line = |tag: &str, i: usize| format!("{tag}-{i:04}-{}\n", "x".repeat(90));
        let original: String = (0..60).map(|i| line("old", i)).collect();
        let rewrite: String = (0..60)
            .map(|i| line(if i < 50 { "old" } else { "NEW" }, i))
            .collect();
        std::fs::write(&path, &original).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut before = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true);
        let read = before
            .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        before.commit_progress(&read[0].progress_end).await.unwrap();
        let ProgressMarker::File {
            end_byte,
            file_size,
            mtime,
            content_fingerprint,
            tail_fingerprint,
            ..
        } = read[0].progress_end.clone()
        else {
            panic!("expected a file marker");
        };
        assert_eq!(end_byte, file_size, "the batch read the file to its end");
        assert!(
            tail_fingerprint.is_some(),
            "a read to the end records its tail"
        );

        // While the engine is down: rewritten past the first page, in place.
        let mut file = std::fs::File::options().write(true).open(&path).unwrap();
        file.write_all(rewrite.as_bytes()).unwrap();
        file.set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000))
            .unwrap();
        drop(file);
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);

        // Seeded from a row without the tail (before #620: the mtime is the
        // only evidence, and it moved) and from one with it (#620: the tail
        // check sees the rewritten lines).
        for (row, tail) in [("no tail", None), ("a tail", tail_fingerprint)] {
            let mut after = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
                .with_delete_quiet_period(Duration::ZERO);
            after.seed_committed_from(
                &path,
                end_byte,
                Some(inode),
                Some(file_size),
                content_fingerprint.clone(),
                tail,
            );
            after.arm_delete_from_seed(&path, Some(&mtime));
            let _ = after
                .read_batch_candidates(&src(&path), 1_000, 1 << 20, Duration::ZERO)
                .await;
            assert!(
                std::path::Path::new(&path).exists(),
                "seeded from a row with {row}: rewritten while the engine was down, and \
                 deleted on the seed's word"
            );
            assert!(std::fs::read_to_string(&path).unwrap() == rewrite);
        }
    }

    /// Review of #631 (P1): moving a file aside must never replace a file
    /// that already has the aside name - one left by a crash, or anyone
    /// else's. A plain rename replaces it silently. The seam forces the
    /// first aside name onto an existing file.
    #[tokio::test]
    async fn moving_a_file_aside_never_replaces_a_file_already_at_that_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let taken = dir.path().join(".vtop-delete.taken");
        std::fs::write(&path, "one\ntwo\n").unwrap();
        std::fs::write(&taken, "a crash left this, and it is not archived\n").unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        fs.first_aside = Some(taken.clone());
        let read = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        fs.commit_progress(&read[0].progress_end).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&taken).ok().as_deref(),
            Some("a crash left this, and it is not archived\n"),
            "the file already at the aside name was replaced"
        );
        assert!(
            !std::path::Path::new(&path).exists(),
            "the delete still went ahead, under a fresh aside name"
        );
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(".vtop-delete.taken")]);
    }

    /// #620: a fixed header longer than one page. Every file that starts with
    /// it has the same first-page fingerprint, so the head alone cannot tell
    /// one such file from another: 70 lines of 64 bytes, 4480 bytes.
    fn header() -> String {
        (0..70)
            .map(|i| format!("# header {i:03} {}\n", "-".repeat(50)))
            .collect()
    }

    /// Body lines `tag-NNNN`, 9 bytes each.
    fn body(tag: &str, lines: std::ops::Range<usize>) -> String {
        lines.map(|i| format!("{tag}-{i:04}\n")).collect()
    }

    fn all_lines(content: &str) -> Vec<String> {
        content.lines().map(str::to_owned).collect()
    }

    /// Seed a cursor from a committed marker, as startup does from the
    /// ledger's newest committed row for the path (`seed_committed_offsets`).
    fn seed_from(fs: &mut FileSource, marker: &ProgressMarker) {
        let ProgressMarker::File {
            path,
            inode,
            end_byte,
            file_size,
            content_fingerprint,
            ..
        } = marker
        else {
            panic!("not a file marker: {marker:?}");
        };
        fs.seed_committed_from(
            path,
            *end_byte,
            *inode,
            Some(*file_size),
            content_fingerprint.clone(),
            marker.tail_fingerprint().cloned(),
        );
    }

    /// The marker as a row written before #620 carries it.
    fn legacy(marker: &ProgressMarker) -> ProgressMarker {
        let mut marker = marker.clone();
        if let ProgressMarker::File {
            tail_fingerprint, ..
        } = &mut marker
        {
            *tail_fingerprint = None;
        }
        marker
    }

    async fn read_all(fs: &mut FileSource, path: &str) -> Vec<String> {
        lines(
            &fs.read_batch_candidates(&src(path), 100_000, 1 << 20, Duration::ZERO)
                .await
                .unwrap(),
        )
    }

    /// #620, case 1 (while running): the file is truncated in place and
    /// rewritten with the same first page, and has grown past the old read
    /// position by the next read. Inode, length and first page all agree
    /// with the old file; resuming at the old offset skipped every new byte
    /// below it. The bytes just below the read position are what differ.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_in_place_rewrite_that_keeps_the_first_page_is_read_from_byte_zero() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let old = header() + &body("old", 0..50);
        std::fs::write(&path, &old).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        assert_eq!(read_all(&mut fs, &path).await, all_lines(&old));

        let new = header() + &body("NEW", 0..80);
        std::fs::write(&path, &new).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        assert!(
            new.len() > old.len(),
            "the case under test regrew past the cursor"
        );
        let got = read_all(&mut fs, &path).await;
        assert_eq!(
            got.len(),
            all_lines(&new).len(),
            "the rewrite must be read from byte 0; resumed at the old offset {} it hands \
             out only from {:?}",
            old.len(),
            got.first()
        );
        assert_eq!(got, all_lines(&new));

        // And from there it is one file again: an append is a continuation.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"NEW-0080\n")
            .unwrap();
        assert_eq!(read_all(&mut fs, &path).await, ["NEW-0080"]);
    }

    /// #620, case 2 (across a restart), which is also what a reused inode
    /// looks like to the adapter: at startup the file at the path has the
    /// committed row's inode and first page and is longer than it was, but
    /// it is other content. The seed carries the fingerprint of the bytes
    /// below the committed offset, and the first read checks it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_seed_restarts_a_file_at_the_same_inode_whose_first_page_is_unchanged() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let old = header() + &body("old", 0..50);
        std::fs::write(&path, &old).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut before = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let committed = before
            .read_batch_candidates(&src(&path), 100_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap()[0]
            .progress_end
            .clone();

        // While the engine is down the file is replaced at the same inode by
        // content with the same first page, longer than the committed offset.
        let new = header() + &body("NEW", 0..80);
        std::fs::write(&path, &new).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);

        let mut after = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        seed_from(&mut after, &committed);
        let got = read_all(&mut after, &path).await;
        assert_eq!(
            got.first().map(String::as_str),
            Some(all_lines(&new)[0].as_str()),
            "the replacement must be read from byte 0, not resumed at the committed \
             offset {} of the file it replaced",
            old.len()
        );
        assert_eq!(got, all_lines(&new));
    }

    /// Reads the old file, then more than `MAX_ISSUED` one-line reads, so the
    /// first read's generation record is evicted; returns the first read's
    /// end marker (the stale commit), the newest read's end marker (the
    /// newest committed row a restart seeds from), and the cursor.
    async fn a_stale_range_and_its_evicted_record(
        path: &str,
    ) -> (ProgressMarker, ProgressMarker, FileSource) {
        let old = header() + &body("old", 0..50);
        std::fs::write(path, &old).unwrap();
        let mut fs = FileSource::new(vec![path.to_owned()], TelemetryFormat::Raw, false);
        let stale = fs
            .read_batch_candidates(&src(path), 100_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap()[0]
            .progress_end
            .clone();
        let mut newest = stale.clone();
        for i in 50..350 {
            std::fs::OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(format!("old-{i:04}\n").as_bytes())
                .unwrap();
            let read = fs
                .read_batch_candidates(&src(path), 1, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(lines(&read), [format!("old-{i:04}")]);
            newest = read[0].progress_end.clone();
        }
        (stale, newest, fs)
    }

    /// #620, case 3: a batch read before the rewrite commits after it, and
    /// its generation record was evicted (more than `MAX_ISSUED` ranges were
    /// issued since), so it settles as unknown and is judged by the file as
    /// it is now: same inode, same first page, longer than its end. Judged
    /// current, the recovered branch put the cursor at its end and adopted
    /// its identity, and the next read resumed in the middle of the new
    /// content. The bytes below its end are not the ones it read.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_late_commit_whose_record_was_evicted_cannot_resume_a_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let (stale, _, mut fs) = a_stale_range_and_its_evicted_record(&path).await;

        let new = header() + &body("NEW", 0..1000);
        std::fs::write(&path, &new).unwrap();
        fs.commit_progress(&stale).await.unwrap();
        let got = read_all(&mut fs, &path).await;
        assert_eq!(
            got.first().map(String::as_str),
            Some(all_lines(&new)[0].as_str()),
            "the rewrite must be read from byte 0; the stale commit {} moved the cursor",
            stale.range_token()
        );
        assert_eq!(got.len(), all_lines(&new).len());
    }

    /// #620, case 3 after a restart: the cursor is seeded from the newest
    /// committed row, then recovery commits an older VERIFIED batch read
    /// before the rewrite. No generation record exists after a restart.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_recovered_commit_from_before_a_rewrite_cannot_resume_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let (stale, newest, _) = a_stale_range_and_its_evicted_record(&path).await;

        let new = header() + &body("NEW", 0..1000);
        std::fs::write(&path, &new).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        seed_from(&mut fs, &newest);
        fs.commit_progress(&stale).await.unwrap();
        let got = read_all(&mut fs, &path).await;
        assert_eq!(
            got.first().map(String::as_str),
            Some(all_lines(&new)[0].as_str()),
            "the rewrite must be read from byte 0; the recovered commit {} resumed it",
            stale.range_token()
        );
        assert_eq!(got.len(), all_lines(&new).len());
    }

    /// #620 compatibility: a row written before the field carries no
    /// fingerprint of the bytes below its end. A cursor seeded from one, or
    /// a commit recovered from one, behaves exactly as before: an appended
    /// file resumes at the offset, and (the stated limit for such rows) a
    /// rewrite that keeps the first page is still taken for a continuation.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_marker_from_before_the_tail_fingerprint_still_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        let old = header() + &body("old", 0..50);
        std::fs::write(&path, &old).unwrap();
        let mut before = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let committed = legacy(
            &before
                .read_batch_candidates(&src(&path), 100_000, 1 << 20, Duration::ZERO)
                .await
                .unwrap()[0]
                .progress_end,
        );
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(body("old", 50..53).as_bytes())
            .unwrap();

        let mut seeded = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        seed_from(&mut seeded, &committed);
        assert_eq!(
            read_all(&mut seeded, &path).await,
            ["old-0050", "old-0051", "old-0052"],
            "a legacy seed resumes at its offset"
        );

        let mut recovered = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        recovered.commit_progress(&committed).await.unwrap();
        assert_eq!(
            read_all(&mut recovered, &path).await,
            ["old-0050", "old-0051", "old-0052"],
            "a legacy recovered commit moves the cursor to its end"
        );

        // Unchanged for such a row: a rewrite that keeps the first page.
        let new = header() + &body("NEW", 0..80);
        std::fs::write(&path, &new).unwrap();
        let mut seeded = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        seed_from(&mut seeded, &committed);
        assert_eq!(
            read_all(&mut seeded, &path)
                .await
                .first()
                .map(String::as_str),
            Some("NEW-0050"),
            "a legacy seed is judged by inode, length and first page, as before"
        );
    }

    /// #620: the tail a marker carries is rebuilt from the bytes the read
    /// consumed (the page below its start, then its records), not read back
    /// from the file afterwards. It must still be exactly the page of the
    /// file that ends at the marker's end - or every append would look like
    /// a rewrite. Checked against the file itself over reads that start
    /// below, across and above one page, end before a partial line, and in
    /// whole-file mode.
    #[tokio::test]
    async fn a_markers_tail_is_the_page_of_the_file_that_ends_at_its_end() {
        for whole_file in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("app.log").to_string_lossy().into_owned();
            std::fs::write(&path, "").unwrap();
            let mut fs =
                FileSource::with_mode(vec![path.clone()], TelemetryFormat::Raw, false, whole_file);
            let mut reads = 0;
            for round in 0..8usize {
                // Completes the previous round's partial line, adds lines of
                // uneven length, and leaves a new partial line.
                let mut chunk = String::from("\n");
                for i in 0..round * 23 + 1 {
                    chunk.push_str(&format!("r{round} line {i} {}\n", "x".repeat(i % 61)));
                }
                chunk.push_str(&format!("partial {round}"));
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(chunk.as_bytes())
                    .unwrap();
                let file = std::fs::read(&path).unwrap();
                loop {
                    let read = fs
                        .read_batch_candidates(&src(&path), 17, 1 << 20, Duration::ZERO)
                        .await
                        .unwrap();
                    for marker in [&read[0].progress_start, &read[0].progress_end] {
                        let ProgressMarker::File { end_byte, .. } = marker else {
                            unreachable!()
                        };
                        let expected = ContentFingerprint::of_tail(&file[..*end_byte as usize]);
                        // A file the stat proved unchanged is not opened
                        // (#100): its empty read's markers know no content,
                        // which is "unknown", never a wrong answer.
                        let skipped =
                            read[0].records.is_empty() && marker.tail_fingerprint().is_none();
                        assert!(
                            skipped || marker.tail_fingerprint() == expected.as_ref(),
                            "whole_file={whole_file} round {round}: the tail of {} is not the \
                             file's page below byte {end_byte}: {:?} != {expected:?}",
                            marker.range_token(),
                            marker.tail_fingerprint()
                        );
                    }
                    reads += 1;
                    if read[0].records.is_empty() {
                        break;
                    }
                }
            }
            // At least one read with records and one without per round.
            assert!(reads >= 16, "whole_file={whole_file}: only {reads} reads");
        }
    }

    /// #620: a range read before a rewrite that kept the first page is never
    /// replayed into the new content: its first page and length agree, the
    /// bytes below its end do not.
    #[cfg(unix)]
    #[tokio::test]
    async fn replay_refuses_a_range_from_before_a_rewrite_that_kept_the_first_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, header() + &body("old", 0..50)).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let read = fs
            .read_batch_candidates(&src(&path), 100_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        std::fs::write(&path, header() + &body("NEW", 0..80)).unwrap();
        let err = fs
            .replay_from_marker(&read[0].progress_end)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("rewritten at the same inode"), "{err}");
        // A legacy marker cannot tell, and is replayed as before.
        fs.replay_from_marker(&legacy(&read[0].progress_end))
            .await
            .unwrap();
    }

    /// Review of #632: the last identity test before `delete_after_commit`
    /// unlinks the file is a probe of its own, after `still_current`. It
    /// judged the inode and the first page only, so a rewrite that kept the
    /// first page between the two probes passed it and was deleted unread.
    #[test]
    fn the_final_delete_check_refuses_a_rewrite_that_kept_the_first_page() {
        let old = header() + &body("old", 0..50);
        let new = header() + &body("NEW", 0..80);
        let end = old.len();
        let head = |file: &str| file.as_bytes()[..ContentFingerprint::MAX_LEN].to_vec();
        let below = |file: &str| file.as_bytes()[end - ContentFingerprint::MAX_LEN..end].to_vec();
        let content = ContentFingerprint::of(old.as_bytes());
        let tail = ContentFingerprint::of_tail(old.as_bytes());
        let judge = |file: &str, tail: Option<&ContentFingerprint>| {
            delete_identity_ok(
                Some(7),
                content.as_ref(),
                tail,
                Some(7),
                &head(file),
                &below(file),
            )
        };
        assert!(judge(&old, tail.as_ref()), "the batch's own file");
        assert!(
            !judge(&new, tail.as_ref()),
            "rewritten below the batch's end, same first page: must not be deleted"
        );
        assert!(
            judge(&new, None),
            "a legacy marker is judged by inode and first page, as before"
        );
    }

    /// Review of #632: a replay refused because the bytes below the range's
    /// end changed must not report where the rewind "landed" for that range:
    /// recovery would take the range as handled at the cursor's position in
    /// content it was never read from.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_range_whose_tail_is_gone_has_no_read_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, header() + &body("old", 0..50)).unwrap();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let old = fs
            .read_batch_candidates(&src(&path), 100_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap()[0]
            .progress_end
            .clone();
        assert!(fs.read_position(&old).is_some(), "the cursor's own file");

        std::fs::write(&path, header() + &body("NEW", 0..80)).unwrap();
        assert!(fs.replay_from_marker(&old).await.is_err());
        assert_eq!(
            fs.read_position(&old),
            None,
            "the range's bytes are no longer at the path; its position is unknown"
        );
        // A legacy marker cannot tell, and is answered as before.
        assert!(fs.read_position(&legacy(&old)).is_some());
    }

    /// Review of #632: reading the bytes under the cursor's tail can fail.
    /// It used to happen before the inode, length and first-page tests ran,
    /// so a rotation those tests had found was dropped with the error: the
    /// cursor kept the old position and the old identity. Here the tail's
    /// range cannot even be sought to (past `i64::MAX`), and the file at the
    /// path is another inode.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_tail_read_does_not_lose_a_rotation_already_found() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log").to_string_lossy().into_owned();
        std::fs::write(&path, "new one\nnew two\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        fs.seed_committed_from(
            &path,
            u64::MAX,
            Some(inode.wrapping_add(1)),
            Some(16),
            None,
            ContentFingerprint::of(b"x"),
        );
        let reads = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .expect("the rotation is decided before the tail is read");
        assert_eq!(lines(&reads), ["new one", "new two"]);
    }

    /// Review of #632: the tail is one page, but the buffer it was hashed
    /// from held every record that reached into that page in full - a
    /// whole-file record or a long line up to the batch limit, copied to
    /// fingerprint 4 KiB of it. The window is a page; its hash is unchanged.
    #[test]
    fn the_tail_window_is_one_page_whatever_the_record_size() {
        let big = vec![b'a'; 1 << 20];
        for records in [
            vec![b"short".to_vec(), big.clone()],
            vec![big.clone(), b"short".to_vec()],
            vec![b"one".to_vec(), b"two".to_vec()],
            vec![],
        ] {
            for newline_terminated in [true, false] {
                let below = vec![b'b'; ContentFingerprint::MAX_LEN];
                let window = tail_window(&below, &records, newline_terminated);
                let mut read = below.clone();
                for record in &records {
                    read.extend_from_slice(record);
                    if newline_terminated {
                        read.push(b'\n');
                    }
                }
                assert!(
                    window.len() <= ContentFingerprint::MAX_LEN,
                    "{} bytes copied to fingerprint one page",
                    window.len()
                );
                assert_eq!(
                    ContentFingerprint::of(&window),
                    ContentFingerprint::of_tail(&read)
                );
            }
        }
    }
}
