//! File source adapter.
//!
//! Reads append-only log files line by line, tracking byte offsets, file
//! identity (inode), size, and mtime. Resumes from the last committed byte and
//! never deletes source files unless explicitly configured, only after the
//! batch is committed, and only once the file has gone a quiet period without
//! a write (#619, see [`DeleteCandidate`]).

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
};
use crate::continuity::{self, Discontinuity, Observed};
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
/// [`FileSource::seed_committed_from`]). With a zero quiet period the commit
/// that covers the file deletes it at once.
#[derive(Debug, Clone)]
struct DeleteCandidate {
    /// The identity the committed range was read from.
    inode: Option<u64>,
    content: Option<ContentFingerprint>,
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
    /// The same file, fully covered, but its mtime moved and nothing else
    /// did (a producer stamping a finished drop, a `touch`). Not a write, so
    /// not a reason to give up the delete; but something touched it, so the
    /// wait starts again from now (review of #631).
    Touched(Option<SystemTime>),
    /// Another file, or bytes the committed range does not cover.
    Different,
}

impl DeleteCandidate {
    /// How the file (metadata and first bytes, from ONE descriptor) compares
    /// with what this candidate recorded.
    fn compare(&self, md: &std::fs::Metadata, head: &[u8]) -> Compared {
        // EQUALITY on the length: an append-only file under a committed
        // range is never shorter than the range, and a longer one has bytes
        // nobody read.
        if !is_the_file_read(self.inode, self.content.as_ref(), md, head)
            || md.len() != self.end_byte
        {
            return Compared::Different;
        }
        let mtime = md.modified().ok();
        if mtime == self.mtime {
            Compared::Unchanged
        } else {
            Compared::Touched(mtime)
        }
    }
}

/// Whether the file now open (metadata and first bytes, from ONE descriptor)
/// is the one a committed batch read: the same inode, and the first page the
/// batch fingerprinted. The ONE identity test every `delete_after_commit`
/// decision goes through - the covering commit, the delete decision, and the
/// check after the file is moved aside - so a stronger identity (a new
/// fingerprint) is added here once and guards all three.
fn is_the_file_read(
    inode: Option<u64>,
    content: Option<&ContentFingerprint>,
    md: &std::fs::Metadata,
    head: &[u8],
) -> bool {
    let same_inode = match (inode, inode_of(md)) {
        (Some(recorded), Some(current)) => recorded == current,
        // A platform without inodes: the length and mtime still decide.
        (None, None) => true,
        _ => false,
    };
    same_inode && content.is_none_or(|content| content.matches_head(head))
}

/// What became of one attempt to delete a candidate.
enum DeleteAttempt {
    Deleted,
    /// Left in place because the file is no longer what the candidate
    /// recorded, or is gone, so the candidate can never qualify again.
    Changed(&'static str),
    /// Left in place because only its mtime moved: the candidate stands with
    /// the new mtime, and its wait starts again.
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
    /// A crash after the rename leaves the file under the hidden name
    /// (`.<name>.<nanos>.vtop-delete`), never deleted. It is fully archived
    /// unless a write landed after the decision to delete it: one the crash
    /// kept from being seen and put back, or one through a held descriptor
    /// after the second check (the window above). If a configured pattern
    /// matches that name it is discovered and archived again: a duplicate of
    /// the archived bytes, plus any such write.
    fn run(self) -> DeleteAttempt {
        let path = Path::new(&self.path);
        let candidate = &self.candidate;
        let Some((md, head)) = continuity::probe(path) else {
            // GONE is not a failure to retry (review of #631): a candidate
            // kept for a vanished file outlives it, and would be held up
            // against whatever is created at that name next.
            return match path.try_exists() {
                Ok(false) => DeleteAttempt::Changed("the file is gone"),
                _ => DeleteAttempt::Failed("the file could not be opened".to_owned()),
            };
        };
        match candidate.compare(&md, &head) {
            Compared::Unchanged => {}
            Compared::Touched(mtime) => return DeleteAttempt::Touched(mtime),
            Compared::Different => {
                return DeleteAttempt::Changed(
                    "the file changed since the commit that covered it \
                     (written, rotated, truncated or replaced)",
                )
            }
        }
        #[cfg(test)]
        if let Some(hook) = &self.hook {
            hook(path);
        }
        let Some(aside) = aside_name(path) else {
            return DeleteAttempt::Changed("the path names no file to move aside");
        };
        if let Err(error) = std::fs::rename(path, &aside) {
            // On Windows, also what a writer that still has it open causes.
            return DeleteAttempt::Failed(format!("the file could not be moved aside: {error}"));
        }
        let Some((md, head)) = continuity::probe(&aside) else {
            // Nothing says it changed; nor can it be checked. Kept, as a
            // failed check always is, and tried again.
            put_back(&aside, path);
            return DeleteAttempt::Failed(
                "the file could not be opened again after it was moved aside".to_owned(),
            );
        };
        match candidate.compare(&md, &head) {
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
            delete_quiet: DEFAULT_DELETE_QUIET,
            #[cfg(test)]
            before_delete: None,
        }
    }

    /// How long a fully archived file must stay unwritten before
    /// `delete_after_commit` removes it (#619). Zero deletes as soon as a
    /// commit covers the file, which is safe only when nothing can still be
    /// writing to it.
    pub fn with_delete_quiet_period(mut self, quiet: Duration) -> Self {
        self.delete_quiet = quiet;
        self
    }

    /// Seed a committed byte offset (used by the replay engine on startup).
    pub fn seed_committed(&mut self, path: &str, committed_byte: u64) {
        let c = self.cursors.entry(path.to_string()).or_default();
        c.committed_byte = committed_byte;
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
    pub fn seed_committed_from(
        &mut self,
        path: &str,
        committed_byte: u64,
        inode: Option<u64>,
        observed_size: Option<u64>,
        content: Option<ContentFingerprint>,
    ) {
        self.seed_committed(path, committed_byte);
        if inode.is_some() || observed_size.is_some() || content.is_some() {
            // The size feeds the truncation test; the empty mtime keeps
            // `can_skip` false, so a seeded cursor always reads first.
            self.cursors.get_mut(path).expect("seeded above").seen = Some(FileIdentity {
                inode,
                file_size: observed_size.unwrap_or(0),
                mtime: String::new(),
                content: content.clone(),
            });
        }
        // A file the engine had fully archived, but not yet deleted, before a
        // restart (#619): the deferred delete was in memory, and without this
        // the file would stay forever, since nothing new is ever committed for
        // it. The quiet period starts again here, and the delete itself checks
        // the file against this identity as it would a commit's. Only a seed
        // that names both the inode and the first page qualifies: a legacy row
        // cannot tell this file from one that replaced it while the engine was
        // down. And only one whose batch saw its file END at the committed
        // offset (review of #631): a batch that read part of a longer file
        // archived only that part, and a same-inode rewrite cut back to the
        // offset with the first page kept would pass every other test here.
        // The read's continuity check catches that rewrite by the length the
        // batch saw, but a delete armed now could run before any read.
        if self.delete_after_commit
            && inode.is_some()
            && content.is_some()
            && observed_size == Some(committed_byte)
        {
            if let Ok(md) = std::fs::metadata(path) {
                if md.len() == committed_byte {
                    self.cursors
                        .get_mut(path)
                        .expect("seeded above")
                        .delete_candidate = Some(DeleteCandidate {
                        inode,
                        content,
                        end_byte: committed_byte,
                        mtime: md.modified().ok(),
                        since: Instant::now(),
                    });
                }
            }
        }
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
                // must not outlive it (#558).
                self.cursors.remove(path);
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
                // Only the mtime moved. A file at its end yields no batch, so
                // no new covering commit would ever record a candidate again:
                // the wait restarts on this one instead (review of #631).
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
                    "delete-after-commit: file kept; only its mtime changed, so the quiet \
                     period starts again"
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

    /// Stat a path without opening it, for the skip decision. A path that
    /// cannot be stat'd is NOT skipped: it is handed to the read, which
    /// reports the error properly instead of having it swallowed here as a
    /// silent skip.
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
    fn probe(path: &Path) -> (Option<u64>, u64, Vec<u8>) {
        match continuity::probe(path) {
            Some((md, head)) => (inode_of(&md), md.len(), head),
            None => (None, 0, Vec::new()),
        }
    }

    /// Marker from a descriptor-derived identity: the inode/size/mtime are of
    /// the file whose BYTES were read, so a rotation between read and marker
    /// construction cannot mix the old file's offsets with the new file's
    /// identity (#65) — which previously let `delete_after_commit` match and
    /// delete the replacement.
    fn marker_from(path: &str, id: &FileIdentity, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::File {
            path: path.to_string(),
            inode: id.inode,
            start_byte: start,
            end_byte: end,
            file_size: id.file_size,
            mtime: id.mtime.clone(),
            content_fingerprint: id.content.clone(),
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
        } = slice;
        let cursor = self.cursors.entry(path.to_string()).or_default();
        if let Some(why) = discontinuity {
            tracing::warn!(path, previous_read_byte = cursor.read_byte, "{why}");
            cursor.committed_byte = 0;
            cursor.generations.discontinuity();
            cursor.delete_candidate = None;
        }
        cursor.generations.issue(start, end);
        cursor.read_byte = end;
        // What the read ACTUALLY observed, not what a stat saw before it: the
        // file can grow between the two, and recording the earlier identity
        // would let the next cycle skip bytes this one never read.
        cursor.seen = Some(identity.clone());
        ReadResult {
            progress_start: Self::marker_from(path, &identity, start, start),
            progress_end: Self::marker_from(path, &identity, start, end),
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
            tracing::warn!(
                path,
                previous_read_byte = cursor.read_byte,
                "{why}; the read of the new content then failed"
            );
            cursor.committed_byte = 0;
            cursor.read_byte = 0;
            cursor.generations.discontinuity();
            cursor.delete_candidate = None;
            cursor.seen = Some(opened);
        }
        failed.error
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
    async fn read_slice(
        path: String,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
        last_seen: Option<Observed>,
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
        let discontinuity = continuity::check(
            last_seen.as_ref(),
            opened.inode,
            opened.file_size,
            &head,
            start,
        );
        let start = if discontinuity.is_some() { 0 } else { start };
        // From here a failure still reports what the judgement found: a read
        // that saw the file truncated and then failed (a first record over
        // max_bytes, say) must restart the cursor all the same, or the file
        // can regrow past its old length before the next read and the
        // truncation is never seen (review of #564).
        let restart = discontinuity.map(|why| Box::new((why, opened.clone())));
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
        })
    }
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
}

impl FileIdentity {
    /// What the next read is judged against.
    fn observed(&self) -> Observed {
        Observed {
            inode: self.inode,
            size: self.file_size,
            content: self.content.clone(),
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
/// (#619). The clock makes it unique per attempt, so a name left by a crash
/// is never overwritten by a later attempt.
fn aside_name(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut aside = std::ffi::OsString::from(".");
    aside.push(name);
    aside.push(format!(".{nanos}.vtop-delete"));
    Some(path.with_file_name(aside))
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
}

enum PassOutcome {
    SkippedUnchanged(FileIdentity),
    Read(Result<SliceRead, SliceError>),
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
        let cursor = self.cursors.entry(path.clone()).or_default();
        let start = cursor.read_byte;
        // Cheapest signal first (#100): an unchanged, fully-read file is
        // skipped without an open. The marker still reports the cursor where
        // it stands, so a skipped file is indistinguishable from one that was
        // read and had nothing to give — which is what it is.
        if let Some(current) = Self::stat_identity(&path) {
            if Self::can_skip(cursor, &current) {
                return Ok(vec![ReadResult {
                    progress_start: Self::marker_from(&path, &current, start, start),
                    progress_end: Self::marker_from(&path, &current, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]);
            }
        }
        let last_seen = self
            .cursors
            .get(&path)
            .and_then(|c| c.seen.as_ref())
            .map(FileIdentity::observed);
        match Self::read_slice(
            path.clone(),
            start,
            max_records,
            max_bytes,
            self.whole_file,
            last_seen,
        )
        .await
        {
            Ok(slice) => Ok(vec![self.apply_slice(&path, slice)]),
            Err(failed) => Err(self.apply_failed_read(&path, failed)),
        }
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
        let jobs: Vec<(usize, String, u64, Option<FileIdentity>)> = sources
            .iter()
            .enumerate()
            .filter(|(i, _)| !deleted.contains(i))
            .map(|(i, s)| {
                let path = s.source_name.clone();
                let cursor = self.cursors.entry(path.clone()).or_default();
                (i, path, cursor.read_byte, cursor.seen.clone())
            })
            .collect();

        let mut results: Vec<(usize, String, u64, PassOutcome)> =
            futures::stream::iter(jobs.into_iter().map(|(i, path, start, seen)| async move {
                // A path that cannot be stat'd is NOT skipped: it goes to
                // the read, which reports the error properly instead of
                // having it swallowed here as a silent skip.
                let current = tokio::fs::metadata(&path)
                    .await
                    .ok()
                    .map(|md| Self::identity_of(&md));
                let snapshot = FileCursor {
                    read_byte: start,
                    seen,
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
                                path.clone(),
                                start,
                                max_records,
                                max_bytes,
                                whole_file,
                                last_seen,
                            )
                            .await,
                        )
                    }
                };
                (i, path, start, outcome)
            }))
            .buffer_unordered(FILE_READ_CONCURRENCY)
            .collect()
            .await;
        results.extend(
            deleted
                .into_iter()
                .map(|i| (i, sources[i].source_name.clone(), 0, PassOutcome::Deleted)),
        );
        // buffer_unordered completes in I/O order; report in source order.
        results.sort_by_key(|(i, ..)| *i);
        // COUNTED, not silent. The issue asks for deferred work to be visible,
        // and a cycle that skipped nine files out of ten is a very different
        // cycle from one that read all ten and found them empty — they look
        // identical in every other metric.
        let skipped_count = results
            .iter()
            .filter(|(_, _, _, outcome)| matches!(outcome, PassOutcome::SkippedUnchanged(_)))
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
        for (source_index, path, start, outcome) in results {
            let result = match outcome {
                PassOutcome::SkippedUnchanged(id) => Ok(vec![ReadResult {
                    progress_start: Self::marker_from(&path, &id, start, start),
                    progress_end: Self::marker_from(&path, &id, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]),
                PassOutcome::Read(Ok(slice)) => {
                    any_records |= !slice.records.is_empty();
                    Ok(vec![self.apply_slice(&path, slice)])
                }
                PassOutcome::Read(Err(failed)) => {
                    any_failed = true;
                    Err(self.apply_failed_read(&path, failed))
                }
                PassOutcome::Deleted => Ok(Vec::new()),
            };
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
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        // A batch read BEFORE a rotation or truncation commits after the
        // cursor has already restarted on the new content. Moving the cursor
        // to its offset would carry it past bytes of a file that were never
        // read. The batch is archived and its ledger row commits; only the
        // cursor stays put (#558). A replacement at the same inode is told
        // apart by its first bytes (#578).
        let (current_inode, current_size, current_head) = Self::probe(Path::new(path));
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
            );
        if still_current {
            let c = self.cursors.entry(path.clone()).or_default();
            c.committed_byte = *end_byte;
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
                // re-read, never a skip.
                let prior = c.seen.as_ref();
                let inode = (*marker_inode)
                    .or(prior.and_then(|seen| seen.inode))
                    .or(current_inode);
                let kept = prior
                    .filter(|seen| seen.inode == inode)
                    .and_then(|seen| seen.content.clone());
                c.seen = Some(FileIdentity {
                    inode,
                    file_size: *marker_file_size,
                    mtime: String::new(),
                    content: marker_content.clone().or(kept),
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
            let probed = continuity::probe(Path::new(path));
            let size = probed.as_ref().map_or(0, |(md, _)| md.len());
            // A file that can no longer be opened is not the one read either.
            let identity_ok = probed.as_ref().is_some_and(|(md, head)| {
                is_the_file_read(*marker_inode, marker_content.as_ref(), md, head)
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
                    end_byte: *end_byte,
                    mtime: probed.as_ref().and_then(|(md, _)| md.modified().ok()),
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
            inode: marker_inode,
            file_size: size_when_read,
            content_fingerprint: marker_content,
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
        // engine reports that the range will not be re-read (#558).
        let (current_inode, current_size, current_head) = Self::probe(Path::new(path));
        if let Some(marker) = marker_inode {
            if current_inode.is_some() && current_inode != Some(*marker) {
                return Err(VtopError::Source(format!(
                    "cannot replay file {path}: file identity changed (rotation/replacement)"
                )));
            }
        }
        // The marker records how long the file was when the range was read,
        // and an append-only file only grows. A file now SHORTER than that has
        // been truncated, even when the range starts early enough to fit in
        // what is left: replaying from byte 0 would archive replacement bytes
        // as though they were the range that was lost.
        if *start_byte > current_size || current_size < *size_when_read {
            return Err(VtopError::Source(format!(
                "cannot replay file {path} at byte {start_byte}: file length is {current_size} \
                 (truncated; it was {size_when_read} when this range was read)"
            )));
        }
        // The same inode and no shorter, but other first bytes: a
        // replacement that reused the inode (#578). Its offsets are not the
        // range's either.
        if let Some(content) = marker_content {
            if !content.matches_head(&current_head) {
                return Err(VtopError::Source(format!(
                    "cannot replay file {path}: its first {} bytes are not the ones this \
                     range was read after (replaced at the same inode)",
                    content.len
                )));
            }
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
            content_fingerprint: marker_content,
            ..
        } = marker
        else {
            return None;
        };
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
        Some(i128::from(c.read_byte))
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
        with_size.seed_committed_from(&path, 12, Some(inode), Some(60), None);
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
        without.seed_committed_from(&path, 12, Some(inode), None, None);
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
        fs.seed_committed_from(&path, 60, Some(inode), Some(60), None);
        // The newer generation's VERIFIED batch: bytes 0..21 of a 21-byte file.
        let recovered = ProgressMarker::File {
            path: path.clone(),
            inode: Some(inode),
            start_byte: 0,
            end_byte: 21,
            file_size: 21,
            mtime: String::new(),
            content_fingerprint: None,
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
        fs.seed_committed_from(&path, 60, Some(inode), Some(60), None);
        let recovered = ProgressMarker::File {
            path: path.clone(),
            inode: None,
            start_byte: 0,
            end_byte: 21,
            file_size: 21,
            mtime: String::new(),
            content_fingerprint: None,
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
        same.seed_committed_from(&path, 8, Some(inode), None, None);
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
        replaced.seed_committed_from(&path, 8, Some(inode + 1_000_000), None, None);
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
        legacy.seed_committed_from(&path, 8, None, None, None);
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
        seeded.seed_committed_from(&path, 16, Some(inode), Some(16), old);
        let reads = seeded
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(lines(&reads), ["NEW one", "NEW two", "NEW three"]);

        let mut legacy = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        legacy.seed_committed_from(&path, 16, Some(inode), Some(16), None);
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
            );
            let recovered = ProgressMarker::File {
                path: path.clone(),
                inode: Some(inode),
                start_byte: 0,
                end_byte: end,
                file_size: b.len() as u64,
                mtime: String::new(),
                content_fingerprint: ContentFingerprint::of(b.as_bytes()),
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
            fs.seed_committed_from(&path, seed, Some(inode), Some(84), None);

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
            content_fingerprint,
            ..
        } = read[0].progress_end.clone()
        else {
            panic!("expected a file marker");
        };

        let quiet = Duration::from_millis(50);
        let mut legacy = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        legacy.seed_committed_from(&path, end_byte, inode, Some(file_size), None);
        let mut after = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(quiet);
        after.seed_committed_from(&path, end_byte, inode, Some(file_size), content_fingerprint);
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
        std::fs::write(&path, &rewrite).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);

        // The newest committed row read [.., 8200) of a file it saw at 16400.
        let mut after = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, true)
            .with_delete_quiet_period(Duration::ZERO);
        after.seed_committed_from(&path, committed, Some(inode), Some(16_400), content);
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

    /// Review of #631: a change to the mtime alone - same inode, first page
    /// and length, as when a producer stamps a finished drop - is not a
    /// write. Dropping the candidate for it kept the file forever, since a
    /// file at its end yields no batch and so no new covering commit. The
    /// wait starts again instead, and the file goes once it has stayed quiet.
    #[tokio::test]
    async fn an_mtime_only_change_restarts_the_wait_instead_of_cancelling_the_delete() {
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

        let stamped = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(stamped)
            .unwrap();
        std::thread::sleep(quiet * 2);
        fs.read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "its mtime changed, so the wait starts again: not deleted yet"
        );

        std::thread::sleep(quiet * 2);
        let reads = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(reads.is_empty(), "a deleted file has nothing to read");
        assert!(
            !std::path::Path::new(&path).exists(),
            "unchanged for the quiet period after its mtime was stamped, so deleted"
        );
    }
}
