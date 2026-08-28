//! File source adapter.
//!
//! Reads append-only log files line by line, tracking byte offsets, file
//! identity (inode), size, and mtime. Resumes from the last committed byte and
//! never deletes source files unless explicitly configured and only after the
//! batch is committed.

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use vtop_core::errors::VtopError;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

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
}

pub struct FileSource {
    paths: Vec<String>,
    format: TelemetryFormat,
    delete_after_commit: bool,
    /// Read each file as a single whole-file record (for binary / compressed
    /// source files that have no line structure) instead of line by line.
    whole_file: bool,
    cursors: HashMap<String, FileCursor>,
    /// Alias groups already warned about (#378), keyed by (device, inode),
    /// so a persistent hard link warns once per process instead of once per
    /// discovery cycle. A sync Mutex because `discover_sources` takes
    /// `&self`; it is locked briefly and never across an await.
    warned_aliases: std::sync::Mutex<std::collections::HashSet<(u64, u64)>>,
}

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
            warned_aliases: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Seed a committed byte offset (used by the replay engine on startup).
    pub fn seed_committed(&mut self, path: &str, committed_byte: u64) {
        let c = self.cursors.entry(path.to_string()).or_default();
        c.committed_byte = committed_byte;
        c.read_byte = committed_byte;
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
        }
    }

    /// Identity of whatever the PATH names right now. Only for comparing the
    /// current file against a previously recorded marker (the delete guard in
    /// `commit_progress`) — never for building a marker, which must describe
    /// the file actually READ (#65): use the open descriptor's identity.
    fn file_identity(path: &Path) -> (Option<u64>, u64, String) {
        match std::fs::metadata(path) {
            Ok(md) => {
                let id = Self::identity_of(&md);
                (id.inode, id.file_size, id.mtime)
            }
            Err(_) => (None, 0, String::new()),
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
        }
    }

    /// Read one file from `start`, honouring the budgets. Pure with respect to
    /// the adapter (no `&self`): takes everything it needs by value so many
    /// files can be read CONCURRENTLY in one pass (#96 B2) — each file's
    /// cursor is snapshotted before and applied after, so concurrent reads
    /// never touch shared state.
    ///
    /// Returns `(records, end_pos, verbatim)`.
    async fn read_slice(
        path: String,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
    ) -> Result<(Vec<Vec<u8>>, u64, bool, FileIdentity), VtopError> {
        // Whole-file mode: read the entire remaining file as one opaque record.
        // Used for binary / already-compressed source files with no line
        // structure. The whole file commits as a single byte range.
        if whole_file {
            // Read AND fingerprint through one open descriptor: a rotation
            // between "read the bytes" and "stat the path" would otherwise mix
            // the old file's offsets with the NEW file's identity (#65).
            let mut file = tokio::fs::File::open(&path).await?;
            let md = file.metadata().await?; // fstat: the opened file, not the path
            let fsize = md.len();
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
            return Ok((records, end, true, identity));
        }

        let mut file = tokio::fs::File::open(&path).await?;
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
        Ok((records, pos, false, identity))
    }
}

/// Inode / size / mtime of the file a read actually consumed, taken from the
/// OPEN descriptor (`fstat`), never from a second path lookup.
#[derive(Debug, Clone)]
struct FileIdentity {
    inode: Option<u64>,
    file_size: u64,
    mtime: String,
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
        // spelling question and are not handled here (#378). They ask whether
        // two directory entries for one inode are one source or two, which has
        // a defensible answer either way, and the answer has to be paired with
        // a cursor identity that survives `delete_after_commit` removing one
        // of the aliases. `canonicalize` looks like it settles this and does
        // not: it merges symlinks, cannot see hard links at all, and leaves
        // the deletion of an alias silently re-reading the survivor from zero.
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
        // Two surviving entries for ONE inode are archived as two sources.
        // Whether they SHOULD be one is #378's open question, deliberately
        // unanswered here — but doing it silently is the wrong way to do it,
        // whichever answer wins. Warned once per alias group per process:
        // the duplication is either intended or a config surprise, and both
        // deserve exactly one line, not one per cycle.
        for (id, group) in alias_groups(&identities) {
            if self
                .warned_aliases
                .lock()
                .expect("alias set lock poisoned")
                .insert(id)
            {
                tracing::warn!(
                    paths = ?group,
                    "two directory entries name one file; each is archived as \
                     its own source, so its records are archived twice (#378)"
                );
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
        let (records, end, verbatim, id) =
            Self::read_slice(path.clone(), start, max_records, max_bytes, self.whole_file).await?;
        let cursor = self.cursors.get_mut(&path).unwrap();
        cursor.read_byte = end;
        cursor.seen = Some(id.clone());
        Ok(vec![ReadResult {
            progress_start: Self::marker_from(&path, &id, start, start),
            progress_end: Self::marker_from(&path, &id, start, end),
            records,
            first_timestamp: None,
            last_timestamp: None,
            verbatim,
        }])
    }

    /// Read every file CONCURRENTLY (#96 B2). Each file's read is independent
    /// — its own handle, its own snapshotted cursor — so disk I/O overlaps
    /// instead of queueing behind the slowest file. Cursor updates are applied
    /// serially after the joins, keeping all shared state on this thread.
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
        // STAT BEFORE READ (#100). A directory of rotated logs is mostly cold,
        // and a stat costs a fraction of an open-seek-read. Files that have
        // not moved since this cursor caught up with them are separated out
        // here and never opened; what is left is the work the cycle actually
        // has to do.
        let mut jobs: Vec<(usize, String, u64)> = Vec::new();
        let mut skipped: Vec<(usize, String, u64, FileIdentity)> = Vec::new();
        for (i, s) in sources.iter().enumerate() {
            let path = s.source_name.clone();
            let cursor = self.cursors.entry(path.clone()).or_default();
            let start = cursor.read_byte;
            match Self::stat_identity(&path) {
                Some(current) if Self::can_skip(cursor, &current) => {
                    skipped.push((i, path, start, current));
                }
                _ => jobs.push((i, path, start)),
            }
        }
        // COUNTED, not silent. The issue asks for deferred work to be visible,
        // and a cycle that skipped nine files out of ten is a very different
        // cycle from one that read all ten and found them empty — they look
        // identical in every other metric.
        let skipped_count = skipped.len();
        if skipped_count > 0 {
            tracing::debug!(
                skipped = skipped_count,
                read = jobs.len(),
                "file_cycle_skipped_unchanged"
            );
        }

        let mut results: Vec<(
            usize,
            String,
            u64,
            Result<(Vec<Vec<u8>>, u64, bool, FileIdentity), VtopError>,
        )> = futures::stream::iter(jobs.into_iter().map(|(i, path, start)| async move {
            let res =
                Self::read_slice(path.clone(), start, max_records, max_bytes, whole_file).await;
            (i, path, start, res)
        }))
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .collect()
        .await;
        // buffer_unordered completes in I/O order; report in source order.
        results.sort_by_key(|(i, ..)| *i);

        let mut report = AdapterReadReport {
            outcomes: Vec::with_capacity(results.len()),
            productive_ms: 0,
            empty_ms: 0,
            failed_ms: 0,
        };
        let mut any_records = false;
        let mut any_failed = false;
        // Skipped files report an empty read at the cursor they already hold,
        // so the caller sees one outcome per source however the cycle got
        // there. Pushed first and sorted back into source order at the end.
        for (source_index, path, start, id) in skipped {
            report.outcomes.push(SourceReadOutcome {
                source_index,
                result: Ok(vec![ReadResult {
                    progress_start: Self::marker_from(&path, &id, start, start),
                    progress_end: Self::marker_from(&path, &id, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]),
            });
        }
        for (source_index, path, start, res) in results {
            let result = match res {
                Ok((records, end, verbatim, id)) => {
                    let cursor = self.cursors.get_mut(&path).unwrap();
                    cursor.read_byte = end;
                    // What the read ACTUALLY observed, not what the stat saw
                    // before it: the file can grow between the two, and
                    // recording the earlier identity would let the next cycle
                    // skip bytes this one never read.
                    cursor.seen = Some(id.clone());
                    any_records |= !records.is_empty();
                    Ok(vec![ReadResult {
                        progress_start: Self::marker_from(&path, &id, start, start),
                        progress_end: Self::marker_from(&path, &id, start, end),
                        records,
                        first_timestamp: None,
                        last_timestamp: None,
                        verbatim,
                    }])
                }
                Err(e) => {
                    any_failed = true;
                    Err(e)
                }
            };
            report.outcomes.push(SourceReadOutcome {
                source_index,
                result,
            });
        }
        // ONE ORDER OUT, whatever order the work happened in. Skipped files
        // were appended before the read results, so without this the caller
        // sees the cold files first and the hot ones after — source order is
        // part of this report's contract and a skip must not change it.
        report.outcomes.sort_by_key(|o| o.source_index);
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
            end_byte,
            inode: marker_inode,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        let c = self.cursors.entry(path.clone()).or_default();
        c.committed_byte = *end_byte;
        if c.read_byte < *end_byte {
            c.read_byte = *end_byte;
        }
        tracing::info!(path, end_byte, "file source progress committed");

        if self.delete_after_commit {
            // Validate file identity before the irreversible delete: if the path
            // now points to a different inode than the batch's marker (rotation
            // / replacement), deleting would destroy an unrelated/newer file.
            let (cur_inode, size, _) = Self::file_identity(Path::new(path));
            let identity_ok = match (marker_inode, cur_inode) {
                (Some(m), Some(cur)) => *m == cur,
                (None, None) => true, // platform without inode support: size-only
                _ => false,
            };
            if !identity_ok {
                tracing::warn!(
                    path,
                    "skipping delete-after-commit: file identity changed (possible rotation/replacement)"
                );
            } else if *end_byte >= size {
                let _ = std::fs::remove_file(path);
            }
        }
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::File {
            path, start_byte, ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        // Rewind the read head to the *start* of the uncommitted range so the
        // data is reprocessed. Never moves the committed point forward.
        let c = self.cursors.entry(path.clone()).or_default();
        c.read_byte = (*start_byte).max(c.committed_byte);
        tracing::warn!(
            path,
            read_byte = c.read_byte,
            "file source rewound for replay"
        );
        Ok(())
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

    /// A symlink and its target are still TWO sources, and `..` still does not
    /// merge — both on purpose (#376, followed up by #378).
    ///
    /// This pins a deliberate non-behaviour, which is worth a test precisely
    /// because it looks like an oversight. Deduplicating by inode would merge
    /// them, and `canonicalize` makes it a one-liner — but it is not a
    /// spelling question, it is "are two directory entries for one inode one
    /// source or two", and the answer has to be paired with a cursor identity
    /// that survives `delete_after_commit` removing one of the aliases. With
    /// the symlink deduplicated and deleted, the next discovery surfaces the
    /// target under a different `source_name`, finds no cursor, and re-reads
    /// the file from zero — #376 re-entered through its own fix.
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
            "a symlink and its target are two directory entries and stay two sources \
             until #378 decides otherwise WITH a cursor identity to match: {symlinked:?}"
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

    /// The alias DETECTION for #378: while the merge question stays open,
    /// two directory entries for one inode must at least be named, because
    /// silent double-archiving is wrong under either eventual answer.
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

        let mut fs = FileSource::new(vec![spath.clone()], TelemetryFormat::Raw, true);
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
        let mut fs = FileSource::with_mode(vec![path.clone()], TelemetryFormat::Raw, true, false);
        // Marker carries a stale inode (as if the file rotated since the read);
        // committing must NOT delete the now-different file on disk.
        let marker = ProgressMarker::File {
            path: path.clone(),
            inode: Some(u64::MAX),
            start_byte: 0,
            end_byte: 2,
            file_size: 2,
            mtime: String::new(),
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
}
