//! Archive coverage audit (#560).
//!
//! `verify-manifest` checks one named manifest. Nothing checked the archive as
//! a whole: that the ranges the ledger says were committed TILE each source -
//! no bytes or offsets between two committed ranges that nothing archived -
//! and that a range that failed was later covered by one that committed. Both
//! data-loss bugs found in review (#552: a failed range below a later commit;
//! #558: a rotated file resumed at the old file's offset) leave exactly that
//! shape in the ledger, and neither was visible to any check.
//!
//! The audit reads the ledger (and, for rows pruned since, the digests the
//! prune wrote before deleting them), groups committed ranges by SOURCE UNIT
//! (one Kafka topic-partition under one consumer group, one file or spool
//! file at one inode) and reports per unit:
//!
//! - **holes**: stretches between the first and last committed byte/offset
//!   that no committed range covers. A hole a FAILED or REPLAY_REQUIRED row
//!   names is a range that failed and was never re-archived. A hole nothing
//!   names is unexplained: for a file or spool that is lost data (bytes are
//!   contiguous); for Kafka it may be compaction or transaction markers,
//!   which occupy offsets but carry no records, so it is reported apart and
//!   is not a failure.
//! - **pending**: failed or in-flight ranges at or above the last committed
//!   point - the source will read them again, so they are not holes yet.
//! - **duplicates**: bytes/offsets archived more than once. Expected under
//!   at-least-once delivery, and counted so a sudden rise is visible.
//!
//! What it cannot see: anything before a unit's first committed range (a
//! tail started mid-file, a group that began at a later offset, history
//! pruned without a digest), and whether an object the ledger names still
//! exists - `--check-storage` asks the store about each one.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType};
use vtop_state::{BatchRecord, StateStore};

/// One source unit: the scope within which committed ranges must tile.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum Unit {
    Kafka {
        topic: String,
        partition: i32,
        consumer_group: String,
    },
    File {
        path: String,
        inode: Option<u64>,
    },
    SyslogSpool {
        path: String,
        inode: Option<u64>,
    },
}

impl Unit {
    fn source_type(&self) -> SourceType {
        match self {
            Unit::Kafka { .. } => SourceType::Kafka,
            Unit::File { .. } => SourceType::File,
            Unit::SyslogSpool { .. } => SourceType::SyslogSpool,
        }
    }

    /// Bytes are contiguous; Kafka offsets are not (compaction, transaction
    /// markers), so a gap nothing explains is lost data only for the former.
    fn contiguous(&self) -> bool {
        !matches!(self, Unit::Kafka { .. })
    }
}

impl std::fmt::Display for Unit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inode = |inode: &Option<u64>| inode.map_or_else(|| "?".to_owned(), |i| i.to_string());
        match self {
            Unit::Kafka {
                topic,
                partition,
                consumer_group,
            } => write!(f, "kafka {topic}[{partition}] group {consumer_group}"),
            Unit::File { path, inode: i } => write!(f, "file {path} (inode {})", inode(i)),
            Unit::SyslogSpool { path, inode: i } => {
                write!(f, "spool {path} (inode {})", inode(i))
            }
        }
    }
}

/// A batch's unit and half-open range `[start, end)`, from its markers.
///
/// The range runs from the window's first read (`progress_start`) to its last
/// (`progress_end`). A window whose two markers name different files (it
/// spans a rotation) is reduced to its last read: the audit then reports at
/// worst a hole that is not one, never hides one. A Kafka `end_offset` is the
/// last offset read, so the range ends one past it.
pub fn unit_and_range(record: &BatchRecord) -> Option<(Unit, u64, u64)> {
    match (&record.progress_start, &record.progress_end) {
        (
            ProgressMarker::Kafka {
                start_offset,
                topic: start_topic,
                partition: start_partition,
                ..
            },
            ProgressMarker::Kafka {
                topic,
                partition,
                end_offset,
                consumer_group,
                start_offset: last_read_start,
            },
        ) => {
            let start = if start_topic == topic && start_partition == partition {
                *start_offset
            } else {
                *last_read_start
            };
            let start = u64::try_from(start).ok()?;
            let end = u64::try_from(*end_offset).ok()?.checked_add(1)?;
            Some((
                Unit::Kafka {
                    topic: topic.clone(),
                    partition: *partition,
                    consumer_group: consumer_group.clone(),
                },
                start,
                end,
            ))
        }
        (
            ProgressMarker::File {
                path: start_path,
                inode: start_inode,
                start_byte,
                ..
            },
            ProgressMarker::File {
                path,
                inode,
                start_byte: last_read_start,
                end_byte,
                ..
            },
        ) => {
            let start = if start_path == path && start_inode == inode {
                *start_byte
            } else {
                *last_read_start
            };
            Some((
                Unit::File {
                    path: path.clone(),
                    inode: *inode,
                },
                start,
                *end_byte,
            ))
        }
        (
            ProgressMarker::SyslogSpool {
                path: start_path,
                inode: start_inode,
                start_byte,
                ..
            },
            ProgressMarker::SyslogSpool {
                path,
                inode,
                start_byte: last_read_start,
                end_byte,
                ..
            },
        ) => {
            let start = if start_path == path && start_inode == inode {
                *start_byte
            } else {
                *last_read_start
            };
            Some((
                Unit::SyslogSpool {
                    path: path.clone(),
                    inode: *inode,
                },
                start,
                *end_byte,
            ))
        }
        _ => None,
    }
}

/// A stretch of a unit no committed range covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Hole {
    pub start: u64,
    pub end: u64,
    /// The failed or replay-required rows whose ranges overlap the hole:
    /// the ranges that failed and were never archived again. Empty when
    /// nothing in the ledger names the hole.
    pub failed_batches: Vec<String>,
}

/// A range at or above the unit's last committed point that has not been
/// committed: the source will read it again, so it is not a hole (yet).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Pending {
    pub batch_id: String,
    pub state: BatchState,
    pub start: u64,
    pub end: u64,
}

/// What the audit found for one unit.
#[derive(Debug, Clone, Serialize)]
pub struct UnitReport {
    pub unit: Unit,
    /// Committed rows read (ledger and digests together).
    pub committed: usize,
    /// The first committed byte/offset: nothing below it is judged.
    pub audited_from: u64,
    /// One past the last committed byte/offset.
    pub committed_through: u64,
    /// Holes a failed row names: ranges that failed and were never archived.
    pub failed_holes: Vec<Hole>,
    /// Holes nothing names.
    pub unexplained_holes: Vec<Hole>,
    pub pending: Vec<Pending>,
    /// Bytes/offsets archived more than once.
    pub duplicated: u64,
}

impl UnitReport {
    /// Whether this unit shows lost data: a failed range never re-archived,
    /// or (for contiguous sources) a stretch nothing archived.
    pub fn has_findings(&self) -> bool {
        !self.failed_holes.is_empty()
            || (self.unit.contiguous() && !self.unexplained_holes.is_empty())
    }
}

/// One row as the audit sees it.
struct Row {
    batch_id: String,
    state: BatchState,
    start: u64,
    end: u64,
}

/// Audit a set of ledger rows. A `batch_id` that appears twice (a digested row
/// whose deletion did not happen) is counted once, first occurrence wins.
pub fn audit(records: impl IntoIterator<Item = BatchRecord>) -> Vec<UnitReport> {
    let mut seen = std::collections::HashSet::new();
    let mut units: BTreeMap<Unit, Vec<Row>> = BTreeMap::new();
    for record in records {
        if !seen.insert(record.batch_id.clone()) {
            continue;
        }
        let Some((unit, start, end)) = unit_and_range(&record) else {
            continue;
        };
        if end <= start {
            continue; // an empty read covers nothing
        }
        units.entry(unit).or_default().push(Row {
            batch_id: record.batch_id,
            state: record.state,
            start,
            end,
        });
    }
    units
        .into_iter()
        .filter_map(|(unit, rows)| audit_unit(unit, rows))
        .collect()
}

/// `None` for a unit with no committed range: there is nothing to tile, and
/// whatever is pending there will be read from the source's own position.
fn audit_unit(unit: Unit, rows: Vec<Row>) -> Option<UnitReport> {
    let mut committed: Vec<(u64, u64)> = rows
        .iter()
        .filter(|row| row.state == BatchState::SourceCommitted)
        .map(|row| (row.start, row.end))
        .collect();
    if committed.is_empty() {
        return None;
    }
    committed.sort_unstable();
    let total: u64 = committed.iter().map(|(s, e)| e - s).sum();
    let mut covered: Vec<(u64, u64)> = Vec::new();
    for (start, end) in committed.iter().copied() {
        match covered.last_mut() {
            Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
            _ => covered.push((start, end)),
        }
    }
    let merged: u64 = covered.iter().map(|(s, e)| e - s).sum();
    let audited_from = covered[0].0;
    let committed_through = covered.last().expect("non-empty").1;

    let failed = |row: &&Row| matches!(row.state, BatchState::Failed | BatchState::ReplayRequired);
    let in_flight = |row: &&Row| !failed(row) && row.state != BatchState::SourceCommitted;
    let overlapping = |start: u64, end: u64, keep: &dyn Fn(&&Row) -> bool| -> Vec<&Row> {
        rows.iter()
            .filter(|row| keep(row) && row.start < end && row.end > start)
            .collect()
    };
    // The stretches to judge: between committed ranges, and - when a failed
    // range lies below the first committed one - from that failed range up.
    // A first batch that failed under later commits is the #552 hole too.
    let mut gaps: Vec<(u64, u64)> = Vec::new();
    if let Some(lowest_failed) = rows
        .iter()
        .filter(|row| failed(row) && row.start < audited_from)
        .map(|row| row.start)
        .min()
    {
        gaps.push((lowest_failed, audited_from));
    }
    gaps.extend(covered.windows(2).map(|pair| (pair[0].1, pair[1].0)));

    let (mut failed_holes, mut unexplained_holes, mut pending) =
        (Vec::new(), Vec::new(), Vec::new());
    for (start, end) in gaps {
        let names: Vec<String> = overlapping(start, end, &failed)
            .into_iter()
            .map(|row| row.batch_id.clone())
            .collect();
        if !names.is_empty() {
            failed_holes.push(Hole {
                start,
                end,
                failed_batches: names,
            });
            continue;
        }
        // A range verified but not yet committed (recovery holds a commit
        // back while a range below it is re-read) is work in progress.
        let working = overlapping(start, end, &in_flight);
        if working.is_empty() {
            unexplained_holes.push(Hole {
                start,
                end,
                failed_batches: Vec::new(),
            });
        }
        pending.extend(working.into_iter().map(|row| Pending {
            batch_id: row.batch_id.clone(),
            state: row.state,
            start: row.start.max(start),
            end: row.end.min(end),
        }));
    }
    let audited_from = failed_holes
        .first()
        .map_or(audited_from, |hole| hole.start.min(audited_from));
    pending.extend(
        rows.iter()
            .filter(|row| row.state != BatchState::SourceCommitted && row.end > committed_through)
            .map(|row| Pending {
                batch_id: row.batch_id.clone(),
                state: row.state,
                start: row.start.max(committed_through),
                end: row.end,
            }),
    );
    Some(UnitReport {
        committed: committed.len(),
        audited_from,
        committed_through,
        failed_holes,
        unexplained_holes,
        pending,
        duplicated: total - merged,
        unit,
    })
}

// ---- Prune digest -----------------------------------------------------------

/// The digest format's version; a reader refuses any other.
pub const DIGEST_VERSION: u32 = 1;

/// The first line of a digest file.
#[derive(Debug, Serialize, Deserialize)]
struct DigestHeader {
    vtop_ledger_digest: u32,
    created_at: String,
    cutoff: String,
    rows: usize,
}

/// The last line: the SHA-256 of every line before it, newline included. It
/// catches a truncated or edited file; it is not a signature - whoever can
/// rewrite the file can rewrite the hash.
#[derive(Debug, Serialize, Deserialize)]
struct DigestTrailer {
    sha256: String,
}

/// Where prune digests go when the caller names no directory.
pub fn default_digest_dir(work_dir: &str) -> PathBuf {
    Path::new(work_dir).join("ledger-digests")
}

/// Write `rows` as one digest file in `dir`, durably (written, fsynced,
/// renamed into place, directory fsynced) - BEFORE the rows are deleted, so a
/// crash between the two leaves rows that are both digested and still in the
/// ledger (which the audit counts once), never rows that are in neither.
pub fn write_digest(dir: &Path, cutoff: &str, rows: &[BatchRecord]) -> Result<PathBuf, VtopError> {
    std::fs::create_dir_all(dir)?;
    let now = chrono::Utc::now();
    let header = DigestHeader {
        vtop_ledger_digest: DIGEST_VERSION,
        created_at: now.to_rfc3339(),
        cutoff: cutoff.to_owned(),
        rows: rows.len(),
    };
    let mut body = serde_json::to_string(&header)?;
    body.push('\n');
    for row in rows {
        body.push_str(&serde_json::to_string(row)?);
        body.push('\n');
    }
    let trailer = DigestTrailer {
        sha256: vtop_core::checksum::sha256_bytes(body.as_bytes()),
    };
    body.push_str(&serde_json::to_string(&trailer)?);
    body.push('\n');

    let name = format!(
        "ledger-digest-{}-{}.jsonl",
        now.format("%Y%m%dT%H%M%S%.9fZ"),
        uuid::Uuid::new_v4().simple()
    );
    let path = dir.join(&name);
    let tmp = dir.join(format!(".{name}.tmp"));
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(path)
}

/// Read every digest in `dir` (none if it does not exist). A file that is not
/// a complete digest of a known version is an error, not skipped: an audit
/// that quietly ignored part of its history would report the pruned ranges as
/// holes, or worse, report clean on a tampered anchor.
pub fn read_digests(dir: &Path) -> Result<(Vec<BatchRecord>, usize), VtopError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(error) => return Err(error.into()),
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("ledger-digest-") && name.ends_with(".jsonl"))
        })
        .collect();
    paths.sort();
    let mut rows = Vec::new();
    for path in &paths {
        rows.extend(read_digest(path)?);
    }
    Ok((rows, paths.len()))
}

fn read_digest(path: &Path) -> Result<Vec<BatchRecord>, VtopError> {
    let bad = |why: String| VtopError::Other(format!("ledger digest {}: {why}", path.display()));
    let text = std::fs::read_to_string(path)?;
    let lines: Vec<&str> = text.lines().collect();
    let (Some(first), Some(last)) = (lines.first(), lines.last()) else {
        return Err(bad("empty".into()));
    };
    let header: DigestHeader =
        serde_json::from_str(first).map_err(|e| bad(format!("header: {e}")))?;
    if header.vtop_ledger_digest != DIGEST_VERSION {
        return Err(bad(format!(
            "version {} is not supported (this build reads {DIGEST_VERSION})",
            header.vtop_ledger_digest
        )));
    }
    let trailer: DigestTrailer =
        serde_json::from_str(last).map_err(|e| bad(format!("no trailer (truncated?): {e}")))?;
    let hashed_len = text.len() - last.len() - 1;
    let actual = vtop_core::checksum::sha256_bytes(&text.as_bytes()[..hashed_len]);
    if actual != trailer.sha256 {
        return Err(bad(format!(
            "content hash {actual} does not match the recorded {}",
            trailer.sha256
        )));
    }
    let body = &lines[1..lines.len() - 1];
    if body.len() != header.rows {
        return Err(bad(format!(
            "{} rows, header says {}",
            body.len(),
            header.rows
        )));
    }
    body.iter()
        .map(|line| serde_json::from_str(line).map_err(|e| bad(format!("row: {e}"))))
        .collect()
}

/// One bounded prune pass that keeps its anchor: list the rows, write them to
/// a digest durably, then delete exactly those (the store re-checks each).
/// Returns the number deleted and the digest written, if any rows were.
pub async fn prune_with_digest(
    store: &dyn StateStore,
    cutoff: &str,
    limit: u32,
    digest_dir: &Path,
) -> Result<(u64, Option<PathBuf>), VtopError> {
    let rows = store.prune_candidates(cutoff, limit).await?;
    if rows.is_empty() {
        return Ok((0, None));
    }
    let digest = write_digest(digest_dir, cutoff, &rows)?;
    let ids: Vec<String> = rows.into_iter().map(|row| row.batch_id).collect();
    let deleted = store.prune_listed(cutoff, &ids).await?;
    Ok((deleted, Some(digest)))
}

// ---- Storage cross-check ----------------------------------------------------

/// A committed row whose object or manifest the store does not hold as the
/// ledger recorded it.
#[derive(Debug, Clone, Serialize)]
pub struct StorageFinding {
    pub batch_id: String,
    pub uri: String,
    pub problem: String,
}

/// Ask the object store about every committed row's object and manifest:
/// present, and of the recorded size. A HEAD cannot prove content; for that,
/// `verify-manifest` downloads and recomputes. Bounded concurrency.
pub async fn check_storage(
    backend: &dyn vtop_upload::UploadBackend,
    records: &[BatchRecord],
) -> Vec<StorageFinding> {
    use futures::StreamExt;
    let probes = records
        .iter()
        .filter(|record| record.state == BatchState::SourceCommitted)
        .flat_map(|record| {
            let object = record
                .object_uri
                .clone()
                .map(|uri| (record.batch_id.clone(), uri, record.object_size_bytes));
            let manifest = record
                .manifest_uri
                .clone()
                .map(|uri| (record.batch_id.clone(), uri, None));
            object.into_iter().chain(manifest)
        })
        .collect::<Vec<_>>();
    futures::stream::iter(probes)
        .map(|(batch_id, uri, size)| async move {
            match backend.head_object(&uri).await {
                Err(error) => Some(StorageFinding {
                    batch_id,
                    uri,
                    problem: format!("not readable: {error}"),
                }),
                Ok(head) => match (size, head.size_bytes) {
                    (Some(want), Some(got)) if u64::try_from(want).ok() != Some(got) => {
                        Some(StorageFinding {
                            batch_id,
                            uri,
                            problem: format!("{got} bytes stored, the ledger recorded {want}"),
                        })
                    }
                    _ => None,
                },
            }
        })
        .buffer_unordered(16)
        .filter_map(|finding| async move { finding })
        .collect()
        .await
}

// ---- Output ---------------------------------------------------------------

/// The audit's result, as printed and as exported.
#[derive(Debug, Serialize)]
pub struct AuditReport {
    pub ledger_rows: usize,
    pub digest_rows: usize,
    pub digest_files: usize,
    pub units: Vec<UnitReport>,
    /// `None` when storage was not checked.
    pub storage: Option<Vec<StorageFinding>>,
}

impl AuditReport {
    pub fn passed(&self) -> bool {
        !self.units.iter().any(UnitReport::has_findings)
            && self.storage.as_ref().is_none_or(Vec::is_empty)
    }

    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "audited {} ledger row(s) and {} digested row(s) from {} digest file(s); {} source unit(s)",
            self.ledger_rows,
            self.digest_rows,
            self.digest_files,
            self.units.len()
        )];
        for unit in &self.units {
            let verdict = if unit.has_findings() { "FAIL" } else { "ok" };
            out.push(format!(
                "{verdict:4} {}: {} committed range(s) over [{}, {}), {} duplicated",
                unit.unit,
                unit.committed,
                unit.audited_from,
                unit.committed_through,
                unit.duplicated
            ));
            for hole in &unit.failed_holes {
                out.push(format!(
                    "     hole [{}, {}): failed and never re-archived ({})",
                    hole.start,
                    hole.end,
                    hole.failed_batches.join(", ")
                ));
            }
            for hole in &unit.unexplained_holes {
                let why = if unit.unit.contiguous() {
                    "nothing in the ledger archived it"
                } else {
                    "no records archived (compaction or transaction markers can leave such gaps)"
                };
                out.push(format!("     gap [{}, {}): {why}", hole.start, hole.end));
            }
            for pending in &unit.pending {
                out.push(format!(
                    "     pending [{}, {}): {} is {}",
                    pending.start,
                    pending.end,
                    pending.batch_id,
                    pending.state.as_str()
                ));
            }
        }
        match &self.storage {
            None => out.push("storage: not checked (pass --check-storage)".into()),
            Some(findings) if findings.is_empty() => {
                out.push("storage: every committed object and manifest is present".into())
            }
            Some(findings) => {
                for finding in findings {
                    out.push(format!(
                        "FAIL storage {} ({}): {}",
                        finding.uri, finding.batch_id, finding.problem
                    ));
                }
            }
        }
        out.push(if self.passed() {
            "coverage audit PASSED".into()
        } else {
            "coverage audit FAILED".into()
        });
        out
    }

    /// The audit as a Prometheus text-format file, for a node exporter's
    /// textfile collector: counts by source type, never by path (paths are
    /// unbounded and would make the series count grow with the archive).
    pub fn prometheus_text(&self, now_unix: i64) -> String {
        let mut by_type: BTreeMap<&str, [u64; 5]> = BTreeMap::new();
        for unit in &self.units {
            let counts = by_type.entry(unit.unit.source_type().as_str()).or_default();
            counts[0] += unit.failed_holes.len() as u64;
            counts[1] += unit.unexplained_holes.len() as u64;
            counts[2] += unit.pending.len() as u64;
            counts[3] += unit.duplicated;
            counts[4] += 1;
        }
        let types: Vec<_> = by_type.into_iter().collect();
        let mut out = String::new();
        let families: [(&str, &str, usize); 5] = [
            (
                "vtop_coverage_audit_failed_holes",
                "Ranges that failed and no later committed range covers (lost data)",
                0,
            ),
            (
                "vtop_coverage_audit_unexplained_gaps",
                "Stretches between committed ranges nothing archived (lost data for file and spool sources)",
                1,
            ),
            (
                "vtop_coverage_audit_pending_ranges",
                "Failed or in-flight ranges above the committed point, to be read again",
                2,
            ),
            (
                "vtop_coverage_audit_duplicated",
                "Bytes (file, spool) or offsets (kafka) archived more than once",
                3,
            ),
            ("vtop_coverage_audit_units", "Source units audited", 4),
        ];
        for (name, help, index) in families {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
            for (source_type, counts) in &types {
                out.push_str(&format!(
                    "{name}{{source_type=\"{source_type}\"}} {}\n",
                    counts[index]
                ));
            }
        }
        let storage = self.storage.as_ref().map_or(-1, |f| f.len() as i64);
        out.push_str(
            "# HELP vtop_coverage_audit_storage_findings Committed objects or manifests missing or \
             of the wrong size (-1: not checked)\n# TYPE vtop_coverage_audit_storage_findings gauge\n",
        );
        out.push_str(&format!("vtop_coverage_audit_storage_findings {storage}\n"));
        out.push_str(
            "# HELP vtop_coverage_audit_passed 1 if the audit found no lost data\n\
             # TYPE vtop_coverage_audit_passed gauge\n",
        );
        out.push_str(&format!(
            "vtop_coverage_audit_passed {}\n",
            u8::from(self.passed())
        ));
        out.push_str(
            "# HELP vtop_coverage_audit_last_run_timestamp_seconds When the audit last ran\n\
             # TYPE vtop_coverage_audit_last_run_timestamp_seconds gauge\n",
        );
        out.push_str(&format!(
            "vtop_coverage_audit_last_run_timestamp_seconds {now_unix}\n"
        ));
        out
    }
}

/// Write `text` to `path` atomically, so a collector never reads half a file.
pub fn write_textfile(path: &Path, text: &str) -> Result<(), VtopError> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("coverage")
    ));
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_core::types::TelemetryFormat;

    fn file(path: &str, inode: u64, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::File {
            path: path.into(),
            inode: Some(inode),
            start_byte: start,
            end_byte: end,
            file_size: end,
            mtime: String::new(),
        }
    }

    fn kafka(start: i64, end: i64) -> ProgressMarker {
        ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: start,
            end_offset: end,
            consumer_group: "g".into(),
        }
    }

    fn row(id: &str, state: BatchState, start: ProgressMarker, end: ProgressMarker) -> BatchRecord {
        BatchRecord {
            batch_id: id.into(),
            tenant: "t".into(),
            source_type: start.source_type(),
            source_name: "s".into(),
            format: TelemetryFormat::Raw,
            state,
            progress_start: start,
            progress_end: end,
            object_uri: Some(format!("s3://b/{id}.raw.gz")),
            manifest_uri: Some(format!("s3://b/{id}.manifest.json")),
            object_sha256: None,
            manifest_sha256: None,
            manifest_version_id: None,
            object_size_bytes: Some(10),
            record_count: Some(1),
            error_message: None,
            owner: None,
            lease_expires_at: None,
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// A one-read window over `[start, end)` of `/a.log`, inode 7.
    fn bytes(id: &str, state: BatchState, start: u64, end: u64) -> BatchRecord {
        row(
            id,
            state,
            file("/a.log", 7, start, start),
            file("/a.log", 7, start, end),
        )
    }

    use BatchState::{Failed, ReplayRequired, SourceCommitted as Committed, Verified};

    #[test]
    fn ranges_that_tile_pass() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            bytes("b", Committed, 10, 25),
            bytes("c", Committed, 25, 40),
        ]);
        assert_eq!(units.len(), 1);
        let unit = &units[0];
        assert!(!unit.has_findings(), "{unit:?}");
        assert_eq!((unit.audited_from, unit.committed_through), (0, 40));
        assert_eq!(unit.duplicated, 0);
    }

    /// #552's shape: a range failed and a later one committed past it.
    #[test]
    fn a_failed_range_under_a_later_commit_is_a_hole_that_names_it() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            bytes("lost", Failed, 10, 20),
            bytes("c", Committed, 20, 30),
        ]);
        let unit = &units[0];
        assert!(unit.has_findings());
        assert_eq!(
            unit.failed_holes,
            vec![Hole {
                start: 10,
                end: 20,
                failed_batches: vec!["lost".into()]
            }]
        );
    }

    /// The same hole when the FIRST range is the one that failed: nothing was
    /// committed below it, so it lies under the first committed byte.
    #[test]
    fn a_failed_first_range_is_a_hole_too() {
        let units = audit([
            bytes("lost", ReplayRequired, 0, 10),
            bytes("b", Committed, 10, 20),
        ]);
        let unit = &units[0];
        assert_eq!(unit.audited_from, 0);
        assert_eq!(unit.failed_holes.len(), 1, "{unit:?}");
        assert_eq!(
            (unit.failed_holes[0].start, unit.failed_holes[0].end),
            (0, 10)
        );
    }

    #[test]
    fn a_failed_range_archived_again_is_covered() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            bytes("first-try", Failed, 10, 20),
            bytes("retry", Committed, 10, 20),
            bytes("c", Committed, 20, 30),
        ]);
        assert!(!units[0].has_findings(), "{:?}", units[0]);
    }

    /// Bytes are contiguous, so a stretch nothing archived is lost data (a
    /// rotation that resumed at the old offset leaves exactly this). Kafka
    /// offsets are not: compaction and transaction markers leave gaps with
    /// no records, so the same shape is reported but not failed.
    #[test]
    fn an_unexplained_gap_fails_a_file_but_not_a_kafka_partition() {
        let file_units = audit([bytes("a", Committed, 0, 10), bytes("b", Committed, 15, 20)]);
        assert!(file_units[0].has_findings());
        assert_eq!(file_units[0].unexplained_holes.len(), 1);

        let kafka_units = audit([
            row("a", Committed, kafka(0, 9), kafka(0, 9)),
            row("b", Committed, kafka(11, 20), kafka(11, 20)),
        ]);
        assert_eq!(kafka_units[0].unexplained_holes.len(), 1);
        assert!(!kafka_units[0].has_findings());
    }

    /// A Kafka `end_offset` is the last offset READ: 0..=9 and 10..=19 tile.
    #[test]
    fn kafka_ranges_end_one_past_the_last_offset_read() {
        let units = audit([
            row("a", Committed, kafka(0, 9), kafka(0, 9)),
            row("b", Committed, kafka(10, 19), kafka(10, 19)),
        ]);
        let unit = &units[0];
        assert!(unit.unexplained_holes.is_empty(), "{unit:?}");
        assert_eq!(unit.committed_through, 20);
    }

    /// A window of several reads starts at its FIRST read.
    #[test]
    fn a_window_runs_from_its_first_read_to_its_last() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            row(
                "b",
                Committed,
                file("/a.log", 7, 10, 10),
                file("/a.log", 7, 30, 40),
            ),
        ]);
        assert!(!units[0].has_findings(), "{:?}", units[0]);
    }

    #[test]
    fn overlapping_commits_are_counted_as_duplicates() {
        let units = audit([
            bytes("a", Committed, 0, 20),
            bytes("b", Committed, 10, 30),
            bytes("c", Committed, 10, 30),
        ]);
        assert_eq!(units[0].duplicated, 10 + 20);
        assert!(!units[0].has_findings());
    }

    /// Above the last committed point the source reads again: not a hole.
    /// Inside a gap, a VERIFIED range held back from committing is work in
    /// progress.
    #[test]
    fn pending_work_is_not_a_hole() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            bytes("held", Verified, 10, 20),
            bytes("c", Committed, 20, 30),
            bytes("next", Failed, 30, 40),
        ]);
        let unit = &units[0];
        assert!(!unit.has_findings(), "{unit:?}");
        let pending: Vec<&str> = unit.pending.iter().map(|p| p.batch_id.as_str()).collect();
        assert_eq!(pending, ["held", "next"]);
    }

    /// Two files that lived at one path are two units: the new one starting
    /// at byte 0 is not an overlap with the old one, and neither is judged
    /// against the other.
    #[test]
    fn each_inode_is_its_own_unit() {
        let units = audit([
            row(
                "old",
                Committed,
                file("/a.log", 1, 0, 0),
                file("/a.log", 1, 0, 900),
            ),
            row(
                "new",
                Committed,
                file("/a.log", 2, 0, 0),
                file("/a.log", 2, 0, 60),
            ),
        ]);
        assert_eq!(units.len(), 2);
        assert!(units.iter().all(|u| u.duplicated == 0 && !u.has_findings()));
    }

    #[test]
    fn a_row_seen_twice_counts_once() {
        let units = audit([bytes("a", Committed, 0, 10), bytes("a", Committed, 0, 10)]);
        assert_eq!(units[0].committed, 1);
        assert_eq!(units[0].duplicated, 0);
    }

    #[test]
    fn a_digest_round_trips_and_refuses_a_damaged_file() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![bytes("a", Committed, 0, 10), bytes("b", Committed, 10, 20)];
        let path = write_digest(dir.path(), "2026-01-01T00:00:00+00:00", &rows).unwrap();
        let (read, files) = read_digests(dir.path()).unwrap();
        assert_eq!(files, 1);
        let ids: Vec<&str> = read.iter().map(|r| r.batch_id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert!(
            std::fs::read_dir(dir.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")),
            "the temporary file was left behind"
        );

        // An edited row: the hash catches it.
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, text.replace("\"end_byte\":20", "\"end_byte\":21")).unwrap();
        let error = read_digests(dir.path()).unwrap_err().to_string();
        assert!(error.contains("does not match"), "{error}");

        // A truncated file: no trailer.
        let cut: String = text.lines().take(2).map(|l| format!("{l}\n")).collect();
        std::fs::write(&path, cut).unwrap();
        assert!(read_digests(dir.path()).is_err());

        // A newer format is refused by name, not misread.
        std::fs::write(
            &path,
            text.replace("\"vtop_ledger_digest\":1", "\"vtop_ledger_digest\":2"),
        )
        .unwrap();
        let error = read_digests(dir.path()).unwrap_err().to_string();
        assert!(error.contains("version 2 is not supported"), "{error}");

        // No directory: nothing pruned yet, not an error.
        assert_eq!(read_digests(&dir.path().join("absent")).unwrap().1, 0);
    }

    /// The point of the digest: pruning deletes the re-archived range that
    /// covers an old failure, and an audit of the ledger alone would then
    /// report the failure as a hole. With the digest it accounts for it.
    #[tokio::test]
    async fn an_audit_after_a_prune_needs_the_digest_and_passes_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = vtop_state::SqliteStateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let walk = [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
            BatchState::SourceCommitted,
        ];
        let save = |record: BatchRecord, commit: bool| {
            let store = &store;
            async move {
                let id = record.batch_id.clone();
                let mut record = record;
                record.state = BatchState::Batching;
                store.save_batch_state(&record).await.unwrap();
                let patch = vtop_state::BatchPatch::default();
                if commit {
                    for st in walk {
                        store.update_batch_state(&id, st, &patch).await.unwrap();
                    }
                } else {
                    store.mark_failed(&id, "upload failed").await.unwrap();
                }
            }
        };
        save(bytes("a", Committed, 0, 10), true).await;
        save(bytes("first-try", Failed, 10, 20), false).await;
        save(bytes("retry", Committed, 10, 20), true).await;
        save(bytes("c", Committed, 20, 30), true).await;

        let future = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
        let digests = dir.path().join("digests");
        let (deleted, digest) = prune_with_digest(&store, &future, 100, &digests)
            .await
            .unwrap();
        assert_eq!(deleted, 2, "a and retry go; c is the path's newest");
        assert!(digest.is_some());

        let ledger = store.list_batches().await.unwrap();
        let alone = audit(ledger.clone());
        assert!(
            alone[0].has_findings(),
            "without the digest the re-archived range is gone and the old failure reads as a \
             hole: {:?}",
            alone[0]
        );
        let (digested, _) = read_digests(&digests).unwrap();
        let both = audit(ledger.into_iter().chain(digested));
        assert!(!both[0].has_findings(), "{:?}", both[0]);
        assert_eq!((both[0].audited_from, both[0].committed_through), (0, 30));

        // Nothing left to prune: no rows, no digest file.
        assert_eq!(
            prune_with_digest(&store, &future, 100, &digests)
                .await
                .unwrap(),
            (0, None)
        );
    }

    #[test]
    fn the_metrics_file_counts_by_source_type_and_says_whether_it_passed() {
        let report = AuditReport {
            ledger_rows: 3,
            digest_rows: 0,
            digest_files: 0,
            units: audit([
                bytes("a", Committed, 0, 10),
                bytes("lost", Failed, 10, 20),
                bytes("c", Committed, 20, 30),
            ]),
            storage: None,
        };
        let text = report.prometheus_text(1_700_000_000);
        for line in [
            "vtop_coverage_audit_failed_holes{source_type=\"file\"} 1",
            "vtop_coverage_audit_unexplained_gaps{source_type=\"file\"} 0",
            "vtop_coverage_audit_units{source_type=\"file\"} 1",
            "vtop_coverage_audit_storage_findings -1",
            "vtop_coverage_audit_passed 0",
            "vtop_coverage_audit_last_run_timestamp_seconds 1700000000",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing {line:?} in:\n{text}"
            );
        }
        assert!(!text.contains("/a.log"), "a path became a label: {text}");
        assert!(report.lines().last().unwrap().contains("FAILED"));
    }
}
