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

use std::collections::{BTreeMap, HashMap};
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
        /// Which content generation at this path and inode (review of
        /// #573): a copytruncate keeps the inode, and a filesystem can hand
        /// a reused inode to a replacement file, so path and inode alone
        /// would merge an old file's ranges with a new one's - and the old
        /// file's commits would cover the new file's lost bytes. A row whose
        /// read saw the file SMALLER than an earlier row of the same path
        /// and inode had already read starts a new generation. Spool markers
        /// record no size, so spool units are not split this way; and a
        /// replacement already as large as the old read position when first
        /// read looks like the same file (#578).
        generation: u32,
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

    /// A file or spool row without an inode (a legacy marker, or a platform
    /// without inodes) cannot be told from another file that lived at the
    /// same path, so ranges from different files would be judged as one
    /// (review of #573). Such a unit is reported, not judged.
    fn judgeable(&self) -> bool {
        !matches!(
            self,
            Unit::File { inode: None, .. } | Unit::SyslogSpool { inode: None, .. }
        )
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
            Unit::File {
                path,
                inode: i,
                generation,
            } => {
                write!(f, "file {path} (inode {}", inode(i))?;
                if *generation > 0 {
                    write!(f, ", generation {generation}")?;
                }
                write!(f, ")")
            }
            Unit::SyslogSpool { path, inode: i } => {
                write!(f, "spool {path} (inode {})", inode(i))
            }
        }
    }
}

/// One end of a range whose upper bound is unknown: the rest of the file.
pub const END_OF_FILE: u64 = u64::MAX;

/// A batch's units and half-open ranges `[start, end)`, from its markers.
///
/// The range runs from the window's first read (`progress_start`) to its last
/// (`progress_end`). A Kafka `end_offset` is the last offset read, so the
/// range ends one past it. A window whose markers name two different files -
/// it spans a rotation - is credited only over what its markers prove
/// (review of #573): the new file over its LAST read, from where that read
/// started to the window's end. How far the window read the old file, and
/// where its reading of the new file began, are not recorded; crediting the
/// old file to its end could hide a lost tail, and the new one from byte 0
/// the skip a release before #564 made (it resumed a new file at the old
/// file's offset). Both stretches are reported as [`UnitReport::unproven`]
/// instead - see [`rotation_unproven`].
pub fn unit_ranges(record: &BatchRecord) -> Vec<(Unit, u64, u64)> {
    let file_like = |start_path: &String,
                     start_inode: &Option<u64>,
                     start_byte: u64,
                     path: &String,
                     inode: &Option<u64>,
                     last_read_start: u64,
                     end_byte: u64,
                     make: &dyn Fn(String, Option<u64>) -> Unit| {
        if start_path == path && start_inode == inode {
            vec![(make(path.clone(), *inode), start_byte, end_byte)]
        } else {
            // Neither file is credited past what the markers prove: the
            // new one over its last read, the old one not at all - the
            // window's reads of it end somewhere nothing records. Both
            // stretches are reported by `rotation_unproven` instead.
            let _ = (start_path, start_inode, start_byte);
            vec![(make(path.clone(), *inode), last_read_start, end_byte)]
        }
    };
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
            // A Kafka window never spans partitions (one buffer each); a pair
            // that does is reduced to its last read.
            let start = if start_topic == topic && start_partition == partition {
                *start_offset
            } else {
                *last_read_start
            };
            let (Ok(start), Some(end)) = (
                u64::try_from(start),
                u64::try_from(*end_offset)
                    .ok()
                    .and_then(|e| e.checked_add(1)),
            ) else {
                return Vec::new();
            };
            vec![(
                Unit::Kafka {
                    topic: topic.clone(),
                    partition: *partition,
                    consumer_group: consumer_group.clone(),
                },
                start,
                end,
            )]
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
        ) => file_like(
            start_path,
            start_inode,
            *start_byte,
            path,
            inode,
            *last_read_start,
            *end_byte,
            &|path, inode| Unit::File {
                path,
                inode,
                generation: 0,
            },
        ),
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
        ) => file_like(
            start_path,
            start_inode,
            *start_byte,
            path,
            inode,
            *last_read_start,
            *end_byte,
            &|path, inode| Unit::SyslogSpool { path, inode },
        ),
        _ => Vec::new(),
    }
}

/// For a window that spans a rotation, the stretches its markers cannot
/// place (review of #573): the old file from where the window started to its
/// end - the window read on into it, how far is not recorded - and the start
/// of the new file below the window's last read, when that is above byte 0.
/// An earlier read in the window may have archived them, or a release
/// before #564 may have skipped them; the two look the same here.
pub fn rotation_unproven(record: &BatchRecord) -> Vec<(Unit, u64, u64)> {
    let (start_path, start_inode, start_byte, path, inode, last_read_start, make): (
        _,
        _,
        _,
        _,
        _,
        _,
        fn(String, Option<u64>) -> Unit,
    ) = match (&record.progress_start, &record.progress_end) {
        (
            ProgressMarker::File {
                path: start_path,
                inode: start_inode,
                start_byte: window_start,
                ..
            },
            ProgressMarker::File {
                path,
                inode,
                start_byte,
                ..
            },
        ) => (
            start_path,
            start_inode,
            *window_start,
            path,
            inode,
            *start_byte,
            |path, inode| Unit::File {
                path,
                inode,
                generation: 0,
            },
        ),
        (
            ProgressMarker::SyslogSpool {
                path: start_path,
                inode: start_inode,
                start_byte: window_start,
                ..
            },
            ProgressMarker::SyslogSpool {
                path,
                inode,
                start_byte,
                ..
            },
        ) => (
            start_path,
            start_inode,
            *window_start,
            path,
            inode,
            *start_byte,
            |path, inode| Unit::SyslogSpool { path, inode },
        ),
        _ => return Vec::new(),
    };
    if start_path == path && start_inode == inode {
        return Vec::new();
    }
    let mut stretches = vec![(
        make(start_path.clone(), *start_inode),
        start_byte,
        END_OF_FILE,
    )];
    if last_read_start > 0 {
        stretches.push((make(path.clone(), *inode), 0, last_read_start));
    }
    stretches
}

/// The size a row's read saw the file at, for the file `unit` names.
fn observed_size(record: &BatchRecord, unit: &Unit) -> Option<u64> {
    let Unit::File { path, inode, .. } = unit else {
        return None;
    };
    [&record.progress_end, &record.progress_start]
        .into_iter()
        .find_map(|marker| match marker {
            ProgressMarker::File {
                path: read_path,
                inode: read_inode,
                file_size,
                ..
            } if read_path == path && read_inode == inode => Some(*file_size),
            _ => None,
        })
}

/// The content generation of each file (see [`Unit::File`]), assigned
/// walking rows in creation order.
#[derive(Default)]
struct Generations {
    /// (path, inode) -> (generation, highest end read in it).
    files: HashMap<(String, Option<u64>), (u32, u64)>,
}

impl Generations {
    /// `unit` with its generation, for a read ending at `end` that saw the
    /// file at `observed` bytes: a file seen smaller than this generation
    /// has already been read is new content.
    fn place(&mut self, unit: Unit, observed: Option<u64>, end: u64) -> Unit {
        let Unit::File { path, inode, .. } = unit else {
            return unit;
        };
        let (generation, read_to) = self.files.entry((path.clone(), inode)).or_insert((0, 0));
        if observed.is_some_and(|size| size > 0 && size < *read_to) {
            *generation += 1;
            *read_to = 0;
        }
        *read_to = (*read_to).max(end);
        Unit::File {
            path,
            inode,
            generation: *generation,
        }
    }

    /// `unit` in the generation its file is in now.
    fn current(&self, unit: Unit) -> Unit {
        let Unit::File { path, inode, .. } = unit else {
            return unit;
        };
        let generation = self
            .files
            .get(&(path.clone(), inode))
            .map_or(0, |(generation, _)| *generation);
        Unit::File {
            path,
            inode,
            generation,
        }
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
    /// The start of a file rotated in mid-window, below that window's last
    /// read, that nothing else covers: the ledger cannot say whether it was
    /// read (see [`rotation_unproven`]). Reported, never a finding - a window
    /// that simply read the new file more than once looks the same.
    pub unproven: Vec<Hole>,
    /// Bytes/offsets archived more than once.
    pub duplicated: u64,
    /// Why the unit was not judged (its holes are then not looked for), or
    /// `None` when it was.
    pub not_judged: Option<&'static str>,
}

impl UnitReport {
    /// Whether this unit shows lost data: a failed range never re-archived,
    /// or (for contiguous sources) a stretch nothing archived.
    pub fn has_findings(&self) -> bool {
        self.not_judged.is_none()
            && (!self.failed_holes.is_empty()
                || (self.unit.contiguous() && !self.unexplained_holes.is_empty()))
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
    let mut records: Vec<BatchRecord> = records
        .into_iter()
        .filter(|record| seen.insert(record.batch_id.clone()))
        .collect();
    // Creation order: a file's generations are found by walking its rows in
    // the order they were read. Stable, so rows created together keep the
    // order they were listed in.
    records
        .sort_by_cached_key(|record| chrono::DateTime::parse_from_rfc3339(&record.created_at).ok());
    let mut generations = Generations::default();
    let mut units: BTreeMap<Unit, Vec<Row>> = BTreeMap::new();
    let mut unproven: BTreeMap<Unit, Vec<(u64, u64)>> = BTreeMap::new();
    for record in &records {
        for (unit, start, end) in unit_ranges(record) {
            if end <= start {
                continue; // an empty read covers nothing
            }
            let observed = observed_size(record, &unit);
            let unit = generations.place(unit, observed, end);
            // A later generation is new content from byte 0, so its start
            // below its first read is not old history: unproven, as at a
            // rotation (a copytruncated file resumed at the old offset by a
            // release before #564 looks like this).
            if let Unit::File { generation, .. } = &unit {
                if *generation > 0 && start > 0 && !units.contains_key(&unit) {
                    unproven.entry(unit.clone()).or_default().push((0, start));
                }
            }
            units.entry(unit).or_default().push(Row {
                batch_id: record.batch_id.clone(),
                state: record.state,
                start,
                end,
            });
        }
        if record.state == BatchState::SourceCommitted {
            for (unit, start, end) in rotation_unproven(record) {
                if end > start {
                    unproven
                        .entry(generations.current(unit))
                        .or_default()
                        .push((start, end));
                }
            }
        }
    }
    let all: std::collections::BTreeSet<Unit> =
        units.keys().chain(unproven.keys()).cloned().collect();
    all.into_iter()
        .filter_map(|unit| {
            let rows = units.remove(&unit).unwrap_or_default();
            let stretches = unproven.remove(&unit).unwrap_or_default();
            audit_unit(unit, rows, &stretches)
        })
        .collect()
}

/// Rows merged into disjoint spans, sorted, each with the rows it came from.
struct Span<'a> {
    start: u64,
    end: u64,
    rows: Vec<&'a Row>,
}

fn spans<'a>(rows: impl Iterator<Item = &'a Row>) -> Vec<Span<'a>> {
    let mut sorted: Vec<&Row> = rows.collect();
    sorted.sort_unstable_by_key(|row| (row.start, row.end));
    let mut out: Vec<Span> = Vec::new();
    for row in sorted {
        match out.last_mut() {
            Some(span) if row.start <= span.end => {
                span.end = span.end.max(row.end);
                span.rows.push(row);
            }
            _ => out.push(Span {
                start: row.start,
                end: row.end,
                rows: vec![row],
            }),
        }
    }
    out
}

/// The spans overlapping `[start, end)`, found by binary search: the sweep
/// costs the overlaps, not a scan of every row per gap (review of #573).
fn overlapping<'s, 'a>(spans: &'s [Span<'a>], start: u64, end: u64) -> &'s [Span<'a>] {
    let first = spans.partition_point(|span| span.end <= start);
    let count = spans[first..].partition_point(|span| span.start < end);
    &spans[first..first + count]
}

/// `[start, end)` less every span: the uncovered pieces, in order.
fn uncovered(start: u64, end: u64, spans: &[Span]) -> Vec<(u64, u64)> {
    let mut pieces = Vec::new();
    let mut cursor = start;
    for span in overlapping(spans, start, end) {
        if span.start > cursor {
            pieces.push((cursor, span.start));
        }
        cursor = cursor.max(span.end);
    }
    if cursor < end {
        pieces.push((cursor, end));
    }
    pieces
}

/// `None` for a unit with no committed range: there is nothing to tile, and
/// whatever is pending there will be read from the source's own position.
fn audit_unit(unit: Unit, rows: Vec<Row>, unproven: &[(u64, u64)]) -> Option<UnitReport> {
    let committed = spans(
        rows.iter()
            .filter(|row| row.state == BatchState::SourceCommitted),
    );
    let failed = spans(
        rows.iter()
            .filter(|row| matches!(row.state, BatchState::Failed | BatchState::ReplayRequired)),
    );
    if committed.is_empty() {
        // Known only through a rotation window's unplaceable stretches:
        // nothing to tile, but the stretches are still reported.
        if unproven.is_empty() || !unit.judgeable() {
            return None;
        }
        let mut report = UnitReport {
            committed: 0,
            audited_from: 0,
            committed_through: 0,
            failed_holes: Vec::new(),
            unexplained_holes: Vec::new(),
            pending: Vec::new(),
            unproven: Vec::new(),
            duplicated: 0,
            not_judged: None,
            unit,
        };
        for &(start, end) in unproven {
            for (start, end) in uncovered(start, end, &failed) {
                report.unproven.push(Hole {
                    start,
                    end,
                    failed_batches: Vec::new(),
                });
            }
        }
        return Some(report);
    }
    let committed_rows: usize = committed.iter().map(|span| span.rows.len()).sum();
    let total: u128 = committed
        .iter()
        .flat_map(|span| &span.rows)
        .map(|row| u128::from(row.end - row.start))
        .sum();
    let merged: u128 = committed
        .iter()
        .map(|span| u128::from(span.end - span.start))
        .sum();
    let audited_from = committed[0].start;
    let committed_through = committed.last().expect("non-empty").end;
    let duplicated = u64::try_from(total - merged).unwrap_or(u64::MAX);
    let in_flight = spans(rows.iter().filter(|row| {
        !matches!(
            row.state,
            BatchState::Failed | BatchState::ReplayRequired | BatchState::SourceCommitted
        )
    }));

    let mut report = UnitReport {
        committed: committed_rows,
        audited_from,
        committed_through,
        failed_holes: Vec::new(),
        unexplained_holes: Vec::new(),
        pending: Vec::new(),
        unproven: Vec::new(),
        duplicated,
        not_judged: None,
        unit,
    };
    if !report.unit.judgeable() {
        report.not_judged =
            Some("rows without an inode: another file at the same path cannot be told apart");
        return Some(report);
    }

    // Below the first committed span nothing is judged - it may be history
    // pruned before digests existed - EXCEPT what a failed row names there:
    // a first batch that failed under later commits is the #552 hole too.
    // Only the failed ranges themselves (review of #573): extending one up to
    // the first commit reported the stretch between as unexplained, outside
    // what the audit can judge.
    for span in failed.iter().take_while(|span| span.start < audited_from) {
        report.failed_holes.push(Hole {
            start: span.start,
            end: span.end.min(audited_from),
            failed_batches: span.rows.iter().map(|row| row.batch_id.clone()).collect(),
        });
    }
    // ...and what a rotation window cannot place (see `rotation_unproven`),
    // outside the stretch the audit judges anyway and not covered by any
    // committed or failed row: unproven, not lost.
    for &(start, end) in unproven {
        for (start, end) in uncovered(start, end, &committed) {
            let outside_judged = [
                (start, end.min(audited_from)),
                (start.max(committed_through), end),
            ];
            for (start, end) in outside_judged {
                if start >= end {
                    continue;
                }
                for (start, end) in uncovered(start, end, &failed) {
                    report.unproven.push(Hole {
                        start,
                        end,
                        failed_batches: Vec::new(),
                    });
                }
            }
        }
    }
    // The stretches to judge: between committed spans.
    let gaps: Vec<(u64, u64)> = committed
        .windows(2)
        .map(|pair| (pair[0].end, pair[1].start))
        .collect();

    // Each gap is split, not judged whole (review of #573): the parts a failed
    // row names are failed holes; of the rest, the parts in-flight work covers
    // are pending; whatever remains is unexplained. A VERIFIED [10,15) inside
    // a [10,20) gap no longer excuses [15,20).
    for (start, end) in gaps {
        for span in overlapping(&failed, start, end) {
            report.failed_holes.push(Hole {
                start: span.start.max(start),
                end: span.end.min(end),
                failed_batches: span.rows.iter().map(|row| row.batch_id.clone()).collect(),
            });
        }
        for (piece_start, piece_end) in uncovered(start, end, &failed) {
            for span in overlapping(&in_flight, piece_start, piece_end) {
                for row in &span.rows {
                    if row.start < piece_end && row.end > piece_start {
                        report.pending.push(Pending {
                            batch_id: row.batch_id.clone(),
                            state: row.state,
                            start: row.start.max(piece_start),
                            end: row.end.min(piece_end),
                        });
                    }
                }
            }
            for (hole_start, hole_end) in uncovered(piece_start, piece_end, &in_flight) {
                report.unexplained_holes.push(Hole {
                    start: hole_start,
                    end: hole_end,
                    failed_batches: Vec::new(),
                });
            }
        }
    }
    report.audited_from = report
        .failed_holes
        .first()
        .map_or(audited_from, |hole| hole.start.min(audited_from));
    report.pending.extend(
        rows.iter()
            .filter(|row| row.state != BatchState::SourceCommitted && row.end > committed_through)
            .map(|row| Pending {
                batch_id: row.batch_id.clone(),
                state: row.state,
                start: row.start.max(committed_through),
                end: row.end,
            }),
    );
    Some(report)
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

/// Where prune digests go when the caller names no directory (review of
/// #573): `engine.ledger_digest_dir` when set; otherwise beside a SQLite
/// database file, on the database's own volume - the digests anchor history
/// the database no longer holds, and must last as long as it does - in a
/// directory named for that database file, so two databases side by side
/// never read each other's digests as their own history; only otherwise
/// (PostgreSQL, an in-memory SQLite) under `work_dir`, which is scratch and
/// which a PostgreSQL deployment should override.
pub fn default_digest_dir(engine: &vtop_core::config::EngineConfig) -> PathBuf {
    if let Some(dir) = engine
        .ledger_digest_dir
        .as_deref()
        .filter(|dir| !dir.trim().is_empty())
    {
        return PathBuf::from(dir);
    }
    if let Some(database) = engine
        .state_store
        .resolve()
        .ok()
        .and_then(|store| sqlite_database_file(store.expose_secret()))
    {
        let beside = database
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = database
            .file_name()
            .map_or_else(|| "state".into(), |name| name.to_string_lossy());
        return beside.join(format!("{name}.ledger-digests"));
    }
    Path::new(&engine.work_dir).join("ledger-digests")
}

/// The file a SQLite connection string names, or `None` for PostgreSQL and
/// an in-memory database.
fn sqlite_database_file(conn: &str) -> Option<PathBuf> {
    let conn = conn.trim();
    let lower = conn.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        return None;
    }
    let rest = conn
        .strip_prefix("sqlite://")
        .or_else(|| conn.strip_prefix("sqlite:"))
        .unwrap_or(conn);
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    // The `mode=memory` QUERY PARAMETER, exactly - not those characters
    // anywhere in the string, which a real path can contain (review).
    let in_memory = query
        .split('&')
        .any(|parameter| parameter.eq_ignore_ascii_case("mode=memory"));
    if path.is_empty() || path == ":memory:" || in_memory {
        return None;
    }
    Some(PathBuf::from(path))
}

/// Write `rows` as one digest file in `dir`, durably (written, fsynced,
/// renamed into place, directory fsynced) - BEFORE the rows are deleted, so a
/// crash between the two leaves rows that are both digested and still in the
/// ledger (which the audit counts once), never rows that are in neither.
pub fn write_digest(dir: &Path, cutoff: &str, rows: &[BatchRecord]) -> Result<PathBuf, VtopError> {
    create_dir_durably(dir)?;
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

    // Named by WHICH rows it holds (review of #573): a prune whose delete
    // failed lists the same candidates a minute later, and a fresh name each
    // time would write the same anchor again on every retry. The same set
    // names the same file, already durable, so it is not rewritten.
    let mut ids: Vec<&str> = rows.iter().map(|row| row.batch_id.as_str()).collect();
    ids.sort_unstable();
    let key = vtop_core::checksum::sha256_bytes(ids.join("\n").as_bytes());
    let name = format!("ledger-digest-{}.jsonl", &key[..32]);
    let path = dir.join(&name);
    // An existing file is TRUSTED ONLY ONCE READ BACK (review of #573): it
    // must verify and hold exactly these rows, or the rows about to be
    // deleted would be anchored by a damaged file. One that does not is
    // replaced by the digest written here - the rows are in hand, so the
    // anchor can be made right rather than refusing the prune forever.
    if path.exists() {
        let mut held: Vec<String> = match read_digest(&path) {
            Ok(rows) => rows.into_iter().map(|row| row.batch_id).collect(),
            Err(_) => Vec::new(),
        };
        held.sort_unstable();
        if held.iter().map(String::as_str).eq(ids.iter().copied()) {
            return Ok(path);
        }
        tracing::warn!(
            digest = %path.display(),
            "an existing prune digest did not verify, or did not hold the rows its name \
             promises; replacing it with the digest of the rows being pruned"
        );
    }
    // A temporary name of this writer's own (review of #573): two prune passes
    // over the same rows - the engine's and `vtopctl prune-ledger` - would
    // otherwise write into one temporary file, and either could rename the
    // other's half-written bytes into place. Each publishes a complete file;
    // the rename is atomic, and both carry the same rows.
    let tmp = dir.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    let written = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)
    })();
    if let Err(error) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(error.into());
    }
    std::fs::File::open(dir)?.sync_all()?;
    Ok(path)
}

/// Create `dir` (and any missing ancestors) so the directories themselves
/// survive a crash, not only the files later written into them (review of
/// #573): a new directory's ENTRY lives in its parent, and syncing the new
/// directory does not persist that. Every directory this call creates has
/// its parent synced before it returns - so a digest synced into place,
/// and then the ledger rows it anchors deleted, cannot be lost with a
/// directory the parent never durably recorded.
fn create_dir_durably(dir: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut at = dir;
    while !at.exists() {
        missing.push(at.to_path_buf());
        match at.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            Some(parent) => at = parent,
            None => break,
        }
    }
    std::fs::create_dir_all(dir)?;
    for created in &missing {
        let parent = created
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
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
    // An entry that cannot be read is an error, not an absence: skipping it
    // could drop a digest and report the pruned history as holes, or clean
    // (review of #573).
    let mut paths: Vec<PathBuf> = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
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
    if lines.len() < 2 {
        return Err(bad("no header and trailer (truncated?)".into()));
    }
    let (first, last) = (lines[0], lines[lines.len() - 1]);
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
    // A committed row that names no object or no manifest is a finding in
    // itself, not a probe to skip: skipped, it let the check pass without
    // having checked that batch at all (review of #573).
    let mut findings: Vec<StorageFinding> = Vec::new();
    let mut probes = Vec::new();
    for record in records
        .iter()
        .filter(|record| record.state == BatchState::SourceCommitted)
    {
        for (what, uri, size) in [
            ("object", &record.object_uri, record.object_size_bytes),
            ("manifest", &record.manifest_uri, None),
        ] {
            match uri {
                Some(uri) => probes.push((record.batch_id.clone(), uri.clone(), size)),
                None => findings.push(StorageFinding {
                    batch_id: record.batch_id.clone(),
                    uri: String::new(),
                    problem: format!("the committed row names no {what}"),
                }),
            }
        }
    }
    let probed: Vec<StorageFinding> = futures::stream::iter(probes)
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
                    // The ledger knows the size and the store did not say:
                    // nothing was compared, so it is not a pass (review of
                    // #573; a command backend whose output did not parse).
                    (Some(want), None) => Some(StorageFinding {
                        batch_id,
                        uri,
                        problem: format!("the store reported no size; the ledger recorded {want}"),
                    }),
                    _ => None,
                },
            }
        })
        .buffer_unordered(16)
        .filter_map(|finding| async move { finding })
        .collect()
        .await;
    findings.extend(probed);
    findings
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
        let bound = |at: u64| {
            if at == END_OF_FILE {
                "end of file".to_owned()
            } else {
                at.to_string()
            }
        };
        for unit in &self.units {
            let verdict = match (unit.not_judged, unit.has_findings()) {
                (Some(_), _) => "skip",
                (None, true) => "FAIL",
                (None, false) => "ok",
            };
            out.push(format!(
                "{verdict:4} {}: {} committed range(s) over [{}, {}), {} duplicated",
                unit.unit,
                unit.committed,
                unit.audited_from,
                bound(unit.committed_through),
                unit.duplicated
            ));
            if let Some(why) = unit.not_judged {
                out.push(format!("     not judged: {why}"));
            }
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
            for hole in &unit.unproven {
                out.push(format!(
                    "     unproven [{}, {}): a stretch a window spanning a rotation may have \
                     read; the ledger does not record whether it did (a skip by a release \
                     before #564 looks the same)",
                    hole.start,
                    bound(hole.end)
                ));
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
        let mut by_type: BTreeMap<&str, [u64; 6]> = BTreeMap::new();
        for unit in &self.units {
            let counts = by_type.entry(unit.unit.source_type().as_str()).or_default();
            counts[0] += unit.failed_holes.len() as u64;
            counts[1] += unit.unexplained_holes.len() as u64;
            counts[2] += unit.pending.len() as u64;
            counts[3] += unit.duplicated;
            counts[4] += 1;
            counts[5] += unit.unproven.len() as u64;
        }
        let types: Vec<_> = by_type.into_iter().collect();
        let mut out = String::new();
        let families: [(&str, &str, usize); 6] = [
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
            (
                "vtop_coverage_audit_unproven_ranges",
                "Stretches of files around a mid-window rotation that the ledger cannot place (not findings)",
                5,
            ),
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
            // A file larger than any read here: one generation throughout,
            // unless a test says otherwise (see `sized`).
            file_size: 1 << 40,
            mtime: String::new(),
        }
    }

    /// A one-read window over `[start, end)` of `path`/`inode` that saw the
    /// file at `size` bytes.
    fn sized(
        id: &str,
        state: BatchState,
        inode: u64,
        start: u64,
        end: u64,
        size: u64,
    ) -> BatchRecord {
        let marker = |end_byte: u64| ProgressMarker::File {
            path: "/a.log".into(),
            inode: Some(inode),
            start_byte: start,
            end_byte,
            file_size: size,
            mtime: String::new(),
        };
        row(id, state, marker(start), marker(end))
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

    /// Review of #573: a VERIFIED range covering part of a gap excuses only
    /// that part; the rest of the gap is still lost.
    #[test]
    fn work_in_flight_excuses_only_the_part_of_a_gap_it_covers() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            bytes("held", Verified, 10, 15),
            bytes("c", Committed, 20, 30),
        ]);
        let unit = &units[0];
        assert!(unit.has_findings(), "{unit:?}");
        assert_eq!(
            unit.unexplained_holes,
            vec![Hole {
                start: 15,
                end: 20,
                failed_batches: vec![]
            }]
        );
        assert_eq!(unit.pending.len(), 1);
        assert_eq!((unit.pending[0].start, unit.pending[0].end), (10, 15));
    }

    /// A failed row covering part of a gap names that part; the rest is
    /// reported apart, not absorbed into it.
    #[test]
    fn a_failed_row_names_only_the_part_of_a_gap_it_covers() {
        let units = audit([
            bytes("a", Committed, 0, 10),
            bytes("lost", Failed, 10, 12),
            bytes("c", Committed, 20, 30),
        ]);
        let unit = &units[0];
        assert_eq!(
            (unit.failed_holes[0].start, unit.failed_holes[0].end),
            (10, 12)
        );
        assert_eq!(
            (
                unit.unexplained_holes[0].start,
                unit.unexplained_holes[0].end
            ),
            (12, 20)
        );
    }

    /// Review of #573: rows without an inode could be two files at one path;
    /// their ranges are reported, never judged as one file.
    #[test]
    fn a_path_known_only_without_inodes_is_not_judged() {
        let legacy = |id: &str, start: u64, end: u64| {
            let marker = |at: u64| ProgressMarker::File {
                path: "/legacy.log".into(),
                inode: None,
                start_byte: at,
                end_byte: end,
                file_size: end,
                mtime: String::new(),
            };
            row(id, Committed, marker(start), marker(start))
        };
        let units = audit([legacy("a", 0, 10), legacy("b", 50, 60)]);
        let unit = &units[0];
        assert!(unit.not_judged.is_some());
        assert!(!unit.has_findings(), "a gap across what may be two files");
    }

    /// Review of #573: a window that spans a rotation covers the old file to
    /// its end and the new file over its LAST read. Below that read the new
    /// file is unproven - the ledger does not say where the window's reading
    /// of it began - which is reported, and is not a finding.
    #[test]
    fn a_window_across_a_rotation_covers_both_files() {
        let units = audit([
            row(
                "old",
                Committed,
                file("/a.log", 1, 0, 0),
                file("/a.log", 1, 0, 100),
            ),
            row(
                "across",
                Committed,
                file("/a.log", 1, 100, 100),
                file("/a.log", 2, 20, 30),
            ),
            row(
                "new",
                Committed,
                file("/a.log", 2, 30, 30),
                file("/a.log", 2, 30, 60),
            ),
        ]);
        assert_eq!(units.len(), 2);
        for unit in &units {
            assert!(!unit.has_findings(), "{unit:?}");
        }
        let old = units
            .iter()
            .find(|u| matches!(u.unit, Unit::File { inode: Some(1), .. }))
            .unwrap();
        // The old file is credited only through its own last commit; how far
        // the rotation window read on into it is unproven, not committed.
        assert_eq!(old.committed_through, 100);
        assert_eq!(
            old.unproven
                .iter()
                .map(|hole| (hole.start, hole.end))
                .collect::<Vec<_>>(),
            vec![(100, END_OF_FILE)]
        );
        let new = units
            .iter()
            .find(|u| matches!(u.unit, Unit::File { inode: Some(2), .. }))
            .unwrap();
        assert_eq!((new.audited_from, new.committed_through), (20, 60));
        assert_eq!(
            new.unproven,
            vec![Hole {
                start: 0,
                end: 20,
                failed_batches: Vec::new()
            }]
        );
        assert!(new.unexplained_holes.is_empty() && new.failed_holes.is_empty());
    }

    /// Review of #573: crediting the new file from byte 0 hid the skip a
    /// release before #564 made - it resumed a rotated-in file at the old
    /// one's offset. A window whose last read on the new file started at 80
    /// leaves [0, 80) visible as unproven; one whose read started at 0 is
    /// credited from 0 and leaves nothing.
    #[test]
    fn a_rotated_in_file_read_from_the_old_offset_is_not_credited_from_zero() {
        let skipped = audit([row(
            "across",
            Committed,
            file("/a.log", 1, 50, 50),
            file("/a.log", 2, 80, 120),
        )]);
        let new = skipped
            .iter()
            .find(|u| matches!(u.unit, Unit::File { inode: Some(2), .. }))
            .unwrap();
        assert_eq!(new.audited_from, 80);
        assert_eq!((new.unproven[0].start, new.unproven[0].end), (0, 80));
        let lines = AuditReport {
            units: skipped.clone(),
            ledger_rows: 1,
            digest_rows: 0,
            digest_files: 0,
            storage: None,
        }
        .lines();
        assert!(
            lines.iter().any(|line| line.contains("unproven [0, 80)")),
            "{lines:?}"
        );

        let whole = audit([row(
            "across",
            Committed,
            file("/a.log", 1, 50, 50),
            file("/a.log", 2, 0, 40),
        )]);
        let new = whole
            .iter()
            .find(|u| matches!(u.unit, Unit::File { inode: Some(2), .. }))
            .unwrap();
        assert_eq!(new.audited_from, 0);
        assert!(new.unproven.is_empty());
    }

    /// Review of #573: a window that read part of the old file and then the
    /// new one does not credit the old file to its end - nothing records how
    /// far it read - so a lost tail stays visible as unproven, even for an
    /// old file known only through that window.
    #[test]
    fn a_rotated_out_tail_is_unproven_not_committed() {
        let units = audit([row(
            "across",
            Committed,
            file("/a.log", 1, 0, 0),
            file("/a.log", 2, 0, 40),
        )]);
        let old = units
            .iter()
            .find(|u| matches!(u.unit, Unit::File { inode: Some(1), .. }))
            .expect("the old file is reported for its unproven tail");
        assert_eq!(old.committed, 0);
        assert_eq!(
            (old.unproven[0].start, old.unproven[0].end),
            (0, END_OF_FILE)
        );
        assert!(!old.has_findings());
    }

    /// Review of #573: a copytruncate keeps the inode, and a filesystem can
    /// reuse one for a replacement file. A read that saw the file SMALLER
    /// than what was already read at that path and inode starts a new
    /// generation, judged on its own: the old content's [0,100) no longer
    /// covers the new content, whose skipped start shows as unproven.
    #[test]
    fn new_content_at_the_same_inode_is_a_new_generation() {
        let units = audit([
            sized("old", Committed, 7, 0, 100, 100),
            // Truncated, then resumed at byte 50 of 80: [0,50) of the new
            // content was never read.
            sized("new", Committed, 7, 50, 80, 80),
        ]);
        assert_eq!(units.len(), 2, "{units:?}");
        let new = units
            .iter()
            .find(|u| matches!(u.unit, Unit::File { generation: 1, .. }))
            .expect("the new content is its own unit");
        assert_eq!((new.audited_from, new.committed_through), (50, 80));
        assert_eq!(
            (new.unproven[0].start, new.unproven[0].end),
            (0, 50),
            "the new content's skipped start must not be covered by the old content"
        );
        assert!(new.unit.to_string().contains("generation 1"));

        // A file that only grew stays one generation.
        let grown = audit([
            sized("a", Committed, 7, 0, 100, 100),
            sized("b", Committed, 7, 100, 150, 150),
        ]);
        assert_eq!(grown.len(), 1);
        assert!(grown[0].unproven.is_empty());
    }

    /// Review of #573: below the first commit only what a failed row names is
    /// judged. FAILED [0,5) under a first commit at 10 is a failed hole of
    /// [0,5) - and [5,10), which nothing names, is history the audit cannot
    /// judge, not an unexplained gap.
    #[test]
    fn below_the_first_commit_only_the_failed_range_is_judged() {
        let units = audit([
            bytes("lost", Failed, 0, 5),
            bytes("first", Committed, 10, 20),
        ]);
        let unit = &units[0];
        assert_eq!(
            unit.failed_holes
                .iter()
                .map(|hole| (hole.start, hole.end))
                .collect::<Vec<_>>(),
            vec![(0, 5)]
        );
        assert!(unit.unexplained_holes.is_empty(), "{unit:?}");
        assert!(unit.has_findings(), "a failed range never re-archived");
    }

    /// Review of #573: digests default to the database's own volume, not the
    /// scratch work_dir - unless a directory is configured.
    #[test]
    fn prune_digests_default_beside_a_sqlite_database() {
        let config = crate::testkit::file_config(
            "/tmp/scratch",
            "sqlite:///var/lib/vtop/state.db",
            Vec::new(),
            "mock",
        );
        assert_eq!(
            default_digest_dir(&config.engine),
            PathBuf::from("/var/lib/vtop/state.db.ledger-digests"),
            "named for the database, so a neighbour's digests are not read as its own"
        );
        let mut configured = config.clone();
        configured.engine.ledger_digest_dir = Some("/backed-up/digests".into());
        assert_eq!(
            default_digest_dir(&configured.engine),
            PathBuf::from("/backed-up/digests")
        );
        let memory =
            crate::testkit::file_config("/tmp/scratch", "sqlite::memory:", Vec::new(), "mock");
        assert_eq!(
            default_digest_dir(&memory.engine),
            PathBuf::from("/tmp/scratch/ledger-digests")
        );
        assert_eq!(
            sqlite_database_file("sqlite:state.db?mode=rwc"),
            Some(PathBuf::from("state.db"))
        );
        assert_eq!(sqlite_database_file("postgres://u@h/db"), None);
        // `mode=memory` counts only as the query parameter (review).
        assert_eq!(
            sqlite_database_file("sqlite:///data/mode=memory/state.db"),
            Some(PathBuf::from("/data/mode=memory/state.db"))
        );
        assert_eq!(
            sqlite_database_file("sqlite:file:state.db?cache=shared&mode=memory"),
            None
        );
    }

    /// Review of #573: a prune whose delete failed lists the same rows again;
    /// the same rows are the same digest, written once.
    #[test]
    fn the_same_rows_are_one_digest() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![bytes("a", Committed, 0, 10), bytes("b", Committed, 10, 20)];
        let first = write_digest(dir.path(), "c", &rows).unwrap();
        let again = write_digest(
            dir.path(),
            "c",
            &rows.iter().rev().cloned().collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(first, again);
        assert_eq!(read_digests(dir.path()).unwrap().1, 1);
        let other = write_digest(dir.path(), "c", &rows[..1]).unwrap();
        assert_ne!(first, other);
    }

    /// Review of #573: an existing digest is reused only when it verifies and
    /// holds exactly these rows. A damaged one - here, truncated - is replaced
    /// with the digest of the rows in hand, and reads back whole.
    #[test]
    fn a_damaged_digest_of_the_same_rows_is_rewritten_not_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![bytes("a", Committed, 0, 10), bytes("b", Committed, 10, 20)];
        let path = write_digest(dir.path(), "c", &rows).unwrap();
        let whole = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, &whole[..whole.len() / 2]).unwrap();
        assert!(
            read_digests(dir.path()).is_err(),
            "the damage is detectable"
        );

        assert_eq!(write_digest(dir.path(), "c", &rows).unwrap(), path);
        let (read, files) = read_digests(dir.path()).unwrap();
        assert_eq!(files, 1);
        assert_eq!(read.len(), 2);
    }

    /// Review of #573: writers of the same digest at once each use their own
    /// temporary file, so every one of them leaves a complete digest behind
    /// and no temporary file is left over.
    #[test]
    fn concurrent_writers_of_one_digest_each_publish_a_complete_file() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<BatchRecord> = (0..200)
            .map(|i| bytes(&format!("r{i:03}"), Committed, i * 10, i * 10 + 10))
            .collect();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| write_digest(dir.path(), "c", &rows).unwrap());
            }
        });
        let (read, files) = read_digests(dir.path()).unwrap();
        assert_eq!((files, read.len()), (1, 200));
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    /// Review of #573: a digest directory that does not exist yet - nested,
    /// here - is created (with its parents synced) and the digest lands in
    /// it, readable.
    #[test]
    fn a_missing_digest_directory_is_created_with_its_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("a").join("b").join("digests");
        let rows = vec![bytes("a", Committed, 0, 10)];
        let path = write_digest(&dir, "c", &rows).unwrap();
        assert!(path.starts_with(&dir));
        assert_eq!(read_digests(&dir).unwrap().0.len(), 1);
        // And again into the now-existing directory.
        write_digest(&dir, "c", &rows).unwrap();
    }

    /// Review of #573: a one-line file is refused, not a panic.
    #[test]
    fn a_one_line_digest_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ledger-digest-x.jsonl"), "{}\n").unwrap();
        assert!(read_digests(dir.path()).is_err());
    }

    /// Review of #573: a committed row that names no object is a finding,
    /// as is one the store does not hold or holds at another size.
    #[tokio::test]
    async fn the_storage_check_finds_what_it_cannot_check() {
        let backend = vtop_upload::mock::MockBackend::new();
        let dir = tempfile::tempdir().unwrap();
        let body = dir.path().join("body");
        std::fs::write(&body, b"0123456789").unwrap();
        use vtop_upload::UploadBackend;
        let mut present = bytes("present", Committed, 0, 10);
        for uri in [
            present.object_uri.clone().unwrap(),
            present.manifest_uri.clone().unwrap(),
        ] {
            backend.put_object(&body, &uri, None).await.unwrap();
        }
        present.object_size_bytes = Some(10);
        let mut no_uris = bytes("no-uris", Committed, 10, 20);
        no_uris.object_uri = None;
        no_uris.manifest_uri = None;
        let missing = bytes("missing", Committed, 20, 30);
        let mut wrong_size = present.clone();
        wrong_size.batch_id = "wrong-size".into();
        wrong_size.object_size_bytes = Some(11);

        let findings = check_storage(&backend, &[present, no_uris, missing, wrong_size]).await;
        let of = |id: &str| findings.iter().filter(|f| f.batch_id == id).count();
        assert_eq!(of("present"), 0, "{findings:?}");
        assert_eq!(of("no-uris"), 2, "{findings:?}");
        assert_eq!(of("missing"), 2, "{findings:?}");
        assert_eq!(of("wrong-size"), 1, "{findings:?}");
    }

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
