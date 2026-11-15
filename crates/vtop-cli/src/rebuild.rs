//! Replays that rebuild the window an earlier attempt sealed (#626).
//!
//! #688 made the same bytes resolve to the same object key: a replay that
//! produces the bytes an earlier attempt uploaded finds that object and
//! reuses it. The key also carries the time partition, and the partition is
//! taken from the instant the window's contents were fixed. A replay sealed
//! in a later hour therefore named another key for the same bytes, and wrote
//! a second object.
//!
//! What a failed window was is recorded here: its range, the instant it was
//! sealed and, where the ledger recorded one, the digest of the object the
//! earlier attempt uploaded. While the engine runs the record comes from
//! memory; after a restart from the ledger row recovery claims, which carries
//! the seal instant since #626 (`BatchRecord::sealed_at`). When a window that
//! covers exactly a recorded range is drained, it is sealed at the recorded
//! instant, so it is filed under the same hour and names the same key, and
//! what it came to is counted in `vtop_replay_rebuilds_total`.
//!
//! Nothing here parses an object key (#688): the key is derived again, from
//! the recorded instant, by the same code that derived it the first time.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use vtop_core::telemetry;
use vtop_core::types::ProgressMarker;
use vtop_state::BatchRecord;

use crate::engine::{end_position, proven_other, start_position, unit_key};

/// How many recorded windows one source unit keeps. A window is forgotten
/// when a window at or above its start is drained, so the records of a unit
/// that is read again never accumulate; this bounds the rest (a unit whose
/// rewind was refused, or that is never read again).
const MAX_RECORDED_PER_UNIT: usize = 1024;

/// How many recorded windows the engine keeps in all. A unit that is never
/// read again (a deleted topic, a path dropped from the config, a file
/// rotated away with no descriptor left) keeps its windows until this
/// evicts the oldest-recorded one.
const MAX_RECORDED: usize = 16 * 1024;

/// A window an earlier attempt sealed and handed back to its source.
#[derive(Debug, Clone)]
pub(crate) struct RecordedWindow {
    /// The first read's start, as the ledger row's `progress_start`.
    pub(crate) start: ProgressMarker,
    /// The last read's end, as the ledger row's `progress_end`.
    pub(crate) end: ProgressMarker,
    /// When the earlier attempt fixed the window's contents; `None` for a row
    /// written before #626, whose replay is filed under the hour it runs in.
    pub(crate) sealed_at: Option<DateTime<Utc>>,
    /// The digest of the object the earlier attempt uploaded, as the ledger
    /// recorded it with OBJECT_UPLOADED; `None` when no object was recorded,
    /// or the batch was not checksummed.
    pub(crate) digest: Option<String>,
    /// The earlier attempt's batch id, for the operator; `None` when its
    /// ledger row could not be written (#627).
    pub(crate) batch_id: Option<String>,
    /// How many records the window held, when known: for the cap on what a
    /// bounded replay may hold in memory.
    pub(crate) record_count: Option<u64>,
    /// Whether the source agreed to stop the re-read at the window's end
    /// ([`vtop_adapters::base::SourceAdapter::bound_replays`]). A window
    /// that is being rebuilt is held until it reaches that end.
    pub(crate) bounded: bool,
}

impl RecordedWindow {
    /// The window a ledger row recorded.
    pub(crate) fn from_row(rec: &BatchRecord) -> Self {
        Self {
            start: rec.progress_start.clone(),
            end: rec.progress_end.clone(),
            sealed_at: rec.sealed_at.as_deref().and_then(parse_sealed_at),
            digest: recorded_digest(rec.object_sha256.as_deref()),
            batch_id: Some(rec.batch_id.clone()),
            record_count: rec.record_count.and_then(|n| u64::try_from(n).ok()),
            bounded: false,
        }
    }

    /// The window's range as the replay messages name it.
    pub(crate) fn range(&self) -> String {
        ProgressMarker::spanning(&self.start, &self.end).range_token()
    }

    /// The source the window was read from, for the replay messages.
    pub(crate) fn source(&self) -> &str {
        match &self.start {
            ProgressMarker::Kafka { topic, .. } => topic,
            ProgressMarker::File { path, .. } | ProgressMarker::SyslogSpool { path, .. } => path,
        }
    }

    /// How many bytes the window spans, for a byte-offset source.
    pub(crate) fn span_bytes(&self) -> Option<u64> {
        match &self.start {
            ProgressMarker::Kafka { .. } => None,
            _ => u64::try_from(end_position(&self.end) - start_position(&self.start)).ok(),
        }
    }

    /// Whether a drained window from `start` to `end` is this window: the
    /// same unit (source, partition or file and inode), the same first and
    /// last position, and content not proven to be another.
    fn is(&self, start: &ProgressMarker, end: &ProgressMarker) -> bool {
        unit_key(&self.start) == unit_key(start)
            && start_position(&self.start) == start_position(start)
            && end_position(&self.end) == end_position(end)
            && !proven_other(
                self.start.content_fingerprint(),
                start.content_fingerprint(),
            )
            // The bytes ending at the window's end (#620): a file rewritten
            // or replaced at the same inode, shorter than a page, agrees with
            // the recorded window on everything above.
            && !proven_other(self.end.tail_fingerprint(), end.tail_fingerprint())
    }

    /// Whether `other` is the same window: the same unit and range, and
    /// content not proven other.
    fn same_window(&self, other: &RecordedWindow) -> bool {
        self.is(&other.start, &other.end)
    }
}

/// A seal instant as the ledger stores it (RFC 3339). One that does not
/// parse is unknown: its replay is filed as a row from before #626 is.
pub(crate) fn parse_sealed_at(stored: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(stored)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// A digest as the ledger stores it: empty when the batch was not
/// checksummed, which is no digest.
pub(crate) fn recorded_digest(stored: Option<&str>) -> Option<String> {
    stored.filter(|d| !d.is_empty()).map(str::to_owned)
}

/// The windows handed back to their sources and not yet read again.
#[derive(Debug, Default)]
pub(crate) struct RecordedWindows {
    /// Per unit, each window with the order it was recorded in.
    by_unit: HashMap<String, Vec<(u64, RecordedWindow)>>,
    next: u64,
    total: usize,
}

impl RecordedWindows {
    /// Record a window that is being handed back. A window already recorded
    /// at the same start is replaced. Only when it is the SAME window (the
    /// same end, content not proven other) does the new record keep what the
    /// earlier one knew and it does not: the digest of the object the
    /// earlier attempt uploaded, and its seal instant. A window with another
    /// end is another window, and inherits nothing.
    pub(crate) fn remember(&mut self, mut window: RecordedWindow) {
        let unit = self.by_unit.entry(unit_key(&window.start)).or_default();
        let start = start_position(&window.start);
        if let Some(i) = unit
            .iter()
            .position(|(_, w)| start_position(&w.start) == start)
        {
            let (_, earlier) = unit.remove(i);
            self.total -= 1;
            if earlier.same_window(&window) {
                if window.digest.is_none() {
                    window.digest = earlier.digest;
                }
                if window.sealed_at.is_none() {
                    window.sealed_at = earlier.sealed_at;
                }
            }
        }
        let seq = self.next;
        self.next += 1;
        unit.push((seq, window));
        self.total += 1;
        if unit.len() > MAX_RECORDED_PER_UNIT {
            // The oldest-recorded, never the window just recorded.
            if let Some(i) = unit
                .iter()
                .enumerate()
                .min_by_key(|(_, (seq, _))| *seq)
                .map(|(i, _)| i)
            {
                unit.remove(i);
                self.total -= 1;
            }
        }
        while self.total > MAX_RECORDED {
            self.evict_oldest();
        }
    }

    /// Drop the oldest-recorded window of any unit.
    fn evict_oldest(&mut self) {
        let Some((key, seq)) = self
            .by_unit
            .iter()
            .flat_map(|(key, unit)| unit.iter().map(move |(seq, _)| (key, *seq)))
            .min_by_key(|(_, seq)| *seq)
            .map(|(key, seq)| (key.clone(), seq))
        else {
            return;
        };
        if let Some(unit) = self.by_unit.get_mut(&key) {
            unit.retain(|(s, _)| *s != seq);
            self.total -= 1;
            if unit.is_empty() {
                self.by_unit.remove(&key);
            }
        }
    }

    /// Forget the window recorded at `start`: its rewind was refused, so no
    /// re-read will rebuild it.
    pub(crate) fn forget(&mut self, start: &ProgressMarker) {
        let key = unit_key(start);
        let Some(unit) = self.by_unit.get_mut(&key) else {
            return;
        };
        let position = start_position(start);
        let before = unit.len();
        unit.retain(|(_, w)| start_position(&w.start) != position);
        self.total -= before - unit.len();
        if unit.is_empty() {
            self.by_unit.remove(&key);
        }
    }

    /// The recorded window a drained window from `start` to `end` rebuilds,
    /// if any. Every window of the unit that starts at or below `start` is
    /// forgotten: the read has reached it, so it is either this one or one a
    /// rewind could not land on.
    pub(crate) fn take(
        &mut self,
        start: &ProgressMarker,
        end: &ProgressMarker,
    ) -> Option<RecordedWindow> {
        let key = unit_key(start);
        let unit = self.by_unit.get_mut(&key)?;
        let position = start_position(start);
        let mut found = None;
        let before = unit.len();
        unit.retain(|(_, w)| {
            if start_position(&w.start) > position {
                return true;
            }
            if found.is_none() && w.is(start, end) {
                found = Some(w.clone());
            }
            false
        });
        self.total -= before - unit.len();
        if unit.is_empty() {
            self.by_unit.remove(&key);
        }
        found
    }

    /// The end of the bounded window that starts where a read starting at
    /// `start` does, if one is being rebuilt there.
    pub(crate) fn bounded_end(&self, start: &ProgressMarker) -> Option<i128> {
        let position = start_position(start);
        self.by_unit
            .get(&unit_key(start))?
            .iter()
            .map(|(_, w)| w)
            .find(|w| {
                w.bounded
                    && start_position(&w.start) == position
                    && !proven_other(w.start.content_fingerprint(), start.content_fingerprint())
            })
            .map(|w| end_position(&w.end))
    }

    /// Record whether the source bounds the re-read of the window recorded at
    /// `start`.
    pub(crate) fn set_bounded(&mut self, start: &ProgressMarker, bounded: bool) {
        let position = start_position(start);
        if let Some(w) = self.by_unit.get_mut(&unit_key(start)).and_then(|unit| {
            unit.iter_mut()
                .map(|(_, w)| w)
                .find(|w| start_position(&w.start) == position)
        }) {
            w.bounded = bounded;
        }
    }

    /// Stop holding the window of `source` recorded from `start` to `end`:
    /// its re-read could not stop at its end. Returns it, if it was held.
    pub(crate) fn unbind(&mut self, source: &str, start: u64, end: u64) -> Option<RecordedWindow> {
        let w = self
            .by_unit
            .values_mut()
            .flatten()
            .map(|(_, w)| w)
            .find(|w| {
                w.bounded
                    && w.source() == source
                    && start_position(&w.start) == i128::from(start)
                    && end_position(&w.end) == i128::from(end)
            })?;
        w.bounded = false;
        Some(w.clone())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        debug_assert_eq!(
            self.total,
            self.by_unit.values().map(Vec::len).sum::<usize>()
        );
        self.total
    }
}

/// Count and report what a replay of `window` came to (#626), once its key
/// has been decided: `digest` is this attempt's, `key` the object it names,
/// and `key_is_derived` whether that is the derived key or the attempt key a
/// fallback took (`object_key_fallback`, `object_key_per_attempt`). Only what
/// happened is claimed: a fallback named another key, so it is not `exact`.
#[expect(
    clippy::too_many_arguments,
    reason = "each is a field of the operator message; a struct would only name them twice"
)]
pub(crate) fn note_rebuilt(
    window: &RecordedWindow,
    batch_id: &str,
    source: &str,
    range: &str,
    key: &str,
    digest: Option<&str>,
    key_is_derived: bool,
    records: usize,
    labels: [&str; 3],
) {
    let earlier = window.batch_id.as_deref().unwrap_or("unrecorded");
    let differs = window.digest.as_deref().zip(digest).filter(|(a, b)| a != b);
    // A window with no recorded instant (a row from before #626, or one a
    // v0.9.0 engine wrote to a shared ledger) was sealed when it was read
    // again, so nothing can be claimed about its hour or its key: only a
    // digest that differs is reported.
    let result = match (differs, window.sealed_at, key_is_derived) {
        (Some(_), _, _) => "bytes_differ",
        (None, None, _) => {
            tracing::debug!(
                batch_id,
                earlier_batch_id = earlier,
                range,
                "replay of a window that recorded no seal instant; filed under the hour it was \
                 read again"
            );
            return;
        }
        (None, Some(_), false) => "fallback",
        (None, Some(_), true) => "exact",
    };
    debug_assert!(
        telemetry::REPLAY_REBUILD_RESULTS.contains(&result),
        "{result} is not a pinned replay result"
    );
    if let Some(mx) = telemetry::metrics() {
        mx.replay_rebuilds_total
            .with_label_values(&[labels[0], labels[1], labels[2], result])
            .inc();
    }
    let sealed_at = window
        .sealed_at
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| "unknown".to_owned());
    let recorded_digest = window.digest.as_deref().unwrap_or("unknown");
    match differs {
        None if !key_is_derived => tracing::info!(
            event = "replay_key_fallback",
            batch_id,
            earlier_batch_id = earlier,
            source,
            range,
            key,
            records,
            sealed_at = %sealed_at,
            "range {range} of {source} failed earlier (batch {earlier}) and was rebuilt exactly as \
             first cut ({records} records) and sealed at the instant first recorded \
             ({sealed_at}), but its derived key could not be used, so it was written under {key}, \
             a second object (nothing is lost); the object_key_fallback or \
             object_key_per_attempt event for this batch says why and what to do"
        ),
        None => {
            // What can be said of the object depends on whether the ledger
            // recorded the earlier attempt's digest.
            let object = if window.digest.is_some() {
                "the object the earlier attempt uploaded, with the same bytes"
            } else {
                "the key the earlier attempt would have used for the same bytes (it recorded no \
                 upload)"
            };
            tracing::info!(
                event = "replay_rebuilt",
                batch_id,
                earlier_batch_id = earlier,
                source,
                range,
                key,
                records,
                sealed_at = %sealed_at,
                recorded_digest,
                "range {range} of {source} failed earlier (batch {earlier}) and was read again: it \
                 was rebuilt exactly as first cut ({records} records) and sealed at the instant \
                 first recorded ({sealed_at}), so under `partitioning.time_source: seal` it is \
                 filed under the same hour and names {object}: {key}; nothing to do"
            )
        }
        Some((recorded, rebuilt)) => tracing::warn!(
            event = "replay_bytes_differ",
            batch_id,
            earlier_batch_id = earlier,
            source,
            range,
            key,
            recorded_digest = recorded,
            digest = rebuilt,
            "range {range} of {source} was rebuilt as first cut, but its bytes differ from the \
             object the earlier attempt (batch {earlier}) uploaded (digest {recorded}, now \
             {rebuilt}); the batch is written under {key} and the earlier object is left \
             untouched, so it may stay unreferenced (nothing is lost); if compression, format or \
             checksum settings changed since, nothing to do, otherwise the source's content \
             changed under the range: run `vtopctl scrub`"
        ),
    }
}

/// How much larger than the current batching limits a recorded window may be
/// and still be rebuilt exactly (#626). A rebuilt window is held, whatever
/// the thresholds say, until it reaches its recorded end; the cap keeps that
/// bounded when the limits were lowered since the window was cut.
pub(crate) const REBUILD_CAP_FACTOR: u64 = 4;

/// Count one replay result that is not decided at the key (#626).
fn count(result: &'static str, labels: [&str; 3]) {
    debug_assert!(
        telemetry::REPLAY_REBUILD_RESULTS.contains(&result),
        "{result} is not a pinned replay result"
    );
    if let Some(mx) = telemetry::metrics() {
        mx.replay_rebuilds_total
            .with_label_values(&[labels[0], labels[1], labels[2], result])
            .inc();
    }
}

/// The window's re-read could not stop at its recorded end (`refused`).
/// Rate-limited by the caller: `suppressed` is how many were not logged since
/// the last notice for this source.
pub(crate) fn note_refused(
    window: &RecordedWindow,
    why: &str,
    labels: [&str; 3],
    suppressed: Option<u64>,
) {
    count("refused", labels);
    let Some(suppressed) = suppressed else {
        return;
    };
    let (range, source) = (window.range(), window.source());
    let earlier = window.batch_id.as_deref().unwrap_or("unrecorded");
    tracing::warn!(
        event = "replay_bound_refused",
        earlier_batch_id = earlier,
        source,
        range,
        reason = why,
        suppressed,
        "range {range} of {source} (batch {earlier}) cannot be rebuilt as first cut: {why}; it is \
         read from its start to the current end as before, so a second object may be written for \
         bytes already archived (the first stays, nothing is lost); find what rewrote or \
         truncated the source, and run `vtopctl audit-coverage` ({suppressed} more like this \
         suppressed since the last notice)"
    );
}

/// The window is over [`REBUILD_CAP_FACTOR`] times the current limits and is
/// not bounded (`too_large`).
pub(crate) fn note_too_large(window: &RecordedWindow, limits: (usize, usize), labels: [&str; 3]) {
    count("too_large", labels);
    let (range, source) = (window.range(), window.source());
    let earlier = window.batch_id.as_deref().unwrap_or("unrecorded");
    let records = window
        .record_count
        .map(|n| n.to_string())
        .unwrap_or_else(|| "?".into());
    let bytes = window
        .span_bytes()
        .map(|n| n.to_string())
        .unwrap_or_else(|| "?".into());
    let (max_records, max_bytes) = limits;
    tracing::warn!(
        event = "replay_bound_too_large",
        earlier_batch_id = earlier,
        source,
        range,
        records = %records,
        bytes = %bytes,
        "range {range} of {source} (batch {earlier}, {records} records, {bytes} bytes) is over \
         {REBUILD_CAP_FACTOR}x the current batching.max_records ({max_records}) or \
         batching.max_bytes ({max_bytes}); it is not rebuilt as first cut, so memory stays \
         bounded, and a second object may be written for bytes already archived (nothing is \
         lost); raise the limits back, or accept it"
    );
}

/// The engine is stopping inside the rebuild of a window (`dropped_at_stop`).
pub(crate) fn note_dropped_at_stop(source: &str, range: &str, labels: [&str; 3]) {
    count("dropped_at_stop", labels);
    tracing::warn!(
        event = "replay_window_dropped",
        source,
        range,
        "the engine is stopping inside the rebuild of range {range} of {source}; the partial \
         window was not flushed, because flushing it would write an object the next start \
         duplicates; the range was never committed, so the next start reads it again; nothing \
         to do"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(start: u64, end: u64, inode: u64) -> ProgressMarker {
        ProgressMarker::File {
            path: "/var/log/a.log".into(),
            inode: Some(inode),
            start_byte: start,
            end_byte: end,
            file_size: end,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        }
    }

    fn window(start: u64, end: u64) -> RecordedWindow {
        RecordedWindow {
            start: file(start, start, 7),
            end: file(start, end, 7),
            sealed_at: parse_sealed_at("2026-01-02T03:04:05Z"),
            digest: None,
            batch_id: Some(format!("b{start}")),
            record_count: None,
            bounded: false,
        }
    }

    /// A drained window is the recorded one only over exactly its range, in
    /// its own file; reaching a recorded start forgets it either way, and a
    /// window above is kept for its own re-read.
    #[test]
    fn a_window_is_found_only_by_its_exact_range_and_is_forgotten_once_reached() {
        let mut windows = RecordedWindows::default();
        windows.remember(window(0, 10));
        windows.remember(window(10, 20));
        assert!(
            windows.take(&file(0, 0, 8), &file(0, 10, 8)).is_none(),
            "another inode is another file"
        );
        assert!(
            windows.take(&file(0, 0, 7), &file(0, 15, 7)).is_none(),
            "a longer window is not the recorded one"
        );
        assert_eq!(windows.len(), 1, "reaching a start forgets it");
        let found = windows.take(&file(10, 10, 7), &file(10, 20, 7)).unwrap();
        assert_eq!(found.batch_id.as_deref(), Some("b10"));
        assert_eq!(windows.len(), 0);
    }

    /// A window handed back again keeps the digest of the object the first
    /// attempt uploaded when the second attempt recorded none: that object is
    /// the one that may be in the store.
    #[test]
    fn a_window_handed_back_twice_keeps_the_first_known_digest() {
        let mut windows = RecordedWindows::default();
        let mut first = window(0, 10);
        first.digest = Some("aa".into());
        windows.remember(first);
        windows.remember(window(0, 10));
        let found = windows.take(&file(0, 0, 7), &file(0, 10, 7)).unwrap();
        assert_eq!(found.digest.as_deref(), Some("aa"));
    }

    /// Review of #755: a window recorded at the same start with ANOTHER end
    /// is another window, and inherits neither the digest nor the instant of
    /// the one it replaces. Inheriting the digest made a grown replay report
    /// `bytes_differ` against an object of a shorter range.
    #[test]
    fn a_window_with_another_end_inherits_nothing() {
        let mut windows = RecordedWindows::default();
        let mut first = window(0, 10);
        first.digest = Some("aa".into());
        windows.remember(first);
        let mut grown = window(0, 15);
        grown.sealed_at = None;
        windows.remember(grown);
        let found = windows.take(&file(0, 0, 7), &file(0, 15, 7)).unwrap();
        assert_eq!(found.digest, None);
        assert_eq!(found.sealed_at, None);
    }

    /// Review of #755: two windows over one range whose bytes before the end
    /// differ (a small file rewritten at the same inode) are not the same.
    #[test]
    fn a_window_whose_end_bytes_differ_is_not_the_recorded_one() {
        let tail = |bytes: &[u8]| ProgressMarker::File {
            path: "/var/log/a.log".into(),
            inode: Some(7),
            start_byte: 0,
            end_byte: 10,
            file_size: 10,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: vtop_core::types::ContentFingerprint::of(bytes),
        };
        let mut windows = RecordedWindows::default();
        let mut w = window(0, 10);
        w.end = tail(b"0123456789");
        windows.remember(w);
        assert!(windows.take(&file(0, 0, 7), &tail(b"abcdefghij")).is_none());
    }

    /// Review of #755: forgetting a window whose rewind was refused, and the
    /// bound on every unit together.
    #[test]
    fn a_refused_window_is_forgotten_and_the_total_is_bounded() {
        let mut windows = RecordedWindows::default();
        windows.remember(window(0, 10));
        windows.forget(&file(0, 0, 7));
        assert_eq!(windows.len(), 0);
        for i in 0..(MAX_RECORDED as u64 + 3) {
            let mut w = window(0, 10);
            w.start = ProgressMarker::File {
                path: format!("/var/log/{i}.log"),
                inode: Some(i),
                start_byte: 0,
                end_byte: 0,
                file_size: 0,
                mtime: String::new(),
                content_fingerprint: None,
                tail_fingerprint: None,
            };
            windows.remember(w);
        }
        assert_eq!(windows.len(), MAX_RECORDED);
    }

    /// The records of one unit are bounded.
    #[test]
    fn a_unit_keeps_a_bounded_number_of_windows() {
        let mut windows = RecordedWindows::default();
        for i in 0..(MAX_RECORDED_PER_UNIT as u64 + 5) {
            windows.remember(window(i * 10, i * 10 + 10));
        }
        assert_eq!(windows.len(), MAX_RECORDED_PER_UNIT);
    }

    #[test]
    fn a_stored_digest_or_instant_that_is_not_one_is_unknown() {
        assert_eq!(recorded_digest(Some("")), None);
        assert_eq!(recorded_digest(None), None);
        assert_eq!(recorded_digest(Some("ab")).as_deref(), Some("ab"));
        assert!(parse_sealed_at("not a time").is_none());
        assert_eq!(
            parse_sealed_at("2026-01-02T03:04:05+00:00").map(|t| t.to_rfc3339()),
            Some("2026-01-02T03:04:05+00:00".to_owned())
        );
    }
}
