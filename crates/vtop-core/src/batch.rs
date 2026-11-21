//! Telemetry batch model and the adaptive batcher.

use crate::errors::VtopError;
use crate::state_machine::{transition, BatchState};
use crate::types::{ProgressMarker, SourceType, TelemetryFormat};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A sealed or in-progress collection of telemetry records bound to a source
/// progress range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryBatch {
    pub batch_id: String,
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    /// Raw record bytes, in source order. Each entry is one record (line).
    pub records: Vec<Vec<u8>>,
    pub record_count: usize,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
    pub progress_start: ProgressMarker,
    pub progress_end: ProgressMarker,
    pub created_at: String,
    pub sealed_at: Option<String>,
    pub state: BatchState,
    /// When true, records are concatenated verbatim (whole-file / binary). When
    /// false, records are logical lines re-framed with a trailing newline so the
    /// object is byte-exact with the covered source byte range. See
    /// [`TelemetryBatch::to_record_bytes`].
    #[serde(default)]
    pub verbatim: bool,
}

impl TelemetryBatch {
    /// Total uncompressed byte size of all records (excluding separators).
    pub fn byte_size(&self) -> usize {
        self.records.iter().map(|r| r.len()).sum()
    }

    /// Serialize the records into a single contiguous buffer, one record per
    /// line (newline-terminated). Source order is preserved.
    pub fn to_record_bytes(&self) -> Vec<u8> {
        // Verbatim (whole-file / binary): concatenate raw bytes, add nothing —
        // the records ARE the object bytes.
        if self.verbatim {
            return self.records.concat();
        }
        // Line-framed: each logical record had its trailing newline stripped on
        // read, so re-add exactly one newline per record. This holds for a
        // single-record batch too, keeping the object byte-exact with the
        // covered source range.
        let total: usize = self.byte_size() + self.records.len();
        let mut buf = Vec::with_capacity(total);
        for rec in &self.records {
            buf.extend_from_slice(rec);
            if !rec.ends_with(b"\n") {
                buf.push(b'\n');
            }
        }
        buf
    }

    /// Seal the batch: transition `Batching -> Sealed`, stamp `sealed_at`, and
    /// finalize the record count. A sealed batch is immutable.
    pub fn seal(&mut self) -> Result<(), VtopError> {
        self.seal_at(Utc::now())
    }

    /// [`Self::seal`], stamping `at` - the instant the batch's contents were
    /// fixed, which the engine takes when it drains the batch from its buffer.
    /// Sealing can run later than that (the batch waits for an upload slot and
    /// for its first ledger write), and the time partition is taken from this
    /// instant, so it must be the one recorded (#559).
    pub fn seal_at(&mut self, at: chrono::DateTime<Utc>) -> Result<(), VtopError> {
        self.state = transition(self.state, BatchState::Sealed)?;
        self.sealed_at = Some(at.to_rfc3339());
        self.record_count = self.records.len();
        Ok(())
    }
}

/// The reason a batch was sealed (for observability and tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    MaxRecords,
    MaxBytes,
    MaxAge,
    PartitionChanged,
    ManualFlush,
    ShutdownFlush,
}

/// Thresholds that control adaptive batch sealing.
#[derive(Debug, Clone)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_batch_age_seconds: u64,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            max_bytes: 104_857_600,
            max_batch_age_seconds: 60,
        }
    }
}

/// Adaptive batcher. Accumulates records for a single source partition and
/// decides when to seal. It preserves source order and, for Kafka, does not
/// mix partitions (the engine constructs one batcher per partition).
pub struct AdaptiveBatcher {
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    pub limits: BatchLimits,
    records: Vec<Vec<u8>>,
    bytes: usize,
    started_at: Option<chrono::DateTime<Utc>>,
    first_timestamp: Option<String>,
    last_timestamp: Option<String>,
    progress_start: Option<ProgressMarker>,
}

impl AdaptiveBatcher {
    pub fn new(
        tenant: impl Into<String>,
        source_type: SourceType,
        source_name: impl Into<String>,
        format: TelemetryFormat,
        limits: BatchLimits,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            source_type,
            source_name: source_name.into(),
            format,
            limits,
            records: Vec::new(),
            bytes: 0,
            started_at: None,
            first_timestamp: None,
            last_timestamp: None,
            progress_start: None,
        }
    }

    /// Number of records currently buffered.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// How many bytes the buffered records hold (the sum the `max_bytes`
    /// threshold is checked against).
    pub fn byte_len(&self) -> usize {
        self.bytes
    }

    /// Where the buffered window starts: the first record's start marker.
    pub fn first_marker(&self) -> Option<&ProgressMarker> {
        self.progress_start.as_ref()
    }

    /// Add a record. `marker` is the progress position *after* this record.
    /// The first marker observed becomes the batch's `progress_start`.
    pub fn push(
        &mut self,
        record: Vec<u8>,
        start_marker: &ProgressMarker,
        timestamp: Option<String>,
    ) {
        if self.progress_start.is_none() {
            self.progress_start = Some(start_marker.clone());
            self.started_at = Some(now_or_epoch());
            self.first_timestamp = timestamp.clone();
        }
        self.last_timestamp = timestamp.or_else(|| self.last_timestamp.clone());
        self.bytes += record.len();
        self.records.push(record);
    }

    /// Check whether any sealing threshold is met based on the buffered state.
    pub fn should_seal(&self, now: chrono::DateTime<Utc>) -> Option<SealReason> {
        if self.records.is_empty() {
            return None;
        }
        if self.records.len() >= self.limits.max_records {
            return Some(SealReason::MaxRecords);
        }
        if self.bytes >= self.limits.max_bytes {
            return Some(SealReason::MaxBytes);
        }
        if let Some(started) = self.started_at {
            let age = (now - started).num_seconds().max(0) as u64;
            if age >= self.limits.max_batch_age_seconds {
                return Some(SealReason::MaxAge);
            }
        }
        None
    }

    /// Build and seal a [`TelemetryBatch`] from the buffered records, resetting
    /// the batcher. `progress_end` is the marker covering the final record.
    pub fn seal(&mut self, progress_end: ProgressMarker) -> Result<TelemetryBatch, VtopError> {
        let progress_start = self
            .progress_start
            .take()
            .ok_or_else(|| VtopError::Other("cannot seal an empty batch".into()))?;

        let records = std::mem::take(&mut self.records);
        let record_count = records.len();
        // Named by the whole range the batch holds, not its last read (#622).
        let batch_id = build_batch_id(
            &self.source_name,
            &ProgressMarker::spanning(&progress_start, &progress_end),
        );

        let mut batch = TelemetryBatch {
            batch_id,
            tenant: self.tenant.clone(),
            source_type: self.source_type.clone(),
            source_name: self.source_name.clone(),
            format: self.format.clone(),
            records,
            record_count,
            first_timestamp: self.first_timestamp.take(),
            last_timestamp: self.last_timestamp.take(),
            progress_start,
            progress_end,
            created_at: Utc::now().to_rfc3339(),
            sealed_at: None,
            // The batcher hands over a batch already past BATCHING.
            state: BatchState::Batching,
            // Framing is tracked by the engine's PendingBuffer and applied to
            // the final batch; the batcher's value is not propagated.
            verbatim: false,
        };

        // reset counters
        self.bytes = 0;
        self.started_at = None;

        batch.seal()?;
        Ok(batch)
    }
}

fn now_or_epoch() -> chrono::DateTime<Utc> {
    Utc::now()
}

/// Build a filesystem-safe batch id of the form
/// `vtop-<UTCstamp>-<source>-<range>-<short-uuid>`. `range` is the marker's
/// range token: pass the batch's whole range ([`ProgressMarker::spanning`]),
/// not its last read's end marker, or the id names only that read (#622).
///
/// The id names one ATTEMPT at a range, not the range: the stamp and the
/// random suffix make every attempt's ledger row and manifest distinct. The
/// object a batch's bytes are stored under is named by [`build_object_name`]
/// instead, so a replay of the same bytes finds it (#626).
pub fn build_batch_id(source_name: &str, range: &ProgressMarker) -> String {
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let safe_source = sanitize_token(source_name);
    let range = range.range_token();
    let suffix = Uuid::new_v4().simple().to_string();
    let short = &suffix[..8];
    let id = format!("vtop-{stamp}-{safe_source}-{range}-{short}");
    if id.len() <= MAX_NAME_STEM_BYTES {
        return id;
    }
    // A long source path would make the manifest, the receipt and the local
    // staging files of this batch too long a file name for the filesystem.
    let safe_source = shorten_token(source_name, &safe_source, SHORT_TOKEN_TAIL_BYTES);
    format!("vtop-{stamp}-{safe_source}-{range}-{short}")
}

/// The longest name stem (an object name or a batch id, before its
/// extension) the engine generates: 200 bytes.
///
/// POSIX `NAME_MAX` is 255 bytes for one path component, and the engine adds
/// to a stem: up to 14 bytes of extension (`.manifest.json`) and, on the
/// localfs backend, a staging suffix of up to 37 bytes
/// (`.<pid>-<counter>.partial`) while it copies the file in. 200 plus those 51
/// is 251, so a stem within this bound leaves every file name the engine
/// derives from it inside 255 bytes. A stem that already fits is never
/// changed: only a longer one has its source token shortened.
pub const MAX_NAME_STEM_BYTES: usize = 200;

/// The readable part (the tail) of a shortened source token, in bytes.
const SHORT_TOKEN_TAIL_BYTES: usize = 48;

/// The shorter tail used when the engine's own name also has to be
/// shortened for a stem to fit.
const SHORTEST_TOKEN_TAIL_BYTES: usize = 24;

/// Hex digits of the SHA-256 of the full, unsanitized source a shortened token
/// carries: 64 bits, enough that two sources sharing their last bytes do not
/// meet, since the token only has to tell the sources of one archive apart.
const SHORT_TOKEN_HASH_HEX: usize = 16;

/// A token cut to the readable END of its sanitized form plus a stable hash
/// of the FULL `raw` value: `<tail>-<16 hex>`. The hash covers everything
/// the cut drops, so two long paths that share their last bytes get
/// different tokens, and the same path always gets the same one (a replay of
/// a range must find the object a first attempt left, #626).
/// `safe` is `sanitize_token(raw)`, ASCII by construction.
fn shorten_token(raw: &str, safe: &str, tail_bytes: usize) -> String {
    let digest = crate::checksum::sha256_bytes(raw.as_bytes());
    let hash = &digest[..SHORT_TOKEN_HASH_HEX];
    // The tail: a path's end is the file's own name, what an operator
    // recognizes; its head is the directory every file of a tree shares.
    let kept = &safe[safe.len().saturating_sub(tail_bytes)..];
    format!("{kept}-{hash}")
}

/// How many hex digits of the content digest an object name carries: 128
/// bits. The name only has to find the candidate; nothing is reused on the
/// strength of the name alone, the stored bytes are verified against the
/// batch's full digest first.
pub const OBJECT_NAME_DIGEST_HEX: usize = 32;

/// The name of the object that holds a batch's bytes (#626), where `digest`
/// is the first [`OBJECT_NAME_DIGEST_HEX`] hex digits of the object's
/// checksum:
///
/// - a file or spool range: `vtop-<engine>-<source>-<range>[-i<inode>]-<digest>`;
/// - a Kafka range: `vtop-<source>-<range>-<digest>`.
///
/// An object is shared only by attempts at ONE unit's range (review): readers
/// may dedupe on the object URI. A file or spool file is a unit of one host,
/// so its name carries the engine (engine names are unique per instance) and
/// the inode: two engines archiving byte-identical windows of their own
/// files, or a new file at a path holding the old file's bytes, get separate
/// objects. A Kafka topic-partition is one unit wherever it is read, so its
/// name carries no engine: an engine that takes over another's replay (two
/// engines sharing a ledger) finds the object the first one stored.
///
/// Derived from what the object IS, not from the attempt that wrote it: a
/// replay that reads the same range of the same source and compresses it to
/// the same bytes (gzip and zstd output is a function of the input and the
/// level) gets the same name, and a replay that produces other bytes - a
/// different batch boundary, a replaced file, another compression setting -
/// gets another name. So a replay can find an object an earlier attempt left,
/// and can never land on an object that holds other bytes unless the stored
/// object was damaged, which the engine checks before it reuses anything.
/// `range` is the batch's whole range, as for [`build_batch_id`].
pub fn build_object_name(
    engine_name: &str,
    source_name: &str,
    range: &ProgressMarker,
    digest_hex: &str,
) -> String {
    let safe_source = sanitize_token(source_name);
    let safe_engine = sanitize_token(engine_name);
    let (has_host, unit) = match range {
        ProgressMarker::File { inode, .. } | ProgressMarker::SyslogSpool { inode, .. } => (
            true,
            inode.map(|inode| format!("-i{inode}")).unwrap_or_default(),
        ),
        ProgressMarker::Kafka { .. } => (false, String::new()),
    };
    let range = range.range_token();
    let digest: String = digest_hex
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(OBJECT_NAME_DIGEST_HEX)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    let name = |engine: &str, source: &str| {
        let host = if has_host {
            format!("{engine}-")
        } else {
            String::new()
        };
        format!("vtop-{host}{source}-{range}{unit}-{digest}")
    };
    // The name every archive written before this bound carries, and still
    // carries whenever it fits: existing objects are found by it on replay.
    let full = name(&safe_engine, &safe_source);
    if full.len() <= MAX_NAME_STEM_BYTES {
        return full;
    }
    // Too long for a file name: shorten the SOURCE token, the readable end
    // plus a hash of the whole path, so the name stays a pure function of the
    // source, the range and the bytes.
    let short_source = shorten_token(source_name, &safe_source, SHORT_TOKEN_TAIL_BYTES);
    let shortened = name(&safe_engine, &short_source);
    if shortened.len() <= MAX_NAME_STEM_BYTES {
        return shortened;
    }
    // Only an engine name of tens of bytes alongside a long source gets here;
    // with both tokens at their shortest the name fits at any offset or inode
    // a u64 can hold.
    name(
        &shorten_token(engine_name, &safe_engine, SHORTEST_TOKEN_TAIL_BYTES),
        &shorten_token(source_name, &safe_source, SHORTEST_TOKEN_TAIL_BYTES),
    )
}

/// Replace characters that are unsafe in object keys / file names.
pub fn sanitize_token(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka_marker(start: i64, end: i64) -> ProgressMarker {
        ProgressMarker::Kafka {
            topic: "app_events".into(),
            partition: 0,
            start_offset: start,
            end_offset: end,
            consumer_group: "vtop-engine".into(),
        }
    }

    #[test]
    fn an_object_name_is_the_same_for_the_same_bytes_of_the_same_range() {
        let range = kafka_marker(100, 249);
        let digest = "AB".repeat(32);
        let name = |engine: &str, source: &str, range: &ProgressMarker, digest: &str| {
            build_object_name(engine, source, range, digest)
        };
        let a = name("eng-1", "app.events", &range, &digest);
        assert_eq!(
            a,
            name("eng-1", "app.events", &range, &digest),
            "no clock and no randomness in the name"
        );
        assert_eq!(a, format!("vtop-app_events-p0-100-249-{}", "ab".repeat(16)));
        for (other, why) in [
            (
                name("eng-1", "app.events", &range, &"cd".repeat(32)),
                "other bytes",
            ),
            (
                name("eng-1", "app.events", &kafka_marker(100, 250), &digest),
                "another range",
            ),
            (name("eng-1", "other", &range, &digest), "another source"),
        ] {
            assert_ne!(a, other, "{why}, another name");
        }
        assert_eq!(
            a,
            name("eng-2", "app.events", &range, &digest),
            "a Kafka partition is one unit wherever it is read: no engine in the name"
        );
        let file = |inode| ProgressMarker::File {
            path: "/var/log/app.log".into(),
            inode,
            start_byte: 0,
            end_byte: 10,
            file_size: 10,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
            ctime: None,
        };
        assert_ne!(
            name("e", "/var/log/app.log", &file(Some(1)), &digest),
            name("e", "/var/log/app.log", &file(Some(2)), &digest),
            "another file at the path, another name"
        );
        assert_ne!(
            name("e1", "/var/log/app.log", &file(Some(1)), &digest),
            name("e2", "/var/log/app.log", &file(Some(1)), &digest),
            "a file is one host's unit: another engine, another name"
        );
        assert!(name("e", "/var/log/app.log", &file(Some(7)), &digest)
            .starts_with("vtop-e-_var_log_app_log-b0-10-i7-"));
        // Two attempts at the same range are still two batch ids.
        assert_ne!(
            build_batch_id("app.events", &range),
            build_batch_id("app.events", &range)
        );
    }

    // ---- Names are bounded (a long source path) ---------------------------

    /// The two functions as they were before the bound, kept so a test can
    /// show a name that fits did not change.
    fn legacy_object_name(
        engine_name: &str,
        source_name: &str,
        range: &ProgressMarker,
        digest_hex: &str,
    ) -> String {
        let safe_source = sanitize_token(source_name);
        let (host, unit) = match range {
            ProgressMarker::File { inode, .. } | ProgressMarker::SyslogSpool { inode, .. } => (
                format!("{}-", sanitize_token(engine_name)),
                inode.map(|inode| format!("-i{inode}")).unwrap_or_default(),
            ),
            ProgressMarker::Kafka { .. } => (String::new(), String::new()),
        };
        let range = range.range_token();
        let digest: String = digest_hex
            .chars()
            .filter(|c| c.is_ascii_hexdigit())
            .take(OBJECT_NAME_DIGEST_HEX)
            .map(|c| c.to_ascii_lowercase())
            .collect();
        format!("vtop-{host}{safe_source}-{range}{unit}-{digest}")
    }

    fn legacy_batch_id_with(source_name: &str, range: &ProgressMarker, id: &str) -> String {
        // `id` is a generated batch id; rebuild what the old code produced
        // from its own stamp and suffix.
        let parts: Vec<&str> = id.splitn(3, '-').collect();
        let stamp = parts[1];
        let suffix = id.rsplit('-').next().unwrap();
        format!(
            "vtop-{stamp}-{}-{}-{suffix}",
            sanitize_token(source_name),
            range.range_token()
        )
    }

    fn file_marker(path: &str, inode: Option<u64>, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::File {
            path: path.into(),
            inode,
            start_byte: start,
            end_byte: end,
            file_size: end,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
            ctime: None,
        }
    }

    /// A directory of 140 characters, as in the run that never drained.
    fn long_dir() -> String {
        format!("/srv/{}", "customer-data-archive/".repeat(6))
    }

    /// Every file name the engine derives from a batch id or an object name,
    /// for the longest object extension (`.syslog.zst`).
    fn derived_names(object_name: &str, batch_id: &str) -> Vec<String> {
        vec![
            object_name.to_string(),
            batch_id.to_string(),
            crate::partitioning::object_file_name(
                object_name,
                TelemetryFormat::Syslog,
                crate::types::CompressionType::Zstd,
            ),
            crate::partitioning::manifest_file_name(batch_id),
            crate::partitioning::receipt_file_name(batch_id),
        ]
    }

    #[test]
    fn a_long_source_path_yields_names_that_fit_a_file_name() {
        let digest = "ab".repeat(16);
        let path = format!("{}app-service-2026-11-20.log", long_dir());
        // The offsets of a 1.5 GB file, in the widest case a u64 allows too.
        for (start, end) in [
            (0, 4_194_219),
            (4_194_219, 8_388_519),
            (1_500_000_000, 1_504_194_219),
            (u64::MAX - 1, u64::MAX),
        ] {
            let range = file_marker(&path, Some(u64::MAX), start, end);
            let object = build_object_name("engine-1", &path, &range, &digest);
            let id = build_batch_id(&path, &range);
            for name in derived_names(&object, &id) {
                assert!(
                    name.len() <= 255,
                    "{} bytes for range {start}-{end}: {name}",
                    name.len()
                );
            }
            // The bound leaves room for the localfs staging suffix too.
            assert!(object.len() <= MAX_NAME_STEM_BYTES, "{object}");
            assert!(id.len() <= MAX_NAME_STEM_BYTES, "{id}");
            assert!(
                object.contains("customer-data-archive_app-service-2026-11-20_log-"),
                "the file's own name stays readable: {object}"
            );
        }
        // The case from the run: this path made the first range's object name
        // 237 bytes and the second's 261.
        let before = legacy_object_name(
            "engine-1",
            &path,
            &file_marker(&path, Some(123), 4_194_219, 8_388_519),
            &digest,
        );
        assert!(
            before.len() + ".raw.zst".len() > 255 - 20,
            "{}",
            before.len()
        );
    }

    #[test]
    fn an_over_long_engine_name_with_a_long_path_still_fits() {
        let path = format!("{}app.log", long_dir());
        let engine = "e".repeat(120);
        let range = file_marker(&path, Some(u64::MAX), u64::MAX - 1, u64::MAX);
        let object = build_object_name(&engine, &path, &range, &"cd".repeat(16));
        assert!(object.len() <= 200, "{} bytes: {object}", object.len());
    }

    #[test]
    fn the_same_range_of_the_same_long_path_has_the_same_name() {
        let digest = "ab".repeat(16);
        let path = format!("{}app.log", long_dir());
        let range = file_marker(&path, Some(9), 4_194_219, 8_388_519);
        let a = build_object_name("e", &path, &range, &digest);
        let b = build_object_name("e", &path, &range, &digest);
        assert_eq!(a, b, "a replay finds the object the first attempt left");
        // The name is a pure function of the inputs, stated outright: a
        // change here changes where a replay looks (#626).
        assert_eq!(
            a,
            format!(
                "vtop-e-tomer-data-archive_customer-data-archive_app_log-{}-b4194219-8388519-i9-{digest}",
                &crate::checksum::sha256_bytes(path.as_bytes())[..16]
            )
        );
    }

    #[test]
    fn two_long_paths_that_share_a_prefix_have_different_names() {
        let digest = "ab".repeat(16);
        // Same directory, so the same first 100 bytes, then other files.
        let a_path = format!("{}app-a.log", long_dir());
        let b_path = format!("{}app-b.log", long_dir());
        let a = build_object_name("e", &a_path, &file_marker(&a_path, Some(1), 0, 10), &digest);
        let b = build_object_name("e", &b_path, &file_marker(&b_path, Some(1), 0, 10), &digest);
        assert_ne!(a, b);
        // Same file name under directories that differ only at the start, so
        // the kept tail is identical and only the hash tells them apart.
        let x_path = format!("/srv-x{}app.log", long_dir());
        let y_path = format!("/srv-y{}app.log", long_dir());
        let x = build_object_name("e", &x_path, &file_marker(&x_path, Some(1), 0, 10), &digest);
        let y = build_object_name("e", &y_path, &file_marker(&y_path, Some(1), 0, 10), &digest);
        assert_ne!(x, y, "{x} / {y}");
        let strip = |id: String| id[id.len() - 8..].to_string();
        let x_id = build_batch_id(&x_path, &file_marker(&x_path, Some(1), 0, 10));
        let y_id = build_batch_id(&y_path, &file_marker(&y_path, Some(1), 0, 10));
        assert_ne!(
            x_id.replace(&strip(x_id.clone()), ""),
            y_id.replace(&strip(y_id.clone()), ""),
            "the ids differ apart from their random suffix"
        );
        // Paths that sanitize to the same token still differ: the hash covers
        // the raw path.
        let c_path = format!("{0}{0}app a.log", long_dir());
        let d_path = format!("{0}{0}app_a.log", long_dir());
        assert_ne!(
            build_object_name("e", &c_path, &file_marker(&c_path, Some(1), 0, 10), &digest),
            build_object_name("e", &d_path, &file_marker(&d_path, Some(1), 0, 10), &digest),
        );
    }

    #[test]
    fn a_name_that_already_fits_is_unchanged_byte_for_byte() {
        let digest = "AB".repeat(32);
        let cases: Vec<(&str, String, ProgressMarker)> = vec![
            (
                "eng-1",
                "/var/log/app.log".into(),
                file_marker("/var/log/app.log", Some(7), 0, 10),
            ),
            (
                "eng-1",
                "/var/log/app.log".into(),
                file_marker("/var/log/app.log", None, 4_194_219, 8_388_519),
            ),
            ("e", "app.events".into(), kafka_marker(100, 249)),
            (
                "host-a",
                "x".repeat(100),
                file_marker(&"x".repeat(100), Some(12345), 1_500_000_000, 1_504_194_219),
            ),
            // The longest source that still fits at these offsets: 200 bytes
            // is the bound, not the source's own length.
            (
                "e",
                "s".repeat(MAX_NAME_STEM_BYTES - 71),
                file_marker(&"s".repeat(MAX_NAME_STEM_BYTES - 71), Some(1), 0, 10),
            ),
        ];
        for (engine, source, range) in cases {
            let now = build_object_name(engine, &source, &range, &digest);
            assert_eq!(now, legacy_object_name(engine, &source, &range, &digest));
            assert!(now.len() <= MAX_NAME_STEM_BYTES, "{}", now.len());
            let id = build_batch_id(&source, &range);
            assert_eq!(id, legacy_batch_id_with(&source, &range, &id));
        }
        assert_eq!(
            build_object_name(
                "eng-1",
                "/var/log/app.log",
                &file_marker("/var/log/app.log", Some(7), 0, 10),
                &digest
            ),
            format!("vtop-eng-1-_var_log_app_log-b0-10-i7-{}", "ab".repeat(16)),
            "a literal, so the expected name is not computed by the code under test"
        );
    }

    #[test]
    fn a_name_one_byte_over_the_bound_is_shortened_and_one_at_it_is_not() {
        let digest = "ab".repeat(16);
        let fixed = legacy_object_name("e", "", &file_marker("", Some(1), 0, 10), &digest).len();
        let at = "s".repeat(MAX_NAME_STEM_BYTES - fixed);
        let over = format!("{at}s");
        let at_name = build_object_name("e", &at, &file_marker(&at, Some(1), 0, 10), &digest);
        let over_name = build_object_name("e", &over, &file_marker(&over, Some(1), 0, 10), &digest);
        assert_eq!(at_name.len(), MAX_NAME_STEM_BYTES);
        assert!(at_name.contains(&at), "unchanged at the bound");
        assert!(!over_name.contains(&over), "shortened one byte over");
        assert!(over_name.len() < MAX_NAME_STEM_BYTES);
    }

    #[test]
    fn seals_on_max_records() {
        let limits = BatchLimits {
            max_records: 3,
            max_bytes: usize::MAX,
            max_batch_age_seconds: u64::MAX,
        };
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            limits,
        );
        for i in 0..3 {
            b.push(b"rec".to_vec(), &kafka_marker(i, i), None);
        }
        assert_eq!(b.should_seal(Utc::now()), Some(SealReason::MaxRecords));
        let batch = b.seal(kafka_marker(0, 2)).unwrap();
        assert_eq!(batch.record_count, 3);
        assert_eq!(batch.state, BatchState::Sealed);
        assert!(batch.sealed_at.is_some());
    }

    #[test]
    fn seals_on_max_bytes() {
        let limits = BatchLimits {
            max_records: usize::MAX,
            max_bytes: 10,
            max_batch_age_seconds: u64::MAX,
        };
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            limits,
        );
        b.push(vec![0u8; 20], &kafka_marker(0, 0), None);
        assert_eq!(b.should_seal(Utc::now()), Some(SealReason::MaxBytes));
    }

    #[test]
    fn seals_on_max_age() {
        let limits = BatchLimits {
            max_records: usize::MAX,
            max_bytes: usize::MAX,
            max_batch_age_seconds: 30,
        };
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::File,
            "/x.log",
            TelemetryFormat::Raw,
            limits,
        );
        b.push(b"rec".to_vec(), &kafka_marker(0, 0), None);
        // Fresh buffer: well within the age window.
        assert_eq!(b.should_seal(Utc::now()), None);
        // Past the age window: must seal on age (records/bytes never trip here).
        let later = Utc::now() + chrono::Duration::seconds(31);
        assert_eq!(b.should_seal(later), Some(SealReason::MaxAge));
    }

    #[test]
    fn preserves_record_order() {
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            BatchLimits::default(),
        );
        for i in 0..5u8 {
            b.push(vec![i], &kafka_marker(i as i64, i as i64), None);
        }
        let batch = b.seal(kafka_marker(0, 4)).unwrap();
        let order: Vec<u8> = batch.records.iter().map(|r| r[0]).collect();
        assert_eq!(order, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn empty_batcher_does_not_seal() {
        let b = AdaptiveBatcher::new(
            "default",
            SourceType::File,
            "/x.log",
            TelemetryFormat::Raw,
            BatchLimits::default(),
        );
        assert_eq!(b.should_seal(Utc::now()), None);
    }

    #[test]
    fn batch_id_is_deterministic_in_shape() {
        let id = build_batch_id("app_events", &kafka_marker(481000, 482499));
        assert!(id.starts_with("vtop-"));
        assert!(id.contains("app_events"));
        assert!(id.contains("p0-481000-482499"));
    }

    /// #622: a batch sealed from several reads is named by its whole range;
    /// its end marker (the last read's, carrying that read's start) is kept.
    #[test]
    fn a_batch_of_several_reads_is_named_by_its_whole_range() {
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            BatchLimits::default(),
        );
        b.push(b"a".to_vec(), &kafka_marker(100, 100), None);
        b.push(b"b".to_vec(), &kafka_marker(150, 150), None);
        let batch = b.seal(kafka_marker(150, 249)).unwrap();
        assert!(
            batch.batch_id.contains("-p0-100-249-"),
            "{}",
            batch.batch_id
        );
        assert_eq!(batch.progress_end, kafka_marker(150, 249));
    }

    // The tests below kill specific surviving mutants reported by cargo-mutants
    // (issue #103) — each pins a return value the earlier tests only used
    // indirectly.

    fn batch_with_records(records: Vec<Vec<u8>>, verbatim: bool) -> TelemetryBatch {
        let record_count = records.len();
        TelemetryBatch {
            batch_id: "b".into(),
            tenant: "default".into(),
            source_type: SourceType::Kafka,
            source_name: "app_events".into(),
            format: TelemetryFormat::Raw,
            records,
            record_count,
            first_timestamp: None,
            last_timestamp: None,
            progress_start: kafka_marker(0, 0),
            progress_end: kafka_marker(0, 0),
            created_at: Utc::now().to_rfc3339(),
            sealed_at: None,
            state: BatchState::Batching,
            verbatim,
        }
    }

    #[test]
    fn byte_size_sums_record_lengths() {
        // 3 + 4 = 7 — kills the `-> 0` and `-> 1` mutants on byte_size.
        let batch = batch_with_records(vec![b"abc".to_vec(), b"defg".to_vec()], false);
        assert_eq!(batch.byte_size(), 7);
        assert_eq!(batch_with_records(vec![], false).byte_size(), 0);
    }

    #[test]
    fn to_record_bytes_line_framed_appends_newlines() {
        let batch = batch_with_records(
            vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()],
            false,
        );
        let buf = batch.to_record_bytes();
        assert_eq!(buf, b"aaaa\nbbbb\ncccc\n");
        // The buffer is preallocated to exactly byte_size + record_count (one
        // newline per record). Sum vs product diverge here (12+3=15 vs 12*3=36),
        // so an over-allocation pins the `+`→`*` mutant on the capacity math.
        assert!(buf.capacity() <= batch.byte_size() + batch.records.len());
    }

    #[test]
    fn to_record_bytes_verbatim_concatenates_without_framing() {
        let batch = batch_with_records(vec![b"aa".to_vec(), b"bb".to_vec()], true);
        assert_eq!(batch.to_record_bytes(), b"aabb");
    }

    #[test]
    fn len_and_is_empty_track_pushes() {
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Raw,
            BatchLimits::default(),
        );
        // Kills `is_empty -> false` and `len -> 1`.
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        b.push(b"one".to_vec(), &kafka_marker(0, 0), None);
        b.push(b"two".to_vec(), &kafka_marker(1, 1), None);
        // Kills `is_empty -> true` and `len -> 0` / `-> 1`.
        assert!(!b.is_empty());
        assert_eq!(b.len(), 2);
    }
}
