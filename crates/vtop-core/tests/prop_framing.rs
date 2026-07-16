//! Property tests for record framing / byte-exactness.
//!
//! Regression guard for a real bug: a single-record line batch was emitted
//! verbatim while line records had their trailing newline stripped on read, so
//! the stored object did not match the source byte range it claimed to cover.
//! Fixed with the `verbatim` framing flag; these properties pin the contract:
//!
//! * line-framed  -> object bytes are byte-exact with the covered source range;
//! * verbatim     -> object bytes are the raw record bytes, untouched.

use proptest::prelude::*;
use vtop_core::batch::TelemetryBatch;
use vtop_core::compression::compress_batch;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};

/// A single logical line: any bytes except the newline delimiter itself.
fn line() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        any::<u8>().prop_filter("no newline", |b| *b != b'\n'),
        0..40,
    )
}

fn marker() -> ProgressMarker {
    ProgressMarker::File {
        path: "/x.log".into(),
        inode: None,
        start_byte: 0,
        end_byte: 0,
        file_size: 0,
        mtime: "now".into(),
    }
}

fn batch(records: Vec<Vec<u8>>, verbatim: bool) -> TelemetryBatch {
    let record_count = records.len();
    TelemetryBatch {
        batch_id: "vtop-prop".into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: "/x.log".into(),
        format: TelemetryFormat::Raw,
        records,
        record_count,
        first_timestamp: None,
        last_timestamp: None,
        progress_start: marker(),
        progress_end: marker(),
        created_at: "now".into(),
        sealed_at: Some("now".into()),
        state: BatchState::Sealed,
        verbatim,
    }
}

proptest! {
    /// Line framing is byte-exact: for any set of lines, the object bytes equal
    /// the exact source bytes those lines were read from. This holds for the
    /// single-record case too — which is precisely where the original bug was.
    #[test]
    fn line_framing_is_byte_exact(lines in prop::collection::vec(line(), 1..12)) {
        // The source file these lines came from: each line terminated by \n.
        let mut source = Vec::new();
        for l in &lines {
            source.extend_from_slice(l);
            source.push(b'\n');
        }
        let out = batch(lines, false).to_record_bytes();
        prop_assert_eq!(out, source, "object bytes must equal the covered source range");
    }

    /// The regression itself, isolated: a one-line batch must not lose its newline.
    #[test]
    fn single_line_batch_keeps_its_newline(l in line()) {
        let mut expected = l.clone();
        expected.push(b'\n');
        prop_assert_eq!(batch(vec![l], false).to_record_bytes(), expected);
    }

    /// Verbatim framing preserves arbitrary binary exactly: nothing appended,
    /// nothing stripped — including inputs that do not end with a newline.
    #[test]
    fn verbatim_preserves_bytes_exactly(data in prop::collection::vec(any::<u8>(), 0..256)) {
        prop_assert_eq!(batch(vec![data.clone()], true).to_record_bytes(), data);
    }

    /// Framing must never silently drop or invent records.
    #[test]
    fn framing_preserves_record_count_and_order(lines in prop::collection::vec(line(), 1..12)) {
        let out = batch(lines.clone(), false).to_record_bytes();
        let split: Vec<&[u8]> = out.strip_suffix(b"\n")
            .unwrap_or(&out)
            .split(|b| *b == b'\n')
            .collect();
        prop_assert_eq!(split.len(), lines.len(), "record count changed under framing");
        for (got, want) in split.iter().zip(lines.iter()) {
            prop_assert_eq!(*got, want.as_slice(), "record content/order changed");
        }
    }

    /// End-to-end round trip: framing -> gzip -> decompress reproduces the source
    /// bytes exactly. This is what a consumer actually reads back out of storage.
    #[test]
    fn gzip_round_trip_reproduces_source_bytes(lines in prop::collection::vec(line(), 1..8)) {
        let mut source = Vec::new();
        for l in &lines {
            source.extend_from_slice(l);
            source.push(b'\n');
        }
        let b = batch(lines, false);
        let dir = tempfile::tempdir().unwrap();
        let obj = compress_batch(&b, CompressionType::Gzip, 6, dir.path())
            .expect("compress must succeed for a sealed batch");
        let raw = std::fs::read(&obj.path).unwrap();
        let mut out = Vec::new();
        {
            use std::io::Read;
            flate2::read::GzDecoder::new(&raw[..]).read_to_end(&mut out).unwrap();
        }
        prop_assert_eq!(out, source, "decompressed object must equal the source bytes");
    }

    /// Verbatim binary survives the same round trip untouched.
    #[test]
    fn gzip_round_trip_preserves_verbatim_binary(data in prop::collection::vec(any::<u8>(), 1..256)) {
        let b = batch(vec![data.clone()], true);
        let dir = tempfile::tempdir().unwrap();
        let obj = compress_batch(&b, CompressionType::Gzip, 6, dir.path()).unwrap();
        let raw = std::fs::read(&obj.path).unwrap();
        let mut out = Vec::new();
        {
            use std::io::Read;
            flate2::read::GzDecoder::new(&raw[..]).read_to_end(&mut out).unwrap();
        }
        prop_assert_eq!(out, data);
    }
}
