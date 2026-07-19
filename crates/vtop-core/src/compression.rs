//! Compression of sealed batches.
//!
//! Rules:
//! * Only SEALED batches may be compressed.
//! * Compression output is written to the engine `work_dir`.
//! * The compressed file is treated as immutable after checksum.
//! * Original record ordering is preserved (the batch serializes in order).
//! * Compression metadata is recorded in the manifest.

use crate::batch::TelemetryBatch;
use crate::errors::VtopError;
use crate::state_machine::BatchState;
use crate::types::CompressionType;
use flate2::write::GzEncoder;
use flate2::Compression as GzLevel;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Result of compressing a batch: the local object path and its byte size.
#[derive(Debug, Clone)]
pub struct CompressedObject {
    pub path: PathBuf,
    pub size_bytes: u64,
    /// Uncompressed payload size (bytes fed to the compressor).
    pub uncompressed_bytes: u64,
    pub compression: CompressionType,
    /// File-name component, e.g. `cef.gz` or `jsonl.zst` or `raw`.
    pub extension: String,
}

/// Compress a sealed batch into `work_dir`, returning the object descriptor.
///
/// The batch MUST be in [`BatchState::Sealed`]; compressing an unsealed batch
/// is rejected to preserve the state machine ordering.
pub fn compress_batch(
    batch: &TelemetryBatch,
    compression: CompressionType,
    level: i32,
    work_dir: &Path,
) -> Result<CompressedObject, VtopError> {
    if batch.state != BatchState::Sealed {
        return Err(VtopError::InvalidStateForOperation {
            expected: BatchState::Sealed,
            actual: batch.state,
        });
    }

    let format_ext = batch.format.extension();
    let extension = match compression {
        CompressionType::Gzip => format!("{format_ext}.gz"),
        CompressionType::Zstd => format!("{format_ext}.zst"),
        CompressionType::None => format_ext.to_string(),
    };

    std::fs::create_dir_all(work_dir)?;
    let path = work_dir.join(format!("{}.{}", batch.batch_id, extension));
    let result = (|| -> Result<u64, VtopError> {
        let file = File::create(&path)?;
        match compression {
            CompressionType::Gzip => {
                let mut encoder = GzEncoder::new(file, GzLevel::new(clamp_gzip_level(level)));
                let written = write_batch_payload(batch, &mut encoder)?;
                encoder
                    .finish()
                    .map_err(|e| VtopError::Compression(e.to_string()))?;
                Ok(written)
            }
            CompressionType::Zstd => {
                let mut encoder = zstd::Encoder::new(file, clamp_zstd_level(level))
                    .map_err(|e| VtopError::Compression(e.to_string()))?;
                let written = write_batch_payload(batch, &mut encoder)?;
                encoder
                    .finish()
                    .map_err(|e| VtopError::Compression(e.to_string()))?;
                Ok(written)
            }
            CompressionType::None => {
                let mut writer = BufWriter::new(file);
                let written = write_batch_payload(batch, &mut writer)?;
                writer.flush()?;
                Ok(written)
            }
        }
    })();
    let uncompressed_bytes = match result {
        Ok(bytes) => bytes,
        Err(error) => {
            // A failed encoder must not leave a plausible-looking partial
            // object for a later operator or recovery scan to mistake as
            // complete.
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
    };
    let size_bytes = match std::fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) => {
            let _ = std::fs::remove_file(&path);
            return Err(error.into());
        }
    };

    Ok(CompressedObject {
        path,
        size_bytes,
        uncompressed_bytes,
        compression,
        extension,
    })
}

/// Stream a batch's exact wire framing into `writer` without first
/// materializing a second batch-sized payload buffer.
fn write_batch_payload<W: Write>(batch: &TelemetryBatch, writer: &mut W) -> Result<u64, VtopError> {
    let mut written = 0_u64;
    for record in &batch.records {
        writer
            .write_all(record)
            .map_err(|e| VtopError::Compression(e.to_string()))?;
        written += record.len() as u64;
        if !batch.verbatim && !record.ends_with(b"\n") {
            writer
                .write_all(b"\n")
                .map_err(|e| VtopError::Compression(e.to_string()))?;
            written += 1;
        }
    }
    Ok(written)
}

fn clamp_gzip_level(level: i32) -> u32 {
    level.clamp(0, 9) as u32
}

fn clamp_zstd_level(level: i32) -> i32 {
    level.clamp(1, 22)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ProgressMarker, SourceType, TelemetryFormat};

    fn sealed_batch(records: Vec<&[u8]>) -> TelemetryBatch {
        sealed_batch_framed(records, false)
    }

    fn sealed_batch_framed(records: Vec<&[u8]>, verbatim: bool) -> TelemetryBatch {
        let marker = ProgressMarker::File {
            path: "/x.log".into(),
            inode: None,
            start_byte: 0,
            end_byte: 10,
            file_size: 10,
            mtime: "now".into(),
        };
        TelemetryBatch {
            batch_id: "vtop-test".into(),
            tenant: "default".into(),
            source_type: SourceType::File,
            source_name: "/x.log".into(),
            format: TelemetryFormat::Raw,
            records: records.into_iter().map(|r| r.to_vec()).collect(),
            record_count: 0,
            first_timestamp: None,
            last_timestamp: None,
            progress_start: marker.clone(),
            progress_end: marker,
            created_at: "now".into(),
            sealed_at: Some("now".into()),
            state: BatchState::Sealed,
            verbatim,
        }
    }

    #[test]
    fn gzip_roundtrip_preserves_order() {
        let dir = tempfile::tempdir().unwrap();
        let batch = sealed_batch(vec![b"line-1", b"line-2", b"line-3"]);
        let obj = compress_batch(&batch, CompressionType::Gzip, 6, dir.path()).unwrap();
        assert!(obj.extension.ends_with(".gz"));

        let bytes = std::fs::read(&obj.path).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(bytes.as_slice());
        let mut out = String::new();
        use std::io::Read;
        decoder.read_to_string(&mut out).unwrap();
        assert_eq!(out, "line-1\nline-2\nline-3\n");
        assert_eq!(obj.uncompressed_bytes, out.len() as u64);
    }

    #[test]
    fn zstd_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let batch = sealed_batch(vec![b"a", b"b"]);
        let obj = compress_batch(&batch, CompressionType::Zstd, 3, dir.path()).unwrap();
        let bytes = std::fs::read(&obj.path).unwrap();
        let out = zstd::decode_all(bytes.as_slice()).unwrap();
        assert_eq!(out, b"a\nb\n");
        assert_eq!(obj.uncompressed_bytes, out.len() as u64);
    }

    #[test]
    fn uncompressed_output_streams_with_exact_framing() {
        let dir = tempfile::tempdir().unwrap();
        let batch = sealed_batch(vec![b"a", b"b\n"]);
        let obj = compress_batch(&batch, CompressionType::None, 0, dir.path()).unwrap();
        assert_eq!(std::fs::read(&obj.path).unwrap(), b"a\nb\n");
        assert_eq!(obj.uncompressed_bytes, 4);
        assert_eq!(obj.size_bytes, 4);
    }

    #[test]
    fn single_line_batch_is_byte_exact() {
        // One logical line (newline stripped on read) re-frames to "x\n", which
        // is byte-exact with the covered source range.
        let batch = sealed_batch(vec![b"x"]);
        assert_eq!(batch.to_record_bytes(), b"x\n");
    }

    #[test]
    fn verbatim_preserves_binary_bytes() {
        // Whole-file / binary: a single record with no trailing newline must be
        // emitted exactly, with nothing appended.
        let raw: &[u8] = &[0x00, 0x01, 0xff, 0x42];
        let batch = sealed_batch_framed(vec![raw], true);
        assert_eq!(batch.to_record_bytes(), raw);
    }

    #[test]
    fn refuses_unsealed_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut batch = sealed_batch(vec![b"x"]);
        batch.state = BatchState::Batching;
        let err = compress_batch(&batch, CompressionType::Gzip, 6, dir.path()).unwrap_err();
        assert!(matches!(err, VtopError::InvalidStateForOperation { .. }));
    }
}
