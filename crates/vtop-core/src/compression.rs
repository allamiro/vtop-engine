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
use std::io::Write;
use std::path::{Path, PathBuf};

/// Result of compressing a batch: the local object path and its byte size.
#[derive(Debug, Clone)]
pub struct CompressedObject {
    pub path: PathBuf,
    pub size_bytes: u64,
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

    let payload = batch.to_record_bytes();

    let format_ext = batch.format.extension();
    let (compressed, extension) = match compression {
        CompressionType::Gzip => {
            let mut enc = GzEncoder::new(Vec::new(), GzLevel::new(clamp_gzip_level(level)));
            enc.write_all(&payload)
                .map_err(|e| VtopError::Compression(e.to_string()))?;
            let out = enc
                .finish()
                .map_err(|e| VtopError::Compression(e.to_string()))?;
            (out, format!("{format_ext}.gz"))
        }
        CompressionType::Zstd => {
            let out = zstd::encode_all(payload.as_slice(), clamp_zstd_level(level))
                .map_err(|e| VtopError::Compression(e.to_string()))?;
            (out, format!("{format_ext}.zst"))
        }
        CompressionType::None => (payload, format_ext.to_string()),
    };

    std::fs::create_dir_all(work_dir)?;
    let path = work_dir.join(format!("{}.{}", batch.batch_id, extension));
    std::fs::write(&path, &compressed)?;

    Ok(CompressedObject {
        path,
        size_bytes: compressed.len() as u64,
        compression,
        extension,
    })
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
    }

    #[test]
    fn zstd_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let batch = sealed_batch(vec![b"a", b"b"]);
        let obj = compress_batch(&batch, CompressionType::Zstd, 3, dir.path()).unwrap();
        let bytes = std::fs::read(&obj.path).unwrap();
        let out = zstd::decode_all(bytes.as_slice()).unwrap();
        assert_eq!(out, b"a\nb\n");
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
