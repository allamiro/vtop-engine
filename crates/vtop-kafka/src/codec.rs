//! Decompression of a compressed RecordBatch's records section (#596).
//!
//! A Kafka producer compresses the records of a batch as one stream: the
//! batch header stays plain, and everything after the record count is the
//! codec's output. The gateway decompresses that stream on Produce and keeps
//! the records as ordinary native records. The native log has no compressed
//! form, and keeping the client's compressed bytes would be a log-format
//! change. Fetch therefore serves an uncompressed batch.
//!
//! Every stream is untrusted input. It is read through a bounded reader, so
//! a stream that expands past `limit` stops at `limit + 1` bytes and is
//! refused. A small batch cannot become an allocation the gateway did not
//! agree to.

use std::io::Read;

/// Kafka's codec ids, the low three bits of a batch's `attributes`.
pub const GZIP: i16 = 1;
pub const SNAPPY: i16 = 2;
pub const LZ4: i16 = 3;
pub const ZSTD: i16 = 4;

/// The widest zstd window a frame may ask for: 2^24 bytes, the field cap.
const ZSTD_WINDOW_LOG_MAX: u32 = 24;

/// The codecs this build decompresses, in the order a refusal lists them.
pub const SERVED: [i16; 2] = [GZIP, ZSTD];

/// The name a producer's `compression.type` uses for a codec id.
pub fn name(codec: i16) -> &'static str {
    match codec {
        GZIP => "gzip",
        SNAPPY => "snappy",
        LZ4 => "lz4",
        ZSTD => "zstd",
        _ => "unknown",
    }
}

/// The values a producer's `compression.type` may take against this
/// gateway, for a refusal's remedy.
pub fn served_list() -> String {
    let mut names: Vec<&str> = SERVED.iter().map(|c| name(*c)).collect();
    names.push("none");
    names.join(", ")
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// A codec Kafka defines and this build does not decompress.
    #[error("codec {codec} ({}) is not served", name(*codec))]
    NotServed { codec: i16 },
    /// A codec id Kafka does not define (5-7).
    #[error("codec {codec} is not a codec Kafka defines")]
    Unknown { codec: i16 },
    /// The stream does not decompress.
    #[error("the {} stream does not decompress: {reason}", name(*codec))]
    Corrupt { codec: i16, reason: String },
    /// The stream expands past the limit.
    #[error("the {} stream expands past {limit} bytes", name(*codec))]
    TooLarge { codec: i16, limit: usize },
}

/// Decompress one batch's records section, refusing output past `limit`.
pub fn decompress(codec: i16, input: &[u8], limit: usize) -> Result<Vec<u8>, CodecError> {
    match codec {
        GZIP => bounded(
            codec,
            // Multi-member, as Java's GZIPInputStream reads: a member boundary
            // inside one batch is legal gzip, and stopping at it would drop
            // the records after it.
            flate2::read::MultiGzDecoder::new(input),
            limit,
        ),
        ZSTD => {
            let corrupt = |error: std::io::Error| CodecError::Corrupt {
                codec,
                reason: error.to_string(),
            };
            let mut decoder = zstd::stream::read::Decoder::with_buffer(input).map_err(corrupt)?;
            // The history window is allocated when the frame header is read,
            // before any output, and libzstd's default ceiling is 128 MiB: a
            // ten-byte frame could cost that much (review). No batch here may
            // expand past MAX_FIELD_BYTES (16 MiB = 2^24), so no window wider
            // than that is needed; a frame asking for more is refused.
            decoder
                .window_log_max(ZSTD_WINDOW_LOG_MAX)
                .map_err(corrupt)?;
            bounded(codec, decoder, limit)
        }
        SNAPPY | LZ4 => Err(CodecError::NotServed { codec }),
        _ => Err(CodecError::Unknown { codec }),
    }
}

/// Read `reader` to its end or to `limit + 1` bytes, whichever is first.
///
/// In chunks, with the buffer's capacity never above `limit + 1`: a plain
/// `read_to_end` doubles its buffer, so output near the limit could hold
/// twice the limit, and more while it copies (review).
fn bounded(codec: i16, mut reader: impl Read, limit: usize) -> Result<Vec<u8>, CodecError> {
    const CHUNK: usize = 64 * 1024;
    let cap = limit.saturating_add(1);
    let mut out: Vec<u8> = Vec::new();
    loop {
        if out.len() == out.capacity() {
            if out.len() >= cap {
                return Err(CodecError::TooLarge { codec, limit });
            }
            let target = out.capacity().saturating_mul(2).max(CHUNK).min(cap);
            out.reserve_exact(target - out.len());
        }
        let start = out.len();
        let end = out.capacity().min(cap);
        out.resize(end, 0);
        match reader.read(&mut out[start..end]) {
            Ok(0) => {
                out.truncate(start);
                break;
            }
            Ok(n) => out.truncate(start + n),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => out.truncate(start),
            Err(error) => {
                return Err(CodecError::Corrupt {
                    codec,
                    reason: error.to_string(),
                })
            }
        }
    }
    if out.len() > limit {
        return Err(CodecError::TooLarge { codec, limit });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn served_codecs_round_trip() {
        let plain = b"records section".repeat(100);
        assert_eq!(decompress(GZIP, &gzip(&plain), 1 << 20).unwrap(), plain);
        let z = zstd::encode_all(&plain[..], 3).unwrap();
        assert_eq!(decompress(ZSTD, &z, 1 << 20).unwrap(), plain);
    }

    /// Java writes one gzip member per batch, but a concatenation is legal
    /// gzip and must not lose the second member.
    #[test]
    fn a_multi_member_gzip_stream_is_read_whole() {
        let mut both = gzip(b"first ");
        both.extend(gzip(b"second"));
        assert_eq!(decompress(GZIP, &both, 1024).unwrap(), b"first second");
    }

    #[test]
    fn output_exactly_at_the_limit_is_accepted_and_one_byte_over_is_not() {
        let plain = vec![7_u8; 4096];
        for (codec, packed) in [
            (GZIP, gzip(&plain)),
            (ZSTD, zstd::encode_all(&plain[..], 3).unwrap()),
        ] {
            assert_eq!(decompress(codec, &packed, 4096).unwrap().len(), 4096);
            assert_eq!(
                decompress(codec, &packed, 4095),
                Err(CodecError::TooLarge { codec, limit: 4095 })
            );
        }
    }

    #[test]
    fn unserved_and_unknown_codecs_are_refused_by_name() {
        assert_eq!(
            decompress(SNAPPY, b"x", 10),
            Err(CodecError::NotServed { codec: SNAPPY })
        );
        assert_eq!(
            decompress(LZ4, b"x", 10),
            Err(CodecError::NotServed { codec: LZ4 })
        );
        for codec in 5..=7 {
            assert_eq!(
                decompress(codec, b"x", 10),
                Err(CodecError::Unknown { codec })
            );
        }
        assert_eq!(served_list(), "gzip, zstd, none");
    }

    /// A zstd frame that asks for a 128 MiB window is refused before the
    /// window is allocated, however little it would output (review); one
    /// within 2^24 decodes.
    #[test]
    fn a_zstd_frame_asking_for_a_wide_window_is_refused() {
        use std::io::Write as _;
        let frame = |window_log: u32| {
            let mut e = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
            e.window_log(window_log).unwrap();
            e.include_contentsize(false).unwrap();
            e.write_all(b"x").unwrap();
            e.finish().unwrap()
        };
        let wide = frame(27);
        assert!(wide.len() < 32, "{} bytes", wide.len());
        assert!(
            matches!(
                decompress(ZSTD, &wide, 1024),
                Err(CodecError::Corrupt { .. })
            ),
            "{:?}",
            decompress(ZSTD, &wide, 1024)
        );
        assert_eq!(decompress(ZSTD, &frame(24), 1024).unwrap(), b"x");
    }

    /// The output buffer never holds more than the limit plus one byte.
    #[test]
    fn the_output_buffer_is_capped_at_the_limit() {
        let plain = vec![9_u8; 300_000];
        let out = decompress(GZIP, &gzip(&plain), 300_000).unwrap();
        assert_eq!(out.len(), 300_000);
        assert!(out.capacity() <= 300_001, "capacity {}", out.capacity());
    }

    #[test]
    fn garbage_is_corrupt_not_a_panic() {
        for codec in SERVED {
            assert!(
                matches!(
                    decompress(codec, b"\x00\x01not a stream", 1024),
                    Err(CodecError::Corrupt { .. })
                ),
                "{}",
                name(codec)
            );
        }
    }
}
