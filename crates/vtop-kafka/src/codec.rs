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
            let decoder = zstd::stream::read::Decoder::with_buffer(input).map_err(|error| {
                CodecError::Corrupt {
                    codec,
                    reason: error.to_string(),
                }
            })?;
            bounded(codec, decoder, limit)
        }
        SNAPPY | LZ4 => Err(CodecError::NotServed { codec }),
        _ => Err(CodecError::Unknown { codec }),
    }
}

/// Read `reader` to its end or to `limit + 1` bytes, whichever is first.
fn bounded(codec: i16, reader: impl Read, limit: usize) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    reader
        .take(cap)
        .read_to_end(&mut out)
        .map_err(|error| CodecError::Corrupt {
            codec,
            reason: error.to_string(),
        })?;
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
