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
pub const SERVED: [i16; 4] = [GZIP, SNAPPY, LZ4, ZSTD];

/// The header of snappy-java's stream format, which Kafka's Java client
/// writes: the magic, then a 4-byte version and a 4-byte compatible version.
/// Blocks follow, each a 4-byte big-endian length and one raw snappy block.
const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];
const XERIAL_HEADER_BYTES: usize = 16;

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
        SNAPPY => snappy(input, limit),
        // The LZ4 frame format, which Kafka writes for v2 batches (the
        // broken header checksum of the pre-0.10 framing is magic 0 and 1
        // only, and those batches are refused before this).
        LZ4 => bounded(codec, lz4_flex::frame::FrameDecoder::new(input), limit),
        _ => Err(CodecError::Unknown { codec }),
    }
}

/// Snappy as Kafka's clients send it: snappy-java's framing (the Java
/// client), or one bare snappy block (some other clients), told apart by the
/// magic as Kafka's own reader does. Each block declares its decompressed
/// length up front, so it is checked against what is left of `limit` before
/// anything is allocated.
fn snappy(input: &[u8], limit: usize) -> Result<Vec<u8>, CodecError> {
    let corrupt = |reason: String| CodecError::Corrupt {
        codec: SNAPPY,
        reason,
    };
    let mut out = Vec::new();
    if !input.starts_with(&XERIAL_MAGIC) {
        snappy_block(input, limit, &mut out)?;
        return Ok(out);
    }
    let Some(mut rest) = input.get(XERIAL_HEADER_BYTES..) else {
        return Err(corrupt(format!(
            "a snappy-java stream of {} bytes is shorter than its {XERIAL_HEADER_BYTES}-byte header",
            input.len()
        )));
    };
    while !rest.is_empty() {
        let Some((len, tail)) = rest.split_first_chunk::<4>() else {
            return Err(corrupt(format!(
                "{} byte(s) left where a 4-byte block length belongs",
                rest.len()
            )));
        };
        let len = u32::from_be_bytes(*len) as usize;
        if len > tail.len() {
            return Err(corrupt(format!(
                "a block declares {len} bytes but {} remain",
                tail.len()
            )));
        }
        let (block, next) = tail.split_at(len);
        snappy_block(block, limit, &mut out)?;
        rest = next;
    }
    Ok(out)
}

/// Append one raw snappy block to `out`, keeping `out` within `limit`.
fn snappy_block(block: &[u8], limit: usize, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let corrupt = |error: snap::Error| CodecError::Corrupt {
        codec: SNAPPY,
        reason: error.to_string(),
    };
    let len = snap::raw::decompress_len(block).map_err(corrupt)?;
    if len > limit.saturating_sub(out.len()) {
        return Err(CodecError::TooLarge {
            codec: SNAPPY,
            limit,
        });
    }
    let start = out.len();
    out.resize(start + len, 0);
    let written = snap::raw::Decoder::new()
        .decompress(block, &mut out[start..])
        .map_err(corrupt)?;
    if written != len {
        return Err(CodecError::Corrupt {
            codec: SNAPPY,
            reason: format!("a block declared {len} bytes and decompressed to {written}"),
        });
    }
    Ok(())
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

    /// snappy-java's framing around raw blocks, as the Java client writes it.
    fn xerial(blocks: &[&[u8]]) -> Vec<u8> {
        let mut out = XERIAL_MAGIC.to_vec();
        out.extend_from_slice(&1_u32.to_be_bytes());
        out.extend_from_slice(&1_u32.to_be_bytes());
        for block in blocks {
            let packed = snap::raw::Encoder::new().compress_vec(block).unwrap();
            out.extend_from_slice(&(packed.len() as u32).to_be_bytes());
            out.extend_from_slice(&packed);
        }
        out
    }

    fn lz4(bytes: &[u8]) -> Vec<u8> {
        let mut e = lz4_flex::frame::FrameEncoder::new(Vec::new());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn served_codecs_round_trip() {
        let plain = b"records section".repeat(100);
        assert_eq!(decompress(GZIP, &gzip(&plain), 1 << 20).unwrap(), plain);
        let z = zstd::encode_all(&plain[..], 3).unwrap();
        assert_eq!(decompress(ZSTD, &z, 1 << 20).unwrap(), plain);
        assert_eq!(decompress(LZ4, &lz4(&plain), 1 << 20).unwrap(), plain);
        let raw = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        assert_eq!(
            decompress(SNAPPY, &raw, 1 << 20).unwrap(),
            plain,
            "a bare block"
        );
        let (a, b) = plain.split_at(700);
        assert_eq!(
            decompress(SNAPPY, &xerial(&[a, b]), 1 << 20).unwrap(),
            plain,
            "snappy-java framing, two blocks"
        );
    }

    /// A snappy block declares its length first: one that declares more than
    /// the limit is refused before anything is allocated, and so is the
    /// block that takes a framed stream past it.
    #[test]
    fn a_snappy_length_past_the_limit_is_refused_before_allocation() {
        // A varint of 2^31 and nothing else: decompressing would need 2 GiB.
        let lying = [0x80, 0x80, 0x80, 0x80, 0x08];
        assert_eq!(
            decompress(SNAPPY, &lying, 1 << 20),
            Err(CodecError::TooLarge {
                codec: SNAPPY,
                limit: 1 << 20
            })
        );
        let block = vec![1_u8; 600];
        assert_eq!(
            decompress(SNAPPY, &xerial(&[&block, &block]), 1000),
            Err(CodecError::TooLarge {
                codec: SNAPPY,
                limit: 1000
            })
        );
    }

    #[test]
    fn a_truncated_snappy_java_stream_is_corrupt() {
        let whole = xerial(&[b"records section records section"]);
        // Cut inside the header (the magic intact), inside a block length, and
        // inside a block. A prefix shorter than the magic is not this format
        // at all: it is read as a bare block, and judged as one.
        for cut in [
            XERIAL_HEADER_BYTES - 2,
            XERIAL_HEADER_BYTES + 2,
            whole.len() - 3,
        ] {
            assert!(
                matches!(
                    decompress(SNAPPY, &whole[..cut], 1024),
                    Err(CodecError::Corrupt { .. })
                ),
                "cut at {cut}: {:?}",
                decompress(SNAPPY, &whole[..cut], 1024)
            );
        }
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
            (LZ4, lz4(&plain)),
            (SNAPPY, xerial(&[&plain])),
            (
                SNAPPY,
                snap::raw::Encoder::new().compress_vec(&plain).unwrap(),
            ),
        ] {
            assert_eq!(decompress(codec, &packed, 4096).unwrap().len(), 4096);
            assert_eq!(
                decompress(codec, &packed, 4095),
                Err(CodecError::TooLarge { codec, limit: 4095 })
            );
        }
    }

    #[test]
    fn unknown_codecs_are_refused_by_name() {
        for codec in 5..=7 {
            assert_eq!(
                decompress(codec, b"x", 10),
                Err(CodecError::Unknown { codec })
            );
        }
        assert_eq!(served_list(), "gzip, snappy, lz4, zstd, none");
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
