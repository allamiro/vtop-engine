//! Fuzz the Kafka gateway's RecordBatch decoder with every codec (#596). A
//! Produce request's records field is untrusted bytes from any client that
//! can reach the listener, and since the gateway decompresses gzip and zstd
//! it also runs a codec over them. The contract is that decoding returns
//! `Err`, never panics, and never holds more than the cap it was given.
//!
//! Three paths per input:
//! 1. the raw bytes as a batch (mostly stopped by framing or the CRC);
//! 2. the raw bytes through each codec id 0-7 with a small cap;
//! 3. the bytes as the records section of a well-framed batch whose CRC is
//!    valid. The first byte picks the codec and the next two the record count,
//!    so the fuzzer reaches the decompressor and the record parser behind it.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_kafka::records::{crc32c, RecordBatch};

const CAP: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    if let Ok((_, expanded)) = RecordBatch::decode_within(data, CAP) {
        assert!(expanded <= CAP, "decoded {expanded} bytes past the {CAP}-byte cap");
    }
    for codec in 0..8 {
        if let Ok(out) = vtop_kafka::codec::decompress(codec, data, CAP) {
            assert!(out.len() <= CAP, "codec {codec} returned {} bytes past the cap", out.len());
        }
    }
    if data.len() < 3 {
        return;
    }
    let codec = i16::from(data[0] & 0x07);
    let count = i32::from(u16::from_be_bytes([data[1], data[2]]));
    let section = &data[3..];
    let mut batch = Vec::with_capacity(61 + section.len());
    batch.extend_from_slice(&0_i64.to_be_bytes()); // baseOffset
    batch.extend_from_slice(&((49 + section.len()) as i32).to_be_bytes()); // batchLength
    batch.extend_from_slice(&(-1_i32).to_be_bytes()); // partitionLeaderEpoch
    batch.push(2); // magic
    batch.extend_from_slice(&0_u32.to_be_bytes()); // crc, filled below
    batch.extend_from_slice(&codec.to_be_bytes()); // attributes
    batch.extend_from_slice(&0_i32.to_be_bytes()); // lastOffsetDelta
    batch.extend_from_slice(&0_i64.to_be_bytes()); // baseTimestamp
    batch.extend_from_slice(&0_i64.to_be_bytes()); // maxTimestamp
    batch.extend_from_slice(&(-1_i64).to_be_bytes()); // producerId
    batch.extend_from_slice(&(-1_i16).to_be_bytes()); // producerEpoch
    batch.extend_from_slice(&(-1_i32).to_be_bytes()); // baseSequence
    batch.extend_from_slice(&count.to_be_bytes()); // recordCount
    batch.extend_from_slice(section);
    let crc = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    if let Ok((_, expanded)) = RecordBatch::decode_within(&batch, CAP) {
        assert!(expanded <= CAP, "decoded {expanded} bytes past the {CAP}-byte cap");
    }
});
