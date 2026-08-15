//! Kafka RecordBatch v2 — the container Produce carries in and Fetch carries
//! out (#225).
//!
//! This is the one part of the protocol where the gateway cannot stay at the
//! surface. A batch is not a list of records with a length: it is a header
//! whose offsets, timestamps and sequences are DELTAS against the batch base,
//! covered by a CRC that must be recomputed whenever any of them changes. So
//! translating between Kafka and native records means taking the batch apart
//! and putting it back together, and the deltas are where an off-by-one turns
//! into a client that silently skips or duplicates a record.
//!
//! Magic 2 only. Magic 0 and 1 are the pre-0.11 message sets, which have no
//! producer id, no sequence and therefore no idempotence to bridge — a client
//! old enough to send them is a client this gateway cannot honour the
//! idempotent contract for, and the honest answer is a version refusal at the
//! API level rather than a silent downgrade here.

use crate::wire::{Decoder, Encoder, WireError};

/// Batch header size through the end of `baseSequence`, i.e. everything before
/// the record count.
const HEADER_BYTES: usize = 61;
/// Offset of the CRC field within the batch, and the point CRC coverage starts
/// (the four CRC bytes themselves are not covered).
///
/// 17, because the header runs baseOffset(8) + batchLength(4) +
/// partitionLeaderEpoch(4) + magic(1) before it. Coverage therefore begins at
/// 21, on `attributes` — every field from there to the end of the records is
/// checksummed, and the four preceding fields are not.
const CRC_OFFSET: usize = 17;
const CRC_COVERAGE_START: usize = CRC_OFFSET + 4;

pub const MAGIC_V2: i8 = 2;
/// Compression codec lives in the low three bits of `attributes`.
const COMPRESSION_MASK: i16 = 0x07;
const TRANSACTIONAL_FLAG: i16 = 0x10;
const CONTROL_FLAG: i16 = 0x20;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchError {
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error("record batch magic {found} is not supported; this gateway serves magic {MAGIC_V2} (Kafka 0.11+) only")]
    UnsupportedMagic { found: i8 },
    #[error("record batch is compressed (codec {codec}), which this gateway does not decompress")]
    Compressed { codec: i16 },
    #[error("record batch is transactional, and transactions are out of scope (#225)")]
    Transactional,
    #[error(
        "record batch CRC mismatch: header says {declared:#010x}, contents are {computed:#010x}"
    )]
    CrcMismatch { declared: u32, computed: u32 },
    #[error("record batch declares {declared} record(s) but {found} could be read")]
    RecordCount { declared: i32, found: usize },
    #[error("record batch is {len} bytes, too short to hold its own {HEADER_BYTES}-byte header")]
    Short { len: usize },
}

/// One record, with its deltas already resolved against the batch base — an
/// absolute offset and an absolute timestamp, because every consumer of this
/// type wants those and re-deriving them per call site is how they diverge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: i64,
    pub timestamp_millis: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// A decoded batch: the producer identity the idempotence bridge needs, plus
/// the records themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Parse one batch, verifying its CRC.
    ///
    /// The CRC is checked BEFORE the contents are trusted, because every field
    /// after it is a length or a delta that steers the parse: verifying
    /// afterwards would mean deciding a batch was corrupt only after acting on
    /// its numbers.
    pub fn decode(buf: &[u8]) -> Result<Self, BatchError> {
        if buf.len() < HEADER_BYTES {
            return Err(BatchError::Short { len: buf.len() });
        }
        let magic = buf[16] as i8;
        if magic != MAGIC_V2 {
            return Err(BatchError::UnsupportedMagic { found: magic });
        }
        let declared_crc = u32::from_be_bytes([
            buf[CRC_OFFSET],
            buf[CRC_OFFSET + 1],
            buf[CRC_OFFSET + 2],
            buf[CRC_OFFSET + 3],
        ]);
        let computed = crc32c(&buf[CRC_COVERAGE_START..]);
        if declared_crc != computed {
            return Err(BatchError::CrcMismatch {
                declared: declared_crc,
                computed,
            });
        }

        let mut d = Decoder::new(buf);
        let base_offset = d.i64("batch.baseOffset")?;
        let _batch_length = d.i32("batch.batchLength")?;
        let _leader_epoch = d.i32("batch.partitionLeaderEpoch")?;
        let _magic = d.i8("batch.magic")?;
        let _crc = d.u32("batch.crc")?;
        let attributes = d.i16("batch.attributes")?;
        let codec = attributes & COMPRESSION_MASK;
        if codec != 0 {
            return Err(BatchError::Compressed { codec });
        }
        if attributes & TRANSACTIONAL_FLAG != 0 {
            return Err(BatchError::Transactional);
        }
        let _last_offset_delta = d.i32("batch.lastOffsetDelta")?;
        let base_timestamp = d.i64("batch.baseTimestamp")?;
        let _max_timestamp = d.i64("batch.maxTimestamp")?;
        let producer_id = d.i64("batch.producerId")?;
        let producer_epoch = d.i16("batch.producerEpoch")?;
        let base_sequence = d.i32("batch.baseSequence")?;
        let declared = d.i32("batch.recordCount")?;
        if declared < 0 {
            return Err(BatchError::RecordCount { declared, found: 0 });
        }

        // A control batch carries no user records; its "records" are markers
        // whose payload only a transaction coordinator understands. Reporting
        // it as an empty batch is the honest read, and the caller refuses it
        // where the transactional flag is checked.
        let is_control = attributes & CONTROL_FLAG != 0;
        let mut records = Vec::new();
        if !is_control {
            for _ in 0..declared {
                if d.is_empty() {
                    return Err(BatchError::RecordCount {
                        declared,
                        found: records.len(),
                    });
                }
                records.push(decode_record(&mut d, base_offset, base_timestamp)?);
            }
        }
        Ok(Self {
            base_offset,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }

    /// Encode records as one batch at `base_offset`, computing the deltas and
    /// the CRC.
    ///
    /// `base_timestamp` anchors the timestamp deltas; the max timestamp is
    /// taken from the records rather than assumed to be the last one, since
    /// nothing requires a producer to send them in time order.
    pub fn encode(
        base_offset: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[Record],
    ) -> Vec<u8> {
        let base_timestamp = records.first().map_or(0, |r| r.timestamp_millis);
        let max_timestamp = records
            .iter()
            .map(|r| r.timestamp_millis)
            .max()
            .unwrap_or(0);
        let last_offset_delta = records
            .last()
            .map_or(0, |r| (r.offset - base_offset) as i32);

        let mut body = Encoder::new();
        body.i16(0); // attributes: no compression, create time, not transactional
        body.i32(last_offset_delta);
        body.i64(base_timestamp);
        body.i64(max_timestamp);
        body.i64(producer_id);
        body.i16(producer_epoch);
        body.i32(base_sequence);
        body.i32(records.len() as i32);
        for record in records {
            encode_record(&mut body, record, base_offset, base_timestamp);
        }
        let body = body.into_vec();

        // batchLength counts everything after itself: the 9 bytes from
        // partitionLeaderEpoch through crc, plus the CRC-covered body.
        let batch_length = (9 + body.len()) as i32;
        let crc = crc32c(&body);

        let mut out = Encoder::with_capacity(HEADER_BYTES + body.len());
        out.i64(base_offset);
        out.i32(batch_length);
        out.i32(-1); // partitionLeaderEpoch: unknown to this gateway
        out.i8(MAGIC_V2);
        out.u32(crc);
        out.raw(&body);
        out.into_vec()
    }
}

fn decode_record(
    d: &mut Decoder<'_>,
    base_offset: i64,
    base_timestamp: i64,
) -> Result<Record, BatchError> {
    let len = d.varint("record.length")?;
    if len < 0 {
        return Err(BatchError::Wire(WireError::NegativeLength {
            field: "record.length",
            len,
        }));
    }
    let _attributes = d.i8("record.attributes")?;
    let timestamp_delta = d.varlong("record.timestampDelta")?;
    let offset_delta = d.varint("record.offsetDelta")?;
    let key = read_varint_bytes(d, "record.key")?;
    let value = read_varint_bytes(d, "record.value")?;
    let header_count = d.varint("record.headerCount")?;
    if header_count < 0 {
        return Err(BatchError::Wire(WireError::NegativeLength {
            field: "record.headerCount",
            len: header_count,
        }));
    }
    let mut headers = Vec::new();
    for _ in 0..header_count {
        let key_len = d.varint("header.keyLength")?;
        if key_len < 0 {
            return Err(BatchError::Wire(WireError::NegativeLength {
                field: "header.keyLength",
                len: key_len,
            }));
        }
        let key_bytes = read_exact(d, key_len as usize, "header.key")?;
        let name = String::from_utf8(key_bytes).map_err(|_| {
            BatchError::Wire(WireError::NotUtf8 {
                field: "header.key",
            })
        })?;
        headers.push((name, read_varint_bytes(d, "header.value")?));
    }
    Ok(Record {
        offset: base_offset + i64::from(offset_delta),
        timestamp_millis: base_timestamp + timestamp_delta,
        key,
        value,
        headers,
    })
}

fn encode_record(out: &mut Encoder, record: &Record, base_offset: i64, base_timestamp: i64) {
    // The record's own length prefix covers everything after it, so the body is
    // built first and measured rather than predicted.
    let mut body = Encoder::new();
    body.i8(0); // record attributes are unused in v2
    body.varlong(record.timestamp_millis - base_timestamp);
    body.varint((record.offset - base_offset) as i32);
    write_varint_bytes(&mut body, record.key.as_deref());
    write_varint_bytes(&mut body, record.value.as_deref());
    body.varint(record.headers.len() as i32);
    for (name, value) in &record.headers {
        body.varint(name.len() as i32);
        body.raw(name.as_bytes());
        write_varint_bytes(&mut body, value.as_deref());
    }
    let body = body.into_vec();
    out.varint(body.len() as i32);
    out.raw(&body);
}

fn read_varint_bytes(
    d: &mut Decoder<'_>,
    field: &'static str,
) -> Result<Option<Vec<u8>>, BatchError> {
    let len = d.varint(field)?;
    if len == -1 {
        return Ok(None);
    }
    if len < 0 {
        return Err(BatchError::Wire(WireError::NegativeLength { field, len }));
    }
    Ok(Some(read_exact(d, len as usize, field)?))
}

fn read_exact(d: &mut Decoder<'_>, len: usize, field: &'static str) -> Result<Vec<u8>, BatchError> {
    if len > d.remaining() {
        return Err(BatchError::Wire(WireError::Truncated {
            field,
            needed: len,
            available: d.remaining(),
        }));
    }
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(d.i8(field)? as u8);
    }
    Ok(out)
}

fn write_varint_bytes(out: &mut Encoder, value: Option<&[u8]>) {
    match value {
        Some(bytes) => {
            out.varint(bytes.len() as i32);
            out.raw(bytes);
        }
        None => out.varint(-1),
    }
}

/// CRC-32C (Castagnoli), the checksum Kafka's v2 batch carries.
///
/// Implemented here rather than pulled in: it is a reflected table CRC in
/// twenty lines, and a dependency added for that is a dependency the
/// supply-chain gate has to carry forever. The table is built once on first
/// use from the reflected polynomial 0x82F63B78.
pub fn crc32c(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut table = [0_u32; 256];
        for (i, slot) in table.iter_mut().enumerate() {
            let mut crc = i as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0x82F6_3B78
                } else {
                    crc >> 1
                };
            }
            *slot = crc;
        }
        table
    });
    let mut crc = !0_u32;
    for byte in data {
        crc = table[((crc ^ u32::from(*byte)) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64, key: &str, value: &str) -> Record {
        Record {
            offset,
            timestamp_millis: 1_700_000_000_000 + offset,
            key: Some(key.as_bytes().to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: Vec::new(),
        }
    }

    /// Published vectors, not self-consistency: a CRC that only agrees with
    /// itself passes every round-trip in this file and is rejected by every
    /// real broker and client.
    #[test]
    fn crc32c_matches_the_published_vectors() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0_u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xff_u8; 32]), 0x62A8_AB43);
    }

    #[test]
    fn a_batch_round_trips_through_its_own_deltas() {
        let records = vec![record(100, "k0", "v0"), record(101, "k1", "v1")];
        let encoded = RecordBatch::encode(100, 42, 7, 0, &records);
        let decoded = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(decoded.base_offset, 100);
        assert_eq!(decoded.producer_id, 42);
        assert_eq!(decoded.producer_epoch, 7);
        assert_eq!(decoded.base_sequence, 0);
        assert_eq!(decoded.records, records);
    }

    /// The deltas are the trap: a batch whose base is not zero must still
    /// resolve to the absolute offsets the records were given.
    #[test]
    fn offsets_and_timestamps_resolve_against_the_batch_base() {
        let records = vec![record(5_000, "a", "b"), record(5_003, "c", "d")];
        let encoded = RecordBatch::encode(5_000, -1, -1, -1, &records);
        let decoded = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(decoded.records[0].offset, 5_000);
        assert_eq!(decoded.records[1].offset, 5_003, "offset delta of 3");
        assert_eq!(
            decoded.records[1].timestamp_millis,
            records[1].timestamp_millis
        );
    }

    #[test]
    fn a_null_key_survives_the_round_trip_as_null() {
        let records = vec![Record {
            offset: 0,
            timestamp_millis: 1,
            key: None,
            value: Some(b"payload".to_vec()),
            headers: vec![
                ("trace".to_owned(), Some(b"abc".to_vec())),
                ("none".to_owned(), None),
            ],
        }];
        let encoded = RecordBatch::encode(0, 1, 0, 0, &records);
        let decoded = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(decoded.records, records, "null key and null header value");
    }

    /// A flipped byte must be refused, not parsed into plausible garbage —
    /// this is the property the CRC exists for.
    #[test]
    fn a_corrupted_payload_is_refused_by_the_crc() {
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert!(matches!(
            RecordBatch::decode(&encoded),
            Err(BatchError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn a_pre_0_11_message_set_is_refused_by_magic() {
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);
        encoded[16] = 1; // magic v1
        assert!(matches!(
            RecordBatch::decode(&encoded),
            Err(BatchError::UnsupportedMagic { found: 1 })
        ));
    }

    /// Compression and transactions are refused by NAME rather than
    /// mis-parsed: a gateway that read a gzip payload as records would hand a
    /// consumer bytes no producer wrote.
    #[test]
    fn compressed_and_transactional_batches_are_refused_by_name() {
        let base = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);

        let mut compressed = base.clone();
        set_attributes(&mut compressed, 1); // gzip
        assert!(matches!(
            RecordBatch::decode(&compressed),
            Err(BatchError::Compressed { codec: 1 })
        ));

        let mut transactional = base.clone();
        set_attributes(&mut transactional, TRANSACTIONAL_FLAG);
        assert!(matches!(
            RecordBatch::decode(&transactional),
            Err(BatchError::Transactional)
        ));
    }

    #[test]
    fn a_batch_claiming_more_records_than_it_carries_is_refused() {
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);
        // recordCount is the last header field, at bytes 57..61.
        encoded[57..61].copy_from_slice(&9_i32.to_be_bytes());
        refresh_crc(&mut encoded);
        assert!(matches!(
            RecordBatch::decode(&encoded),
            Err(BatchError::RecordCount { declared: 9, .. })
        ));
    }

    #[test]
    fn a_truncated_batch_is_refused_before_it_is_indexed() {
        assert!(matches!(
            RecordBatch::decode(&[0_u8; 10]),
            Err(BatchError::Short { len: 10 })
        ));
    }

    /// Set the attributes field and repair the CRC, so the test exercises the
    /// attribute check rather than tripping the checksum first.
    fn set_attributes(batch: &mut [u8], attributes: i16) {
        batch[21..23].copy_from_slice(&attributes.to_be_bytes());
        refresh_crc(batch);
    }

    fn refresh_crc(batch: &mut [u8]) {
        let crc = crc32c(&batch[CRC_COVERAGE_START..]);
        batch[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    }
}
