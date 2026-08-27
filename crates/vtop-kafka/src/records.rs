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
/// The hard ceiling on records in one batch, matching `vtop-protocol`'s own
/// `ABSOLUTE_MAX_RECORDS`. A batch larger than the native protocol will accept
/// cannot be forwarded, so refusing it here refuses it while the reason is
/// still legible instead of deep in a translation.
pub const MAX_RECORDS: usize = 65_536;
/// The hard ceiling on headers in one record.
///
/// Unlike [`MAX_RECORDS`], this one is the gateway's own policy rather than a
/// mirror of the native protocol — the native `LogRecord` has no headers at
/// all, so nothing here is forwardable and no upstream number constrains it.
/// It exists because headers AMPLIFY: the smallest one on the wire is two
/// bytes (an empty name and a null value), and each becomes a `String` plus
/// an `Option<Vec<u8>>` — some fifty bytes resident. A record near the
/// 16 MiB field cap could therefore declare millions of them and turn a small
/// Produce into hundreds of megabytes, which the per-field size cap does
/// nothing to prevent because no single field is large (review).
///
/// A thousand is three orders of magnitude above what real records carry — a
/// trace id, a schema id, a routing hint — so it bounds the amplification
/// without being a limit anyone meets by accident.
pub const MAX_HEADERS: usize = 1024;
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
    #[error("record batch declares {declared} byte(s) after its length field but {available} are present")]
    Framing { declared: i64, available: usize },
    #[error("record {index} declares {declared} byte(s) but its fields consumed {consumed}")]
    RecordFraming {
        index: usize,
        declared: usize,
        consumed: usize,
    },
    #[error("record batch declares {declared} records, above the {MAX_RECORDS}-record ceiling")]
    TooManyRecords { declared: i64 },
    #[error("record batch is a control batch, whose markers only a transaction coordinator can interpret")]
    Control,
    #[error(
        "record batch declares {declared} record(s) but {remaining} byte(s) remain after them"
    )]
    TrailingBytes { declared: i32, remaining: usize },
    #[error(
        "record {index} declares {declared} header(s), above the {MAX_HEADERS}-header ceiling"
    )]
    TooManyHeaders { index: usize, declared: i64 },
    #[error("record {index} would sit at an offset or timestamp that overflows i64")]
    CoordinateOverflow { index: usize },
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
        // BATCHLENGTH IS ENFORCED, NOT READ AND DISCARDED. It counts every
        // byte after itself, and it is the only thing that says where this
        // batch ends — so parsing the whole supplied slice instead means a
        // RECORDS field holding two concatenated batches has the first
        // batch's CRC computed over both, and a truncated batch is checked
        // against bytes that belong to something else. The field sits outside
        // CRC coverage, so it is also the one header value an attacker can
        // change freely (review).
        let declared = i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let total = i64::from(declared) + 12;
        if declared < 0 || total > buf.len() as i64 || (total as usize) < HEADER_BYTES {
            return Err(BatchError::Framing {
                declared: i64::from(declared),
                available: buf.len(),
            });
        }
        let buf = &buf[..total as usize];
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
        // REFUSED BY NAME, beside the other two, rather than read as an empty
        // batch. A control batch's "records" are transaction markers only a
        // coordinator understands — but the flag is independent of the
        // transactional one, so an untrusted batch can set CONTROL alone and
        // sail past the check above. Reporting that as a successful empty
        // batch makes the caller's records DISAPPEAR: a Produce that returns
        // success having stored nothing, which is the one failure mode a
        // gateway must never have. The promise for a format this gateway
        // cannot honour is a refusal that names it (review).
        if attributes & CONTROL_FLAG != 0 {
            return Err(BatchError::Control);
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
        // The ceiling the native protocol itself enforces. A batch above it
        // could never be forwarded, so refusing it here refuses it while the
        // reason is still legible rather than deep inside a translation
        // (review).
        if declared as usize > MAX_RECORDS {
            return Err(BatchError::TooManyRecords {
                declared: i64::from(declared),
            });
        }

        // No `with_capacity(declared)`: the count is an attacker's number
        // until the records are actually there, and the ceiling above is
        // 65,536 entries of reserve on a lie.
        let mut records = Vec::new();
        for index in 0..declared as usize {
            if d.is_empty() {
                return Err(BatchError::RecordCount {
                    declared,
                    found: records.len(),
                });
            }
            records.push(decode_record(&mut d, base_offset, base_timestamp, index)?);
        }
        // THE COUNT IS BINDING IN BOTH DIRECTIONS. Declaring too many was
        // already refused above; declaring too FEW was not, and the batch
        // simply returned with the surplus unread — so a batch whose count
        // is rewritten to zero (with the CRC recomputed, which costs an
        // attacker nothing) decoded as a valid empty batch and every record
        // in it was silently dropped. Data that a Produce acknowledged and
        // nobody stored is worse than a refusal (review). The batch was
        // sliced to its own declared length before the CRC check, so
        // anything left here is surplus inside this batch rather than the
        // next one.
        if !d.is_empty() {
            return Err(BatchError::TrailingBytes {
                declared,
                remaining: d.remaining(),
            });
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
        assert!(
            records.len() <= MAX_RECORDS,
            "a batch cannot carry {} records: the ceiling is {MAX_RECORDS}",
            records.len()
        );
        let last_offset_delta = records
            .last()
            .map_or(0, |r| delta_i32(r.offset, base_offset));

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
    outer: &mut Decoder<'_>,
    base_offset: i64,
    base_timestamp: i64,
    index: usize,
) -> Result<Record, BatchError> {
    let len = outer.varint("record.length")?;
    if len < 0 {
        return Err(BatchError::Wire(WireError::NegativeLength {
            field: "record.length",
            len,
        }));
    }
    // DECODED FROM ITS OWN BYTES. The length was previously read and then
    // ignored while the fields came off the outer decoder, so a record
    // claiming a length its contents disagree with parsed anyway — bleeding
    // into the next record, or leaving part of itself behind for the next one
    // to misread (review). A sub-decoder makes the boundary real, and the
    // check below makes the declaration binding in both directions.
    let mut d = outer.sub(len as usize, "record.body")?;
    let d = &mut d;
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
    // BOUNDED BEFORE ANYTHING IS PUSHED. Two guards, and they refuse
    // different lies: the ceiling refuses a count that is merely enormous,
    // and the structural bound refuses one this record could not possibly
    // hold — every header costs at least two bytes (an empty name, a null
    // value), so a count above half the remaining bytes is arithmetically
    // impossible and can be rejected without reading a single one.
    if header_count as usize > MAX_HEADERS {
        return Err(BatchError::TooManyHeaders {
            index,
            declared: i64::from(header_count),
        });
    }
    if header_count as usize > d.remaining() / 2 {
        return Err(BatchError::Wire(WireError::Truncated {
            field: "record.headerCount",
            needed: header_count as usize * 2,
            available: d.remaining(),
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
    if !d.is_empty() {
        return Err(BatchError::RecordFraming {
            index,
            declared: len as usize,
            consumed: len as usize - d.remaining(),
        });
    }
    // CHECKED, because both operands come off the wire and neither is bounded
    // by the other: baseOffset sits OUTSIDE CRC coverage, so eight bytes at
    // the front of an otherwise valid batch are enough to overflow this —
    // a panic in a debug build, a wrapped negative offset in a release one
    // (review).
    let offset = base_offset
        .checked_add(i64::from(offset_delta))
        .ok_or(BatchError::CoordinateOverflow { index })?;
    let timestamp_millis = base_timestamp
        .checked_add(timestamp_delta)
        .ok_or(BatchError::CoordinateOverflow { index })?;
    Ok(Record {
        offset,
        timestamp_millis,
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
    body.varint(delta_i32(record.offset, base_offset));
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

/// The offset delta a record carries, which the wire stores as an i32.
///
/// # Panics
///
/// If the record sits further from the batch base than an i32 can express.
/// Casting instead would silently change the record's offset on the way out —
/// the batch would encode, decode, and describe a different record than the
/// one handed in (review). Callers build batches from a contiguous run of
/// offsets, so the distance is bounded by the batch size.
fn delta_i32(offset: i64, base_offset: i64) -> i32 {
    let delta = offset - base_offset;
    i32::try_from(delta).unwrap_or_else(|_| {
        panic!("record offset {offset} is {delta} from base {base_offset}, beyond an i32 delta")
    })
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
    // THE SAME HARD CAP THE WIRE DECODER APPLIES. Without it this path
    // allocates whatever a record declares — the request buffer is already
    // resident, so a large declared field simply duplicates it, and nothing
    // else stood between a produce and the gateway's memory (review).
    if len > crate::wire::MAX_FIELD_BYTES {
        return Err(BatchError::Wire(WireError::TooLong {
            field,
            len,
            limit: crate::wire::MAX_FIELD_BYTES,
        }));
    }
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

    /// batchLength sits OUTSIDE CRC coverage, so it is the one header field an
    /// attacker can rewrite freely — and it was being read and discarded.
    #[test]
    fn a_batch_whose_declared_length_disagrees_is_refused() {
        let good = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);

        let mut short = good.clone();
        short[8..12].copy_from_slice(&9_i32.to_be_bytes()); // shorter than its own header
        assert!(
            matches!(RecordBatch::decode(&short), Err(BatchError::Framing { .. })),
            "a length that cannot even cover the header is refused as framing"
        );

        let mut truncating = good.clone();
        // Long enough to be a header, short enough to cut the records off:
        // the CRC then covers a different span than the one that produced it.
        truncating[8..12].copy_from_slice(&((HEADER_BYTES - 12) as i32).to_be_bytes());
        assert!(
            matches!(
                RecordBatch::decode(&truncating),
                Err(BatchError::CrcMismatch { .. }) | Err(BatchError::Framing { .. })
            ),
            "a batch cut short by its own length must not verify"
        );

        let mut long = good.clone();
        long[8..12].copy_from_slice(&(good.len() as i32).to_be_bytes()); // beyond the buffer
        assert!(matches!(
            RecordBatch::decode(&long),
            Err(BatchError::Framing { .. })
        ));

        // Two batches concatenated: the first must be parsed alone, not have
        // its CRC computed over both.
        let mut concatenated = good.clone();
        concatenated.extend_from_slice(&good);
        let decoded = RecordBatch::decode(&concatenated).expect("the first batch alone");
        assert_eq!(decoded.records.len(), 1);
    }

    /// A record's length was read and then ignored while its fields came off
    /// the OUTER decoder, so a record could bleed into the one after it.
    #[test]
    fn a_record_whose_declared_length_disagrees_is_refused() {
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);
        // The first record's length varint sits immediately after the 61-byte
        // header; shrink it and the fields no longer fit the declaration.
        encoded[HEADER_BYTES] = 2;
        refresh_crc(&mut encoded);
        assert!(
            matches!(
                RecordBatch::decode(&encoded),
                Err(BatchError::RecordFraming { .. }) | Err(BatchError::Wire(_))
            ),
            "a record must be decoded from exactly the bytes it claims"
        );
    }

    /// baseOffset is outside CRC coverage: eight bytes at the front of an
    /// otherwise valid batch are enough to overflow the absolute coordinate —
    /// a panic in a debug build, a wrapped negative offset in a release one.
    #[test]
    fn a_batch_that_would_overflow_an_absolute_offset_is_refused() {
        let mut encoded =
            RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v"), record(1, "k", "v")]);
        encoded[0..8].copy_from_slice(&i64::MAX.to_be_bytes());
        assert!(matches!(
            RecordBatch::decode(&encoded),
            Err(BatchError::CoordinateOverflow { .. })
        ));
    }

    /// The record count is an attacker's number; the ceiling matches the
    /// native protocol's own, so a batch it could never forward is refused
    /// while the reason is still legible.
    #[test]
    fn a_batch_above_the_record_ceiling_is_refused() {
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);
        encoded[57..61].copy_from_slice(&(MAX_RECORDS as i32 + 1).to_be_bytes());
        refresh_crc(&mut encoded);
        assert!(matches!(
            RecordBatch::decode(&encoded),
            Err(BatchError::TooManyRecords { .. })
        ));
    }

    /// The count binds in BOTH directions. Declaring more records than a batch
    /// carries was always refused; declaring fewer was not, and the surplus
    /// went unread — so a batch whose count is rewritten to zero decoded as a
    /// valid empty batch and every record in it vanished. Recomputing the CRC
    /// costs an attacker nothing, which is what makes this reachable.
    #[test]
    fn a_batch_declaring_fewer_records_than_it_carries_is_refused() {
        let records = vec![record(0, "k0", "v0"), record(1, "k1", "v1")];
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &records);
        assert_eq!(
            RecordBatch::decode(&encoded).unwrap().records.len(),
            2,
            "the batch is honest before it is tampered with"
        );

        encoded[57..61].copy_from_slice(&0_i32.to_be_bytes());
        refresh_crc(&mut encoded);
        match RecordBatch::decode(&encoded) {
            Err(BatchError::TrailingBytes {
                declared,
                remaining,
            }) => {
                assert_eq!(declared, 0);
                assert!(remaining > 0, "the unread records are the surplus");
            }
            other => panic!(
                "a batch under-declaring its count must be refused, not decoded as empty — \
                 acknowledging a Produce and storing nothing is the one failure a gateway \
                 must not have: {other:?}"
            ),
        }
    }

    /// The control flag is INDEPENDENT of the transactional one, so a batch
    /// can set it alone and miss the transactional refusal entirely. Reading
    /// it as an empty batch then discards whatever it carried.
    #[test]
    fn a_control_batch_is_refused_rather_than_read_as_empty() {
        let records = vec![record(0, "k0", "v0")];
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &records);
        set_attributes(&mut encoded, CONTROL_FLAG);
        assert!(
            matches!(RecordBatch::decode(&encoded), Err(BatchError::Control)),
            "a control batch must be refused by name; reporting it as an empty \
             success makes the caller's records disappear"
        );

        // And with BOTH flags the transactional refusal still wins, because it
        // is the more specific thing to say about the batch.
        let mut both = RecordBatch::encode(0, 1, 0, 0, &records);
        set_attributes(&mut both, CONTROL_FLAG | TRANSACTIONAL_FLAG);
        assert!(matches!(
            RecordBatch::decode(&both),
            Err(BatchError::Transactional)
        ));
    }

    /// Headers amplify: two bytes on the wire become some fifty resident, so
    /// an unbounded count turns a small Produce into hundreds of megabytes.
    /// The per-field size cap cannot see this, because no single field is big.
    #[test]
    fn a_record_above_the_header_ceiling_is_refused() {
        let hostile = Record {
            offset: 0,
            timestamp_millis: 1,
            key: None,
            value: None,
            // The cheapest header there is: an empty name and a null value,
            // two bytes each on the wire.
            headers: vec![(String::new(), None); MAX_HEADERS + 1],
        };
        let encoded = RecordBatch::encode(0, 1, 0, 0, &[hostile]);
        match RecordBatch::decode(&encoded) {
            Err(BatchError::TooManyHeaders { index, declared }) => {
                assert_eq!(index, 0);
                assert_eq!(declared, MAX_HEADERS as i64 + 1);
            }
            other => panic!("a record over the header ceiling must be refused: {other:?}"),
        }

        // The ceiling is not a round-trip barrier for anything real: a record
        // AT the ceiling still decodes, so the bound refuses only the abuse.
        let at_ceiling = Record {
            offset: 0,
            timestamp_millis: 1,
            key: None,
            value: None,
            headers: vec![(String::new(), None); MAX_HEADERS],
        };
        let encoded = RecordBatch::encode(0, 1, 0, 0, std::slice::from_ref(&at_ceiling));
        assert_eq!(
            RecordBatch::decode(&encoded).unwrap().records[0],
            at_ceiling
        );
    }

    /// A count no record could hold is refused without reading a header at
    /// all: every one costs at least two bytes, so a count above half the
    /// remainder is arithmetically impossible.
    #[test]
    fn a_header_count_larger_than_the_record_could_hold_is_refused() {
        let mut encoded = RecordBatch::encode(0, 1, 0, 0, &[record(0, "k", "v")]);
        // The header count is the last byte of the record body, which is the
        // last byte of the batch: a lone zero varint with no headers after it.
        let last = encoded.len() - 1;
        assert_eq!(encoded[last], 0, "the sample record carries no headers");
        encoded[last] = 100; // varint 50, in a record with nothing left to read
        refresh_crc(&mut encoded);
        assert!(
            matches!(
                RecordBatch::decode(&encoded),
                Err(BatchError::Wire(WireError::Truncated { .. }))
            ),
            "a header count the record cannot hold must be refused before any \
             of them is materialized"
        );
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
