//! On-disk encoding for the proof-carrying v2 segment format.
//!
//! The envelope and frame shapes deliberately mirror `codec.rs`; v2 adds the
//! producer epoch and reserved attribute bits to every record frame so sealed
//! segments can carry per-epoch producer summaries and future record flags.

use crate::codec::{DecodedFrame, FrameRead};
use crate::types::{
    LogError, LogRecord, SegmentConfigV2, SegmentDescriptorV2, VtopLogResult, FORMAT_NAME,
    FORMAT_VERSION_V2,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

pub(crate) const HEADER_MAGIC_V2: &[u8; 8] = b"VTOPSEG2";
pub(crate) const RECORD_MAGIC_V2: &[u8; 8] = b"VTOPREC2";
pub(crate) const CHECKSUM_LEN: usize = 32;
pub(crate) const FRAME_PREFIX_LEN: usize = 12;
/// Bytes added around every key/value payload by the v2 record frame.
pub const RECORD_FRAME_OVERHEAD_BYTES_V2: u64 = 12 + 8 + 16 + 8 + 8 + 2 + 8 + 4 + 4 + 32;
pub(crate) const RECORD_FIXED_BODY_LEN_V2: usize =
    RECORD_FRAME_OVERHEAD_BYTES_V2 as usize - FRAME_PREFIX_LEN;
const MAX_HEADER_BYTES: u32 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SegmentHeaderV2 {
    pub format: String,
    pub version: u16,
    pub descriptor: SegmentDescriptorV2,
    pub config: SegmentConfigV2,
}

impl SegmentHeaderV2 {
    pub(crate) fn new(descriptor: SegmentDescriptorV2, config: SegmentConfigV2) -> Self {
        Self {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION_V2,
            descriptor,
            config,
        }
    }
}

pub(crate) fn encode_header_v2(header: &SegmentHeaderV2) -> VtopLogResult<Vec<u8>> {
    let json = serde_json::to_vec(header)
        .map_err(|error| LogError::InvalidDescriptor(format!("cannot encode header: {error}")))?;
    let json_len = u32::try_from(json.len()).map_err(|_| {
        LogError::InvalidDescriptor("encoded segment header is too large".to_owned())
    })?;
    if json_len > MAX_HEADER_BYTES {
        return Err(LogError::InvalidDescriptor(
            "encoded segment header is too large".to_owned(),
        ));
    }
    let mut encoded = Vec::with_capacity(12 + json.len() + CHECKSUM_LEN);
    encoded.extend_from_slice(HEADER_MAGIC_V2);
    encoded.extend_from_slice(&json_len.to_be_bytes());
    encoded.extend_from_slice(&json);
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

pub(crate) fn read_header_v2<R: Read + Seek>(
    reader: &mut R,
) -> VtopLogResult<(SegmentHeaderV2, u64)> {
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|source| LogError::Io {
            path: "<segment>".into(),
            source,
        })?;
    let mut prefix = [0_u8; 12];
    reader
        .read_exact(&mut prefix)
        .map_err(|source| header_read_error(source, 0, "segment header"))?;
    if &prefix[..8] != HEADER_MAGIC_V2 {
        return Err(LogError::Corrupt {
            position: 0,
            reason: "invalid segment header magic".to_owned(),
        });
    }
    let json_len = u32::from_be_bytes(prefix[8..12].try_into().expect("fixed slice"));
    if json_len == 0 || json_len > MAX_HEADER_BYTES {
        return Err(LogError::Corrupt {
            position: 8,
            reason: format!("invalid header length {json_len}"),
        });
    }
    let mut json = vec![0_u8; json_len as usize];
    reader
        .read_exact(&mut json)
        .map_err(|source| header_read_error(source, 12, "segment header JSON"))?;
    let mut stored_hash = [0_u8; CHECKSUM_LEN];
    reader.read_exact(&mut stored_hash).map_err(|source| {
        header_read_error(source, 12 + u64::from(json_len), "segment header checksum")
    })?;
    let mut authenticated = prefix.to_vec();
    authenticated.extend_from_slice(&json);
    if blake3::hash(&authenticated).as_bytes() != &stored_hash {
        return Err(LogError::Corrupt {
            position: 0,
            reason: "segment header checksum mismatch".to_owned(),
        });
    }
    let header: SegmentHeaderV2 =
        serde_json::from_slice(&json).map_err(|error| LogError::Corrupt {
            position: 12,
            reason: format!("invalid segment header JSON: {error}"),
        })?;
    if header.format != FORMAT_NAME {
        return Err(LogError::Corrupt {
            position: 12,
            reason: format!("unexpected segment format {:?}", header.format),
        });
    }
    if header.version != FORMAT_VERSION_V2 {
        return Err(LogError::UnsupportedVersion(header.version));
    }
    header.descriptor.validate()?;
    header.config.validate()?;
    Ok((header, 12 + u64::from(json_len) + CHECKSUM_LEN as u64))
}

pub(crate) fn record_payload_len_v2(record: &LogRecord) -> VtopLogResult<usize> {
    record
        .key
        .len()
        .checked_add(record.value.len())
        .ok_or(LogError::RecordTooLarge {
            actual: usize::MAX,
            maximum: u32::MAX,
        })
}

pub(crate) fn encode_record_v2(
    record: &LogRecord,
    relative_offset: u64,
    maximum: u32,
) -> VtopLogResult<Vec<u8>> {
    if record.attributes != 0 {
        return Err(LogError::UnsupportedRecordField("attributes"));
    }
    let payload_len = record_payload_len_v2(record)?;
    if payload_len > maximum as usize {
        return Err(LogError::RecordTooLarge {
            actual: payload_len,
            maximum,
        });
    }
    let key_len = u32::try_from(record.key.len()).map_err(|_| LogError::RecordTooLarge {
        actual: payload_len,
        maximum,
    })?;
    let value_len = u32::try_from(record.value.len()).map_err(|_| LogError::RecordTooLarge {
        actual: payload_len,
        maximum,
    })?;
    let body_len = RECORD_FIXED_BODY_LEN_V2
        .checked_add(payload_len)
        .and_then(|length| u32::try_from(length).ok())
        .ok_or(LogError::RecordTooLarge {
            actual: payload_len,
            maximum,
        })?;

    let mut encoded = Vec::with_capacity(FRAME_PREFIX_LEN + body_len as usize);
    encoded.extend_from_slice(RECORD_MAGIC_V2);
    encoded.extend_from_slice(&body_len.to_be_bytes());
    encoded.extend_from_slice(&relative_offset.to_be_bytes());
    encoded.extend_from_slice(record.producer_id.as_bytes());
    encoded.extend_from_slice(&record.producer_epoch.to_be_bytes());
    encoded.extend_from_slice(&record.sequence.to_be_bytes());
    encoded.extend_from_slice(&record.attributes.to_be_bytes());
    encoded.extend_from_slice(&record.timestamp_millis.to_be_bytes());
    encoded.extend_from_slice(&key_len.to_be_bytes());
    encoded.extend_from_slice(&value_len.to_be_bytes());
    encoded.extend_from_slice(&record.key);
    encoded.extend_from_slice(&record.value);
    let checksum = blake3::hash(&encoded);
    encoded.extend_from_slice(checksum.as_bytes());
    Ok(encoded)
}

pub(crate) fn read_frame_v2<R: Read>(
    reader: &mut R,
    position: u64,
    maximum: u32,
) -> VtopLogResult<FrameRead> {
    let mut prefix = [0_u8; FRAME_PREFIX_LEN];
    let mut prefix_read = 0;
    while prefix_read < prefix.len() {
        match reader.read(&mut prefix[prefix_read..]) {
            Ok(0) if prefix_read == 0 => return Ok(FrameRead::End),
            Ok(0) => {
                let comparable = prefix_read.min(RECORD_MAGIC_V2.len());
                if prefix[..comparable] != RECORD_MAGIC_V2[..comparable] {
                    return Err(LogError::Corrupt {
                        position,
                        reason: "invalid record magic in incomplete frame".to_owned(),
                    });
                }
                return Ok(FrameRead::Torn);
            }
            Ok(count) => prefix_read += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(source) => {
                return Err(LogError::Io {
                    path: "<segment>".into(),
                    source,
                });
            }
        }
    }
    if &prefix[..8] != RECORD_MAGIC_V2 {
        return Err(LogError::Corrupt {
            position,
            reason: "invalid record magic".to_owned(),
        });
    }
    let body_len = u32::from_be_bytes(prefix[8..12].try_into().expect("fixed slice")) as usize;
    let largest = RECORD_FIXED_BODY_LEN_V2 + maximum as usize;
    if !(RECORD_FIXED_BODY_LEN_V2..=largest).contains(&body_len) {
        return Err(LogError::Corrupt {
            position,
            reason: format!("invalid record frame length {body_len}"),
        });
    }
    let mut body = vec![0_u8; body_len];
    let mut body_read = 0;
    while body_read < body.len() {
        match reader.read(&mut body[body_read..]) {
            Ok(0) => return Ok(FrameRead::Torn),
            Ok(count) => body_read += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(source) => {
                return Err(LogError::Io {
                    path: "<segment>".into(),
                    source,
                });
            }
        }
    }
    let payload_end = body_len - CHECKSUM_LEN;
    let mut authenticated = prefix.to_vec();
    authenticated.extend_from_slice(&body[..payload_end]);
    if blake3::hash(&authenticated).as_bytes() != &body[payload_end..] {
        return Err(LogError::Corrupt {
            position,
            reason: "record checksum mismatch".to_owned(),
        });
    }
    let relative_offset = u64::from_be_bytes(body[..8].try_into().expect("fixed slice"));
    let producer_id = uuid::Uuid::from_slice(&body[8..24]).map_err(|error| LogError::Corrupt {
        position,
        reason: format!("invalid producer id: {error}"),
    })?;
    let producer_epoch = u64::from_be_bytes(body[24..32].try_into().expect("fixed slice"));
    let sequence = u64::from_be_bytes(body[32..40].try_into().expect("fixed slice"));
    let attributes = u16::from_be_bytes(body[40..42].try_into().expect("fixed slice"));
    if attributes != 0 {
        return Err(LogError::Corrupt {
            position,
            reason: format!("record attributes must be zero in schema v2, got {attributes:#06x}"),
        });
    }
    let timestamp_millis = i64::from_be_bytes(body[42..50].try_into().expect("fixed slice"));
    let key_len = u32::from_be_bytes(body[50..54].try_into().expect("fixed slice")) as usize;
    let value_len = u32::from_be_bytes(body[54..58].try_into().expect("fixed slice")) as usize;
    if key_len.checked_add(value_len) != Some(payload_end - 58) {
        return Err(LogError::Corrupt {
            position,
            reason: "record key/value lengths do not match the frame".to_owned(),
        });
    }
    let key_end = 58 + key_len;
    let mut encoded = prefix.to_vec();
    encoded.extend_from_slice(&body);
    Ok(FrameRead::Complete(DecodedFrame {
        relative_offset,
        record: LogRecord {
            producer_id,
            producer_epoch,
            sequence,
            timestamp_millis,
            attributes,
            key: body[58..key_end].to_vec(),
            value: body[key_end..payload_end].to_vec(),
        },
        encoded_len: encoded.len(),
        encoded,
    }))
}

fn header_read_error(source: std::io::Error, position: u64, part: &str) -> LogError {
    if source.kind() == std::io::ErrorKind::UnexpectedEof {
        LogError::Corrupt {
            position,
            reason: format!("incomplete {part}: {source}"),
        }
    } else {
        LogError::Io {
            path: "<segment>".into(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::FrameRead;
    use std::io::Cursor;

    fn golden_descriptor() -> SegmentDescriptorV2 {
        SegmentDescriptorV2 {
            segment_id: uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            topic: "audit.v1".to_owned(),
            topic_epoch: 3,
            lineage: crate::RangeLineage::root(
                uuid::Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
            ),
            base_offset: 42,
            segment_generation: 7,
            creation_node_id: uuid::Uuid::parse_str("12345678-9abc-def0-1234-56789abcdef0")
                .unwrap(),
            creation_fencing_epoch: 5,
        }
    }

    fn golden_config() -> SegmentConfigV2 {
        SegmentConfigV2 {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16_384,
            max_segment_records: 100,
            index_stride: 2,
            chunk_size: 65_536,
        }
    }

    fn golden_header() -> SegmentHeaderV2 {
        SegmentHeaderV2::new(golden_descriptor(), golden_config())
    }

    fn golden_record() -> LogRecord {
        LogRecord {
            producer_id: uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            producer_epoch: 0x1112_1314_1516_1718,
            sequence: 0x0102_0304_0506_0708,
            timestamp_millis: -2,
            attributes: 0,
            key: b"k".to_vec(),
            value: b"value".to_vec(),
        }
    }

    fn to_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    #[test]
    fn v2_segment_header_matches_golden_vector() {
        let encoded = encode_header_v2(&golden_header()).unwrap();
        assert_eq!(
            to_hex(&encoded),
            concat!(
                "56544f5053454732000002177b22666f726d6174223a2276746f702d6e61746976652d7365676d656e7422",
                "2c2276657273696f6e223a322c2264657363726970746f72223a7b227365676d656e745f6964223a223030",
                "3131323233332d343435352d363637372d383839392d616162626363646465656666222c22746f70696322",
                "3a2261756469742e7631222c22746f7069635f65706f6368223a332c226c696e65616765223a7b2272616e",
                "67655f6964223a2266666565646463632d626261612d393938382d373736362d3535343433333232313130",
                "30222c2267656e65726174696f6e223a302c226b65795f72616e6765223a7b22707265666978223a302c22",
                "7072656669785f62697473223a307d7d2c22626173655f6f6666736574223a34322c227365676d656e745f",
                "67656e65726174696f6e223a372c226372656174696f6e5f6e6f64655f6964223a2231323334353637382d",
                "396162632d646566302d313233342d353637383961626364656630222c226372656174696f6e5f66656e63",
                "696e675f65706f6368223a357d2c22636f6e666967223a7b226d61785f7265636f72645f6279746573223a",
                "313032342c226d61785f67726f75705f6279746573223a343039362c226d61785f7365676d656e745f6279",
                "746573223a31363338342c226d61785f7365676d656e745f7265636f726473223a3130302c22696e646578",
                "5f737472696465223a322c226368756e6b5f73697a65223a36353533367d7d87f6e96950934b7abb87feb2",
                "8529f265405dc683bab27ad2624cba3fcb1e2e5e"
            )
        );
    }

    #[test]
    fn v2_record_frame_with_nonzero_producer_epoch_matches_golden_vector() {
        let encoded = encode_record_v2(&golden_record(), 9, 1024).unwrap();
        assert_eq!(
            to_hex(&encoded),
            concat!(
                "56544f505245433200000060000000000000000900112233445566778899aabbccddeeff11121314151617",
                "1801020304050607080000fffffffffffffffe00000001000000056b76616c75656aaf7fcaa5a99c503034",
                "921fe4fa33425ee6f7ff38304c375692ffc1aceeed54"
            )
        );
    }

    #[test]
    fn v2_header_round_trips_and_rejects_mutations() {
        let encoded = encode_header_v2(&golden_header()).unwrap();
        let (decoded, content_start) = read_header_v2(&mut Cursor::new(encoded.clone())).unwrap();
        assert_eq!(decoded, golden_header());
        assert_eq!(content_start, encoded.len() as u64);

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            read_header_v2(&mut Cursor::new(bad_magic)),
            Err(LogError::Corrupt { position: 0, .. })
        ));

        // A v1 envelope must never decode as v2.
        let v1_magic = crate::codec::HEADER_MAGIC.to_vec();
        assert!(matches!(
            read_header_v2(&mut Cursor::new(v1_magic)),
            Err(LogError::Corrupt { position: 0, .. })
        ));

        let mut bad_checksum = encoded;
        *bad_checksum.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            read_header_v2(&mut Cursor::new(bad_checksum)),
            Err(LogError::Corrupt { position: 0, .. })
        ));

        let mut future = golden_header();
        future.version = FORMAT_VERSION_V2 + 1;
        assert!(matches!(
            read_header_v2(&mut Cursor::new(encode_header_v2(&future).unwrap())),
            Err(LogError::UnsupportedVersion(version)) if version == FORMAT_VERSION_V2 + 1
        ));

        let mut oversized = HEADER_MAGIC_V2.to_vec();
        oversized.extend_from_slice(&(MAX_HEADER_BYTES + 1).to_be_bytes());
        assert!(matches!(
            read_header_v2(&mut Cursor::new(oversized)),
            Err(LogError::Corrupt { position: 8, .. })
        ));
    }

    #[test]
    fn v2_record_frame_round_trips_all_fields() {
        let record = golden_record();
        let encoded = encode_record_v2(&record, 9, 1024).unwrap();
        match read_frame_v2(&mut Cursor::new(encoded.clone()), 0, 1024).unwrap() {
            FrameRead::Complete(frame) => {
                assert_eq!(frame.relative_offset, 9);
                assert_eq!(frame.record, record);
                assert_eq!(frame.encoded, encoded);
                assert_eq!(frame.encoded_len, encoded.len());
            }
            _ => panic!("expected a complete frame"),
        }
    }

    #[test]
    fn nonzero_attributes_are_rejected_on_encode_and_decode() {
        let mut flagged = golden_record();
        flagged.attributes = 1;
        assert!(matches!(
            encode_record_v2(&flagged, 0, 1024),
            Err(LogError::UnsupportedRecordField("attributes"))
        ));

        // Hand-craft a frame with nonzero attribute bits and a valid checksum
        // so the schema check itself is what rejects it.
        let mut forged = encode_record_v2(&golden_record(), 9, 1024).unwrap();
        let attributes_at = FRAME_PREFIX_LEN + 8 + 16 + 8 + 8;
        forged[attributes_at..attributes_at + 2].copy_from_slice(&0x8001_u16.to_be_bytes());
        let checksum_at = forged.len() - CHECKSUM_LEN;
        let checksum = blake3::hash(&forged[..checksum_at]);
        forged[checksum_at..].copy_from_slice(checksum.as_bytes());
        assert!(matches!(
            read_frame_v2(&mut Cursor::new(forged), 17, 1024),
            Err(LogError::Corrupt { position: 17, reason })
                if reason.contains("attributes must be zero")
        ));
    }

    #[test]
    fn incomplete_v2_frame_requires_a_valid_magic_prefix() {
        for retained in 1..FRAME_PREFIX_LEN {
            let mut valid = Cursor::new(RECORD_MAGIC_V2[..retained.min(8)].to_vec());
            assert!(matches!(
                read_frame_v2(&mut valid, 99, 1024).unwrap(),
                FrameRead::Torn
            ));
        }

        let mut invalid = Cursor::new(b"XTOPREC2bad".to_vec());
        assert!(matches!(
            read_frame_v2(&mut invalid, 123, 1024),
            Err(LogError::Corrupt { position: 123, .. })
        ));
    }

    #[test]
    fn v2_record_rejects_magic_checksum_length_trailing_and_torn_mutations() {
        let encoded = encode_record_v2(&golden_record(), 0, 1024).unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            read_frame_v2(&mut Cursor::new(bad_magic), 7, 1024),
            Err(LogError::Corrupt { position: 7, .. })
        ));

        let mut bad_checksum = encoded.clone();
        *bad_checksum.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            read_frame_v2(&mut Cursor::new(bad_checksum), 8, 1024),
            Err(LogError::Corrupt { position: 8, .. })
        ));

        let mut bad_length = encoded.clone();
        bad_length[8..12].copy_from_slice(&((RECORD_FIXED_BODY_LEN_V2 - 1) as u32).to_be_bytes());
        assert!(matches!(
            read_frame_v2(&mut Cursor::new(bad_length), 9, 1024),
            Err(LogError::Corrupt { position: 9, .. })
        ));

        let mut oversized = RECORD_MAGIC_V2.to_vec();
        oversized.extend_from_slice(&((RECORD_FIXED_BODY_LEN_V2 + 1025) as u32).to_be_bytes());
        assert!(matches!(
            read_frame_v2(&mut Cursor::new(oversized), 10, 1024),
            Err(LogError::Corrupt { position: 10, .. })
        ));

        // Truncation anywhere inside the body reads as a torn frame, exactly
        // like the v1 recovery contract.
        for keep in [FRAME_PREFIX_LEN, FRAME_PREFIX_LEN + 1, encoded.len() - 1] {
            let mut torn = Cursor::new(encoded[..keep].to_vec());
            assert!(matches!(
                read_frame_v2(&mut torn, 12, 1024).unwrap(),
                FrameRead::Torn
            ));
        }

        let mut trailing = encoded;
        trailing.push(b'X');
        let mut reader = Cursor::new(trailing);
        assert!(matches!(
            read_frame_v2(&mut reader, 11, 1024).unwrap(),
            FrameRead::Complete(_)
        ));
        let trailing_position = reader.position();
        assert!(matches!(
            read_frame_v2(&mut reader, trailing_position, 1024),
            Err(LogError::Corrupt { .. })
        ));
    }
}
