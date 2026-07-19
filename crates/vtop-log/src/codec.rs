use crate::types::{
    LogError, LogRecord, SegmentConfig, SegmentDescriptor, VtopLogResult, FORMAT_NAME,
    FORMAT_VERSION, RECORD_FRAME_OVERHEAD_BYTES,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

pub(crate) const HEADER_MAGIC: &[u8; 8] = b"VTOPSEG1";
pub(crate) const RECORD_MAGIC: &[u8; 8] = b"VTOPREC1";
pub(crate) const INDEX_MAGIC: &[u8; 8] = b"VTOPIDX1";
pub(crate) const CHECKSUM_LEN: usize = 32;
pub(crate) const FRAME_PREFIX_LEN: usize = 12;
pub(crate) const RECORD_FIXED_BODY_LEN: usize =
    RECORD_FRAME_OVERHEAD_BYTES as usize - FRAME_PREFIX_LEN;
const MAX_HEADER_BYTES: u32 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SegmentHeader {
    pub format: String,
    pub version: u16,
    pub descriptor: SegmentDescriptor,
    pub config: SegmentConfig,
}

impl SegmentHeader {
    pub(crate) fn new(descriptor: SegmentDescriptor, config: SegmentConfig) -> Self {
        Self {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION,
            descriptor,
            config,
        }
    }
}

pub(crate) struct DecodedFrame {
    pub relative_offset: u64,
    pub record: LogRecord,
    pub encoded_len: usize,
    pub encoded: Vec<u8>,
}

pub(crate) enum FrameRead {
    Complete(DecodedFrame),
    End,
    Torn,
}

pub(crate) fn encode_header(header: &SegmentHeader) -> VtopLogResult<Vec<u8>> {
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
    encoded.extend_from_slice(HEADER_MAGIC);
    encoded.extend_from_slice(&json_len.to_be_bytes());
    encoded.extend_from_slice(&json);
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

pub(crate) fn read_header<R: Read + Seek>(reader: &mut R) -> VtopLogResult<(SegmentHeader, u64)> {
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
    if &prefix[..8] != HEADER_MAGIC {
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
    let header: SegmentHeader =
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
    if header.version != FORMAT_VERSION {
        return Err(LogError::UnsupportedVersion(header.version));
    }
    header.descriptor.validate()?;
    header.config.validate()?;
    Ok((header, 12 + u64::from(json_len) + CHECKSUM_LEN as u64))
}

pub(crate) fn record_content_hash(record: &LogRecord) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(record.producer_id.as_bytes());
    hasher.update(&record.sequence.to_be_bytes());
    hasher.update(&record.timestamp_millis.to_be_bytes());
    hasher.update(&(record.key.len() as u64).to_be_bytes());
    hasher.update(&record.key);
    hasher.update(&(record.value.len() as u64).to_be_bytes());
    hasher.update(&record.value);
    hasher.finalize()
}

pub(crate) fn record_payload_len(record: &LogRecord) -> VtopLogResult<usize> {
    record
        .key
        .len()
        .checked_add(record.value.len())
        .ok_or(LogError::RecordTooLarge {
            actual: usize::MAX,
            maximum: u32::MAX,
        })
}

pub(crate) fn encode_record(
    record: &LogRecord,
    relative_offset: u64,
    maximum: u32,
) -> VtopLogResult<Vec<u8>> {
    let payload_len = record_payload_len(record)?;
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
    let body_len = RECORD_FIXED_BODY_LEN
        .checked_add(payload_len)
        .and_then(|length| u32::try_from(length).ok())
        .ok_or(LogError::RecordTooLarge {
            actual: payload_len,
            maximum,
        })?;

    let mut encoded = Vec::with_capacity(FRAME_PREFIX_LEN + body_len as usize);
    encoded.extend_from_slice(RECORD_MAGIC);
    encoded.extend_from_slice(&body_len.to_be_bytes());
    encoded.extend_from_slice(&relative_offset.to_be_bytes());
    encoded.extend_from_slice(record.producer_id.as_bytes());
    encoded.extend_from_slice(&record.sequence.to_be_bytes());
    encoded.extend_from_slice(&record.timestamp_millis.to_be_bytes());
    encoded.extend_from_slice(&key_len.to_be_bytes());
    encoded.extend_from_slice(&value_len.to_be_bytes());
    encoded.extend_from_slice(&record.key);
    encoded.extend_from_slice(&record.value);
    let checksum = blake3::hash(&encoded);
    encoded.extend_from_slice(checksum.as_bytes());
    Ok(encoded)
}

pub(crate) fn read_frame<R: Read>(
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
                let comparable = prefix_read.min(RECORD_MAGIC.len());
                if prefix[..comparable] != RECORD_MAGIC[..comparable] {
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
    if &prefix[..8] != RECORD_MAGIC {
        return Err(LogError::Corrupt {
            position,
            reason: "invalid record magic".to_owned(),
        });
    }
    let body_len = u32::from_be_bytes(prefix[8..12].try_into().expect("fixed slice")) as usize;
    let largest = RECORD_FIXED_BODY_LEN + maximum as usize;
    if !(RECORD_FIXED_BODY_LEN..=largest).contains(&body_len) {
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
    let sequence = u64::from_be_bytes(body[24..32].try_into().expect("fixed slice"));
    let timestamp_millis = i64::from_be_bytes(body[32..40].try_into().expect("fixed slice"));
    let key_len = u32::from_be_bytes(body[40..44].try_into().expect("fixed slice")) as usize;
    let value_len = u32::from_be_bytes(body[44..48].try_into().expect("fixed slice")) as usize;
    if key_len.checked_add(value_len) != Some(payload_end - 48) {
        return Err(LogError::Corrupt {
            position,
            reason: "record key/value lengths do not match the frame".to_owned(),
        });
    }
    let key_end = 48 + key_len;
    let mut encoded = prefix.to_vec();
    encoded.extend_from_slice(&body);
    Ok(FrameRead::Complete(DecodedFrame {
        relative_offset,
        record: LogRecord {
            producer_id,
            producer_epoch: 0,
            sequence,
            timestamp_millis,
            attributes: 0,
            key: body[48..key_end].to_vec(),
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
    use std::io::{Cursor, Error, ErrorKind};

    fn golden_header() -> SegmentHeader {
        SegmentHeader::new(
            SegmentDescriptor {
                segment_id: uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                topic: "audit.v1".to_owned(),
                topic_epoch: 3,
                lineage: crate::RangeLineage::root(
                    uuid::Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
                ),
                base_offset: 42,
            },
            SegmentConfig {
                max_record_bytes: 1024,
                max_group_bytes: 4096,
                max_segment_bytes: 16_384,
                max_segment_records: 100,
                index_stride: 2,
            },
        )
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

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "injected read failure",
            ))
        }
    }

    impl Seek for FailingReader {
        fn seek(&mut self, _position: SeekFrom) -> std::io::Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn header_preserves_non_eof_io_errors() {
        assert!(matches!(
            read_header(&mut FailingReader),
            Err(LogError::Io { source, .. }) if source.kind() == ErrorKind::PermissionDenied
        ));
    }

    #[test]
    fn incomplete_header_is_corruption() {
        let mut reader = Cursor::new(HEADER_MAGIC[..4].to_vec());
        assert!(matches!(
            read_header(&mut reader),
            Err(LogError::Corrupt { position: 0, .. })
        ));
    }

    #[test]
    fn incomplete_frame_requires_a_valid_magic_prefix() {
        for retained in 1..FRAME_PREFIX_LEN {
            let mut valid = Cursor::new(RECORD_MAGIC[..retained.min(8)].to_vec());
            assert!(matches!(
                read_frame(&mut valid, 99, 1024).unwrap(),
                FrameRead::Torn
            ));
        }

        let mut invalid = Cursor::new(b"XTOPREC1bad".to_vec());
        assert!(matches!(
            read_frame(&mut invalid, 123, 1024),
            Err(LogError::Corrupt { position: 123, .. })
        ));
    }

    #[test]
    fn v1_segment_header_matches_golden_vector() {
        let encoded = encode_header(&golden_header()).unwrap();
        assert_eq!(
            to_hex(&encoded),
            concat!(
                "56544f5053454731000001987b22666f726d6174223a2276746f702d6e61746976652d7365676d656e7422",
                "2c2276657273696f6e223a312c2264657363726970746f72223a7b227365676d656e745f6964223a223030",
                "3131323233332d343435352d363637372d383839392d616162626363646465656666222c22746f70696322",
                "3a2261756469742e7631222c22746f7069635f65706f6368223a332c226c696e65616765223a7b2272616e",
                "67655f6964223a2266666565646463632d626261612d393938382d373736362d3535343433333232313130",
                "30222c2267656e65726174696f6e223a302c226b65795f72616e6765223a7b22707265666978223a302c22",
                "7072656669785f62697473223a307d7d2c22626173655f6f6666736574223a34327d2c22636f6e66696722",
                "3a7b226d61785f7265636f72645f6279746573223a313032342c226d61785f67726f75705f627974657322",
                "3a343039362c226d61785f7365676d656e745f6279746573223a31363338342c226d61785f7365676d656e",
                "745f7265636f726473223a3130302c22696e6465785f737472696465223a327d7d9635f1e071d009753a44",
                "126f4ea31b10525c890d2f6ee46ed14722fdac29e97a"
            )
        );
    }

    #[test]
    fn v1_record_frame_matches_golden_vector() {
        let record = LogRecord {
            producer_id: uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            producer_epoch: 0,
            sequence: 0x0102_0304_0506_0708,
            timestamp_millis: -2,
            attributes: 0,
            key: b"k".to_vec(),
            value: b"value".to_vec(),
        };
        let encoded = encode_record(&record, 9, 1024).unwrap();
        assert_eq!(
            to_hex(&encoded),
            concat!(
                "56544f505245433100000056000000000000000900112233445566778899aabbccddeeff0102030405060708",
                "fffffffffffffffe00000001000000056b76616c7565ef3974965bf3cbf7b4b1e10ef15253c40e667430aca",
                "62a68b9b9162cc67d8627"
            )
        );
    }

    #[test]
    fn v1_header_rejects_magic_checksum_version_and_oversize_mutations() {
        let encoded = encode_header(&golden_header()).unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            read_header(&mut Cursor::new(bad_magic)),
            Err(LogError::Corrupt { position: 0, .. })
        ));

        let mut bad_checksum = encoded;
        *bad_checksum.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            read_header(&mut Cursor::new(bad_checksum)),
            Err(LogError::Corrupt { position: 0, .. })
        ));

        let mut future = golden_header();
        future.version = FORMAT_VERSION + 1;
        assert!(matches!(
            read_header(&mut Cursor::new(encode_header(&future).unwrap())),
            Err(LogError::UnsupportedVersion(version)) if version == FORMAT_VERSION + 1
        ));

        let mut oversized = HEADER_MAGIC.to_vec();
        oversized.extend_from_slice(&(MAX_HEADER_BYTES + 1).to_be_bytes());
        assert!(matches!(
            read_header(&mut Cursor::new(oversized)),
            Err(LogError::Corrupt { position: 8, .. })
        ));
    }

    #[test]
    fn v1_record_rejects_magic_checksum_length_trailing_and_over_limit_mutations() {
        let record = LogRecord {
            producer_id: uuid::Uuid::from_u128(9),
            producer_epoch: 0,
            sequence: 0,
            timestamp_millis: 1,
            attributes: 0,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        let encoded = encode_record(&record, 0, 1024).unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            read_frame(&mut Cursor::new(bad_magic), 7, 1024),
            Err(LogError::Corrupt { position: 7, .. })
        ));

        let mut bad_checksum = encoded.clone();
        *bad_checksum.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            read_frame(&mut Cursor::new(bad_checksum), 8, 1024),
            Err(LogError::Corrupt { position: 8, .. })
        ));

        let mut bad_length = encoded.clone();
        bad_length[8..12].copy_from_slice(&((RECORD_FIXED_BODY_LEN - 1) as u32).to_be_bytes());
        assert!(matches!(
            read_frame(&mut Cursor::new(bad_length), 9, 1024),
            Err(LogError::Corrupt { position: 9, .. })
        ));

        let mut oversized = RECORD_MAGIC.to_vec();
        oversized.extend_from_slice(&((RECORD_FIXED_BODY_LEN + 1025) as u32).to_be_bytes());
        assert!(matches!(
            read_frame(&mut Cursor::new(oversized), 10, 1024),
            Err(LogError::Corrupt { position: 10, .. })
        ));

        let mut trailing = encoded;
        trailing.push(b'X');
        let mut reader = Cursor::new(trailing);
        assert!(matches!(
            read_frame(&mut reader, 11, 1024).unwrap(),
            FrameRead::Complete(_)
        ));
        let trailing_position = reader.position();
        assert!(matches!(
            read_frame(&mut reader, trailing_position, 1024),
            Err(LogError::Corrupt { .. })
        ));
    }
}
