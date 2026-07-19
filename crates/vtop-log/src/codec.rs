use crate::types::{
    LogError, LogRecord, SegmentConfig, SegmentDescriptor, VtopLogResult, FORMAT_NAME,
    FORMAT_VERSION,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

pub(crate) const HEADER_MAGIC: &[u8; 8] = b"VTOPSEG1";
pub(crate) const RECORD_MAGIC: &[u8; 8] = b"VTOPREC1";
pub(crate) const INDEX_MAGIC: &[u8; 8] = b"VTOPIDX1";
pub(crate) const CHECKSUM_LEN: usize = 32;
pub(crate) const FRAME_PREFIX_LEN: usize = 12;
pub(crate) const RECORD_FIXED_BODY_LEN: usize = 8 + 16 + 8 + 8 + 4 + 4 + CHECKSUM_LEN;
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
        .map_err(|source| LogError::Corrupt {
            position: 0,
            reason: format!("incomplete segment header: {source}"),
        })?;
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
        .map_err(|source| LogError::Corrupt {
            position: 12,
            reason: format!("incomplete segment header: {source}"),
        })?;
    let mut stored_hash = [0_u8; CHECKSUM_LEN];
    reader
        .read_exact(&mut stored_hash)
        .map_err(|source| LogError::Corrupt {
            position: 12 + u64::from(json_len),
            reason: format!("incomplete segment header checksum: {source}"),
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
            Ok(0) => return Ok(FrameRead::Torn),
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
            sequence,
            timestamp_millis,
            key: body[48..key_end].to_vec(),
            value: body[key_end..payload_end].to_vec(),
        },
        encoded_len: encoded.len(),
        encoded,
    }))
}
