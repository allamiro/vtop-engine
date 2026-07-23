//! Bounded big-endian wire helpers shared by every hand-coded codec in the
//! crate.
//!
//! There is deliberately no serde here: every durable byte of metadata is
//! written by these helpers so the on-disk format is exactly what the code
//! says, byte for byte. All integers are big-endian, all variable-length
//! fields are length-delimited with an explicit bound, and every top-level
//! decode must call [`Reader::finish`] so trailing bytes are rejected.

use thiserror::Error;
use uuid::Uuid;

/// Precise, deterministic decode failures. These are codec errors, never I/O
/// errors: the caller decides whether a failure means corruption on disk or a
/// malformed proposal.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("truncated while decoding {0}")]
    Truncated(&'static str),
    #[error("{0} trailing bytes after a complete value")]
    Trailing(usize),
    #[error("unknown {what} tag {tag}")]
    UnknownTag { what: &'static str, tag: u32 },
    #[error("{what} is {actual} bytes; the bound is {maximum}")]
    BoundExceeded {
        what: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("{0} is not valid UTF-8")]
    InvalidUtf8(&'static str),
    #[error("invalid {what}: {reason}")]
    InvalidValue {
        what: &'static str,
        reason: &'static str,
    },
}

pub(crate) fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_uuid(out: &mut Vec<u8>, value: Uuid) {
    out.extend_from_slice(value.as_bytes());
}

pub(crate) fn put_bytes32(out: &mut Vec<u8>, value: &[u8; 32]) {
    out.extend_from_slice(value);
}

/// Encode a `u16`-length-delimited string, rejecting anything over `maximum`
/// at encode time so an oversized value can never even leave the process.
pub(crate) fn put_bounded_str(
    out: &mut Vec<u8>,
    value: &str,
    maximum: usize,
    what: &'static str,
) -> Result<(), CodecError> {
    if value.len() > maximum {
        return Err(CodecError::BoundExceeded {
            what,
            actual: value.len(),
            maximum,
        });
    }
    put_u16(out, value.len() as u16);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

/// Cursor over an immutable byte slice with bounded, named reads.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn take(
        &mut self,
        count: usize,
        what: &'static str,
    ) -> Result<&'a [u8], CodecError> {
        if self.remaining() < count {
            return Err(CodecError::Truncated(what));
        }
        let slice = &self.bytes[self.offset..self.offset + count];
        self.offset += count;
        Ok(slice)
    }

    pub(crate) fn u8(&mut self, what: &'static str) -> Result<u8, CodecError> {
        Ok(self.take(1, what)?[0])
    }

    pub(crate) fn u16(&mut self, what: &'static str) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.take(2, what)?.try_into().expect("fixed slice"),
        ))
    }

    pub(crate) fn u32(&mut self, what: &'static str) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.take(4, what)?.try_into().expect("fixed slice"),
        ))
    }

    pub(crate) fn u64(&mut self, what: &'static str) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.take(8, what)?.try_into().expect("fixed slice"),
        ))
    }

    pub(crate) fn i64(&mut self, what: &'static str) -> Result<i64, CodecError> {
        Ok(i64::from_be_bytes(
            self.take(8, what)?.try_into().expect("fixed slice"),
        ))
    }

    pub(crate) fn uuid(&mut self, what: &'static str) -> Result<Uuid, CodecError> {
        Ok(Uuid::from_bytes(
            self.take(16, what)?.try_into().expect("fixed slice"),
        ))
    }

    pub(crate) fn bytes32(&mut self, what: &'static str) -> Result<[u8; 32], CodecError> {
        Ok(self.take(32, what)?.try_into().expect("fixed slice"))
    }

    /// A strict boolean: any byte other than 0 or 1 is rejected so encodings
    /// stay canonical.
    pub(crate) fn flag(&mut self, what: &'static str) -> Result<bool, CodecError> {
        match self.u8(what)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodecError::InvalidValue {
                what,
                reason: "flag byte must be 0 or 1",
            }),
        }
    }

    pub(crate) fn bounded_str(
        &mut self,
        maximum: usize,
        what: &'static str,
    ) -> Result<String, CodecError> {
        let length = self.u16(what)? as usize;
        if length > maximum {
            return Err(CodecError::BoundExceeded {
                what,
                actual: length,
                maximum,
            });
        }
        let bytes = self.take(length, what)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidUtf8(what))
    }

    /// Reject trailing bytes; every top-level decode must end with this.
    pub(crate) fn finish(self) -> Result<(), CodecError> {
        if self.remaining() != 0 {
            return Err(CodecError::Trailing(self.remaining()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_str_round_trips_and_rejects_over_bound_and_invalid_utf8() {
        let mut out = Vec::new();
        put_bounded_str(&mut out, "addr", 8, "test string").unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(reader.bounded_str(8, "test string").unwrap(), "addr");
        reader.finish().unwrap();

        assert_eq!(
            put_bounded_str(&mut Vec::new(), "too-long", 4, "test string"),
            Err(CodecError::BoundExceeded {
                what: "test string",
                actual: 8,
                maximum: 4,
            })
        );

        // A declared length over the bound is rejected before any byte of the
        // payload is read.
        let mut oversized = Vec::new();
        put_u16(&mut oversized, 9);
        oversized.extend_from_slice(b"123456789");
        assert_eq!(
            Reader::new(&oversized).bounded_str(4, "test string"),
            Err(CodecError::BoundExceeded {
                what: "test string",
                actual: 9,
                maximum: 4,
            })
        );

        let mut invalid = Vec::new();
        put_u16(&mut invalid, 2);
        invalid.extend_from_slice(&[0xff, 0xfe]);
        assert_eq!(
            Reader::new(&invalid).bounded_str(8, "test string"),
            Err(CodecError::InvalidUtf8("test string"))
        );
    }

    #[test]
    fn flags_are_canonical_and_trailing_bytes_are_rejected() {
        assert!(!Reader::new(&[0]).flag("flag").unwrap());
        assert!(Reader::new(&[1]).flag("flag").unwrap());
        assert_eq!(
            Reader::new(&[2]).flag("flag"),
            Err(CodecError::InvalidValue {
                what: "flag",
                reason: "flag byte must be 0 or 1",
            })
        );

        let mut reader = Reader::new(&[0, 7]);
        reader.flag("flag").unwrap();
        assert_eq!(reader.finish(), Err(CodecError::Trailing(1)));
    }
}
