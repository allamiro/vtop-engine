//! Kafka wire primitives — the encoding every request and response is built
//! from (#225).
//!
//! Kafka's protocol is positional and self-describing only by version: a field
//! is where the schema for that (api_key, api_version) says it is, and nothing
//! on the wire admits a mistake. So this module owns the primitives and NOTHING
//! else; message schemas live beside their API. That split is what lets the
//! nastiest part — flexible versions, where the same logical field changes
//! representation — be tested once here rather than re-derived per message.
//!
//! ALLOCATION IS BOUNDED AT THE DECODER, matching the native protocol's
//! doctrine (`vtop-protocol`): a length prefix read off a socket is an
//! attacker's number until it has been checked against a limit, and
//! `Vec::with_capacity` on an unchecked u32 is a remote OOM. Every length here
//! is validated against the remaining buffer before anything is reserved.

use std::fmt;

/// A decode failure. Every variant names the field, because a protocol error
/// with no field name means reading a hex dump to find out which one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    #[error("truncated reading {field}: needed {needed} byte(s), {available} left")]
    Truncated {
        field: &'static str,
        needed: usize,
        available: usize,
    },
    #[error("{field} has length {len}, which exceeds the {limit}-byte limit")]
    TooLong {
        field: &'static str,
        len: usize,
        limit: usize,
    },
    #[error("{field} has negative length {len}, which only null may carry")]
    NegativeLength { field: &'static str, len: i32 },
    #[error("{field} is not valid UTF-8")]
    NotUtf8 { field: &'static str },
    #[error("a varint in {field} did not terminate within {max} bytes")]
    VarIntTooLong { field: &'static str, max: usize },
    #[error("{field} carried {len} bytes of trailing data that no schema claims")]
    Trailing { field: &'static str, len: usize },
}

/// The largest string or byte block this codec will materialise from a length
/// prefix. Kafka's own topic-name limit is 249 bytes; this covers record
/// payloads too, and anything larger belongs in a batch, not a field.
pub const MAX_FIELD_BYTES: usize = 16 * 1024 * 1024;

/// A cursor over a request body.
///
/// Borrowed, not owned: a request arrives as one framed buffer, and every
/// string and byte block inside it is a slice of that buffer until something
/// actually needs an owned copy.
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl fmt::Debug for Decoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decoder({} of {} consumed)", self.pos, self.buf.len())
    }
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Fail if anything is left. Callers use this at the end of a request whose
    /// schema they believe they decoded in full: silently ignoring a tail means
    /// a version mismatch reads as success and produces garbage semantics.
    pub fn expect_consumed(&self, field: &'static str) -> Result<(), WireError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(WireError::Trailing {
                field,
                len: self.remaining(),
            })
        }
    }

    fn take(&mut self, n: usize, field: &'static str) -> Result<&'a [u8], WireError> {
        if self.remaining() < n {
            return Err(WireError::Truncated {
                field,
                needed: n,
                available: self.remaining(),
            });
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub fn i8(&mut self, field: &'static str) -> Result<i8, WireError> {
        Ok(self.take(1, field)?[0] as i8)
    }

    pub fn i16(&mut self, field: &'static str) -> Result<i16, WireError> {
        let b = self.take(2, field)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn i32(&mut self, field: &'static str) -> Result<i32, WireError> {
        let b = self.take(4, field)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u32(&mut self, field: &'static str) -> Result<u32, WireError> {
        Ok(self.i32(field)? as u32)
    }

    pub fn i64(&mut self, field: &'static str) -> Result<i64, WireError> {
        let b = self.take(8, field)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn bool(&mut self, field: &'static str) -> Result<bool, WireError> {
        Ok(self.i8(field)? != 0)
    }

    pub fn uuid(&mut self, field: &'static str) -> Result<uuid::Uuid, WireError> {
        let b = self.take(16, field)?;
        let mut raw = [0_u8; 16];
        raw.copy_from_slice(b);
        Ok(uuid::Uuid::from_bytes(raw))
    }

    /// Unsigned LEB128, Kafka's length prefix in flexible versions.
    ///
    /// Bounded at five bytes: a u32 cannot need more, and an unterminated
    /// varint is otherwise an infinite read on a hostile socket.
    pub fn uvarint(&mut self, field: &'static str) -> Result<u32, WireError> {
        let mut value: u32 = 0;
        for shift in 0..5 {
            let byte = self.take(1, field)?[0];
            value |= u32::from(byte & 0x7f) << (shift * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(WireError::VarIntTooLong { field, max: 5 })
    }

    /// Zigzag varint, the record-level integer encoding inside a batch.
    pub fn varint(&mut self, field: &'static str) -> Result<i32, WireError> {
        let raw = self.uvarint(field)?;
        Ok(((raw >> 1) as i32) ^ -((raw & 1) as i32))
    }

    /// Zigzag varlong. Ten bytes covers i64.
    pub fn varlong(&mut self, field: &'static str) -> Result<i64, WireError> {
        let mut value: u64 = 0;
        for shift in 0..10 {
            let byte = self.take(1, field)?[0];
            value |= u64::from(byte & 0x7f) << (shift * 7);
            if byte & 0x80 == 0 {
                return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
            }
        }
        Err(WireError::VarIntTooLong { field, max: 10 })
    }

    /// STRING: an i16 length that may not be negative.
    pub fn string(&mut self, field: &'static str) -> Result<&'a str, WireError> {
        match self.nullable_string(field)? {
            Some(s) => Ok(s),
            None => Err(WireError::NegativeLength { field, len: -1 }),
        }
    }

    /// NULLABLE_STRING: -1 is null, and only -1. Any other negative length is a
    /// malformed frame rather than a second spelling of null.
    pub fn nullable_string(&mut self, field: &'static str) -> Result<Option<&'a str>, WireError> {
        let len = self.i16(field)?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(WireError::NegativeLength {
                field,
                len: i32::from(len),
            });
        }
        let bytes = self.take(len as usize, field)?;
        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| WireError::NotUtf8 { field })
    }

    /// COMPACT_STRING: an unsigned varint of length+1, where 0 means null.
    pub fn compact_string(&mut self, field: &'static str) -> Result<&'a str, WireError> {
        match self.compact_nullable_string(field)? {
            Some(s) => Ok(s),
            None => Err(WireError::NegativeLength { field, len: -1 }),
        }
    }

    pub fn compact_nullable_string(
        &mut self,
        field: &'static str,
    ) -> Result<Option<&'a str>, WireError> {
        let raw = self.uvarint(field)?;
        if raw == 0 {
            return Ok(None);
        }
        let len = (raw - 1) as usize;
        self.guard_len(len, field)?;
        let bytes = self.take(len, field)?;
        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| WireError::NotUtf8 { field })
    }

    /// NULLABLE_BYTES: an i32 length, -1 for null.
    pub fn nullable_bytes(&mut self, field: &'static str) -> Result<Option<&'a [u8]>, WireError> {
        let len = self.i32(field)?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(WireError::NegativeLength { field, len });
        }
        self.guard_len(len as usize, field)?;
        self.take(len as usize, field).map(Some)
    }

    /// COMPACT_NULLABLE_BYTES: unsigned varint of length+1, 0 for null.
    pub fn compact_nullable_bytes(
        &mut self,
        field: &'static str,
    ) -> Result<Option<&'a [u8]>, WireError> {
        let raw = self.uvarint(field)?;
        if raw == 0 {
            return Ok(None);
        }
        let len = (raw - 1) as usize;
        self.guard_len(len, field)?;
        self.take(len, field).map(Some)
    }

    /// The element count of an ARRAY, or `None` for a null array.
    pub fn array_len(&mut self, field: &'static str) -> Result<Option<usize>, WireError> {
        let len = self.i32(field)?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(WireError::NegativeLength { field, len });
        }
        self.guard_count(len as usize, field)?;
        Ok(Some(len as usize))
    }

    /// The element count of a COMPACT_ARRAY (varint of count+1, 0 for null).
    pub fn compact_array_len(&mut self, field: &'static str) -> Result<Option<usize>, WireError> {
        let raw = self.uvarint(field)?;
        if raw == 0 {
            return Ok(None);
        }
        let count = (raw - 1) as usize;
        self.guard_count(count, field)?;
        Ok(Some(count))
    }

    /// Skip the tagged-field section that terminates every flexible-version
    /// struct. The gateway understands no tags, and skipping is CORRECT rather
    /// than lossy: tagged fields are defined to be optional, so a peer that
    /// needs one honoured must not have sent it to a version that omits it.
    pub fn skip_tagged_fields(&mut self, field: &'static str) -> Result<(), WireError> {
        let count = self.uvarint(field)?;
        self.guard_count(count as usize, field)?;
        for _ in 0..count {
            let _tag = self.uvarint(field)?;
            let size = self.uvarint(field)? as usize;
            self.guard_len(size, field)?;
            self.take(size, field)?;
        }
        Ok(())
    }

    /// A length is only allocated against after it fits both the hard limit and
    /// what is actually left in the buffer.
    fn guard_len(&self, len: usize, field: &'static str) -> Result<(), WireError> {
        if len > MAX_FIELD_BYTES {
            return Err(WireError::TooLong {
                field,
                len,
                limit: MAX_FIELD_BYTES,
            });
        }
        if len > self.remaining() {
            return Err(WireError::Truncated {
                field,
                needed: len,
                available: self.remaining(),
            });
        }
        Ok(())
    }

    /// An element count is bounded by the bytes left: every element costs at
    /// least one byte, so a count larger than the remainder is a lie, and
    /// reserving for it is the OOM this check exists to refuse.
    fn guard_count(&self, count: usize, field: &'static str) -> Result<(), WireError> {
        if count > self.remaining() {
            return Err(WireError::Truncated {
                field,
                needed: count,
                available: self.remaining(),
            });
        }
        Ok(())
    }
}

/// A growable response buffer.
///
/// Encoding cannot fail: everything written is already a well-typed Rust value,
/// and the only length that could overflow the wire's i32 is a body larger than
/// this process can hold. That is why nothing here returns `Result` — a
/// `Result` nobody can trigger trains callers to `unwrap` the ones that matter.
#[derive(Debug, Default)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bytes),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    pub fn raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    pub fn i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn bool(&mut self, v: bool) {
        self.i8(i8::from(v));
    }

    pub fn uuid(&mut self, v: uuid::Uuid) {
        self.buf.extend_from_slice(v.as_bytes());
    }

    pub fn uvarint(&mut self, mut v: u32) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.buf.push(byte);
                return;
            }
            self.buf.push(byte | 0x80);
        }
    }

    pub fn varint(&mut self, v: i32) {
        self.uvarint(((v << 1) ^ (v >> 31)) as u32);
    }

    pub fn varlong(&mut self, v: i64) {
        let mut zig = ((v << 1) ^ (v >> 63)) as u64;
        loop {
            let byte = (zig & 0x7f) as u8;
            zig >>= 7;
            if zig == 0 {
                self.buf.push(byte);
                return;
            }
            self.buf.push(byte | 0x80);
        }
    }

    pub fn string(&mut self, v: &str) {
        self.i16(v.len() as i16);
        self.raw(v.as_bytes());
    }

    pub fn nullable_string(&mut self, v: Option<&str>) {
        match v {
            Some(s) => self.string(s),
            None => self.i16(-1),
        }
    }

    pub fn compact_string(&mut self, v: &str) {
        self.uvarint(v.len() as u32 + 1);
        self.raw(v.as_bytes());
    }

    pub fn compact_nullable_string(&mut self, v: Option<&str>) {
        match v {
            Some(s) => self.compact_string(s),
            None => self.uvarint(0),
        }
    }

    pub fn nullable_bytes(&mut self, v: Option<&[u8]>) {
        match v {
            Some(b) => {
                self.i32(b.len() as i32);
                self.raw(b);
            }
            None => self.i32(-1),
        }
    }

    pub fn compact_nullable_bytes(&mut self, v: Option<&[u8]>) {
        match v {
            Some(b) => {
                self.uvarint(b.len() as u32 + 1);
                self.raw(b);
            }
            None => self.uvarint(0),
        }
    }

    pub fn array_len(&mut self, count: usize) {
        self.i32(count as i32);
    }

    pub fn compact_array_len(&mut self, count: usize) {
        self.uvarint(count as u32 + 1);
    }

    /// The empty tagged-field section every flexible struct ends with.
    pub fn empty_tagged_fields(&mut self) {
        self.uvarint(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-tripping is the cheap half. The half that matters is that the
    /// COMPACT forms differ from the classic ones on the wire — the same
    /// logical field, two encodings, chosen by version. A codec that quietly
    /// used one for the other would round-trip perfectly against itself and
    /// fail against every real client.
    #[test]
    fn compact_and_classic_strings_are_different_bytes() {
        let mut classic = Encoder::new();
        classic.string("abc");
        assert_eq!(classic.as_slice(), &[0, 3, b'a', b'b', b'c']);

        let mut compact = Encoder::new();
        compact.compact_string("abc");
        assert_eq!(compact.as_slice(), &[4, b'a', b'b', b'c']);
    }

    #[test]
    fn null_has_one_spelling_in_each_form() {
        let mut e = Encoder::new();
        e.nullable_string(None);
        e.compact_nullable_string(None);
        assert_eq!(e.as_slice(), &[0xff, 0xff, 0x00]);

        let mut d = Decoder::new(e.as_slice());
        assert_eq!(d.nullable_string("s").unwrap(), None);
        assert_eq!(d.compact_nullable_string("s").unwrap(), None);
    }

    /// -2 is not null. Kafka defines exactly -1, and accepting other negatives
    /// would let a peer choose which of two decoders sees a value.
    #[test]
    fn a_negative_length_that_is_not_minus_one_is_refused() {
        let bytes = [0xff, 0xfe];
        let mut d = Decoder::new(&bytes);
        assert!(matches!(
            d.nullable_string("s"),
            Err(WireError::NegativeLength { len: -2, .. })
        ));
    }

    #[test]
    fn zigzag_varints_round_trip_across_the_sign() {
        for v in [0_i32, -1, 1, 63, -64, i32::MAX, i32::MIN] {
            let mut e = Encoder::new();
            e.varint(v);
            let mut d = Decoder::new(e.as_slice());
            assert_eq!(d.varint("v").unwrap(), v, "varint {v}");
        }
        for v in [0_i64, -1, 1, i64::MAX, i64::MIN, 1_234_567_890_123] {
            let mut e = Encoder::new();
            e.varlong(v);
            let mut d = Decoder::new(e.as_slice());
            assert_eq!(d.varlong("v").unwrap(), v, "varlong {v}");
        }
    }

    /// Known vectors, not just self-consistency: these are the encodings Kafka
    /// itself produces, so a sign or shift error cannot hide behind a symmetric
    /// bug in this file's own encoder.
    #[test]
    fn varint_encodings_match_the_protocol_definition() {
        let cases: &[(i32, &[u8])] = &[
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (-2, &[0x03]),
            (300, &[0xd8, 0x04]),
        ];
        for (value, expected) in cases {
            let mut e = Encoder::new();
            e.varint(*value);
            assert_eq!(e.as_slice(), *expected, "varint {value}");
        }
    }

    /// A length prefix off a socket is an attacker's number. Reserving for it
    /// before checking is the remote OOM this codec refuses to have.
    #[test]
    fn a_length_larger_than_the_buffer_is_refused_before_allocating() {
        let mut bytes = vec![0x7f, 0xff, 0xff, 0xff]; // i32::MAX, ~2GiB
        bytes.extend_from_slice(b"only a few bytes follow");
        let mut d = Decoder::new(&bytes);
        assert!(matches!(
            d.nullable_bytes("payload"),
            Err(WireError::TooLong { .. }) | Err(WireError::Truncated { .. })
        ));
    }

    #[test]
    fn an_array_count_larger_than_the_remaining_bytes_is_refused() {
        let bytes = [0x7f, 0xff, 0xff, 0xff]; // count ~2 billion, no elements
        let mut d = Decoder::new(&bytes);
        assert!(matches!(
            d.array_len("topics"),
            Err(WireError::Truncated { .. })
        ));
    }

    #[test]
    fn an_unterminated_varint_cannot_read_forever() {
        let bytes = [0x80_u8; 12];
        let mut d = Decoder::new(&bytes);
        assert!(matches!(
            d.uvarint("len"),
            Err(WireError::VarIntTooLong { max: 5, .. })
        ));
    }

    #[test]
    fn tagged_fields_are_skipped_whole() {
        let mut e = Encoder::new();
        e.uvarint(2); // two tagged fields
        e.uvarint(0); // tag 0
        e.uvarint(3); // three bytes
        e.raw(b"abc");
        e.uvarint(9); // tag 9
        e.uvarint(1);
        e.raw(b"z");
        e.i16(0x4242); // the field that follows the section

        let mut d = Decoder::new(e.as_slice());
        d.skip_tagged_fields("header").unwrap();
        assert_eq!(d.i16("after").unwrap(), 0x4242);
        d.expect_consumed("header").unwrap();
    }

    #[test]
    fn a_trailing_tail_is_reported_rather_than_ignored() {
        let bytes = [0_u8, 1, 2, 3];
        let mut d = Decoder::new(&bytes);
        d.i16("first").unwrap();
        assert!(matches!(
            d.expect_consumed("request"),
            Err(WireError::Trailing { len: 2, .. })
        ));
    }
}
