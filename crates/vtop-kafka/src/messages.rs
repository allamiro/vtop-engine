//! Request headers, API identity, and error codes — the frame every Kafka
//! exchange sits inside (#225).
//!
//! Kafka has no self-describing envelope: a request is an api key, an api
//! version, and a body whose shape those two select. Everything this module
//! owns is that outer frame, and the one decision it forces — WHICH VERSIONS
//! THIS GATEWAY ADMITS — which is the difference between refusing a client
//! cleanly and mis-parsing it.
//!
//! A version this gateway does not implement is refused with
//! `UNSUPPORTED_VERSION` *before* the body is decoded. That is not politeness:
//! the flexible-versions change moved strings and arrays to a different
//! encoding, so decoding a v9 body with a v8 schema does not fail, it succeeds
//! and produces nonsense.

use crate::wire::{Decoder, Encoder, WireError};

/// The phase-1 API surface. Everything outside it is refused by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    ApiVersions,
    /// Idempotent producers (#457): the id and epoch a client's batches carry.
    InitProducerId,
    /// Consumer groups (#457, slice 2): this gateway coordinates every group
    /// it serves, and keeps their committed offsets through an offset store.
    FindCoordinator,
    JoinGroup,
    SyncGroup,
    Heartbeat,
    LeaveGroup,
    OffsetCommit,
    OffsetFetch,
}

impl ApiKey {
    pub fn from_i16(key: i16) -> Option<Self> {
        match key {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            18 => Some(Self::ApiVersions),
            22 => Some(Self::InitProducerId),
            10 => Some(Self::FindCoordinator),
            11 => Some(Self::JoinGroup),
            14 => Some(Self::SyncGroup),
            12 => Some(Self::Heartbeat),
            13 => Some(Self::LeaveGroup),
            8 => Some(Self::OffsetCommit),
            9 => Some(Self::OffsetFetch),
            _ => None,
        }
    }

    pub fn as_i16(self) -> i16 {
        match self {
            Self::Produce => 0,
            Self::Fetch => 1,
            Self::ListOffsets => 2,
            Self::Metadata => 3,
            Self::ApiVersions => 18,
            Self::InitProducerId => 22,
            Self::FindCoordinator => 10,
            Self::JoinGroup => 11,
            Self::SyncGroup => 14,
            Self::Heartbeat => 12,
            Self::LeaveGroup => 13,
            Self::OffsetCommit => 8,
            Self::OffsetFetch => 9,
        }
    }

    /// The version range this gateway serves, inclusive.
    ///
    /// Deliberately narrow, and the floors are chosen rather than inherited.
    /// Produce starts at v3 because that is where the v2 record batch — the
    /// only one carrying a producer id, epoch and sequence — became the wire
    /// format, and idempotence cannot be bridged without those. Fetch starts
    /// at v4 for the same reason plus the isolation level, which a client uses
    /// to ask for committed-only reads and which this gateway must see in
    /// order to refuse read-committed rather than silently serve everything.
    pub fn version_range(self) -> (i16, i16) {
        match self {
            // Through v8: the rule is "up to the last non-flexible version",
            // and v8 is non-flexible. It is not free — v8's RESPONSE adds a
            // per-partition record-error list and an error message the writer
            // must carry — but excluding it while claiming the rule would be
            // an undocumented exception, and an undocumented exception is how
            // a range drifts from its reason (review).
            Self::Produce => (3, 8),
            Self::Fetch => (4, 11),
            Self::ListOffsets => (1, 5),
            Self::Metadata => (1, 8),
            // v0 and v1 are the idempotent-only shapes: a transactional id
            // (refused here) and a transaction timeout. v2 is flexible, and
            // v3+ carry a producer id and epoch to bump — the transactional
            // re-init this gateway does not have.
            Self::InitProducerId => (0, 1),
            // The group protocol, up to the last classic version of each
            // (#457 slice 2). OffsetCommit starts at v5: v0 and v1 carry a
            // per-partition timestamp, v2 to v4 a per-commit retention time
            // (retired at v5), and a version whose contract the store cannot
            // keep is not offered (review). OffsetFetch starts at v1: v0 read
            // ZooKeeper-era offsets.
            Self::FindCoordinator => (0, 2),
            Self::JoinGroup => (0, 5),
            Self::SyncGroup => (0, 3),
            Self::Heartbeat => (0, 3),
            Self::LeaveGroup => (0, 2),
            Self::OffsetCommit => (5, 7),
            Self::OffsetFetch => (1, 5),
            // v0 always answers, by protocol rule: a client that knows nothing
            // about this broker sends ApiVersions v0 to find out what it can
            // speak, so refusing v0 would refuse the conversation that
            // establishes what to refuse.
            Self::ApiVersions => (0, 2),
        }
    }

    pub fn supports(self, version: i16) -> bool {
        let (min, max) = self.version_range();
        version >= min && version <= max
    }

    /// Whether requests and responses at this version use the flexible
    /// (tagged-field, compact-encoding) form.
    ///
    /// PHASE 1 SERVES NO FLEXIBLE VERSION, and that is a decision rather than
    /// an accident: every range above stops one version below its flexible
    /// boundary, because admitting a flexible request means writing a flexible
    /// RESPONSE, and a response written in the wrong encoding is not a clean
    /// failure — the client parses it into nonsense. A test below pins the
    /// invariant, so widening a range without teaching the writer compact
    /// encoding fails there rather than in somebody's consumer.
    ///
    /// The one exception the protocol carries: an ApiVersions RESPONSE header
    /// is always v0, never flexible, even when the request was — because the
    /// client must be able to parse the reply that tells it what the broker
    /// supports without already knowing. Handled at the response writer, not
    /// here, but recorded here because this is where a reader will look.
    pub fn is_flexible(self, version: i16) -> bool {
        match self {
            Self::Produce => version >= 9,
            Self::Fetch => version >= 12,
            Self::ListOffsets => version >= 6,
            Self::Metadata => version >= 9,
            Self::ApiVersions => version >= 3,
            Self::InitProducerId => version >= 2,
            Self::FindCoordinator => version >= 3,
            Self::JoinGroup => version >= 6,
            Self::SyncGroup => version >= 4,
            Self::Heartbeat => version >= 4,
            Self::LeaveGroup => version >= 4,
            Self::OffsetCommit => version >= 8,
            Self::OffsetFetch => version >= 6,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Produce => "Produce",
            Self::Fetch => "Fetch",
            Self::ListOffsets => "ListOffsets",
            Self::Metadata => "Metadata",
            Self::ApiVersions => "ApiVersions",
            Self::InitProducerId => "InitProducerId",
            Self::FindCoordinator => "FindCoordinator",
            Self::JoinGroup => "JoinGroup",
            Self::SyncGroup => "SyncGroup",
            Self::Heartbeat => "Heartbeat",
            Self::LeaveGroup => "LeaveGroup",
            Self::OffsetCommit => "OffsetCommit",
            Self::OffsetFetch => "OffsetFetch",
        }
    }
}

/// The Kafka error codes this gateway can return.
///
/// Only codes with a truthful meaning here are listed. Inventing a plausible
/// one is worse than refusing: a client's retry policy is keyed on these, so a
/// wrong code does not merely misinform, it makes the client do the wrong
/// thing — retry forever, or give up on a condition that would have cleared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    MessageTooLarge = 10,
    /// The gateway understood the request and will not serve it — used for the
    /// capabilities phase 1 deliberately does not have, never for a transient
    /// condition a client should retry.
    UnsupportedVersion = 35,
    InvalidRecord = 87,
    UnsupportedCompressionType = 76,
    /// Transactions and anything else out of scope.
    UnsupportedForMessageFormat = 43,
    /// An idempotent batch whose sequence is not the next one (#457): a gap,
    /// or a new producer not starting at zero. A client treats it as fatal
    /// for that producer, which is right — its own bookkeeping disagrees
    /// with the log's.
    OutOfOrderSequenceNumber = 45,
    /// A retry the log has already persisted but can no longer verify — its
    /// sequence fell below the dedup window. A client treats it as delivered.
    DuplicateSequenceNumber = 46,
    /// A batch naming a producer id with an epoch below zero.
    InvalidProducerEpoch = 47,
    /// A commit's metadata over the cap.
    OffsetMetadataTooLarge = 12,
    /// The gateway cannot hold another group right now; a client retries.
    CoordinatorNotAvailable = 15,
    /// A member speaking for a generation the group has left behind.
    IllegalGeneration = 22,
    /// Members that cannot agree on a protocol, or an assignment that names
    /// what this gateway does not serve.
    InconsistentGroupProtocol = 23,
    /// An empty group id.
    InvalidGroupId = 24,
    /// A member the group does not know.
    UnknownMemberId = 25,
    /// A session timeout outside the bounds the coordinator serves.
    InvalidSessionTimeout = 26,
    /// The group is rebalancing: rejoin.
    RebalanceInProgress = 27,
    /// KIP-394: a first join must come back with the id minted for it.
    MemberIdRequired = 79,
    /// One member over the group's cap.
    GroupMaxSizeReached = 84,
}

impl ErrorCode {
    pub fn as_i16(self) -> i16 {
        self as i16
    }
}

/// A decoded request header, plus the body cursor positioned after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

/// What the header decoder concluded. A malformed header and an unsupported
/// one are different outcomes: the first cannot be answered at all (there may
/// be no correlation id to answer with), the second must be answered with a
/// proper error response carrying the correlation id back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderVerdict {
    /// Understood, and this gateway serves it.
    Serve(RequestHeader),
    /// Understood enough to reply, but not served.
    Refuse {
        header: RequestHeader,
        code: ErrorCode,
        reason: String,
    },
}

impl RequestHeader {
    /// Decode a request header, leaving `d` at the first byte of the body.
    ///
    /// The header's own version is implied by the api key and version, not
    /// carried on the wire — v2 (flexible) for flexible requests, v1
    /// otherwise. ApiVersions is the exception once more: its REQUEST header
    /// follows the normal rule, and only its response header is pinned.
    pub fn decode(d: &mut Decoder<'_>) -> Result<HeaderVerdict, WireError> {
        let api_key = d.i16("header.apiKey")?;
        let api_version = d.i16("header.apiVersion")?;
        let correlation_id = d.i32("header.correlationId")?;

        // THE CLIENT ID IS DECODED IN THE FORM THIS (KEY, VERSION) USES, and
        // that has to happen before the version gate rather than after it. A
        // client speaking a flexible version this gateway does not serve sends
        // a COMPACT client id; reading it as a classic nullable string yields a
        // malformed-header error, so the client is told its frame is broken
        // when the truthful answer is UNSUPPORTED_VERSION — the one answer that
        // would have made it downgrade and succeed (review).
        //
        // An unknown api key is refused without reading the client id at all:
        // its encoding is unknowable, and the reply needs only the correlation
        // id, which is already in hand.
        let Some(key) = ApiKey::from_i16(api_key) else {
            return Ok(HeaderVerdict::Refuse {
                header: RequestHeader {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id: None,
                },
                code: ErrorCode::UnsupportedVersion,
                reason: format!(
                    "api key {api_key} is not served by this gateway (Produce, Fetch, \
                     ListOffsets, Metadata, ApiVersions, InitProducerId, and the group protocol: \
                     FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, \
                     OffsetFetch)"
                ),
            });
        };

        // CLASSIC NULLABLE STRING, even in header v2 (review).
        //
        // Request header v2 adds a tagged-field section but deliberately keeps
        // `client_id` in the pre-flexible encoding — the header predates
        // flexible versions and Kafka never migrated this field. API-body
        // flexibility says nothing about it. Reading it as a compact string
        // misreads the length by one and then treats the remainder as payload:
        // a short client id whose first byte is 0x00 decodes as null, and a
        // longer one bleeds into the body.
        let client_id = d.nullable_string("header.clientId")?.map(str::to_owned);
        let header = RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };

        if !key.supports(api_version) {
            let (min, max) = key.version_range();
            return Ok(HeaderVerdict::Refuse {
                header,
                code: ErrorCode::UnsupportedVersion,
                reason: format!(
                    "{} v{api_version} is outside the served range v{min}..=v{max}",
                    key.name()
                ),
            });
        }

        // The tagged-field section belongs to the header, so it is consumed
        // here — leaving it for the body decoder would put every field one
        // varint out of place, which decodes without error and yields
        // nonsense.
        if key.is_flexible(api_version) {
            d.skip_tagged_fields("header.taggedFields")?;
        }

        Ok(HeaderVerdict::Serve(header))
    }
}

/// Write a response header.
///
/// `flexible` must be the RESPONSE's own flexibility, which is not always the
/// request's: an ApiVersions response header is v0 even at flexible request
/// versions, because a client parses it before it knows what the broker
/// speaks. Getting this wrong shifts every following byte by one varint.
pub fn write_response_header(out: &mut Encoder, correlation_id: i32, flexible: bool) {
    out.i32(correlation_id);
    if flexible {
        out.empty_tagged_fields();
    }
}

/// Frame a response body: a four-byte length followed by the payload.
pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Encoder::with_capacity(body.len() + 4);
    out.i32(body.len() as i32);
    out.raw(body);
    out.into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes(api_key: i16, api_version: i16, client: Option<&str>) -> Encoder {
        let mut e = Encoder::new();
        e.i16(api_key);
        e.i16(api_version);
        e.i32(7);
        e.nullable_string(client);
        e
    }

    #[test]
    fn a_served_request_decodes_with_its_identity() {
        let bytes = header_bytes(3, 4, Some("rdkafka")).into_vec();
        let mut d = Decoder::new(&bytes);
        match RequestHeader::decode(&mut d).unwrap() {
            HeaderVerdict::Serve(h) => {
                assert_eq!(h.api_key, 3);
                assert_eq!(h.api_version, 4);
                assert_eq!(h.correlation_id, 7);
                assert_eq!(h.client_id.as_deref(), Some("rdkafka"));
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    /// An unknown api key must still yield a header, because the client is
    /// owed a reply carrying its correlation id. Dropping the connection
    /// instead makes a client that asked for one unsupported thing look like a
    /// broker that is down.
    #[test]
    fn an_unserved_api_is_refused_with_a_correlation_id_to_answer_on() {
        let bytes = header_bytes(19, 0, None).into_vec(); // CreateTopics
        let mut d = Decoder::new(&bytes);
        match RequestHeader::decode(&mut d).unwrap() {
            HeaderVerdict::Refuse {
                header,
                code,
                reason,
            } => {
                assert_eq!(header.correlation_id, 7);
                assert_eq!(code, ErrorCode::UnsupportedVersion);
                assert!(reason.contains("19"), "{reason}");
            }
            other => panic!("expected Refuse, got {other:?}"),
        }
    }

    /// The version gate exists because decoding a flexible body with a classic
    /// schema SUCCEEDS and produces nonsense. Refusing before the body is read
    /// is the whole point.
    #[test]
    fn a_version_outside_the_served_range_is_refused_before_the_body() {
        // Non-flexible versions only; the flexible ones have their own test
        // below, because their header is encoded differently.
        for (key, version) in [(0_i16, 2_i16), (1, 3), (2, 0), (3, 0)] {
            let bytes = header_bytes(key, version, None).into_vec();
            let mut d = Decoder::new(&bytes);
            assert!(
                matches!(
                    RequestHeader::decode(&mut d).unwrap(),
                    HeaderVerdict::Refuse {
                        code: ErrorCode::UnsupportedVersion,
                        ..
                    }
                ),
                "api {key} v{version} must be refused"
            );
        }
    }

    /// A client speaking a flexible version this gateway does not serve sends
    /// a COMPACT client id. Reading it as a classic nullable string yields a
    /// malformed-header error — telling the client its frame is broken when
    /// the truthful answer is UNSUPPORTED_VERSION, which is the one answer
    /// that would have made it downgrade and succeed.
    #[test]
    fn an_unserved_flexible_version_is_refused_rather_than_called_malformed() {
        for (key, version) in [(0_i16, 9_i16), (1, 12), (2, 6), (3, 9), (18, 3)] {
            let mut e = Encoder::new();
            e.i16(key);
            e.i16(version);
            e.i32(7);
            // CLASSIC nullable string, not compact — request header v2 keeps
            // `client_id` in the pre-flexible encoding
            // (`"flexibleVersions": "none"` in Kafka's own
            // RequestHeaderData.json), and only the tagged-field section that
            // follows is flexible. This test used to encode it compactly,
            // which meant it asserted the decoder's matching mistake rather
            // than the wire (review).
            e.nullable_string(Some("modern-client"));
            e.empty_tagged_fields();
            let bytes = e.into_vec();

            let mut d = Decoder::new(&bytes);
            match RequestHeader::decode(&mut d).unwrap() {
                HeaderVerdict::Refuse { header, code, .. } => {
                    assert_eq!(code, ErrorCode::UnsupportedVersion, "api {key} v{version}");
                    assert_eq!(header.correlation_id, 7);
                    assert_eq!(
                        header.client_id.as_deref(),
                        Some("modern-client"),
                        "api {key} v{version}: the client id must decode"
                    );
                }
                other => panic!("api {key} v{version}: expected Refuse, got {other:?}"),
            }
        }
    }

    /// An unknown api key is refused WITHOUT reading the client id, whose
    /// encoding is unknowable — the reply needs only the correlation id.
    #[test]
    fn an_unknown_api_key_is_refused_without_guessing_its_encoding() {
        let mut e = Encoder::new();
        e.i16(19); // CreateTopics
        e.i16(4);
        e.i32(11);
        e.raw(&[0xff, 0xff, 0xff, 0xff]); // not a valid client id in either form
        let bytes = e.into_vec();

        let mut d = Decoder::new(&bytes);
        match RequestHeader::decode(&mut d).unwrap() {
            HeaderVerdict::Refuse { header, code, .. } => {
                assert_eq!(code, ErrorCode::UnsupportedVersion);
                assert_eq!(header.correlation_id, 11);
                assert_eq!(header.client_id, None);
            }
            other => panic!("expected Refuse, got {other:?}"),
        }
    }

    /// ApiVersions v0 must always be answerable: it is how a client discovers
    /// what the broker speaks, so refusing it refuses the conversation that
    /// establishes what to refuse.
    #[test]
    fn api_versions_v0_is_always_served() {
        assert!(ApiKey::ApiVersions.supports(0));
        let bytes = header_bytes(18, 0, None).into_vec();
        let mut d = Decoder::new(&bytes);
        assert!(matches!(
            RequestHeader::decode(&mut d).unwrap(),
            HeaderVerdict::Serve(_)
        ));
    }

    /// THE INVARIANT PHASE 1 RESTS ON: nothing served is flexible.
    ///
    /// Every range stops one version below its API's flexible boundary,
    /// because serving a flexible request means writing a flexible response,
    /// and a response in the wrong encoding is not a clean failure — the
    /// client parses it into nonsense. Widening a range without teaching the
    /// writer compact encoding must fail HERE, not in somebody's consumer.
    #[test]
    fn no_served_version_uses_the_flexible_encoding() {
        for key in [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::ApiVersions,
            ApiKey::InitProducerId,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::SyncGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
        ] {
            let (min, max) = key.version_range();
            for version in min..=max {
                assert!(
                    !key.is_flexible(version),
                    "{} v{version} is served but flexible: either the writer learned compact \
                     encoding and this test should say so, or the range is wrong",
                    key.name()
                );
            }
        }
    }

    /// The flexible boundary itself is still pinned per API, so the flag is
    /// already correct on the day a range is widened onto it.
    #[test]
    fn the_flexible_boundary_is_the_one_the_protocol_defines() {
        assert!(!ApiKey::Produce.is_flexible(8) && ApiKey::Produce.is_flexible(9));
        assert!(!ApiKey::Fetch.is_flexible(11) && ApiKey::Fetch.is_flexible(12));
        assert!(!ApiKey::ListOffsets.is_flexible(5) && ApiKey::ListOffsets.is_flexible(6));
        assert!(!ApiKey::Metadata.is_flexible(8) && ApiKey::Metadata.is_flexible(9));
        assert!(!ApiKey::ApiVersions.is_flexible(2) && ApiKey::ApiVersions.is_flexible(3));
    }

    /// A flexible header carries a tagged-field section belonging to the
    /// HEADER; leaving it for the body decoder puts every field one varint out
    /// of place, and being varints, that decodes without error. Exercised
    /// through the decoder directly, since no served version reaches it yet.
    #[test]
    fn a_flexible_header_would_consume_its_own_tagged_fields() {
        let mut e = Encoder::new();
        e.empty_tagged_fields();
        e.i16(0x1234); // the body's first field
        let bytes = e.into_vec();

        let mut d = Decoder::new(&bytes);
        d.skip_tagged_fields("header.taggedFields").unwrap();
        assert_eq!(
            d.i16("body").unwrap(),
            0x1234,
            "the body must start where the header ended"
        );
    }

    #[test]
    fn a_response_frame_is_length_prefixed() {
        let framed = frame(b"abcd");
        assert_eq!(&framed[..4], &4_i32.to_be_bytes());
        assert_eq!(&framed[4..], b"abcd");
    }

    #[test]
    fn every_served_api_round_trips_its_key() {
        for key in [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::ApiVersions,
            ApiKey::InitProducerId,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::SyncGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
        ] {
            assert_eq!(ApiKey::from_i16(key.as_i16()), Some(key));
        }
    }
}
