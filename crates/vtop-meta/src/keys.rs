//! Typed metadata keys with a canonical binary encoding.
//!
//! Every record in the metadata state machine lives under a [`MetaKey`]. The
//! encoded form — shard prefix, category byte, then fixed-width ids — is the
//! *only* ordering that matters: the state machine's map, snapshots, and any
//! future range scans all sort by these bytes, so [`Ord`] on `MetaKey` is
//! defined as the order of the encoded bytes and nothing else.
//!
//! The `/meta/0/...` display form exists for admin tooling and debug output
//! only; it is never parsed and never stored.

use crate::wire::{put_u16, put_u64, put_u8, put_uuid, CodecError, Reader};
use std::cmp::Ordering;
use std::fmt;
use uuid::Uuid;

/// The single metadata shard this slice supports. The shard id is encoded
/// into every key so a future sharded control plane changes bytes, not shape.
pub const META_SHARD_ID: u16 = 0;

/// Topic names reuse the broker's 249-byte topic bound (`MAX_TOPIC_BYTES` in
/// vtop-protocol): 1..=249 bytes of UTF-8, validated both at the codec and in
/// `apply`.
pub const MAX_TOPIC_NAME_BYTES: usize = 249;

const CATEGORY_CLUSTER_CONFIG: u8 = 1;
const CATEGORY_NODE: u8 = 2;
const CATEGORY_TOPIC_BY_NAME: u8 = 3;
const CATEGORY_TOPIC: u8 = 4;
const CATEGORY_RANGE: u8 = 5;
const CATEGORY_SEGMENT: u8 = 6;
const CATEGORY_KEY: u8 = 7;
const CATEGORY_REQUEST: u8 = 8;
const CATEGORY_GROUP: u8 = 9;
const CATEGORY_GROUP_BY_NAME: u8 = 10;
const CATEGORY_GROUP_MEMBER: u8 = 11;
const CATEGORY_GROUP_CURSOR: u8 = 12;
const CATEGORY_SEGMENT_PLACEMENT: u8 = 13;
const CATEGORY_SEGMENT_REPLACEMENT_PROOF: u8 = 14;
const CATEGORY_SEGMENT_REBALANCE_INTENT: u8 = 15;
const CATEGORY_SEGMENT_TIER_COPY: u8 = 16;
const CATEGORY_TOPIC_RETENTION_POLICY: u8 = 17;
const CATEGORY_RANGE_TRANSITION: u8 = 18;

/// Consumer-group names reuse the topic-name bound so group identity stays
/// allocation-bounded on the wire and in snapshots.
pub const MAX_GROUP_NAME_BYTES: usize = MAX_TOPIC_NAME_BYTES;

/// Raft-level node identifier, distinct from the storage-level node UUID so
/// consensus membership (small dense ids) and node records (stable UUIDs)
/// cannot be confused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetaNodeId(pub u64);

impl fmt::Display for MetaNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A typed key in the metadata keyspace.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetaKey {
    /// Reserved for the durable cluster configuration record (later PRs).
    ClusterConfig,
    Node {
        node_uuid: Uuid,
    },
    TopicByName {
        name: String,
    },
    Topic {
        topic_uuid: Uuid,
    },
    Range {
        topic_uuid: Uuid,
        range_uuid: Uuid,
    },
    Segment {
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
    },
    Key {
        key_uuid: Uuid,
    },
    /// Reserved for request bookkeeping records (the dedup table itself is
    /// carried in the snapshot, not the keyspace, in this slice).
    Request {
        request_id: Uuid,
    },
    Group {
        group_uuid: Uuid,
    },
    GroupByName {
        name: String,
    },
    GroupMember {
        group_uuid: Uuid,
        member_uuid: Uuid,
    },
    GroupCursor {
        group_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    },
    /// Ordered replica-node set for a verified segment, committed after
    /// deterministic placement validation.
    SegmentPlacement {
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
    },
    /// Authenticated replacement evidence gating replica retirement.
    SegmentReplacementProof {
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
    },
    /// The single in-flight rebalance move for a segment; its presence blocks
    /// a second proposal until the move completes or is cancelled.
    SegmentRebalanceIntent {
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
    },
    /// Verified cold-tier copy evidence for a sealed segment (one per segment
    /// in this slice); its presence plus a `Verified` segment is the derived
    /// TIER_VERIFIED condition that authorizes retention planning.
    SegmentTierCopy {
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
    },
    /// Per-topic retention policy record. An absent record is the fail-closed
    /// default: no unarchived deletion.
    TopicRetentionPolicy {
        topic_uuid: Uuid,
    },
    /// One epoch transition of a range (#240 item 5), keyed by the epoch it
    /// minted. The epoch is the last component and big-endian, so the
    /// records of a range scan in transition order — the chain is a key
    /// range, not a search.
    RangeTransition {
        topic_uuid: Uuid,
        range_uuid: Uuid,
        fencing_epoch: u64,
    },
}

/// Validate a topic name against the shared 249-byte semantics.
pub fn validate_topic_name(name: &str) -> Result<(), CodecError> {
    if name.is_empty() || name.len() > MAX_TOPIC_NAME_BYTES {
        return Err(CodecError::BoundExceeded {
            what: "topic name",
            actual: name.len(),
            maximum: MAX_TOPIC_NAME_BYTES,
        });
    }
    Ok(())
}

/// Validate a consumer-group name against the shared 249-byte semantics.
pub fn validate_group_name(name: &str) -> Result<(), CodecError> {
    if name.is_empty() || name.len() > MAX_GROUP_NAME_BYTES {
        return Err(CodecError::BoundExceeded {
            what: "group name",
            actual: name.len(),
            maximum: MAX_GROUP_NAME_BYTES,
        });
    }
    Ok(())
}

impl MetaKey {
    fn category(&self) -> u8 {
        match self {
            MetaKey::ClusterConfig => CATEGORY_CLUSTER_CONFIG,
            MetaKey::Node { .. } => CATEGORY_NODE,
            MetaKey::TopicByName { .. } => CATEGORY_TOPIC_BY_NAME,
            MetaKey::Topic { .. } => CATEGORY_TOPIC,
            MetaKey::Range { .. } => CATEGORY_RANGE,
            MetaKey::Segment { .. } => CATEGORY_SEGMENT,
            MetaKey::Key { .. } => CATEGORY_KEY,
            MetaKey::Request { .. } => CATEGORY_REQUEST,
            MetaKey::Group { .. } => CATEGORY_GROUP,
            MetaKey::GroupByName { .. } => CATEGORY_GROUP_BY_NAME,
            MetaKey::GroupMember { .. } => CATEGORY_GROUP_MEMBER,
            MetaKey::GroupCursor { .. } => CATEGORY_GROUP_CURSOR,
            MetaKey::SegmentPlacement { .. } => CATEGORY_SEGMENT_PLACEMENT,
            MetaKey::SegmentReplacementProof { .. } => CATEGORY_SEGMENT_REPLACEMENT_PROOF,
            MetaKey::SegmentRebalanceIntent { .. } => CATEGORY_SEGMENT_REBALANCE_INTENT,
            MetaKey::SegmentTierCopy { .. } => CATEGORY_SEGMENT_TIER_COPY,
            MetaKey::TopicRetentionPolicy { .. } => CATEGORY_TOPIC_RETENTION_POLICY,
            MetaKey::RangeTransition { .. } => CATEGORY_RANGE_TRANSITION,
        }
    }

    /// Canonical binary encoding: `shard:u16` (always 0), `category:u8`,
    /// then the fixed-width ids — or, for `TopicByName`, the raw name bytes,
    /// whose length is implied by the key length.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(3 + 48);
        put_u16(&mut out, META_SHARD_ID);
        put_u8(&mut out, self.category());
        match self {
            MetaKey::ClusterConfig => {}
            MetaKey::Node { node_uuid } => put_uuid(&mut out, *node_uuid),
            MetaKey::TopicByName { name } => out.extend_from_slice(name.as_bytes()),
            MetaKey::Topic { topic_uuid } => put_uuid(&mut out, *topic_uuid),
            MetaKey::Range {
                topic_uuid,
                range_uuid,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
            }
            MetaKey::Segment {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
            }
            MetaKey::Key { key_uuid } => put_uuid(&mut out, *key_uuid),
            MetaKey::Request { request_id } => put_uuid(&mut out, *request_id),
            MetaKey::Group { group_uuid } => put_uuid(&mut out, *group_uuid),
            MetaKey::GroupByName { name } => out.extend_from_slice(name.as_bytes()),
            MetaKey::GroupMember {
                group_uuid,
                member_uuid,
            } => {
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
            }
            MetaKey::GroupCursor {
                group_uuid,
                topic_uuid,
                range_uuid,
            } => {
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
            }
            MetaKey::SegmentPlacement {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
            }
            MetaKey::SegmentReplacementProof {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
            }
            MetaKey::SegmentRebalanceIntent {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
            }
            MetaKey::SegmentTierCopy {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
            }
            MetaKey::TopicRetentionPolicy { topic_uuid } => put_uuid(&mut out, *topic_uuid),
            MetaKey::RangeTransition {
                topic_uuid,
                range_uuid,
                fencing_epoch,
            } => {
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_u64(&mut out, *fencing_epoch);
            }
        }
        out
    }

    /// Decode a canonical key, rejecting unknown shards, unknown categories,
    /// trailing bytes, and out-of-bound or non-UTF-8 topic names.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let shard = reader.u16("meta key shard")?;
        if shard != META_SHARD_ID {
            return Err(CodecError::InvalidValue {
                what: "meta key shard",
                reason: "only shard 0 exists in this format version",
            });
        }
        let category = reader.u8("meta key category")?;
        let key = match category {
            CATEGORY_CLUSTER_CONFIG => MetaKey::ClusterConfig,
            CATEGORY_NODE => MetaKey::Node {
                node_uuid: reader.uuid("node uuid")?,
            },
            CATEGORY_TOPIC_BY_NAME => {
                let raw = reader.remaining();
                if raw == 0 || raw > MAX_TOPIC_NAME_BYTES {
                    return Err(CodecError::BoundExceeded {
                        what: "topic name",
                        actual: raw,
                        maximum: MAX_TOPIC_NAME_BYTES,
                    });
                }
                let name = String::from_utf8(reader.take(raw, "topic name")?.to_vec())
                    .map_err(|_| CodecError::InvalidUtf8("topic name"))?;
                MetaKey::TopicByName { name }
            }
            CATEGORY_TOPIC => MetaKey::Topic {
                topic_uuid: reader.uuid("topic uuid")?,
            },
            CATEGORY_RANGE => MetaKey::Range {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
            },
            CATEGORY_SEGMENT => MetaKey::Segment {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
            },
            CATEGORY_KEY => MetaKey::Key {
                key_uuid: reader.uuid("key uuid")?,
            },
            CATEGORY_REQUEST => MetaKey::Request {
                request_id: reader.uuid("request id")?,
            },
            CATEGORY_GROUP => MetaKey::Group {
                group_uuid: reader.uuid("group uuid")?,
            },
            CATEGORY_GROUP_BY_NAME => {
                let raw = reader.remaining();
                if raw == 0 || raw > MAX_GROUP_NAME_BYTES {
                    return Err(CodecError::BoundExceeded {
                        what: "group name",
                        actual: raw,
                        maximum: MAX_GROUP_NAME_BYTES,
                    });
                }
                let name = String::from_utf8(reader.take(raw, "group name")?.to_vec())
                    .map_err(|_| CodecError::InvalidUtf8("group name"))?;
                MetaKey::GroupByName { name }
            }
            CATEGORY_GROUP_MEMBER => MetaKey::GroupMember {
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
            },
            CATEGORY_GROUP_CURSOR => MetaKey::GroupCursor {
                group_uuid: reader.uuid("group uuid")?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
            },
            CATEGORY_SEGMENT_PLACEMENT => MetaKey::SegmentPlacement {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
            },
            CATEGORY_SEGMENT_REPLACEMENT_PROOF => MetaKey::SegmentReplacementProof {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
            },
            CATEGORY_SEGMENT_REBALANCE_INTENT => MetaKey::SegmentRebalanceIntent {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
            },
            CATEGORY_SEGMENT_TIER_COPY => MetaKey::SegmentTierCopy {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
            },
            CATEGORY_RANGE_TRANSITION => MetaKey::RangeTransition {
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                fencing_epoch: reader.u64("fencing epoch")?,
            },
            CATEGORY_TOPIC_RETENTION_POLICY => MetaKey::TopicRetentionPolicy {
                topic_uuid: reader.uuid("topic uuid")?,
            },
            other => {
                return Err(CodecError::UnknownTag {
                    what: "meta key category",
                    tag: u32::from(other),
                });
            }
        };
        reader.finish()?;
        Ok(key)
    }
}

impl Ord for MetaKey {
    /// Keys order by their encoded bytes — the one ordering snapshots, the
    /// state map, and future range scans all agree on.
    fn cmp(&self, other: &Self) -> Ordering {
        self.encode().cmp(&other.encode())
    }
}

impl PartialOrd for MetaKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for MetaKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaKey::ClusterConfig => write!(formatter, "/meta/0/cluster-config"),
            MetaKey::Node { node_uuid } => write!(formatter, "/meta/0/node/{node_uuid}"),
            MetaKey::TopicByName { name } => write!(formatter, "/meta/0/topic-by-name/{name}"),
            MetaKey::Topic { topic_uuid } => write!(formatter, "/meta/0/topic/{topic_uuid}"),
            MetaKey::Range {
                topic_uuid,
                range_uuid,
            } => write!(formatter, "/meta/0/range/{topic_uuid}/{range_uuid}"),
            MetaKey::Segment {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => write!(
                formatter,
                "/meta/0/segment/{topic_uuid}/{range_uuid}/{segment_uuid}"
            ),
            MetaKey::Key { key_uuid } => write!(formatter, "/meta/0/key/{key_uuid}"),
            MetaKey::Request { request_id } => write!(formatter, "/meta/0/request/{request_id}"),
            MetaKey::Group { group_uuid } => write!(formatter, "/meta/0/group/{group_uuid}"),
            MetaKey::GroupByName { name } => write!(formatter, "/meta/0/group-by-name/{name}"),
            MetaKey::GroupMember {
                group_uuid,
                member_uuid,
            } => write!(formatter, "/meta/0/group/{group_uuid}/member/{member_uuid}"),
            MetaKey::GroupCursor {
                group_uuid,
                topic_uuid,
                range_uuid,
            } => write!(
                formatter,
                "/meta/0/group/{group_uuid}/cursor/{topic_uuid}/{range_uuid}"
            ),
            MetaKey::SegmentPlacement {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => write!(
                formatter,
                "/meta/0/segment-placement/{topic_uuid}/{range_uuid}/{segment_uuid}"
            ),
            MetaKey::SegmentReplacementProof {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => write!(
                formatter,
                "/meta/0/segment-replacement-proof/{topic_uuid}/{range_uuid}/{segment_uuid}"
            ),
            MetaKey::SegmentRebalanceIntent {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => write!(
                formatter,
                "/meta/0/segment-rebalance-intent/{topic_uuid}/{range_uuid}/{segment_uuid}"
            ),
            MetaKey::SegmentTierCopy {
                topic_uuid,
                range_uuid,
                segment_uuid,
            } => write!(
                formatter,
                "/meta/0/segment-tier-copy/{topic_uuid}/{range_uuid}/{segment_uuid}"
            ),
            MetaKey::TopicRetentionPolicy { topic_uuid } => {
                write!(formatter, "/meta/0/topic-retention-policy/{topic_uuid}")
            }
            MetaKey::RangeTransition {
                topic_uuid,
                range_uuid,
                fencing_epoch,
            } => write!(
                formatter,
                "/meta/0/range-transition/{topic_uuid}/{range_uuid}/{fencing_epoch}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_key() -> Vec<MetaKey> {
        vec![
            MetaKey::ClusterConfig,
            MetaKey::Node {
                node_uuid: Uuid::from_u128(1),
            },
            MetaKey::TopicByName {
                name: "events.v1".to_owned(),
            },
            MetaKey::Topic {
                topic_uuid: Uuid::from_u128(2),
            },
            MetaKey::Range {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
            },
            MetaKey::Segment {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                segment_uuid: Uuid::from_u128(4),
            },
            MetaKey::Key {
                key_uuid: Uuid::from_u128(5),
            },
            MetaKey::Request {
                request_id: Uuid::from_u128(6),
            },
            MetaKey::Group {
                group_uuid: Uuid::from_u128(7),
            },
            MetaKey::GroupByName {
                name: "audit.consumers".to_owned(),
            },
            MetaKey::GroupMember {
                group_uuid: Uuid::from_u128(7),
                member_uuid: Uuid::from_u128(8),
            },
            MetaKey::GroupCursor {
                group_uuid: Uuid::from_u128(7),
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
            },
            MetaKey::SegmentPlacement {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                segment_uuid: Uuid::from_u128(4),
            },
            MetaKey::SegmentReplacementProof {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                segment_uuid: Uuid::from_u128(4),
            },
            MetaKey::SegmentRebalanceIntent {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                segment_uuid: Uuid::from_u128(4),
            },
            MetaKey::SegmentTierCopy {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                segment_uuid: Uuid::from_u128(4),
            },
            MetaKey::TopicRetentionPolicy {
                topic_uuid: Uuid::from_u128(2),
            },
            MetaKey::RangeTransition {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                fencing_epoch: 7,
            },
        ]
    }

    /// A range's transitions sort by epoch under one prefix (#240 item 5):
    /// that is what lets the chain be read as a key range, in order.
    #[test]
    fn range_transitions_sort_by_epoch_under_their_range() {
        let at = |fencing_epoch: u64| MetaKey::RangeTransition {
            topic_uuid: Uuid::from_u128(2),
            range_uuid: Uuid::from_u128(3),
            fencing_epoch,
        };
        assert!(at(1) < at(2));
        assert!(
            at(255) < at(256),
            "big-endian: the width of the number never reorders it"
        );
        assert!(at(u64::MAX - 1) < at(u64::MAX));
        let other_range = MetaKey::RangeTransition {
            topic_uuid: Uuid::from_u128(2),
            range_uuid: Uuid::from_u128(4),
            fencing_epoch: 0,
        };
        assert!(
            at(u64::MAX) < other_range,
            "every epoch of one range precedes the next range"
        );
    }

    #[test]
    fn every_key_round_trips_through_the_canonical_encoding() {
        for key in every_key() {
            let encoded = key.encode();
            assert_eq!(MetaKey::decode(&encoded).unwrap(), key, "{key}");
            assert_eq!(&encoded[..2], &[0, 0], "shard prefix must be 0 ({key})");
        }
    }

    #[test]
    fn key_ordering_is_exactly_encoded_byte_ordering() {
        let mut keys = every_key();
        keys.sort();
        let mut encodings: Vec<Vec<u8>> = every_key().iter().map(MetaKey::encode).collect();
        encodings.sort();
        let sorted_encodings: Vec<Vec<u8>> = keys.iter().map(MetaKey::encode).collect();
        assert_eq!(sorted_encodings, encodings);
    }

    #[test]
    fn decode_rejects_trailing_bytes_unknown_categories_and_foreign_shards() {
        let mut trailing = MetaKey::Node {
            node_uuid: Uuid::from_u128(1),
        }
        .encode();
        trailing.push(0);
        assert_eq!(MetaKey::decode(&trailing), Err(CodecError::Trailing(1)));

        let mut truncated = MetaKey::Topic {
            topic_uuid: Uuid::from_u128(2),
        }
        .encode();
        truncated.pop();
        assert!(matches!(
            MetaKey::decode(&truncated),
            Err(CodecError::Truncated(_))
        ));

        assert_eq!(
            MetaKey::decode(&[0, 0, 99]),
            Err(CodecError::UnknownTag {
                what: "meta key category",
                tag: 99,
            })
        );

        assert!(matches!(
            MetaKey::decode(&[0, 1, CATEGORY_CLUSTER_CONFIG]),
            Err(CodecError::InvalidValue { .. })
        ));
    }

    #[test]
    fn topic_name_keys_enforce_the_249_byte_bound_and_utf8() {
        let long = "x".repeat(MAX_TOPIC_NAME_BYTES);
        let key = MetaKey::TopicByName { name: long.clone() };
        assert_eq!(MetaKey::decode(&key.encode()).unwrap(), key);

        let mut over = MetaKey::ClusterConfig.encode();
        over[2] = CATEGORY_TOPIC_BY_NAME;
        over.extend_from_slice("y".repeat(MAX_TOPIC_NAME_BYTES + 1).as_bytes());
        assert!(matches!(
            MetaKey::decode(&over),
            Err(CodecError::BoundExceeded { .. })
        ));

        let mut empty = MetaKey::ClusterConfig.encode();
        empty[2] = CATEGORY_TOPIC_BY_NAME;
        assert!(matches!(
            MetaKey::decode(&empty),
            Err(CodecError::BoundExceeded { .. })
        ));

        let mut invalid = MetaKey::ClusterConfig.encode();
        invalid[2] = CATEGORY_TOPIC_BY_NAME;
        invalid.extend_from_slice(&[0xff, 0xfe]);
        assert_eq!(
            MetaKey::decode(&invalid),
            Err(CodecError::InvalidUtf8("topic name"))
        );

        assert!(validate_topic_name("events.v1").is_ok());
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(&"z".repeat(250)).is_err());
    }

    #[test]
    fn display_forms_use_the_meta_shard_prefix_for_admin_output_only() {
        assert_eq!(MetaKey::ClusterConfig.to_string(), "/meta/0/cluster-config");
        assert_eq!(
            MetaKey::TopicByName {
                name: "events.v1".to_owned()
            }
            .to_string(),
            "/meta/0/topic-by-name/events.v1"
        );
        assert_eq!(
            MetaKey::Node {
                node_uuid: Uuid::from_u128(1)
            }
            .to_string(),
            "/meta/0/node/00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(
            MetaKey::SegmentTierCopy {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                segment_uuid: Uuid::from_u128(4),
            }
            .to_string(),
            concat!(
                "/meta/0/segment-tier-copy/00000000-0000-0000-0000-000000000002",
                "/00000000-0000-0000-0000-000000000003",
                "/00000000-0000-0000-0000-000000000004"
            )
        );
        assert_eq!(
            MetaKey::TopicRetentionPolicy {
                topic_uuid: Uuid::from_u128(2)
            }
            .to_string(),
            "/meta/0/topic-retention-policy/00000000-0000-0000-0000-000000000002"
        );
        assert_eq!(
            MetaKey::RangeTransition {
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                fencing_epoch: 7,
            }
            .to_string(),
            "/meta/0/range-transition/00000000-0000-0000-0000-000000000002\
             /00000000-0000-0000-0000-000000000003/7"
        );
    }
}
