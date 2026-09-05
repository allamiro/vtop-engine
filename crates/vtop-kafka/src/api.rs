//! The five phase-1 APIs, request and response, at every served version
//! (#225).
//!
//! Each version gate here is a line in Kafka's own schema: a field added at
//! version N is read or written at N and above, never guessed. The ranges the
//! header gate admits ([`crate::messages::ApiKey::version_range`]) are exactly
//! the ones these codecs implement, and a test pins that every admitted
//! version round-trips, so widening a range without teaching a codec its new
//! field fails here rather than in a client.

use crate::messages::{ApiKey, ErrorCode};
use crate::wire::{Decoder, Encoder, WireError};

// ------------------------------------------------------------------ ApiVersions

/// The response: which api keys this gateway serves, at which versions.
pub fn encode_api_versions(out: &mut Encoder, version: i16, error: ErrorCode) {
    out.i16(error.as_i16());
    let keys = [
        ApiKey::Produce,
        ApiKey::Fetch,
        ApiKey::ListOffsets,
        ApiKey::Metadata,
        ApiKey::ApiVersions,
    ];
    out.array_len(keys.len());
    for key in keys {
        let (min, max) = key.version_range();
        out.i16(key.as_i16());
        out.i16(min);
        out.i16(max);
    }
    if version >= 1 {
        out.i32(0); // throttle_time_ms
    }
}

// --------------------------------------------------------------------- Metadata

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequest {
    /// `None` asks for every topic (v1+); an empty list asks for none.
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool,
}

pub fn decode_metadata(d: &mut Decoder<'_>, version: i16) -> Result<MetadataRequest, WireError> {
    let topics = match d.array_len("metadata.topics")? {
        None => None,
        Some(n) => {
            let mut names = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                names.push(d.string("metadata.topics.name")?.to_owned());
            }
            Some(names)
        }
    };
    let allow_auto_topic_creation = if version >= 4 {
        d.bool("metadata.allowAutoTopicCreation")?
    } else {
        true
    };
    if version >= 8 {
        d.bool("metadata.includeClusterAuthorizedOperations")?;
        d.bool("metadata.includeTopicAuthorizedOperations")?;
    }
    Ok(MetadataRequest {
        topics,
        allow_auto_topic_creation,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBroker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataTopic {
    pub error: ErrorCode,
    pub name: String,
    /// Partition zero's leader, when the topic exists: phase 1 has one
    /// partition per topic and this gateway leads it.
    pub leader: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    pub brokers: Vec<MetadataBroker>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<MetadataTopic>,
}

pub fn encode_metadata(out: &mut Encoder, version: i16, r: &MetadataResponse) {
    if version >= 3 {
        out.i32(0); // throttle_time_ms
    }
    out.array_len(r.brokers.len());
    for broker in &r.brokers {
        out.i32(broker.node_id);
        out.string(&broker.host);
        out.i32(broker.port);
        if version >= 1 {
            out.nullable_string(None); // rack
        }
    }
    if version >= 2 {
        out.nullable_string(r.cluster_id.as_deref());
    }
    if version >= 1 {
        out.i32(r.controller_id);
    }
    out.array_len(r.topics.len());
    for topic in &r.topics {
        out.i16(topic.error.as_i16());
        out.string(&topic.name);
        if version >= 1 {
            out.bool(false); // is_internal
        }
        match topic.leader {
            None => out.array_len(0),
            Some(leader) => {
                out.array_len(1);
                out.i16(ErrorCode::None.as_i16());
                out.i32(0); // partition_index
                out.i32(leader);
                if version >= 7 {
                    out.i32(0); // leader_epoch
                }
                out.array_len(1);
                out.i32(leader); // replica_nodes
                out.array_len(1);
                out.i32(leader); // isr_nodes
                if version >= 5 {
                    out.array_len(0); // offline_replicas
                }
            }
        }
        if version >= 8 {
            out.i32(i32::MIN); // topic_authorized_operations: not requested
        }
    }
    if version >= 8 {
        out.i32(i32::MIN); // cluster_authorized_operations: not requested
    }
}

// ---------------------------------------------------------------------- Produce

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartition {
    pub index: i32,
    pub records: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopic {
    pub name: String,
    pub partitions: Vec<ProducePartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopic>,
}

pub fn decode_produce(d: &mut Decoder<'_>, _version: i16) -> Result<ProduceRequest, WireError> {
    // v3+ throughout the served range: transactional_id leads.
    let transactional_id = d
        .nullable_string("produce.transactionalId")?
        .map(str::to_owned);
    let acks = d.i16("produce.acks")?;
    let timeout_ms = d.i32("produce.timeoutMs")?;
    let topic_count = d.array_len("produce.topicData")?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count.min(1024));
    for _ in 0..topic_count {
        let name = d.string("produce.topicData.name")?.to_owned();
        let partition_count = d.array_len("produce.partitionData")?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(partition_count.min(1024));
        for _ in 0..partition_count {
            let index = d.i32("produce.partitionData.index")?;
            let records = d
                .nullable_bytes("produce.partitionData.records")?
                .map(<[u8]>::to_vec);
            partitions.push(ProducePartition { index, records });
        }
        topics.push(ProduceTopic { name, partitions });
    }
    Ok(ProduceRequest {
        transactional_id,
        acks,
        timeout_ms,
        topics,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponse {
    pub index: i32,
    pub error: ErrorCode,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
    /// Why, when refused (v8+ carries it on the wire; the log carries it
    /// always).
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopicResponse {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponse>,
}

pub fn encode_produce(out: &mut Encoder, version: i16, topics: &[ProduceTopicResponse]) {
    out.array_len(topics.len());
    for topic in topics {
        out.string(&topic.name);
        out.array_len(topic.partitions.len());
        for p in &topic.partitions {
            out.i32(p.index);
            out.i16(p.error.as_i16());
            out.i64(p.base_offset);
            if version >= 2 {
                out.i64(p.log_append_time_ms);
            }
            if version >= 5 {
                out.i64(p.log_start_offset);
            }
            if version >= 8 {
                out.array_len(0); // record_errors: the whole batch is refused, not one record
                out.nullable_string(p.error_message.as_deref());
            }
        }
    }
    if version >= 1 {
        out.i32(0); // throttle_time_ms
    }
}

// ------------------------------------------------------------------------ Fetch

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartition {
    pub index: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopic {
    pub name: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    /// 0 = read uncommitted, 1 = read committed.
    pub isolation_level: i8,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopic>,
}

pub fn decode_fetch(d: &mut Decoder<'_>, version: i16) -> Result<FetchRequest, WireError> {
    d.i32("fetch.replicaId")?;
    let max_wait_ms = d.i32("fetch.maxWaitMs")?;
    let min_bytes = d.i32("fetch.minBytes")?;
    let max_bytes = d.i32("fetch.maxBytes")?; // v3+
    let isolation_level = d.i8("fetch.isolationLevel")?; // v4+
    let session_epoch = if version >= 7 {
        d.i32("fetch.sessionId")?;
        d.i32("fetch.sessionEpoch")?
    } else {
        -1
    };
    let topic_count = d.array_len("fetch.topics")?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count.min(1024));
    for _ in 0..topic_count {
        let name = d.string("fetch.topics.topic")?.to_owned();
        let partition_count = d.array_len("fetch.topics.partitions")?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(partition_count.min(1024));
        for _ in 0..partition_count {
            let index = d.i32("fetch.partitions.partition")?;
            if version >= 9 {
                d.i32("fetch.partitions.currentLeaderEpoch")?;
            }
            let fetch_offset = d.i64("fetch.partitions.fetchOffset")?;
            if version >= 5 {
                d.i64("fetch.partitions.logStartOffset")?;
            }
            let max_bytes = d.i32("fetch.partitions.partitionMaxBytes")?;
            partitions.push(FetchPartition {
                index,
                fetch_offset,
                max_bytes,
            });
        }
        topics.push(FetchTopic { name, partitions });
    }
    if version >= 7 {
        let forgotten = d.array_len("fetch.forgottenTopicsData")?.unwrap_or(0);
        for _ in 0..forgotten {
            d.string("fetch.forgottenTopicsData.topic")?;
            let n = d
                .array_len("fetch.forgottenTopicsData.partitions")?
                .unwrap_or(0);
            for _ in 0..n {
                d.i32("fetch.forgottenTopicsData.partitions")?;
            }
        }
    }
    if version >= 11 {
        d.string("fetch.rackId")?;
    }
    Ok(FetchRequest {
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        session_epoch,
        topics,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponse {
    pub index: i32,
    pub error: ErrorCode,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponse {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

pub fn encode_fetch(out: &mut Encoder, version: i16, topics: &[FetchTopicResponse]) {
    out.i32(0); // throttle_time_ms (v1+)
    if version >= 7 {
        out.i16(ErrorCode::None.as_i16());
        out.i32(0); // session_id: no fetch sessions, every fetch is full
    }
    out.array_len(topics.len());
    for topic in topics {
        out.string(&topic.name);
        out.array_len(topic.partitions.len());
        for p in &topic.partitions {
            out.i32(p.index);
            out.i16(p.error.as_i16());
            out.i64(p.high_watermark);
            // last_stable_offset (v4+): with no transactions anywhere, every
            // record below the high watermark is stable, so the two are one.
            out.i64(p.high_watermark);
            if version >= 5 {
                out.i64(p.log_start_offset);
            }
            out.array_len(0); // aborted_transactions (v4+): there are none
            if version >= 11 {
                out.i32(-1); // preferred_read_replica
            }
            out.nullable_bytes(Some(&p.records));
        }
    }
}

// ------------------------------------------------------------------ ListOffsets

pub const TIMESTAMP_LATEST: i64 = -1;
pub const TIMESTAMP_EARLIEST: i64 = -2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartition {
    pub index: i32,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsRequest {
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsTopic>,
}

pub fn decode_list_offsets(
    d: &mut Decoder<'_>,
    version: i16,
) -> Result<ListOffsetsRequest, WireError> {
    d.i32("listOffsets.replicaId")?;
    let isolation_level = if version >= 2 {
        d.i8("listOffsets.isolationLevel")?
    } else {
        0
    };
    let topic_count = d.array_len("listOffsets.topics")?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count.min(1024));
    for _ in 0..topic_count {
        let name = d.string("listOffsets.topics.name")?.to_owned();
        let partition_count = d.array_len("listOffsets.topics.partitions")?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(partition_count.min(1024));
        for _ in 0..partition_count {
            let index = d.i32("listOffsets.partitions.partitionIndex")?;
            if version >= 4 {
                d.i32("listOffsets.partitions.currentLeaderEpoch")?;
            }
            let timestamp = d.i64("listOffsets.partitions.timestamp")?;
            partitions.push(ListOffsetsPartition { index, timestamp });
        }
        topics.push(ListOffsetsTopic { name, partitions });
    }
    Ok(ListOffsetsRequest {
        isolation_level,
        topics,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartitionResponse {
    pub index: i32,
    pub error: ErrorCode,
    pub timestamp: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicResponse {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

pub fn encode_list_offsets(out: &mut Encoder, version: i16, topics: &[ListOffsetsTopicResponse]) {
    if version >= 2 {
        out.i32(0); // throttle_time_ms
    }
    out.array_len(topics.len());
    for topic in topics {
        out.string(&topic.name);
        out.array_len(topic.partitions.len());
        for p in &topic.partitions {
            out.i32(p.index);
            out.i16(p.error.as_i16());
            out.i64(p.timestamp);
            out.i64(p.offset);
            if version >= 4 {
                out.i32(0); // leader_epoch
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions(key: ApiKey) -> impl Iterator<Item = i16> {
        let (min, max) = key.version_range();
        min..=max
    }

    /// Every admitted Metadata version decodes its request and encodes a
    /// response whose length changes exactly where the schema adds a field.
    #[test]
    fn metadata_round_trips_at_every_served_version() {
        let response = MetadataResponse {
            brokers: vec![MetadataBroker {
                node_id: 1,
                host: "gw".to_owned(),
                port: 9092,
            }],
            cluster_id: Some("vtop".to_owned()),
            controller_id: 1,
            topics: vec![
                MetadataTopic {
                    error: ErrorCode::None,
                    name: "events".to_owned(),
                    leader: Some(1),
                },
                MetadataTopic {
                    error: ErrorCode::UnknownTopicOrPartition,
                    name: "nope".to_owned(),
                    leader: None,
                },
            ],
        };
        let mut previous = 0;
        for version in versions(ApiKey::Metadata) {
            let mut e = Encoder::new();
            e.array_len(1);
            e.string("events");
            if version >= 4 {
                e.bool(false);
            }
            if version >= 8 {
                e.bool(false);
                e.bool(false);
            }
            let bytes = e.into_vec();
            let mut d = Decoder::new(&bytes);
            let request = decode_metadata(&mut d, version).unwrap();
            d.expect_consumed("metadata").unwrap();
            assert_eq!(request.topics, Some(vec!["events".to_owned()]));

            let mut out = Encoder::new();
            encode_metadata(&mut out, version, &response);
            assert!(out.len() >= previous, "v{version} must not shrink");
            previous = out.len();
        }
        // A null topics array asks for everything.
        let mut e = Encoder::new();
        e.i32(-1);
        let bytes = e.into_vec();
        assert_eq!(
            decode_metadata(&mut Decoder::new(&bytes), 1)
                .unwrap()
                .topics,
            None
        );
    }

    fn produce_request_bytes(version: i16, records: &[u8]) -> Vec<u8> {
        let mut e = Encoder::new();
        e.nullable_string(None);
        e.i16(-1);
        e.i32(1_500);
        e.array_len(1);
        e.string("events");
        e.array_len(1);
        e.i32(0);
        e.nullable_bytes(Some(records));
        let _ = version;
        e.into_vec()
    }

    #[test]
    fn produce_decodes_and_its_response_grows_only_where_the_schema_does() {
        let mut previous = 0;
        for version in versions(ApiKey::Produce) {
            let bytes = produce_request_bytes(version, b"batch-bytes");
            let mut d = Decoder::new(&bytes);
            let request = decode_produce(&mut d, version).unwrap();
            d.expect_consumed("produce").unwrap();
            assert_eq!(request.acks, -1);
            assert_eq!(
                request.topics[0].partitions[0].records.as_deref(),
                Some(b"batch-bytes".as_slice())
            );

            let mut out = Encoder::new();
            encode_produce(
                &mut out,
                version,
                &[ProduceTopicResponse {
                    name: "events".to_owned(),
                    partitions: vec![ProducePartitionResponse {
                        index: 0,
                        error: ErrorCode::None,
                        base_offset: 7,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        error_message: None,
                    }],
                }],
            );
            assert!(out.len() >= previous, "v{version}");
            previous = out.len();
        }
    }

    fn fetch_request_bytes(version: i16) -> Vec<u8> {
        let mut e = Encoder::new();
        e.i32(-1); // replica_id
        e.i32(500); // max_wait_ms
        e.i32(1); // min_bytes
        e.i32(1 << 20); // max_bytes
        e.i8(0); // isolation
        if version >= 7 {
            e.i32(0); // session_id
            e.i32(-1); // session_epoch
        }
        e.array_len(1);
        e.string("events");
        e.array_len(1);
        e.i32(0);
        if version >= 9 {
            e.i32(-1); // current_leader_epoch
        }
        e.i64(3); // fetch_offset
        if version >= 5 {
            e.i64(0); // log_start_offset
        }
        e.i32(1 << 16); // partition_max_bytes
        if version >= 7 {
            e.array_len(0); // forgotten
        }
        if version >= 11 {
            e.string(""); // rack_id
        }
        e.into_vec()
    }

    #[test]
    fn fetch_decodes_at_every_served_version_and_serves_the_stable_offset_as_the_watermark() {
        for version in versions(ApiKey::Fetch) {
            let bytes = fetch_request_bytes(version);
            let mut d = Decoder::new(&bytes);
            let request = decode_fetch(&mut d, version).unwrap();
            d.expect_consumed("fetch").unwrap();
            assert_eq!(request.max_wait_ms, 500);
            assert_eq!(request.topics[0].partitions[0].fetch_offset, 3);

            let mut out = Encoder::new();
            encode_fetch(
                &mut out,
                version,
                &[FetchTopicResponse {
                    name: "events".to_owned(),
                    partitions: vec![FetchPartitionResponse {
                        index: 0,
                        error: ErrorCode::None,
                        high_watermark: 9,
                        log_start_offset: 0,
                        records: b"rb".to_vec(),
                    }],
                }],
            );
            // Read the response back far enough to check the watermark pair.
            let bytes = out.into_vec();
            let mut d = Decoder::new(&bytes);
            d.i32("throttle").unwrap();
            if version >= 7 {
                d.i16("error").unwrap();
                d.i32("session").unwrap();
            }
            d.array_len("topics").unwrap();
            d.string("topic").unwrap();
            d.array_len("partitions").unwrap();
            d.i32("index").unwrap();
            d.i16("error").unwrap();
            assert_eq!(d.i64("hwm").unwrap(), 9);
            assert_eq!(
                d.i64("lso").unwrap(),
                9,
                "no transactions: stable is the watermark"
            );
        }
    }

    #[test]
    fn list_offsets_decodes_at_every_served_version() {
        for version in versions(ApiKey::ListOffsets) {
            let mut e = Encoder::new();
            e.i32(-1);
            if version >= 2 {
                e.i8(0);
            }
            e.array_len(1);
            e.string("events");
            e.array_len(1);
            e.i32(0);
            if version >= 4 {
                e.i32(-1);
            }
            e.i64(TIMESTAMP_LATEST);
            let bytes = e.into_vec();
            let mut d = Decoder::new(&bytes);
            let request = decode_list_offsets(&mut d, version).unwrap();
            d.expect_consumed("listOffsets").unwrap();
            assert_eq!(request.topics[0].partitions[0].timestamp, TIMESTAMP_LATEST);

            let mut out = Encoder::new();
            encode_list_offsets(
                &mut out,
                version,
                &[ListOffsetsTopicResponse {
                    name: "events".to_owned(),
                    partitions: vec![ListOffsetsPartitionResponse {
                        index: 0,
                        error: ErrorCode::None,
                        timestamp: TIMESTAMP_LATEST,
                        offset: 42,
                    }],
                }],
            );
            let bytes = out.into_vec();
            let mut d = Decoder::new(&bytes);
            if version >= 2 {
                d.i32("throttle").unwrap();
            }
            d.array_len("topics").unwrap();
            d.string("topic").unwrap();
            d.array_len("partitions").unwrap();
            d.i32("index").unwrap();
            d.i16("error").unwrap();
            d.i64("timestamp").unwrap();
            assert_eq!(d.i64("offset").unwrap(), 42);
        }
    }

    #[test]
    fn api_versions_lists_exactly_the_served_ranges() {
        for version in versions(ApiKey::ApiVersions) {
            let mut out = Encoder::new();
            encode_api_versions(&mut out, version, ErrorCode::None);
            let bytes = out.into_vec();
            let mut d = Decoder::new(&bytes);
            assert_eq!(d.i16("error").unwrap(), 0);
            let n = d.array_len("keys").unwrap().unwrap();
            assert_eq!(n, 5);
            for _ in 0..n {
                let key = ApiKey::from_i16(d.i16("key").unwrap()).unwrap();
                let (min, max) = key.version_range();
                assert_eq!((d.i16("min").unwrap(), d.i16("max").unwrap()), (min, max));
            }
            if version >= 1 {
                d.i32("throttle").unwrap();
            }
            d.expect_consumed("apiVersions").unwrap();
        }
    }
}
