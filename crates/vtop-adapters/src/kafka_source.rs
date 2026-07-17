//! Kafka source adapter (rdkafka, manual offset management).
//!
//! Auto-commit is ALWAYS disabled. The engine commits offsets only after a
//! batch reaches `VERIFIED`. By default a batch never mixes partitions: one
//! batch = one topic + one partition + one offset range.

use crate::base::{DiscoveredSource, ReadResult, SourceAdapter};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use regex::Regex;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use vtop_core::config::KafkaSourceConfig;
use vtop_core::errors::VtopError;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// Build the rdkafka [`ClientConfig`]. Auto-commit is forced to `false`
/// regardless of the input config. Secrets are read from the environment, not
/// logged.
pub fn build_client_config(cfg: &KafkaSourceConfig) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", cfg.bootstrap_servers.join(","));
    cc.set("group.id", &cfg.consumer_group);
    // The engine NEVER uses Kafka auto-commit.
    cc.set("enable.auto.commit", "false");
    cc.set("auto.offset.reset", &cfg.auto_offset_reset);

    if let Some(sp) = &cfg.security_protocol {
        cc.set("security.protocol", sp);
    }
    if let Some(m) = &cfg.sasl_mechanism {
        cc.set("sasl.mechanism", m);
    }
    if let Some(u) = &cfg.sasl_username {
        cc.set("sasl.username", u);
    }
    if let Some(env_name) = &cfg.sasl_password_env {
        if let Ok(pw) = std::env::var(env_name) {
            cc.set("sasl.password", pw); // value never logged
        }
    }
    if let Some(ca) = &cfg.ssl_ca_location {
        cc.set("ssl.ca.location", ca);
    }
    cc
}

#[derive(Debug, Clone, Default)]
struct PartitionCursor {
    /// Highest offset read into memory this session (`None` = nothing read yet).
    last_read_offset: Option<i64>,
    /// Committed "next offset to read" (`None` = nothing committed yet).
    committed_offset: Option<i64>,
}

pub struct KafkaSource {
    cfg: KafkaSourceConfig,
    format: TelemetryFormat,
    include: Regex,
    exclude: Regex,
    consumer: Option<BaseConsumer>,
    // key: (topic, partition)
    cursors: HashMap<(String, i32), PartitionCursor>,
    active: Option<(String, i32)>,
    /// Partition ids per topic, cached with a TTL.
    ///
    /// assign() needs the partition list, but fetching metadata on EVERY read
    /// costs a broker round trip per topic per cycle. With a few topics that is
    /// invisible; with 28 topics under load it dominated the cycle and the
    /// engine appeared to stall for minutes at ~0% CPU. A topic's partition
    /// count changes rarely, so cache it and re-check on the TTL.
    partitions: HashMap<String, (Vec<i32>, Instant)>,
}

/// How long a cached partition list is trusted. Short enough that adding
/// partitions to a live topic is picked up promptly, long enough that the read
/// path is not a metadata storm.
const PARTITION_CACHE_TTL: Duration = Duration::from_secs(60);

impl KafkaSource {
    pub fn new(cfg: KafkaSourceConfig, format: TelemetryFormat) -> Result<Self, VtopError> {
        let include = Regex::new(&cfg.topic_include_regex)
            .map_err(|e| VtopError::Config(format!("bad topic_include_regex: {e}")))?;
        let exclude = Regex::new(&cfg.topic_exclude_regex)
            .map_err(|e| VtopError::Config(format!("bad topic_exclude_regex: {e}")))?;
        Ok(Self {
            cfg,
            format,
            include,
            exclude,
            consumer: None,
            cursors: HashMap::new(),
            active: None,
            partitions: HashMap::new(),
        })
    }

    fn consumer(&mut self) -> Result<&BaseConsumer, VtopError> {
        if self.consumer.is_none() {
            let c: BaseConsumer = build_client_config(&self.cfg)
                .create()
                .map_err(|e| VtopError::Source(format!("creating kafka consumer: {e}")))?;
            self.consumer = Some(c);
        }
        Ok(self.consumer.as_ref().unwrap())
    }

    fn topic_allowed(&self, topic: &str) -> bool {
        self.include.is_match(topic) && !self.exclude.is_match(topic)
    }

    /// Partition ids for `topic`, served from cache when fresh.
    ///
    /// The read path runs once per topic per cycle, so an uncached metadata
    /// round trip here is multiplied by the topic count: with 28 topics it made
    /// each cycle take minutes and looked exactly like a hang.
    fn partitions_for(&mut self, topic: &str) -> Result<Vec<i32>, VtopError> {
        if let Some((ids, at)) = self.partitions.get(topic) {
            if at.elapsed() < PARTITION_CACHE_TTL {
                return Ok(ids.clone());
            }
        }
        let ids: Vec<i32> = {
            let consumer = self.consumer()?;
            let md = consumer
                .fetch_metadata(Some(topic), Duration::from_secs(10))
                .map_err(|e| VtopError::Source(format!("fetch_metadata {topic}: {e}")))?;
            md.topics()
                .iter()
                .find(|t| t.name() == topic)
                .map(|t| t.partitions().iter().map(|p| p.id()).collect())
                .unwrap_or_default()
        };
        self.partitions
            .insert(topic.to_string(), (ids.clone(), Instant::now()));
        Ok(ids)
    }
}

#[async_trait]
impl SourceAdapter for KafkaSource {
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
        // Build a throwaway consumer to fetch metadata.
        let consumer: BaseConsumer = build_client_config(&self.cfg)
            .create()
            .map_err(|e| VtopError::Source(format!("creating kafka consumer: {e}")))?;
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| VtopError::Source(format!("fetch_metadata: {e}")))?;

        let mut out = Vec::new();
        for t in metadata.topics() {
            if self.topic_allowed(t.name()) {
                out.push(DiscoveredSource {
                    source_type: SourceType::Kafka,
                    source_name: t.name().to_string(),
                    format: self.format.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn read_batch_candidates(
        &mut self,
        source: &DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<ReadResult, VtopError> {
        let topic = source.source_name.clone();
        let group = self.cfg.consumer_group.clone();

        // Discover the topic's partitions, then ASSIGN them at our tracked
        // offsets. We deliberately use assign() rather than subscribe(): a
        // subscribe() on every read triggers a consumer-group rebalance, and
        // because the engine commits offsets only AFTER verification there is no
        // committed offset to resume from — so each rebalance reseeks to the
        // configured reset (earliest) and the short poll window is spent
        // rebalancing instead of fetching. assign() is rebalance-free and lets
        // us control the exact start offset per partition.
        self.consumer()?;
        let partitions: Vec<i32> = self.partitions_for(&topic)?;
        if partitions.is_empty() {
            // Topic has no partitions (e.g. just deleted) — nothing to read.
            self.active = Some((topic.clone(), 0));
            return Ok(ReadResult {
                progress_start: ProgressMarker::Kafka {
                    topic: topic.clone(),
                    partition: 0,
                    start_offset: 0,
                    end_offset: 0,
                    consumer_group: group,
                },
                progress_end: ProgressMarker::Kafka {
                    topic,
                    partition: 0,
                    start_offset: 0,
                    end_offset: 0,
                    consumer_group: self.cfg.consumer_group.clone(),
                },
                records: Vec::new(),
                first_timestamp: None,
                last_timestamp: None,
                verbatim: false,
            });
        }

        // Build the assignment from each partition's tracked position.
        let mut tpl = TopicPartitionList::new();
        for &p in &partitions {
            let cur = self.cursors.entry((topic.clone(), p)).or_default();
            let off = match cur.last_read_offset {
                // Continue from our in-session read head.
                Some(lr) => Offset::Offset(lr + 1),
                None => match cur.committed_offset {
                    // Resume from a position we committed this session.
                    Some(c) => Offset::Offset(c),
                    // Nothing in memory (fresh process / first read for this
                    // partition): resolve the GROUP'S COMMITTED offset from the
                    // broker, falling back to `auto.offset.reset` when the group
                    // has never committed.
                    //
                    // This must not be Offset::Beginning: subscribe() used to
                    // resume from the committed offset implicitly, and assign()
                    // does not. Starting at Beginning would make every engine
                    // restart re-read the topic from the start and re-archive
                    // already-committed records.
                    None => Offset::Stored,
                },
            };
            tpl.add_partition_offset(&topic, p, off)
                .map_err(|e| VtopError::Source(format!("assign tpl {topic}-{p}: {e}")))?;
        }
        {
            let consumer = self.consumer()?;
            consumer
                .assign(&tpl)
                .map_err(|e| VtopError::Source(format!("assign {topic}: {e}")))?;
        }
        let consumer = self.consumer()?;

        let mut records: Vec<Vec<u8>> = Vec::new();
        let mut bytes: usize = 0;
        // To avoid mixing partitions, lock onto the first partition we see.
        let mut locked_partition: Option<i32> = None;
        let mut start_offset: Option<i64> = None;
        let mut end_offset: i64 = -1;

        let deadline = std::time::Instant::now() + max_wait;
        loop {
            if records.len() >= max_records || bytes >= max_bytes {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(msg)) => {
                    let p = msg.partition();
                    if let Some(lp) = locked_partition {
                        if p != lp {
                            // Different partition — do not mix into this batch.
                            // The message was already returned by poll(), so the
                            // consumer's position has advanced past it; rewind
                            // that partition to this offset so the record is
                            // redelivered on a later read instead of being lost.
                            consumer
                                .seek(
                                    msg.topic(),
                                    p,
                                    Offset::Offset(msg.offset()),
                                    Duration::from_secs(5),
                                )
                                .map_err(|e| {
                                    VtopError::Source(format!(
                                        "kafka seek (unmix partition {p}): {e}"
                                    ))
                                })?;
                            break;
                        }
                    } else {
                        locked_partition = Some(p);
                    }
                    let off = msg.offset();
                    if start_offset.is_none() {
                        start_offset = Some(off);
                    }
                    end_offset = off;
                    let payload = msg.payload().unwrap_or_default().to_vec();
                    bytes += payload.len();
                    records.push(payload);
                }
                Some(Err(e)) => {
                    return Err(VtopError::Source(format!("kafka poll error: {e}")));
                }
                None => break, // no more messages within the wait window
            }
            if remaining.is_zero() {
                break;
            }
        }

        let partition = locked_partition.unwrap_or(0);
        let start = start_offset.unwrap_or(0);
        self.active = Some((topic.clone(), partition));
        let cur = self.cursors.entry((topic.clone(), partition)).or_default();
        if !records.is_empty() {
            cur.last_read_offset = Some(end_offset);
        }

        let mk = |s: i64, e: i64| ProgressMarker::Kafka {
            topic: topic.clone(),
            partition,
            start_offset: s,
            end_offset: e,
            consumer_group: group.clone(),
        };

        Ok(ReadResult {
            progress_start: mk(start, start),
            progress_end: mk(start, end_offset.max(start)),
            records,
            first_timestamp: None,
            last_timestamp: None,
            // Kafka messages are newline-framed into the object (offset-based,
            // not byte-exact), so records are re-framed on serialization.
            verbatim: false,
        })
    }

    async fn get_progress_marker(&self) -> Result<ProgressMarker, VtopError> {
        let (topic, partition) = self
            .active
            .clone()
            .ok_or_else(|| VtopError::Source("no active kafka partition".into()))?;
        let cur = self
            .cursors
            .get(&(topic.clone(), partition))
            .cloned()
            .unwrap_or_default();
        let start_offset = cur.committed_offset.unwrap_or(0);
        Ok(ProgressMarker::Kafka {
            topic,
            partition,
            start_offset,
            end_offset: cur.last_read_offset.unwrap_or(start_offset),
            consumer_group: self.cfg.consumer_group.clone(),
        })
    }

    async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::Kafka {
            topic,
            partition,
            end_offset,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "kafka adapter given non-kafka marker".into(),
            ));
        };

        // Committed offset is "next to read" = last consumed + 1.
        let commit_at = end_offset + 1;
        let consumer = self.consumer()?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, *partition, Offset::Offset(commit_at))
            .map_err(|e| VtopError::Source(format!("tpl: {e}")))?;
        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| VtopError::Source(format!("kafka commit: {e}")))?;

        let cur = self.cursors.entry((topic.clone(), *partition)).or_default();
        cur.committed_offset = Some(commit_at);
        tracing::info!(
            topic,
            partition,
            commit_at,
            "kafka offset committed (post-verify)"
        );
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::Kafka {
            topic,
            partition,
            start_offset,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "kafka adapter given non-kafka marker".into(),
            ));
        };
        // Reads assign() from the cursor, so rewinding means resetting the
        // cursor: clear the in-memory read head and pin the resume position to
        // start_offset. The next read assigns the partition at that offset.
        let cur = self.cursors.entry((topic.clone(), *partition)).or_default();
        cur.last_read_offset = None;
        cur.committed_offset = Some(*start_offset);
        tracing::warn!(topic, partition, start_offset, "kafka rewound for replay");
        Ok(())
    }

    fn source_type(&self) -> SourceType {
        SourceType::Kafka
    }

    fn source_name(&self) -> String {
        self.active.clone().map(|(t, _)| t).unwrap_or_default()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> KafkaSourceConfig {
        KafkaSourceConfig {
            enabled: true,
            bootstrap_servers: vec!["kafka:9092".into()],
            consumer_group: "vtop-engine".into(),
            topic_include_regex: ".*".into(),
            topic_exclude_regex: "^__.*".into(),
            auto_offset_reset: "earliest".into(),
            enable_auto_commit: false,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password_env: None,
            ssl_ca_location: None,
        }
    }

    #[test]
    fn never_enables_auto_commit() {
        let cc = build_client_config(&cfg());
        assert_eq!(cc.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn topic_filters_apply() {
        let src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        assert!(src.topic_allowed("app_events"));
        assert!(!src.topic_allowed("__consumer_offsets"));
    }
}
