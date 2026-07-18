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

/// Turn the lookup of the password environment variable into either the password
/// or a precise configuration error.
///
/// A referenced-but-missing secret is a configuration error, never a silent
/// fallback to no password. The two failure modes are distinguished so an
/// operator is not told "not set" when the variable IS set but unreadable.
/// Only the variable NAME ever appears in a message - never the value.
///
/// Kept pure (the lookup is passed in) so both branches are unit-testable
/// without mutating the process environment: `std::env::set_var` is unsound
/// while other tests and Tokio runtimes run concurrently in the same binary.
fn require_password_env(
    env_name: &str,
    looked_up: Result<String, std::env::VarError>,
) -> Result<String, VtopError> {
    match looked_up {
        Ok(pw) => Ok(pw),
        Err(std::env::VarError::NotPresent) => Err(VtopError::Config(format!(
            "sasl_password_env names ${env_name}, but that environment variable is not set; \
             refusing to connect without the password"
        ))),
        Err(std::env::VarError::NotUnicode(_)) => Err(VtopError::Config(format!(
            "sasl_password_env names ${env_name}, but its value is not valid Unicode; \
             refusing to connect without a usable password"
        ))),
    }
}

/// Build the rdkafka [`ClientConfig`]. Auto-commit is forced to `false`
/// regardless of the input config. Secrets are read from the environment, not
/// logged.
///
/// Fails if the config NAMES a password environment variable that is not set.
/// Silently skipping it would start the engine unauthenticated when the operator
/// explicitly asked for SASL - a downgrade that looks like a working connection
/// until the broker rejects it, or worse, accepts it.
pub fn build_client_config(cfg: &KafkaSourceConfig) -> Result<ClientConfig, VtopError> {
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
        let pw = require_password_env(env_name, std::env::var(env_name))?;
        cc.set("sasl.password", pw); // value never logged
    }
    if let Some(ca) = &cfg.ssl_ca_location {
        cc.set("ssl.ca.location", ca);
    }
    Ok(cc)
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

/// How long a cached partition list is trusted.
///
/// Must comfortably exceed a full engine cycle, or the cache expires before it
/// is reused and the metadata storm returns. A cycle reads sources sequentially
/// with a 2s poll each, so ~28 topics is already ~56s - a 60s TTL would expire
/// every entry mid-cycle under exactly the load this cache exists for. Five
/// minutes leaves generous headroom while still picking up a repartitioned topic
/// promptly.
const PARTITION_CACHE_TTL: Duration = Duration::from_secs(300);

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
            let c: BaseConsumer = build_client_config(&self.cfg)?
                .create()
                .map_err(|e| VtopError::Source(format!("creating kafka consumer: {e}")))?;
            self.consumer = Some(c);
        }
        Ok(self.consumer.as_ref().unwrap())
    }

    fn topic_allowed(&self, topic: &str) -> bool {
        self.include.is_match(topic) && !self.exclude.is_match(topic)
    }

    /// Drop cached partition lists for topics that no longer exist, so a broker
    /// that churns through many short-lived topics does not grow the cache
    /// without bound.
    pub fn prune_partition_cache(&mut self, live_topics: &[String]) {
        let live: std::collections::HashSet<&str> =
            live_topics.iter().map(|s| s.as_str()).collect();
        self.partitions
            .retain(|topic, _| live.contains(topic.as_str()));
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
        let consumer: BaseConsumer = build_client_config(&self.cfg)?
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
    ) -> Result<Vec<ReadResult>, VtopError> {
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
            return Ok(vec![ReadResult {
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
            }]);
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

        // Accumulate PER PARTITION. Kafka interleaves partitions within one
        // consumer, so the old code locked onto the first partition seen, and
        // on any other partition it rewound with seek() and broke out of the
        // read entirely. On a 24-partition topic that ended most reads after a
        // handful of records — batches never filled, every switch cost a broker
        // seek round-trip, and a cycle touched a fraction of the topic.
        //
        // Nothing needs the lock: the ENGINE already isolates batches per
        // partition (`buffer_key` appends `#p{partition}`), so mixed-partition
        // reads are safe as long as records are handed back grouped. Returning
        // one ReadResult per partition does exactly that, and lets a single read
        // feed every partition's buffer at once.
        struct PartAcc {
            records: Vec<Vec<u8>>,
            start_offset: i64,
            end_offset: i64,
        }
        let mut parts: std::collections::HashMap<i32, PartAcc> = std::collections::HashMap::new();
        let mut bytes: usize = 0;
        let mut total_records: usize = 0;

        let deadline = std::time::Instant::now() + max_wait;
        loop {
            // Budgets are across the whole read, not per partition, so one busy
            // partition cannot starve memory bounds.
            if total_records >= max_records || bytes >= max_bytes {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(msg)) => {
                    let p = msg.partition();
                    let off = msg.offset();
                    let payload = msg.payload().unwrap_or_default().to_vec();
                    bytes += payload.len();
                    total_records += 1;
                    let acc = parts.entry(p).or_insert_with(|| PartAcc {
                        records: Vec::new(),
                        start_offset: off,
                        end_offset: off,
                    });
                    acc.end_offset = off;
                    acc.records.push(payload);
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

        // Record the last partition touched so `get_progress_marker` keeps
        // working. Single-slot `active` is a known wart with many partitions in
        // play; it is not on the commit path (commit_progress takes an explicit
        // marker) and is tracked separately.
        let mut out: Vec<ReadResult> = Vec::with_capacity(parts.len());
        let mut partitions: Vec<i32> = parts.keys().copied().collect();
        partitions.sort_unstable();
        for p in partitions {
            let acc = &parts[&p];
            self.active = Some((topic.clone(), p));
            let cur = self.cursors.entry((topic.clone(), p)).or_default();
            if !acc.records.is_empty() {
                cur.last_read_offset = Some(acc.end_offset);
            }
            let mk = |s: i64, e: i64| ProgressMarker::Kafka {
                topic: topic.clone(),
                partition: p,
                start_offset: s,
                end_offset: e,
                consumer_group: group.clone(),
            };
            out.push(ReadResult {
                progress_start: mk(acc.start_offset, acc.start_offset),
                progress_end: mk(acc.start_offset, acc.end_offset.max(acc.start_offset)),
                records: acc.records.clone(),
                first_timestamp: None,
                last_timestamp: None,
                // Kafka messages are newline-framed into the object (offset-based,
                // not byte-exact), so records are re-framed on serialization.
                verbatim: false,
            });
        }
        Ok(out)
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
    fn prune_drops_dead_topics_and_keeps_live_ones() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        // Seed the cache directly - partitions_for would need a broker.
        src.partitions
            .insert("live".into(), (vec![0], std::time::Instant::now()));
        src.partitions
            .insert("dead".into(), (vec![0], std::time::Instant::now()));
        src.prune_partition_cache(&["live".to_string()]);
        assert!(src.partitions.contains_key("live"), "live topic kept");
        assert!(!src.partitions.contains_key("dead"), "dead topic dropped");
    }

    #[test]
    fn never_enables_auto_commit() {
        let cc = build_client_config(&cfg()).expect("config without secrets builds");
        assert_eq!(cc.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn missing_password_env_is_a_config_error_not_a_silent_downgrade() {
        // The operator asked for SASL by naming an env var. If that var is not
        // set we must REFUSE, not connect without a password.
        let err = require_password_env("VTOP_PW_VAR", Err(std::env::VarError::NotPresent))
            .expect_err("missing secret must fail");
        assert!(
            matches!(err, VtopError::Config(_)),
            "must be a Config error, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("VTOP_PW_VAR"),
            "should name the variable: {msg}"
        );
        assert!(msg.contains("not set"), "should say it is not set: {msg}");
    }

    #[test]
    #[cfg(unix)]
    fn non_unicode_password_env_is_reported_as_such_not_as_missing() {
        use std::os::unix::ffi::OsStringExt;
        let bad = std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
        let err = require_password_env("VTOP_PW_VAR", Err(std::env::VarError::NotUnicode(bad)))
            .expect_err("unreadable secret must fail");
        assert!(matches!(err, VtopError::Config(_)));
        let msg = err.to_string();
        assert!(msg.contains("VTOP_PW_VAR"));
        assert!(
            msg.contains("Unicode"),
            "a set-but-unreadable value must not be reported as 'not set': {msg}"
        );
    }

    #[test]
    fn password_env_that_is_set_is_applied() {
        let pw = require_password_env("VTOP_PW_VAR", Ok("hunter2".to_string()))
            .expect("present secret resolves");
        assert_eq!(pw, "hunter2");
    }

    #[test]
    fn topic_filters_apply() {
        let src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        assert!(src.topic_allowed("app_events"));
        assert!(!src.topic_allowed("__consumer_offsets"));
    }
}
