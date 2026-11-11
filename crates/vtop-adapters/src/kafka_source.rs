//! Kafka source adapter (rdkafka, manual offset management).
//!
//! Auto-commit is ALWAYS disabled. The engine commits offsets only after a
//! batch reaches `VERIFIED`. By default a batch never mixes partitions: one
//! batch = one topic + one partition + one offset range.

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
    SourceRetentionLoss,
};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
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

/// How long a watermark query may take before the check is skipped.
///
/// Bounded because it runs on a read path, and an unbounded network call there
/// is the mistake #374 was made of. Skipping is the right failure: a broker
/// that will not answer has not lost records, it has declined to say.
const WATERMARK_TIMEOUT_SECONDS: u64 = 5;

/// How often the retention check runs. Retention moves on the scale of hours;
/// a minute is free and keeps a broker round trip off the per-pass path.
const RETENTION_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Where a rewind to `start_offset` should actually land.
///
/// The committed frontier is the offset everything below which is archived. A
/// rewind below it would re-read, and re-archive, records that are already
/// safe, and recovery asks for exactly that on every restart for a ledger row
/// that a later batch has long since superseded. So a rewind never moves below
/// the committed frontier, the same rule the file and spool adapters apply to
/// their committed byte. When the frontier is not known the requested start is
/// used unchanged.
fn clamp_rewind(start_offset: i64, frontier: Option<i64>) -> i64 {
    frontier.map_or(start_offset, |committed| start_offset.max(committed))
}

/// The committed frontier a rewind is clamped to (#614).
///
/// `known` is what the ledger recorded for this partition and group, or what
/// this process committed since it started; `in_memory` is the cursor's own
/// committed position - a commit, the ledger's seed, or where an earlier
/// rewind landed. When either is there, the broker is not consulted: the
/// group's committed offset is written by any engine that reaches
/// `commit_progress`, including one whose claim on the batch has moved on, and
/// Kafka cannot refuse it (the adapter reads with `assign()`, so the commit
/// carries no group generation). A stale commit that lands AHEAD of a range
/// recovery is about to re-read would clamp that rewind past the range, and
/// the range would never be archived: the clamp must come from the fenced
/// ledger, not from the broker.
///
/// That includes an in-memory position alone. It exists without `known` only
/// after a rewind of a partition the ledger does not know, and that rewind
/// already took the broker's offset into account; asking again would let a
/// stale commit made since raise the clamp past the range being re-read
/// (review of #614 - recovery rewinds every range twice).
///
/// Only a partition this process knows nothing about - no committed row in the
/// ledger, nothing committed or rewound this session - falls back to the
/// broker, as every partition did before.
fn rewind_frontier(known: Option<i64>, in_memory: Option<i64>, broker: Option<i64>) -> Option<i64> {
    match (known, in_memory) {
        (Some(k), Some(m)) => Some(k.max(m)),
        (Some(k), None) => Some(k),
        (None, Some(m)) => Some(m),
        (None, None) => broker,
    }
}

/// Records that retention removed between where this cursor wanted to resume
/// and the oldest offset the broker still holds (#361).
///
/// The comparison is against the partition's LOW WATERMARK, read from the
/// broker, and not against anything inferred from delivered offsets. A jump in
/// what arrives is not evidence: compaction produces one, a recreated topic
/// produces one, and a first assignment produces one — none of them is data
/// lost, and a heuristic that cannot tell them apart would cry wolf until it
/// was ignored.
///
/// `None` for a partition with no committed cursor. Nothing has been read, so
/// nothing can have been missed, and reporting a "gap" the size of the whole
/// topic on first assignment is the fastest way to make this signal useless.
fn retention_gap(desired_offset: Option<i64>, low_watermark: i64) -> Option<u64> {
    let desired = desired_offset?;
    // A NEGATIVE OFFSET IS A SENTINEL, NOT A POSITION. librdkafka spells
    // Beginning, End, Stored and Invalid as negative numbers, and subtracting
    // one from a watermark produces a gap out of nothing — the first draft of
    // this claimed to handle that in a comment and did not, which a standalone
    // check of the arithmetic caught before CI did.
    if desired < 0 {
        return None;
    }
    if desired >= low_watermark {
        return None;
    }
    // Saturating for the same reason: the subtraction must not be able to
    // wrap, however absurd its inputs.
    Some(low_watermark.saturating_sub(desired).max(0) as u64)
}

/// The portion of a retention gap that has NOT been counted before (#361).
///
/// The periodic check re-observes a gap for as long as the cursor stays
/// behind the low watermark, and a counter fed the full gap on every
/// observation would double-count the same lost records once a minute —
/// `reported_through` is the watermark loss has already been counted to, so
/// only the watermark's advance beyond it (and beyond the cursor) is new.
///
/// The warn line deliberately still reports the FULL current gap: an
/// operator deciding where to resume needs the whole distance, while a
/// monotonic counter needs each record counted exactly once. Two questions,
/// two numbers.
fn newly_lost(
    desired_offset: Option<i64>,
    low_watermark: i64,
    reported_through: Option<i64>,
) -> u64 {
    // No gap at all (caught up, sentinel offset, or no cursor) counts
    // nothing, whatever was reported before.
    if retention_gap(desired_offset, low_watermark).is_none() {
        return 0;
    }
    let desired = desired_offset.expect("retention_gap returned Some only with a desired offset");
    let counted_from = reported_through.unwrap_or(desired).max(desired);
    low_watermark.saturating_sub(counted_from).max(0) as u64
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

/// Records accumulated for one (topic, partition) during a poll pass.
#[derive(Debug)]
struct PartAcc {
    records: Vec<Vec<u8>>,
    start_offset: i64,
    end_offset: i64,
    /// Earliest and latest message timestamp (ms) among the kept records, for
    /// the batch's event-time range (#559). Producer timestamps arrive out of
    /// order, so these are a minimum and a maximum, not the first and last.
    min_ts_ms: Option<i64>,
    max_ts_ms: Option<i64>,
}

/// Record/byte usage of one topic during a poll pass.
#[derive(Debug, Default)]
struct TopicUsage {
    records: usize,
    bytes: usize,
}

/// Demultiplexes one poll pass over MANY topics into per-(topic, partition)
/// accumulators, with the `max_records` / `max_bytes` budgets enforced PER
/// TOPIC — that is the trait contract ("per-source budgets"), and it is what
/// bounds memory: an aggregate budget would let one busy topic consume the
/// whole topic-count multiple (29 topics x 100 MiB = ~2.9 GiB in one unit).
///
/// Topics are identified by their INDEX into the pass's source slice, not by
/// name: the poll loop handles every message, and a `String` key would mean an
/// allocation per message on the read-throughput path. The caller resolves
/// names to indices once via its topic map and back again during demux.
///
/// A message pushed to a topic that is already at budget is REJECTED, not
/// truncated: cursors advance only over kept records, so rejected offsets are
/// simply re-read on the next pass. Rejection is monotonic within a pass (a
/// full topic never becomes un-full), so a partition's kept offsets are always
/// contiguous.
///
/// Kept free of rdkafka types so the demux/budget logic is unit-testable
/// without a broker.
#[derive(Debug)]
struct MultiTopicAcc {
    parts: HashMap<(usize, i32), PartAcc>,
    usage: Vec<TopicUsage>,
    full: Vec<bool>,
    full_topics: usize,
    total_records: usize,
    max_records: usize,
    max_bytes: usize,
}

impl MultiTopicAcc {
    fn new(n_topics: usize, max_records: usize, max_bytes: usize) -> Self {
        Self {
            parts: HashMap::new(),
            usage: (0..n_topics).map(|_| TopicUsage::default()).collect(),
            full: vec![false; n_topics],
            full_topics: 0,
            total_records: 0,
            max_records,
            max_bytes,
        }
    }

    /// Keep `payload` for `(topic_idx, partition)` unless the topic is at
    /// budget. Returns whether the message was kept.
    fn push(&mut self, topic_idx: usize, partition: i32, offset: i64, payload: Vec<u8>) -> bool {
        if self.full[topic_idx] {
            return false;
        }
        let usage = &mut self.usage[topic_idx];
        if usage.records >= self.max_records
            || payload.len() > self.max_bytes.saturating_sub(usage.bytes)
        {
            self.full[topic_idx] = true;
            self.full_topics += 1;
            return false;
        }
        usage.records += 1;
        usage.bytes += payload.len();
        if usage.records >= self.max_records || usage.bytes >= self.max_bytes {
            self.full[topic_idx] = true;
            self.full_topics += 1;
        }
        self.total_records += 1;
        let acc = self
            .parts
            .entry((topic_idx, partition))
            .or_insert_with(|| PartAcc {
                records: Vec::new(),
                start_offset: offset,
                end_offset: offset,
                min_ts_ms: None,
                max_ts_ms: None,
            });
        acc.end_offset = offset;
        acc.records.push(payload);
        true
    }

    /// Fold a KEPT message's timestamp into its partition's event-time range.
    /// A negative value is Kafka's "no timestamp" and is ignored.
    fn note_timestamp(&mut self, topic_idx: usize, partition: i32, ts_ms: i64) {
        if ts_ms < 0 {
            return;
        }
        if let Some(acc) = self.parts.get_mut(&(topic_idx, partition)) {
            acc.min_ts_ms = Some(acc.min_ts_ms.map_or(ts_ms, |m| m.min(ts_ms)));
            acc.max_ts_ms = Some(acc.max_ts_ms.map_or(ts_ms, |m| m.max(ts_ms)));
        }
    }

    /// Whether this topic has reached its budget for the pass.
    fn is_full(&self, topic_idx: usize) -> bool {
        self.full[topic_idx]
    }

    /// Discard a topic's accumulated prefix after a source-local failure.
    /// Other topics in the multiplexed pass remain independently returnable.
    fn fail_topic(&mut self, topic_idx: usize) {
        let keys = self
            .parts
            .keys()
            .filter(|(idx, _)| *idx == topic_idx)
            .copied()
            .collect::<Vec<_>>();
        for key in keys {
            self.parts.remove(&key);
        }
        self.total_records = self
            .total_records
            .saturating_sub(self.usage[topic_idx].records);
        self.usage[topic_idx] = TopicUsage::default();
        if !self.full[topic_idx] {
            self.full[topic_idx] = true;
            self.full_topics += 1;
        }
    }

    /// True once every one of the `assigned_topics` topics that can receive
    /// messages has reached its budget — the pass has nothing left to gain
    /// from polling.
    fn all_full(&self, assigned_topics: usize) -> bool {
        assigned_topics > 0 && self.full_topics >= assigned_topics
    }

    /// Drain into `(topic_idx, partition, acc)` tuples, ordered for
    /// determinism.
    fn into_sorted(self) -> Vec<(usize, i32, PartAcc)> {
        let mut out: Vec<(usize, i32, PartAcc)> = self
            .parts
            .into_iter()
            .map(|((t, p), acc)| (t, p, acc))
            .collect();
        out.sort_by_key(|&(t, p, _)| (t, p));
        out
    }
}

/// Passes in a row an assigned partition must deliver nothing before the
/// stall check looks at it (#623).
///
/// Counted in passes AND in time (`STALL_AFTER`), because neither alone means
/// "the consumer kept asking and got nothing": a pass count alone is reached
/// in under a second with short poll windows, and wall time alone is reached
/// when the engine simply stopped reading for a while (backpressure).
const STALL_PASSES: u32 = 8;

/// How long an assigned partition must deliver nothing before the stall check
/// looks at it (#623). See `STALL_PASSES`.
const STALL_AFTER: Duration = Duration::from_secs(10);

/// The most often the stall check asks the broker (#623). It asks only when a
/// partition has been idle for `STALL_PASSES` and `STALL_AFTER`, so a busy
/// consumer never asks at all, and an idle one asks once per interval.
const STALL_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// A run of passes in which one partition delivered nothing.
#[derive(Debug, Clone, Copy)]
struct IdleRun {
    passes: u32,
    since: Instant,
}

/// Per-partition liveness, for finding a partition that has stopped being
/// read while the broker holds records for it (#623).
///
/// The adapter's assignment is sticky, so nothing else re-assigns a partition
/// whose fetcher has died: a fresh assign() happens only when the topic set
/// changes, a record is rejected, or a poll fails. Before this, the only other
/// retry re-assigned EVERY partition after any pass that read nothing while a
/// partition assigned at `Offset::Stored` had not delivered yet. That missed a
/// dead partition while other partitions had traffic, and it could stop the
/// whole consumer: each re-assign restarts every fetcher, the restart does not
/// finish within a short poll window, so that pass also reads nothing, and the
/// next pass re-assigns again, until a restart.
///
/// Kept free of rdkafka types, and the clock is passed in, so the decisions
/// are unit-testable without a broker.
#[derive(Debug, Default)]
struct PartitionLiveness {
    /// Assigned partitions that delivered nothing in the passes since their
    /// last delivery or the last assign().
    idle: HashMap<(String, i32), IdleRun>,
    /// When the stall check last asked the broker.
    last_check: Option<Instant>,
    /// The high watermark at which each partition was last re-assigned for a
    /// stall. A partition whose re-assign did not help, with no new records
    /// since, holds records it cannot deliver (a transaction marker at the
    /// end of the log, for example), not a dead fetcher, and re-assigning it
    /// again would only restart every fetcher once per interval for nothing.
    reassigned_at_high: HashMap<(String, i32), i64>,
}

impl PartitionLiveness {
    /// A fresh assign() restarts every fetcher, so every idle run starts over.
    fn on_assign(&mut self, assignment: &[(String, i32)]) {
        self.idle.clear();
        self.reassigned_at_high
            .retain(|key, _| assignment.binary_search(key).is_ok());
    }

    /// Record one completed pass over `assigned`, the set given to the last
    /// `on_assign`. A partition progressed if it delivered anything (kept or
    /// rejected: either way its fetcher is alive) or if its topic filled its
    /// budget, which leaves a partition crowded out rather than stalled.
    ///
    /// Runs on every pass, so it allocates only when a partition starts an
    /// idle run.
    fn record_pass(
        &mut self,
        assigned: &[(String, i32)],
        progressed: impl Fn(&(String, i32)) -> bool,
        now: Instant,
    ) {
        for key in assigned {
            if progressed(key) {
                self.idle.remove(key);
            } else if let Some(run) = self.idle.get_mut(key) {
                run.passes += 1;
            } else {
                self.idle.insert(
                    key.clone(),
                    IdleRun {
                        passes: 1,
                        since: now,
                    },
                );
            }
        }
    }

    /// The partitions idle long enough to ask the broker about, sorted; empty
    /// when the check ran less than `STALL_CHECK_INTERVAL` ago.
    fn due(&mut self, now: Instant) -> Vec<(String, i32)> {
        if self
            .last_check
            .is_some_and(|at| now.saturating_duration_since(at) < STALL_CHECK_INTERVAL)
        {
            return Vec::new();
        }
        let mut due: Vec<(String, i32)> = self
            .idle
            .iter()
            .filter(|(_, run)| {
                run.passes >= STALL_PASSES
                    && now.saturating_duration_since(run.since) >= STALL_AFTER
            })
            .map(|(key, _)| key.clone())
            .collect();
        if !due.is_empty() {
            self.last_check = Some(now);
        }
        due.sort();
        due
    }

    /// The lag of a due partition that is stalled: the broker holds records at
    /// or past `next`, the offset it would read next, and it was not already
    /// re-assigned at this high watermark. `None` when `next` is unknown.
    fn stalled_lag(&self, key: &(String, i32), next: Option<i64>, high: i64) -> Option<u64> {
        let next = next.filter(|n| *n >= 0)?;
        if high <= next || self.reassigned_at_high.get(key) == Some(&high) {
            return None;
        }
        Some(high.saturating_sub(next) as u64)
    }

    fn note_reassigned(&mut self, key: &(String, i32), high: i64) {
        self.reassigned_at_high.insert(key.clone(), high);
    }
}

/// The offset a partition would read next, for the stall check (#623): where
/// it last delivered up to, or else where `Offset::Stored` resolves - the
/// group's committed offset, or the low watermark when nothing is committed
/// and `auto.offset.reset` starts at the beginning. `None` when none of these
/// is known: a partition that starts at the END of the log has no backlog to
/// measure until it delivers.
fn next_read_offset(
    delivered: Option<i64>,
    group_committed: Option<i64>,
    low_watermark: Option<i64>,
    reset_to_earliest: bool,
) -> Option<i64> {
    delivered
        .or(group_committed)
        .or(low_watermark.filter(|_| reset_to_earliest))
}

/// Whether `auto.offset.reset` starts a partition with nothing committed at
/// the beginning of the log (librdkafka's spellings).
fn resets_to_earliest(auto_offset_reset: &str) -> bool {
    matches!(
        auto_offset_reset.to_ascii_lowercase().as_str(),
        "earliest" | "smallest" | "beginning"
    )
}

/// The offset at `at` (`Offset::Beginning` or `Offset::End`, i.e. the low or
/// high watermark) for each of `partitions`, in one bounded ListOffsets call
/// per leader. A partition the broker did not answer for is absent.
fn list_offsets(
    consumer: &BaseConsumer,
    partitions: &[(String, i32)],
    at: Offset,
) -> HashMap<(String, i32), i64> {
    let mut answers = HashMap::new();
    if partitions.is_empty() {
        return answers;
    }
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        if tpl.add_partition_offset(topic, *partition, at).is_err() {
            return answers;
        }
    }
    let Ok(listed) =
        consumer.offsets_for_times(tpl, Duration::from_secs(WATERMARK_TIMEOUT_SECONDS))
    else {
        return answers;
    };
    for (topic, partition) in partitions {
        if let Some(element) = listed.find_partition(topic, *partition) {
            if let (Ok(()), Offset::Offset(offset)) = (element.error(), element.offset()) {
                answers.insert((topic.clone(), *partition), offset);
            }
        }
    }
    answers
}

pub struct KafkaSource {
    cfg: KafkaSourceConfig,
    format: TelemetryFormat,
    include: Regex,
    exclude: Regex,
    consumer: Option<BaseConsumer>,
    // key: (topic, partition)
    cursors: HashMap<(String, i32), PartitionCursor>,
    /// Where each partition resumes according to the LEDGER, for this
    /// consumer group: seeded from its committed rows at startup
    /// (`seed_committed_from_ledger`) and raised by every commit this process
    /// makes (#614). A partition in here never asks the broker for its
    /// frontier (see `rewind_frontier`).
    known_committed: HashMap<(String, i32), i64>,
    /// The consumer group's committed offset as this process last read it
    /// from the broker or wrote it there. Only used to put the group's offset
    /// back in step with the ledger, for lag monitoring (#614); nothing here
    /// decides where a partition is read from.
    group_at: HashMap<(String, i32), i64>,
    /// The (topic, partition) set currently assigned on the consumer, sorted.
    ///
    /// Assignment is STICKY: calling assign() resets librdkafka's fetchers,
    /// and with a short poll window the fetch restart eats most of the pass —
    /// measured at ~300 records per 250ms pass against a 3M-record backlog.
    /// While the set is unchanged the consumer stays assigned and fetchers
    /// stay warm; only diverged partitions get an explicit seek().
    assigned: Vec<(String, i32)>,
    /// Next offset librdkafka will deliver per partition (last polled + 1),
    /// including messages the accumulator REJECTED. Where this diverges from
    /// the cursor's desired offset, the pass seeks that partition back.
    delivered: HashMap<(String, i32), i64>,
    /// When retention was last checked (#361). Bounds a broker round trip on
    /// a read path to something retention's own timescale makes free.
    last_retention_check: Option<std::time::Instant>,
    /// Low watermark per (topic, partition) up to which retention loss has
    /// already been COUNTED (#361). Never cleared on re-assignment — it
    /// tracks a broker-side fact, not assignment state, and clearing it
    /// would re-count the same lost records after every rebalance — but
    /// pruned with the live topic set (`prune_partition_cache`): the
    /// bookmark lives exactly as long as the broker-side subject it
    /// describes.
    reported_lost_through: HashMap<(String, i32), i64>,
    /// Retention loss observed but not yet delivered, per topic (#361). The
    /// bookmark above dedupes OBSERVATION; this accumulator guarantees
    /// DELIVERY: the count reaches the engine only on a pass that returns
    /// `Ok`, the check runs before the poll loop, and the bookmark scores a
    /// re-observed gap zero — so loss observed in a pass that then failed
    /// would otherwise never be counted at all. Whatever a check observes is
    /// owed to the counter, and the first pass that completes pays it.
    /// Deliberately NOT pruned with dead topics: observed loss stays owed
    /// even if its topic died. BTreeMap so delivery order is deterministic.
    pending_lost: BTreeMap<String, u64>,
    /// Partitions paused mid-pass (topic hit its budget); resumed at the
    /// start of the next pass.
    paused: Vec<(String, i32)>,
    /// Which assigned partitions have stopped delivering, and since when
    /// (#623).
    ///
    /// Sticky assignment never restarts a fetcher on its own, and a fetcher
    /// can die: `Offset::Stored` resolution needs the group coordinator, which
    /// a FRESH broker creates lazily on the first group operation, and an
    /// assign() that lands before then leaves dead fetchers. The stall check
    /// (`check_stalled_partitions`) re-assigns when a partition has delivered
    /// nothing for `STALL_PASSES` passes and `STALL_AFTER` while the broker
    /// holds records for it, whatever the other partitions do.
    liveness: PartitionLiveness,
    /// Re-assigns the stall check has made in this process, for the log.
    stall_reassigns: u64,
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
            known_committed: HashMap::new(),
            group_at: HashMap::new(),
            assigned: Vec::new(),
            delivered: HashMap::new(),
            last_retention_check: None,
            reported_lost_through: HashMap::new(),
            pending_lost: BTreeMap::new(),
            paused: Vec::new(),
            liveness: PartitionLiveness::default(),
            stall_reassigns: 0,
            partitions: HashMap::new(),
        })
    }

    /// Say what retention took, for every assigned partition (#361).
    ///
    /// PERIODIC, NOT ASSIGNMENT-TRIGGERED, and that is the second design this
    /// had. Hooking it to a changed assignment missed both cases it exists
    /// for (review):
    ///
    /// - **After a restart** the desired offset is `Offset::Stored` — the
    ///   group commit is librdkafka's to resolve — so a guard that only
    ///   accepted a concrete offset skipped the primary case entirely.
    /// - **While lagging** the assignment never changes. Retention cannot
    ///   overtake a cursor between two consecutive passes, which is true and
    ///   beside the point: it does so over hundreds of them.
    ///
    /// One mechanism covers both. The offset compared is the one this source
    /// would read NEXT — the in-memory cursor where there is one, the group's
    /// committed offset otherwise — which is exactly the position retention
    /// can have moved past.
    ///
    /// Reports; does not decide. Whether the adapter should REFUSE to resume
    /// past a gap is a deployment question — refusing turns a partial archive
    /// into no archive — and it stays on #361.
    ///
    /// Never fails the pass. A broker that will not answer has not lost
    /// anything; it has only declined to say, and turning that into a read
    /// failure would trade a real archive for a diagnostic.
    ///
    /// Accumulates the records NEWLY observed lost into `pending_lost`, per
    /// topic — deduplicated against what previous checks already counted, so
    /// the counter fed downstream counts each record exactly once however
    /// long the gap persists. Per topic and not one sum, because the topic
    /// names the stream and the stream names the tenant the loss belongs to.
    /// Observation and delivery are split on purpose: the count reaches the
    /// engine only via a pass that returns `Ok`, and this pass can still
    /// fail after the check ran — the accumulator holds the debt until a
    /// pass completes (see `pending_lost`). Passes where the interval gate
    /// skips the check accumulate nothing, which is "nothing new observed",
    /// not "nothing lost".
    fn report_retention_gaps(&mut self) {
        // Retention moves on the scale of hours. Once a minute is free, and
        // an unbounded network call on a read path is the mistake #374 was
        // made of — so both the interval and each query are bounded.
        if self
            .last_retention_check
            .is_some_and(|at| at.elapsed() < RETENTION_CHECK_INTERVAL)
        {
            return;
        }
        self.last_retention_check = Some(std::time::Instant::now());

        let Some(consumer) = self.consumer.as_ref() else {
            return;
        };
        // ONE call for every partition, rather than one per partition: the
        // group's committed offsets are what a restart would resume from.
        let committed = consumer
            .committed(std::time::Duration::from_secs(WATERMARK_TIMEOUT_SECONDS))
            .ok();

        let mut newly_reported: Vec<((String, i32), i64)> = Vec::new();
        for (topic, partition) in &self.assigned {
            // The in-memory cursor is what this process would read next; the
            // group commit is what a fresh one would. Whichever exists is the
            // position retention can have moved past.
            let next_offset = self
                .delivered
                .get(&(topic.clone(), *partition))
                .copied()
                .or_else(|| {
                    committed
                        .as_ref()
                        .and_then(|tpl| tpl.find_partition(topic, *partition))
                        .and_then(|elem| match elem.offset() {
                            Offset::Offset(at) => Some(at),
                            _ => None,
                        })
                });
            let Ok((low_watermark, _high)) = consumer.fetch_watermarks(
                topic,
                *partition,
                std::time::Duration::from_secs(WATERMARK_TIMEOUT_SECONDS),
            ) else {
                continue;
            };
            if let Some(lost) = retention_gap(next_offset, low_watermark) {
                let key = (topic.clone(), *partition);
                let newly = newly_lost(
                    next_offset,
                    low_watermark,
                    self.reported_lost_through.get(&key).copied(),
                );
                if newly > 0 {
                    *self.pending_lost.entry(topic.clone()).or_insert(0) += newly;
                }
                newly_reported.push((key, low_watermark));
                tracing::warn!(
                    topic = %topic,
                    partition = *partition,
                    resuming_from = next_offset.unwrap_or_default(),
                    oldest_available = low_watermark,
                    records_lost = lost,
                    "retention removed records this source had not read: the commit rule \
                     covers records the engine read, and these were never read, never \
                     manifested and never verified. Size retention above worst-case lag \
                     and monitor consumer lag independently (#361)"
                );
            }
        }
        // Recorded with max(), never overwritten downward: a broker answer
        // whose watermark went backwards (leader change, absurd input) must
        // not re-open loss that was already counted once.
        for (key, low_watermark) in newly_reported {
            let entry = self
                .reported_lost_through
                .entry(key)
                .or_insert(low_watermark);
            *entry = (*entry).max(low_watermark);
        }
    }

    /// Convert everything observed-but-undelivered into report entries,
    /// leaving the accumulator empty. Called ONLY on paths that return a
    /// successful report — the empty-assignment early return included,
    /// since a debt whose topic was deleted has no polling pass left to
    /// wait for. A drain on a failing path is exactly the dropped count
    /// `pending_lost` exists to prevent.
    fn take_pending_losses(&mut self) -> Vec<SourceRetentionLoss> {
        std::mem::take(&mut self.pending_lost)
            .into_iter()
            .map(|(source_name, records)| SourceRetentionLoss {
                source_name,
                records,
            })
            .collect()
    }

    /// Resume each partition where the LEDGER says it was committed, not where
    /// the consumer group's committed offset on the broker says (#614).
    ///
    /// `committed` is the ledger's newest committed batch per (topic,
    /// partition, consumer group), as `(topic, partition, consumer_group,
    /// end_offset)`. Rows of another consumer group are ignored: this group
    /// resumes from its own progress, as `foreign_group` requires (#563).
    ///
    /// Why not the group's committed offset: the adapter reads with
    /// `assign()`, so it is never a group member and its commits carry no
    /// generation Kafka could check. An engine that stalled past its claim
    /// and resumed can still commit, after the batch was reclaimed and
    /// committed further by another engine - Kafka accepts it, and only the
    /// ledger's write is fenced (#557). Resuming from the broker then
    /// re-reads everything after the stale offset, or, if the stale offset is
    /// ahead of a range that failed its re-check, skips that range for good.
    /// The ledger records only commits whose claim was still held, so neither
    /// can happen when it decides. The group's offset is still committed, for
    /// lag monitoring.
    ///
    /// The ledger is NOT trusted for a partition whose group has no committed
    /// offset at all (or when the broker cannot be asked): that partition
    /// follows `auto.offset.reset`, exactly as before. Deleting a topic
    /// deletes its group offsets, and the ledger, which names topics and not
    /// topic ids, cannot tell a recreated topic from the old one - its
    /// offset would skip the new topic's first records. A stale commit cannot
    /// cause this case: it would create an offset, not remove one.
    ///
    /// Call before any read. A partition this process has already read or
    /// committed keeps its in-memory position.
    pub fn seed_committed_from_ledger<'a>(
        &mut self,
        committed: impl IntoIterator<Item = (&'a str, i32, &'a str, i64)>,
    ) {
        let own: Vec<((String, i32), i64)> = committed
            .into_iter()
            .filter(|(_, _, group, _)| *group == self.cfg.consumer_group)
            .map(|(topic, partition, _, end)| ((topic.to_owned(), partition), end))
            .collect();
        if own.is_empty() {
            return;
        }
        let partitions: Vec<(String, i32)> = own.iter().map(|(key, _)| key.clone()).collect();
        let broker = self.group_committed_offsets(&partitions);
        for ((topic, partition), end) in own {
            let resume_at = end.saturating_add(1);
            let key = (topic, partition);
            let Some(&group_at) = broker.get(&key) else {
                tracing::warn!(
                    topic = %key.0,
                    partition,
                    ledger_resume_at = resume_at,
                    group = %self.cfg.consumer_group,
                    "the ledger has a resume point for this partition, but the consumer group has \
                     no committed offset for it (or the broker could not be asked): following \
                     auto.offset.reset, because a deleted and recreated topic looks exactly like \
                     this and the ledger's offset would skip its first records"
                );
                continue;
            };
            self.group_at.insert(key.clone(), group_at);
            if group_at < resume_at {
                tracing::warn!(
                    topic = %key.0,
                    partition,
                    ledger_resume_at = resume_at,
                    group_committed = group_at,
                    "the consumer group's committed offset is BEHIND the ledger (a commit the \
                     ledger did not accept, or an older range committed after a newer one): \
                     resuming from the ledger instead of re-reading archived records, and \
                     moving the group's offset up to it"
                );
                self.advance_group_offset(&key, resume_at);
            } else if group_at > resume_at {
                tracing::info!(
                    topic = %key.0,
                    partition,
                    ledger_resume_at = resume_at,
                    group_committed = group_at,
                    "the consumer group's committed offset is ahead of the ledger (a commit whose \
                     ledger write did not land): resuming from the ledger; recovery decides the \
                     range in between"
                );
            }
            self.known_committed.insert(key.clone(), resume_at);
            let cur = self.cursors.entry(key).or_default();
            if cur.last_read_offset.is_none() && cur.committed_offset.is_none() {
                cur.committed_offset = Some(resume_at);
            }
        }
    }

    /// Move the group's committed offset UP to `to`, the ledger's frontier,
    /// so lag monitoring stops reporting archived records as unread (#614).
    ///
    /// Only forward, and only to a frontier the ledger (or this process's own
    /// commits) holds as committed: everything below it is archived and
    /// verified, so this commits nothing the commit rule forbids. A stale
    /// engine can still move the offset back afterwards; that is cosmetic now,
    /// and the next restart puts it right again. A failure is logged, never
    /// returned: the group's offset decides nothing.
    fn advance_group_offset(&mut self, key: &(String, i32), to: i64) {
        let result = self.consumer().and_then(|consumer| {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&key.0, key.1, Offset::Offset(to))
                .map_err(|e| VtopError::Source(format!("tpl: {e}")))?;
            consumer
                .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
                .map_err(|e| VtopError::Source(format!("kafka commit: {e}")))
        });
        match result {
            Ok(()) => {
                self.group_at.insert(key.clone(), to);
            }
            Err(e) => tracing::warn!(
                topic = %key.0,
                partition = key.1,
                to,
                error = %e,
                "could not move the consumer group's offset up to the ledger; lag monitoring \
                 reads it behind until the next commit"
            ),
        }
    }

    /// Re-assign when an assigned partition has stopped being read while the
    /// broker holds records for it, whatever the other partitions do (#623).
    ///
    /// Runs after a completed pass, and asks the broker only for partitions
    /// that have delivered nothing for `STALL_PASSES` passes and
    /// `STALL_AFTER`, at most once per `STALL_CHECK_INTERVAL`: a consumer
    /// whose partitions all deliver never asks at all. At most three bounded
    /// calls, each one request per leader for every due partition: the high
    /// watermarks, then the group's committed offsets and the low watermarks
    /// for a partition that has not delivered since `Offset::Stored`.
    ///
    /// A partition is stalled when it would read next below the high
    /// watermark. A stalled partition is logged and the whole assignment is
    /// re-assigned on the next pass, which restarts its fetcher; that costs
    /// every fetcher one restart, and happens at most once per interval.
    /// Never fails the pass: a broker that does not answer leaves things as
    /// they are until the next check.
    fn check_stalled_partitions(&mut self) {
        let due = self.liveness.due(Instant::now());
        if due.is_empty() {
            return;
        }
        let Some(consumer) = self.consumer.as_ref() else {
            return;
        };
        let highs = list_offsets(consumer, &due, Offset::End);
        let reset_to_earliest = resets_to_earliest(&self.cfg.auto_offset_reset);
        // Where Offset::Stored resolved is not known here for a partition
        // that has not delivered since the assign: ask the group, then the
        // low watermark when the group has committed nothing.
        let unresolved: Vec<(String, i32)> = due
            .iter()
            .filter(|key| highs.contains_key(*key) && !self.delivered.contains_key(*key))
            .cloned()
            .collect();
        let committed = self.group_committed_offsets(&unresolved);
        let need_low: Vec<(String, i32)> = if reset_to_earliest {
            unresolved
                .iter()
                .filter(|key| !committed.contains_key(*key))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let lows = match self.consumer.as_ref() {
            Some(consumer) => list_offsets(consumer, &need_low, Offset::Beginning),
            None => HashMap::new(),
        };

        let now = Instant::now();
        let mut stalled = false;
        for key in &due {
            let Some(&high) = highs.get(key) else {
                continue;
            };
            let next = next_read_offset(
                self.delivered.get(key).copied(),
                committed.get(key).copied(),
                lows.get(key).copied(),
                reset_to_earliest,
            );
            let Some(lag) = self.liveness.stalled_lag(key, next, high) else {
                continue;
            };
            let (idle_passes, idle_for) = self
                .liveness
                .idle
                .get(key)
                .map(|run| (run.passes, now.saturating_duration_since(run.since)))
                .unwrap_or_default();
            self.liveness.note_reassigned(key, high);
            self.stall_reassigns += 1;
            stalled = true;
            tracing::warn!(
                topic = %key.0,
                partition = key.1,
                next_offset = next.unwrap_or_default(),
                high_watermark = high,
                lag,
                idle_passes,
                idle_secs = idle_for.as_secs(),
                stall_reassigns_total = self.stall_reassigns,
                "kafka partition re-assigned after stall: it delivered nothing while the broker \
                 holds records for it, so its fetcher is restarted (#623)"
            );
        }
        if stalled {
            self.assigned.clear();
        }
    }

    /// TEST SEAM (#623): stop one partition's fetcher on the underlying
    /// consumer without the adapter knowing, which is what a dead fetcher
    /// looks like from here. Only the live-broker stall test calls this.
    #[doc(hidden)]
    pub fn stop_fetcher_behind_the_adapter(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(), VtopError> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| VtopError::Source("no kafka consumer yet".into()))?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, partition);
        consumer
            .pause(&tpl)
            .map_err(|e| VtopError::Source(format!("pause {topic}-{partition}: {e}")))
    }

    /// The marker's consumer group, when it is not the one this adapter
    /// consumes as.
    fn foreign_group<'m>(&self, marker: &'m ProgressMarker) -> Option<&'m str> {
        match marker {
            ProgressMarker::Kafka { consumer_group, .. }
                if *consumer_group != self.cfg.consumer_group =>
            {
                Some(consumer_group.as_str())
            }
            _ => None,
        }
    }

    /// The group's committed "next offset" for each of `partitions`, from the
    /// broker, in ONE request. A partition with nothing committed, or every
    /// partition when the broker cannot answer within the bound, is simply
    /// absent; callers then fall back to what they already know.
    fn group_committed_offsets(
        &mut self,
        partitions: &[(String, i32)],
    ) -> HashMap<(String, i32), i64> {
        let mut answers = HashMap::new();
        if partitions.is_empty() {
            return answers;
        }
        let Ok(consumer) = self.consumer() else {
            return answers;
        };
        let mut wanted = TopicPartitionList::new();
        let mut seen = std::collections::HashSet::new();
        for (topic, partition) in partitions {
            if seen.insert((topic.as_str(), *partition)) {
                wanted.add_partition(topic, *partition);
            }
        }
        let Ok(committed) = consumer.committed_offsets(
            wanted,
            std::time::Duration::from_secs(WATERMARK_TIMEOUT_SECONDS),
        ) else {
            return answers;
        };
        for (topic, partition) in partitions {
            if let Some(element) = committed.find_partition(topic, *partition) {
                if let Offset::Offset(at) = element.offset() {
                    answers.insert((topic.clone(), *partition), at);
                }
            }
        }
        answers
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

    /// Drop per-topic state for topics that no longer exist, so a broker that
    /// churns through many short-lived topics does not grow the caches
    /// without bound.
    pub fn prune_partition_cache(&mut self, live_topics: &[String]) {
        let live: std::collections::HashSet<&str> =
            live_topics.iter().map(|s| s.as_str()).collect();
        self.partitions
            .retain(|topic, _| live.contains(topic.as_str()));
        // The loss bookmark tracks a broker-side fact, so it lives exactly as
        // long as the broker-side subject: a topic absent from live metadata
        // can never report that gap again, and a topic recreated under the
        // same name is a NEW subject whose loss must be counted from its own
        // cursor — a stale bookmark would score the new topic's real loss
        // zero until its watermark happened to pass it. A delete+recreate
        // falling entirely inside one discovery cycle is indistinguishable
        // with the metadata this API returns (names, not topic ids) and
        // stays a documented gap: treating a watermark regression as a new
        // incarnation instead would re-count counted loss on every routine
        // leader change. `pending_lost` is deliberately NOT pruned — loss a
        // check already observed is owed to the counter even if its topic
        // died.
        self.reported_lost_through
            .retain(|(topic, _), _| live.contains(topic.as_str()));
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
        // One topic is the degenerate case of the multiplexed pass; a single
        // code path keeps the offset-seeding and demux semantics identical.
        let report = self
            .read_all_batch_candidates(
                std::slice::from_ref(source),
                max_records,
                max_bytes,
                max_wait,
            )
            .await?;
        report
            .outcomes
            .into_iter()
            .next()
            .map(|o| o.result)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Read EVERY topic in one pass: assign all (topic, partition) pairs once,
    /// then run a SINGLE poll loop and demultiplex messages by (topic,
    /// partition).
    ///
    /// This replaces per-topic reads on the shared consumer, which paid up to
    /// `max_wait` PER TOPIC serially — measured at 87-92% of the read cycle
    /// waiting on empty topics (~61k rec/s while reading vs ~12.7k observed,
    /// #96). One pass pays the wait once for the whole topic set, which is how
    /// a Kafka consumer is designed to be used.
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[DiscoveredSource],
        max_records: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<AdapterReadReport, VtopError> {
        let group = self.cfg.consumer_group.clone();
        let started = std::time::Instant::now();

        // Demux target: topic name -> index into `sources`.
        let topic_index: HashMap<&str, usize> = sources
            .iter()
            .enumerate()
            .map(|(i, s)| (s.source_name.as_str(), i))
            .collect();

        // ASSIGN every topic's partitions at our tracked offsets. We
        // deliberately use assign() rather than subscribe(): a subscribe() on
        // every read triggers a consumer-group rebalance, and because the
        // engine commits offsets only AFTER verification there is no committed
        // offset to resume from — so each rebalance reseeks to the configured
        // reset (earliest) and the short poll window is spent rebalancing
        // instead of fetching. assign() is rebalance-free and lets us control
        // the exact start offset per partition.
        self.consumer()?;
        // A metadata failure for ONE topic must not abort the pass for the
        // others — the serial loop isolated it per source, and so does this:
        // the failing topic is skipped and its outcome carries the error.
        let mut source_errors: Vec<Option<VtopError>> = Vec::with_capacity(sources.len());
        // Topics that contributed at least one partition to the assignment —
        // the only topics that can ever fill up, so the count the poll loop's
        // all-full early exit must compare against.
        let mut assigned_topics: usize = 0;
        // Partition lists aligned with `sources`, kept so a topic that fills
        // its budget mid-pass can be pause()d without refetching metadata.
        let mut partitions_by_source: Vec<Vec<i32>> = Vec::with_capacity(sources.len());
        // (topic, partition, desired next offset) for every readable partition,
        // plus the sorted set for the sticky-assignment comparison.
        let mut desired: Vec<(String, i32, Offset)> = Vec::new();
        let mut assignment_set: Vec<(String, i32)> = Vec::new();
        for source in sources {
            let topic = &source.source_name;
            // A topic with no partitions (e.g. just deleted) simply contributes
            // nothing to the assignment; its outcome will be empty.
            let partitions = match self.partitions_for(topic) {
                Ok(p) => {
                    source_errors.push(None);
                    p
                }
                Err(e) => {
                    source_errors.push(Some(e));
                    partitions_by_source.push(Vec::new());
                    continue;
                }
            };
            if !partitions.is_empty() {
                assigned_topics += 1;
            }
            partitions_by_source.push(partitions.clone());
            for p in partitions {
                let cur = self.cursors.entry((topic.clone(), p)).or_default();
                let off = match cur.last_read_offset {
                    // Continue from our in-session read head.
                    Some(lr) => Offset::Offset(lr + 1),
                    None => match cur.committed_offset {
                        // Resume from a position we committed this session,
                        // a rewind, or the ledger's resume point (#614).
                        Some(c) => Offset::Offset(c),
                        // Nothing in memory (fresh process / first read for
                        // this partition): resolve the GROUP'S COMMITTED offset
                        // from the broker, falling back to `auto.offset.reset`
                        // when the group has never committed. Only a partition
                        // the ledger did not seed gets here: a fresh
                        // deployment, a partition with no committed batch, or
                        // one whose group offset is gone (#614,
                        // `seed_committed_from_ledger`).
                        //
                        // This must not be Offset::Beginning: subscribe() used
                        // to resume from the committed offset implicitly, and
                        // assign() does not. Starting at Beginning would make
                        // every engine restart re-read the topic from the start
                        // and re-archive already-committed records.
                        None => Offset::Stored,
                    },
                };
                desired.push((topic.clone(), p, off));
                assignment_set.push((topic.clone(), p));
            }
        }
        assignment_set.sort();
        // Time spent resolving metadata is FAILED time when any topic errored:
        // a broker that times out metadata fetches is exactly the wait
        // `read_cycle_profile` exists to expose, and classifying it as
        // productive/empty would hide it.
        let meta_ms = started.elapsed().as_millis() as u64;
        let meta_failed_ms = if source_errors.iter().any(Option::is_some) {
            meta_ms
        } else {
            0
        };

        // Nothing to assign (no sources, or every topic is partitionless):
        // return without polling. The serial path never waited in this case,
        // and blocking `max_wait` on an empty assignment would turn an idle
        // adapter into a fixed per-cycle stall.
        if assignment_set.is_empty() {
            let elapsed = started.elapsed().as_millis() as u64;
            return Ok(AdapterReadReport {
                outcomes: source_errors
                    .into_iter()
                    .enumerate()
                    .map(|(source_index, err)| SourceReadOutcome {
                        source_index,
                        result: match err {
                            Some(e) => Err(e),
                            None => Ok(Vec::new()),
                        },
                    })
                    .collect(),
                productive_ms: 0,
                empty_ms: elapsed.saturating_sub(meta_failed_ms),
                failed_ms: meta_failed_ms,
                // The retention check has not run on this path — nothing is
                // assigned, so there is no cursor a watermark could have
                // passed. But this IS a successful report, and loss a
                // PREVIOUS pass observed and failed to deliver must not wait
                // for a pass that polls: if the indebted topic was deleted,
                // no such pass ever comes, and the debt would outlive its
                // subject unpaid. Any pass that completes pays it.
                retention_lost: self.take_pending_losses(),
            });
        }
        let consumer = self.consumer.as_ref().expect("consumer built above");

        // STICKY ASSIGNMENT. assign() tears down librdkafka's fetchers; with a
        // short poll window the restart eats the pass (measured: ~300 records
        // per 250ms pass against a 3M-record backlog when re-assigning every
        // pass). While the (topic, partition) set is unchanged, the consumer
        // stays assigned and fetchers stay warm:
        //   - partitions paused by a previous pass's budget stop are resumed;
        //   - a partition is seek()ed ONLY where the desired offset diverges
        //     from what librdkafka would deliver next (i.e. after rejected
        //     messages or an explicit replay rewind).
        if self.assigned != assignment_set {
            let mut tpl = TopicPartitionList::new();
            for (topic, p, off) in &desired {
                tpl.add_partition_offset(topic, *p, *off)
                    .map_err(|e| VtopError::Source(format!("assign tpl {topic}-{p}: {e}")))?;
            }
            consumer.assign(&tpl).map_err(|e| {
                VtopError::Source(format!("assign ({} topics): {e}", sources.len()))
            })?;
            // Defensively clear any pause state a retained toppar might carry
            // across the re-assign; a silently-paused partition reads as an
            // empty topic. A failure is logged, not discarded (#623), and does
            // not fail the pass: the partitions it did resume are readable,
            // and one left paused stops delivering, which the stall check
            // re-assigns once records wait for it.
            if let Err(e) = consumer.resume(&tpl) {
                tracing::warn!(
                    error = %e,
                    partitions = desired.len(),
                    "kafka resume after assign failed: a partition left paused is re-assigned \
                     by the stall check once records wait for it (#623)"
                );
            }
            self.liveness.on_assign(&assignment_set);
            self.assigned = assignment_set;
            self.delivered.clear();
            // Seed delivered with the offsets just assigned, so idle
            // partitions (which deliver nothing) are not seek-reset on every
            // subsequent pass — that would re-create the fetcher-restart cost
            // for exactly the partitions that never have data.
            for (topic, p, off) in &desired {
                if let Offset::Offset(want) = off {
                    self.delivered.insert((topic.clone(), *p), *want);
                }
            }
            self.paused.clear();
        } else {
            if !self.paused.is_empty() {
                let mut rtpl = TopicPartitionList::new();
                for (topic, p) in &self.paused {
                    rtpl.add_partition(topic, *p);
                }
                consumer
                    .resume(&rtpl)
                    .map_err(|e| VtopError::Source(format!("resume paused: {e}")))?;
                self.paused.clear();
            }
            for (topic, p, off) in &desired {
                // Only a concrete offset can diverge; Stored means "nothing
                // read or committed this session", which on an unchanged
                // assignment is exactly where the consumer already is.
                let Offset::Offset(want) = off else { continue };
                let at = self.delivered.get(&(topic.clone(), *p)).copied();
                if at != Some(*want) {
                    consumer
                        .seek(topic, *p, *off, Duration::from_secs(5))
                        .map_err(|e| VtopError::Source(format!("seek {topic}-{p}: {e}")))?;
                    self.delivered.insert((topic.clone(), *p), *want);
                }
            }
        }

        // AFTER THE ASSIGNMENT IS SETTLED AND BEFORE ANY POLL (#361). Placed
        // at the top of the pass it ran against an empty `self.assigned` on a
        // restart — examining no partitions at all — and then the first poll
        // advanced `delivered` past the gap, masking the group commit for the
        // life of the process. That is the third time this hook has been in
        // the wrong place, and each time for the same reason: the restart is
        // the case the check exists for, and the restart is the case where the
        // state it reads has not been built yet.
        self.report_retention_gaps();
        // The check takes `&mut self` (it advances its own clock), which ends
        // the borrow the assignment block held — so the poll loop takes its
        // own.
        let consumer = self.consumer.as_ref().expect("consumer built above");

        // Budgets are PER TOPIC (the trait contract's per-source budgets),
        // enforced inside the accumulator. An aggregate budget would let one
        // busy topic monopolise the whole topic-count multiple in a single
        // in-memory unit. Messages for a full topic are skipped and re-read
        // next pass; the loop stops early once every assigned topic is full.
        let mut acc = MultiTopicAcc::new(sources.len(), max_records, max_bytes);
        let mut paused = vec![false; sources.len()];
        // Delivery positions touched THIS pass, keyed by index (no per-message
        // allocation); merged into self.delivered after the loop.
        let mut delivered_local: HashMap<(usize, i32), i64> = HashMap::new();
        // Any record rejected after librdkafka delivered it requires a fresh
        // assignment next pass. A concrete cursor can be seek()ed, but a
        // previously unread partition still uses Offset::Stored; keeping the
        // sticky assignment there would continue after the rejected record.
        let mut must_reassign = false;
        let mut poll_source_failed = false;
        let deadline = std::time::Instant::now() + max_wait;
        loop {
            if acc.all_full(assigned_topics) {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match consumer.poll(remaining) {
                Some(Ok(msg)) => {
                    // A message for a topic we did not assign should be
                    // impossible; dropping it silently would lose data, so
                    // fail loudly (cursors have not advanced — nothing is
                    // abandoned).
                    let Some(&idx) = topic_index.get(msg.topic()) else {
                        // librdkafka did advance its internal delivery head.
                        // Since this pass returns no results, rebuild the next
                        // assignment from VTOP's unchanged cursors.
                        self.assigned.clear();
                        return Err(VtopError::Source(format!(
                            "kafka poll returned unassigned topic {}",
                            msg.topic()
                        )));
                    };
                    let payload = msg.payload().unwrap_or_default();
                    if payload.len() > max_bytes {
                        // librdkafka owns the fetched message buffer, but do
                        // not clone an oversized record into engine memory or
                        // advance its cursor. Fail only this topic: valid
                        // results already accumulated for other topics remain
                        // returnable from the shared pass.
                        source_errors[idx] = Some(VtopError::Source(format!(
                            "Kafka record {}-{}@{} is {} bytes, exceeding max_bytes={max_bytes}",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            payload.len()
                        )));
                        acc.fail_topic(idx);
                        must_reassign = true;
                        poll_source_failed = true;
                    } else {
                        // Track what librdkafka delivered — kept OR rejected —
                        // so a concrete cursor can detect divergence. A
                        // rejected record also invalidates sticky assignment
                        // for Offset::Stored partitions below.
                        delivered_local.insert((idx, msg.partition()), msg.offset() + 1);
                        let kept = acc.push(idx, msg.partition(), msg.offset(), payload.to_vec());
                        if kept {
                            if let Some(ts_ms) = msg.timestamp().to_millis() {
                                acc.note_timestamp(idx, msg.partition(), ts_ms);
                            }
                        }
                        must_reassign |= !kept;
                    }
                    // A full topic keeps DELIVERING otherwise: rdkafka would
                    // go on fetching its tail, and every discarded message is
                    // bandwidth spent on data the next pass refetches anyway.
                    // Pausing its partitions stops the fetcher for the rest of
                    // the pass; the next pass resumes them.
                    if acc.is_full(idx) && !paused[idx] {
                        let topic = &sources[idx].source_name;
                        let mut ptpl = TopicPartitionList::new();
                        for &p in &partitions_by_source[idx] {
                            ptpl.add_partition(topic, p);
                        }
                        if let Err(e) = consumer.pause(&ptpl) {
                            // Fetching a discarded tail is waste, not danger —
                            // never fail the pass over pause.
                            tracing::debug!(topic, error = %e, "pause of full topic failed");
                        }
                        paused[idx] = true;
                    }
                }
                // Cursors are only advanced AFTER the loop, so failing the
                // whole pass here abandons nothing: every accumulated offset is
                // re-read on the next cycle.
                Some(Err(e)) => {
                    // A prior message in this pass may already have advanced
                    // librdkafka beyond VTOP's unchanged cursor. Force the
                    // next pass to assign from the cursor instead of retaining
                    // that delivery position.
                    self.assigned.clear();
                    return Err(VtopError::Source(format!("kafka poll error: {e}")));
                }
                None => break, // no more messages within the wait window
            }
            if remaining.is_zero() {
                break;
            }
        }
        let total_records = acc.total_records;

        if must_reassign {
            self.assigned.clear();
        }

        // Persist the pass's delivery positions and pause state for the next
        // pass's sticky-assignment decisions.
        let progressed: std::collections::HashSet<(usize, i32)> =
            delivered_local.keys().copied().collect();
        for ((idx, p), next) in delivered_local {
            let key = (sources[idx].source_name.clone(), p);
            self.delivered.insert(key, next);
        }
        for (idx, was_paused) in paused.iter().enumerate() {
            if *was_paused {
                for &p in &partitions_by_source[idx] {
                    self.paused.push((sources[idx].source_name.clone(), p));
                }
            }
        }
        // Per-partition liveness (#623). This replaces a retry that
        // re-assigned every partition after ANY pass that read nothing while a
        // partition assigned at Offset::Stored had not delivered yet. That
        // retry never fired while other partitions had traffic, and it could
        // stop the whole consumer: the re-assign restarts every fetcher, the
        // restart does not finish within a 250ms window, so the next pass also
        // read nothing and re-assigned again, until a restart.
        if !self.assigned.is_empty() {
            self.liveness.record_pass(
                &self.assigned,
                |(topic, p)| {
                    topic_index
                        .get(topic.as_str())
                        .is_some_and(|&idx| paused[idx] || progressed.contains(&(idx, *p)))
                },
                Instant::now(),
            );
            self.check_stalled_partitions();
        }

        // Demux into one ReadResult per (topic, partition), routed to the
        // outcome of the source that owns the topic. The ENGINE isolates
        // batches per topic and partition (`buffer_key`), so mixed reads are
        // safe as long as records are handed back grouped.
        let mut per_source: Vec<Vec<ReadResult>> = vec![Vec::new(); sources.len()];
        for (idx, p, part) in acc.into_sorted() {
            let topic = &sources[idx].source_name;
            let cur = self.cursors.entry((topic.clone(), p)).or_default();
            if !part.records.is_empty() {
                cur.last_read_offset = Some(part.end_offset);
            }
            let mk = |s: i64, e: i64| ProgressMarker::Kafka {
                topic: topic.clone(),
                partition: p,
                start_offset: s,
                end_offset: e,
                consumer_group: group.clone(),
            };
            per_source[idx].push(ReadResult {
                progress_start: mk(part.start_offset, part.start_offset),
                progress_end: mk(part.start_offset, part.end_offset.max(part.start_offset)),
                records: part.records,
                first_timestamp: part
                    .min_ts_ms
                    .and_then(vtop_core::event_time::format_millis),
                last_timestamp: part
                    .max_ts_ms
                    .and_then(vtop_core::event_time::format_millis),
                // Kafka messages are newline-framed into the object
                // (offset-based, not byte-exact), so records are re-framed on
                // serialization.
                verbatim: false,
            });
        }

        // One shared wait covered every topic, so the pass's wall-clock beyond
        // the (possibly failed) metadata phase is one bucket: productive if
        // ANY topic yielded, otherwise empty. Splitting it per topic would
        // double-count the shared wait and break the engine's reconciliation
        // of buckets against the phase total.
        let elapsed = started.elapsed().as_millis() as u64;
        let shared = elapsed.saturating_sub(meta_failed_ms);
        let (productive_ms, empty_ms, poll_failed_ms) = if total_records > 0 {
            (shared, 0, 0)
        } else if poll_source_failed {
            (0, 0, shared)
        } else {
            (0, shared, 0)
        };
        // Pay the retention debt HERE and only here: the error returns above
        // drop the report on the floor, and with it any count it carried —
        // the accumulator holds those until a pass gets this far.
        let retention_lost = self.take_pending_losses();
        Ok(AdapterReadReport {
            outcomes: per_source
                .into_iter()
                .zip(source_errors)
                .enumerate()
                .map(|(source_index, (reads, err))| SourceReadOutcome {
                    source_index,
                    result: match err {
                        Some(e) => Err(e),
                        None => Ok(reads),
                    },
                })
                .collect(),
            productive_ms,
            empty_ms,
            failed_ms: meta_failed_ms.saturating_add(poll_failed_ms),
            retention_lost,
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
        // A range read under another consumer group (the engine's group was
        // renamed since): it is archived and verified, but its offset is that
        // group's. Committing it here would move THIS group's offset to
        // wherever the old group had read, past records this group has not
        // archived; reporting success would mark the row SOURCE_COMMITTED and
        // lose the fact that the old group's offset was never committed - a
        // rollback to that group would then re-read the range. It is an
        // error: the row stays VERIFIED until an engine consuming as that
        // group recovers it (review of #563).
        if let Some(group) = self.foreign_group(marker) {
            return Err(VtopError::Source(format!(
                "range of {topic}[{partition}] was read under consumer group {group}; this engine \
                 consumes as {} and cannot commit {group}'s offset, so the batch stays VERIFIED",
                self.cfg.consumer_group
            )));
        }

        // Committed offset is "next to read" = last consumed + 1.
        let commit_at = end_offset + 1;
        let key = (topic.clone(), *partition);

        // A range the known frontier already covers is SUPERSEDED, not
        // committed again. One engine reaches this on its own: a batch whose
        // Kafka commit failed stays VERIFIED with no rewind, later batches
        // commit past it, and recovery then commits the older row - which
        // moved the group's offset BACKWARDS, so anything that still read it
        // (lag monitoring, a rollback to an older build) saw the archived
        // range as unread. The frontier is the ledger's or this process's own
        // commits (`known_committed`), never a rewind's landing. Success is
        // reported so the row is marked SOURCE_COMMITTED: it is VERIFIED, and
        // everything up to the frontier is committed already. Where the
        // group's offset is known to be behind the frontier, it is moved up to
        // the frontier instead (review of #614: lag monitoring).
        if let Some(&frontier) = self.known_committed.get(&key) {
            if commit_at <= frontier {
                tracing::info!(
                    topic,
                    partition,
                    commit_at,
                    frontier,
                    "kafka commit skipped: the range is superseded by a later committed one, and \
                     committing it would move the group's offset backwards"
                );
                if self.group_at.get(&key).is_some_and(|&at| at < frontier) {
                    self.advance_group_offset(&key, frontier);
                }
                return Ok(());
            }
        }

        let consumer = self.consumer()?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, *partition, Offset::Offset(commit_at))
            .map_err(|e| VtopError::Source(format!("tpl: {e}")))?;
        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| VtopError::Source(format!("kafka commit: {e}")))?;

        // What THIS process committed: the partition's frontier from now on is
        // known here, and a stale commit by another engine must not be asked
        // for (#614). Never lowered: a rewind can leave the cursor above a
        // range that is committed afterwards, and where this process resumes
        // must not go back below what it has archived.
        self.group_at.insert(key.clone(), commit_at);
        let known = self.known_committed.entry(key.clone()).or_insert(commit_at);
        *known = (*known).max(commit_at);
        let cur = self.cursors.entry(key).or_default();
        cur.committed_offset = Some(cur.committed_offset.map_or(commit_at, |c| c.max(commit_at)));
        tracing::info!(
            topic,
            partition,
            commit_at,
            "kafka offset committed (post-verify)"
        );
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        self.replay_from_markers(std::slice::from_ref(marker))
            .await
            .pop()
            .unwrap_or_else(|| Err(VtopError::Source("kafka rewind produced no result".into())))
    }

    /// Each rewind is clamped to the committed frontier (see `clamp_rewind`):
    /// the ledger's, for a partition it has a committed row for (#614; see
    /// `rewind_frontier`), and the group's committed offset otherwise. The
    /// broker is asked in ONE round trip for the whole batch, however many
    /// partitions it names: asking per partition would cost a bounded wait each
    /// when the broker is slow or down, stalling startup in proportion to the
    /// number of partitions being replayed.
    async fn replay_from_markers(
        &mut self,
        markers: &[ProgressMarker],
    ) -> Vec<Result<(), VtopError>> {
        let partitions: Vec<(String, i32)> = markers
            .iter()
            .filter(|m| self.foreign_group(m).is_none())
            .filter_map(|m| match m {
                ProgressMarker::Kafka {
                    topic, partition, ..
                } => Some((topic.clone(), *partition)),
                _ => None,
            })
            .filter(|key| !self.known_committed.contains_key(key))
            .collect();
        let broker = self.group_committed_offsets(&partitions);
        markers
            .iter()
            .map(|marker| {
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
                // The cursor is keyed by (topic, partition) - it is THIS
                // group's position. A marker from another group (left in the
                // ledger by the engine before its group was renamed) must not
                // move it: recovery rewinds its units in an order that knows
                // nothing of groups, and an old group's rewind landing last
                // would overwrite the active group's lower one and skip
                // records (review of #563). The old group's range is not
                // re-read under this group; this group reads from its own
                // committed offset.
                if let Some(group) = self.foreign_group(marker) {
                    tracing::warn!(
                        topic,
                        partition,
                        marker_group = group,
                        group = %self.cfg.consumer_group,
                        "not rewinding for a range read under another consumer group"
                    );
                    return Ok(());
                }
                // Reads assign() from the cursor, so rewinding means resetting
                // the cursor: clear the in-memory read head and pin the resume
                // position. The next read assigns the partition at that offset.
                //
                // Never below the committed frontier: see `clamp_rewind`, and
                // `rewind_frontier` for why it is the ledger's and not the
                // broker's wherever the ledger knows the partition (#614).
                let key = (topic.clone(), *partition);
                let in_memory = self.cursors.get(&key).and_then(|cur| cur.committed_offset);
                let frontier = rewind_frontier(
                    self.known_committed.get(&key).copied(),
                    in_memory,
                    broker.get(&key).copied(),
                );
                let resume_at = clamp_rewind(*start_offset, frontier);
                let cur = self.cursors.entry(key).or_default();
                cur.last_read_offset = None;
                cur.committed_offset = Some(resume_at);
                if resume_at != *start_offset {
                    tracing::info!(
                        topic,
                        partition,
                        requested = start_offset,
                        committed = resume_at,
                        "kafka rewind clamped to the committed frontier: the range is already archived"
                    );
                }
                tracing::warn!(topic, partition, resume_at, "kafka rewound for replay");
                Ok(())
            })
            .collect()
    }

    fn read_position(&self, marker: &ProgressMarker) -> Option<i128> {
        let ProgressMarker::Kafka {
            topic, partition, ..
        } = marker
        else {
            return None;
        };
        // This group's cursor says nothing about where another group's
        // range would be read from.
        if self.foreign_group(marker).is_some() {
            return None;
        }
        let cursor = self.cursors.get(&(topic.clone(), *partition))?;
        // The next offset a read would ask for: past what was read ahead, or
        // the committed/rewound position when nothing has been read since.
        cursor
            .last_read_offset
            .map(|read| read + 1)
            .or(cursor.committed_offset)
            .map(i128::from)
    }

    fn source_type(&self) -> SourceType {
        SourceType::Kafka
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn tp(topic: &str, partition: i32) -> (String, i32) {
        (topic.to_string(), partition)
    }

    /// A 250ms pass, the engine's default poll window.
    const PASS: Duration = Duration::from_millis(250);

    /// #623, hypothesis 1: one partition stops delivering while the others
    /// carry traffic. The retry this replaced re-assigned only after a pass
    /// that read NOTHING, which never happens while other partitions deliver,
    /// so the partition stayed unread until a restart. Liveness is per
    /// partition: it is due once it has delivered nothing for STALL_PASSES
    /// passes and STALL_AFTER, whatever the other partitions do.
    #[test]
    fn a_partition_that_stops_while_others_deliver_is_found_stalled() {
        let assigned = vec![tp("a", 0), tp("a", 1), tp("b", 0)];
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        let start = Instant::now();
        let dead = tp("a", 1);
        let passes = (STALL_AFTER.as_millis() / PASS.as_millis()) as u32 + 1;
        let mut due_at = None;
        for pass in 1..=passes {
            let now = start + PASS * pass;
            // a-0 and b-0 deliver on every pass, so no pass reads nothing and
            // the retry this replaced would never have fired.
            live.record_pass(&assigned, |key| *key != dead, now);
            let due = live.due(now);
            if !due.is_empty() && due_at.is_none() {
                assert_eq!(due, vec![dead.clone()], "only the silent partition is due");
                due_at = Some(pass);
            }
        }
        let due_at = due_at.expect("the silent partition must become due");
        assert!(due_at > STALL_PASSES, "not before STALL_PASSES passes");
        assert!(
            PASS * (due_at - 1) >= STALL_AFTER,
            "not before STALL_AFTER of idleness, got {due_at} passes"
        );
        // Records wait for it: it is stalled, with the lag the log reports.
        assert_eq!(live.stalled_lag(&dead, Some(100), 140), Some(40));
    }

    #[test]
    fn an_idle_partition_is_not_due_until_both_thresholds_pass() {
        let assigned = vec![tp("a", 0)];
        let start = Instant::now();

        // Many passes in a short time: short poll windows, not a stall.
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        for pass in 1..=(STALL_PASSES * 4) {
            live.record_pass(
                &assigned,
                |_| false,
                start + Duration::from_millis(10) * pass,
            );
        }
        assert!(live.due(start + Duration::from_secs(1)).is_empty());

        // A long time with few passes: the engine stopped reading for a
        // while (backpressure), and the partition was not asked often enough
        // to say it is stalled.
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        for pass in 1..STALL_PASSES {
            live.record_pass(&assigned, |_| false, start + STALL_AFTER * pass);
        }
        assert!(live.due(start + STALL_AFTER * STALL_PASSES).is_empty());
        live.record_pass(&assigned, |_| false, start + STALL_AFTER * STALL_PASSES);
        assert_eq!(
            live.due(start + STALL_AFTER * STALL_PASSES),
            vec![tp("a", 0)]
        );
    }

    /// A partition that delivers, or whose topic filled its budget (crowded
    /// out, not stalled), starts its idle run over. So does every partition
    /// on a fresh assign(), which restarts every fetcher.
    #[test]
    fn delivery_a_full_topic_and_a_fresh_assign_each_start_the_idle_run_over() {
        let assigned = vec![tp("a", 0)];
        let start = Instant::now();
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        let idle_until = |live: &mut PartitionLiveness, from: u32| {
            for pass in from..from + STALL_PASSES + 1 {
                live.record_pass(&assigned, |_| false, start + STALL_AFTER * pass);
            }
        };
        idle_until(&mut live, 0);
        live.record_pass(&assigned, |_| true, start + STALL_AFTER * 20);
        assert!(live.due(start + STALL_AFTER * 20).is_empty());

        idle_until(&mut live, 30);
        live.on_assign(&assigned);
        assert!(live.due(start + STALL_AFTER * 40).is_empty());
    }

    /// The broker is asked at most once per STALL_CHECK_INTERVAL, however
    /// many passes run in between.
    #[test]
    fn the_stall_check_asks_the_broker_at_most_once_per_interval() {
        let assigned = vec![tp("a", 0)];
        let start = Instant::now();
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        for pass in 1..=STALL_PASSES {
            live.record_pass(&assigned, |_| false, start + STALL_AFTER * pass);
        }
        let first = start + STALL_AFTER * STALL_PASSES;
        assert_eq!(live.due(first), vec![tp("a", 0)]);
        assert!(live.due(first + PASS).is_empty());
        assert!(live.due(first + STALL_CHECK_INTERVAL - PASS).is_empty());
        assert_eq!(live.due(first + STALL_CHECK_INTERVAL), vec![tp("a", 0)]);
    }

    /// Stalled means records are waiting: the high watermark is past the
    /// offset the partition would read next. An idle partition that is caught
    /// up is not stalled, and one whose re-assign did not help is not
    /// re-assigned again until a new record arrives, so an undeliverable tail
    /// (a transaction marker) cannot restart every fetcher once per interval.
    #[test]
    fn a_partition_is_stalled_only_while_records_wait_for_it() {
        let key = tp("a", 0);
        let mut live = PartitionLiveness::default();
        assert_eq!(live.stalled_lag(&key, Some(40), 40), None, "caught up");
        assert_eq!(live.stalled_lag(&key, Some(41), 40), None);
        assert_eq!(live.stalled_lag(&key, None, 40), None, "next unknown");
        assert_eq!(
            live.stalled_lag(&key, Some(-1001), 40),
            None,
            "a sentinel offset is not a position"
        );
        assert_eq!(live.stalled_lag(&key, Some(39), 40), Some(1));

        live.note_reassigned(&key, 40);
        assert_eq!(live.stalled_lag(&key, Some(39), 40), None);
        assert_eq!(live.stalled_lag(&key, Some(39), 41), Some(2));
        // The bookmark is kept across an assign of the same partition, and
        // dropped with a partition that is no longer assigned.
        live.on_assign(std::slice::from_ref(&key));
        assert_eq!(live.stalled_lag(&key, Some(39), 40), None);
        live.on_assign(&[tp("b", 0)]);
        assert_eq!(live.stalled_lag(&key, Some(39), 40), Some(1));
    }

    #[test]
    fn the_next_offset_is_the_delivery_position_else_where_stored_resolves() {
        assert_eq!(next_read_offset(Some(7), Some(5), Some(0), true), Some(7));
        assert_eq!(next_read_offset(None, Some(5), Some(0), true), Some(5));
        assert_eq!(next_read_offset(None, None, Some(3), true), Some(3));
        assert_eq!(
            next_read_offset(None, None, Some(3), false),
            None,
            "a partition that starts at the end has no backlog to measure"
        );
        assert!(resets_to_earliest("earliest"));
        assert!(resets_to_earliest("smallest"));
        assert!(resets_to_earliest("Beginning"));
        assert!(!resets_to_earliest("latest"));
        assert!(!resets_to_earliest("error"));
    }

    #[test]
    fn a_rewind_never_goes_below_the_groups_committed_offset() {
        // A genuine rewind: nothing committed past the failed range, so it
        // lands where asked.
        assert_eq!(clamp_rewind(100, Some(100)), 100);
        assert_eq!(clamp_rewind(100, Some(40)), 100);
        // A superseded ledger row: the group has committed well past it, and
        // rewinding to it would re-archive everything since.
        assert_eq!(clamp_rewind(100, Some(5_000)), 5_000);
        // Nothing known: the request is honoured.
        assert_eq!(clamp_rewind(100, None), 100);
        assert_eq!(clamp_rewind(0, Some(0)), 0);
    }

    /// #614: where the ledger knows a partition, the broker's committed offset
    /// has no say in the frontier - in either direction. An engine whose claim
    /// moved on can still commit it, and Kafka accepts that commit.
    #[test]
    fn the_ledger_decides_the_frontier_and_a_stale_group_offset_does_not() {
        // A stale commit moved the group BACK to 5: the frontier stays at the
        // ledger's 20, so nothing archived is read again.
        assert_eq!(rewind_frontier(Some(20), None, Some(5)), Some(20));
        // A stale commit moved the group FORWARD to 20, past a range at 10
        // that recovery is about to re-read: the frontier stays at 10, so the
        // rewind to 10 lands at 10 instead of being clamped past the range.
        assert_eq!(rewind_frontier(Some(10), None, Some(20)), Some(10));
        assert_eq!(
            clamp_rewind(10, rewind_frontier(Some(10), None, Some(20))),
            10
        );
        // This process's own position counts as known too.
        assert_eq!(rewind_frontier(Some(10), Some(30), Some(99)), Some(30));
        assert_eq!(rewind_frontier(None, Some(30), Some(5)), Some(30));
        // A position only an earlier rewind set is not raised by a stale
        // commit made since: recovery rewinds twice, and the second rewind of
        // a range at 10 must still land at 10 (review of #614).
        assert_eq!(rewind_frontier(None, Some(10), Some(20)), Some(10));
        // A partition known nowhere falls back to the broker, as before.
        assert_eq!(rewind_frontier(None, None, Some(7)), Some(7));
        assert_eq!(rewind_frontier(None, None, None), None);
    }

    /// #614: a rewind of a partition the ledger knows is clamped to the
    /// ledger's frontier, and never asks the broker (there is none here: the
    /// test would hang on a broker round trip if it did, and a broker that
    /// cannot be asked must not change the answer).
    #[tokio::test]
    async fn a_rewind_of_a_partition_the_ledger_knows_is_clamped_to_the_ledger() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.known_committed.insert(("t".into(), 0), 20);
        let marker = |start: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: start,
            end_offset: start + 9,
            consumer_group: "vtop-engine".into(),
        };
        // A superseded row below the ledger's frontier: already archived.
        src.replay_from_marker(&marker(0)).await.unwrap();
        assert_eq!(src.read_position(&marker(0)), Some(20));
        // A range at the frontier is re-read from its first offset.
        src.cursors.clear();
        src.replay_from_marker(&marker(20)).await.unwrap();
        assert_eq!(src.read_position(&marker(20)), Some(20));
    }

    /// #614, one engine on its own: a batch whose Kafka commit failed stays
    /// VERIFIED, later batches commit past it, and recovery commits it last.
    /// That commit is superseded - reported as done, so the row is marked
    /// SOURCE_COMMITTED, but never sent to the broker, where it would move the
    /// group's offset backwards. No broker exists here: sending it would fail.
    #[tokio::test]
    async fn a_commit_below_the_known_frontier_is_superseded_not_sent() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.known_committed.insert(("t".into(), 0), 20);
        let older = ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: 0,
            end_offset: 9,
            consumer_group: "vtop-engine".into(),
        };
        src.commit_progress(&older)
            .await
            .expect("a superseded range is committed already: success, not an error");
        assert!(
            src.consumer.is_none(),
            "the superseded commit reached for the broker"
        );
        assert_eq!(src.known_committed[&("t".to_string(), 0)], 20);
        assert!(
            !src.cursors.contains_key(&("t".to_string(), 0)),
            "a superseded commit must not move where the partition resumes"
        );
    }

    /// #614, #563: the ledger's rows of another consumer group never seed this
    /// group's position - and seeding nothing must not ask the broker.
    #[test]
    fn another_groups_ledger_rows_never_seed_this_groups_position() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.seed_committed_from_ledger([("t", 0, "vtop-engine-old", 700)]);
        assert!(src.known_committed.is_empty());
        assert!(src.cursors.is_empty());
        assert!(
            src.consumer.is_none(),
            "nothing to seed must not even build a consumer"
        );
    }

    /// #614: the ledger is trusted only where the consumer group has a
    /// committed offset. A broker that cannot be asked (none here) leaves the
    /// partition to `auto.offset.reset`, as before, rather than guessing - a
    /// recreated topic has no group offset and the ledger's would skip its
    /// first records.
    #[test]
    fn a_ledger_position_is_not_trusted_when_the_group_offset_cannot_be_read() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.seed_committed_from_ledger([("t", 0, "vtop-engine", 19)]);
        assert!(src.known_committed.is_empty());
        assert_eq!(
            src.cursors
                .get(&("t".to_string(), 0))
                .and_then(|c| c.committed_offset),
            None
        );
    }

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

    /// Retention overtaking a cursor is measured against the broker's low
    /// watermark, and only when there is a cursor to overtake (#361).
    ///
    /// The commit rule protects records the engine READ. This is the one
    /// failure it cannot see, so the detection has to be exact: a heuristic
    /// that fires on compaction, on a recreated topic, or on first assignment
    /// would be ignored within a week, and then the real event would be too.
    #[test]
    fn a_cursor_below_the_low_watermark_reports_exactly_what_retention_took() {
        assert_eq!(
            retention_gap(Some(100), 350),
            Some(250),
            "the gap is (low watermark - desired), which is the count of records that \
             existed, were never read, and are now unreadable"
        );
        assert_eq!(
            retention_gap(Some(349), 350),
            Some(1),
            "one record lost is still a loss and must be reported as one"
        );

        assert_eq!(
            retention_gap(Some(350), 350),
            None,
            "a cursor exactly at the oldest surviving offset has lost nothing"
        );
        assert_eq!(
            retention_gap(Some(1_000), 350),
            None,
            "and a cursor ahead of it is simply healthy"
        );

        assert_eq!(
            retention_gap(None, 10_000),
            None,
            "a partition with no committed cursor has read nothing, so it can have missed \
             nothing — reporting the whole topic as a gap on first assignment is the \
             fastest way to make this signal ignored"
        );

        // librdkafka spells Beginning, End, Stored and Invalid as negative
        // numbers. Subtracting one from a watermark invents a gap.
        for sentinel in [-1_i64, -2, -1_000, -1_001] {
            assert_eq!(
                retention_gap(Some(sentinel), 10_000),
                None,
                "{sentinel} is a position marker, not an offset, and must not become a gap"
            );
        }
    }

    /// The counter is fed only what previous checks have not already counted
    /// (#361): the check re-observes a persisting gap once a minute, and a
    /// counter fed the full gap every time would multiply the same lost
    /// records by however long the operator took to notice.
    #[test]
    fn a_gap_is_counted_once_however_often_it_is_observed() {
        assert_eq!(
            newly_lost(Some(100), 350, None),
            250,
            "the first observation counts the whole gap"
        );
        assert_eq!(
            newly_lost(Some(100), 350, Some(350)),
            0,
            "the second observation of the SAME gap counts nothing — this is \
             the double-count a naive counter would add once a minute"
        );
        assert_eq!(
            newly_lost(Some(100), 500, Some(350)),
            150,
            "when the watermark advances further, only the advance is new loss"
        );
        assert_eq!(
            newly_lost(Some(400), 500, Some(350)),
            100,
            "a cursor that advanced past the counted watermark and fell behind \
             again lost only what is behind the CURSOR, not the stale bookmark"
        );
        assert_eq!(
            newly_lost(Some(350), 350, Some(300)),
            0,
            "no current gap means nothing new, whatever was counted before"
        );
        assert_eq!(
            newly_lost(None, 10_000, Some(5_000)),
            0,
            "no cursor still means nothing missed, bookmark or not"
        );
        assert_eq!(
            newly_lost(Some(-2), 10_000, None),
            0,
            "sentinels stay sentinels here too"
        );
    }

    /// Review of #563: the cursor is this group's position. A range left in
    /// the ledger by the engine under its previous group name must neither
    /// rewind it (recovery's order knows nothing of groups, so an old group's
    /// rewind could land last and overwrite the active group's lower one),
    /// nor answer for it, nor be committed under this group.
    #[tokio::test]
    async fn a_range_from_another_consumer_group_never_moves_this_groups_cursor() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.cursors.insert(
            ("t".into(), 0),
            PartitionCursor {
                last_read_offset: None,
                committed_offset: Some(100),
            },
        );
        let marker = |group: &str, start: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: start,
            end_offset: start + 9,
            consumer_group: group.into(),
        };
        let old = marker("vtop-engine-old", 700);

        src.replay_from_marker(&old).await.unwrap();
        assert_eq!(
            src.cursors[&("t".into(), 0)].committed_offset,
            Some(100),
            "another group's range rewound this group's cursor"
        );
        assert_eq!(src.read_position(&old), None);
        assert_eq!(src.read_position(&marker("vtop-engine", 50)), Some(100));

        let refused = src.commit_progress(&old).await;
        assert!(
            refused.as_ref().is_err_and(|e| e
                .to_string()
                .contains("cannot commit vtop-engine-old's offset")),
            "another group's range must not report a commit that did not happen: {refused:?}"
        );
        assert_eq!(
            src.cursors[&("t".into(), 0)].committed_offset,
            Some(100),
            "another group's range was committed as this group's progress"
        );
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

    /// The report carries one entry per topic (#361): the topic names the
    /// stream, and the stream names the tenant the loss is charged to — one
    /// adapter-wide sum could only ever land on the engine default.
    #[test]
    fn losses_on_two_topics_are_reported_separately_not_as_one_sum() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        // Seed the accumulator as report_retention_gaps does: once per
        // (topic, partition) observation, merged by topic.
        *src.pending_lost.entry("t_a".into()).or_insert(0) += 3;
        *src.pending_lost.entry("t_a".into()).or_insert(0) += 4;
        *src.pending_lost.entry("t_b".into()).or_insert(0) += 5;

        let delivered = src.take_pending_losses();
        let entries: Vec<(&str, u64)> = delivered
            .iter()
            .map(|l| (l.source_name.as_str(), l.records))
            .collect();
        assert_eq!(
            entries,
            vec![("t_a", 7), ("t_b", 5)],
            "partitions of one topic merge (they share a stream and a tenant), \
             topics never do — a single sum would charge t_b's loss to \
             whatever tenant t_a resolves to, or both to the engine default"
        );
    }

    /// The bookmark lives exactly as long as the broker-side subject it
    /// describes (#361): a topic recreated under a pruned name starts from
    /// its own cursor, while the pending count — loss already OBSERVED — is
    /// owed to the counter even after its topic died.
    #[test]
    fn a_dead_topics_loss_bookmark_is_dropped_so_a_recreated_topic_counts_its_own_loss() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.reported_lost_through.insert(("live".into(), 0), 350);
        src.reported_lost_through.insert(("dead".into(), 0), 350);
        src.pending_lost.insert("dead".into(), 9);

        src.prune_partition_cache(&["live".to_string()]);

        assert_eq!(
            src.reported_lost_through.get(&("live".into(), 0)),
            Some(&350),
            "a live topic's bookmark must survive the prune, or every \
             discovery cycle would re-count its whole standing gap"
        );
        assert!(
            !src.reported_lost_through.contains_key(&("dead".into(), 0)),
            "a topic absent from live metadata can never report that gap \
             again — keeping its bookmark would score a recreated topic's \
             real loss zero until the new watermark passed the stale one"
        );
        assert_eq!(
            src.pending_lost.get("dead"),
            Some(&9),
            "observed loss is owed to the counter even if its topic died; \
             pruning it would drop a count the warn line already announced"
        );
    }

    /// REGRESSION shape for the delivery half of #361: the check observes a
    /// gap and advances the bookmark, but its count reaches the engine only
    /// on a pass that returns Ok — and the bookmark scores the same gap zero
    /// on every later check. Without the accumulator, a pass that failed
    /// after the check turned "counted exactly once" into "at most once".
    #[test]
    fn a_count_observed_in_a_failed_pass_is_delivered_by_the_next_successful_one() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        // Stage what a failed pass leaves behind: the observation accrued and
        // the bookmark advanced, then the pass returned Err and the report —
        // the only vehicle to the engine — was dropped.
        *src.pending_lost.entry("t".into()).or_insert(0) += 250;
        src.reported_lost_through.insert(("t".into(), 0), 350);

        assert_eq!(
            newly_lost(Some(100), 350, Some(350)),
            0,
            "the next check scores the unchanged gap zero — the bookmark \
             dedupes observation, so delivery cannot rely on re-observing"
        );
        let delivered = src.take_pending_losses();
        let entries: Vec<(&str, u64)> = delivered
            .iter()
            .map(|l| (l.source_name.as_str(), l.records))
            .collect();
        assert_eq!(
            entries,
            vec![("t", 250)],
            "the first pass that completes pays the debt: the counter must \
             never read 0 while the warn line said 250 records were lost"
        );
        assert!(
            src.take_pending_losses().is_empty(),
            "the drain empties the accumulator, so a second successful pass \
             delivers nothing — anything else double-counts the same loss"
        );
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

    // The single poll loop (#96 A1) interleaves messages from EVERY topic and
    // partition; MultiTopicAcc is the demux that turns that stream back into
    // per-(topic, partition) units. These tests pin the demux and budget logic
    // without a broker.

    #[test]
    fn multi_topic_acc_demuxes_by_topic_and_partition() {
        // Topic indices: 0 = "t_a", 1 = "t_b" (the caller maps names once).
        let mut acc = MultiTopicAcc::new(2, usize::MAX, usize::MAX);
        // Interleaved arrival order across two topics and two partitions.
        assert!(acc.push(0, 0, 10, b"a0".to_vec()));
        assert!(acc.push(1, 0, 5, b"b0".to_vec()));
        assert!(acc.push(0, 1, 20, b"a1".to_vec()));
        assert!(acc.push(0, 0, 11, b"a0b".to_vec()));
        assert_eq!(acc.total_records, 4);

        let sorted = acc.into_sorted();
        let keys: Vec<(usize, i32)> = sorted.iter().map(|&(t, p, _)| (t, p)).collect();
        // Deterministic order: by topic index, then partition.
        assert_eq!(keys, vec![(0, 0), (0, 1), (1, 0)]);
        // Same partition accumulates in arrival order and tracks the offset range.
        let (_, _, a0) = &sorted[0];
        assert_eq!(a0.records, vec![b"a0".to_vec(), b"a0b".to_vec()]);
        assert_eq!((a0.start_offset, a0.end_offset), (10, 11));
        // Different partitions of the same topic never mix.
        let (_, _, a1) = &sorted[1];
        assert_eq!(a1.records, vec![b"a1".to_vec()]);
        assert_eq!((a1.start_offset, a1.end_offset), (20, 20));
    }

    #[test]
    fn multi_topic_acc_enforces_record_budget_per_topic() {
        // Budget of 2 records PER TOPIC: a busy topic (idx 0) must not eat
        // another topic's allowance (an aggregate budget would let one topic
        // take topic_count x max_bytes in a single unit — P1 review on #106).
        let (busy, quiet) = (0usize, 1usize);
        let mut acc = MultiTopicAcc::new(2, 2, usize::MAX);
        assert!(acc.push(busy, 0, 0, b"r".to_vec()));
        assert!(!acc.is_full(busy));
        assert!(acc.push(busy, 0, 1, b"r".to_vec()));
        assert!(acc.is_full(busy), "at budget after the second record");
        // busy is now at budget: further messages are REJECTED, not kept.
        assert!(!acc.push(busy, 0, 2, b"r".to_vec()));
        assert!(
            !acc.push(busy, 1, 0, b"r".to_vec()),
            "budget spans the topic's partitions"
        );
        // The quiet topic still has its own full allowance.
        assert!(acc.push(quiet, 0, 0, b"r".to_vec()));
        assert!(!acc.all_full(2), "quiet topic below budget");
        assert!(acc.push(quiet, 0, 1, b"r".to_vec()));
        assert!(acc.all_full(2), "both topics at budget ends the pass");
        // The rejected offset 2 was never recorded: busy p0 ends at offset 1,
        // so the cursor advance re-reads offset 2 next pass (no gap, no loss).
        let sorted = acc.into_sorted();
        let busy_p0 = sorted
            .iter()
            .find(|&&(t, p, _)| t == busy && p == 0)
            .unwrap();
        assert_eq!(busy_p0.2.end_offset, 1);
        assert_eq!(busy_p0.2.records.len(), 2);
    }

    #[test]
    fn multi_topic_acc_enforces_byte_budget_per_topic() {
        let (t_a, t_b) = (0usize, 1usize);
        let mut acc = MultiTopicAcc::new(2, usize::MAX, 10);
        // 6 bytes accepted; the next push would cross the 10-byte budget and
        // is rejected without recording its offset.
        assert!(acc.push(t_a, 0, 0, vec![0u8; 6]));
        assert!(!acc.push(t_a, 0, 1, vec![0u8; 6]));
        // The topic is then full and continues to reject.
        assert!(!acc.push(t_a, 0, 2, vec![0u8; 1]));
        // Other topics are unaffected by t_a's bytes.
        assert!(acc.push(t_b, 0, 0, vec![0u8; 6]));
        assert_eq!(acc.full_topics, 1, "only t_a is at its byte budget");
        assert!(!acc.all_full(2), "t_b still has allowance");
    }

    #[test]
    fn multi_topic_acc_failure_discards_only_the_failed_topic() {
        let mut acc = MultiTopicAcc::new(2, usize::MAX, usize::MAX);
        assert!(acc.push(0, 0, 10, b"discard".to_vec()));
        assert!(acc.push(1, 0, 20, b"preserve".to_vec()));

        acc.fail_topic(0);

        assert!(acc.is_full(0));
        assert_eq!(acc.total_records, 1);
        let sorted = acc.into_sorted();
        assert_eq!(sorted.len(), 1);
        assert_eq!((sorted[0].0, sorted[0].1), (1, 0));
        assert_eq!(sorted[0].2.records, vec![b"preserve".to_vec()]);
    }

    /// #559: a partition's records carry the earliest and latest message
    /// timestamp of what was KEPT - out-of-order producer times give a range,
    /// a rejected message does not widen it, and "no timestamp" is ignored.
    #[test]
    fn a_partition_reports_the_time_range_of_the_records_it_kept() {
        let mut acc = MultiTopicAcc::new(1, 3, usize::MAX);
        for (offset, ts) in [(0, 2_000), (1, 1_000), (2, -1)] {
            assert!(acc.push(0, 0, offset, b"r".to_vec()));
            acc.note_timestamp(0, 0, ts);
        }
        // At budget: rejected, and the caller only notes kept messages.
        assert!(!acc.push(0, 0, 3, b"r".to_vec()));
        let parts = acc.into_sorted();
        assert_eq!(parts[0].2.min_ts_ms, Some(1_000));
        assert_eq!(parts[0].2.max_ts_ms, Some(2_000));
    }
}
