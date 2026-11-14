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
use std::collections::{BTreeMap, HashMap, HashSet};
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
    /// The high watermark at which each partition was last recovered from a
    /// stall, and how many recoveries it has had at that watermark. The first
    /// re-assigns the partition; the second, when the first did not help and
    /// no record has arrived since, rebuilds the consumer, which is what a
    /// restart does (#623: a run of the research benchmark sat at a static
    /// high watermark until a restart). After `MAX_RECOVERIES_PER_HIGH` the
    /// partition is left alone until a new record arrives: it holds records
    /// it cannot deliver (a transaction marker at the end of the log, for
    /// example), not a dead fetcher.
    reassigned_at_high: HashMap<(String, i32), (i64, u8)>,
    /// For a partition that has not delivered since an assign at
    /// `Offset::Stored`, where the group answered that nothing is committed
    /// and `auto.offset.reset` is `latest`: where the fetcher would have
    /// started, which is the end of the log as it was at the assign (review of
    /// #644). The stall check finds it from record timestamps, from the
    /// assign's own time (`end_start_lookup_time`); when the broker cannot
    /// say, nothing is recorded and the next check asks again (a check's own
    /// high watermark is not a start: it would give up every record written
    /// between the assign and that check). A stall re-assigns such a
    /// partition at this offset, never at `Offset::Stored`, which would
    /// resolve to the NEW end and skip what piled up.
    end_baseline: HashMap<(String, i32), i64>,
    /// Wall-clock time (ms since the epoch) of the last assign(), for finding
    /// where a partition assigned at the end of the log started.
    assigned_at_ms: i64,
}

impl PartitionLiveness {
    /// A fresh assign() restarts every fetcher, so every idle run starts over,
    /// and a partition assigned at the end of the log starts at a new end.
    fn on_assign(&mut self, assignment: &[(String, i32)]) {
        self.idle.clear();
        self.end_baseline.clear();
        self.reassigned_at_high
            .retain(|key, _| assignment.binary_search(key).is_ok());
        self.assigned_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    }

    /// Record where a partition assigned at the end of the log started, unless
    /// already known.
    fn note_baseline(&mut self, key: &(String, i32), start: i64) {
        self.end_baseline.entry(key.clone()).or_insert(start);
    }

    fn has_baseline(&self, key: &(String, i32)) -> bool {
        self.end_baseline.contains_key(key)
    }

    fn baseline(&self, key: &(String, i32)) -> Option<i64> {
        self.end_baseline.get(key).copied()
    }

    /// The offset a due partition would read next: `next` when it is known,
    /// otherwise the end-of-log baseline the start lookup established. With
    /// neither, nothing is known and nothing is decided: the lookup is tried
    /// again at the next check. A check never invents a start (#623, the
    /// flake hunt on #644): taking its own high watermark as one counted every
    /// record written between the assign and that check as before the start,
    /// and a dead fetcher's backlog was given up for good.
    fn next_or_baseline(&self, key: &(String, i32), next: Option<i64>) -> Option<i64> {
        next.or_else(|| self.end_baseline.get(key).copied())
    }

    /// Where a due partition assigned at the end of the log would read next:
    /// the start an earlier lookup established, else what this check's start
    /// lookup answered for it - its first offset since the assign, or `high`
    /// (read BEFORE the lookup) when no record is that new. A failed lookup,
    /// or an answer that omits the partition or carries an error for it,
    /// establishes nothing: the partition stays unresolved and is asked again
    /// at the next check (#623; review of #644).
    fn end_start(
        &mut self,
        key: &(String, i32),
        answer: Option<&HashMap<(String, i32), Option<i64>>>,
        high: i64,
    ) -> Option<i64> {
        if let Some(start) = answer.and_then(|answer| answer.get(key)) {
            self.note_baseline(key, start.unwrap_or(high));
        }
        self.next_or_baseline(key, None)
    }

    /// Record one completed pass over `assigned`, the set given to the last
    /// `on_assign`. A partition progressed only if IT delivered something
    /// (kept or rejected: either way its fetcher is alive). Its topic filling
    /// its budget is not progress: another partition of the topic can fill it
    /// on every pass while this one's fetcher is dead (review of #644), and a
    /// partition that is merely crowded out still has to have records waiting
    /// before the stall check acts on it.
    ///
    /// Delivery also drops the partition's re-assign bookmark and end-of-log
    /// baseline: a partition that reads again is a new case, and a later
    /// stall at the same high watermark must still be recovered.
    ///
    /// Runs on every pass, so it allocates only when a partition starts an
    /// idle run.
    fn record_pass(
        &mut self,
        assigned: &[(String, i32)],
        delivered: impl Fn(&(String, i32)) -> bool,
        now: Instant,
    ) {
        for key in assigned {
            if delivered(key) {
                // Not only when an idle run ends: the stall's own re-assign
                // clears the idle runs, and the first delivery after it is
                // the one that must drop the bookmark.
                self.idle.remove(key);
                if !self.reassigned_at_high.is_empty() {
                    self.reassigned_at_high.remove(key);
                }
                if !self.end_baseline.is_empty() {
                    self.end_baseline.remove(key);
                }
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
    /// or past `next`, the offset it would read next, and it has not used up
    /// its recoveries at this high watermark. `None` when `next` is unknown.
    fn stalled_lag(&self, key: &(String, i32), next: Option<i64>, high: i64) -> Option<u64> {
        let next = next.filter(|n| *n >= 0)?;
        if high <= next || self.recoveries_at(key, high) >= MAX_RECOVERIES_PER_HIGH {
            return None;
        }
        Some(high.saturating_sub(next) as u64)
    }

    /// How a stalled partition is recovered this time: a re-assign of it
    /// first, then, if that did not help at the same high watermark, a new
    /// consumer.
    fn recovery(&self, key: &(String, i32), high: i64) -> Recovery {
        if self.recoveries_at(key, high) == 0 {
            Recovery::Reassign
        } else {
            Recovery::RebuildConsumer
        }
    }

    fn recoveries_at(&self, key: &(String, i32), high: i64) -> u8 {
        match self.reassigned_at_high.get(key) {
            Some(&(at, count)) if at == high => count,
            _ => 0,
        }
    }

    fn note_reassigned(&mut self, key: &(String, i32), high: i64) {
        let count = self.recoveries_at(key, high).saturating_add(1);
        self.reassigned_at_high.insert(key.clone(), (high, count));
    }
}

/// One stalled partition found by a stall check, logged once the check knows
/// what it did about it.
struct StallReport {
    key: (String, i32),
    next_offset: i64,
    high: i64,
    lag: u64,
    idle_passes: u32,
    idle_secs: u64,
    recovery: Recovery,
}

/// What a stall check did about one stalled partition, for its log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StallOutcome {
    Reassigned,
    Rebuilt,
    RebuildDeferred,
}

impl StallOutcome {
    /// Logged once the outcome is known, so a deferred rebuild never reads
    /// as a recovery (review of #644).
    fn message(self) -> &'static str {
        match self {
            StallOutcome::Reassigned => {
                "kafka partition re-assigned after stall: it delivered nothing while the broker \
                 holds records for it, so its fetcher is restarted (#623)"
            }
            StallOutcome::Rebuilt => {
                "kafka consumer rebuilt after stall: a re-assign did not bring this partition \
                 back and no record has arrived since, so a new consumer reads it (#623)"
            }
            StallOutcome::RebuildDeferred => {
                "kafka partition still stalled; its consumer rebuild is deferred because where a \
                 partition assigned at the end of the log started could not be established, and \
                 is tried again at the next check (#623)"
            }
        }
    }
}

/// Recoveries of one partition at one high watermark (#623). See
/// `PartitionLiveness::reassigned_at_high`.
const MAX_RECOVERIES_PER_HIGH: u8 = 2;

/// How the stall check recovers a stalled partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recovery {
    /// Restart its fetcher alone: unassign and assign it again, at its own
    /// position, leaving every other partition's fetcher (and the offset an
    /// `Offset::Stored` partition resolved to) as it is.
    Reassign,
    /// Drop the consumer and build a new one, as a restart would.
    RebuildConsumer,
}

/// The offset a partition would read next, for the stall check (#623): where
/// it last delivered up to, or else where `Offset::Stored` resolves - the
/// group's committed offset, or the low watermark when nothing is committed
/// and `auto.offset.reset` starts at the beginning. `None` when none of these
/// is known (a partition that starts at the END of the log): the stall check
/// then measures from `PartitionLiveness::next_or_baseline`.
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

/// Whether the stall check may measure an unresolved partition from an
/// end-of-log baseline, and pin its recovery there (review of #644): only
/// when the group was asked and answered that nothing is committed, and the
/// policy starts at the end. A failed lookup is "don't know": a pin past a
/// commit the broker could not report would skip the records between them.
/// `error` keeps librdkafka's own behaviour; no start is invented for it.
fn may_measure_from_end(group_answered: bool, has_commit: bool, auto_offset_reset: &str) -> bool {
    group_answered && !has_commit && resets_to_latest(auto_offset_reset)
}

/// The record time from which the stall check looks for where a partition
/// assigned at the end of the log started: the assign itself, with no margin
/// before it (review of #644). A margin finds records written shortly
/// before the assign, which `latest` skips on purpose, and makes a healthy
/// idle partition look stalled and get rewound onto them. The cost is the
/// other direction of clock skew: a record written after the assign by a
/// producer whose clock runs behind this host's carries an earlier time and
/// is not found, so a stall recovery can start past it.
fn end_start_lookup_time(assigned_at_ms: i64) -> i64 {
    assigned_at_ms
}

/// Whether `auto.offset.reset` starts a partition with nothing committed at
/// the end of the log (librdkafka's spellings).
fn resets_to_latest(auto_offset_reset: &str) -> bool {
    matches!(
        auto_offset_reset.to_ascii_lowercase().as_str(),
        "latest" | "largest" | "end"
    )
}

/// Whether `auto.offset.reset` starts a partition with nothing committed at
/// the beginning of the log (librdkafka's spellings).
fn resets_to_earliest(auto_offset_reset: &str) -> bool {
    matches!(
        auto_offset_reset.to_ascii_lowercase().as_str(),
        "earliest" | "smallest" | "beginning"
    )
}

/// The position a partition is held at, when the broker's end offset
/// (`high`) is below it (#675 follow-up). A live log never shrinks below a
/// position read from it: the topic was deleted and recreated (its offsets
/// start at 0 again), or the log was truncated by an unclean leader election.
/// The two cannot be told apart from offsets, and topic ids are not in the
/// metadata this client exposes, so it is reported for the operator, not
/// acted on: re-reading a truncated log from its start would archive it again.
fn end_below_position(position: Option<i64>, high: i64) -> Option<i64> {
    position.filter(|position| *position > high)
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

/// The first offset whose record timestamp is at or after `timestamp_ms`, for
/// each of `partitions`, in one bounded ListOffsets call per leader: `Some`
/// for an offset, `None` when every record is older (the start is the end of
/// the log). A partition the broker did not answer for is absent; `None`
/// overall when the call itself failed.
fn offsets_at_time(
    consumer: &BaseConsumer,
    partitions: &[(String, i32)],
    timestamp_ms: impl Fn(&(String, i32)) -> i64,
) -> Option<HashMap<(String, i32), Option<i64>>> {
    let mut tpl = TopicPartitionList::new();
    for key in partitions {
        tpl.add_partition_offset(&key.0, key.1, Offset::Offset(timestamp_ms(key)))
            .ok()?;
    }
    let listed = consumer
        .offsets_for_times(tpl, Duration::from_secs(WATERMARK_TIMEOUT_SECONDS))
        .ok()?;
    let mut answers = HashMap::new();
    for (topic, partition) in partitions {
        let Some(element) = listed.find_partition(topic, *partition) else {
            continue;
        };
        let answer = match (element.error(), element.offset()) {
            (Ok(()), Offset::Offset(offset)) => Some(offset),
            (Ok(()), Offset::End) => None,
            _ => continue,
        };
        answers.insert((topic.clone(), *partition), answer);
    }
    Some(answers)
}

/// A (topic, partition).
type PartitionKey = (String, i32);

/// Where each of `keys`, assigned at the end of the log with nothing
/// committed, started: the first offset since the assign, or the end of the
/// log when no record is that new. `None` when any answer is missing.
/// `high_watermarks` and `first_since_assign` are the broker lookups, passed
/// in so the order they run in can be tested.
///
/// The high watermark is read FIRST (review of #644). Read after the
/// timestamp lookup, a record appended in between would sit below the
/// fallback and be skipped for good, although it arrived after the assign.
/// Read before, any record appended after it carries a time after the
/// assign, so the timestamp lookup finds it and the fallback is not used.
#[cfg(test)]
fn end_of_log_starts(
    keys: &[(String, i32)],
    high_watermarks: impl FnOnce(&[(String, i32)]) -> HashMap<(String, i32), i64>,
    first_since_assign: impl FnOnce(&[(String, i32)]) -> Option<HashMap<(String, i32), Option<i64>>>,
) -> Option<Vec<((String, i32), i64)>> {
    end_of_log_starts_each(keys, high_watermarks, first_since_assign)
        .into_iter()
        .map(|(key, start)| start.map(|start| (key, start)))
        .collect()
}

/// `end_of_log_starts` per partition: `None` for a partition whose answer is
/// missing, so the others can still be pinned (#675 follow-up: one partition
/// whose leader is offline must not hold back every other one).
fn end_of_log_starts_each(
    keys: &[(String, i32)],
    high_watermarks: impl FnOnce(&[(String, i32)]) -> HashMap<(String, i32), i64>,
    first_since_assign: impl FnOnce(&[(String, i32)]) -> Option<HashMap<(String, i32), Option<i64>>>,
) -> Vec<((String, i32), Option<i64>)> {
    let highs = high_watermarks(keys);
    let starts = first_since_assign(keys).unwrap_or_default();
    keys.iter()
        .map(|key| {
            let start = match starts.get(key) {
                Some(Some(start)) => Some(*start),
                Some(None) => highs.get(key).copied(),
                None => None,
            };
            (key.clone(), start)
        })
        .collect()
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
    /// Partition ids per topic, cached with a TTL, and what each metadata
    /// answer changed (#675 follow-up: see `PartitionCache`).
    ///
    /// assign() needs the partition list, but fetching metadata on EVERY read
    /// costs a broker round trip per topic per cycle. With a few topics that is
    /// invisible; with 28 topics under load it dominated the cycle and the
    /// engine appeared to stall for minutes at ~0% CPU. A topic's partition
    /// count changes rarely, so cache it and re-check on the TTL.
    partitions: PartitionCache,
    /// Partitions assigned at `Offset::Stored` under `latest` that have not
    /// delivered and hold no position of their own, with the wall-clock time
    /// (ms since the epoch) of the assign that first put them there. Any later
    /// assign of one must pin it where it started, found from this time, or
    /// `Offset::Stored` resolves at the NEW end and skips what arrived since
    /// (#675 follow-up, `pin_latest_starts`). Kept per partition, not as the
    /// last assign's time, because a partition whose pin is deferred misses
    /// the assigns in between.
    stored_since: HashMap<(String, i32), i64>,
    /// Partitions whose pin could not be established, and when it may be
    /// asked again: left out of the assignment until then, never assigned at
    /// `Offset::Stored`.
    pin_deferred: HashMap<(String, i32), Instant>,
    /// The partition lists the last `discover_sources` saw, handed to the
    /// next read pass so it re-checks partition counts without a request of
    /// its own (#675 follow-up).
    discovered: std::sync::Mutex<Option<HashMap<String, Vec<i32>>>>,
    /// Rate limit for the read path's repeated warnings (#675 follow-up).
    warnings: WarnLimiter,
}

/// How long a cached partition list is trusted.
///
/// While partition counts are re-checked (`recheck_partition_counts`), every
/// re-check refreshes the entries it confirms, so a topic's entry only
/// expires when re-checks stop answering for it.
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
            partitions: PartitionCache::default(),
            stored_since: HashMap::new(),
            pin_deferred: HashMap::new(),
            discovered: std::sync::Mutex::new(None),
            warnings: WarnLimiter::default(),
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
        let mut end_fell: Vec<((String, i32), i64, i64, i64)> = Vec::new();
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
            let Ok((low_watermark, high_watermark)) = consumer.fetch_watermarks(
                topic,
                *partition,
                std::time::Duration::from_secs(WATERMARK_TIMEOUT_SECONDS),
            ) else {
                continue;
            };
            if let Some(position) = end_below_position(next_offset, high_watermark) {
                end_fell.push((
                    (topic.clone(), *partition),
                    position,
                    low_watermark,
                    high_watermark,
                ));
            }
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
        // #675 follow-up: an end offset below the position this process holds
        // cannot happen in a live log. Reported, not acted on: see
        // `end_below_position`.
        let now = Instant::now();
        for ((topic, partition), position, low_watermark, high_watermark) in end_fell {
            count_partition_event("recreated_or_truncated");
            let subject = format!("end:{topic}:{partition}");
            if let Some(suppressed) = self.warnings.allow(&subject, now) {
                tracing::warn!(
                    topic = %topic,
                    partition,
                    position,
                    high_watermark,
                    low_watermark,
                    auto_offset_reset = %self.cfg.auto_offset_reset,
                    suppressed,
                    "kafka partition's end offset fell below this consumer's position: the topic \
                     was deleted and recreated, or its log was truncated (unclean leader \
                     election). The engine does not move the position itself: the broker \
                     answers the next fetch out of range and auto.offset.reset applies \
                     (earliest: the new log from its start; latest: its end, skipping what it \
                     holds). The ledger cannot tell the two logs apart: check whether the topic \
                     was recreated and what was archived for it, and restart the engine with \
                     auto.offset.reset=earliest if the new log must be read from its start \
                     (#675)"
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
    /// whose partitions all deliver never asks at all. At most four bounded
    /// calls, each one request per leader for every due partition: the high
    /// watermarks, then, for a partition that has not delivered since
    /// `Offset::Stored`, the group's committed offsets and either the low
    /// watermarks (earliest) or the offsets at the assign's time (latest).
    ///
    /// A partition is stalled when it would read next below the high
    /// watermark. A stalled partition is logged and its fetcher alone is
    /// restarted (`reassign_stalled`); if that did not help and no record has
    /// arrived since, the next stall at the same high watermark rebuilds the
    /// consumer (`rebuild_consumer_after_stall`), and after that the
    /// partition is left alone until a record arrives. At most once per
    /// interval.
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
        // A partition whose group answer is unknown (the lookup failed, or
        // its answer carried an error) is skipped until the next check: "no
        // answer" is not "nothing committed" (review of #644).
        let answers = self
            .group_committed_answers(&unresolved)
            .unwrap_or_default();
        let committed: HashMap<(String, i32), i64> = answers
            .iter()
            .filter_map(|(key, at)| at.map(|at| (key.clone(), at)))
            .collect();
        let need_low: Vec<(String, i32)> = if reset_to_earliest {
            unresolved
                .iter()
                .filter(|key| matches!(answers.get(*key), Some(None)))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let lows = match self.consumer.as_ref() {
            Some(consumer) => list_offsets(consumer, &need_low, Offset::Beginning),
            None => HashMap::new(),
        };
        // A partition that started at the END of the log (latest, the group
        // answered that nothing is committed): find where, from record
        // timestamps, once per assign (review of #644). No record since then
        // means the start is the current end, exactly.
        let may_use_end = |key: &(String, i32)| {
            may_measure_from_end(
                answers.contains_key(key),
                committed.contains_key(key),
                &self.cfg.auto_offset_reset,
            )
        };
        let need_start: Vec<(String, i32)> = unresolved
            .iter()
            .filter(|key| may_use_end(key) && !self.liveness.has_baseline(key))
            .cloned()
            .collect();
        let end_measured: std::collections::HashSet<(String, i32)> = unresolved
            .iter()
            .filter(|key| may_use_end(key))
            .cloned()
            .collect();
        let starts = if need_start.is_empty() {
            None
        } else {
            let since = end_start_lookup_time(self.liveness.assigned_at_ms);
            self.consumer
                .as_ref()
                .and_then(|consumer| offsets_at_time(consumer, &need_start, |_| since))
        };

        let now = Instant::now();
        let mut stalled: Vec<(String, i32)> = Vec::new();
        let mut rebuild: Vec<((String, i32), i64)> = Vec::new();
        let mut reports: Vec<StallReport> = Vec::new();
        for key in &due {
            let Some(&high) = highs.get(key) else {
                continue;
            };
            let delivered = self.delivered.get(key).copied();
            if delivered.is_none() && !answers.contains_key(key) {
                // Where Stored resolved is unknown this time: no measure, no
                // baseline, no pin. Asked again at the next check.
                continue;
            }
            let resolved = next_read_offset(
                delivered,
                committed.get(key).copied(),
                lows.get(key).copied(),
                reset_to_earliest,
            );
            let next = if resolved.is_none() && end_measured.contains(key) {
                self.liveness.end_start(key, starts.as_ref(), high)
            } else {
                resolved
            };
            let Some(lag) = self.liveness.stalled_lag(key, next, high) else {
                continue;
            };
            // Measured from the end-of-log baseline: re-assign at THAT offset.
            // At Offset::Stored, latest would resolve to the new end and skip
            // every record that piled up during the stall (review of #644).
            // Pinned like a rewind's landing; never over a position this
            // process already holds, and never from the broker's group offset
            // (#614: the clamp must not come from the broker). Only reached
            // when the group answered that nothing is committed.
            if resolved.is_none() {
                if let Some(start) = next {
                    let cur = self.cursors.entry(key.clone()).or_default();
                    if cur.last_read_offset.is_none() && cur.committed_offset.is_none() {
                        cur.committed_offset = Some(start);
                    }
                }
            }
            let (idle_passes, idle_for) = self
                .liveness
                .idle
                .get(key)
                .map(|run| (run.passes, now.saturating_duration_since(run.since)))
                .unwrap_or_default();
            let recovery = self.liveness.recovery(key, high);
            match recovery {
                Recovery::Reassign => {
                    self.liveness.note_reassigned(key, high);
                    self.stall_reassigns += 1;
                    stalled.push(key.clone());
                }
                // Counted only once the rebuild happens: a deferred one must
                // not use up the partition's last recovery at this watermark.
                Recovery::RebuildConsumer => rebuild.push((key.clone(), high)),
            }
            reports.push(StallReport {
                key: key.clone(),
                next_offset: next.unwrap_or_default(),
                high,
                lag,
                idle_passes,
                idle_secs: idle_for.as_secs(),
                recovery,
            });
        }
        let rebuilt = !rebuild.is_empty() && self.rebuild_consumer_after_stall();
        if rebuilt {
            for (key, high) in &rebuild {
                self.liveness.note_reassigned(key, *high);
                self.stall_reassigns += 1;
            }
        } else if !stalled.is_empty() {
            self.reassign_stalled(&stalled);
        }
        // Logged once the outcome is known (review of #644).
        for report in reports {
            let StallReport {
                key,
                next_offset,
                high,
                lag,
                idle_passes,
                idle_secs,
                recovery,
            } = report;
            let outcome = match (rebuilt, recovery) {
                (true, _) => StallOutcome::Rebuilt,
                (false, Recovery::Reassign) => StallOutcome::Reassigned,
                (false, Recovery::RebuildConsumer) => StallOutcome::RebuildDeferred,
            };
            tracing::warn!(
                topic = %key.0,
                partition = key.1,
                next_offset,
                high_watermark = high,
                lag,
                idle_passes,
                idle_secs,
                stall_reassigns_total = self.stall_reassigns,
                outcome = ?outcome,
                "{}",
                outcome.message()
            );
        }
    }

    /// Restart the fetchers of `stalled` partitions ALONE (review of #644):
    /// unassign them and assign them again at their own position. A whole
    /// re-assign would restart every fetcher, and resolve every partition
    /// still at `Offset::Stored` again - for `latest`, at the NEW end of the
    /// log, skipping whatever arrived since its first assign. Should the
    /// consumer refuse the incremental calls, the whole assignment is renewed
    /// on the next pass instead, once every partition waiting at the end of
    /// the log is pinned where it started (`pin_latest_starts`, #675); when
    /// that cannot be established the assignment is kept for the next check.
    fn reassign_stalled(&mut self, stalled: &[(String, i32)]) {
        let positions: Vec<((String, i32), Offset)> = stalled
            .iter()
            .map(|key| {
                let cursor = self.cursors.get(key);
                let at = match (
                    cursor.and_then(|c| c.last_read_offset),
                    cursor.and_then(|c| c.committed_offset),
                ) {
                    (Some(read), _) => Offset::Offset(read + 1),
                    (None, Some(committed)) => Offset::Offset(committed),
                    (None, None) => Offset::Stored,
                };
                (key.clone(), at)
            })
            .collect();
        let mut plain = TopicPartitionList::new();
        let mut at = TopicPartitionList::new();
        let mut result = Ok(());
        for ((topic, partition), offset) in &positions {
            plain.add_partition(topic, *partition);
            if let Err(e) = at.add_partition_offset(topic, *partition, *offset) {
                result = Err(e);
                break;
            }
        }
        let Some(consumer) = self.consumer.as_ref() else {
            return;
        };
        let result = result.and_then(|()| {
            consumer
                .incremental_unassign(&plain)
                .and_then(|()| consumer.incremental_assign(&at))
                .and_then(|()| consumer.resume(&plain))
        });
        match result {
            Ok(()) => {
                for (key, offset) in positions {
                    self.liveness.idle.remove(&key);
                    match offset {
                        Offset::Offset(next) => self.delivered.insert(key, next),
                        _ => self.delivered.remove(&key),
                    };
                }
            }
            // A whole re-assign resolves every partition still at
            // `Offset::Stored` again, so those that started at the end of the
            // log are pinned first (#675). Without a pin it would skip what
            // arrived since their first assign, and nothing would recover it:
            // their new start is the new end.
            Err(e) => {
                let assigned = self.assigned.clone();
                if self.pin_latest_starts(&assigned).is_empty() {
                    tracing::warn!(
                        error = %e,
                        "kafka could not re-assign the stalled partitions alone; renewing the \
                         whole assignment instead (#623)"
                    );
                    self.assigned.clear();
                } else {
                    tracing::warn!(
                        error = %e,
                        "kafka could not re-assign the stalled partitions alone, and where a \
                         partition assigned at the end of the log started could not be \
                         established, so the assignment is kept; the next check rebuilds the \
                         consumer (#623, #675)"
                    );
                }
            }
        }
    }

    /// Drop the consumer and build a new one on the next pass, as a restart
    /// would (#623): a re-assign of the stalled partition did not bring it
    /// back, and no record has arrived since. Every position this process
    /// holds survives in its cursors. A partition still waiting on
    /// `Offset::Stored` under `latest`, with nothing committed, is first
    /// pinned where it started, so the new consumer does not resolve it at
    /// the new end of the log (review of #644). When that start cannot be
    /// established - the group or the broker does not answer - the rebuild
    /// waits for a later check and the consumer is kept: a failed lookup is
    /// not permission to discard the assignment. Returns whether it rebuilt.
    fn rebuild_consumer_after_stall(&mut self) -> bool {
        let assigned = self.assigned.clone();
        if !self.pin_latest_starts(&assigned).is_empty() {
            tracing::warn!(
                "kafka consumer rebuild deferred: where a partition assigned at the end of the \
                 log started could not be established, and a new consumer would skip what \
                 arrived since; asked again at the next check (#623)"
            );
            return false;
        }
        self.consumer = None;
        self.assigned.clear();
        self.delivered.clear();
        self.paused.clear();
        true
    }

    /// Before anything assigns partitions again (a whole re-assign, a rebuilt
    /// consumer): pin each of `candidates` that is still waiting on
    /// `Offset::Stored` under `latest` - assigned there before, nothing
    /// delivered, no position of its own - where it started. Assigned at
    /// `Offset::Stored` again, it would resolve at the NEW end of the log and
    /// skip what arrived since its first assign (review of #644; #675).
    ///
    /// Only `candidates` are pinned: a partition leaving the assignment (its
    /// topic deleted, say) needs no start, and asking the broker about it
    /// would only fail (#675 follow-up). Returns the partitions whose start
    /// could not be established; each is asked again no sooner than
    /// `STALL_CHECK_INTERVAL` later.
    fn pin_latest_starts(&mut self, candidates: &[(String, i32)]) -> Vec<(String, i32)> {
        if !resets_to_latest(&self.cfg.auto_offset_reset) {
            return Vec::new();
        }
        let now = Instant::now();
        let waiting: Vec<(String, i32)> = candidates
            .iter()
            .filter(|key| {
                self.stored_since.contains_key(*key)
                    && !self.delivered.contains_key(*key)
                    && self.cursors.get(*key).is_none_or(|c| {
                        c.last_read_offset.is_none() && c.committed_offset.is_none()
                    })
            })
            .cloned()
            .collect();
        let (due, mut unpinned): (Vec<_>, Vec<_>) = waiting
            .into_iter()
            .partition(|key| self.pin_deferred.get(key).is_none_or(|at| now >= *at));
        if due.is_empty() {
            return unpinned;
        }
        let (pins, unresolved) = self.latest_starts(&due);
        for (key, start) in pins {
            tracing::info!(
                topic = %key.0,
                partition = key.1,
                offset = start,
                "kafka partition still waiting at the end of the log is pinned where it \
                 started before it is assigned again, so what arrived since is read, not \
                 skipped (#675)"
            );
            count_partition_event("pinned_before_renewal");
            self.pin_deferred.remove(&key);
            self.stored_since.remove(&key);
            self.cursors.entry(key).or_default().committed_offset = Some(start);
        }
        for key in unresolved {
            count_partition_event("start_deferred");
            let subject = format!("pin:{}:{}", key.0, key.1);
            if let Some(suppressed) = self.warnings.allow(&subject, now) {
                tracing::warn!(
                    topic = %key.0,
                    partition = key.1,
                    suppressed,
                    "kafka partition still waiting at the end of the log is left out of the \
                     assignment: where it started could not be established (the group or the \
                     broker did not answer for it), and assigning it without that would skip \
                     what arrived since. Asked again shortly; nothing is skipped, and every \
                     other partition is read. Check the partition's leader and the group \
                     coordinator if this repeats (#675)"
                );
            }
            self.pin_deferred
                .insert(key.clone(), now + STALL_CHECK_INTERVAL);
            unpinned.push(key);
        }
        unpinned
    }

    /// Where each of `keys` - a partition born after this process read its
    /// topic, with no position of its own - starts (#675 follow-up). Under
    /// `latest`: the group's committed offset when it has one, else the
    /// partition's beginning, pinned in its cursor; every record in it is
    /// newer than the start `latest` promised when this process began reading
    /// the topic. `Offset::Stored` would resolve to its END and skip what was
    /// written to it before the assign. A partition whose start cannot be
    /// established (the group or the broker did not answer) is left out of
    /// this pass and asked again at the next. Other policies resolve at
    /// `Offset::Stored` as before: `earliest` already starts at the beginning.
    fn start_born_partitions(&mut self, keys: &[(String, i32)]) -> Vec<((String, i32), Offset)> {
        if keys.is_empty() {
            return Vec::new();
        }
        if !resets_to_latest(&self.cfg.auto_offset_reset) {
            for key in keys {
                self.partitions.settle_born(key);
            }
            return keys
                .iter()
                .map(|key| (key.clone(), Offset::Stored))
                .collect();
        }
        let answers = self.group_committed_answers(keys);
        let need_low: Vec<(String, i32)> = keys
            .iter()
            .filter(|key| matches!(answers.as_ref().and_then(|a| a.get(*key)), Some(None)))
            .cloned()
            .collect();
        let lows = match self.consumer.as_ref() {
            Some(consumer) => list_offsets(consumer, &need_low, Offset::Beginning),
            None => HashMap::new(),
        };
        let mut starts = Vec::new();
        for key in keys {
            match (answers.as_ref().and_then(|a| a.get(key)), lows.get(key)) {
                (Some(Some(committed)), _) => {
                    tracing::info!(
                        topic = %key.0,
                        partition = key.1,
                        offset = *committed,
                        "kafka partition added after this consumer began reading its topic is \
                         assigned from the group's committed offset (#675)"
                    );
                    self.partitions.settle_born(key);
                    starts.push((key.clone(), Offset::Stored));
                }
                (Some(None), Some(&low)) => {
                    tracing::info!(
                        topic = %key.0,
                        partition = key.1,
                        offset = low,
                        "kafka partition added after this consumer began reading its topic is \
                         assigned from its beginning, with nothing committed for it, so no \
                         record written to it is skipped (#675)"
                    );
                    self.cursors
                        .entry(key.clone())
                        .or_default()
                        .committed_offset = Some(low);
                    self.partitions.settle_born(key);
                    count_partition_event("new_partition_from_beginning");
                    starts.push((key.clone(), Offset::Offset(low)));
                }
                _ => {
                    count_partition_event("start_deferred");
                    let subject = format!("born:{}:{}", key.0, key.1);
                    if let Some(suppressed) = self.warnings.allow(&subject, Instant::now()) {
                        tracing::warn!(
                            topic = %key.0,
                            partition = key.1,
                            suppressed,
                            "kafka partition added after this consumer began reading its topic \
                             is not assigned yet: where it starts could not be established (the \
                             group or the broker did not answer). Asked again next pass; \
                             nothing is skipped. Check the group coordinator if this repeats \
                             (#675)"
                        );
                    }
                }
            }
        }
        starts
    }

    /// Where each of `waiting` (assigned, `latest`, nothing delivered or held
    /// in its cursor) started, for pinning before it is assigned again (review
    /// of #644). An end-of-log baseline the stall check already established is
    /// used as it is; otherwise the group is asked, and for a partition with
    /// nothing committed, the broker for the first offset since the assign
    /// that put it at `Offset::Stored` (the end of the log when there is
    /// none). A partition with a commit needs no pin - `Offset::Stored`
    /// resolves to that commit - and stops counting as waiting, so it is not
    /// asked about again (#675 follow-up). Returns the pins, and the
    /// partitions whose start could not be established.
    fn latest_starts(
        &mut self,
        waiting: &[(String, i32)],
    ) -> (Vec<(PartitionKey, i64)>, Vec<PartitionKey>) {
        let mut pins = Vec::new();
        let mut unknown = Vec::new();
        for key in waiting {
            match self.liveness.baseline(key) {
                Some(start) => pins.push((key.clone(), start)),
                None => unknown.push(key.clone()),
            }
        }
        if unknown.is_empty() {
            return (pins, Vec::new());
        }
        let Some(answers) = self.group_committed_answers(&unknown) else {
            return (pins, unknown);
        };
        let mut uncommitted = Vec::new();
        let mut unresolved = Vec::new();
        for key in unknown {
            match answers.get(&key) {
                Some(Some(_committed)) => {
                    self.stored_since.remove(&key);
                }
                Some(None) => uncommitted.push(key),
                None => unresolved.push(key),
            }
        }
        if uncommitted.is_empty() {
            return (pins, unresolved);
        }
        let Some(consumer) = self.consumer.as_ref() else {
            unresolved.extend(uncommitted);
            return (pins, unresolved);
        };
        let fallback = self.liveness.assigned_at_ms;
        let stored_since = &self.stored_since;
        let since = |key: &(String, i32)| {
            end_start_lookup_time(stored_since.get(key).copied().unwrap_or(fallback))
        };
        for (key, start) in end_of_log_starts_each(
            &uncommitted,
            |keys| list_offsets(consumer, keys, Offset::End),
            |keys| offsets_at_time(consumer, keys, since),
        ) {
            match start {
                Some(start) => pins.push((key, start)),
                None => unresolved.push(key),
            }
        }
        (pins, unresolved)
    }

    /// TEST SEAM (#623): how many stall recoveries this adapter has made, so
    /// a live test can act in the gap right after one is decided.
    #[doc(hidden)]
    pub fn stall_recoveries(&self) -> u64 {
        self.stall_reassigns
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

    /// The group's committed offsets for the stall check, telling "nothing
    /// committed" apart from "could not ask" (review of #644): `None` when
    /// the lookup failed, else per partition `Some(offset)`, or `None` for an
    /// answer of nothing committed. A partition whose answer carries an error
    /// is left out: it is unknown, like a failed lookup.
    fn group_committed_answers(
        &mut self,
        partitions: &[(String, i32)],
    ) -> Option<HashMap<(String, i32), Option<i64>>> {
        if partitions.is_empty() {
            return Some(HashMap::new());
        }
        let consumer = self.consumer().ok()?;
        let mut wanted = TopicPartitionList::new();
        for (topic, partition) in partitions {
            wanted.add_partition(topic, *partition);
        }
        let committed = consumer
            .committed_offsets(wanted, Duration::from_secs(WATERMARK_TIMEOUT_SECONDS))
            .ok()?;
        let mut answers = HashMap::new();
        for (topic, partition) in partitions {
            let Some(element) = committed.find_partition(topic, *partition) else {
                continue;
            };
            if element.error().is_err() {
                continue;
            }
            let at = match element.offset() {
                Offset::Offset(at) => Some(at),
                _ => None,
            };
            answers.insert((topic.clone(), *partition), at);
        }
        Some(answers)
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
        self.partitions.retain(&live);
        self.warnings.retain_topics(&live);
        self.stored_since
            .retain(|(topic, _), _| live.contains(topic.as_str()));
        self.pin_deferred
            .retain(|(topic, _), _| live.contains(topic.as_str()));
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
        self.consumer()?;
        let consumer = self.consumer.as_ref().expect("consumer built above");
        self.partitions.ids(topic, Instant::now(), |topic| {
            fetch_partition_ids(consumer, topic)
        })
    }

    /// Re-check the partition counts of every cached topic, at most once per
    /// `PARTITION_RECHECK_INTERVAL`, in ONE metadata request (#675 follow-up).
    /// A partition added to a topic this adapter reads was otherwise found
    /// only when the topic's entry expired, up to `PARTITION_CACHE_TTL` later,
    /// and until then it was in no assignment, so the stall check never saw
    /// it either. A failed request changes nothing; the next one is due one
    /// interval later.
    fn recheck_partition_counts(&mut self) {
        let now = Instant::now();
        // Discovery's own listing, when the engine discovered this cycle: no
        // request of our own, and a re-check every cycle.
        let from_discovery = self
            .discovered
            .get_mut()
            .ok()
            .and_then(|discovered| discovered.take());
        if let Some(listed) = from_discovery {
            self.partitions.rechecked(now);
            for topic in self.partitions.cached_topics() {
                if let Some(ids) = listed.get(&topic).filter(|ids| !ids.is_empty()) {
                    self.partitions.observe(&topic, ids, now);
                }
            }
            return;
        }
        if !self.partitions.recheck_due(now) {
            return;
        }
        let Some(consumer) = self.consumer.as_ref() else {
            return;
        };
        let listed = match consumer.fetch_metadata(None, Duration::from_secs(10)) {
            Ok(md) => md
                .topics()
                .iter()
                .map(|t| {
                    let ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
                    (t.name().to_string(), ids)
                })
                .collect::<HashMap<String, Vec<i32>>>(),
            Err(e) => {
                if let Some(suppressed) = self.warnings.allow("recheck", now) {
                    tracing::warn!(
                        error = %e,
                        suppressed,
                        "kafka partition re-check failed: partitions added since the last \
                         answer are found at the next re-check or when a topic's cached list \
                         expires; nothing already assigned is affected. Check the broker's \
                         metadata service if this repeats (#675)"
                    );
                }
                return;
            }
        };
        for topic in self.partitions.cached_topics() {
            if let Some(ids) = listed.get(&topic).filter(|ids| !ids.is_empty()) {
                self.partitions.observe(&topic, ids, now);
            }
        }
    }
}

/// The partition ids a metadata request lists for `topic`; empty when the
/// broker does not list the topic or lists it without partitions.
fn fetch_partition_ids(consumer: &BaseConsumer, topic: &str) -> Result<Vec<i32>, VtopError> {
    let md = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| VtopError::Source(format!("fetch_metadata {topic}: {e}")))?;
    let answer = md.topics().iter().find(|t| t.name() == topic).map(|t| {
        let ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
        (t.error(), ids)
    });
    partition_ids_from_answer(topic, answer)
}

/// The partition ids in one topic's metadata answer (#675 follow-up). No
/// entry, or "unknown topic", is a topic the broker does not know: no
/// partitions. Any other topic-level error with no partitions listed - an
/// authorization failure, a leader election - is a failed lookup, not an
/// absent topic: reporting it as absent would later call the topic
/// recreated and read it from its beginning. Partitions listed beside an
/// error are still its partitions.
fn partition_ids_from_answer(
    topic: &str,
    answer: Option<(Option<rdkafka::types::RDKafkaRespErr>, Vec<i32>)>,
) -> Result<Vec<i32>, VtopError> {
    use rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
    match answer {
        None => Ok(Vec::new()),
        Some((Some(error), ids))
            if ids.is_empty() && error != RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART =>
        {
            Err(VtopError::Source(format!(
                "fetch_metadata {topic}: the broker answered {error:?}"
            )))
        }
        Some((_, ids)) => Ok(ids),
    }
}

/// How often the read path re-checks the partition counts of the topics it
/// reads when no discovery listing came since the last re-check (#675
/// follow-up; the engine discovers every cycle, so it re-checks from that
/// listing instead, with no request of its own). One all-topic metadata
/// request per interval,
/// whatever the topic count, rather than a shorter per-topic TTL, which would
/// multiply the per-topic requests the cache exists to avoid. A partition
/// added in between is assigned from its beginning when nothing is committed
/// for it, so the interval bounds how late it is read, not what is read.
const PARTITION_RECHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Count one partition-list event (#675), when metrics are on.
fn count_partition_event(event: &str) {
    if let Some(metrics) = vtop_core::telemetry::metrics() {
        metrics
            .kafka_partition_events_total
            .with_label_values(&[event])
            .inc();
    }
}

/// How often one warning repeats for one subject while its condition lasts
/// (#675 follow-up): a condition asked about on every pass would otherwise log
/// on every pass.
const WARN_EVERY: Duration = Duration::from_secs(60);

/// Rate limit for warnings about a condition that persists across passes.
#[derive(Debug, Default)]
struct WarnLimiter {
    /// Per subject: when it was last logged, and how many were held back since.
    last: HashMap<String, (Instant, u64)>,
}

impl WarnLimiter {
    /// Drop subjects of topics no longer `live`. A subject is `kind`,
    /// `kind:topic` or `kind:topic:partition`; Kafka topic names cannot
    /// contain ':'. Subjects without a topic are kept.
    fn retain_topics(&mut self, live: &HashSet<&str>) {
        self.last.retain(|subject, _| {
            subject
                .split(':')
                .nth(1)
                .is_none_or(|topic| live.contains(topic))
        });
    }

    /// `Some(held back since the last one)` when `subject` may be logged now.
    fn allow(&mut self, subject: &str, now: Instant) -> Option<u64> {
        match self.last.get_mut(subject) {
            Some((at, held)) if now.saturating_duration_since(*at) < WARN_EVERY => {
                *held += 1;
                None
            }
            Some((at, held)) => {
                let suppressed = *held;
                *at = now;
                *held = 0;
                Some(suppressed)
            }
            None => {
                self.last.insert(subject.to_string(), (now, 0));
                Some(0)
            }
        }
    }
}

/// What one metadata answer for a topic changed (#675 follow-up).
#[derive(Debug, Clone, PartialEq, Eq)]
enum PartitionChange {
    /// The same partitions as before.
    Unchanged,
    /// The first answer for the topic in this process.
    First,
    /// The topic listed no partitions.
    NoneListed,
    /// Partitions after an answer that listed none; `recreated` when the
    /// topic had listed partitions before that.
    Appeared { recreated: bool },
    /// More partitions than before; `added` are the new ones.
    Grew {
        from: usize,
        to: usize,
        added: Vec<i32>,
    },
    /// Fewer partitions than before. Kafka cannot remove partitions, so the
    /// topic was deleted and recreated.
    Shrank { from: usize, to: usize },
}

/// Classify an answer listing `ids` for a topic that last listed `previous`
/// (`None`: never answered, or listed nothing), `absent` when the last answer
/// listed nothing, `had_partitions` when it ever listed some.
fn classify_partition_answer(
    previous: Option<&[i32]>,
    absent: bool,
    had_partitions: bool,
    ids: &[i32],
) -> PartitionChange {
    if ids.is_empty() {
        return PartitionChange::NoneListed;
    }
    if absent {
        return PartitionChange::Appeared {
            recreated: had_partitions,
        };
    }
    let Some(previous) = previous else {
        return PartitionChange::First;
    };
    if ids.len() < previous.len() {
        return PartitionChange::Shrank {
            from: previous.len(),
            to: ids.len(),
        };
    }
    let added: Vec<i32> = ids
        .iter()
        .copied()
        .filter(|id| !previous.contains(id))
        .collect();
    if added.is_empty() {
        PartitionChange::Unchanged
    } else {
        PartitionChange::Grew {
            from: previous.len(),
            to: ids.len(),
            added,
        }
    }
}

/// Partition ids per topic, and what the answers that filled it changed
/// (#675 and its follow-up). Kept free of rdkafka types, and the clock and
/// the lookup are passed in, so the decisions are unit-testable.
#[derive(Debug, Default)]
struct PartitionCache {
    /// The last list each topic answered with, and when. Only lists that
    /// name partitions are kept.
    entries: HashMap<String, (Vec<i32>, Instant)>,
    /// Topics whose last answer listed no partitions, and whether they had
    /// listed partitions before that.
    absent: HashMap<String, bool>,
    /// Partitions that did not exist when this process first read their
    /// topic (added later, or the topic was created after it was asked for).
    /// Every record in one is newer than the start `latest` promised, so one
    /// with nothing committed is assigned from its beginning, never at
    /// `Offset::Stored`, which under `latest` resolves to its END and skips
    /// what was written to it before the assign.
    born: HashSet<(String, i32)>,
    /// When the partition counts were last re-checked.
    last_recheck: Option<Instant>,
    /// Set once the first read pass has asked for its topics. A topic first
    /// answered for after that was created while this process was reading
    /// (the engine reads every topic discovery lists, every pass), so it is
    /// born too: under `latest` its start is when this process began
    /// reading, not when it got round to the topic (#675 follow-up).
    initial_done: bool,
    /// The topics that first pass asked for: whatever they answer later,
    /// they are not new.
    initial_topics: HashSet<String>,
    /// Rate limit for repeated lookup warnings.
    warnings: WarnLimiter,
}

impl PartitionCache {
    /// Partition ids for `topic`: from the cache while its entry is younger
    /// than `PARTITION_CACHE_TTL`, else from `lookup`.
    ///
    /// An answer that lists NO partitions is returned but never cached (#675).
    /// A broker answers that way for a topic it does not know yet - one
    /// created moments before, whose metadata the broker has not applied - as
    /// well as for one just deleted. Cached, the topic was left out of every
    /// assignment for the whole TTL, five minutes: none of its records were
    /// read, and the stall check never saw it, since it watches assigned
    /// partitions only.
    ///
    /// A failed lookup serves the last list the topic answered with (#675
    /// follow-up): its partitions cannot have gone - only a recreate changes
    /// them, and that shows as a later answer. Failing instead dropped the
    /// topic from the assignment set, which renewed every other partition's
    /// assignment too. A topic never answered for has nothing to serve.
    fn ids(
        &mut self,
        topic: &str,
        now: Instant,
        lookup: impl FnOnce(&str) -> Result<Vec<i32>, VtopError>,
    ) -> Result<Vec<i32>, VtopError> {
        if let Some((ids, at)) = self.entries.get(topic) {
            if now.saturating_duration_since(*at) < PARTITION_CACHE_TTL {
                return Ok(ids.clone());
            }
        }
        match lookup(topic) {
            Ok(ids) => {
                self.observe(topic, &ids, now);
                Ok(ids)
            }
            Err(e) => {
                count_partition_event("lookup_failed");
                let stale = self
                    .entries
                    .get(topic)
                    .map(|(ids, at)| (ids.clone(), now.saturating_duration_since(*at).as_secs()));
                let held = self.warnings.allow(&format!("lookup:{topic}"), now);
                match stale {
                    Some((ids, age_secs)) => {
                        if let Some(suppressed) = held {
                            tracing::warn!(
                                topic,
                                error = %e,
                                partitions = ids.len(),
                                age_secs,
                                suppressed,
                                "kafka partition lookup failed; serving the topic's last known \
                                 partition list and asking again next pass. Nothing is skipped; \
                                 check the broker's metadata service if this repeats (#675)"
                            );
                        }
                        Ok(ids)
                    }
                    None => {
                        if let Some(suppressed) = held {
                            tracing::warn!(
                                topic,
                                error = %e,
                                suppressed,
                                "kafka partition lookup failed for a topic never answered for; \
                                 it is not read this pass and is asked again next pass. Check \
                                 the broker's metadata service if this repeats (#675)"
                            );
                        }
                        Err(e)
                    }
                }
            }
        }
    }

    /// Record an answer listing `ids` for `topic`, log what it changed, and
    /// mark the partitions that were born after this process read the topic.
    fn observe(&mut self, topic: &str, ids: &[i32], now: Instant) {
        let previous = self.entries.get(topic).map(|(ids, _)| ids.clone());
        let absent = self.absent.get(topic).copied();
        let change = classify_partition_answer(
            previous.as_deref(),
            absent.is_some(),
            absent.unwrap_or(false) || previous.is_some(),
            ids,
        );
        match &change {
            PartitionChange::NoneListed => {
                let had = absent.unwrap_or(false) || previous.is_some();
                self.entries.remove(topic);
                self.absent.insert(topic.to_string(), had);
                count_partition_event("no_partitions");
                if let Some(suppressed) = self.warnings.allow(&format!("none:{topic}"), now) {
                    tracing::warn!(
                        topic,
                        suppressed,
                        "kafka topic listed with no partitions: the broker does not know it \
                         (created moments ago, or deleted). Not cached; asked again next pass. \
                         Once listed, its partitions start from the group's committed offset, \
                         else from their beginning under latest (per auto.offset.reset \
                         otherwise). If the topic should exist, check that it does (#675)"
                    );
                }
                return;
            }
            PartitionChange::Appeared { recreated: false } => {
                tracing::info!(
                    topic,
                    partitions = ids.len(),
                    "kafka topic now lists partitions after listing none: it did not exist when \
                     this consumer asked for it, so its partitions are assigned from the \
                     group's committed offset, else from their beginning (#675)"
                );
                self.born
                    .extend(ids.iter().map(|&p| (topic.to_string(), p)));
            }
            PartitionChange::Appeared { recreated: true } => {
                count_partition_event("recreated_or_truncated");
                tracing::warn!(
                    topic,
                    partitions = ids.len(),
                    "kafka topic lists partitions again after listing none: it was deleted and \
                     recreated. A partition this consumer has no position for is assigned from \
                     the group's committed offset, else its beginning. One it read before keeps \
                     its old position: if the new log is shorter, the broker answers it out of \
                     range and auto.offset.reset applies; if the new log already reaches past \
                     it, the records below it are skipped without any error. The ledger cannot \
                     tell the two topics apart: check what was archived for it, and restart \
                     the engine with auto.offset.reset=earliest if the new topic must be read \
                     from its start (#675)"
                );
                self.born
                    .extend(ids.iter().map(|&p| (topic.to_string(), p)));
            }
            PartitionChange::Grew { from, to, added } => {
                count_partition_event("grew");
                tracing::info!(
                    topic,
                    from,
                    to,
                    added = ?added,
                    "kafka topic grew: its new partitions are assigned on the next pass, from \
                     the group's committed offset, else from their beginning, so nothing \
                     written to them is skipped (#675)"
                );
                self.born
                    .extend(added.iter().map(|&p| (topic.to_string(), p)));
            }
            PartitionChange::Shrank { from, to } => {
                count_partition_event("recreated_or_truncated");
                tracing::warn!(
                    topic,
                    from,
                    to,
                    "kafka topic lists fewer partitions than before: Kafka cannot remove \
                     partitions, so the topic was deleted and recreated. The new list is \
                     assigned; positions held for the old topic are kept. Where the new log is \
                     shorter, the broker answers them out of range and auto.offset.reset \
                     applies; where it already reaches past them, the records below are \
                     skipped without any error. The ledger cannot tell the two topics apart: \
                     check what was archived for it, and restart the engine with \
                     auto.offset.reset=earliest if the new topic must be read from its start \
                     (#675)"
                );
            }
            PartitionChange::First if self.initial_done && !self.initial_topics.contains(topic) => {
                tracing::info!(
                    topic,
                    partitions = ids.len(),
                    "kafka topic first read after this consumer started: it was created since, \
                     so its partitions are assigned from the group's committed offset, else \
                     from their beginning under latest, and nothing written to it is skipped \
                     (#675)"
                );
                self.born
                    .extend(ids.iter().map(|&p| (topic.to_string(), p)));
            }
            PartitionChange::First | PartitionChange::Unchanged => {}
        }
        self.absent.remove(topic);
        self.entries.insert(topic.to_string(), (ids.to_vec(), now));
    }

    /// Whether a re-check of the cached partition counts is due; the first
    /// call only starts the clock.
    fn recheck_due(&mut self, now: Instant) -> bool {
        match self.last_recheck {
            Some(at) if now.saturating_duration_since(at) < PARTITION_RECHECK_INTERVAL => false,
            Some(_) => {
                self.last_recheck = Some(now);
                true
            }
            None => {
                self.last_recheck = Some(now);
                false
            }
        }
    }

    /// The first read pass has asked for `topics`, the starting topic set.
    fn initial_pass_done<'a>(&mut self, topics: impl Iterator<Item = &'a str>) {
        if !self.initial_done {
            self.initial_done = true;
            self.initial_topics = topics.map(str::to_string).collect();
        }
    }

    /// A partition-count re-check happened now, from discovery's listing.
    fn rechecked(&mut self, now: Instant) {
        self.last_recheck = Some(now);
    }

    fn cached_topics(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    fn is_born(&self, key: &(String, i32)) -> bool {
        self.born.contains(key)
    }

    /// The partition's start is decided (or it has a position of its own).
    fn settle_born(&mut self, key: &(String, i32)) {
        self.born.remove(key);
    }

    /// Keep only `live` topics.
    fn retain(&mut self, live: &std::collections::HashSet<&str>) {
        self.entries
            .retain(|topic, _| live.contains(topic.as_str()));
        self.absent.retain(|topic, _| live.contains(topic.as_str()));
        self.born.retain(|(topic, _)| live.contains(topic.as_str()));
        self.initial_topics
            .retain(|topic| live.contains(topic.as_str()));
        self.warnings.retain_topics(live);
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
        let mut listed = HashMap::new();
        for t in metadata.topics() {
            if self.topic_allowed(t.name()) {
                listed.insert(
                    t.name().to_string(),
                    t.partitions().iter().map(|p| p.id()).collect::<Vec<i32>>(),
                );
                out.push(DiscoveredSource {
                    source_type: SourceType::Kafka,
                    source_name: t.name().to_string(),
                    format: self.format.clone(),
                });
            }
        }
        // Handed to the next read pass, which re-checks partition counts from
        // it instead of asking again (#675 follow-up).
        if let Ok(mut discovered) = self.discovered.lock() {
            *discovered = Some(listed);
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
        self.recheck_partition_counts();
        // A metadata failure for ONE topic must not abort the pass for the
        // others — the serial loop isolated it per source, and so does this:
        // the failing topic is skipped and its outcome carries the error.
        let mut source_errors: Vec<Option<VtopError>> = Vec::with_capacity(sources.len());
        // Partition lists aligned with `sources`, kept so a topic that fills
        // its budget mid-pass can be pause()d without refetching metadata.
        let mut partitions_by_source: Vec<Vec<i32>> = Vec::with_capacity(sources.len());
        // (topic, partition, desired next offset) for every readable partition,
        // plus the sorted set for the sticky-assignment comparison.
        let mut desired: Vec<(String, i32, Offset)> = Vec::new();
        let mut assignment_set: Vec<(String, i32)> = Vec::new();
        // Partitions born after this process read their topic, with no
        // position of their own: where they start is decided below.
        let mut born_waiting: Vec<(String, i32)> = Vec::new();
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
            partitions_by_source.push(partitions.clone());
            for p in partitions {
                let key = (topic.clone(), p);
                if self.partitions.is_born(&key) {
                    let cur = self.cursors.entry(key.clone()).or_default();
                    if cur.last_read_offset.is_none() && cur.committed_offset.is_none() {
                        born_waiting.push(key);
                        continue;
                    }
                    self.partitions.settle_born(&key);
                }
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
        for ((topic, p), off) in self.start_born_partitions(&born_waiting) {
            assignment_set.push((topic.clone(), p));
            desired.push((topic, p, off));
        }
        self.partitions
            .initial_pass_done(sources.iter().map(|s| s.source_name.as_str()));
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
        // A whole re-assign assigns every partition still at `Offset::Stored`
        // again; under `latest`, one assigned there before that has not
        // delivered would resolve at the NEW end and skip what arrived since,
        // and nothing would recover it: its new start is the new end (#675
        // follow-up). Pin those first. A partition whose start cannot be
        // established is left out of the assignment, never assigned at
        // `Offset::Stored`, and asked again shortly; every other partition is
        // assigned and read. Once it is left out, the set without it matches
        // the assignment, so later passes keep their fetchers warm.
        if self.assigned != assignment_set {
            let unpinned = self.pin_latest_starts(&assignment_set);
            if !unpinned.is_empty() {
                desired.retain(|(topic, p, _)| !unpinned.contains(&(topic.clone(), *p)));
                assignment_set.retain(|key| !unpinned.contains(key));
            }
            for (topic, p, off) in &mut desired {
                if matches!(off, Offset::Stored) {
                    if let Some(pin) = self
                        .cursors
                        .get(&(topic.clone(), *p))
                        .and_then(|c| c.committed_offset)
                    {
                        *off = Offset::Offset(pin);
                    }
                }
            }
        }
        // Topics that contributed at least one partition to the assignment —
        // the only topics that can ever fill up, so the count the poll loop's
        // all-full early exit must compare against. Counted after the starts
        // are decided: a topic whose every partition waits for its start
        // cannot fill up (review of #675).
        let topics_assigned: HashSet<&str> = assignment_set
            .iter()
            .map(|(topic, _)| topic.as_str())
            .collect();
        let assigned_topics = sources
            .iter()
            .filter(|s| topics_assigned.contains(s.source_name.as_str()))
            .count();
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
            // Under `latest`, where each partition assigned at
            // `Offset::Stored` started: any later assign must pin it there.
            if resets_to_latest(&self.cfg.auto_offset_reset) {
                for (topic, p, off) in &desired {
                    if matches!(off, Offset::Stored) {
                        self.stored_since
                            .entry((topic.clone(), *p))
                            .or_insert(self.liveness.assigned_at_ms);
                    }
                }
            }
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
        // The first offset each partition delivered this pass, kept or not.
        let mut first_delivered: HashMap<(usize, i32), i64> = HashMap::new();
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
                        first_delivered
                            .entry((idx, msg.partition()))
                            .or_insert(msg.offset());
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
        // A partition with no position of its own whose every record this
        // pass was rejected (its topic filled its budget first) or discarded
        // (an oversized record failed its topic) is pinned at the first one:
        // the forced re-assign would otherwise put it back at
        // `Offset::Stored`, which under `latest` resolves past them (review of
        // #675). Exact, with no broker call.
        for (&(idx, p), &first) in &first_delivered {
            if acc.parts.contains_key(&(idx, p)) {
                continue;
            }
            let cur = self
                .cursors
                .entry((sources[idx].source_name.clone(), p))
                .or_default();
            if cur.last_read_offset.is_none() && cur.committed_offset.is_none() {
                cur.committed_offset = Some(first);
            }
        }
        let progressed: std::collections::HashSet<(usize, i32)> =
            delivered_local.keys().copied().collect();
        for &(idx, p) in &progressed {
            self.stored_since
                .remove(&(sources[idx].source_name.clone(), p));
        }
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
                        .is_some_and(|&idx| progressed.contains(&(idx, *p)))
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

    /// The known frontier (`known_committed`): the ledger's newest committed
    /// row for this partition and group, or what this process committed
    /// since. Never a rewind's landing, and never the broker's group offset,
    /// which an engine whose claim moved on can still write (#614). Another
    /// group's range is not answered: this group's frontier says nothing
    /// about it.
    fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
        let ProgressMarker::Kafka {
            topic, partition, ..
        } = marker
        else {
            return None;
        };
        if self.foreign_group(marker).is_some() {
            return None;
        }
        self.known_committed
            .get(&(topic.clone(), *partition))
            .map(|&frontier| i128::from(frontier))
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

    /// Review of #644: two partitions of ONE topic, one busy enough to fill
    /// the topic's budget on every pass, the other stopped. The topic being
    /// full must not count as progress for the stopped partition: it is due
    /// like any other silent partition (and the lag check then decides).
    #[test]
    fn a_stopped_partition_next_to_one_that_fills_the_topic_is_found_stalled() {
        let assigned = vec![tp("a", 0), tp("a", 1)];
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        let start = Instant::now();
        let stopped = tp("a", 1);
        let passes = (STALL_AFTER.as_millis() / PASS.as_millis()) as u32 + 1;
        let mut due = Vec::new();
        for pass in 1..=passes {
            let now = start + PASS * pass;
            live.record_pass(&assigned, |key| *key != stopped, now);
            due.extend(live.due(now));
        }
        assert_eq!(due, vec![stopped]);
    }

    /// Review of #644: the "already re-assigned at this high watermark"
    /// bookmark is dropped once the partition delivers again, so a later
    /// stall at the same watermark (a backlog left after a partial read) is
    /// still recovered.
    #[test]
    fn a_partition_that_delivers_again_can_be_recovered_at_the_same_watermark() {
        let key = tp("a", 0);
        let assigned = vec![key.clone()];
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);
        live.note_reassigned(&key, 100);
        live.note_reassigned(&key, 100);
        assert_eq!(live.stalled_lag(&key, Some(40), 100), None);
        live.record_pass(&assigned, |_| true, Instant::now());
        assert_eq!(live.stalled_lag(&key, Some(60), 100), Some(40));
        assert_eq!(live.recovery(&key, 100), Recovery::Reassign);
    }

    /// #623, from a run of the research benchmark: a partition sat at a
    /// static high watermark (the producer had finished) until a restart. A
    /// re-assign that does not bring it back must not be the last word: the
    /// second recovery at the same watermark builds a new consumer, as a
    /// restart does. A new record starts the ladder over.
    #[test]
    fn a_stall_a_reassign_did_not_clear_rebuilds_the_consumer() {
        let key = tp("a", 0);
        let mut live = PartitionLiveness::default();
        assert_eq!(live.recovery(&key, 655_147), Recovery::Reassign);
        live.note_reassigned(&key, 655_147);
        assert_eq!(live.stalled_lag(&key, Some(77_054), 655_147), Some(578_093));
        assert_eq!(live.recovery(&key, 655_147), Recovery::RebuildConsumer);
        live.note_reassigned(&key, 655_147);
        assert_eq!(live.stalled_lag(&key, Some(77_054), 655_147), None);
        assert_eq!(live.recovery(&key, 655_148), Recovery::Reassign);
    }

    /// Review of #644: the end-of-log fallback is the high watermark read
    /// BEFORE the timestamp lookup. Read after it, a record appended in
    /// between (offset 50 here) lands below the pin and is skipped for good,
    /// although it arrived after the assign; read before, the timestamp
    /// lookup sees that record and the pin stays at or below it.
    #[test]
    fn the_end_of_log_fallback_is_read_before_the_timestamp_lookup() {
        let key = tp("t", 0);
        let log_end = std::cell::Cell::new(50_i64);
        let pins = end_of_log_starts(
            std::slice::from_ref(&key),
            |keys| keys.iter().map(|k| (k.clone(), log_end.get())).collect(),
            |keys| {
                // Nothing since the assign yet - then a producer appends 50.
                let answer = keys.iter().map(|k| (k.clone(), None)).collect();
                log_end.set(51);
                Some(answer)
            },
        );
        assert_eq!(
            pins,
            Some(vec![(key, 50)]),
            "offset 50 arrived after the assign and must still be read"
        );
    }

    /// Review of #644: only an outcome that restarted something says so. A
    /// deferred rebuild must not read as a recovery.
    #[test]
    fn a_deferred_rebuild_is_not_logged_as_a_recovery() {
        let deferred = StallOutcome::RebuildDeferred.message();
        assert!(!deferred.contains("restarted") && !deferred.contains("re-assigned"));
        assert!(deferred.contains("deferred"));
        assert_ne!(
            StallOutcome::Reassigned.message(),
            StallOutcome::Rebuilt.message()
        );
    }

    fn latest_source_assigned_to(keys: &[(String, i32)]) -> KafkaSource {
        let mut cfg = cfg();
        cfg.auto_offset_reset = "latest".into();
        let mut src = KafkaSource::new(cfg, TelemetryFormat::Cef).unwrap();
        src.assigned = keys.to_vec();
        src.stored_since = keys.iter().map(|key| (key.clone(), 0)).collect();
        src
    }

    /// Review of #644: a rebuild drops the consumer, and every partition
    /// still at `Offset::Stored` resolves again on the new one - under
    /// `latest`, at the NEW end. A partition that has not delivered must be
    /// pinned where it started first; when that cannot be established (here
    /// no broker answers), the rebuild waits for a later check instead of
    /// discarding the assignment and skipping what arrived since.
    #[test]
    fn a_rebuild_waits_while_a_latest_start_cannot_be_established() {
        let waiting = tp("t", 0);
        let mut src = latest_source_assigned_to(std::slice::from_ref(&waiting));
        let rebuilt = src.rebuild_consumer_after_stall();
        assert!(!rebuilt, "the rebuild must wait");
        assert_eq!(
            src.assigned,
            vec![waiting.clone()],
            "the assignment is kept for a later check"
        );
        assert!(
            src.cursors
                .get(&waiting)
                .and_then(|c| c.committed_offset)
                .is_none(),
            "nothing is pinned from a lookup that failed"
        );
    }

    /// #675, review: when the consumer refuses the incremental re-assign of a
    /// stalled partition, the fallback renews the whole assignment. Another
    /// partition still waiting at the end of the log (`latest`, nothing
    /// delivered) must be pinned where it started first; at `Offset::Stored`
    /// it would resolve at the new end and skip what arrived since, the same
    /// signature as the #675 CI failure (victim 10 of 10, quiet 0 of 3).
    /// A consumer with no assignment refuses the incremental unassign.
    #[test]
    fn a_whole_reassign_after_a_refused_incremental_one_pins_a_latest_partition() {
        let quiet = tp("q", 0);
        let victim = tp("v", 0);
        let mut src = latest_source_assigned_to(&[quiet.clone(), victim.clone()]);
        src.consumer().expect("consumer");
        src.cursors
            .entry(victim.clone())
            .or_default()
            .committed_offset = Some(0);
        src.liveness.note_baseline(&quiet, 0);
        src.reassign_stalled(std::slice::from_ref(&victim));
        assert!(src.assigned.is_empty(), "the whole assignment is renewed");
        assert_eq!(
            src.cursors.get(&quiet).and_then(|c| c.committed_offset),
            Some(0),
            "the waiting partition is pinned where it started"
        );
    }

    /// #675, review: when that start cannot be established either (here no
    /// broker answers), the assignment is kept rather than renewed unpinned;
    /// the next check escalates to a rebuild, which asks again.
    #[test]
    fn a_refused_incremental_reassign_keeps_the_assignment_while_a_start_is_unknown() {
        let quiet = tp("q", 0);
        let victim = tp("v", 0);
        let assigned = vec![quiet.clone(), victim.clone()];
        let mut src = latest_source_assigned_to(&assigned);
        src.consumer().expect("consumer");
        src.cursors
            .entry(victim.clone())
            .or_default()
            .committed_offset = Some(0);
        src.reassign_stalled(std::slice::from_ref(&victim));
        assert_eq!(src.assigned, assigned, "the assignment is kept");
        assert!(
            src.cursors
                .get(&quiet)
                .and_then(|c| c.committed_offset)
                .is_none(),
            "nothing is pinned from a lookup that failed"
        );
    }

    /// Review of #644: a start the stall check already established (an
    /// end-of-log baseline, recorded only after the group answered that
    /// nothing is committed) is used as it is, without asking the broker
    /// again, and the rebuild goes ahead with the partition pinned there.
    #[test]
    fn a_rebuild_pins_a_latest_partition_at_its_established_baseline() {
        let waiting = tp("t", 0);
        let mut src = latest_source_assigned_to(std::slice::from_ref(&waiting));
        src.liveness.note_baseline(&waiting, 42);
        let rebuilt = src.rebuild_consumer_after_stall();
        assert!(rebuilt);
        assert_eq!(
            src.cursors.get(&waiting).and_then(|c| c.committed_offset),
            Some(42)
        );
        assert!(src.assigned.is_empty() && src.consumer.is_none());
    }

    /// Review of #644: a partition assigned at `Offset::Stored` with nothing
    /// committed and `auto.offset.reset=latest` starts at the end of the log,
    /// so there is no offset to measure lag from until the start lookup (by
    /// record time, see `end_start_lookup_time`) establishes one. Records past
    /// that start are records a dead fetcher did not deliver.
    #[test]
    fn a_partition_started_at_the_end_of_the_log_is_measured_from_a_baseline() {
        let key = tp("a", 0);
        let assigned = vec![key.clone()];
        let mut live = PartitionLiveness::default();
        live.on_assign(&assigned);

        let first = live.next_or_baseline(&key, None);
        assert_eq!(first, None, "no start until the lookup establishes one");
        assert_eq!(live.stalled_lag(&key, first, 50), None);
        live.note_baseline(&key, 50);
        let quiet = live.next_or_baseline(&key, None);
        assert_eq!(live.stalled_lag(&key, quiet, 50), None, "nothing new: idle");
        let later = live.next_or_baseline(&key, None);
        assert_eq!(live.stalled_lag(&key, later, 57), Some(7));
        // A known offset always wins over the baseline.
        assert_eq!(live.next_or_baseline(&key, Some(40)), Some(40));

        // An established start is not overwritten by a later lookup.
        let other = tp("a", 1);
        live.note_baseline(&other, 3);
        live.note_baseline(&other, 9);
        assert_eq!(live.next_or_baseline(&other, None), Some(3));
        assert_eq!(live.stalled_lag(&other, Some(3), 57), Some(54));

        // Delivery and a fresh assign both drop the baseline.
        live.record_pass(&assigned, |_| true, Instant::now());
        assert_eq!(live.next_or_baseline(&key, None), None);
        live.note_baseline(&key, 60);
        live.on_assign(&assigned);
        assert_eq!(live.next_or_baseline(&key, None), None);
    }

    /// #623, the flake hunt on #644: a partition assigned at the end of the
    /// log (`latest`, nothing committed) whose fetcher died. When the lookup
    /// that finds where it started fails (a broker too slow to answer within
    /// the bound), the check must decide nothing and record nothing. It used
    /// to fall back to the high watermark of that check as the start, which
    /// counts every record written between the assign and the check as
    /// "before the start": those records were never recovered, and with no
    /// later records the partition was never recovered at all. When a later
    /// lookup succeeds, every record since the assign is recovered.
    #[test]
    fn a_failed_start_lookup_gives_up_no_records() {
        let key = tp("a", 0);
        let mut live = PartitionLiveness::default();
        live.on_assign(std::slice::from_ref(&key));

        // First check: 10 records waiting, the start lookup failed.
        let next = live.next_or_baseline(&key, None);
        assert_eq!(live.stalled_lag(&key, next, 10), None, "nothing decided");
        // Later check: 15 records waiting, the lookup failed again.
        let next = live.next_or_baseline(&key, None);
        assert_eq!(live.stalled_lag(&key, next, 15), None, "nothing decided");
        // The lookup succeeds: the partition started at 0.
        live.note_baseline(&key, 0);
        let next = live.next_or_baseline(&key, None);
        assert_eq!(
            live.stalled_lag(&key, next, 15),
            Some(15),
            "every record since the assign is recovered"
        );
    }

    /// Review of #644 (Codex 4533729305): the start lookup for a partition
    /// assigned at the end of the log either (a) fails outright, or (b)
    /// succeeds but omits the partition (or answers it with an error). In
    /// both cases the start stays unresolved and the next check asks again;
    /// nothing is decided from a start the check made up. When an answer for
    /// the partition does arrive, every record since the assign is recovered.
    #[test]
    fn an_unanswered_start_lookup_is_retried_not_replaced() {
        let key = tp("a", 0);
        let answered = |start: Option<i64>| -> HashMap<(String, i32), Option<i64>> {
            [(tp("a", 0), start)].into_iter().collect()
        };
        let omitted: HashMap<(String, i32), Option<i64>> =
            [(tp("other", 0), Some(3))].into_iter().collect();
        for (case, first, second) in [
            ("(a) the lookup failed", None, None),
            (
                "(b) the answer omits the partition",
                Some(&omitted),
                Some(&omitted),
            ),
        ] {
            let mut live = PartitionLiveness::default();
            live.on_assign(std::slice::from_ref(&key));
            // 10 records waiting at the first check, 15 at the second.
            let next = live.end_start(&key, first, 10);
            assert_eq!(
                live.stalled_lag(&key, next, 10),
                None,
                "{case}: first check"
            );
            let next = live.end_start(&key, second, 15);
            assert_eq!(
                live.stalled_lag(&key, next, 15),
                None,
                "{case}: second check"
            );
            // The lookup answers: the partition started at 0.
            let next = live.end_start(&key, Some(&answered(Some(0))), 15);
            assert_eq!(
                live.stalled_lag(&key, next, 15),
                Some(15),
                "{case}: every record since the assign is recovered"
            );
        }
        // An answer of "no record since the assign" is the high watermark
        // read before the lookup: exact, so it is recorded.
        let mut live = PartitionLiveness::default();
        assert_eq!(live.end_start(&key, Some(&answered(None)), 15), Some(15));
    }

    /// Review of #644: an end-of-log baseline (and the recovery pin it leads
    /// to) is allowed only when the group was ASKED and has no committed
    /// offset, and the policy really starts at the end. A failed lookup is
    /// "don't know", not "nothing committed": a pin past an existing commit
    /// would skip the records between them. `error` (or anything that is not
    /// a `latest` spelling) must keep librdkafka's own behaviour.
    #[test]
    fn an_end_of_log_baseline_needs_an_answer_from_the_group_and_latest() {
        assert!(may_measure_from_end(true, false, "latest"));
        assert!(may_measure_from_end(true, false, "Largest"));
        assert!(may_measure_from_end(true, false, "end"));
        assert!(
            !may_measure_from_end(false, false, "latest"),
            "a failed group lookup is not 'nothing committed'"
        );
        assert!(!may_measure_from_end(true, true, "latest"));
        assert!(!may_measure_from_end(true, false, "error"));
        assert!(!may_measure_from_end(true, false, "earliest"));
    }

    /// Review of #644: where a partition assigned at the end of the log
    /// started is looked up from the assign's own time, with no margin
    /// before it. A margin found records written shortly BEFORE the assign,
    /// which `latest` skips on purpose, and made a healthy idle partition
    /// look stalled and get rewound onto them.
    #[test]
    fn the_end_of_log_start_is_looked_up_from_the_assign_not_before_it() {
        assert_eq!(end_start_lookup_time(1_000_000), 1_000_000);
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

    /// A partition that delivers starts its idle run over. So does every
    /// partition on a fresh assign(), which restarts every fetcher.
    #[test]
    fn delivery_and_a_fresh_assign_each_start_the_idle_run_over() {
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
    /// up is not stalled. At one high watermark a partition is recovered at
    /// most twice (a re-assign, then a new consumer); after that it is left
    /// alone until a new record arrives, so an undeliverable tail (a
    /// transaction marker) cannot restart fetchers once per interval forever.
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

    /// #567: the committed point recovery retires a handed-back range against
    /// is the known frontier - the ledger's, or this process's own commits -
    /// never where a rewind landed, and never another consumer group's.
    #[tokio::test]
    async fn the_committed_point_is_the_known_frontier_and_only_this_groups() {
        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        let range = |group: &str| ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: 0,
            end_offset: 9,
            consumer_group: group.into(),
        };
        assert_eq!(src.committed_through(&range("vtop-engine")), None);
        // A rewind's landing is not a commit.
        src.cursors
            .entry(("t".into(), 0))
            .or_default()
            .committed_offset = Some(50);
        assert_eq!(src.committed_through(&range("vtop-engine")), None);
        src.known_committed.insert(("t".into(), 0), 20);
        assert_eq!(src.committed_through(&range("vtop-engine")), Some(20));
        assert_eq!(src.committed_through(&range("vtop-engine-old")), None);
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
            .entries
            .insert("live".into(), (vec![0], std::time::Instant::now()));
        src.partitions
            .entries
            .insert("dead".into(), (vec![0], std::time::Instant::now()));
        src.prune_partition_cache(&["live".to_string()]);
        assert!(
            src.partitions.entries.contains_key("live"),
            "live topic kept"
        );
        assert!(
            !src.partitions.entries.contains_key("dead"),
            "dead topic dropped"
        );
    }

    /// Every event logged while `f` runs, as `LEVEL message field=value ...`
    /// (#675 follow-up: the operator-facing messages are part of the
    /// contract). A minimal subscriber, so no logging crate is needed here.
    fn logged<R>(f: impl FnOnce() -> R) -> (R, Vec<String>) {
        use std::fmt::Write as _;
        use std::sync::{Arc, Mutex};
        use tracing::field::{Field, Visit};

        struct Line(String);
        impl Visit for Line {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    let _ = write!(self.0, " {value:?}");
                } else {
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }
            }
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "message" {
                    let _ = write!(self.0, " {value}");
                } else {
                    let _ = write!(self.0, " {}={value}", field.name());
                }
            }
        }
        struct Capture(Arc<Mutex<Vec<String>>>);
        impl tracing::Subscriber for Capture {
            fn register_callsite(
                &self,
                _: &'static tracing::Metadata<'static>,
            ) -> tracing::subscriber::Interest {
                tracing::subscriber::Interest::always()
            }
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                let mut line = Line(event.metadata().level().to_string());
                event.record(&mut line);
                self.0.lock().unwrap().push(line.0);
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }
        let lines = Arc::new(Mutex::new(Vec::new()));
        let out = tracing::subscriber::with_default(Capture(lines.clone()), f);
        let lines = lines.lock().unwrap().clone();
        (out, lines)
    }

    fn one_line<'a>(lines: &'a [String], needle: &str) -> &'a str {
        let found: Vec<&String> = lines.iter().filter(|l| l.contains(needle)).collect();
        assert_eq!(
            found.len(),
            1,
            "one line containing {needle:?} in {lines:#?}"
        );
        found[0]
    }

    /// #675: a metadata answer that lists no partitions - a topic the broker
    /// does not know yet, moments after it was created - is not cached. The
    /// next pass asks again and gets the partitions; before the fix the empty
    /// answer was served from the cache for `PARTITION_CACHE_TTL`, so the
    /// second lookup below was never made and the topic stayed unassigned for
    /// five minutes. A list that names partitions is cached as before. The
    /// operator is told once, not on every pass.
    #[test]
    fn a_topic_listed_without_partitions_is_asked_again_on_the_next_pass() {
        let mut cache = PartitionCache::default();
        let mut asked = 0;
        let mut answers = [Vec::new(), Vec::new(), vec![0]].into_iter();
        let mut lookup = |_: &str| {
            asked += 1;
            Ok(answers.next().expect("asked at most three times"))
        };
        let t0 = Instant::now();
        let ((first, again, second, third), lines) = logged(|| {
            (
                cache.ids("t", t0, &mut lookup).unwrap(),
                cache.ids("t", t0 + PASS, &mut lookup).unwrap(),
                cache.ids("t", t0 + PASS * 2, &mut lookup).unwrap(),
                cache.ids("t", t0 + PASS * 3, &mut lookup).unwrap(),
            )
        });
        assert!(
            first.is_empty() && again.is_empty(),
            "the broker did not know it yet"
        );
        assert_eq!(second, vec![0], "the next pass asks again");
        assert_eq!(third, vec![0]);
        assert_eq!(asked, 3, "a list with partitions is served from the cache");
        let warned = one_line(&lines, "listed with no partitions");
        assert!(
            warned.starts_with("WARN") && warned.contains("topic=t"),
            "{warned}"
        );
        assert!(warned.contains("asked again next pass"), "{warned}");
        let appeared = one_line(&lines, "now lists partitions after listing none");
        assert!(appeared.starts_with("INFO"), "{appeared}");
        assert!(
            cache.is_born(&tp("t", 0)),
            "a topic that did not exist when asked for is read from its beginning"
        );

        // Once the entry is stale, a topic the broker no longer lists (deleted)
        // is dropped from the cache, not served from it.
        let stale = t0 + PARTITION_CACHE_TTL + PASS * 4;
        let gone = cache.ids("t", stale, |_| Ok(Vec::new())).unwrap();
        assert!(
            gone.is_empty() && !cache.entries.contains_key("t"),
            "a stale entry the broker no longer lists is dropped, not served"
        );
    }

    /// #675 follow-up: a failed lookup for a topic whose cached list has gone
    /// stale keeps serving that list (a topic's partitions cannot disappear;
    /// only a recreate changes them, and that shows as a new answer). Failing
    /// instead dropped the topic from the assignment set, which renewed every
    /// other partition's assignment as well. A topic never answered for is an
    /// error, as before. Repeated failures warn once per `WARN_EVERY`, naming
    /// the error.
    #[test]
    fn a_failed_lookup_serves_the_last_known_partition_list() {
        let mut cache = PartitionCache::default();
        let t0 = Instant::now();
        cache.ids("t", t0, |_| Ok(vec![0, 1])).unwrap();
        let stale = t0 + PARTITION_CACHE_TTL + PASS;
        let failed = |_: &str| Err(VtopError::Source("fetch_metadata t: timed out".into()));
        let ((served, again, never), lines) = logged(|| {
            (
                cache.ids("t", stale, failed),
                cache.ids("t", stale + PASS, failed),
                cache.ids("never", stale, failed),
            )
        });
        assert_eq!(
            served.ok(),
            Some(vec![0, 1]),
            "the last known list is served"
        );
        assert_eq!(again.ok(), Some(vec![0, 1]));
        assert!(
            never.is_err(),
            "a topic never answered for has no list to serve"
        );
        let served = one_line(&lines, "serving the topic's last known partition list");
        assert!(served.starts_with("WARN"), "{served}");
        assert!(
            served.contains("timed out") && served.contains("topic=t"),
            "{served}"
        );
        let never = one_line(&lines, "for a topic never answered for");
        assert!(
            never.contains("topic=never") && never.contains("timed out"),
            "{never}"
        );
    }

    /// #675 follow-up: what one metadata answer changed for a topic.
    #[test]
    fn a_partition_answer_is_classified_against_the_last_one() {
        use PartitionChange::*;
        assert_eq!(classify_partition_answer(None, false, false, &[0]), First);
        assert_eq!(
            classify_partition_answer(Some(&[0, 1]), false, true, &[0, 1]),
            Unchanged
        );
        assert_eq!(
            classify_partition_answer(Some(&[0]), false, true, &[]),
            NoneListed
        );
        assert_eq!(
            classify_partition_answer(Some(&[0]), false, true, &[0, 1, 2]),
            Grew {
                from: 1,
                to: 3,
                added: vec![1, 2]
            }
        );
        // Kafka cannot remove partitions: fewer means deleted and recreated.
        assert_eq!(
            classify_partition_answer(Some(&[0, 1, 2]), false, true, &[0]),
            Shrank { from: 3, to: 1 }
        );
        assert_eq!(
            classify_partition_answer(None, true, false, &[0]),
            Appeared { recreated: false }
        );
        assert_eq!(
            classify_partition_answer(None, true, true, &[0]),
            Appeared { recreated: true }
        );
    }

    /// #675 follow-up: a partition added to a topic this adapter reads is
    /// marked to start from its beginning, and the operator is told; a topic
    /// that shrinks (deleted and recreated) or comes back after listing none
    /// is a warning. The first answer and an unchanged one say nothing.
    #[test]
    fn partition_changes_are_reported_and_new_partitions_marked() {
        let mut cache = PartitionCache::default();
        let t0 = Instant::now();
        let ((), lines) = logged(|| {
            cache.observe("g", &[0], t0);
            cache.observe("g", &[0], t0 + PASS);
            cache.observe("g", &[0, 1, 2], t0 + PASS * 2);
            cache.observe("s", &[0, 1], t0);
            cache.observe("s", &[0], t0 + PASS);
            cache.observe("r", &[0], t0);
            cache.observe("r", &[], t0 + PASS);
            cache.observe("r", &[0], t0 + PASS * 2);
        });
        let grew = one_line(&lines, "kafka topic grew");
        assert!(grew.starts_with("INFO"), "{grew}");
        for field in [
            "topic=g",
            "from=1",
            "to=3",
            "added=[1, 2]",
            "from their beginning",
        ] {
            assert!(grew.contains(field), "{field} in {grew}");
        }
        assert!(
            !cache.is_born(&tp("g", 0)),
            "an existing partition keeps its start"
        );
        assert!(
            cache.is_born(&tp("g", 1)) && cache.is_born(&tp("g", 2)),
            "the added partitions start from their beginning"
        );
        let shrank = one_line(&lines, "fewer partitions than before");
        assert!(
            shrank.starts_with("WARN") && shrank.contains("deleted and recreated"),
            "{shrank}"
        );
        assert!(
            shrank.contains("from=2") && shrank.contains("to=1"),
            "{shrank}"
        );
        let back = one_line(&lines, "lists partitions again after listing none");
        assert!(
            back.starts_with("WARN") && back.contains("topic=r"),
            "{back}"
        );
        assert_eq!(lines.len(), 4, "nothing else is logged: {lines:#?}");
        cache.settle_born(&tp("g", 1));
        assert!(
            !cache.is_born(&tp("g", 1)),
            "a settled start is not decided again"
        );
    }

    /// #675 follow-up: the partition counts are re-checked at most once per
    /// interval; the first pass only starts the clock.
    #[test]
    fn the_partition_recheck_runs_once_per_interval() {
        let mut cache = PartitionCache::default();
        let t0 = Instant::now();
        assert!(
            !cache.recheck_due(t0),
            "the first pass only starts the clock"
        );
        assert!(!cache.recheck_due(t0 + PASS), "not due within the interval");
        assert!(
            cache.recheck_due(t0 + PARTITION_RECHECK_INTERVAL),
            "due after it"
        );
        assert!(
            !cache.recheck_due(t0 + PARTITION_RECHECK_INTERVAL + PASS),
            "a re-check restarts the interval"
        );
        assert!(
            cache.recheck_due(t0 + PARTITION_RECHECK_INTERVAL * 2),
            "due again one interval later"
        );
    }

    /// #675 follow-up: a repeated warning is held back for `WARN_EVERY`, and
    /// the next one says how many were held back.
    #[test]
    fn a_repeated_warning_is_rate_limited_and_counts_what_it_held_back() {
        let mut limiter = WarnLimiter::default();
        let t0 = Instant::now();
        assert_eq!(limiter.allow("a", t0), Some(0));
        assert_eq!(limiter.allow("a", t0 + PASS), None);
        assert_eq!(limiter.allow("a", t0 + PASS * 2), None);
        assert_eq!(limiter.allow("b", t0 + PASS), Some(0), "per subject");
        assert_eq!(limiter.allow("a", t0 + WARN_EVERY), Some(2));
    }

    /// #675 follow-up: an end offset below the position held for a partition
    /// is a recreated topic or a truncated log; anything else is not.
    #[test]
    fn an_end_offset_below_the_position_is_found() {
        assert_eq!(end_below_position(Some(500), 3), Some(500));
        assert_eq!(end_below_position(Some(3), 3), None, "caught up");
        assert_eq!(end_below_position(Some(2), 3), None, "behind");
        assert_eq!(end_below_position(None, 3), None, "no position yet");
    }

    /// Review of #675: a topic answer that carries an error other than
    /// "unknown topic" and lists no partitions is a failed lookup (served from
    /// the last known list), not an absent topic: calling it absent would
    /// later report the topic recreated and read it from its beginning.
    #[test]
    fn a_topic_answer_with_an_error_is_a_failed_lookup_not_an_absent_topic() {
        use rdkafka::types::RDKafkaRespErr::*;
        assert_eq!(partition_ids_from_answer("t", None).ok(), Some(vec![]));
        assert_eq!(
            partition_ids_from_answer(
                "t",
                Some((Some(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART), vec![]))
            )
            .ok(),
            Some(vec![]),
            "unknown topic: absent"
        );
        let denied = partition_ids_from_answer(
            "t",
            Some((Some(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED), vec![])),
        );
        assert!(
            denied
                .as_ref()
                .is_err_and(|e| e.to_string().contains("AUTHORIZATION")),
            "an authorization failure is a failed lookup naming the error: {denied:?}"
        );
        assert_eq!(
            partition_ids_from_answer(
                "t",
                Some((Some(RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE), vec![0, 1]))
            )
            .ok(),
            Some(vec![0, 1]),
            "partitions listed beside an error are still its partitions"
        );
    }

    /// Review of #675: a topic in the first pass's set is not new, whatever it
    /// answers later; a topic first answered for after that was created while
    /// this process was reading, so it starts from its beginning, and the
    /// operator is told.
    #[test]
    fn a_topic_first_read_after_the_first_pass_is_new() {
        let mut cache = PartitionCache::default();
        let t0 = Instant::now();
        let ((), lines) = logged(|| {
            cache.observe("a", &[0], t0);
            cache.initial_pass_done(["a", "b"].into_iter());
            cache.observe("b", &[0], t0 + PASS);
            cache.observe("c", &[0, 1], t0 + PASS);
        });
        assert!(!cache.is_born(&tp("a", 0)), "answered in the first pass");
        assert!(!cache.is_born(&tp("b", 0)), "asked for in the first pass");
        assert!(
            cache.is_born(&tp("c", 0)) && cache.is_born(&tp("c", 1)),
            "a topic first read later starts from its beginning"
        );
        let new = one_line(&lines, "first read after this consumer started");
        assert!(new.starts_with("INFO") && new.contains("topic=c"), "{new}");
        assert_eq!(lines.len(), 1, "nothing else is logged: {lines:#?}");
    }

    /// Review of #675: rate-limit subjects of topics that are gone are
    /// dropped with the topic, so a broker churning through short-lived topics
    /// does not grow the map without bound.
    #[test]
    fn warning_subjects_of_dead_topics_are_dropped() {
        let mut limiter = WarnLimiter::default();
        let t0 = Instant::now();
        for subject in ["none:dead", "pin:dead:0", "none:live", "renewal"] {
            limiter.allow(subject, t0);
        }
        limiter.retain_topics(&["live"].into_iter().collect());
        let mut kept: Vec<&str> = limiter.last.keys().map(String::as_str).collect();
        kept.sort_unstable();
        assert_eq!(kept, vec!["none:live", "renewal"]);
    }

    /// Review of #675: only partitions that stay assigned are pinned before a
    /// renewal. One leaving the assignment (its topic deleted) is not asked
    /// about - asking would only fail, and before this a failure stopped
    /// every pass. One whose start cannot be established (here no broker
    /// answers) is returned to be left out, warned about once, and not asked
    /// about again until `STALL_CHECK_INTERVAL` has passed.
    #[test]
    fn a_pin_is_asked_only_for_partitions_that_stay_and_is_backed_off() {
        let quiet = tp("q", 0);
        let mut src = latest_source_assigned_to(std::slice::from_ref(&quiet));
        src.consumer().expect("consumer");
        let leaving = src.pin_latest_starts(&[]);
        assert!(
            leaving.is_empty(),
            "a partition leaving the assignment needs no pin"
        );

        let ((first, again, asked_again_in), lines) = logged(|| {
            let first = src.pin_latest_starts(std::slice::from_ref(&quiet));
            let started = Instant::now();
            let again = src.pin_latest_starts(std::slice::from_ref(&quiet));
            (first, again, started.elapsed())
        });
        assert_eq!(first, vec![quiet.clone()], "no start: left out");
        assert_eq!(again, vec![quiet.clone()], "still left out");
        assert!(
            asked_again_in < Duration::from_secs(1),
            "not asked again within the back-off (took {asked_again_in:?})"
        );
        let warned = one_line(&lines, "is left out of the assignment");
        assert!(
            warned.starts_with("WARN") && warned.contains("topic=q"),
            "{warned}"
        );
        assert!(
            src.cursors
                .get(&quiet)
                .and_then(|c| c.committed_offset)
                .is_none(),
            "nothing is pinned from a lookup that failed"
        );
    }

    /// Review of #675: a partition born after this process read its topic,
    /// whose start cannot be established, is not assigned (never at
    /// `Offset::Stored`, which under `latest` would skip what it holds) and
    /// stays marked; under `earliest` it resolves at `Offset::Stored`, which
    /// already starts at its beginning.
    #[test]
    fn a_new_partition_whose_start_is_unknown_waits_and_earliest_needs_none() {
        let key = tp("n", 1);
        let mut src = latest_source_assigned_to(&[]);
        src.consumer().expect("consumer");
        src.partitions.born.insert(key.clone());
        let (starts, lines) = logged(|| src.start_born_partitions(std::slice::from_ref(&key)));
        assert!(
            starts.is_empty(),
            "not assigned without a start: {starts:?}"
        );
        assert!(src.partitions.is_born(&key), "decided at a later pass");
        let warned = one_line(&lines, "is not assigned yet");
        assert!(
            warned.contains("topic=n") && warned.contains("partition=1"),
            "{warned}"
        );

        let mut src = KafkaSource::new(cfg(), TelemetryFormat::Cef).unwrap();
        src.partitions.born.insert(key.clone());
        let starts = src.start_born_partitions(std::slice::from_ref(&key));
        assert_eq!(starts, vec![(key.clone(), Offset::Stored)]);
        assert!(!src.partitions.is_born(&key), "earliest: settled");
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
