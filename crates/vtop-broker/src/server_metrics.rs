//! Request-path counters and latency histograms for the native server (#224).
//!
//! # Why the broker owns no Prometheus types
//!
//! Same rule as [`crate::group_commit::GroupCommitMetrics`] and
//! [`crate::memory_budget::MemoryBudgetMetrics`]: the data plane records into
//! plain atomics, and whatever process hosts it decides how those become
//! metrics. That keeps the exporter's dependency out of the produce path and
//! lets the same broker be embedded in a test, a benchmark, or a node without
//! dragging a registry along.
//!
//! # Why a hand-rolled histogram
//!
//! Rates and totals can be derived from counters, but a tail cannot: p99
//! produce latency is exactly the number an operator needs and exactly the one
//! an average destroys. Quantiles need bucket counts, so this carries a fixed
//! bucket ladder of atomics. Buckets are cumulative-by-convention only at
//! export time; each atomic here counts observations that fell *in* its bucket,
//! because a non-cumulative increment is one `fetch_add` rather than a walk up
//! the ladder on every request.
//!
//! # Cost on the hot path
//!
//! One relaxed `fetch_add` per counter, plus a linear scan of at most
//! [`LATENCY_BUCKETS_MICROS`]`.len()` comparisons to place an observation. No
//! allocation, no locks, no label hashing — the label sets are closed enums
//! resolved to array indices at compile time.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use vtop_protocol::{Message, Role};

/// Upper bounds, in microseconds, of the latency buckets.
///
/// The ladder is deliberately dense from 50µs to 10ms — that is where a local
/// fsync and a quorum round-trip live, so it is where the interesting shape is
/// — and coarse above, where anything landing is already an incident.
pub const LATENCY_BUCKETS_MICROS: [u64; 14] = [
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000,
];

/// Which request a measurement belongs to. Closed set: it becomes a metric
/// label, and a free-form string there would be unbounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Produce,
    Fetch,
    CommitCursor,
    FetchCursor,
    ReplicaAppend,
    Other,
}

impl RequestKind {
    pub const ALL: [Self; 6] = [
        Self::Produce,
        Self::Fetch,
        Self::CommitCursor,
        Self::FetchCursor,
        Self::ReplicaAppend,
        Self::Other,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::Fetch => "fetch",
            Self::CommitCursor => "commit_cursor",
            Self::FetchCursor => "fetch_cursor",
            Self::ReplicaAppend => "replica_append",
            Self::Other => "other",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Produce => 0,
            Self::Fetch => 1,
            Self::CommitCursor => 2,
            Self::FetchCursor => 3,
            Self::ReplicaAppend => 4,
            Self::Other => 5,
        }
    }

    /// Classify a request frame. Responses are never passed here.
    pub fn of(message: &Message) -> Self {
        match message {
            Message::ProduceRequest(_) => Self::Produce,
            Message::FetchRequest(_) => Self::Fetch,
            Message::CommitCursorRequest(_) => Self::CommitCursor,
            Message::FetchCursorRequest(_) => Self::FetchCursor,
            Message::ReplicaAppendRequest(_)
            | Message::ReplicaAppendBatchRequest(_)
            | Message::ReplicaStatusRequest(_)
            | Message::CommittedHwmUpdate(_) => Self::ReplicaAppend,
            _ => Self::Other,
        }
    }
}

/// Whether the broker answered or refused.
///
/// "Refused" is not a synonym for "broken": a fencing rejection is the system
/// working. Splitting the two keeps an error-rate panel from lighting up during
/// a correct failover, which is the fastest way to teach an operator to ignore
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestOutcome {
    Ok,
    Error,
}

impl RequestOutcome {
    pub const ALL: [Self; 2] = [Self::Ok, Self::Error];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Ok => 0,
            Self::Error => 1,
        }
    }

    /// Read the outcome off a response frame.
    pub fn of(message: &Message) -> Self {
        match message {
            Message::Error(_) => Self::Error,
            _ => Self::Ok,
        }
    }
}

/// Session roles, in the order they are indexed.
pub const ROLES: [Role; 4] = [
    Role::Producer,
    Role::Consumer,
    Role::Peer,
    Role::Administrator,
];

pub fn role_label(role: Role) -> &'static str {
    match role {
        Role::Producer => "producer",
        Role::Consumer => "consumer",
        Role::Peer => "peer",
        Role::Administrator => "administrator",
    }
}

fn role_index(role: Role) -> usize {
    match role {
        Role::Producer => 0,
        Role::Consumer => 1,
        Role::Peer => 2,
        Role::Administrator => 3,
    }
}

/// A fixed-bucket latency histogram built from atomics.
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    /// Observations that fell inside each bucket, not cumulative.
    buckets: [AtomicU64; LATENCY_BUCKETS_MICROS.len()],
    /// Observations above the last bucket bound — the `+Inf` overflow.
    overflow: AtomicU64,
    count: AtomicU64,
    total_nanos: AtomicU64,
}

impl LatencyHistogram {
    pub fn observe(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        match LATENCY_BUCKETS_MICROS
            .iter()
            .position(|bound| micros <= *bound)
        {
            Some(index) => self.buckets[index].fetch_add(1, Ordering::Relaxed),
            None => self.overflow.fetch_add(1, Ordering::Relaxed),
        };
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_nanos.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Cumulative bucket counts, as Prometheus expects them, plus the totals.
    ///
    /// The conversion to cumulative happens here rather than on every
    /// observation: a scrape is rare and a request is not.
    pub fn snapshot(&self) -> LatencySnapshot {
        let mut cumulative = [0_u64; LATENCY_BUCKETS_MICROS.len()];
        let mut running = 0_u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            running = running.saturating_add(bucket.load(Ordering::Relaxed));
            cumulative[index] = running;
        }
        LatencySnapshot {
            cumulative_counts: cumulative,
            count: self.count.load(Ordering::Relaxed),
            total_seconds: self.total_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        }
    }
}

/// One histogram's exportable state.
#[derive(Clone, Debug, PartialEq)]
pub struct LatencySnapshot {
    /// Cumulative count at each bound in [`LATENCY_BUCKETS_MICROS`].
    pub cumulative_counts: [u64; LATENCY_BUCKETS_MICROS.len()],
    /// Total observations, which is also the `+Inf` bucket.
    pub count: u64,
    pub total_seconds: f64,
}

/// Everything the native server records about its own request path.
#[derive(Debug, Default)]
pub struct ServerMetrics {
    sessions_accepted: [AtomicU64; ROLES.len()],
    sessions_active: [AtomicU64; ROLES.len()],
    sessions_closed: [AtomicU64; ROLES.len()],
    sessions_refused_at_capacity: AtomicU64,
    sessions_refused_unauthorized: AtomicU64,
    sessions_refused_handshake: AtomicU64,
    requests: [[AtomicU64; RequestOutcome::ALL.len()]; RequestKind::ALL.len()],
    latency: [LatencyHistogram; RequestKind::ALL.len()],
    records_produced: AtomicU64,
    bytes_produced: AtomicU64,
    records_fetched: AtomicU64,
    bytes_fetched: AtomicU64,
}

impl ServerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// A session passed authorization and negotiation.
    pub fn session_opened(&self, role: Role) {
        self.sessions_accepted[role_index(role)].fetch_add(1, Ordering::Relaxed);
        self.sessions_active[role_index(role)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_closed(&self, role: Role) {
        self.sessions_closed[role_index(role)].fetch_add(1, Ordering::Relaxed);
        // `fetch_update` rather than `fetch_sub`: an unbalanced close would
        // wrap the gauge to u64::MAX and render as an absurd session count.
        // Clamping at zero keeps a bookkeeping slip from looking like an
        // incident.
        let _ = self.sessions_active[role_index(role)].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }

    pub fn session_refused_at_capacity(&self) {
        self.sessions_refused_at_capacity
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_refused_unauthorized(&self) {
        self.sessions_refused_unauthorized
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A connection that never became a session: TLS or the protocol
    /// handshake failed, or the peer hung up first.
    pub fn session_refused_handshake(&self) {
        self.sessions_refused_handshake
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one served request: its kind, how it ended, and how long the
    /// broker held it.
    pub fn request_completed(&self, kind: RequestKind, outcome: RequestOutcome, elapsed: Duration) {
        self.requests[kind.index()][outcome.index()].fetch_add(1, Ordering::Relaxed);
        self.latency[kind.index()].observe(elapsed);
    }

    /// Volume accepted by a produce. Rates are derived from these in PromQL,
    /// never exported as a gauge — a gauge of a rate lies between scrapes.
    pub fn produced(&self, records: u64, bytes: u64) {
        self.records_produced.fetch_add(records, Ordering::Relaxed);
        self.bytes_produced.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn fetched(&self, records: u64, bytes: u64) {
        self.records_fetched.fetch_add(records, Ordering::Relaxed);
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn sessions_accepted(&self, role: Role) -> u64 {
        self.sessions_accepted[role_index(role)].load(Ordering::Relaxed)
    }

    pub fn sessions_active(&self, role: Role) -> u64 {
        self.sessions_active[role_index(role)].load(Ordering::Relaxed)
    }

    pub fn sessions_closed(&self, role: Role) -> u64 {
        self.sessions_closed[role_index(role)].load(Ordering::Relaxed)
    }

    pub fn sessions_refused_at_capacity_total(&self) -> u64 {
        self.sessions_refused_at_capacity.load(Ordering::Relaxed)
    }

    pub fn sessions_refused_unauthorized_total(&self) -> u64 {
        self.sessions_refused_unauthorized.load(Ordering::Relaxed)
    }

    pub fn sessions_refused_handshake_total(&self) -> u64 {
        self.sessions_refused_handshake.load(Ordering::Relaxed)
    }

    pub fn requests_total(&self, kind: RequestKind, outcome: RequestOutcome) -> u64 {
        self.requests[kind.index()][outcome.index()].load(Ordering::Relaxed)
    }

    pub fn latency(&self, kind: RequestKind) -> LatencySnapshot {
        self.latency[kind.index()].snapshot()
    }

    pub fn records_produced_total(&self) -> u64 {
        self.records_produced.load(Ordering::Relaxed)
    }

    pub fn bytes_produced_total(&self) -> u64 {
        self.bytes_produced.load(Ordering::Relaxed)
    }

    pub fn records_fetched_total(&self) -> u64 {
        self.records_fetched.load(Ordering::Relaxed)
    }

    pub fn bytes_fetched_total(&self) -> u64 {
        self.bytes_fetched.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_and_outcome_label_is_distinct() {
        let kinds: std::collections::BTreeSet<_> =
            RequestKind::ALL.iter().map(|k| k.as_str()).collect();
        assert_eq!(kinds.len(), RequestKind::ALL.len());
        let outcomes: std::collections::BTreeSet<_> =
            RequestOutcome::ALL.iter().map(|o| o.as_str()).collect();
        assert_eq!(outcomes.len(), RequestOutcome::ALL.len());
        let roles: std::collections::BTreeSet<_> = ROLES.iter().map(|r| role_label(*r)).collect();
        assert_eq!(roles.len(), ROLES.len());
    }

    #[test]
    fn a_request_frame_classifies_to_its_own_kind() {
        assert_eq!(RequestKind::of(&Message::Ping), RequestKind::Other);
        assert_eq!(
            RequestKind::of(&Message::FetchRequest(vtop_protocol::FetchRequest {
                range: range(),
                fencing_epoch: 0,
                start_offset: 0,
                max_records: 1,
                max_bytes: 1,
            })),
            RequestKind::Fetch
        );
    }

    fn range() -> vtop_protocol::RangeIdentity {
        vtop_protocol::RangeIdentity {
            topic: "t".into(),
            topic_epoch: 0,
            range_id: uuid::Uuid::nil(),
            range_generation: 0,
        }
    }

    /// A fencing refusal is the system working; it must be countable
    /// separately from a successful append.
    #[test]
    fn an_error_response_is_recorded_as_a_refusal_not_a_success() {
        let metrics = ServerMetrics::new();
        metrics.request_completed(
            RequestKind::Produce,
            RequestOutcome::of(&Message::Error(vtop_protocol::ErrorResponse {
                code: vtop_protocol::ErrorCode::Fenced,
                retryable: false,
                message: "fenced".into(),
            })),
            Duration::from_micros(10),
        );
        assert_eq!(
            metrics.requests_total(RequestKind::Produce, RequestOutcome::Error),
            1
        );
        assert_eq!(
            metrics.requests_total(RequestKind::Produce, RequestOutcome::Ok),
            0
        );
    }

    #[test]
    fn buckets_are_cumulative_and_the_count_is_the_plus_inf_bucket() {
        let histogram = LatencyHistogram::default();
        // One in the first bucket, one in the middle, one past the ladder.
        histogram.observe(Duration::from_micros(10));
        histogram.observe(Duration::from_micros(3_000));
        histogram.observe(Duration::from_secs(5));

        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 3);
        assert_eq!(
            snapshot.cumulative_counts[0], 1,
            "the 50us bucket holds only the fast observation"
        );
        let five_ms = LATENCY_BUCKETS_MICROS
            .iter()
            .position(|b| *b == 5_000)
            .unwrap();
        assert_eq!(
            snapshot.cumulative_counts[five_ms], 2,
            "cumulative means the 5ms bucket also contains the 10us observation"
        );
        assert_eq!(
            *snapshot.cumulative_counts.last().unwrap(),
            2,
            "the 5s observation belongs to +Inf only, never to the last finite bound"
        );
        assert!(
            snapshot.total_seconds > 5.0,
            "the sum must include the overflow observation: {snapshot:?}"
        );
    }

    /// An observation exactly on a bound belongs to that bound: Prometheus
    /// buckets are `le`, less-than-or-equal.
    #[test]
    fn an_observation_on_a_bound_lands_in_that_bucket() {
        let histogram = LatencyHistogram::default();
        histogram.observe(Duration::from_micros(50));
        assert_eq!(histogram.snapshot().cumulative_counts[0], 1);
    }

    #[test]
    fn active_sessions_track_open_and_close() {
        let metrics = ServerMetrics::new();
        metrics.session_opened(Role::Producer);
        metrics.session_opened(Role::Producer);
        metrics.session_opened(Role::Consumer);
        metrics.session_closed(Role::Producer);
        assert_eq!(metrics.sessions_active(Role::Producer), 1);
        assert_eq!(metrics.sessions_active(Role::Consumer), 1);
        assert_eq!(metrics.sessions_accepted(Role::Producer), 2);
    }

    /// An unbalanced close must not wrap the gauge into an absurd number that
    /// reads as a session leak.
    #[test]
    fn closing_more_than_was_opened_clamps_at_zero() {
        let metrics = ServerMetrics::new();
        metrics.session_closed(Role::Peer);
        assert_eq!(metrics.sessions_active(Role::Peer), 0);
    }
}
