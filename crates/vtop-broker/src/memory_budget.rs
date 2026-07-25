//! End-to-end native-broker memory budgets and explicit overload actions (#187).
//!
//! Budgets bound produce, fetch-response, and replication buffering so overload
//! never silently drops an accepted record. Hitting a ceiling takes exactly one
//! documented [`OverloadAction`] — for this slice, primarily
//! [`OverloadAction::RejectRetryable`] (`ErrorCode::Overloaded`, `retryable`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Explicit action taken when a memory budget is exhausted.
///
/// Call sites must pick one of these; silent drops of accepted work are forbidden.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverloadAction {
    /// Return [`vtop_protocol::ErrorCode::Overloaded`] with `retryable = true`.
    RejectRetryable,
    /// Pause reading new client frames until credit returns (session-level).
    PauseReads,
    /// Block admission up to a documented timeout, then reject retryably.
    BlockWithTimeout,
}

/// Which ledger rejected an admission attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BudgetRejectReason {
    ProducerConn,
    ConsumerConn,
    ShardCeiling,
    ProcessCeiling,
    ReplicaFollower,
    ReplicaCatchUp,
    FetchQueue,
    OversizedRecord,
}

impl BudgetRejectReason {
    pub const ALL: [Self; 8] = [
        Self::ProducerConn,
        Self::ConsumerConn,
        Self::ShardCeiling,
        Self::ProcessCeiling,
        Self::ReplicaFollower,
        Self::ReplicaCatchUp,
        Self::FetchQueue,
        Self::OversizedRecord,
    ];

    fn index(self) -> usize {
        match self {
            Self::ProducerConn => 0,
            Self::ConsumerConn => 1,
            Self::ShardCeiling => 2,
            Self::ProcessCeiling => 3,
            Self::ReplicaFollower => 4,
            Self::ReplicaCatchUp => 5,
            Self::FetchQueue => 6,
            Self::OversizedRecord => 7,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProducerConn => "producer_conn",
            Self::ConsumerConn => "consumer_conn",
            Self::ShardCeiling => "shard_ceiling",
            Self::ProcessCeiling => "process_ceiling",
            Self::ReplicaFollower => "replica_follower",
            Self::ReplicaCatchUp => "replica_catch_up",
            Self::FetchQueue => "fetch_queue",
            Self::OversizedRecord => "oversized_record",
        }
    }

    /// Default overload action for this reason in the first #187 slice.
    pub fn default_action(self) -> OverloadAction {
        match self {
            Self::OversizedRecord => OverloadAction::RejectRetryable,
            Self::FetchQueue | Self::ConsumerConn => OverloadAction::RejectRetryable,
            Self::ProducerConn
            | Self::ShardCeiling
            | Self::ProcessCeiling
            | Self::ReplicaFollower
            | Self::ReplicaCatchUp => OverloadAction::RejectRetryable,
        }
    }
}

/// Configured byte ceilings for native-broker subsystems.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryBudgetConfig {
    /// In-flight produce payload bytes charged to one producer TLS session.
    pub per_producer_conn_bytes: u64,
    /// Pending fetch-response bytes charged to one consumer TLS session.
    pub per_consumer_conn_bytes: u64,
    /// Produce + group-commit occupancy for one append shard / active segment.
    pub per_shard_bytes: u64,
    /// Hard ceiling across the broker process (sum of shard + fetch + replica).
    pub process_ceiling_bytes: u64,
    /// Per-follower inflight replication bytes (networked replica set).
    pub per_follower_bytes: u64,
    /// Per-follower catch-up / retransmission buffer bytes.
    pub catch_up_bytes: u64,
    /// Aggregate bytes queued for fetch responses across consumer sessions.
    pub fetch_response_queue_bytes: u64,
    /// Single-record payload (key+value) rejected before expensive allocation.
    pub max_record_bytes: u64,
    /// Optional timed wait before reject when using [`OverloadAction::BlockWithTimeout`].
    pub overload_block_timeout: Duration,
}

impl Default for MemoryBudgetConfig {
    fn default() -> Self {
        Self {
            per_producer_conn_bytes: 64 * 1024 * 1024,
            per_consumer_conn_bytes: 64 * 1024 * 1024,
            per_shard_bytes: 256 * 1024 * 1024,
            process_ceiling_bytes: 512 * 1024 * 1024,
            per_follower_bytes: 32 * 1024 * 1024,
            catch_up_bytes: 16 * 1024 * 1024,
            fetch_response_queue_bytes: 128 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            overload_block_timeout: Duration::from_millis(50),
        }
    }
}

impl MemoryBudgetConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("per_producer_conn_bytes", self.per_producer_conn_bytes),
            ("per_consumer_conn_bytes", self.per_consumer_conn_bytes),
            ("per_shard_bytes", self.per_shard_bytes),
            ("process_ceiling_bytes", self.process_ceiling_bytes),
            ("per_follower_bytes", self.per_follower_bytes),
            ("catch_up_bytes", self.catch_up_bytes),
            ("fetch_response_queue_bytes", self.fetch_response_queue_bytes),
            ("max_record_bytes", self.max_record_bytes),
        ] {
            if value == 0 {
                return Err(format!("{name} must be greater than zero"));
            }
        }
        if self.per_shard_bytes > self.process_ceiling_bytes {
            return Err(
                "per_shard_bytes must be less than or equal to process_ceiling_bytes".to_owned(),
            );
        }
        if self.catch_up_bytes > self.per_follower_bytes {
            return Err("catch_up_bytes must be less than or equal to per_follower_bytes".to_owned());
        }
        Ok(())
    }
}

/// Process-local counters for budget observability.
#[derive(Debug, Default)]
pub struct MemoryBudgetMetrics {
    process_used_bytes: AtomicU64,
    shard_used_bytes: AtomicU64,
    fetch_queue_used_bytes: AtomicU64,
    replica_used_bytes: AtomicU64,
    queue_depth: AtomicU64,
    rejections: [AtomicU64; 8],
    backpressure_nanos_total: AtomicU64,
    backpressure_events: AtomicU64,
}

impl MemoryBudgetMetrics {
    pub fn process_used_bytes(&self) -> u64 {
        self.process_used_bytes.load(Ordering::Relaxed)
    }

    pub fn shard_used_bytes(&self) -> u64 {
        self.shard_used_bytes.load(Ordering::Relaxed)
    }

    pub fn fetch_queue_used_bytes(&self) -> u64 {
        self.fetch_queue_used_bytes.load(Ordering::Relaxed)
    }

    pub fn replica_used_bytes(&self) -> u64 {
        self.replica_used_bytes.load(Ordering::Relaxed)
    }

    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    pub fn rejections(&self, reason: BudgetRejectReason) -> u64 {
        self.rejections[reason.index()].load(Ordering::Relaxed)
    }

    pub fn rejections_total(&self) -> u64 {
        self.rejections
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum()
    }

    pub fn backpressure_nanos_total(&self) -> u64 {
        self.backpressure_nanos_total.load(Ordering::Relaxed)
    }

    pub fn backpressure_events(&self) -> u64 {
        self.backpressure_events.load(Ordering::Relaxed)
    }

    fn record_rejection(&self, reason: BudgetRejectReason) {
        self.rejections[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn record_backpressure(&self, waited: Duration) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
        self.backpressure_nanos_total.fetch_add(
            u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

/// Shared broker-wide ledgers (process, shard, fetch queue, replica aggregate).
#[derive(Debug)]
pub struct MemoryBudgetPool {
    config: MemoryBudgetConfig,
    metrics: MemoryBudgetMetrics,
    process_used: AtomicU64,
    shard_used: AtomicU64,
    fetch_queue_used: AtomicU64,
    replica_used: AtomicU64,
    queue_depth: AtomicU64,
}

impl MemoryBudgetPool {
    pub fn new(config: MemoryBudgetConfig) -> Result<Arc<Self>, String> {
        config.validate()?;
        Ok(Arc::new(Self {
            config,
            metrics: MemoryBudgetMetrics::default(),
            process_used: AtomicU64::new(0),
            shard_used: AtomicU64::new(0),
            fetch_queue_used: AtomicU64::new(0),
            replica_used: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
        }))
    }

    pub fn config(&self) -> &MemoryBudgetConfig {
        &self.config
    }

    pub fn metrics(&self) -> &MemoryBudgetMetrics {
        // Keep gauge mirrors fresh for readers.
        self.metrics
            .process_used_bytes
            .store(self.process_used.load(Ordering::Relaxed), Ordering::Relaxed);
        self.metrics
            .shard_used_bytes
            .store(self.shard_used.load(Ordering::Relaxed), Ordering::Relaxed);
        self.metrics.fetch_queue_used_bytes.store(
            self.fetch_queue_used.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.metrics
            .replica_used_bytes
            .store(self.replica_used.load(Ordering::Relaxed), Ordering::Relaxed);
        self.metrics
            .queue_depth
            .store(self.queue_depth.load(Ordering::Relaxed), Ordering::Relaxed);
        &self.metrics
    }

    pub fn open_producer_connection(self: &Arc<Self>) -> ConnectionBudget {
        ConnectionBudget::new(Arc::clone(self), ConnectionKind::Producer)
    }

    pub fn open_consumer_connection(self: &Arc<Self>) -> ConnectionBudget {
        ConnectionBudget::new(Arc::clone(self), ConnectionKind::Consumer)
    }

    pub fn open_follower(self: &Arc<Self>) -> FollowerBudget {
        FollowerBudget::new(Arc::clone(self))
    }

    /// Reject a single record that exceeds `max_record_bytes` before allocation.
    pub fn check_record_size(&self, key_len: usize, value_len: usize) -> Result<(), BudgetRejectReason> {
        let bytes = (key_len as u64).saturating_add(value_len as u64);
        if bytes > self.config.max_record_bytes {
            self.metrics
                .record_rejection(BudgetRejectReason::OversizedRecord);
            return Err(BudgetRejectReason::OversizedRecord);
        }
        Ok(())
    }

    /// Reserve produce payload against process + shard (+ optional producer conn).
    pub fn try_reserve_produce(
        self: &Arc<Self>,
        bytes: u64,
        conn: Option<&ConnectionBudget>,
    ) -> Result<BudgetReservation, BudgetRejectReason> {
        if bytes == 0 {
            return Ok(BudgetReservation::empty(Arc::clone(self)));
        }
        if let Some(conn) = conn {
            if conn.kind != ConnectionKind::Producer {
                return Err(BudgetRejectReason::ProducerConn);
            }
            conn.try_charge(bytes)?;
        }
        if let Err(reason) = try_add_capped(
            &self.shard_used,
            bytes,
            self.config.per_shard_bytes,
            BudgetRejectReason::ShardCeiling,
        ) {
            if let Some(conn) = conn {
                conn.release(bytes);
            }
            self.metrics.record_rejection(reason);
            return Err(reason);
        }
        if let Err(reason) = try_add_capped(
            &self.process_used,
            bytes,
            self.config.process_ceiling_bytes,
            BudgetRejectReason::ProcessCeiling,
        ) {
            let _ = self.shard_used.fetch_sub(bytes, Ordering::Relaxed);
            if let Some(conn) = conn {
                conn.release(bytes);
            }
            self.metrics.record_rejection(reason);
            return Err(reason);
        }
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        Ok(BudgetReservation {
            pool: Arc::clone(self),
            bytes,
            kind: ReservationKind::Produce {
                conn: conn.map(|c| Arc::clone(&c.inner)),
            },
        })
    }

    /// Reserve fetch-response bytes against process + fetch queue (+ consumer conn).
    pub fn try_reserve_fetch(
        self: &Arc<Self>,
        bytes: u64,
        conn: &ConnectionBudget,
    ) -> Result<BudgetReservation, BudgetRejectReason> {
        if bytes == 0 {
            return Ok(BudgetReservation::empty(Arc::clone(self)));
        }
        if conn.kind != ConnectionKind::Consumer {
            return Err(BudgetRejectReason::ConsumerConn);
        }
        conn.try_charge(bytes)?;
        if let Err(reason) = try_add_capped(
            &self.fetch_queue_used,
            bytes,
            self.config.fetch_response_queue_bytes,
            BudgetRejectReason::FetchQueue,
        ) {
            conn.release(bytes);
            self.metrics.record_rejection(reason);
            return Err(reason);
        }
        if let Err(reason) = try_add_capped(
            &self.process_used,
            bytes,
            self.config.process_ceiling_bytes,
            BudgetRejectReason::ProcessCeiling,
        ) {
            let _ = self.fetch_queue_used.fetch_sub(bytes, Ordering::Relaxed);
            conn.release(bytes);
            self.metrics.record_rejection(reason);
            return Err(reason);
        }
        Ok(BudgetReservation {
            pool: Arc::clone(self),
            bytes,
            kind: ReservationKind::Fetch {
                conn: Arc::clone(&conn.inner),
            },
        })
    }

    pub fn record_backpressure(&self, waited: Duration) {
        if !waited.is_zero() {
            self.metrics.record_backpressure(waited);
        }
    }

    pub fn record_rejection(&self, reason: BudgetRejectReason) {
        self.metrics.record_rejection(reason);
    }

    fn release_produce(&self, bytes: u64, conn: Option<&ConnectionInner>) {
        if bytes == 0 {
            return;
        }
        let _ = self.process_used.fetch_sub(bytes, Ordering::Relaxed);
        let _ = self.shard_used.fetch_sub(bytes, Ordering::Relaxed);
        let _ = self.queue_depth.fetch_sub(1, Ordering::Relaxed);
        if let Some(conn) = conn {
            conn.release(bytes);
        }
    }

    fn release_fetch(&self, bytes: u64, conn: &ConnectionInner) {
        if bytes == 0 {
            return;
        }
        let _ = self.process_used.fetch_sub(bytes, Ordering::Relaxed);
        let _ = self
            .fetch_queue_used
            .fetch_sub(bytes, Ordering::Relaxed);
        conn.release(bytes);
    }

    fn charge_replica_aggregate(&self, bytes: u64) -> Result<(), BudgetRejectReason> {
        try_add_capped(
            &self.replica_used,
            bytes,
            self.config.process_ceiling_bytes,
            BudgetRejectReason::ProcessCeiling,
        )?;
        try_add_capped(
            &self.process_used,
            bytes,
            self.config.process_ceiling_bytes,
            BudgetRejectReason::ProcessCeiling,
        )
        .inspect_err(|_| {
            let _ = self.replica_used.fetch_sub(bytes, Ordering::Relaxed);
        })
    }

    fn release_replica_aggregate(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.replica_used.fetch_sub(bytes, Ordering::Relaxed);
        let _ = self.process_used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionKind {
    Producer,
    Consumer,
}

#[derive(Debug)]
struct ConnectionInner {
    pool: Arc<MemoryBudgetPool>,
    kind: ConnectionKind,
    used: AtomicU64,
    ceiling: u64,
}

impl ConnectionInner {
    fn try_charge(&self, bytes: u64) -> Result<(), BudgetRejectReason> {
        let reason = match self.kind {
            ConnectionKind::Producer => BudgetRejectReason::ProducerConn,
            ConnectionKind::Consumer => BudgetRejectReason::ConsumerConn,
        };
        try_add_capped(&self.used, bytes, self.ceiling, reason).inspect_err(|&reason| {
            self.pool.metrics.record_rejection(reason);
        })
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// Per TLS session byte ledger (producer or consumer).
#[derive(Clone, Debug)]
pub struct ConnectionBudget {
    inner: Arc<ConnectionInner>,
    kind: ConnectionKind,
}

impl ConnectionBudget {
    fn new(pool: Arc<MemoryBudgetPool>, kind: ConnectionKind) -> Self {
        let ceiling = match kind {
            ConnectionKind::Producer => pool.config.per_producer_conn_bytes,
            ConnectionKind::Consumer => pool.config.per_consumer_conn_bytes,
        };
        Self {
            inner: Arc::new(ConnectionInner {
                pool,
                kind,
                used: AtomicU64::new(0),
                ceiling,
            }),
            kind,
        }
    }

    pub fn used_bytes(&self) -> u64 {
        self.inner.used.load(Ordering::Relaxed)
    }

    pub fn ceiling_bytes(&self) -> u64 {
        self.inner.ceiling
    }

    fn try_charge(&self, bytes: u64) -> Result<(), BudgetRejectReason> {
        self.inner.try_charge(bytes)
    }

    fn release(&self, bytes: u64) {
        self.inner.release(bytes);
    }
}

/// Per replication follower inflight + catch-up ledger.
#[derive(Debug)]
pub struct FollowerBudget {
    pool: Arc<MemoryBudgetPool>,
    inflight_used: AtomicU64,
    catch_up_used: AtomicU64,
}

impl FollowerBudget {
    fn new(pool: Arc<MemoryBudgetPool>) -> Self {
        Self {
            pool,
            inflight_used: AtomicU64::new(0),
            catch_up_used: AtomicU64::new(0),
        }
    }

    pub fn inflight_used_bytes(&self) -> u64 {
        self.inflight_used.load(Ordering::Relaxed)
    }

    pub fn catch_up_used_bytes(&self) -> u64 {
        self.catch_up_used.load(Ordering::Relaxed)
    }

    /// Charge inflight replication bytes (also against process aggregate).
    pub fn try_reserve_inflight(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<FollowerReservation, BudgetRejectReason> {
        if bytes == 0 {
            return Ok(FollowerReservation {
                budget: Arc::clone(self),
                bytes: 0,
                kind: FollowerReserveKind::Inflight,
            });
        }
        if let Err(reason) = try_add_capped(
            &self.inflight_used,
            bytes,
            self.pool.config.per_follower_bytes,
            BudgetRejectReason::ReplicaFollower,
        ) {
            self.pool.metrics.record_rejection(reason);
            return Err(reason);
        }
        if let Err(reason) = self.pool.charge_replica_aggregate(bytes) {
            let _ = self.inflight_used.fetch_sub(bytes, Ordering::Relaxed);
            self.pool.metrics.record_rejection(reason);
            return Err(reason);
        }
        Ok(FollowerReservation {
            budget: Arc::clone(self),
            bytes,
            kind: FollowerReserveKind::Inflight,
        })
    }

    /// Charge catch-up / retransmission buffer bytes.
    pub fn try_reserve_catch_up(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<FollowerReservation, BudgetRejectReason> {
        self.try_charge_catch_up(bytes)?;
        Ok(FollowerReservation {
            budget: Arc::clone(self),
            bytes,
            kind: FollowerReserveKind::CatchUp,
        })
    }

    /// Charge catch-up bytes without an RAII guard (caller must [`Self::release_catch_up`]).
    pub fn try_charge_catch_up(&self, bytes: u64) -> Result<(), BudgetRejectReason> {
        if bytes == 0 {
            return Ok(());
        }
        if let Err(reason) = try_add_capped(
            &self.catch_up_used,
            bytes,
            self.pool.config.catch_up_bytes,
            BudgetRejectReason::ReplicaCatchUp,
        ) {
            self.pool.metrics.record_rejection(reason);
            return Err(reason);
        }
        if let Err(reason) = self.pool.charge_replica_aggregate(bytes) {
            let _ = self.catch_up_used.fetch_sub(bytes, Ordering::Relaxed);
            self.pool.metrics.record_rejection(reason);
            return Err(reason);
        }
        Ok(())
    }

    pub fn release_catch_up(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.catch_up_used.fetch_sub(bytes, Ordering::Relaxed);
        self.pool.release_replica_aggregate(bytes);
    }

    pub fn release_inflight(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.inflight_used.fetch_sub(bytes, Ordering::Relaxed);
        self.pool.release_replica_aggregate(bytes);
    }
}

#[derive(Clone, Copy, Debug)]
enum FollowerReserveKind {
    Inflight,
    CatchUp,
}

/// RAII release for follower inflight / catch-up bytes.
#[derive(Debug)]
pub struct FollowerReservation {
    budget: Arc<FollowerBudget>,
    bytes: u64,
    kind: FollowerReserveKind,
}

impl FollowerReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for FollowerReservation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        match self.kind {
            FollowerReserveKind::Inflight => {
                self.budget.release_inflight(self.bytes);
            }
            FollowerReserveKind::CatchUp => {
                self.budget.release_catch_up(self.bytes);
            }
        }
    }
}

#[derive(Debug)]
enum ReservationKind {
    Empty,
    Produce { conn: Option<Arc<ConnectionInner>> },
    Fetch { conn: Arc<ConnectionInner> },
}

/// RAII reservation that releases charged bytes on drop.
#[derive(Debug)]
pub struct BudgetReservation {
    pool: Arc<MemoryBudgetPool>,
    bytes: u64,
    kind: ReservationKind,
}

impl BudgetReservation {
    fn empty(pool: Arc<MemoryBudgetPool>) -> Self {
        Self {
            pool,
            bytes: 0,
            kind: ReservationKind::Empty,
        }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Disarm automatic release (caller transferred ownership elsewhere).
    pub fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        match &self.kind {
            ReservationKind::Empty => {}
            ReservationKind::Produce { conn } => {
                self.pool.release_produce(self.bytes, conn.as_deref());
            }
            ReservationKind::Fetch { conn } => {
                self.pool.release_fetch(self.bytes, conn);
            }
        }
    }
}

fn try_add_capped(
    counter: &AtomicU64,
    bytes: u64,
    ceiling: u64,
    reason: BudgetRejectReason,
) -> Result<(), BudgetRejectReason> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let Some(next) = current.checked_add(bytes) else {
            return Err(reason);
        };
        if next > ceiling {
            return Err(reason);
        }
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

/// Human-readable message for an overloaded produce/fetch rejection.
pub fn reject_message(reason: BudgetRejectReason) -> &'static str {
    match reason {
        BudgetRejectReason::ProducerConn => {
            "producer connection memory budget exhausted; retry later"
        }
        BudgetRejectReason::ConsumerConn => {
            "consumer connection memory budget exhausted; send WindowUpdate or retry"
        }
        BudgetRejectReason::ShardCeiling => "append shard memory budget exhausted; retry later",
        BudgetRejectReason::ProcessCeiling => "broker process memory budget exhausted; retry later",
        BudgetRejectReason::ReplicaFollower => {
            "replication follower memory budget exhausted; retry later"
        }
        BudgetRejectReason::ReplicaCatchUp => {
            "replication catch-up buffer budget exhausted; retry later"
        }
        BudgetRejectReason::FetchQueue => "fetch response queue memory budget exhausted; retry later",
        BudgetRejectReason::OversizedRecord => {
            "record exceeds configured max_record_bytes memory budget"
        }
    }
}

/// Spin-wait helper for [`OverloadAction::BlockWithTimeout`] admission.
pub fn block_with_timeout<F>(
    pool: &MemoryBudgetPool,
    timeout: Duration,
    mut attempt: F,
) -> Result<BudgetReservation, BudgetRejectReason>
where
    F: FnMut() -> Result<BudgetReservation, BudgetRejectReason>,
{
    let started = Instant::now();
    loop {
        match attempt() {
            Ok(reservation) => {
                pool.record_backpressure(started.elapsed());
                return Ok(reservation);
            }
            Err(reason) if started.elapsed() >= timeout => {
                pool.record_backpressure(started.elapsed());
                return Err(reason);
            }
            Err(_) => {
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_config() -> MemoryBudgetConfig {
        MemoryBudgetConfig {
            per_producer_conn_bytes: 1_024,
            per_consumer_conn_bytes: 1_024,
            per_shard_bytes: 2_048,
            process_ceiling_bytes: 4_096,
            per_follower_bytes: 2_048,
            catch_up_bytes: 1_024,
            fetch_response_queue_bytes: 2_048,
            max_record_bytes: 512,
            overload_block_timeout: Duration::from_millis(5),
        }
    }

    #[test]
    fn reserve_and_release_produce_bytes() {
        let pool = MemoryBudgetPool::new(tiny_config()).unwrap();
        let conn = pool.open_producer_connection();
        let reservation = pool.try_reserve_produce(800, Some(&conn)).unwrap();
        assert_eq!(reservation.bytes(), 800);
        assert_eq!(pool.metrics().shard_used_bytes(), 800);
        assert_eq!(conn.used_bytes(), 800);
        drop(reservation);
        assert_eq!(pool.metrics().shard_used_bytes(), 0);
        assert_eq!(conn.used_bytes(), 0);
    }

    #[test]
    fn process_ceiling_rejects_without_silent_drop() {
        let mut config = tiny_config();
        config.per_consumer_conn_bytes = 4_096;
        config.fetch_response_queue_bytes = 4_096;
        let pool = MemoryBudgetPool::new(config).unwrap();
        let consumer = pool.open_consumer_connection();
        // Consume process budget via the fetch-queue ledger first.
        let _fetch = pool.try_reserve_fetch(2_500, &consumer).unwrap();
        let err = pool.try_reserve_produce(1_800, None).unwrap_err();
        assert_eq!(err, BudgetRejectReason::ProcessCeiling);
        assert_eq!(pool.metrics().rejections(BudgetRejectReason::ProcessCeiling), 1);
        assert_eq!(pool.metrics().shard_used_bytes(), 0);
    }

    #[test]
    fn oversized_record_rejected_early() {
        let pool = MemoryBudgetPool::new(tiny_config()).unwrap();
        let err = pool.check_record_size(300, 300).unwrap_err();
        assert_eq!(err, BudgetRejectReason::OversizedRecord);
        assert_eq!(
            pool.metrics().rejections(BudgetRejectReason::OversizedRecord),
            1
        );
    }

    #[test]
    fn follower_inflight_and_catch_up_are_bounded() {
        let pool = MemoryBudgetPool::new(tiny_config()).unwrap();
        let follower = Arc::new(pool.open_follower());
        let a = follower.try_reserve_inflight(1_500).unwrap();
        let err = follower.try_reserve_inflight(1_000).unwrap_err();
        assert_eq!(err, BudgetRejectReason::ReplicaFollower);
        drop(a);
        let catch = follower.try_reserve_catch_up(1_000).unwrap();
        let err = follower.try_reserve_catch_up(100).unwrap_err();
        assert_eq!(err, BudgetRejectReason::ReplicaCatchUp);
        drop(catch);
        assert_eq!(follower.inflight_used_bytes(), 0);
        assert_eq!(follower.catch_up_used_bytes(), 0);
    }

    #[test]
    fn fetch_queue_budget_releases_on_drop() {
        let pool = MemoryBudgetPool::new(tiny_config()).unwrap();
        let conn = pool.open_consumer_connection();
        let reservation = pool.try_reserve_fetch(1_000, &conn).unwrap();
        assert_eq!(pool.metrics().fetch_queue_used_bytes(), 1_000);
        drop(reservation);
        assert_eq!(pool.metrics().fetch_queue_used_bytes(), 0);
    }
}
