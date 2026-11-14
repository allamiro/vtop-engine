//! Adaptive cross-session group commit for shared durability barriers.
//!
//! Concurrent produce requests join a bounded queue. The first waiter becomes
//! the flush leader, seals the group when the first configured threshold is
//! met, and runs one local durability / quorum cycle for every member. Each
//! request is acknowledged only after that shared barrier succeeds for the
//! offsets that include its records.
//!
//! A leader flushes ONE group (#752). Its own request is always in it (a
//! leader is the front of the queue when it takes the lead), so once the
//! group is published the leader's answer is ready, and it hands the lead to
//! the next waiter instead of flushing the groups queued behind it. Leading
//! on until the queue emptied answered the first leader last: under steady
//! load the queue never empties, and an append that succeeded could reach the
//! client's produce timeout unanswered.
//!
//! With `max_delay` zero (natural batching) a leader seals whatever is queued
//! the moment it leads: a lone request pays nothing, and the requests that
//! arrive while one group's barrier runs form the next group, so a group is
//! exactly as large as the barrier is long.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(vtop_loom))]
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// The coordinator's own synchronisation, swapped for loom's under the model
// test (#752); see `loom_model` below. Everything else stays std.
#[cfg(vtop_loom)]
use loom::sync::{Arc, Condvar, Mutex};
use vtop_protocol::{ErrorCode, Message, ProduceRequest, WireFrame};

/// Thresholds that seal a commit group. The first matched trigger wins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// Maximum time the flush leader waits for additional joiners.
    pub max_delay: Duration,
    /// Maximum records admitted into one commit group.
    pub max_records: usize,
    /// Maximum payload bytes (sum of key+value lengths) per commit group.
    pub max_bytes: u64,
    /// Hard ceiling on queued produce requests. Additional joins fail closed
    /// with [`ErrorCode::Overloaded`] rather than dropping silently.
    pub max_pending_requests: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_millis(5),
            max_records: 1_024,
            max_bytes: 1024 * 1024,
            max_pending_requests: 256,
        }
    }
}

impl GroupCommitConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_records == 0 {
            return Err("group commit max_records must be greater than zero".to_owned());
        }
        if self.max_bytes == 0 {
            return Err("group commit max_bytes must be greater than zero".to_owned());
        }
        if self.max_pending_requests == 0 {
            return Err("group commit max_pending_requests must be greater than zero".to_owned());
        }
        Ok(())
    }
}

/// Why a commit group sealed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushReason {
    MaxDelay,
    MaxRecords,
    MaxBytes,
    MaxPending,
    SingleRequest,
    /// No window (`max_delay` zero): the leader sealed the requests already
    /// queued when it took the lead.
    Queued,
    /// The next queued request asked for a different durability, so the
    /// group sealed before it rather than acknowledge it at another (#752).
    Durability,
}

/// One sealed commit group's observed sizes and timings.
#[derive(Clone, Debug, Default)]
pub struct GroupCommitSample {
    pub requests: u64,
    pub records: u64,
    pub bytes: u64,
    pub sync_duration: Duration,
    pub max_queue_wait: Duration,
    pub flush_reason: Option<FlushReason>,
}

/// Process-local counters for group-commit observability.
#[derive(Debug, Default)]
pub struct GroupCommitMetrics {
    commits_total: AtomicU64,
    requests_total: AtomicU64,
    records_total: AtomicU64,
    bytes_total: AtomicU64,
    sync_nanos_total: AtomicU64,
    queue_wait_nanos_total: AtomicU64,
    /// Requests refused because the queue was full.
    overloaded_total: AtomicU64,
    /// Groups refused whole by the log and re-run request by request (#752).
    split_groups_total: AtomicU64,
    // std's, not the model's: the metrics are not part of the protocol.
    last: std::sync::Mutex<GroupCommitSample>,
}

impl GroupCommitMetrics {
    pub fn record(&self, sample: GroupCommitSample) {
        self.commits_total.fetch_add(1, Ordering::Relaxed);
        self.requests_total
            .fetch_add(sample.requests, Ordering::Relaxed);
        self.records_total
            .fetch_add(sample.records, Ordering::Relaxed);
        self.bytes_total.fetch_add(sample.bytes, Ordering::Relaxed);
        self.sync_nanos_total.fetch_add(
            u64::try_from(sample.sync_duration.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.queue_wait_nanos_total.fetch_add(
            u64::try_from(sample.max_queue_wait.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = sample;
    }

    pub fn commits_total(&self) -> u64 {
        self.commits_total.load(Ordering::Relaxed)
    }

    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    pub fn records_total(&self) -> u64 {
        self.records_total.load(Ordering::Relaxed)
    }

    pub fn bytes_total(&self) -> u64 {
        self.bytes_total.load(Ordering::Relaxed)
    }

    pub fn sync_nanos_total(&self) -> u64 {
        self.sync_nanos_total.load(Ordering::Relaxed)
    }

    pub fn queue_wait_nanos_total(&self) -> u64 {
        self.queue_wait_nanos_total.load(Ordering::Relaxed)
    }

    /// Requests answered `Overloaded` because the queue was full.
    pub fn overloaded_total(&self) -> u64 {
        self.overloaded_total.load(Ordering::Relaxed)
    }

    /// Groups the log refused as a whole for a reason one member can cause,
    /// re-run request by request so only that member failed (#752).
    pub fn split_groups_total(&self) -> u64 {
        self.split_groups_total.load(Ordering::Relaxed)
    }

    /// Count a group re-run request by request.
    pub fn record_split(&self) {
        self.split_groups_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn last_sample(&self) -> GroupCommitSample {
        self.last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// A produce request waiting for (or participating in) a commit group.
#[derive(Debug)]
pub struct QueuedProduce {
    pub request_id: u64,
    pub stream_id: u64,
    pub request: ProduceRequest,
    pub enqueued_at: Instant,
    pub record_count: usize,
    pub payload_bytes: u64,
}

impl QueuedProduce {
    pub fn new(request_id: u64, stream_id: u64, request: ProduceRequest) -> Self {
        let record_count = request.records.len();
        let payload_bytes = request
            .records
            .iter()
            .map(|record| (record.key.len() + record.value.len()) as u64)
            .sum();
        Self {
            request_id,
            stream_id,
            request,
            enqueued_at: Instant::now(),
            record_count,
            payload_bytes,
        }
    }
}

#[derive(Debug)]
enum SlotState {
    Waiting,
    /// The previous leader handed this waiter the lead (#752).
    Lead,
    Ready(Box<WireFrame>),
}

/// What woke a waiting request.
enum Wake {
    /// Its group was flushed: this is its answer.
    Answer(Box<WireFrame>),
    /// It is the front of the queue and must flush the next group.
    Lead,
}

struct WaitSlot {
    state: Mutex<SlotState>,
    cv: Condvar,
}

impl WaitSlot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SlotState::Waiting),
            cv: Condvar::new(),
        })
    }

    fn publish(&self, frame: WireFrame) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = SlotState::Ready(Box::new(frame));
        self.cv.notify_one();
    }

    /// Wake this waiter to lead. Only a waiter still waiting can be handed
    /// the lead; one already answered is never at the front of the queue.
    fn hand_lead(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        debug_assert!(
            matches!(*state, SlotState::Waiting),
            "the lead is handed only to a queued, unanswered request"
        );
        *state = SlotState::Lead;
        self.cv.notify_one();
    }

    fn recv(&self) -> Wake {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            match std::mem::replace(&mut *state, SlotState::Waiting) {
                SlotState::Ready(frame) => return Wake::Answer(frame),
                SlotState::Lead => return Wake::Lead,
                SlotState::Waiting => {
                    state = self
                        .cv
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }
}

struct PendingEntry {
    item: QueuedProduce,
    slot: Arc<WaitSlot>,
}

struct CoordinatorState {
    pending: VecDeque<PendingEntry>,
    flushing: bool,
}

/// Per-broker (active segment) group-commit coordinator.
pub struct GroupCommitCoordinator {
    config: GroupCommitConfig,
    metrics: Arc<GroupCommitMetrics>,
    state: Mutex<CoordinatorState>,
    cv: Condvar,
}

impl GroupCommitCoordinator {
    pub fn new(config: GroupCommitConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            config,
            metrics: Arc::new(GroupCommitMetrics::default()),
            state: Mutex::new(CoordinatorState {
                pending: VecDeque::new(),
                flushing: false,
            }),
            cv: Condvar::new(),
        })
    }

    pub fn config(&self) -> &GroupCommitConfig {
        &self.config
    }

    pub fn metrics(&self) -> &Arc<GroupCommitMetrics> {
        &self.metrics
    }

    /// Enqueue `item` and block until its commit group has been flushed.
    ///
    /// `flush` runs on the leader waiter for each sealed group and must return
    /// one response frame per queued member, in order.
    pub fn enqueue_and_wait<F>(&self, item: QueuedProduce, mut flush: F) -> WireFrame
    where
        F: FnMut(&[QueuedProduce]) -> Vec<WireFrame>,
    {
        let slot = WaitSlot::new();
        let become_leader = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.pending.len() >= self.config.max_pending_requests {
                let queued = state.pending.len();
                drop(state);
                return self.refuse_full(item, queued);
            }
            // A leader exists, or a lead is being handed over, exactly while
            // `flushing` is set; with no leader the queue is empty, because
            // a leader that leaves work queued hands the lead on.
            let lead = !state.flushing;
            debug_assert!(
                !lead || state.pending.is_empty(),
                "a queue with no leader must be empty"
            );
            if lead {
                state.flushing = true;
            }
            state.pending.push_back(PendingEntry {
                item,
                slot: Arc::clone(&slot),
            });
            self.cv.notify_all();
            lead
        };

        if become_leader {
            self.lead_one_group(&mut flush);
        }
        loop {
            match slot.recv() {
                Wake::Answer(frame) => return *frame,
                Wake::Lead => self.lead_one_group(&mut flush),
            }
        }
    }

    /// Answer a request the full queue cannot take: `Overloaded`
    /// (retryable), nothing appended, and said so — rate-limited, with the
    /// count it suppressed — and counted.
    fn refuse_full(&self, item: QueuedProduce, queued: usize) -> WireFrame {
        self.metrics
            .overloaded_total
            .fetch_add(1, Ordering::Relaxed);
        vtop_meta::transport::warn_rate_limited(
            "group-commit-queue-full",
            &format!(
                "produce refused: {queued} requests are queued for the group commit and the \
                 limit is {}. This request was answered Overloaded (retryable) and nothing was \
                 appended. The range is receiving more than one barrier can drain; raise \
                 `group_commit.max_pending_requests` only if the disk and followers keep up, \
                 or add producers' back-off",
                self.config.max_pending_requests
            ),
        );
        overloaded_frame(
            item.request_id,
            item.stream_id,
            "group commit queue is full",
        )
    }

    /// Flush the group at the front of the queue, which holds this thread's
    /// own request, publish every member's answer, then hand the lead to
    /// the next waiter or, with nothing queued, give it up (#752).
    fn lead_one_group<F>(&self, flush: &mut F)
    where
        F: FnMut(&[QueuedProduce]) -> Vec<WireFrame>,
    {
        let (members, slots, reason, max_queue_wait) = self.collect_group();
        if !members.is_empty() {
            let started = Instant::now();
            let frames = flush(&members);
            let sync_duration = started.elapsed();
            debug_assert_eq!(
                frames.len(),
                slots.len(),
                "flush must return one frame per queued produce"
            );
            let records = members.iter().map(|item| item.record_count as u64).sum();
            let bytes = members.iter().map(|item| item.payload_bytes).sum();
            self.metrics.record(GroupCommitSample {
                requests: members.len() as u64,
                records,
                bytes,
                sync_duration,
                max_queue_wait,
                flush_reason: Some(reason),
            });
            for (slot, frame) in slots.into_iter().zip(frames) {
                slot.publish(frame);
            }
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match state.pending.front() {
            // `flushing` stays set: the lead passes, it is not given up, so
            // no arriving request can become a second leader meanwhile.
            Some(next) => next.slot.hand_lead(),
            None => state.flushing = false,
        }
        self.cv.notify_all();
    }

    fn collect_group(
        &self,
    ) -> (
        Vec<QueuedProduce>,
        Vec<Arc<WaitSlot>>,
        FlushReason,
        Duration,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadline = Instant::now() + self.config.max_delay;
        let reason = loop {
            let (records, bytes) = pending_totals(&state.pending);
            if state.pending.len() >= self.config.max_pending_requests {
                break FlushReason::MaxPending;
            }
            if records >= self.config.max_records {
                break FlushReason::MaxRecords;
            }
            if bytes >= self.config.max_bytes {
                break FlushReason::MaxBytes;
            }
            // Natural batching: no window, so no wait — seal what is queued.
            if self.config.max_delay.is_zero() && !state.pending.is_empty() {
                break if state.pending.len() == 1 {
                    FlushReason::SingleRequest
                } else {
                    FlushReason::Queued
                };
            }
            if !state.pending.is_empty() && Instant::now() >= deadline {
                break if state.pending.len() == 1 {
                    FlushReason::SingleRequest
                } else {
                    FlushReason::MaxDelay
                };
            }
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break FlushReason::MaxDelay;
            }
            let (guard, wait_result) = self
                .cv
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = guard;
            if wait_result.timed_out() && !state.pending.is_empty() {
                break if state.pending.len() == 1 {
                    FlushReason::SingleRequest
                } else {
                    FlushReason::MaxDelay
                };
            }
        };

        let mut members: Vec<QueuedProduce> = Vec::new();
        let mut slots = Vec::new();
        let mut taken_records = 0usize;
        let mut taken_bytes = 0u64;
        let mut max_queue_wait = Duration::ZERO;
        let mut split_by_durability = false;
        while let Some(front) = state.pending.front() {
            // A group is homogeneous (#752, D8): the barrier it runs is the
            // one its members asked for, so a member asking for another
            // seals the group before it and leads the next one.
            if let Some(first) = members.first() {
                if front.item.request.durability != first.request.durability {
                    split_by_durability = true;
                    break;
                }
            }
            let next_records = taken_records + front.item.record_count;
            let next_bytes = taken_bytes + front.item.payload_bytes;
            if !members.is_empty()
                && (next_records > self.config.max_records || next_bytes > self.config.max_bytes)
            {
                // Fairness: leave the remainder for the next group rather than
                // letting an oversized tail monopolize this cycle. A single
                // request that alone exceeds the limits still flushes alone.
                break;
            }
            let entry = state.pending.pop_front().expect("front existed");
            max_queue_wait = max_queue_wait.max(entry.item.enqueued_at.elapsed());
            taken_records += entry.item.record_count;
            taken_bytes += entry.item.payload_bytes;
            members.push(entry.item);
            slots.push(entry.slot);
            if taken_records >= self.config.max_records || taken_bytes >= self.config.max_bytes {
                break;
            }
        }
        debug_assert!(
            members
                .iter()
                .all(|item| item.request.durability == members[0].request.durability),
            "a sealed group is homogeneous in durability"
        );
        let reason = if split_by_durability {
            FlushReason::Durability
        } else if matches!(reason, FlushReason::SingleRequest) && members.len() > 1 {
            FlushReason::MaxDelay
        } else if members.len() == 1 && matches!(reason, FlushReason::Queued) {
            FlushReason::SingleRequest
        } else {
            reason
        };
        (members, slots, reason, max_queue_wait)
    }
}

fn pending_totals(pending: &VecDeque<PendingEntry>) -> (usize, u64) {
    let records = pending.iter().map(|entry| entry.item.record_count).sum();
    let bytes = pending.iter().map(|entry| entry.item.payload_bytes).sum();
    (records, bytes)
}

fn overloaded_frame(request_id: u64, stream_id: u64, message: &str) -> WireFrame {
    WireFrame {
        request_id,
        stream_id,
        message: Message::Error(vtop_protocol::ErrorResponse {
            code: ErrorCode::Overloaded,
            message: message.to_owned(),
            retryable: true,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;
    use vtop_protocol::{Durability, ProduceRecord, ProduceResponse, RangeIdentity};

    fn sample_request(sequence: u64) -> ProduceRequest {
        ProduceRequest {
            range: RangeIdentity {
                topic: "t".to_owned(),
                topic_epoch: 1,
                range_id: Uuid::from_u128(1),
                range_generation: 0,
            },
            fencing_epoch: 1,
            producer_id: Uuid::from_u128(u128::from(sequence) + 10),
            producer_epoch: 1,
            first_sequence: 0,
            durability: Durability::LocalFsync,
            records: vec![ProduceRecord {
                timestamp_millis: 1,
                key: b"k".to_vec(),
                value: format!("v{sequence}").into_bytes(),
            }],
        }
    }

    #[test]
    fn rejects_invalid_config() {
        let config = GroupCommitConfig {
            max_records: 0,
            ..Default::default()
        };
        assert!(GroupCommitCoordinator::new(config).is_err());
    }

    #[test]
    fn concurrent_waiters_share_one_flush() {
        let coord = Arc::new(
            GroupCommitCoordinator::new(GroupCommitConfig {
                max_delay: Duration::from_millis(50),
                max_records: 32,
                max_bytes: 1024 * 1024,
                max_pending_requests: 32,
            })
            .unwrap(),
        );
        let flush_count = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for index in 0..8u64 {
            let coord = Arc::clone(&coord);
            let flush_count = Arc::clone(&flush_count);
            handles.push(thread::spawn(move || {
                // Stagger slightly so several join before the delay seal.
                if index > 0 {
                    thread::sleep(Duration::from_millis(2));
                }
                let item = QueuedProduce::new(index + 1, 1, sample_request(index));
                coord.enqueue_and_wait(item, |batch| {
                    flush_count.fetch_add(1, Ordering::SeqCst);
                    batch
                        .iter()
                        .map(|item| WireFrame {
                            request_id: item.request_id,
                            stream_id: item.stream_id,
                            message: Message::ProduceResponse(ProduceResponse {
                                outcomes: Vec::new(),
                                committed_next_offset: batch.len() as u64,
                            }),
                        })
                        .collect()
                })
            }));
        }
        for handle in handles {
            let frame = handle.join().unwrap();
            assert!(matches!(frame.message, Message::ProduceResponse(_)));
        }
        assert_eq!(
            flush_count.load(Ordering::SeqCst),
            1,
            "concurrent producers must share one commit group flush"
        );
        assert_eq!(coord.metrics().commits_total(), 1);
        assert_eq!(coord.metrics().requests_total(), 8);
        assert_eq!(coord.metrics().last_sample().requests, 8);
    }

    #[test]
    fn full_queue_fails_closed_without_drop() {
        let coord = Arc::new(
            GroupCommitCoordinator::new(GroupCommitConfig {
                max_delay: Duration::from_millis(50),
                max_records: 1_000,
                max_bytes: 1024 * 1024,
                max_pending_requests: 1,
            })
            .unwrap(),
        );
        let gate = Arc::new(Mutex::new(()));
        let flush_hold = gate.lock().unwrap();
        let coord_leader = Arc::clone(&coord);
        let gate_leader = Arc::clone(&gate);
        let leader = thread::spawn(move || {
            let item = QueuedProduce::new(1, 1, sample_request(0));
            coord_leader.enqueue_and_wait(item, |batch| {
                let _hold = gate_leader.lock().unwrap();
                batch
                    .iter()
                    .map(|item| WireFrame {
                        request_id: item.request_id,
                        stream_id: item.stream_id,
                        message: Message::ProduceResponse(ProduceResponse {
                            outcomes: Vec::new(),
                            committed_next_offset: 1,
                        }),
                    })
                    .collect()
            })
        });
        thread::sleep(Duration::from_millis(20));
        let coord_second = Arc::clone(&coord);
        let second = thread::spawn(move || {
            let item = QueuedProduce::new(2, 1, sample_request(1));
            // The first leader hands this waiter the lead once its own
            // group is answered (#752), so it flushes its own group.
            coord_second.enqueue_and_wait(item, answer)
        });
        thread::sleep(Duration::from_millis(20));
        let rejected = coord.enqueue_and_wait(QueuedProduce::new(3, 1, sample_request(2)), |_| {
            unreachable!("overloaded join must not flush")
        });
        match rejected.message {
            Message::Error(error) => assert_eq!(error.code, ErrorCode::Overloaded),
            other => panic!("expected overloaded, got {other:?}"),
        }
        drop(flush_hold);
        assert!(matches!(
            leader.join().unwrap().message,
            Message::ProduceResponse(_)
        ));
        assert!(matches!(
            second.join().unwrap().message,
            Message::ProduceResponse(_)
        ));
    }

    fn answer(batch: &[QueuedProduce]) -> Vec<WireFrame> {
        batch
            .iter()
            .map(|item| WireFrame {
                request_id: item.request_id,
                stream_id: item.stream_id,
                message: Message::ProduceResponse(ProduceResponse {
                    outcomes: Vec::new(),
                    committed_next_offset: 0,
                }),
            })
            .collect()
    }

    fn zero_delay() -> GroupCommitConfig {
        GroupCommitConfig {
            max_delay: Duration::ZERO,
            ..GroupCommitConfig::default()
        }
    }

    /// D1 (#752), a guard: a request alone in the queue is flushed at once
    /// when the window is zero (natural batching), where the 5 ms default
    /// holds every solo request for the whole window. The best of five, so
    /// one preempted run cannot fail it.
    #[test]
    fn a_lone_request_is_not_delayed_by_the_window() {
        let lone = |config: GroupCommitConfig| {
            let coord = GroupCommitCoordinator::new(config).unwrap();
            (0..5u64)
                .map(|index| {
                    let started = Instant::now();
                    coord.enqueue_and_wait(
                        QueuedProduce::new(index, 1, sample_request(index)),
                        answer,
                    );
                    started.elapsed()
                })
                .min()
                .unwrap()
        };
        let windowed = lone(GroupCommitConfig::default());
        assert!(
            windowed >= Duration::from_millis(5),
            "the 5 ms default holds a lone request for its window: {windowed:?}"
        );
        let natural = lone(zero_delay());
        assert!(
            natural < Duration::from_millis(5),
            "with no window a lone request must not wait: {natural:?}"
        );
    }

    /// Defect 1 (#752, D7): the thread that leads a group is answered as
    /// soon as ITS group is flushed, not after it has gone on to flush every
    /// group that queued behind it. Six producers keep the queue busy for
    /// 600 ms, each re-joining 5 ms after its answer, so the queue is never
    /// empty when a group ends. Before the fix the first leader kept
    /// leading until a lull, which here never comes before the producers
    /// stop: its own append, flushed in the first 20 ms, was answered after
    /// about 600 ms, and in a node after up to the gateway's 30 s produce
    /// timeout. Groups hold three requests, so the six producers form two
    /// cohorts that keep each other's groups queued.
    #[test]
    fn the_leader_is_answered_before_it_starts_another_group() {
        const GROUP: Duration = Duration::from_millis(20);
        // Three requests a group: six producers form two cohorts, and the
        // one answered at the end of a group re-joins while the other's
        // group runs, so the queue is never empty when a group ends.
        let coord = Arc::new(
            GroupCommitCoordinator::new(GroupCommitConfig {
                max_records: 3,
                ..zero_delay()
            })
            .unwrap(),
        );
        let first_flush_started = Arc::new(std::sync::Barrier::new(2));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flushes = Arc::new(AtomicU64::new(0));

        let leader = {
            let coord = Arc::clone(&coord);
            let first_flush_started = Arc::clone(&first_flush_started);
            let flushes = Arc::clone(&flushes);
            thread::spawn(move || {
                let started = Instant::now();
                coord.enqueue_and_wait(QueuedProduce::new(1, 1, sample_request(1)), |batch| {
                    if flushes.fetch_add(1, Ordering::SeqCst) == 0 {
                        first_flush_started.wait();
                    }
                    thread::sleep(GROUP);
                    answer(batch)
                });
                started.elapsed()
            })
        };
        first_flush_started.wait();
        let producers: Vec<_> = (0..6u64)
            .map(|index| {
                let coord = Arc::clone(&coord);
                let stop = Arc::clone(&stop);
                let flushes = Arc::clone(&flushes);
                thread::spawn(move || {
                    while !stop.load(Ordering::SeqCst) {
                        coord.enqueue_and_wait(
                            QueuedProduce::new(100 + index, 1, sample_request(100 + index)),
                            |batch| {
                                flushes.fetch_add(1, Ordering::SeqCst);
                                thread::sleep(GROUP);
                                answer(batch)
                            },
                        );
                        thread::sleep(Duration::from_millis(5));
                    }
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(600));
        stop.store(true, Ordering::SeqCst);
        let leader_latency = leader.join().unwrap();
        for producer in producers {
            producer.join().unwrap();
        }
        assert!(
            flushes.load(Ordering::SeqCst) >= 10,
            "the producers must have kept the queue busy for the test to mean anything: {} \
             flushes",
            flushes.load(Ordering::SeqCst)
        );
        assert!(
            leader_latency < GROUP * 2 + Duration::from_millis(60),
            "the first leader's own append was flushed in its first {GROUP:?} group, and it \
             was answered after {leader_latency:?}: it kept leading the groups queued behind \
             it instead of answering itself"
        );
    }

    /// D8 (#752): a group is homogeneous in durability. A member whose
    /// durability differs from the group's first seals the group before it
    /// and starts its own, in queue order, so no member is acknowledged at
    /// a durability weaker than it asked for. Before the fix a group took
    /// `batch[0]`'s durability for every member.
    #[test]
    fn a_group_never_mixes_durabilities() {
        let coord = Arc::new(GroupCommitCoordinator::new(zero_delay()).unwrap());
        let hold = Arc::new(Mutex::new(()));
        let held = hold.lock().unwrap();
        type Groups = Vec<Vec<(u64, Durability)>>;
        let groups: Arc<Mutex<Groups>> = Arc::default();
        let flush = {
            let hold = Arc::clone(&hold);
            let groups = Arc::clone(&groups);
            move |batch: &[QueuedProduce]| {
                let _hold = hold.lock().unwrap();
                groups.lock().unwrap().push(
                    batch
                        .iter()
                        .map(|item| (item.request_id, item.request.durability))
                        .collect(),
                );
                answer(batch)
            }
        };
        let durable = |request_id: u64, durability: Durability| {
            let mut request = sample_request(request_id);
            request.durability = durability;
            QueuedProduce::new(request_id, 1, request)
        };
        let mut threads = Vec::new();
        // The first holds the flush; the next three queue behind it.
        for (request_id, durability) in [
            (1, Durability::Quorum),
            (2, Durability::Quorum),
            (3, Durability::LocalFsync),
            (4, Durability::Quorum),
        ] {
            let coord = Arc::clone(&coord);
            let flush = flush.clone();
            let item = durable(request_id, durability);
            threads.push(thread::spawn(move || coord.enqueue_and_wait(item, flush)));
            thread::sleep(Duration::from_millis(30));
        }
        drop(held);
        for thread in threads {
            assert!(matches!(
                thread.join().unwrap().message,
                Message::ProduceResponse(_)
            ));
        }
        let groups = groups.lock().unwrap();
        for group in groups.iter() {
            assert!(
                group
                    .iter()
                    .all(|(_, durability)| *durability == group[0].1),
                "a group mixed durabilities: {group:?} (all groups: {groups:?})"
            );
        }
        let order: Vec<u64> = groups.iter().flatten().map(|(id, _)| *id).collect();
        assert_eq!(order, [1, 2, 3, 4], "queue order is kept across the split");
    }
}

/// Every interleaving of a small model of the coordinator (#752): lost
/// wake-ups and a lead handed to nobody would show as a deadlock, which loom
/// reports; an answer delivered to the wrong waiter or twice would fail the
/// assertions. Natural batching (no window), so no timed wait is involved.
///
/// Run with
/// `RUSTFLAGS="--cfg vtop_loom" cargo test -p vtop-broker --lib --release loom_model`.
#[cfg(all(test, vtop_loom))]
mod loom_model {
    use super::*;
    use uuid::Uuid;
    use vtop_protocol::{Durability, ProduceRecord, ProduceResponse, RangeIdentity};

    fn item(request_id: u64) -> QueuedProduce {
        QueuedProduce::new(
            request_id,
            1,
            ProduceRequest {
                range: RangeIdentity {
                    topic: "t".to_owned(),
                    topic_epoch: 1,
                    range_id: Uuid::from_u128(1),
                    range_generation: 0,
                },
                fencing_epoch: 1,
                producer_id: Uuid::from_u128(u128::from(request_id)),
                producer_epoch: 1,
                first_sequence: 0,
                durability: Durability::Quorum,
                records: vec![ProduceRecord {
                    timestamp_millis: 1,
                    key: Vec::new(),
                    value: Vec::new(),
                }],
            },
        )
    }

    /// Answers each member with its own request id as the offset, so a
    /// misdelivered answer is visible.
    fn echo(batch: &[QueuedProduce]) -> Vec<WireFrame> {
        batch
            .iter()
            .map(|item| WireFrame {
                request_id: item.request_id,
                stream_id: item.stream_id,
                message: Message::ProduceResponse(ProduceResponse {
                    outcomes: Vec::new(),
                    committed_next_offset: item.request_id,
                }),
            })
            .collect()
    }

    fn answered_with_its_own(frame: WireFrame, request_id: u64) {
        assert_eq!(frame.request_id, request_id);
        match frame.message {
            Message::ProduceResponse(response) => {
                assert_eq!(response.committed_next_offset, request_id)
            }
            other => panic!("request {request_id} was not answered: {other:?}"),
        }
    }

    #[test]
    fn three_producers_are_each_answered_once_and_the_lead_is_never_lost() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let coord = Arc::new(
                GroupCommitCoordinator::new(GroupCommitConfig {
                    max_delay: Duration::ZERO,
                    ..GroupCommitConfig::default()
                })
                .unwrap(),
            );
            let others: Vec<_> = [2u64, 3]
                .into_iter()
                .map(|request_id| {
                    let coord = Arc::clone(&coord);
                    loom::thread::spawn(move || {
                        answered_with_its_own(
                            coord.enqueue_and_wait(item(request_id), echo),
                            request_id,
                        )
                    })
                })
                .collect();
            answered_with_its_own(coord.enqueue_and_wait(item(1), echo), 1);
            for other in others {
                other.join().unwrap();
            }
            let state = coord.state.lock().unwrap();
            assert!(state.pending.is_empty(), "every request was taken");
            assert!(
                !state.flushing,
                "the lead was given up once the queue emptied"
            );
            assert_eq!(coord.metrics().requests_total(), 3);
        });
    }
}
