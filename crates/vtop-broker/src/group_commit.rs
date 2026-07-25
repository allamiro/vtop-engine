//! Adaptive cross-session group commit for shared durability barriers.
//!
//! Concurrent produce requests join a bounded queue. The first waiter becomes
//! the flush leader, seals the group when the first configured threshold is
//! met, and runs one local durability / quorum cycle for every member. Each
//! request is acknowledged only after that shared barrier succeeds for the
//! offsets that include its records.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
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
    last: Mutex<GroupCommitSample>,
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
    Ready(Box<WireFrame>),
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

    fn recv(&self) -> WireFrame {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            match &*state {
                SlotState::Ready(_) => {
                    if let SlotState::Ready(frame) =
                        std::mem::replace(&mut *state, SlotState::Waiting)
                    {
                        return *frame;
                    }
                }
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
                return overloaded_frame(
                    item.request_id,
                    item.stream_id,
                    "group commit queue is full",
                );
            }
            let lead = !state.flushing && state.pending.is_empty();
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
            self.run_leader(&mut flush);
        }
        slot.recv()
    }

    fn run_leader<F>(&self, flush: &mut F)
    where
        F: FnMut(&[QueuedProduce]) -> Vec<WireFrame>,
    {
        loop {
            let (members, slots, reason, max_queue_wait) = self.collect_group();
            if members.is_empty() {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.flushing = false;
                self.cv.notify_all();
                return;
            }
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

            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.pending.is_empty() {
                state.flushing = false;
                self.cv.notify_all();
                return;
            }
            // More waiters arrived during the barrier; keep leading so they
            // cannot deadlock waiting for a new enqueue to pick up leadership.
        }
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
            if !state.pending.is_empty() && Instant::now() >= deadline {
                break if state.pending.len() == 1 {
                    FlushReason::SingleRequest
                } else {
                    FlushReason::MaxDelay
                };
            }
            if state.pending.is_empty() {
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
                continue;
            }
            // Have at least one request: wait for joiners until delay/threshold.
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break if state.pending.len() == 1 {
                    FlushReason::SingleRequest
                } else {
                    FlushReason::MaxDelay
                };
            }
            let (guard, wait_result) = self
                .cv
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = guard;
            if wait_result.timed_out() {
                break if state.pending.len() == 1 {
                    FlushReason::SingleRequest
                } else {
                    FlushReason::MaxDelay
                };
            }
        };

        let mut members = Vec::new();
        let mut slots = Vec::new();
        let mut taken_records = 0usize;
        let mut taken_bytes = 0u64;
        let mut max_queue_wait = Duration::ZERO;
        while let Some(front) = state.pending.front() {
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
        let reason = if matches!(reason, FlushReason::SingleRequest) && members.len() > 1 {
            FlushReason::MaxDelay
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
            coord_second.enqueue_and_wait(item, |_batch| {
                unreachable!("second waiter is not the flush leader");
            })
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
}
