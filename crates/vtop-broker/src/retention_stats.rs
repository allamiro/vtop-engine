//! What retention has been doing to a range, for the exporter (#561).
//!
//! A retention pass runs after every append and used to be silent unless it
//! failed: a range whose committed floor blocked every segment returned the
//! same empty outcome as one comfortably under its bound, so disk grew past
//! the bound with nothing on any panel. The pass's outcome is now recorded
//! here, and a change in whether (and why) the range is blocked is logged
//! once, not on every append.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use vtop_log::{LogError, RetentionBlocked, RetentionOutcome};

/// Monotonic counters and the latest sizes, readable without a lock.
#[derive(Debug, Default)]
pub struct RetentionStats {
    retained_bytes: AtomicU64,
    bound_bytes: AtomicU64,
    passes_blocked: [AtomicU64; RetentionBlocked::ALL.len()],
    failures: AtomicU64,
    reclaimed_bytes: AtomicU64,
    reclaimed_segments: AtomicU64,
    /// 0 = not blocked; otherwise 1 + the index of the reason in `ALL`.
    blocked_now: AtomicU8,
    last_pass_failed: AtomicBool,
    /// A pass has SUCCEEDED, so `retained_bytes` and `blocked_now` are
    /// measurements. A first pass that failed measured nothing, and its zero
    /// must not be exported as a size (review of #572).
    measured: AtomicBool,
    /// The retention policy's generation, bumped by every policy change and
    /// held while a pass's result is recorded or the state cleared (review
    /// of #572). A pass that loaded the OLD bound and finishes after the
    /// policy changed would otherwise record over the clear - restoring a
    /// bound, size and blocked reason for retention that no longer runs,
    /// with no later pass to replace them.
    policy: std::sync::Mutex<u64>,
}

/// One reading of [`RetentionStats`] for an exporter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionSnapshot {
    /// Content bytes after the latest successful pass; `None` while no pass
    /// has succeeded (only failures so far).
    pub retained_bytes: Option<u64>,
    /// The bound the latest pass ran against.
    pub bound_bytes: u64,
    /// Whether the latest successful pass ended above the bound, and why.
    pub blocked: Option<RetentionBlocked>,
    /// Whether the latest pass failed outright.
    pub last_pass_failed: bool,
}

fn index(reason: RetentionBlocked) -> usize {
    RetentionBlocked::ALL
        .iter()
        .position(|r| *r == reason)
        .expect("every reason is in ALL")
}

impl RetentionStats {
    fn policy_lock(&self) -> std::sync::MutexGuard<'_, u64> {
        self.policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The policy generation a pass runs under.
    pub fn policy_generation(&self) -> u64 {
        *self.policy_lock()
    }

    /// Read the bound a pass will run with, together with the generation it
    /// belongs to - under the policy lock, so the pair is never torn by a
    /// concurrent [`Self::change_policy`] (review of #572).
    pub fn policy<R>(&self, read_bound: impl FnOnce() -> R) -> (u64, R) {
        let policy = self.policy_lock();
        (*policy, read_bound())
    }

    /// Change the retention policy: `store_bound` publishes the new bound,
    /// and under the same lock the generation moves and what the last pass
    /// recorded is forgotten (review of #572). A pass of the old generation
    /// then records nothing; one of the new generation records as always.
    /// Forgetting on EVERY change, not only on disable: a pass that ran
    /// under the old bound and is rejected must not leave the old bound and
    /// blocked state exported until some later append replaces them.
    pub fn change_policy(&self, store_bound: impl FnOnce()) {
        let mut policy = self.policy_lock();
        store_bound();
        *policy += 1;
        self.forget();
    }

    /// Fold one pass's result in, under the current policy.
    pub fn record(
        &self,
        what: &str,
        bound_bytes: u64,
        result: &Result<RetentionOutcome, LogError>,
    ) {
        self.record_for(self.policy_generation(), what, bound_bytes, result);
    }

    /// Fold in the result of a pass that ran under policy `generation`;
    /// nothing is recorded if the policy has changed since (review of #572).
    /// `what` names the range in a log line.
    pub fn record_for(
        &self,
        generation: u64,
        what: &str,
        bound_bytes: u64,
        result: &Result<RetentionOutcome, LogError>,
    ) {
        let policy = self.policy_lock();
        if *policy != generation {
            return;
        }
        self.bound_bytes.store(bound_bytes, Ordering::Relaxed);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(problem) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                self.last_pass_failed.store(true, Ordering::Relaxed);
                eprintln!(
                    "retention on {what} failed and will be retried after the next append: {problem}"
                );
                return;
            }
        };
        self.last_pass_failed.store(false, Ordering::Relaxed);
        self.retained_bytes
            .store(outcome.retained_bytes, Ordering::Relaxed);
        self.reclaimed_bytes
            .fetch_add(outcome.bytes_removed, Ordering::Relaxed);
        self.reclaimed_segments
            .fetch_add(outcome.segments_removed as u64, Ordering::Relaxed);
        let now = match outcome.blocked {
            Some(reason) => {
                self.passes_blocked[index(reason)].fetch_add(1, Ordering::Relaxed);
                1 + index(reason) as u8
            }
            None => 0,
        };
        let before = self.blocked_now.swap(now, Ordering::Relaxed);
        // LAST, and Release (review): a scrape that sees `measured` (with
        // Acquire) sees the size and blocked state this pass measured, not
        // the zero/unblocked defaults a first pass is still replacing.
        self.measured.store(true, Ordering::Release);
        if before != now {
            match outcome.blocked {
                Some(reason) => eprintln!(
                    "retention on {what} cannot reach its bound ({bound_bytes} bytes; {} retained): \
                     blocked by {}",
                    outcome.retained_bytes,
                    match reason {
                        RetentionBlocked::CommittedFloor =>
                            "the committed floor - a follower or the quorum mark is behind, \
                             and the oldest segment still holds records it needs",
                        RetentionBlocked::ActiveSegment =>
                            "the active segment, which is never reclaimed - the bound is \
                             smaller than one segment",
                    }
                ),
                None => eprintln!(
                    "retention on {what} is back under its bound ({} of {bound_bytes} bytes)",
                    outcome.retained_bytes
                ),
            }
        }
    }

    /// A reading for an exporter; `None` before any pass has run (retention
    /// disabled, or nothing appended yet), so a range without retention
    /// exports nothing rather than a misleading bound of zero.
    pub fn snapshot(&self) -> Option<RetentionSnapshot> {
        let bound_bytes = self.bound_bytes();
        let measured = self.measured.load(Ordering::Acquire);
        (bound_bytes > 0).then(|| RetentionSnapshot {
            retained_bytes: measured.then(|| self.retained_bytes()),
            bound_bytes,
            blocked: self.blocked_now(),
            last_pass_failed: self.last_pass_failed.load(Ordering::Relaxed),
        })
    }

    /// Retention was turned off: no pass will run to replace what was last
    /// recorded, so it is forgotten and the snapshot is `None` again - the
    /// exporter removes the series instead of reporting a bound, a size and
    /// a blocked reason that no longer apply (review of #572). The counters
    /// stay; they count what happened.
    pub fn clear(&self) {
        let mut policy = self.policy_lock();
        *policy += 1;
        self.forget();
    }

    fn forget(&self) {
        self.bound_bytes.store(0, Ordering::Relaxed);
        self.measured.store(false, Ordering::Relaxed);
        self.blocked_now.store(0, Ordering::Relaxed);
        self.last_pass_failed.store(false, Ordering::Relaxed);
    }

    /// Content bytes the range held after the latest pass.
    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    /// The bound the latest pass ran against; 0 before any pass.
    pub fn bound_bytes(&self) -> u64 {
        self.bound_bytes.load(Ordering::Relaxed)
    }

    /// Passes that ended above the bound, by reason.
    pub fn passes_blocked(&self, reason: RetentionBlocked) -> u64 {
        self.passes_blocked[index(reason)].load(Ordering::Relaxed)
    }

    /// Whether the latest pass ended above the bound, and why.
    pub fn blocked_now(&self) -> Option<RetentionBlocked> {
        match self.blocked_now.load(Ordering::Relaxed) {
            0 => None,
            n => RetentionBlocked::ALL.get(usize::from(n) - 1).copied(),
        }
    }

    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn reclaimed_bytes(&self) -> u64 {
        self.reclaimed_bytes.load(Ordering::Relaxed)
    }

    pub fn reclaimed_segments(&self) -> u64 {
        self.reclaimed_segments.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(removed: u64, retained: u64, blocked: Option<RetentionBlocked>) -> RetentionOutcome {
        RetentionOutcome {
            segments_removed: usize::from(removed > 0),
            bytes_removed: removed,
            retained_bytes: retained,
            blocked,
        }
    }

    #[test]
    fn a_blocked_range_is_counted_and_visible_until_it_recovers() {
        let stats = RetentionStats::default();
        stats.record("t", 100, &Ok(outcome(0, 50, None)));
        assert_eq!(stats.blocked_now(), None);
        assert_eq!(stats.retained_bytes(), 50);
        assert_eq!(stats.bound_bytes(), 100);

        for _ in 0..3 {
            stats.record(
                "t",
                100,
                &Ok(outcome(0, 400, Some(RetentionBlocked::CommittedFloor))),
            );
        }
        assert_eq!(stats.blocked_now(), Some(RetentionBlocked::CommittedFloor));
        assert_eq!(stats.passes_blocked(RetentionBlocked::CommittedFloor), 3);
        assert_eq!(stats.passes_blocked(RetentionBlocked::ActiveSegment), 0);
        assert_eq!(stats.retained_bytes(), 400);

        stats.record("t", 100, &Ok(outcome(350, 50, None)));
        assert_eq!(stats.blocked_now(), None);
        assert_eq!(stats.reclaimed_bytes(), 350);
        assert_eq!(stats.reclaimed_segments(), 1);
    }

    #[test]
    fn a_failed_pass_is_counted_and_leaves_the_last_sizes() {
        let stats = RetentionStats::default();
        stats.record("t", 100, &Ok(outcome(0, 70, None)));
        stats.record(
            "t",
            100,
            &Err(LogError::TruncateBeyondTail {
                requested: 1,
                next_offset: 0,
            }),
        );
        assert_eq!(stats.failures(), 1);
        assert_eq!(stats.retained_bytes(), 70);
        assert!(stats.snapshot().unwrap().last_pass_failed);
        stats.record("t", 100, &Ok(outcome(0, 70, None)));
        assert!(!stats.snapshot().unwrap().last_pass_failed);
    }

    #[test]
    fn nothing_is_exported_before_a_pass_has_run() {
        assert_eq!(RetentionStats::default().snapshot(), None);
    }

    /// Review of #572: a first pass that failed measured nothing. Its failure
    /// is exported; a retained size of zero is not.
    #[test]
    fn a_failed_first_pass_reports_the_failure_and_no_size() {
        let stats = RetentionStats::default();
        stats.record(
            "t",
            100,
            &Err(LogError::TruncateBeyondTail {
                requested: 1,
                next_offset: 0,
            }),
        );
        let snapshot = stats.snapshot().expect("the failure is reported");
        assert!(snapshot.last_pass_failed);
        assert_eq!(snapshot.retained_bytes, None, "a size nothing measured");
        stats.record("t", 100, &Ok(outcome(0, 70, None)));
        assert_eq!(stats.snapshot().unwrap().retained_bytes, Some(70));
    }

    /// Review of #572: once retention is turned off no pass replaces the last
    /// one's state, so clearing is what stops a stale bound or blocked reason
    /// being exported.
    /// Review of #572: a pass that loaded the old bound and records after
    /// retention was disabled (or rebound) records nothing - otherwise it
    /// restores the state the clear removed, and no later pass replaces it.
    #[test]
    fn a_pass_from_before_a_policy_change_records_nothing() {
        let stats = RetentionStats::default();
        let outcome = |retained_bytes| {
            Ok(RetentionOutcome {
                retained_bytes,
                ..RetentionOutcome::default()
            })
        };
        let before = stats.policy_generation();
        stats.clear(); // disabled while the pass ran
        stats.record_for(before, "r", 4_096, &outcome(9_000));
        assert_eq!(
            stats.snapshot(),
            None,
            "the stale pass restored the cleared state"
        );

        // Rebound while a pass ran: the old pass records nothing, and the
        // state of the passes before it is forgotten too.
        stats.record_for(stats.policy_generation(), "r", 4_096, &outcome(9_000));
        assert!(stats.snapshot().is_some());
        let before = stats.policy_generation();
        stats.change_policy(|| {});
        assert_eq!(
            stats.snapshot(),
            None,
            "a rebind forgets the old bound's state"
        );
        stats.record_for(before, "r", 4_096, &outcome(9_000));
        assert_eq!(stats.snapshot(), None);

        // A pass under the current policy records as always.
        stats.record_for(stats.policy_generation(), "r", 8_192, &outcome(100));
        assert_eq!(stats.snapshot().map(|s| s.bound_bytes), Some(8_192));
    }

    #[test]
    fn clearing_forgets_the_state_and_keeps_the_counts() {
        let stats = RetentionStats::default();
        stats.record(
            "t",
            100,
            &Ok(outcome(0, 400, Some(RetentionBlocked::CommittedFloor))),
        );
        stats.clear();
        assert_eq!(stats.snapshot(), None);
        assert_eq!(stats.passes_blocked(RetentionBlocked::CommittedFloor), 1);
    }
}
