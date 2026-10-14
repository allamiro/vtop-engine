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
}

/// One reading of [`RetentionStats`] for an exporter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionSnapshot {
    /// Content bytes after the latest successful pass.
    pub retained_bytes: u64,
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
    /// Fold one pass's result in. `what` names the range in a log line.
    pub fn record(
        &self,
        what: &str,
        bound_bytes: u64,
        result: &Result<RetentionOutcome, LogError>,
    ) {
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
        (bound_bytes > 0).then(|| RetentionSnapshot {
            retained_bytes: self.retained_bytes(),
            bound_bytes,
            blocked: self.blocked_now(),
            last_pass_failed: self.last_pass_failed.load(Ordering::Relaxed),
        })
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
}
