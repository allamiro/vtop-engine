//! Where a produce's time goes, by phase (#752).
//!
//! A produce on a replicated range pays three things in series: the leader's
//! local append and fsync, the replication round (each acking follower's own
//! fsync included), and the commit (the lease re-check, the cluster
//! high-water mark advance and the follower mark propagation). The committed
//! benchmark run showed about 6 to 7 ms per request whatever the record size,
//! and nothing said which of the three it was. These totals do, before
//! anything about the path is changed.
//!
//! Counted per FLUSH, not per request: with group commit on, one flush
//! covers a whole group, and dividing a total by
//! `vtop_broker_group_commits_total` gives the mean per group; with it off,
//! every produce is its own flush. A flush that fails a phase still counts
//! the time it spent there.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// One phase of the produce path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducePhase {
    /// The leader's local append and its fsync, under the broker's locks.
    LocalFsync,
    /// The replica fan-out, until a quorum acked or the round gave up.
    Replicate,
    /// The lease re-check, the cluster high-water mark advance, retention's
    /// second pass and the mark's propagation to followers.
    Commit,
}

impl ProducePhase {
    /// Every phase, in the order a produce pays them.
    pub const ALL: [ProducePhase; 3] = [
        ProducePhase::LocalFsync,
        ProducePhase::Replicate,
        ProducePhase::Commit,
    ];

    /// The bounded label value this phase is exported under.
    pub fn as_str(self) -> &'static str {
        match self {
            ProducePhase::LocalFsync => "local_fsync",
            ProducePhase::Replicate => "replicate",
            ProducePhase::Commit => "commit",
        }
    }
}

/// Monotonic per-phase totals, in nanoseconds.
#[derive(Debug, Default)]
pub struct ProducePhaseMetrics {
    local_fsync_nanos: AtomicU64,
    replicate_nanos: AtomicU64,
    commit_nanos: AtomicU64,
}

impl ProducePhaseMetrics {
    fn cell(&self, phase: ProducePhase) -> &AtomicU64 {
        match phase {
            ProducePhase::LocalFsync => &self.local_fsync_nanos,
            ProducePhase::Replicate => &self.replicate_nanos,
            ProducePhase::Commit => &self.commit_nanos,
        }
    }

    /// Add `spent` to `phase`'s total. Saturates rather than wrapping: a
    /// counter that went backwards would read as a process restart.
    pub fn record(&self, phase: ProducePhase, spent: Duration) {
        let nanos = u64::try_from(spent.as_nanos()).unwrap_or(u64::MAX);
        let cell = self.cell(phase);
        let mut current = cell.load(Ordering::Relaxed);
        // A compare-exchange loop, not `fetch_update`, for the MSRV (see
        // vtop-rust-practices): the saturating add is the point.
        loop {
            let next = current.saturating_add(nanos);
            match cell.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Nanoseconds spent in `phase` since the broker was built.
    pub fn nanos(&self, phase: ProducePhase) -> u64 {
        self.cell(phase).load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_add_per_phase_and_saturate() {
        let metrics = ProducePhaseMetrics::default();
        metrics.record(ProducePhase::LocalFsync, Duration::from_micros(3));
        metrics.record(ProducePhase::LocalFsync, Duration::from_micros(4));
        metrics.record(ProducePhase::Commit, Duration::from_nanos(9));
        assert_eq!(metrics.nanos(ProducePhase::LocalFsync), 7_000);
        assert_eq!(metrics.nanos(ProducePhase::Replicate), 0);
        assert_eq!(metrics.nanos(ProducePhase::Commit), 9);
        metrics.record(ProducePhase::Commit, Duration::MAX);
        assert_eq!(metrics.nanos(ProducePhase::Commit), u64::MAX);
        let labels: Vec<_> = ProducePhase::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(labels, ["local_fsync", "replicate", "commit"]);
    }
}
