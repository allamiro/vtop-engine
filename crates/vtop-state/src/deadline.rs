//! A deadline on every state-store request (#556).
//!
//! The engine awaits the ledger on its hot path: a batch cannot be recorded,
//! verified or committed without it. A store that accepts a connection and
//! then never answers (a Postgres primary that was blackholed mid-failover,
//! a half-open TCP connection) blocks that await until the kernel gives up on
//! the socket, which is many minutes, with the process alive, the health
//! endpoint green and no error logged anywhere. Every request here is bounded
//! instead, and an expired one is a [`VtopError::State`] the engine already
//! handles like any other ledger failure.
//!
//! A timed-out request is an UNKNOWN outcome, not a failed one: the statement
//! may have committed on the server. That is safe for this ledger by
//! construction. Every transition is checked against the state machine and is
//! idempotent in effect, a source commit is only ever applied after the
//! batch is recorded VERIFIED, and recovery re-reads rows it finds
//! incomplete. The worst case is a range archived twice, never a lost one.

use crate::models::{BatchPatch, BatchRecord, CommittedCursor};
use crate::store::StateStore;
use async_trait::async_trait;
use std::future::Future;
use std::time::Duration;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;

/// The deadline [`crate::connect_state_store`] applies when the caller does
/// not choose one.
pub const DEFAULT_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Whole-ledger operations (startup cursor seeding, recovery claims, history
/// pruning) get this many times the single-row deadline: they legitimately
/// touch many rows, and failing startup because a scan took longer than a
/// point read would trade a hang for an outage.
const SCAN_FACTOR: u32 = 10;

/// Wraps a [`StateStore`] so that no request outlives its deadline.
pub struct DeadlineStore {
    inner: Box<dyn StateStore>,
    point: Duration,
    scan: Duration,
}

impl DeadlineStore {
    /// `point` bounds single-row operations; scans get ten times it.
    pub fn new(inner: Box<dyn StateStore>, point: Duration) -> Self {
        Self {
            inner,
            point,
            scan: point.saturating_mul(SCAN_FACTOR),
        }
    }

    async fn within<T>(
        &self,
        operation: &'static str,
        limit: Duration,
        call: impl Future<Output = Result<T, VtopError>>,
    ) -> Result<T, VtopError> {
        match tokio::time::timeout(limit, call).await {
            Ok(result) => result,
            Err(_) => Err(VtopError::State(format!(
                "state store {operation} did not complete within {limit:?}: no answer \
                 (blackholed connection, failover in progress?). The request's outcome is unknown"
            ))),
        }
    }
}

#[async_trait]
impl StateStore for DeadlineStore {
    async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
        self.within(
            "save_batch_state",
            self.point,
            self.inner.save_batch_state(rec),
        )
        .await
    }

    async fn update_batch_state(
        &self,
        batch_id: &str,
        to: BatchState,
        patch: &BatchPatch,
    ) -> Result<(), VtopError> {
        self.within(
            "update_batch_state",
            self.point,
            self.inner.update_batch_state(batch_id, to, patch),
        )
        .await
    }

    async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
        self.within("get_batch", self.point, self.inner.get_batch(batch_id))
            .await
    }

    async fn claim_incomplete_batches(
        &self,
        owner: &str,
        now: &str,
        lease_until: &str,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        self.within(
            "claim_incomplete_batches",
            self.scan,
            self.inner.claim_incomplete_batches(owner, now, lease_until),
        )
        .await
    }

    async fn max_committed_end_bytes(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<(String, u64)>, VtopError> {
        self.within(
            "max_committed_end_bytes",
            self.scan,
            self.inner.max_committed_end_bytes(source_type),
        )
        .await
    }

    async fn committed_cursors(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<CommittedCursor>, VtopError> {
        self.within(
            "committed_cursors",
            self.scan,
            self.inner.committed_cursors(source_type),
        )
        .await
    }

    async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.within("list_batches", self.scan, self.inner.list_batches())
            .await
    }

    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.within(
            "list_incomplete_batches",
            self.scan,
            self.inner.list_incomplete_batches(),
        )
        .await
    }

    async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.within(
            "list_failed_batches",
            self.scan,
            self.inner.list_failed_batches(),
        )
        .await
    }

    async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError> {
        self.within(
            "mark_failed",
            self.point,
            self.inner.mark_failed(batch_id, message),
        )
        .await
    }

    async fn mark_verified(&self, batch_id: &str) -> Result<(), VtopError> {
        self.within(
            "mark_verified",
            self.point,
            self.inner.mark_verified(batch_id),
        )
        .await
    }

    async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError> {
        self.within(
            "mark_source_committed",
            self.point,
            self.inner.mark_source_committed(batch_id),
        )
        .await
    }

    async fn prune_committed_history(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<u64, VtopError> {
        self.within(
            "prune_committed_history",
            self.scan,
            self.inner.prune_committed_history(older_than, limit),
        )
        .await
    }

    async fn prune_candidates(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        self.within(
            "prune_candidates",
            self.scan,
            self.inner.prune_candidates(older_than, limit),
        )
        .await
    }

    async fn prune_listed(&self, older_than: &str, batch_ids: &[String]) -> Result<u64, VtopError> {
        self.within(
            "prune_listed",
            self.scan,
            self.inner.prune_listed(older_than, batch_ids),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite_store::SqliteStateStore;

    /// A ledger that accepts every request and never answers: the blackholed
    /// Postgres of a failover.
    struct Silent;

    macro_rules! never {
        () => {
            std::future::pending().await
        };
    }

    #[async_trait]
    impl StateStore for Silent {
        async fn save_batch_state(&self, _: &BatchRecord) -> Result<(), VtopError> {
            never!()
        }
        async fn update_batch_state(
            &self,
            _: &str,
            _: BatchState,
            _: &BatchPatch,
        ) -> Result<(), VtopError> {
            never!()
        }
        async fn get_batch(&self, _: &str) -> Result<Option<BatchRecord>, VtopError> {
            never!()
        }
        async fn claim_incomplete_batches(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Vec<BatchRecord>, VtopError> {
            never!()
        }
        async fn max_committed_end_bytes(
            &self,
            _: SourceType,
        ) -> Result<Vec<(String, u64)>, VtopError> {
            never!()
        }
        async fn committed_cursors(
            &self,
            _: SourceType,
        ) -> Result<Vec<CommittedCursor>, VtopError> {
            never!()
        }
        async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            never!()
        }
        async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            never!()
        }
        async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            never!()
        }
        async fn mark_failed(&self, _: &str, _: &str) -> Result<(), VtopError> {
            never!()
        }
        async fn mark_verified(&self, _: &str) -> Result<(), VtopError> {
            never!()
        }
        async fn mark_source_committed(&self, _: &str) -> Result<(), VtopError> {
            never!()
        }
        async fn prune_committed_history(&self, _: &str, _: u32) -> Result<u64, VtopError> {
            never!()
        }
        async fn prune_candidates(&self, _: &str, _: u32) -> Result<Vec<BatchRecord>, VtopError> {
            never!()
        }
        async fn prune_listed(&self, _: &str, _: &[String]) -> Result<u64, VtopError> {
            never!()
        }
    }

    fn expired(result: Result<impl std::fmt::Debug, VtopError>, operation: &str) {
        match result {
            Err(VtopError::State(message)) => assert!(
                message.contains(operation) && message.contains("did not complete within"),
                "the error must name the operation and the bound: {message}"
            ),
            other => panic!("{operation} did not expire: {other:?}"),
        }
    }

    /// Every method of a silent store comes back as an error instead of hanging
    /// the caller — a point operation after its own bound, a scan after ten
    /// times it. `start_paused` makes the clock advance by itself, so a hang
    /// would fail the test by never finishing rather than by being slow.
    #[tokio::test(start_paused = true)]
    async fn a_silent_store_cannot_hang_any_request() {
        let store = DeadlineStore::new(Box::new(Silent), Duration::from_secs(5));
        let rec = crate::test_battery::sample_record("deadline-1");
        expired(store.save_batch_state(&rec).await, "save_batch_state");
        expired(
            store
                .update_batch_state("b", BatchState::Sealed, &BatchPatch::default())
                .await,
            "update_batch_state",
        );
        expired(store.get_batch("b").await, "get_batch");
        expired(store.mark_failed("b", "x").await, "mark_failed");
        expired(store.mark_verified("b").await, "mark_verified");
        expired(
            store.mark_source_committed("b").await,
            "mark_source_committed",
        );
        // The scans.
        expired(
            store.claim_incomplete_batches("o", "n", "l").await,
            "claim_incomplete_batches",
        );
        expired(
            store.max_committed_end_bytes(SourceType::File).await,
            "max_committed_end_bytes",
        );
        expired(store.list_batches().await, "list_batches");
        expired(
            store.list_incomplete_batches().await,
            "list_incomplete_batches",
        );
        expired(store.list_failed_batches().await, "list_failed_batches");
        expired(
            store
                .prune_committed_history("2000-01-01T00:00:00Z", 1)
                .await,
            "prune_committed_history",
        );
    }

    /// A point operation is bounded by its own limit and a scan by ten times
    /// it: a scan that takes longer than a point read must not fail startup.
    #[tokio::test(start_paused = true)]
    async fn a_scan_gets_ten_times_the_single_row_deadline() {
        let store = DeadlineStore::new(Box::new(Silent), Duration::from_secs(5));
        let started = tokio::time::Instant::now();
        let _ = store.get_batch("b").await;
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        let started = tokio::time::Instant::now();
        let _ = store.list_batches().await;
        assert_eq!(started.elapsed(), Duration::from_secs(50));
    }

    /// The wrapper must be invisible to a store that answers.
    #[tokio::test]
    async fn a_responsive_store_behaves_exactly_as_it_did() {
        let inner = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        let store = DeadlineStore::new(Box::new(inner), DEFAULT_REQUEST_DEADLINE);
        crate::test_battery::run_all(&store).await;
    }
}
