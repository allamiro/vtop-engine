//! Backend-agnostic behavioural contract for any [`StateStore`].
//!
//! This is the single battery of assertions every backend must pass. SQLite runs
//! it today; the Postgres backend (Phase 3) runs the *same* functions, so the
//! two cannot diverge on the invariant or the CRUD semantics. Because the guard
//! that forbids committing before VERIFIED lives once in
//! [`vtop_core::state_machine`], the battery is really checking that a backend
//! routes its transitions through that guard rather than inventing its own.
//!
//! Available to in-crate tests (`cfg(test)`) and, for cross-crate use such as a
//! Postgres integration test, behind the `test-support` feature.
//!
//! [`run_all`] assumes it is handed a FRESH, empty store; each check uses
//! uniquely-prefixed `batch_id`s so they remain independent on a shared store.

use crate::{BatchPatch, BatchRecord, StateStore};
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// A minimal valid record in the initial `Batching` state, with a Kafka marker.
pub fn sample_record(id: &str) -> BatchRecord {
    let now = chrono::Utc::now().to_rfc3339();
    let marker = ProgressMarker::Kafka {
        topic: "app_events".into(),
        partition: 0,
        start_offset: 0,
        end_offset: 10,
        consumer_group: "vtop-engine".into(),
    };
    BatchRecord {
        batch_id: id.into(),
        tenant: "default".into(),
        source_type: SourceType::Kafka,
        source_name: "app_events".into(),
        format: TelemetryFormat::Cef,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        record_count: None,
        error_message: None,
        created_at: now.clone(),
        updated_at: now,
    }
}

/// The full legal path from first save to committed.
const LEGAL_WALK: [BatchState; 7] = [
    BatchState::Sealed,
    BatchState::Compressed,
    BatchState::Checksummed,
    BatchState::ObjectUploaded,
    BatchState::ManifestUploaded,
    BatchState::Verified,
    BatchState::SourceCommitted,
];

/// Run every contract check against a FRESH, empty store. Panics (via
/// `assert!`) on any violation, so call it from a `#[tokio::test]`.
pub async fn run_all(store: &dyn StateStore) {
    empty_store_is_empty(store).await;
    save_and_reload(store).await;
    duplicate_save_is_rejected(store).await;
    get_missing_returns_none(store).await;
    rejects_commit_before_verified(store).await;
    full_legal_walk_commits(store).await;
    mark_failed_from_any_state(store).await;
    lists_incomplete_and_failed(store).await;
}

async fn empty_store_is_empty(store: &dyn StateStore) {
    assert!(
        store.list_batches().await.unwrap().is_empty(),
        "a fresh store must start empty"
    );
}

async fn save_and_reload(store: &dyn StateStore) {
    store
        .save_batch_state(&sample_record("save-1"))
        .await
        .unwrap();
    let got = store.get_batch("save-1").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::Batching);
    assert_eq!(got.source_name, "app_events");
    assert_eq!(got.tenant, "default");
}

async fn duplicate_save_is_rejected(store: &dyn StateStore) {
    store
        .save_batch_state(&sample_record("dup-1"))
        .await
        .unwrap();
    // A second insert of the same batch_id must FAIL, never silently overwrite a
    // row that might already be committed.
    let err = store.save_batch_state(&sample_record("dup-1")).await;
    assert!(err.is_err(), "duplicate batch_id must be rejected");
}

async fn get_missing_returns_none(store: &dyn StateStore) {
    assert!(store.get_batch("does-not-exist").await.unwrap().is_none());
}

async fn rejects_commit_before_verified(store: &dyn StateStore) {
    store
        .save_batch_state(&sample_record("early-1"))
        .await
        .unwrap();
    let p = BatchPatch::default();
    // SOURCE_COMMITTED is reachable ONLY from VERIFIED, so commit must be refused
    // from EVERY state that precedes it - not just the initial one. A backend
    // that guards Batching but slips on, say, ManifestUploaded would still be a
    // data-loss bug. Walk through each pre-verified state and assert refusal at
    // each, confirming the state is left unchanged.
    let pre_verified = [
        BatchState::Batching, // the initial state (no advance needed)
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
    ];
    for st in pre_verified {
        if st != BatchState::Batching {
            store.update_batch_state("early-1", st, &p).await.unwrap();
        }
        let err = store.mark_source_committed("early-1").await.unwrap_err();
        assert!(
            matches!(err, VtopError::CommitBeforeVerified { .. }),
            "commit must be refused from {st:?}, got {err:?}"
        );
        let cur = store.get_batch("early-1").await.unwrap().unwrap().state;
        assert_eq!(cur, st, "a rejected commit must leave the state unchanged");
    }
    // From VERIFIED - and only from VERIFIED - the commit is finally allowed.
    store
        .update_batch_state("early-1", BatchState::Verified, &p)
        .await
        .unwrap();
    store.mark_source_committed("early-1").await.unwrap();
    assert_eq!(
        store.get_batch("early-1").await.unwrap().unwrap().state,
        BatchState::SourceCommitted
    );
}

async fn full_legal_walk_commits(store: &dyn StateStore) {
    store
        .save_batch_state(&sample_record("walk-1"))
        .await
        .unwrap();
    let p = BatchPatch::default();
    for st in LEGAL_WALK {
        store.update_batch_state("walk-1", st, &p).await.unwrap();
    }
    let got = store.get_batch("walk-1").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::SourceCommitted);
    assert!(!got.is_incomplete(), "a committed batch is not incomplete");
}

async fn mark_failed_from_any_state(store: &dyn StateStore) {
    // FAILED is legal from any non-terminal state.
    store
        .save_batch_state(&sample_record("fail-1"))
        .await
        .unwrap();
    store.mark_failed("fail-1", "boom").await.unwrap();
    let got = store.get_batch("fail-1").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::Failed);
    assert_eq!(got.error_message.as_deref(), Some("boom"));
}

async fn lists_incomplete_and_failed(store: &dyn StateStore) {
    store
        .save_batch_state(&sample_record("list-inc"))
        .await
        .unwrap();
    store
        .save_batch_state(&sample_record("list-fail"))
        .await
        .unwrap();
    store.mark_failed("list-fail", "nope").await.unwrap();

    let incomplete = store.list_incomplete_batches().await.unwrap();
    // Everything saved by earlier checks that has not reached SOURCE_COMMITTED is
    // incomplete; assert the two we just added are present rather than an exact
    // count, so the check is independent of the others on a shared store.
    let inc_ids: Vec<_> = incomplete.iter().map(|b| b.batch_id.as_str()).collect();
    assert!(inc_ids.contains(&"list-inc"));
    assert!(inc_ids.contains(&"list-fail"));
    assert!(
        !inc_ids.contains(&"walk-1"),
        "committed batch must not be incomplete"
    );

    let failed = store.list_failed_batches().await.unwrap();
    let fail_ids: Vec<_> = failed.iter().map(|b| b.batch_id.as_str()).collect();
    assert!(fail_ids.contains(&"list-fail"));
    assert!(fail_ids.contains(&"fail-1"));
}
