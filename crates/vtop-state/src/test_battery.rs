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

use crate::{
    BatchPatch, BatchRecord, CommittedCursor, CommittedKafkaOffset, StateStore, VerificationClass,
};
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
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: None,
        error_message: None,
        owner: None,
        lease_expires_at: None,
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
    insert_refuses_any_state_but_batching(store).await;
    get_missing_returns_none(store).await;
    rejects_commit_before_verified(store).await;
    full_legal_walk_commits(store).await;
    mark_failed_from_any_state(store).await;
    lists_incomplete_and_failed(store).await;
    max_committed_end_bytes_aggregates_in_store(store).await;
    committed_cursors_never_mix_the_offsets_of_two_files(store).await;
    committed_cursors_carry_the_newest_rows_content_fingerprint(store).await;
    committed_kafka_offsets_are_the_newest_row_of_each_partition_and_group(store).await;
    claims_respect_live_leases_and_take_over_expired_ones(store).await;
    committed_cursors_by_file_answer_each_file_that_lived_at_a_path(store).await;
    // Last: it claims every claimable row on the shared store.
    a_superseded_row_is_terminal_and_never_recovery_work(store).await;
}

/// #557: the claim fence. Two handles onto ONE ledger stand for two engines
/// (or one engine before and after a stall); each backend runs this with two
/// connections to the same database.
///
/// A lease is a lock with a timeout, not a fence: an engine that stalls past its
/// lease and then resumes used to write over a row another engine had claimed
/// and moved on, whenever the new owner happened to be in the state the stale
/// one expected. Every claim now bumps a generation on the row and a writer
/// that holds an older one is refused.
pub async fn run_fencing_battery(a: &dyn StateStore, b: &dyn StateStore) {
    const EXPIRED: &str = "2000-01-01T00:00:00+00:00";
    const NOW: &str = "2050-01-01T00:00:00+00:00";
    const LATER: &str = "2100-01-01T00:00:00+00:00";
    let refused = |result: Result<(), VtopError>, id: &str| match result {
        Err(e) => assert!(
            e.to_string().contains("claimed by another engine") && e.to_string().contains(id),
            "must be refused as a lost claim, naming the batch: {e}"
        ),
        Ok(()) => panic!("{id}: a write from an engine that lost its claim was accepted"),
    };
    let batch = |id: &str, owner: &str| {
        let mut rec = sample_record(id);
        rec.owner = Some(owner.into());
        rec.lease_expires_at = Some(EXPIRED.into());
        rec
    };
    let patch = BatchPatch::default();

    // 1. The stalled engine. A creates a batch and works on it; its lease
    //    lapses (it stalls); B claims the batch and takes it forward.
    a.save_batch_state(&batch("fence-stalled", "engine-a"))
        .await
        .unwrap();
    a.update_batch_state("fence-stalled", BatchState::Sealed, &patch)
        .await
        .unwrap();
    let claimed = b
        .claim_incomplete_batches("engine-b", NOW, LATER)
        .await
        .unwrap();
    assert!(claimed.iter().any(|r| r.batch_id == "fence-stalled"));
    // A resumes. Its next transition is legal for the state the row is in, and
    // the state guard alone would let it through; the claim generation does not.
    refused(
        a.update_batch_state("fence-stalled", BatchState::Compressed, &patch)
            .await,
        "fence-stalled",
    );
    refused(
        a.mark_failed("fence-stalled", "stale").await,
        "fence-stalled",
    );
    assert_eq!(
        b.get_batch("fence-stalled").await.unwrap().unwrap().state,
        BatchState::Sealed,
        "the stale write changed nothing"
    );
    // B, the owner, proceeds — including to the same transition A tried.
    b.update_batch_state("fence-stalled", BatchState::Compressed, &patch)
        .await
        .unwrap();

    // 2. Two engines that share an `owner` string are still fenced apart: the
    //    claim predicate accepts `owner = $1`, so a name match must not stand in
    //    for holding the claim.
    a.save_batch_state(&batch("fence-shared-name", "shared"))
        .await
        .unwrap();
    let claimed = b
        .claim_incomplete_batches("shared", NOW, LATER)
        .await
        .unwrap();
    assert!(claimed.iter().any(|r| r.batch_id == "fence-shared-name"));
    refused(
        a.update_batch_state("fence-shared-name", BatchState::Sealed, &patch)
            .await,
        "fence-shared-name",
    );
    // A claims it back (a restart, say): now B is the stale one.
    let claimed = a
        .claim_incomplete_batches("shared", NOW, LATER)
        .await
        .unwrap();
    assert!(claimed.iter().any(|r| r.batch_id == "fence-shared-name"));
    refused(
        b.update_batch_state("fence-shared-name", BatchState::Sealed, &patch)
            .await,
        "fence-shared-name",
    );
    a.update_batch_state("fence-shared-name", BatchState::Sealed, &patch)
        .await
        .unwrap();

    // 3. A process that holds no claim on a row - it neither created nor
    //    claimed it, or dropped the token - is refused, not let through
    //    unfenced: an unchecked write is exactly what a stalled writer whose
    //    token went missing would make (review of #569). The creator's own
    //    later writes are unaffected, because nothing moved the generation.
    a.save_batch_state(&batch("fence-tool", "engine-a"))
        .await
        .unwrap();
    match b
        .update_batch_state("fence-tool", BatchState::Sealed, &patch)
        .await
    {
        Err(e) => assert!(
            e.to_string().contains("not claimed by this engine")
                && e.to_string().contains("fence-tool"),
            "must be refused as not held, naming the batch: {e}"
        ),
        Ok(()) => panic!("fence-tool: a write from a process holding no claim was accepted"),
    }
    a.update_batch_state("fence-tool", BatchState::Sealed, &patch)
        .await
        .unwrap();

    // 3b. #624 (review): before the running engine moves a source for a row
    //     it has held a while, it renews its claim - fenced, in one write - so
    //     the row cannot be claimed away between the check and the move. A
    //     process that still holds the row extends its lease; one whose claim
    //     moved on is told so and changes nothing.
    a.save_batch_state(&batch("fence-renew", "engine-a"))
        .await
        .unwrap();
    a.update_batch_state("fence-renew", BatchState::Sealed, &patch)
        .await
        .unwrap();
    assert!(
        a.renew_claim("fence-renew", LATER).await.unwrap(),
        "the creator holds its row and renews the claim"
    );
    assert_eq!(
        a.get_batch("fence-renew")
            .await
            .unwrap()
            .unwrap()
            .lease_expires_at
            .as_deref()
            .map(|at| chrono::DateTime::parse_from_rfc3339(at).unwrap()),
        Some(chrono::DateTime::parse_from_rfc3339(LATER).unwrap()),
        "the renewal extends the lease"
    );
    // A renewed lease is live: B cannot claim the row before it runs out...
    let claimed = b
        .claim_incomplete_batches("engine-b", NOW, LATER)
        .await
        .unwrap();
    assert!(
        !claimed.iter().any(|r| r.batch_id == "fence-renew"),
        "a row whose claim was just renewed was claimed away"
    );
    // ...and once it has, the old holder's renewal is refused.
    let after = "2200-01-01T00:00:00+00:00";
    let claimed = b
        .claim_incomplete_batches("engine-b", after, "2300-01-01T00:00:00+00:00")
        .await
        .unwrap();
    assert!(claimed.iter().any(|r| r.batch_id == "fence-renew"));
    let before = b.get_batch("fence-renew").await.unwrap().unwrap();
    assert!(
        !a.renew_claim("fence-renew", "2400-01-01T00:00:00+00:00")
            .await
            .unwrap(),
        "a process whose claim moved on renewed it"
    );
    assert_eq!(
        b.get_batch("fence-renew")
            .await
            .unwrap()
            .unwrap()
            .lease_expires_at,
        before.lease_expires_at,
        "a refused renewal changed the new owner's lease"
    );
    assert!(
        b.renew_claim("fence-renew", "2400-01-01T00:00:00+00:00")
            .await
            .unwrap(),
        "the new owner holds it"
    );
    assert!(
        !a.renew_claim("no-such-batch", LATER).await.unwrap(),
        "nothing is held for a row that does not exist"
    );

    // 4. A committed batch is finished: no claim, no fence.
    a.save_batch_state(&batch("fence-done", "engine-a"))
        .await
        .unwrap();
    for st in LEGAL_WALK {
        a.update_batch_state("fence-done", st, &patch)
            .await
            .unwrap();
    }
    assert_eq!(
        a.get_batch("fence-done").await.unwrap().unwrap().state,
        BatchState::SourceCommitted
    );
}

/// #128 prune contract, separated from [`run_all`] because pruning DELETEs
/// rows: it is a MAINTENANCE-identity operation. The PostgreSQL battery runs
/// it with the migrator/owner connection; the runtime role is denied DELETE
/// by design and must keep failing it.
pub async fn run_prune_battery(store: &dyn StateStore) {
    prune_preserves_cursor_seeds_and_bounds_deletion(store).await;
    listed_prune_deletes_only_what_was_listed(store).await;
    superseded_rows_are_pruned_like_committed_history(store).await;
    prune_never_touches_the_audit_table(store).await;
}

/// The audit table (#694), as every backend must keep it: a row reads back
/// exactly as appended, each row chains to the one before it, and the whole
/// table's chain verifies. Unkeyed only, so a database that runs the battery
/// again (a reused PostgreSQL) chains to the earlier runs' rows.
pub async fn run_audit_battery(store: &dyn StateStore) {
    let run = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let mut appended = Vec::new();
    for (i, outcome) in ["attempt", "success", "refused"].iter().enumerate() {
        let audit_id = format!("battery-{run}-{i}");
        let record = format!(
            r#"{{"audit_version":1,"audit_id":"{audit_id}","time":"2026-11-14T00:00:0{i}.000Z","action":"ledger.prune","stage":"local","outcome":"{outcome}","actor":{{"kind":"os","uid":1000,"name":"ops","source":"os"}},"target":{{"kind":"ledger"}}}}"#
        );
        let row = crate::audit::AuditInsert::from_record(&record).unwrap();
        let stored = store.append_audit(&row, None).await.unwrap();
        assert_eq!(stored.audit_id, audit_id);
        assert_eq!(stored.actor.as_deref(), Some("ops"));
        assert_eq!(stored.chain, "blake3");
        if let Some(previous) = appended.last() {
            let previous: &crate::audit::AuditRow = previous;
            assert!(stored.seq > previous.seq, "seq orders the chain");
            assert_eq!(stored.prev_hash.as_deref(), Some(previous.hash.as_str()));
        }
        appended.push(stored);
    }
    // Read back exactly as appended.
    let first = appended[0].seq;
    let listed = store.list_audit(first - 1, 10).await.unwrap();
    assert_eq!(&listed[..3], &appended[..], "rows read back as appended");
    // A page boundary returns the rest in order.
    let page = store.list_audit(first, 1).await.unwrap();
    assert_eq!(page, vec![appended[1].clone()]);
    // The whole table's chain verifies, earlier runs' rows included.
    let mut all = Vec::new();
    let mut after = 0;
    loop {
        let page = store.list_audit(after, 500).await.unwrap();
        let Some(last) = page.last() else { break };
        after = last.seq;
        all.extend(page);
    }
    let report = crate::audit::verify_chain(&all, None);
    assert!(report.passed(), "{:?}", report.problems);
    assert_eq!(report.last_hash.as_deref(), Some(appended[2].hash.as_str()));
    // An audit_id is unique: a record is stored once.
    let again = crate::audit::AuditInsert::from_record(&appended[0].record).unwrap();
    store
        .append_audit(&again, None)
        .await
        .expect_err("an audit_id is stored once");
}

/// D11 of the #694 plan: `prune-ledger` deletes ledger rows and never an
/// audit row, however old.
async fn prune_never_touches_the_audit_table(store: &dyn StateStore) {
    let record = r#"{"audit_version":1,"audit_id":"prune-keeps-me","time":"2000-01-01T00:00:00.000Z","action":"ledger.prune","stage":"local","outcome":"success","actor":{"kind":"os","uid":0,"source":"os"},"target":{"kind":"ledger"}}"#;
    let row = crate::audit::AuditInsert::from_record(record).unwrap();
    // A reused database may hold it from an earlier run.
    let _ = store.append_audit(&row, None).await;
    let count = |rows: Vec<crate::audit::AuditRow>| rows.len();
    let before = count(store.list_audit(0, 100_000).await.unwrap());
    store
        .prune_committed_history("2100-01-01T00:00:00+00:00", 1000)
        .await
        .unwrap();
    let after = count(store.list_audit(0, 100_000).await.unwrap());
    assert_eq!(before, after, "prune-ledger must not delete audit rows");
    assert!(store
        .list_audit(0, 100_000)
        .await
        .unwrap()
        .iter()
        .any(|r| r.audit_id == "prune-keeps-me"));
}

/// #664: the state machine lets a row leave VERIFIED (FAILED, REPLAY_REQUIRED)
/// and come back through BATCHING to VERIFIED. The engine never takes that
/// path - a replay is a new batch with a new id - but the store allows it, so
/// it must not leave the first verification's class on the second: the
/// transition to VERIFIED writes the class it is given, `None` included.
async fn a_reverified_row_takes_the_class_of_its_new_verification(store: &dyn StateStore) {
    let id = "vclass-reverified";
    store.save_batch_state(&sample_record(id)).await.unwrap();
    let walk_to_verified = |class: Option<VerificationClass>| async move {
        for st in &LEGAL_WALK[..5] {
            store
                .update_batch_state(id, *st, &BatchPatch::default())
                .await
                .unwrap();
        }
        match class {
            Some(class) => store.mark_verified(id, class).await.unwrap(),
            None => store
                .update_batch_state(id, BatchState::Verified, &BatchPatch::default())
                .await
                .unwrap(),
        }
    };
    let back_to_batching = || async move {
        store.mark_failed(id, "re-check failed").await.unwrap();
        // Leaving VERIFIED keeps the class it was verified with.
        let failed = store.get_batch(id).await.unwrap().unwrap();
        assert_eq!(failed.state, BatchState::Failed);
        store
            .update_batch_state(id, BatchState::ReplayRequired, &BatchPatch::default())
            .await
            .unwrap();
        claim_again(store, id).await;
        store
            .update_batch_state(id, BatchState::Batching, &BatchPatch::default())
            .await
            .unwrap();
        failed.verification_class
    };
    walk_to_verified(Some(VerificationClass::SizeOnly)).await;
    assert_eq!(back_to_batching().await, Some(VerificationClass::SizeOnly));
    walk_to_verified(Some(VerificationClass::Strong)).await;
    let got = store.get_batch(id).await.unwrap().unwrap();
    assert_eq!(
        got.verification_class,
        Some(VerificationClass::Strong),
        "a second verification records its own class, not the first's"
    );
    assert_eq!(back_to_batching().await, Some(VerificationClass::Strong));
    walk_to_verified(None).await;
    let got = store.get_batch(id).await.unwrap().unwrap();
    assert_eq!(
        (got.state, got.verification_class),
        (BatchState::Verified, None),
        "a verification with no class (an older writer) leaves none, not the last one's"
    );
    store.mark_failed(id, "done").await.unwrap();
}

/// #664 with #567: a row that was VERIFIED, failed its re-check and was
/// handed back, then retired as SUPERSEDED, keeps the class it was verified
/// with - the record of what admitted it - and is never recovery work again.
async fn a_retired_row_keeps_its_class_and_is_never_recovery_work(store: &dyn StateStore) {
    let id = "vclass-superseded";
    store.save_batch_state(&sample_record(id)).await.unwrap();
    for st in &LEGAL_WALK[..5] {
        store
            .update_batch_state(id, *st, &BatchPatch::default())
            .await
            .unwrap();
    }
    store
        .mark_verified(id, VerificationClass::SizeOnly)
        .await
        .unwrap();
    store.mark_failed(id, "re-check failed").await.unwrap();
    store
        .update_batch_state(id, BatchState::ReplayRequired, &BatchPatch::default())
        .await
        .unwrap();
    claim_again(store, id).await;
    store
        .update_batch_state(id, BatchState::Superseded, &BatchPatch::default())
        .await
        .unwrap();
    let got = store.get_batch(id).await.unwrap().unwrap();
    assert_eq!(
        (got.state, got.verification_class),
        (BatchState::Superseded, Some(VerificationClass::SizeOnly))
    );
    let incomplete = store.list_incomplete_batches().await.unwrap();
    assert!(
        incomplete.iter().all(|row| row.batch_id != id),
        "a retired row is never recovery work: {incomplete:?}"
    );
}

/// Rows for [`every_read_path_returns_whole_rows`]: one VERIFIED with a class,
/// one FAILED. Call once, on a store no other battery claims from afterwards.
pub async fn seed_rows_for_every_read_path(store: &dyn StateStore) {
    store
        .save_batch_state(&sample_record("reads-verified"))
        .await
        .unwrap();
    for st in &LEGAL_WALK[..5] {
        store
            .update_batch_state("reads-verified", *st, &BatchPatch::default())
            .await
            .unwrap();
    }
    store
        .mark_verified("reads-verified", VerificationClass::SizeOnly)
        .await
        .unwrap();
    store
        .save_batch_state(&sample_record("reads-failed"))
        .await
        .unwrap();
    store
        .mark_failed("reads-failed", "upload failed")
        .await
        .unwrap();
}

/// Every read that returns whole rows returns the rows
/// [`seed_rows_for_every_read_path`] wrote, class included. Run it, have
/// ANOTHER connection add a column to `batches` (as `vtopctl migrate`, or a
/// newer release opening a SQLite file, does while this one runs), and run it
/// again on the same store (review of #664): a statement that reads `*` was
/// prepared against the old column list, and failed on the new one - on
/// Postgres "cached plan must not change result type" until the connection
/// was replaced, on SQLite a panic in the driver and an empty answer.
pub async fn every_read_path_returns_whole_rows(store: &dyn StateStore) {
    const NOW: &str = "2080-01-01T00:00:00+00:00";
    const LEASE: &str = "2080-01-01T00:05:00+00:00";
    let class = Some(VerificationClass::SizeOnly);
    let got = store.get_batch("reads-verified").await.unwrap().unwrap();
    assert_eq!(
        (got.state, got.verification_class),
        (BatchState::Verified, class)
    );
    let has = |rows: &[BatchRecord], id: &str| rows.iter().any(|row| row.batch_id == id);
    let all = store.list_batches().await.unwrap();
    assert!(
        has(&all, "reads-verified") && has(&all, "reads-failed"),
        "list_batches: {all:?}"
    );
    let incomplete = store.list_incomplete_batches().await.unwrap();
    assert!(has(&incomplete, "reads-verified"), "{incomplete:?}");
    let failed = store.list_failed_batches().await.unwrap();
    assert!(has(&failed, "reads-failed"), "{failed:?}");
    let claimed = store
        .claim_incomplete_batches("engine-reads", NOW, LEASE)
        .await
        .unwrap();
    let row = claimed
        .iter()
        .find(|row| row.batch_id == "reads-verified")
        .unwrap_or_else(|| panic!("the claim did not return reads-verified: {claimed:?}"));
    assert_eq!(row.verification_class, class);
    // Nothing is old enough to prune; the statement must still run.
    store
        .prune_candidates("2000-01-01T00:00:00+00:00", 10)
        .await
        .unwrap();
}

/// The verified rows [`verification_class_before_restart`] leaves, and the
/// class each was verified with: `None` is a legacy row, VERIFIED through
/// `update_batch_state` with no class, as every release before #664 wrote it.
const CLASS_ROWS: [(&str, Option<VerificationClass>); 3] = [
    ("vclass-size-only", Some(VerificationClass::SizeOnly)),
    ("vclass-strong", Some(VerificationClass::Strong)),
    ("vclass-legacy", None),
];

/// #664, the half before a restart: the class a batch was verified with is
/// written with its VERIFIED transition, and only with that transition.
///
/// Leaves the [`CLASS_ROWS`] VERIFIED and ownerless, for
/// [`verification_class_after_restart`] to recover through another handle on
/// the same ledger - a reopened SQLite file, a new Postgres connection -
/// standing for the engine after a restart. Run the two on a store no other
/// battery claims from afterwards: the second claims every claimable row.
pub async fn verification_class_before_restart(store: &dyn StateStore) {
    let to_manifest_uploaded = &LEGAL_WALK[..5];
    for (id, class) in CLASS_ROWS {
        store.save_batch_state(&sample_record(id)).await.unwrap();
        for st in to_manifest_uploaded {
            // Refused on every transition before VERIFIED, and the row is
            // left as it was: the class describes the verification that
            // admitted the row, and nothing else.
            let refused = store
                .update_batch_state(
                    id,
                    *st,
                    &BatchPatch {
                        verification_class: Some(VerificationClass::SizeOnly),
                        ..Default::default()
                    },
                )
                .await;
            assert!(
                refused.is_err(),
                "{id}: a verification class written with the transition to {st} was accepted"
            );
            store
                .update_batch_state(id, *st, &BatchPatch::default())
                .await
                .unwrap();
        }
        match class {
            Some(class) => store.mark_verified(id, class).await.unwrap(),
            None => store
                .update_batch_state(id, BatchState::Verified, &BatchPatch::default())
                .await
                .unwrap(),
        }
        let got = store.get_batch(id).await.unwrap().unwrap();
        assert_eq!(got.state, BatchState::Verified, "{id}");
        assert_eq!(
            got.verification_class, class,
            "{id}: the class read back is not the class it was verified with"
        );
    }
    // Nor on the commit: a committed row keeps the class it was verified
    // with, and a commit cannot rewrite it.
    store
        .save_batch_state(&sample_record("vclass-commit"))
        .await
        .unwrap();
    for st in to_manifest_uploaded {
        store
            .update_batch_state("vclass-commit", *st, &BatchPatch::default())
            .await
            .unwrap();
    }
    store
        .mark_verified("vclass-commit", VerificationClass::Strong)
        .await
        .unwrap();
    let refused = store
        .update_batch_state(
            "vclass-commit",
            BatchState::SourceCommitted,
            &BatchPatch {
                verification_class: Some(VerificationClass::SizeOnly),
                ..Default::default()
            },
        )
        .await;
    assert!(
        refused.is_err(),
        "a verification class written with the commit was accepted"
    );
    store.mark_source_committed("vclass-commit").await.unwrap();
    let got = store.get_batch("vclass-commit").await.unwrap().unwrap();
    assert_eq!(
        (got.state, got.verification_class),
        (BatchState::SourceCommitted, Some(VerificationClass::Strong)),
        "a committed row keeps the class it was verified with"
    );
}

/// #664, the half after a restart: recovery's claim returns each VERIFIED row
/// with the class it was verified with before the restart (`None` for a
/// legacy row), and committing it keeps the class.
pub async fn verification_class_after_restart(store: &dyn StateStore) {
    const NOW: &str = "2070-01-01T00:00:00+00:00";
    const LEASE: &str = "2070-01-01T00:05:00+00:00";
    let claimed = store
        .claim_incomplete_batches("engine-restarted", NOW, LEASE)
        .await
        .unwrap();
    for (id, class) in CLASS_ROWS {
        let row = claimed
            .iter()
            .find(|row| row.batch_id == id)
            .unwrap_or_else(|| panic!("{id}: recovery did not claim the VERIFIED row"));
        assert_eq!(row.state, BatchState::Verified, "{id}");
        assert_eq!(
            row.verification_class, class,
            "{id}: recovery read a class other than the one the row was verified with"
        );
        store.mark_source_committed(id).await.unwrap();
        let got = store.get_batch(id).await.unwrap().unwrap();
        assert_eq!(
            (got.state, got.verification_class),
            (BatchState::SourceCommitted, class),
            "{id}: the commit after recovery changed the class"
        );
    }
    // Both claim the rows they hand back, as recovery does.
    a_reverified_row_takes_the_class_of_its_new_verification(store).await;
    a_retired_row_keeps_its_class_and_is_never_recovery_work(store).await;
}

/// Claims `id` again after REPLAY_REQUIRED released this handle's claim on
/// it, as recovery does.
async fn claim_again(store: &dyn StateStore, id: &str) {
    let claimed = store
        .claim_incomplete_batches(
            "engine-restarted",
            "2070-01-01T00:00:00+00:00",
            "2070-01-01T00:05:00+00:00",
        )
        .await
        .unwrap();
    assert!(
        claimed.iter().any(|row| row.batch_id == id),
        "{id}: {claimed:?}"
    );
}

/// #567: a FAILED / REPLAY_REQUIRED row whose range its source has since
/// committed past is retired as SUPERSEDED - once. Before the state existed
/// such a row never reached a terminal state: every restart claimed it again,
/// wrote FAILED and REPLAY_REQUIRED over it and re-planned its rewind.
async fn a_superseded_row_is_terminal_and_never_recovery_work(store: &dyn StateStore) {
    const NOW: &str = "2060-01-01T00:00:00+00:00";
    const LEASE: &str = "2060-01-01T00:05:00+00:00";
    const AFTER_LEASE: &str = "2061-01-01T00:00:00+00:00";
    let p = BatchPatch::default();

    // A stale REPLAY_REQUIRED row, as an engine leaves one: writing
    // REPLAY_REQUIRED releases its claim, so recovery claims it again first.
    store
        .save_batch_state(&sample_record("sup-replay"))
        .await
        .unwrap();
    store
        .mark_failed("sup-replay", "upload failed")
        .await
        .unwrap();
    store
        .update_batch_state("sup-replay", BatchState::ReplayRequired, &p)
        .await
        .unwrap();
    // And a FAILED row, retired straight from FAILED.
    store
        .save_batch_state(&sample_record("sup-failed"))
        .await
        .unwrap();
    store
        .mark_failed("sup-failed", "upload failed")
        .await
        .unwrap();
    // Neither in-flight nor VERIFIED work may be retired: only a range that
    // was already handed back.
    store
        .save_batch_state(&sample_record("sup-inflight"))
        .await
        .unwrap();
    let refused = store
        .update_batch_state("sup-inflight", BatchState::Superseded, &p)
        .await;
    assert!(
        matches!(
            refused,
            Err(VtopError::IllegalStateTransition {
                from: BatchState::Batching,
                to: BatchState::Superseded,
            })
        ),
        "BATCHING -> SUPERSEDED must be refused, got {refused:?}"
    );
    // A VERIFIED row holds an archived object whose commit is still to be
    // decided: it is never retired directly (review).
    store
        .save_batch_state(&sample_record("sup-verified"))
        .await
        .unwrap();
    for st in &LEGAL_WALK[..6] {
        store
            .update_batch_state("sup-verified", *st, &p)
            .await
            .unwrap();
    }
    let refused = store
        .update_batch_state("sup-verified", BatchState::Superseded, &p)
        .await;
    assert!(
        matches!(
            refused,
            Err(VtopError::IllegalStateTransition {
                from: BatchState::Verified,
                to: BatchState::Superseded,
            })
        ),
        "VERIFIED -> SUPERSEDED must be refused, got {refused:?}"
    );

    let claimed = store
        .claim_incomplete_batches("sup-engine", NOW, LEASE)
        .await
        .unwrap();
    let ids: Vec<&str> = claimed.iter().map(|r| r.batch_id.as_str()).collect();
    // Both handed-back states are recovery work until retired (review: the
    // FAILED row's own token would otherwise hide a backend that skipped it).
    assert!(ids.contains(&"sup-replay"), "{ids:?}");
    assert!(ids.contains(&"sup-failed"), "{ids:?}");
    for id in ["sup-replay", "sup-failed"] {
        store
            .update_batch_state(id, BatchState::Superseded, &p)
            .await
            .unwrap_or_else(|e| panic!("{id}: retiring a handed-back range: {e}"));
        let row = store.get_batch(id).await.unwrap().unwrap();
        assert_eq!(row.state, BatchState::Superseded, "{id}");
        assert!(
            !row.is_incomplete(),
            "{id}: a superseded row is not incomplete"
        );
    }

    // Never recovery work again: not listed, and not claimed - by this engine,
    // or by another once every lease has run out.
    let listed: Vec<String> = store
        .list_incomplete_batches()
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.batch_id)
        .collect();
    for (owner, at) in [("sup-engine", NOW), ("sup-other", AFTER_LEASE)] {
        let reclaimed: Vec<String> = store
            .claim_incomplete_batches(owner, at, AFTER_LEASE)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.batch_id)
            .collect();
        for id in ["sup-replay", "sup-failed"] {
            assert!(
                !listed.contains(&id.to_owned()),
                "{id} is listed as incomplete: {listed:?}"
            );
            assert!(
                !reclaimed.contains(&id.to_owned()),
                "{id} was claimed again by {owner}: {reclaimed:?}"
            );
        }
    }

    // Terminal: nothing moves it, and it is no way into SOURCE_COMMITTED.
    let before = store.get_batch("sup-replay").await.unwrap().unwrap();
    for to in [
        BatchState::Failed,
        BatchState::ReplayRequired,
        BatchState::Batching,
        BatchState::Verified,
    ] {
        let result = store.update_batch_state("sup-replay", to, &p).await;
        assert!(
            matches!(
                result,
                Err(VtopError::IllegalStateTransition {
                    from: BatchState::Superseded,
                    ..
                })
            ),
            "SUPERSEDED -> {to:?} must be refused, got {result:?}"
        );
    }
    let result = store.mark_source_committed("sup-replay").await;
    assert!(
        matches!(
            result,
            Err(VtopError::CommitBeforeVerified {
                actual: BatchState::Superseded
            })
        ),
        "a superseded row must not commit, got {result:?}"
    );
    let after = store.get_batch("sup-replay").await.unwrap().unwrap();
    assert_eq!(after.state, BatchState::Superseded);
    assert_eq!(
        after.updated_at, before.updated_at,
        "a refused write changed the row"
    );
}

/// #567: retention deletes SUPERSEDED rows as it deletes committed history,
/// and lists them for the digest first - including one newer than every
/// committed row of its source, which a committed row would never be (the
/// newest committed row is the restart cursor). A superseded row is never a
/// cursor, so pruning it moves none.
async fn superseded_rows_are_pruned_like_committed_history(store: &dyn StateStore) {
    let marker = |start: u64, end: u64| ProgressMarker::File {
        path: "/logs/superseded".into(),
        inode: Some(7),
        start_byte: start,
        end_byte: end,
        file_size: end,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let row = |id: &str, start: u64, end: u64, created: &str| {
        let mut rec = sample_record(id);
        rec.source_type = SourceType::File;
        rec.source_name = "superseded-src".into();
        rec.progress_start = marker(start, end);
        rec.progress_end = marker(start, end);
        rec.created_at = created.into();
        rec
    };
    let p = BatchPatch::default();
    // The commit that covers the stale ranges: the only committed row of its
    // path, so it is the cursor and never a prune candidate.
    store
        .save_batch_state(&row("sp-commit", 0, 30, "2030-01-01T00:00:02Z"))
        .await
        .unwrap();
    for st in LEGAL_WALK {
        store.update_batch_state("sp-commit", st, &p).await.unwrap();
    }
    // Two retired ranges: one read before the commit, and one created after
    // it (the newest row of the path).
    for (id, start, end, created) in [
        ("sp-old", 0, 10, "2030-01-01T00:00:01Z"),
        ("sp-new", 10, 20, "2030-01-01T00:00:03Z"),
    ] {
        store
            .save_batch_state(&row(id, start, end, created))
            .await
            .unwrap();
        store.mark_failed(id, "upload failed").await.unwrap();
        store
            .update_batch_state(id, BatchState::Superseded, &p)
            .await
            .unwrap();
    }
    let cursors_before = store.committed_cursors(SourceType::File).await.unwrap();

    let future = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
    let past = "1970-01-01T00:00:00+00:00";
    let ours = |rows: Vec<BatchRecord>| -> Vec<String> {
        let mut ids: Vec<String> = rows
            .into_iter()
            .map(|r| r.batch_id)
            .filter(|id| id.starts_with("sp-"))
            .collect();
        ids.sort();
        ids
    };
    // Listed for the digest, and the age cutoff still protects them.
    assert_eq!(
        ours(store.prune_candidates(&future, 10_000).await.unwrap()),
        ["sp-new", "sp-old"],
        "both superseded rows are prune candidates, the committed cursor row is not"
    );
    assert!(ours(store.prune_candidates(past, 10_000).await.unwrap()).is_empty());
    assert_eq!(
        store
            .prune_listed(past, &["sp-old".into(), "sp-new".into()])
            .await
            .unwrap(),
        0,
        "rows younger than the cutoff are not deleted"
    );
    // Deleted by the listed prune, as the engine's retention pass does...
    assert_eq!(
        store
            .prune_listed(&future, &["sp-old".into(), "sp-commit".into()])
            .await
            .unwrap(),
        1,
        "only the superseded row goes; the cursor row stays though named"
    );
    assert!(store.get_batch("sp-old").await.unwrap().is_none());
    // ...and by the unlisted one.
    while store.prune_committed_history(&future, 100).await.unwrap() > 0 {}
    assert!(store.get_batch("sp-new").await.unwrap().is_none());
    assert_eq!(
        store.get_batch("sp-commit").await.unwrap().map(|r| r.state),
        Some(BatchState::SourceCommitted),
        "the cursor row survives every prune"
    );
    assert_eq!(
        store.committed_cursors(SourceType::File).await.unwrap(),
        cursors_before,
        "pruning superseded rows moves no restart cursor"
    );
}

/// #614: the Kafka adapter resumes each partition from the ledger, not from
/// the group's committed offset, which an engine whose claim has moved on can
/// still overwrite. The answer is per (topic, partition, consumer group), and
/// it is the NEWEST committed row's end offset: not a row of another
/// partition or group, not a row that is only VERIFIED, and not the highest
/// offset - a recreated topic numbers its records from zero again.
async fn committed_kafka_offsets_are_the_newest_row_of_each_partition_and_group(
    store: &dyn StateStore,
) {
    let topic = "kafka_resume_614";
    let row = |id: &str, partition: i32, group: &str, end: i64, created: &str| {
        let marker = ProgressMarker::Kafka {
            topic: topic.into(),
            partition,
            start_offset: 0,
            end_offset: end,
            consumer_group: group.into(),
        };
        let mut r = sample_record(id);
        r.source_name = topic.into();
        r.progress_start = marker.clone();
        r.progress_end = marker;
        r.created_at = created.into();
        r
    };
    let committed = [
        row("ko-p0-a", 0, "g", 9, "2030-01-01T00:00:01Z"),
        row("ko-p0-b", 0, "g", 19, "2030-01-01T00:00:02Z"),
        row("ko-p1", 1, "g", 4, "2030-01-01T00:00:03Z"),
        row("ko-p0-old-group", 0, "g-old", 99, "2030-01-01T00:00:04Z"),
        // Partition 2's topic was recreated: the newer row ends LOWER.
        row("ko-p2-old-topic", 2, "g", 500, "2030-01-01T00:00:05Z"),
        row("ko-p2-new-topic", 2, "g", 7, "2030-01-01T00:00:06Z"),
    ];
    for rec in &committed {
        store.save_batch_state(rec).await.unwrap();
        for st in LEGAL_WALK {
            store
                .update_batch_state(&rec.batch_id, st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }
    // Newer and higher, but only VERIFIED: never a resume point.
    let verified = row("ko-p0-verified", 0, "g", 29, "2030-01-01T00:00:09Z");
    store.save_batch_state(&verified).await.unwrap();
    for st in &LEGAL_WALK[..6] {
        store
            .update_batch_state("ko-p0-verified", *st, &BatchPatch::default())
            .await
            .unwrap();
    }

    let mut got: Vec<CommittedKafkaOffset> = store
        .committed_kafka_offsets()
        .await
        .unwrap()
        .into_iter()
        .filter(|o| o.topic == topic)
        .collect();
    got.sort_by(|a, b| (a.partition, &a.consumer_group).cmp(&(b.partition, &b.consumer_group)));
    let want = |partition: i32, group: &str, end: i64| CommittedKafkaOffset {
        topic: topic.into(),
        partition,
        consumer_group: group.into(),
        end_offset: end,
    };
    assert_eq!(
        got,
        vec![
            want(0, "g", 19),
            want(0, "g-old", 99),
            want(1, "g", 4),
            want(2, "g", 7),
        ],
        "one resume point per (topic, partition, consumer group): the newest committed row's"
    );
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

/// #625: the insert is the one write that does not go through the state
/// machine, so a row born in any state but BATCHING skips every check behind
/// it - born VERIFIED, it can be committed without ever having been verified.
async fn insert_refuses_any_state_but_batching(store: &dyn StateStore) {
    let not_initial = [
        BatchState::Discovered,
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
        BatchState::SourceCommitted,
        BatchState::Failed,
        BatchState::ReplayRequired,
        BatchState::Superseded,
    ];
    for st in not_initial {
        let id = format!("born-{}", st.as_str());
        let mut rec = sample_record(&id);
        rec.state = st;
        let result = store.save_batch_state(&rec).await;
        assert!(
            matches!(
                result,
                Err(VtopError::InvalidStateForOperation {
                    expected: BatchState::Batching,
                    actual,
                }) if actual == st
            ),
            "an insert as {st:?} must be refused as not BATCHING, got {result:?}"
        );
        assert!(
            store.get_batch(&id).await.unwrap().is_none(),
            "a refused insert as {st:?} must leave no row"
        );
    }
    store
        .save_batch_state(&sample_record("born-batching"))
        .await
        .expect("an insert as BATCHING must be accepted");
    assert_eq!(
        store
            .get_batch("born-batching")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::Batching
    );
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

/// #77: recovery seeds byte-offset cursors from a store-side aggregate, so
/// the aggregate must return the per-path MAX over COMMITTED rows only —
/// uncommitted progress must never advance a cursor.
async fn max_committed_end_bytes_aggregates_in_store(store: &dyn StateStore) {
    let file_marker = |path: &str, end: u64| ProgressMarker::File {
        path: path.into(),
        inode: None,
        start_byte: 0,
        end_byte: end,
        file_size: end,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let mk = |id: &str, path: &str, end: u64| {
        let mut r = sample_record(id);
        r.source_type = SourceType::File;
        r.source_name = path.to_string();
        r.progress_start = file_marker(path, 0);
        r.progress_end = file_marker(path, end);
        r
    };
    // Two committed generations for /a.log (100 then 250), one for /b.log,
    // and an UNCOMMITTED 999 for /a.log that must not win.
    for (id, path, end, commit) in [
        ("agg-1", "/a.log", 100, true),
        ("agg-2", "/a.log", 250, true),
        ("agg-3", "/b.log", 40, true),
        ("agg-4", "/a.log", 999, false),
    ] {
        store.save_batch_state(&mk(id, path, end)).await.unwrap();
        if commit {
            let p = BatchPatch::default();
            for st in LEGAL_WALK {
                store.update_batch_state(id, st, &p).await.unwrap();
            }
        }
    }

    let mut got = store
        .max_committed_end_bytes(SourceType::File)
        .await
        .unwrap();
    got.sort();
    got.retain(|(p, _)| p == "/a.log" || p == "/b.log");
    assert_eq!(
        got,
        vec![("/a.log".to_string(), 250), ("/b.log".to_string(), 40)],
        "per-path MAX over committed rows only (uncommitted 999 must not win)"
    );
    // Kafka rows exist in the store (from earlier checks) but are a different
    // source_type: they must not appear in a File aggregate.
    assert!(
        store
            .max_committed_end_bytes(SourceType::Kafka)
            .await
            .unwrap()
            .iter()
            .all(|(p, _)| !p.is_empty()),
        "kafka markers have no path field; the aggregate must not fabricate one"
    );
}

/// #578: the restart cursor carries the content fingerprint (and, for a
/// spool, the length) the NEWEST committed row recorded - read out of the
/// stored JSON by both backends - and a newest row from before the field
/// answers "unknown", never an older row's fingerprint. #620: the same for
/// the fingerprint of the bytes below the end.
async fn committed_cursors_carry_the_newest_rows_content_fingerprint(store: &dyn StateStore) {
    use vtop_core::types::ContentFingerprint;
    let old = ContentFingerprint::of(b"old first line\n");
    let new = ContentFingerprint::of(&[b'x'; 5000]);
    let tail = |end: u64| ContentFingerprint::of_tail(format!("bytes below {end}\n").as_bytes());
    let file = |end: u64, content: Option<ContentFingerprint>| ProgressMarker::File {
        path: "/fp.log".into(),
        inode: Some(9),
        start_byte: 0,
        end_byte: end,
        file_size: end,
        // Carried to the seed for the deferred delete (#619): the newest's.
        mtime: format!("2026-01-01T00:00:00.{end:09}+00:00"),
        // A row that records a first page records the bytes below its end
        // too, as the adapter writes them.
        tail_fingerprint: content.as_ref().and(tail(end)),
        content_fingerprint: content,
    };
    let spool = |end: u64, content: Option<ContentFingerprint>| ProgressMarker::SyslogSpool {
        spool_id: "fp".into(),
        path: "/fp-spool.log".into(),
        inode: Some(10),
        start_byte: 0,
        end_byte: end,
        received_time_start: None,
        received_time_end: None,
        file_size: Some(end + 1),
        content_fingerprint: content,
    };
    let p = BatchPatch::default();
    for (id, source_type, marker) in [
        // A legacy row, then two fingerprinted ones: the newest's wins.
        ("fp-1", SourceType::File, file(10, None)),
        ("fp-2", SourceType::File, file(20, old.clone())),
        ("fp-3", SourceType::File, file(5000, new.clone())),
        ("fp-s1", SourceType::SyslogSpool, spool(30, new.clone())),
        // The newest spool row is from before the field: unknown, not the
        // older row's fingerprint.
        ("fp-s2", SourceType::SyslogSpool, {
            let mut legacy = spool(40, None);
            if let ProgressMarker::SyslogSpool { file_size, .. } = &mut legacy {
                *file_size = None;
            }
            legacy
        }),
    ] {
        let mut r = sample_record(id);
        r.source_type = source_type;
        r.progress_start = marker.clone();
        r.progress_end = marker;
        store.save_batch_state(&r).await.unwrap();
        for st in LEGAL_WALK {
            store.update_batch_state(id, st, &p).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let files = store.committed_cursors(SourceType::File).await.unwrap();
    assert_eq!(
        files.iter().find(|c| c.path == "/fp.log"),
        Some(&CommittedCursor {
            path: "/fp.log".into(),
            inode: Some(9),
            end_byte: 5000,
            observed_size: Some(5000),
            content_fingerprint: new.clone(),
            observed_mtime: Some("2026-01-01T00:00:00.000005000+00:00".into()),
            tail_fingerprint: tail(5000),
        })
    );
    let spools = store
        .committed_cursors(SourceType::SyslogSpool)
        .await
        .unwrap();
    assert_eq!(
        spools.iter().find(|c| c.path == "/fp-spool.log"),
        Some(&CommittedCursor {
            path: "/fp-spool.log".into(),
            inode: Some(10),
            end_byte: 40,
            observed_size: None,
            content_fingerprint: None,
            observed_mtime: None,
            tail_fingerprint: None,
        }),
        "the newest row predates the fields: unknown, not an older row's values"
    );

    // And a spool row that records them hands both to the seed.
    let mut r = sample_record("fp-s3");
    r.source_type = SourceType::SyslogSpool;
    r.progress_start = spool(50, old.clone());
    r.progress_end = spool(50, old.clone());
    store.save_batch_state(&r).await.unwrap();
    for st in LEGAL_WALK {
        store.update_batch_state("fp-s3", st, &p).await.unwrap();
    }
    let spools = store
        .committed_cursors(SourceType::SyslogSpool)
        .await
        .unwrap();
    let got = spools.iter().find(|c| c.path == "/fp-spool.log").unwrap();
    assert_eq!(
        (got.observed_size, got.content_fingerprint.clone()),
        (Some(51), old)
    );

    // #620: a newest file row from before the tail field answers "unknown"
    // for it, never the older row's tail.
    let mut r = sample_record("fp-4");
    r.source_type = SourceType::File;
    r.progress_start = file(6000, new.clone());
    r.progress_end = {
        let mut legacy = file(6000, new.clone());
        if let ProgressMarker::File {
            tail_fingerprint, ..
        } = &mut legacy
        {
            *tail_fingerprint = None;
        }
        legacy
    };
    store.save_batch_state(&r).await.unwrap();
    for st in LEGAL_WALK {
        store.update_batch_state("fp-4", st, &p).await.unwrap();
    }
    let files = store.committed_cursors(SourceType::File).await.unwrap();
    let got = files.iter().find(|c| c.path == "/fp.log").unwrap();
    assert_eq!(
        (
            got.end_byte,
            got.content_fingerprint.clone(),
            got.tail_fingerprint.clone()
        ),
        (6000, new, None)
    );
}

/// #621: the restart seed must find the row of the file now at a path, and a
/// range re-read from a rotated file must start above what a later batch
/// already archived of THAT file; the per-path cursor can say neither once
/// another file has taken the path. `committed_cursors_by_file` answers with
/// the newest committed batch of each (path, inode) - the newest, not the
/// furthest, exactly as the per-path cursor (a `copytruncate` keeps the inode)
/// - each path's rows together and newest first, nothing uncommitted.
async fn committed_cursors_by_file_answer_each_file_that_lived_at_a_path(store: &dyn StateStore) {
    let marker = |path: &str, inode: Option<u64>, end: u64| ProgressMarker::File {
        path: path.into(),
        inode,
        start_byte: 0,
        end_byte: end,
        file_size: end * 10,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let p = BatchPatch::default();
    for (id, path, inode, end, commit) in [
        ("at-1", "/at.log", Some(11), 400, true),
        ("at-2", "/at.log", Some(11), 900, true),
        ("at-3", "/at.log", Some(22), 20, true),
        ("at-4", "/at.log", Some(22), 60, true),
        ("at-5", "/at.log", Some(22), 5_000, false),
        ("at-6", "/at.log", Some(33), 70, true),
        ("at-7", "/at.log", Some(33), 30, true),
        ("at-8", "/at.log", None, 15, true),
        ("at-8b", "/at.log", None, 25, true),
        ("at-9", "/elsewhere.log", Some(11), 7, true),
    ] {
        let mut r = sample_record(id);
        r.source_type = SourceType::File;
        r.source_name = path.to_string();
        r.progress_start = marker(path, inode, 0);
        r.progress_end = marker(path, inode, end);
        store.save_batch_state(&r).await.unwrap();
        if commit {
            for st in LEGAL_WALK {
                store.update_batch_state(id, st, &p).await.unwrap();
            }
        }
        // Created strictly in this order: "newest" is by creation.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // Spool rows at the SAME path: answered for SyslogSpool alone, per inode,
    // and never mixed into the File answer.
    let spool_marker = |inode: u64, end: u64| ProgressMarker::SyslogSpool {
        spool_id: "s".into(),
        path: "/at.log".into(),
        inode: Some(inode),
        start_byte: 0,
        end_byte: end,
        received_time_start: None,
        received_time_end: None,
        file_size: Some(end * 10),
        content_fingerprint: None,
    };
    for (id, inode, end, commit) in [
        ("at-s1", 41, 100, true),
        ("at-s2", 41, 50, true),
        ("at-s3", 42, 70, true),
        ("at-s4", 42, 9_000, false),
    ] {
        let mut r = sample_record(id);
        r.source_type = SourceType::SyslogSpool;
        r.source_name = "/at.log".to_string();
        r.progress_start = spool_marker(inode, 0);
        r.progress_end = spool_marker(inode, end);
        store.save_batch_state(&r).await.unwrap();
        if commit {
            for st in LEGAL_WALK {
                store.update_batch_state(id, st, &p).await.unwrap();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // A SUPERSEDED row (#567) is terminal but never a committed cursor: its
    // range was handed back, not archived by it. Newest of all, at a new
    // inode and the furthest end, it must not appear.
    let mut superseded = sample_record("at-10");
    superseded.source_type = SourceType::File;
    superseded.source_name = "/at.log".into();
    superseded.progress_start = marker("/at.log", Some(44), 0);
    superseded.progress_end = marker("/at.log", Some(44), 99_999);
    store.save_batch_state(&superseded).await.unwrap();
    store.mark_failed("at-10", "handed back").await.unwrap();
    store
        .update_batch_state("at-10", BatchState::Superseded, &p)
        .await
        .unwrap();
    let all = store
        .committed_cursors_by_file(SourceType::File)
        .await
        .unwrap();
    // Each path's rows form ONE run, so a caller can take a path's files
    // together (the store is shared with other checks, whose rows are here
    // too).
    let mut runs: Vec<&str> = Vec::new();
    for c in &all {
        if runs.last() != Some(&c.path.as_str()) {
            assert!(
                !runs.contains(&c.path.as_str()),
                "{} appears in two runs: {all:?}",
                c.path
            );
            runs.push(&c.path);
        }
    }
    let got: Vec<_> = all
        .into_iter()
        .filter(|c| c.path == "/at.log" || c.path == "/elsewhere.log")
        .collect();
    let want = |path: &str, inode: Option<u64>, end_byte: u64| CommittedCursor {
        path: path.into(),
        inode,
        end_byte,
        observed_size: Some(end_byte * 10),
        content_fingerprint: None,
        // These markers record no mtime: the empty string reads as unknown.
        observed_mtime: None,
        tail_fingerprint: None,
    };
    assert_eq!(
        got,
        vec![
            // Newest first within the path, so the first is the one
            // `committed_cursors` answers for it. Rows without an inode cannot
            // be told apart by file: they are answered together, as the newest
            // of them (see the trait doc).
            want("/at.log", None, 25),
            // The newest row, not the furthest: a copytruncate keeps the inode.
            want("/at.log", Some(33), 30),
            // The uncommitted 5000 does not count.
            want("/at.log", Some(22), 60),
            want("/at.log", Some(11), 900),
            // Another path's file with the same inode is its own file.
            want("/elsewhere.log", Some(11), 7),
        ]
    );
    let spool: Vec<_> = store
        .committed_cursors_by_file(SourceType::SyslogSpool)
        .await
        .unwrap()
        .into_iter()
        .filter(|c| c.path == "/at.log")
        .collect();
    assert_eq!(
        spool,
        vec![
            // Newest, not furthest; and the uncommitted 9000 does not count.
            want("/at.log", Some(42), 70),
            want("/at.log", Some(41), 50),
        ]
    );
}

/// #558: a restart seeds each file's cursor from the ledger. The ledger's
/// per-path MAX(end_byte) mixes every file that has ever lived at the path, so
/// after a rotation it handed the NEW file the OLD file's high-water mark.
/// `committed_cursors` answers with the newest committed batch's inode, end
/// byte and observed length - the newest by the instant it was created.
async fn committed_cursors_never_mix_the_offsets_of_two_files(store: &dyn StateStore) {
    // `file_size` is deliberately NOT the end byte: a bounded read leaves the
    // cursor behind the file's end, and the size the batch observed is what a
    // restart compares a shrunken file against.
    let marker = |path: &str, inode: Option<u64>, end: u64| ProgressMarker::File {
        path: path.into(),
        inode,
        start_byte: 0,
        end_byte: end,
        file_size: end * 10,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let mk = |id: &str, path: &str, inode: Option<u64>, end: u64| {
        let mut r = sample_record(id);
        r.source_type = SourceType::File;
        r.source_name = path.to_string();
        r.progress_start = marker(path, inode, 0);
        r.progress_end = marker(path, inode, end);
        r
    };
    // /rot.log: inode 11 reached byte 900, then the file was rotated and the
    // new one (inode 22) reached byte 60. /same.log was copytruncated: the
    // same inode reached 70, then the new content 30. /legacy.log predates
    // inode tracking. The newest row wins outright.
    for (id, path, inode, end) in [
        ("cur-1", "/rot.log", Some(11), 400),
        ("cur-2", "/rot.log", Some(11), 900),
        ("cur-3", "/rot.log", Some(22), 20),
        ("cur-4", "/rot.log", Some(22), 60),
        ("cur-5", "/same.log", Some(5), 70),
        ("cur-6", "/same.log", Some(5), 30),
        ("cur-7", "/legacy.log", None, 15),
    ] {
        store
            .save_batch_state(&mk(id, path, inode, end))
            .await
            .unwrap();
        let p = BatchPatch::default();
        for st in LEGAL_WALK {
            store.update_batch_state(id, st, &p).await.unwrap();
        }
        // `updated_at` orders "most recent": make each commit strictly later.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // The restart cursor is the window READ last, not the one COMMITTED last:
    // recovery can commit an older VERIFIED window after a newer one (review of
    // #564). L1 (inode 41) was read first, L2 (inode 42) second; L2 commits
    // first and L1 last, so `updated_at` would name the older file.
    for (id, inode, end) in [("cur-l1", 41, 100), ("cur-l2", 42, 20)] {
        store
            .save_batch_state(&mk(id, "/late.log", Some(inode), end))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let p = BatchPatch::default();
    for id in ["cur-l2", "cur-l1"] {
        for st in LEGAL_WALK {
            store.update_batch_state(id, st, &p).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // "Newest" compares instants, not text: `.1Z` sorts after `.15Z` as a
    // string though it is 50 ms earlier (review of #564). Created in that
    // order, as the engine would.
    for (id, created_at, inode, end) in [
        ("cur-z1", "2026-01-01T00:00:00.1Z", 61, 10),
        ("cur-z2", "2026-01-01T00:00:00.15Z", 62, 20),
    ] {
        let mut r = mk(id, "/frac.log", Some(inode), end);
        r.created_at = created_at.into();
        store.save_batch_state(&r).await.unwrap();
        for st in LEGAL_WALK {
            store.update_batch_state(id, st, &p).await.unwrap();
        }
    }
    // An inode above i64::MAX must survive the round trip exactly: a lossy
    // conversion would read as a different (or unknown) identity.
    let huge = u64::MAX - 1;
    store
        .save_batch_state(&mk("cur-huge", "/huge.log", Some(huge), 7))
        .await
        .unwrap();
    for st in LEGAL_WALK {
        store.update_batch_state("cur-huge", st, &p).await.unwrap();
    }
    // An UNCOMMITTED row on the new inode with a bigger end must not count.
    store
        .save_batch_state(&mk("cur-8", "/rot.log", Some(22), 5_000))
        .await
        .unwrap();

    let mut got = store.committed_cursors(SourceType::File).await.unwrap();
    got.retain(|c| {
        [
            "/rot.log",
            "/same.log",
            "/legacy.log",
            "/late.log",
            "/huge.log",
            "/frac.log",
        ]
        .contains(&c.path.as_str())
    });
    got.sort_by(|a, b| a.path.cmp(&b.path));
    let want = |path: &str, inode: Option<u64>, end_byte: u64| CommittedCursor {
        path: path.into(),
        inode,
        end_byte,
        // The NEWEST committed row's observed size, not a maximum: /same.log's
        // newest row ended at 30 (file_size 300) though an earlier one ended at
        // 70, and /rot.log's old inode (9000) must not leak into the new one.
        observed_size: Some(end_byte * 10),
        content_fingerprint: None,
        observed_mtime: None,
        tail_fingerprint: None,
    };
    assert_eq!(
        got,
        vec![
            want("/frac.log", Some(62), 20),
            want("/huge.log", Some(huge), 7),
            // Read last, committed first: still the restart cursor.
            want("/late.log", Some(42), 20),
            want("/legacy.log", None, 15),
            // The rotated path resumes at the NEW file's 60, not the old 900.
            want("/rot.log", Some(22), 60),
            // copytruncate keeps the inode: the newest row's 30, not the old
            // content's 70 - with a maximum, a restart after the new content
            // regrew past 70 would skip its bytes 30..70 (review of #564).
            want("/same.log", Some(5), 30),
        ]
    );
    // Rows of another source type never appear in a File answer, and a
    // SyslogSpool row is answered for SyslogSpool alone — with its own inode.
    let spool_marker = |path: &str, inode: Option<u64>, end: u64| ProgressMarker::SyslogSpool {
        spool_id: "s".into(),
        path: path.into(),
        inode,
        start_byte: 0,
        end_byte: end,
        received_time_start: None,
        received_time_end: None,
        file_size: None,
        content_fingerprint: None,
    };
    for (id, inode, end) in [("cur-s1", Some(31), 500), ("cur-s2", Some(32), 20)] {
        let mut r = sample_record(id);
        r.source_type = SourceType::SyslogSpool;
        r.source_name = "/rot-spool.log".to_string();
        r.progress_start = spool_marker("/rot-spool.log", inode, 0);
        r.progress_end = spool_marker("/rot-spool.log", inode, end);
        store.save_batch_state(&r).await.unwrap();
        let p = BatchPatch::default();
        for st in LEGAL_WALK {
            store.update_batch_state(id, st, &p).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let spool = store
        .committed_cursors(SourceType::SyslogSpool)
        .await
        .unwrap();
    assert_eq!(
        spool
            .iter()
            .filter(|c| c.path == "/rot-spool.log")
            .collect::<Vec<_>>(),
        vec![&CommittedCursor {
            path: "/rot-spool.log".into(),
            inode: Some(32),
            end_byte: 20,
            // These spool markers record no file length (as before #578).
            observed_size: None,
            content_fingerprint: None,
            observed_mtime: None,
            tail_fingerprint: None,
        }],
        "the spool answers with the newest inode and that inode's maximum"
    );
    assert!(spool
        .iter()
        .all(|c| !["/rot.log", "/same.log", "/legacy.log"].contains(&c.path.as_str())));
    assert!(store
        .committed_cursors(SourceType::File)
        .await
        .unwrap()
        .iter()
        .all(|c| c.path != "/rot-spool.log"));
}

/// #93: the claim primitive is what makes N engines safe over one store. A
/// live engine's in-flight batches must NEVER transfer; a dead engine's must
/// transfer wholesale once its lease expires; committed rows are never
/// claimable.
async fn claims_respect_live_leases_and_take_over_expired_ones(store: &dyn StateStore) {
    let past = "2000-01-01T00:00:00+00:00".to_string();
    let future = "2100-01-01T00:00:00+00:00".to_string();
    let now = "2050-01-01T00:00:00+00:00";

    // engine-a owns one live-leased batch and one expired-leased batch.
    let mut live = sample_record("claim-live");
    live.owner = Some("engine-a".into());
    live.lease_expires_at = Some(future.clone());
    store.save_batch_state(&live).await.unwrap();
    let mut dead = sample_record("claim-dead");
    dead.owner = Some("engine-a".into());
    dead.lease_expires_at = Some(past.clone());
    store.save_batch_state(&dead).await.unwrap();
    // An ownerless pre-#93 row is claimable by anyone.
    store
        .save_batch_state(&sample_record("claim-orphan"))
        .await
        .unwrap();
    // A committed row must never be claimed, whatever its lease says.
    let mut done = sample_record("claim-done");
    done.owner = Some("engine-a".into());
    done.lease_expires_at = Some(past.clone());
    store.save_batch_state(&done).await.unwrap();
    let p = BatchPatch::default();
    for st in LEGAL_WALK {
        store
            .update_batch_state("claim-done", st, &p)
            .await
            .unwrap();
    }

    // A lease expiring EXACTLY at `now` is expired (inclusive boundary): a
    // batch must never sit unclaimable at the instant of expiry.
    let mut edge = sample_record("claim-edge");
    edge.owner = Some("engine-a".into());
    edge.lease_expires_at = Some(now.to_string());
    store.save_batch_state(&edge).await.unwrap();

    // engine-b recovers: gets the expired batch and the orphan - NOT the live
    // one, NOT the committed one.
    let claimed = store
        .claim_incomplete_batches("engine-b", now, &future)
        .await
        .unwrap();
    let ids: Vec<_> = claimed.iter().map(|b| b.batch_id.as_str()).collect();
    assert!(
        ids.contains(&"claim-dead"),
        "expired lease must transfer: {ids:?}"
    );
    assert!(
        ids.contains(&"claim-orphan"),
        "ownerless row must be claimable: {ids:?}"
    );
    assert!(
        ids.contains(&"claim-edge"),
        "expiry is inclusive - a lease ending AT now transfers: {ids:?}"
    );
    assert!(
        !ids.contains(&"claim-live"),
        "a LIVE engine's batch must never transfer: {ids:?}"
    );
    assert!(
        !ids.contains(&"claim-done"),
        "committed rows are never claimable: {ids:?}"
    );
    for b in claimed.iter().filter(|b| b.batch_id.starts_with("claim-")) {
        assert_eq!(b.owner.as_deref(), Some("engine-b"));
        assert_eq!(b.lease_expires_at.as_deref(), Some(future.as_str()));
    }

    // engine-a recovers afterwards: its live batch is still its own; the
    // transferred one now belongs to engine-b under a live lease and must not
    // come back.
    let reclaimed = store
        .claim_incomplete_batches("engine-a", now, &future)
        .await
        .unwrap();
    let ids: Vec<_> = reclaimed.iter().map(|b| b.batch_id.as_str()).collect();
    assert!(
        ids.contains(&"claim-live"),
        "own live batch stays claimable by its owner: {ids:?}"
    );
    assert!(
        !ids.contains(&"claim-dead"),
        "a batch transferred under a live lease must not bounce back: {ids:?}"
    );
}

/// #128: pruning committed history must never move a recovery cursor, never
/// touch non-committed rows, never delete past the age cutoff, and always
/// honor the batch limit.
async fn prune_preserves_cursor_seeds_and_bounds_deletion(store: &dyn StateStore) {
    let file_marker = |path: &str, end: u64| ProgressMarker::File {
        path: path.into(),
        inode: None,
        start_byte: 0,
        end_byte: end,
        file_size: end,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let kafka_marker = |partition: i32, group: &str| ProgressMarker::Kafka {
        topic: "app_events".into(),
        partition,
        start_offset: 0,
        end_offset: 10,
        consumer_group: group.into(),
    };
    let record = |id: &str, source_type: SourceType, name: &str, marker: ProgressMarker| {
        let mut r = sample_record(id);
        r.source_type = source_type;
        r.source_name = name.into();
        r.progress_start = marker.clone();
        r.progress_end = marker;
        r
    };
    let committed = [
        record(
            "pr-a10",
            SourceType::File,
            "dir-src",
            file_marker("/logs/a", 10),
        ),
        record(
            "pr-a20",
            SourceType::File,
            "dir-src",
            file_marker("/logs/a", 20),
        ),
        record(
            "pr-a30",
            SourceType::File,
            "dir-src",
            file_marker("/logs/a", 30),
        ),
        record(
            "pr-b05",
            SourceType::File,
            "dir-src",
            file_marker("/logs/b", 5),
        ),
        // #614: the same topic's other partition and another consumer group,
        // each older than pr-k02. Each is its own resume point and survives.
        record(
            "pr-k1p",
            SourceType::Kafka,
            "app_events",
            kafka_marker(1, "vtop-engine"),
        ),
        record(
            "pr-kog",
            SourceType::Kafka,
            "app_events",
            kafka_marker(0, "vtop-engine-old"),
        ),
        record(
            "pr-k01",
            SourceType::Kafka,
            "app_events",
            sample_record("x").progress_end,
        ),
        record(
            "pr-k02",
            SourceType::Kafka,
            "app_events",
            sample_record("x").progress_end,
        ),
    ];
    for rec in &committed {
        store.save_batch_state(rec).await.unwrap();
        for st in LEGAL_WALK {
            store
                .update_batch_state(&rec.batch_id, st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }
    let kafka_offsets = || async {
        let mut offsets = store.committed_kafka_offsets().await.unwrap();
        offsets.sort_by(|a, b| {
            (&a.topic, a.partition, &a.consumer_group).cmp(&(
                &b.topic,
                b.partition,
                &b.consumer_group,
            ))
        });
        offsets
    };
    let kafka_before = kafka_offsets().await;
    // A VERIFIED row on the same path with a HIGHER end_byte: it must be
    // ignored by both the seed aggregate and the prune (state protection).
    let verified = record(
        "pr-v40",
        SourceType::File,
        "dir-src",
        file_marker("/logs/a", 40),
    );
    store.save_batch_state(&verified).await.unwrap();
    for st in &LEGAL_WALK[..6] {
        store
            .update_batch_state("pr-v40", *st, &BatchPatch::default())
            .await
            .unwrap();
    }

    // /logs/c was copytruncated: the newer row ends LOWER than the older one.
    // The newer one is the restart cursor and is what pruning must keep; the
    // highest offset, which pruning used to keep, is the old content's.
    for (id, end) in [("pr-c90", 90), ("pr-c15", 15)] {
        let rec = record(id, SourceType::File, "dir-src", file_marker("/logs/c", end));
        store.save_batch_state(&rec).await.unwrap();
        for st in LEGAL_WALK {
            store
                .update_batch_state(id, st, &BatchPatch::default())
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // The battery shares one store across checks, so earlier residue may
    // contribute additional paths; assert on this check's paths and on
    // before/after invariance rather than absolute contents.
    let seeds = || async {
        let mut seeds = store.committed_cursors(SourceType::File).await.unwrap();
        seeds.sort_by(|a, b| a.path.cmp(&b.path));
        seeds
    };
    let seeds_before = seeds().await;
    let end_of = |seeds: &[crate::models::CommittedCursor], path: &str| {
        seeds.iter().find(|c| c.path == path).map(|c| c.end_byte)
    };
    assert_eq!(end_of(&seeds_before, "/logs/a"), Some(30));
    assert_eq!(end_of(&seeds_before, "/logs/b"), Some(5));
    assert_eq!(end_of(&seeds_before, "/logs/c"), Some(15));

    // Nothing is older than the epoch: age protection deletes zero rows.
    assert_eq!(
        store
            .prune_committed_history("1970-01-01T00:00:00+00:00", 100)
            .await
            .unwrap(),
        0
    );

    // Deletable set under a future cutoff: pr-a10, pr-a20 (path /logs/a has a
    // newer committed row), pr-c90 (older than pr-c15 though its offset is
    // higher), pr-k01 (a newer row of its partition and group exists).
    // Protected: pr-a30, pr-b05 and pr-c15 (newest per path), pr-k02, pr-k1p
    // and pr-kog (newest of their partition and consumer group, #614), pr-v40
    // (not SOURCE_COMMITTED).
    let future = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
    assert_eq!(
        store.prune_committed_history(&future, 1).await.unwrap(),
        1,
        "the batch limit bounds every pass"
    );
    // Drain in bounded passes until quiescent: incremental pruning must
    // converge and a converged store must report zero.
    let mut passes = 0;
    while store.prune_committed_history(&future, 2).await.unwrap() > 0 {
        passes += 1;
        assert!(passes < 64, "pruning must converge");
    }
    assert_eq!(
        store.prune_committed_history(&future, 100).await.unwrap(),
        0
    );

    assert_eq!(
        seeds_before,
        seeds().await,
        "cursor seeds are prune-invariant"
    );
    assert_eq!(
        kafka_before,
        kafka_offsets().await,
        "kafka resume points are prune-invariant"
    );
    for survivor in [
        "pr-a30", "pr-b05", "pr-c15", "pr-k02", "pr-k1p", "pr-kog", "pr-v40",
    ] {
        assert!(
            store.get_batch(survivor).await.unwrap().is_some(),
            "{survivor} must survive"
        );
    }
    for deleted in ["pr-a10", "pr-a20", "pr-c90", "pr-k01"] {
        assert!(
            store.get_batch(deleted).await.unwrap().is_none(),
            "{deleted} must be pruned"
        );
    }
}

/// #560: a prune that must keep an anchor lists its rows first and deletes
/// only those - never a row it did not list, never a row that has since
/// gained a reason to stay, and never more than the listing's limit.
async fn listed_prune_deletes_only_what_was_listed(store: &dyn StateStore) {
    let marker = |end: u64| ProgressMarker::File {
        path: "/logs/listed".into(),
        inode: None,
        start_byte: 0,
        end_byte: end,
        file_size: end,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    for (id, end) in [("lp-10", 10), ("lp-20", 20), ("lp-30", 30), ("lp-40", 40)] {
        let mut rec = sample_record(id);
        rec.source_type = SourceType::File;
        rec.source_name = "listed-src".into();
        rec.progress_start = marker(end - 10);
        rec.progress_end = marker(end);
        store.save_batch_state(&rec).await.unwrap();
        for st in LEGAL_WALK {
            store
                .update_batch_state(id, st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }
    let future = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
    let ours = |rows: Vec<BatchRecord>| -> Vec<String> {
        rows.into_iter()
            .map(|r| r.batch_id)
            .filter(|id| id.starts_with("lp-"))
            .collect()
    };

    // Listing deletes nothing, and never names the row a seed needs.
    let listed = ours(store.prune_candidates(&future, 1_000).await.unwrap());
    assert_eq!(
        listed,
        ["lp-10", "lp-20", "lp-30"],
        "the newest row is never a candidate"
    );
    assert!(
        store.get_batch("lp-10").await.unwrap().is_some(),
        "listing deleted a row"
    );
    let limited = store.prune_candidates(&future, 1).await.unwrap();
    assert_eq!(limited.len(), 1, "the limit bounds the listing");

    // Delete a listed SUBSET plus a row that was never a candidate: only the
    // listed candidates go.
    let deleted = store
        .prune_listed(
            &future,
            &["lp-10".into(), "lp-40".into(), "no-such-row".into()],
        )
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    assert!(store.get_batch("lp-10").await.unwrap().is_none());
    assert!(
        store.get_batch("lp-40").await.unwrap().is_some(),
        "the newest row of its path was deleted because it was named"
    );
    assert!(
        store.get_batch("lp-20").await.unwrap().is_some(),
        "a row that was listed but not named in the delete was deleted"
    );
    // An age cutoff the rows do not meet protects them even when named.
    let past = "1970-01-01T00:00:00+00:00";
    assert_eq!(
        store
            .prune_listed(past, &["lp-20".into(), "lp-30".into()])
            .await
            .unwrap(),
        0
    );
    assert_eq!(store.prune_listed(&future, &[]).await.unwrap(), 0);
    assert_eq!(
        store
            .prune_listed(&future, &["lp-20".into(), "lp-30".into()])
            .await
            .unwrap(),
        2
    );
}
