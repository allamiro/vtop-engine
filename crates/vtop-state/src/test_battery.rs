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

use crate::{BatchPatch, BatchRecord, CommittedCursor, StateStore};
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
    get_missing_returns_none(store).await;
    rejects_commit_before_verified(store).await;
    full_legal_walk_commits(store).await;
    mark_failed_from_any_state(store).await;
    lists_incomplete_and_failed(store).await;
    max_committed_end_bytes_aggregates_in_store(store).await;
    committed_cursors_never_mix_the_offsets_of_two_files(store).await;
    claims_respect_live_leases_and_take_over_expired_ones(store).await;
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
            // A spool marker records no file length.
            observed_size: None,
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
    // higher), pr-k01 (newer kafka row exists). Protected: pr-a30, pr-b05 and
    // pr-c15 (newest per path), pr-k02 (newest for its source), pr-v40 (not
    // SOURCE_COMMITTED).
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
    for survivor in ["pr-a30", "pr-b05", "pr-c15", "pr-k02", "pr-v40"] {
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
