//! Codex review regressions for Raft adapter durable frontiers.
//!
//! 1. Applied frontier limits recovery replay (uncommitted tail stays truncatable).
//! 2. Exact purged LogId persists across reopen (not inferred from chunk layout).
//! 3. Snapshot install discards a stale physical log ending below the snapshot.
//! 4. Reopen heals a stale tail left by an interrupted snapshot install.
//! 5. Purge persists the logical frontier before physical chunk deletion.

use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{CommittedLeaderId, LogId, Membership, SnapshotMeta, StoredMembership};
use std::collections::BTreeSet;
use std::io::Cursor;
use std::path::Path;
use uuid::Uuid;
use vtop_log::sim::SimStorage;
use vtop_meta::raft::{MetaRaftLogStore, MetaRaftStateMachine, MetaRaftStore};
use vtop_meta::{
    CommandEnvelope, MetaLogEntry, MetaLogPayload, MetaMembership, MetaNodeId, MetaSnapshots,
    MetaStorage, MetadataCommand,
};

fn cluster() -> Uuid {
    Uuid::from_u128(0x00ad_b7ad_a071)
}

fn envelope(request: u128) -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::from_u128(request),
        issued_at_ms: 1_750_000_000_000,
    }
}

fn put_entry(term: u64, index: u64, seed: u128) -> MetaLogEntry {
    MetaLogEntry {
        term,
        index,
        payload: MetaLogPayload::Normal(MetadataCommand::PutKeyRecord {
            env: envelope(seed),
            key_uuid: Uuid::from_u128(seed),
            scheme: 1,
            public_material_digest: [seed as u8; 32],
        }),
    }
}

#[test]
fn uncommitted_tail_is_not_applied_on_reopen_once_applied_file_exists() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/n1"));
    let env = sim.env(0x51);
    let mut storage = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
    storage
        .append(&[put_entry(1, 1, 1), put_entry(1, 2, 2), put_entry(1, 3, 3)])
        .unwrap();
    storage.apply_through(2).unwrap();
    assert_eq!(storage.last_applied(), 2);

    // Crash window: entry 3 is durable but not applied.
    drop(storage);
    sim.reboot();
    let mut recovered = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
    assert_eq!(recovered.last_applied(), 2);
    assert_eq!(recovered.log().last_index(), Some(3));
    // Conflict truncation of the uncommitted tail must still be legal.
    recovered.truncate_since(3).unwrap();
    assert_eq!(recovered.log().last_index(), Some(2));
}

#[test]
fn purged_log_id_persists_exact_term_and_index_across_adapter_reopen() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/n1"));
    let env = sim.env(0x53);
    let mut storage = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
    let mut entries = Vec::new();
    for i in 1..=8u64 {
        entries.push(put_entry(2, i, u128::from(i)));
    }
    entries[0] = MetaLogEntry {
        term: 2,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1)],
            learners: vec![],
        }),
    };
    storage.append(&entries).unwrap();
    storage.apply_through(8).unwrap();
    storage.write_snapshot().unwrap();
    // Exact purged id may sit above the physical first-1 after whole-chunk purge.
    storage.save_purged(7, 5).unwrap();
    assert_eq!(
        storage.last_purged().map(|p| (p.term, p.index)),
        Some((7, 5))
    );
    drop(storage);

    sim.reboot();
    let store = MetaRaftStore::open_in(&env, "/n1", cluster()).unwrap();
    assert_eq!(
        store.with_storage(|s| s.last_purged().map(|p| (p.term, p.index))),
        Some((7, 5))
    );
}

#[tokio::test]
async fn snapshot_install_discards_stale_tail_and_allows_append_at_frontier() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/leader"));
    sim.create_dir_all(Path::new("/follower"));
    let env = sim.env(0x54);

    let mut leader = MetaStorage::open_in(&env, "/leader", cluster()).unwrap();
    let mut entries = Vec::new();
    for i in 1..=10u64 {
        entries.push(put_entry(3, i, u128::from(i)));
    }
    entries[0] = MetaLogEntry {
        term: 3,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1), MetaNodeId(2)],
            learners: vec![],
        }),
    };
    leader.append(&entries).unwrap();
    leader.apply_through(10).unwrap();
    let snap_meta = leader.write_snapshot().unwrap();
    let snap_bytes = env.storage.read(&snap_meta.path).unwrap();

    // Follower physical log ends at 3 — below the snapshot frontier of 10.
    let mut follower = MetaStorage::open_in(&env, "/follower", cluster()).unwrap();
    follower
        .append(&[
            put_entry(1, 1, 100),
            put_entry(1, 2, 101),
            put_entry(1, 3, 102),
        ])
        .unwrap();
    follower.apply_through(3).unwrap();
    assert_eq!(follower.log().last_index(), Some(3));
    drop(follower);

    let store = MetaRaftStore::open_in(&env, "/follower", cluster()).unwrap();
    let mut sm = MetaRaftStateMachine::new(store.clone());
    let membership = Membership::new(
        vec![BTreeSet::from([1u64, 2u64])],
        None::<std::collections::BTreeSet<u64>>,
    );
    let last_log_id = LogId::new(CommittedLeaderId::new(3, 0), 9); // meta 10 → raft 9
    let meta = SnapshotMeta {
        last_log_id: Some(last_log_id),
        last_membership: StoredMembership::new(Some(last_log_id), membership),
        snapshot_id: snap_meta.snapshot_id.clone(),
    };
    sm.install_snapshot(&meta, Box::new(Cursor::new(snap_bytes)))
        .await
        .expect("install snapshot");

    store.with_storage(|s| {
        assert_eq!(s.last_applied(), 10);
        assert!(
            s.log().last_index().is_none(),
            "stale physical tail must be discarded"
        );
    });

    // Next append must continue from the snapshot frontier.
    let mut follower = MetaStorage::open_in(&env, "/follower", cluster()).unwrap();
    assert_eq!(follower.last_applied(), 10);
    assert!(follower.log().last_index().is_none());
    follower.append(&[put_entry(3, 11, 11)]).unwrap();
    assert_eq!(follower.log().last_index(), Some(11));
}

#[test]
fn reopen_discards_stale_tail_left_by_interrupted_snapshot_install() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/leader"));
    sim.create_dir_all(Path::new("/follower"));
    let env = sim.env(0x55);

    let mut leader = MetaStorage::open_in(&env, "/leader", cluster()).unwrap();
    let mut entries = Vec::new();
    for i in 1..=10u64 {
        entries.push(put_entry(3, i, u128::from(i)));
    }
    entries[0] = MetaLogEntry {
        term: 3,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1), MetaNodeId(2)],
            learners: vec![],
        }),
    };
    leader.append(&entries).unwrap();
    leader.apply_through(10).unwrap();
    let snap_meta = leader.write_snapshot().unwrap();
    let snap_bytes = env.storage.read(&snap_meta.path).unwrap();

    // Follower physical log ends at 3.
    let mut follower = MetaStorage::open_in(&env, "/follower", cluster()).unwrap();
    follower
        .append(&[
            put_entry(1, 1, 100),
            put_entry(1, 2, 101),
            put_entry(1, 3, 102),
        ])
        .unwrap();
    follower.apply_through(3).unwrap();
    drop(follower);

    // Crash window: snapshot + applied frontier durable, discard never ran.
    let mut snaps = MetaSnapshots::open_in(&env, "/follower", cluster()).unwrap();
    snaps.install(&snap_bytes).unwrap();
    let mut interrupted = MetaStorage::open_in(&env, "/follower", cluster()).unwrap();
    assert_eq!(interrupted.last_applied(), 10);
    assert_eq!(interrupted.log().last_index(), Some(3));
    interrupted.sync_applied_frontier().unwrap();
    drop(interrupted);

    sim.reboot();
    let store = MetaRaftStore::open_in(&env, "/follower", cluster()).unwrap();
    store.with_storage(|s| {
        assert_eq!(s.last_applied(), 10);
        assert!(
            s.log().last_index().is_none(),
            "reopen must discard the stale physical tail"
        );
    });
    // Append at last_applied + 1 must succeed after reopen heal.
    let mut healed = MetaStorage::open_in(&env, "/follower", cluster()).unwrap();
    healed.append(&[put_entry(3, 11, 11)]).unwrap();
    assert_eq!(healed.log().last_index(), Some(11));
}

#[tokio::test]
async fn purge_persists_frontier_before_chunk_deletion_survives_interrupt() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/n1"));
    let env = sim.env(0x56);

    // Tiny chunks so purge can drop whole files below the snapshot.
    let mut storage = MetaStorage::open_with(
        &env,
        "/n1",
        cluster(),
        vtop_meta::MetaStorageConfig {
            log: vtop_meta::MetaLogConfig {
                max_chunk_bytes: 256,
            },
        },
    )
    .unwrap();
    let mut storage_entries = Vec::new();
    for i in 1..=12u64 {
        storage_entries.push(put_entry(4, i, u128::from(i)));
    }
    storage_entries[0] = MetaLogEntry {
        term: 4,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1)],
            learners: vec![],
        }),
    };
    storage.append(&storage_entries).unwrap();
    storage.apply_through(12).unwrap();
    storage.write_snapshot().unwrap();
    // Seed a lower durable purged frontier so the "stale file" hazard exists.
    storage.save_purged(1, 2).unwrap();
    drop(storage);

    let store = MetaRaftStore::open_tiny(&env, "/n1", cluster()).unwrap();
    assert_eq!(
        store.with_storage(|s| s.last_purged().map(|p| (p.term, p.index))),
        Some((1, 2))
    );

    let mut log_store = MetaRaftLogStore::new(store.clone());
    // Persist frontier through meta 8 (raft index 7) before chunk delete.
    let purge_id = LogId::new(CommittedLeaderId::new(4, 0), 7);
    log_store.purge(purge_id).await.unwrap();
    assert_eq!(
        store.with_storage(|s| s.last_purged().map(|p| (p.term, p.index))),
        Some((4, 8))
    );

    // Safe interrupt window: frontier already advanced; reopen must keep it
    // even if further physical deletion is skipped.
    drop(log_store);
    drop(store);
    sim.reboot();

    let recovered = MetaRaftStore::open_tiny(&env, "/n1", cluster()).unwrap();
    assert_eq!(
        recovered.with_storage(|s| s.last_purged().map(|p| (p.term, p.index))),
        Some((4, 8)),
        "advanced purged frontier must survive even if chunk deletion was interrupted"
    );
}
