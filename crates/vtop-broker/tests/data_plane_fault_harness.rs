//! Deterministic distributed data-plane fault harness (#188).
//!
//! ## What this extends (does not rebuild)
//!
//! | Harness | Owns |
//! |---|---|
//! | `vtop_log::sim` / `sim_crash_sweep` (#155) | Disk crash, torn write, `FailOp` |
//! | `vtop-meta` storage / Raft three-node (#167/#171) | Metadata consensus + partition router |
//! | `quorum_replication` / `meta_lease_fencing` (#175/#176) | Quorum HWM + lease fencing unit paths |
//! | **This file** | Combining **real** `InProcessReplicaSet` quorum logic with controllable **network** faults, delayed follower fsync, leader/follower restart, producer retry across fencing, and consumer fetch during leadership change |
//!
//! Disk faults stay on [`vtop_log::sim::SimStorage`] / [`FaultPlan`]. Network
//! faults stay on [`FaultInjectingReplicaSet`]. They are configured
//! independently and may be combined in one scenario.
//!
//! ## Seeds and replay
//!
//! Every scenario takes a `u64` seed. Failure messages include `seed=…`.
//! Re-running the same scenario with the same seed must yield the same
//! oracle (acked offsets, HWM, invariant outcomes).
//!
//! ## How to run
//!
//! ```text
//! # CI suite (every PR; also covered by `cargo test --workspace --locked`)
//! cargo test -p vtop-broker --test data_plane_fault_harness --locked
//!
//! # Extended nightly-style suite (matrix + multi-seed replay)
//! cargo test -p vtop-broker --test data_plane_fault_harness --locked -- --ignored
//! ```
//!
//! Deferred from this first slice: full 10k-scenario combinatorial matrix in
//! PR CI, and sealed-segment transfer / repair path faults.

use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use vtop_broker::replication::{
    ClusterCommittedOffset, FaultInjectingReplicaSet, FollowerNetworkFault, InProcessFollower,
    InProcessReplicaSet, ReplicaSet,
};
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage};
use vtop_log::{
    ActiveSegment, FetchBatch, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, ErrorResponse, FetchRequest, FetchResponse, Message,
    ProduceRecord, ProduceRequest, ProduceResponse, RangeIdentity, Role, WireFrame,
};

const SEED: u64 = 0x5eed_0188;
const LEADER: Uuid = Uuid::from_u128(0xA1);
const FOLLOWER_1: Uuid = Uuid::from_u128(0xA2);
const FOLLOWER_2: Uuid = Uuid::from_u128(0xA3);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const FENCING_EPOCH: u64 = 18;

#[derive(Clone, Debug)]
struct NodePaths {
    root: PathBuf,
    segment: PathBuf,
    epochs: PathBuf,
}

struct Cluster {
    seed: u64,
    sims: Vec<SimStorage>,
    envs: Vec<Env>,
    paths: Vec<NodePaths>,
    range: RangeIdentity,
    meta: MetaFencingEpoch,
    cluster_committed: ClusterCommittedOffset,
    leader: Option<LocalBroker>,
    followers: Vec<Arc<InProcessFollower>>,
    replica_set: Arc<FaultInjectingReplicaSet>,
    /// Offsets that received a successful Quorum produce ack.
    acked: Vec<u64>,
    /// Sequences that were submitted but not acked (may be retried).
    unacked: Vec<u64>,
    next_request_id: u64,
}

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn descriptor(segment_id: u128, range: &RangeIdentity) -> SegmentDescriptor {
    SegmentDescriptor {
        segment_id: Uuid::from_u128(segment_id),
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: RangeLineage {
            range_id: range.range_id,
            generation: range.range_generation,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    }
}

fn node_root(index: usize) -> PathBuf {
    PathBuf::from(format!("/node-{index}"))
}

fn boot_cluster(seed: u64) -> Cluster {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);

    let mut sims = Vec::new();
    let mut envs = Vec::new();
    let mut paths = Vec::new();

    for index in 0..3 {
        let sim = SimStorage::new();
        let root = node_root(index);
        sim.create_dir_all(&root);
        let env = sim.env(seed ^ (index as u64 + 1));
        paths.push(NodePaths {
            root: root.clone(),
            segment: root.join("range.active"),
            epochs: root.join("epochs"),
        });
        sims.push(sim);
        envs.push(env);
    }

    let mut followers = Vec::new();
    for (index, node_id) in [(1usize, FOLLOWER_1), (2, FOLLOWER_2)] {
        let segment = ActiveSegment::create_in(
            &envs[index],
            &paths[index].segment,
            descriptor(0xD1 + index as u128, &range),
            SegmentConfig::default(),
        )
        .unwrap_or_else(|e| panic!("seed={seed:#x}: create follower {index}: {e}"));
        let epochs = ProducerEpochJournal::open_in(&envs[index], &paths[index].epochs)
            .unwrap_or_else(|e| panic!("seed={seed:#x}: epochs follower {index}: {e}"));
        followers.push(Arc::new(
            InProcessFollower::new(
                node_id,
                segment,
                epochs,
                range.clone(),
                FENCING_EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap_or_else(|e| panic!("seed={seed:#x}: follower {index}: {e}")),
        ));
    }

    let leader_segment = ActiveSegment::create_in(
        &envs[0],
        &paths[0].segment,
        descriptor(0xD1, &range),
        SegmentConfig::default(),
    )
    .unwrap_or_else(|e| panic!("seed={seed:#x}: create leader: {e}"));
    let leader_epochs = ProducerEpochJournal::open_in(&envs[0], &paths[0].epochs)
        .unwrap_or_else(|e| panic!("seed={seed:#x}: leader epochs: {e}"));

    let inner = Arc::new(InProcessReplicaSet::new(followers.clone()));
    let replica_set = Arc::new(FaultInjectingReplicaSet::new(inner, seed));
    let leader = LocalBroker::with_replication(
        leader_segment,
        leader_epochs,
        range.clone(),
        FENCING_EPOCH,
        meta.clone(),
        LEADER,
        Some(cluster_committed.clone()),
        Some(replica_set.clone() as Arc<dyn ReplicaSet>),
    )
    .unwrap_or_else(|e| panic!("seed={seed:#x}: leader: {e}"));

    Cluster {
        seed,
        sims,
        envs,
        paths,
        range,
        meta,
        cluster_committed,
        leader: Some(leader),
        followers,
        replica_set,
        acked: Vec::new(),
        unacked: Vec::new(),
        next_request_id: 1,
    }
}

impl Cluster {
    fn ctx(&self) -> String {
        format!(
            "seed={:#x} tick={} hwm={} acked={:?} unacked={:?}",
            self.seed,
            self.replica_set.tick(),
            self.cluster_committed.get(),
            self.acked,
            self.unacked
        )
    }

    fn leader(&self) -> &LocalBroker {
        self.leader
            .as_ref()
            .unwrap_or_else(|| panic!("{}: leader missing during restart", self.ctx()))
    }

    fn produce_frame(&mut self, sequence: u64, fencing_epoch: u64) -> WireFrame {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        WireFrame {
            request_id,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range: self.range.clone(),
                fencing_epoch,
                producer_id: PRODUCER,
                producer_epoch: 1,
                first_sequence: sequence,
                durability: WireDurability::Quorum,
                records: vec![ProduceRecord {
                    timestamp_millis: 1_000 + sequence as i64,
                    key: b"k".to_vec(),
                    value: format!("v{sequence}").into_bytes(),
                }],
            }),
        }
    }

    fn produce(&mut self, sequence: u64) -> Result<ProduceResponse, ErrorResponse> {
        let frame = self.produce_frame(sequence, FENCING_EPOCH);
        match self.leader().handle(Role::Producer, frame).message {
            Message::ProduceResponse(value) => {
                let offset = value.outcomes[0].offset;
                if !self.acked.contains(&offset) {
                    self.acked.push(offset);
                    self.acked.sort_unstable();
                }
                self.unacked.retain(|s| *s != sequence);
                Ok(value)
            }
            Message::Error(err) => {
                if !self.unacked.contains(&sequence) {
                    self.unacked.push(sequence);
                }
                Err(err)
            }
            other => panic!("{}: unexpected produce response {other:?}", self.ctx()),
        }
    }

    fn produce_with_epoch(
        &mut self,
        sequence: u64,
        fencing_epoch: u64,
    ) -> Result<ProduceResponse, ErrorResponse> {
        let frame = self.produce_frame(sequence, fencing_epoch);
        match self.leader().handle(Role::Producer, frame).message {
            Message::ProduceResponse(value) => Ok(value),
            Message::Error(err) => Err(err),
            other => panic!("{}: unexpected produce response {other:?}", self.ctx()),
        }
    }

    fn fetch_leader(&self, start_offset: u64, max_records: u32) -> FetchResponse {
        let frame = WireFrame {
            request_id: 0,
            stream_id: 1,
            message: Message::FetchRequest(FetchRequest {
                range: self.range.clone(),
                fencing_epoch: FENCING_EPOCH,
                start_offset,
                max_bytes: 64 * 1024,
                max_records,
            }),
        };
        match self.leader().handle(Role::Consumer, frame).message {
            Message::FetchResponse(batch) => batch,
            Message::Error(err) => panic!("{}: fetch error {err:?}", self.ctx()),
            other => panic!("{}: unexpected fetch response {other:?}", self.ctx()),
        }
    }

    fn fetch_follower(&self, index: usize, start_offset: u64, max_records: usize) -> FetchBatch {
        self.followers[index]
            .fetch(start_offset, 64 * 1024, max_records)
            .unwrap_or_else(|e| panic!("{}: follower {index} fetch: {e}", self.ctx()))
    }

    /// Automatic invariant checks required by #188.
    fn assert_invariants(&self) {
        let hwm = self.cluster_committed.get();
        let ctx = self.ctx();

        // 1. No acknowledged record is lost: HWM covers every ack, and each
        //    acked offset is visible via leader fetch. The fetch is sized to
        //    the acked offset range rather than a fixed record cap.
        if let Some(max_acked) = self.acked.last().copied() {
            assert!(
                hwm > max_acked,
                "{ctx}: HWM {hwm} must cover acked offset {max_acked}"
            );
            let batch = self.fetch_leader(0, u32::try_from(max_acked + 1).unwrap_or(u32::MAX));
            for offset in &self.acked {
                assert!(
                    batch.records.iter().any(|r| r.offset == *offset),
                    "{ctx}: acked offset {offset} missing from leader fetch"
                );
            }
        }

        // 2. Fetch never crosses committed HWM.
        let leader_fetch = self.fetch_leader(0, u32::try_from(hwm).unwrap_or(u32::MAX));
        assert!(
            leader_fetch
                .records
                .iter()
                .all(|r| r.offset < leader_fetch.committed_high_watermark),
            "{ctx}: leader fetch returned offset at/above HWM"
        );
        assert_eq!(
            leader_fetch.committed_high_watermark, hwm,
            "{ctx}: leader fetch HWM mismatch"
        );
        for (index, follower) in self.followers.iter().enumerate() {
            if !follower.is_online() {
                continue;
            }
            if let Ok(batch) = follower.fetch(0, 64 * 1024, 64) {
                assert!(
                    batch.high_watermark == 0
                        || batch
                            .records
                            .iter()
                            .all(|r| r.offset < batch.high_watermark),
                    "{ctx}: follower {index} fetch crossed HWM"
                );
                assert!(
                    batch.high_watermark <= hwm,
                    "{ctx}: follower {index} HWM {} > cluster {hwm}",
                    batch.high_watermark
                );
            }
        }

        // 3. A follower's observed HWM never exceeds its own local
        //    durability or the cluster HWM.
        for (index, follower) in self.followers.iter().enumerate() {
            let local = follower.local_committed_offset();
            let observed = follower.cluster_committed().get();
            assert!(
                observed <= local,
                "{ctx}: follower {index} observed HWM {observed} > local {local}"
            );
            assert!(
                observed <= hwm,
                "{ctx}: follower {index} observed HWM {observed} > cluster {hwm}"
            );
        }

        // 4. Follower log completeness: up to its observed HWM, each online
        //    follower's log must contain exactly the leader's records
        //    (offset, key, value, timestamp) — a silently diverged follower
        //    must not pass on a bounded HWM alone. Fetches are sized to the
        //    HWM range, so this stays offset-bounded and cheap.
        for (index, follower) in self.followers.iter().enumerate() {
            if !follower.is_online() {
                continue;
            }
            let observed = follower.cluster_committed().get();
            if observed == 0 {
                continue;
            }
            let leader_batch = self.fetch_leader(0, u32::try_from(observed).unwrap_or(u32::MAX));
            let follower_batch =
                self.fetch_follower(index, 0, usize::try_from(observed).unwrap_or(usize::MAX));
            assert_eq!(
                follower_batch.records.len(),
                leader_batch.records.len(),
                "{ctx}: follower {index} holds {} records below its observed HWM {observed}, leader holds {}",
                follower_batch.records.len(),
                leader_batch.records.len()
            );
            for (leader_record, follower_record) in leader_batch
                .records
                .iter()
                .zip(follower_batch.records.iter())
            {
                assert_eq!(
                    follower_record.offset, leader_record.offset,
                    "{ctx}: follower {index} offset mismatch below HWM {observed}"
                );
                assert_eq!(
                    follower_record.record.key, leader_record.key,
                    "{ctx}: follower {index} key diverged at offset {}",
                    leader_record.offset
                );
                assert_eq!(
                    follower_record.record.value, leader_record.value,
                    "{ctx}: follower {index} value diverged at offset {}",
                    leader_record.offset
                );
                assert_eq!(
                    follower_record.record.timestamp_millis, leader_record.timestamp_millis,
                    "{ctx}: follower {index} timestamp diverged at offset {}",
                    leader_record.offset
                );
            }
        }
    }

    fn restart_follower(&mut self, index: usize) {
        // Offline so no concurrent apply races the storage swap. Crash drops
        // volatile bytes; previously fsynced quorum data survives reboot.
        self.followers[index].set_online(false);
        let node = index + 1;
        let parking_seg = self.paths[node].root.join("parking.active");
        let parking_epochs = self.paths[node].root.join("parking.epochs");
        let parking_segment = ActiveSegment::create_in(
            &self.envs[node],
            &parking_seg,
            descriptor(0xF0 + index as u128, &self.range),
            SegmentConfig::default(),
        )
        .unwrap_or_else(|e| panic!("{}: parking segment follower {index}: {e}", self.ctx()));
        let parking_journal = ProducerEpochJournal::open_in(&self.envs[node], &parking_epochs)
            .unwrap_or_else(|e| panic!("{}: parking epochs follower {index}: {e}", self.ctx()));
        let (old_segment, old_epochs) = self.followers[index]
            .swap_storage(parking_segment, parking_journal)
            .unwrap_or_else(|e| panic!("{}: park follower {index}: {e}", self.ctx()));
        drop(old_segment);
        drop(old_epochs);

        self.sims[node].crash();
        self.sims[node].reboot();
        let env = self.sims[node].env(self.seed ^ ((index + 2) as u64));
        self.envs[node] = env.clone();
        let segment = ActiveSegment::recover_in(&env, &self.paths[node].segment)
            .unwrap_or_else(|e| panic!("{}: recover follower {index}: {e}", self.ctx()));
        let epochs = ProducerEpochJournal::open_in(&env, &self.paths[node].epochs)
            .unwrap_or_else(|e| panic!("{}: recover follower {index} epochs: {e}", self.ctx()));
        let (parking_segment, parking_journal) = self.followers[index]
            .swap_storage(segment, epochs)
            .unwrap_or_else(|e| panic!("{}: install follower {index}: {e}", self.ctx()));
        drop(parking_segment);
        drop(parking_journal);
        self.followers[index].set_online(true);
        self.followers[index].set_hold_fsync(false);
    }

    fn restart_leader(&mut self) {
        let replica_set = self.replica_set.clone();
        let range = self.range.clone();
        let meta = self.meta.clone();
        let cluster_committed = self.cluster_committed.clone();
        // Drop the live broker so segment/epoch file handles release.
        self.leader = None;
        self.sims[0].crash();
        self.sims[0].reboot();
        let env = self.sims[0].env(self.seed ^ 1);
        self.envs[0] = env.clone();
        let segment = ActiveSegment::recover_in(&env, &self.paths[0].segment)
            .unwrap_or_else(|e| panic!("{}: recover leader: {e}", self.ctx()));
        let epochs = ProducerEpochJournal::open_in(&env, &self.paths[0].epochs)
            .unwrap_or_else(|e| panic!("{}: recover leader epochs: {e}", self.ctx()));
        self.leader = Some(
            LocalBroker::with_replication(
                segment,
                epochs,
                range,
                FENCING_EPOCH,
                meta,
                LEADER,
                Some(cluster_committed),
                Some(replica_set as Arc<dyn ReplicaSet>),
            )
            .unwrap_or_else(|e| panic!("{}: rebuild leader: {e}", self.ctx())),
        );
    }
}

// ---------------------------------------------------------------------------
// CI scenarios
// ---------------------------------------------------------------------------

#[test]
fn partition_one_follower_preserves_quorum_and_invariants() {
    let mut c = boot_cluster(SEED);
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.followers[1].set_online(false);
    c.produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.assert_invariants();
    assert_eq!(c.cluster_committed.get(), 2, "{}", c.ctx());
    // Heal and catch up via producer retry / next produce.
    c.followers[1].set_online(true);
    c.replica_set.clear_follower_fault(1);
    c.produce(2)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    // Follower 1 may still lag until a replicate covers it; drain + produce.
    c.replica_set.drain_all();
    c.assert_invariants();
}

#[test]
fn delayed_follower_fsync_is_independent_of_network() {
    let mut c = boot_cluster(SEED ^ 0x10);
    // Hold fsync on follower 0; network remains healthy.
    c.followers[0].set_hold_fsync(true);
    // Majority is leader + follower 1 → produce still acks.
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert_eq!(c.followers[0].local_committed_offset(), 0, "{}", c.ctx());
    assert_eq!(c.followers[1].local_committed_offset(), 1, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 1, "{}", c.ctx());

    // Inject a network drop on follower 1 while fsync still held on 0 —
    // proves the two axes compose; next produce needs heal or flush.
    c.replica_set.set_follower_fault(
        1,
        FollowerNetworkFault {
            drop_next: 1,
            ..FollowerNetworkFault::default()
        },
    );
    let err = c.produce(1).expect_err("quorum should fail");
    assert_eq!(err.code, ErrorCode::Overloaded, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 1, "{}", c.ctx());

    // Flush delayed fsync and clear network drop → retry commits.
    c.followers[0]
        .flush_held_fsync()
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.followers[0].set_hold_fsync(false);
    c.replica_set.clear_follower_fault(1);
    c.replica_set.drain_all();
    c.produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.assert_invariants();
}

#[test]
fn network_loss_dup_reorder_delay_and_catch_up() {
    let mut c = boot_cluster(SEED ^ 0x20);
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));

    // Delay follower 0 by 3 ticks; duplicate once toward follower 1.
    c.replica_set.set_follower_fault(
        0,
        FollowerNetworkFault {
            delay_ticks: 3,
            ..FollowerNetworkFault::default()
        },
    );
    c.replica_set.set_follower_fault(
        1,
        FollowerNetworkFault {
            duplicate_next: 1,
            ..FollowerNetworkFault::default()
        },
    );
    c.produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert_eq!(c.followers[0].local_committed_offset(), 1, "{}", c.ctx());
    assert_eq!(c.followers[1].local_committed_offset(), 2, "{}", c.ctx());

    // Reorder window on follower 0 while catching up delayed traffic.
    c.replica_set.advance_tick(3);
    assert_eq!(c.followers[0].local_committed_offset(), 2, "{}", c.ctx());

    // Activate a reorder window on follower 0 and drop follower 1's next two
    // appends, then produce through the window: produce 2 stays buffered,
    // produce 3 fills the window and flushes it LIFO. Follower 0 rejects the
    // offset-3 append (base mismatch against its tip) and then applies offset
    // 2 — a real reorder-induced gap at offset 3. Both produces miss quorum
    // (follower 0 lags, follower 1 drops), so the sequences stay unacked.
    c.replica_set.set_follower_fault(
        0,
        FollowerNetworkFault {
            reorder_window: 2,
            ..FollowerNetworkFault::default()
        },
    );
    c.replica_set.set_follower_fault(
        1,
        FollowerNetworkFault {
            drop_next: 2,
            ..FollowerNetworkFault::default()
        },
    );
    let err = c.produce(2).expect_err("no quorum while reorder buffers");
    assert_eq!(err.code, ErrorCode::Overloaded, "{}", c.ctx());
    assert_eq!(
        c.replica_set.pending_stats().reorder_buffered,
        1,
        "{}: reorder window must hold the first append",
        c.ctx()
    );
    let err = c.produce(3).expect_err("no quorum while reorder flushes");
    assert_eq!(err.code, ErrorCode::Overloaded, "{}", c.ctx());
    // LIFO proof: FIFO delivery would have applied offsets 2 then 3 (local
    // offset 4); LIFO rejects offset 3 first, so follower 0 stops at 3 with
    // offset 3 missing while the leader is durable through 4.
    assert_eq!(c.followers[0].local_committed_offset(), 3, "{}", c.ctx());
    assert_eq!(c.followers[1].local_committed_offset(), 2, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 2, "{}", c.ctx());

    // Heal and catch up via the harness's normal path: idempotent producer
    // retries of the unacked sequences. The seq-2 retry alone cannot form a
    // quorum (leader committed is 4; replaying one record only covers offset
    // 2) but backfills follower 1. The seq-3 retry then repairs follower 0's
    // reorder gap (its tip equals the replayed base offset) and commits.
    c.replica_set.clear_follower_fault(0);
    c.replica_set.clear_follower_fault(1);
    c.replica_set.drain_all();
    let _ = c.produce(2);
    let caught = c
        .produce(3)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert!(caught.outcomes[0].duplicate, "{}", c.ctx());
    assert_eq!(
        c.followers[0].local_committed_offset(),
        4,
        "{}: retry must repair the reorder gap",
        c.ctx()
    );
    assert_eq!(c.followers[1].local_committed_offset(), 4, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 4, "{}", c.ctx());
    let retry = c
        .produce(2)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert!(retry.outcomes[0].duplicate, "{}", c.ctx());

    c.produce(4)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.assert_invariants();
}

#[test]
fn kill_leader_around_quorum_boundary_and_fence() {
    let mut c = boot_cluster(SEED ^ 0x30);
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));

    // Push toward a quorum boundary: partition one follower, hold fsync on the
    // other so the next produce cannot form majority.
    c.followers[1].set_online(false);
    c.followers[0].set_hold_fsync(true);
    let err = c.produce(1).expect_err("no quorum");
    assert_eq!(err.code, ErrorCode::Overloaded, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 1, "{}", c.ctx());

    // Steal the lease (kill/fence leader) before durability can complete.
    c.meta.set(FENCING_EPOCH + 1);
    c.followers[0].set_hold_fsync(false);
    let _ = c.followers[0].flush_held_fsync();
    c.followers[1].set_online(true);

    let fenced = c
        .produce_with_epoch(1, FENCING_EPOCH)
        .expect_err("stale leader must not commit");
    assert_eq!(fenced.code, ErrorCode::Fenced, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 1, "{}", c.ctx());

    // Fetch during leadership change: stale epoch is fenced; HWM stays put.
    match c
        .leader()
        .handle(
            Role::Consumer,
            WireFrame {
                request_id: 42,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range: c.range.clone(),
                    fencing_epoch: FENCING_EPOCH,
                    start_offset: 0,
                    max_bytes: 1024,
                    max_records: 8,
                }),
            },
        )
        .message
    {
        Message::Error(ErrorResponse {
            code: ErrorCode::Fenced,
            ..
        }) => {}
        Message::FetchResponse(batch) => {
            assert_eq!(batch.committed_high_watermark, 1, "{}", c.ctx());
            assert!(
                batch
                    .records
                    .iter()
                    .all(|r| r.offset < batch.committed_high_watermark),
                "{}",
                c.ctx()
            );
        }
        other => panic!("{}: unexpected fetch {other:?}", c.ctx()),
    }
    assert_eq!(c.cluster_committed.get(), 1, "{}", c.ctx());
}

#[test]
fn producer_retry_across_failover_is_idempotent() {
    let mut c = boot_cluster(SEED ^ 0x40);
    let first = c
        .produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert_eq!(first.outcomes[0].offset, 0);

    // Lose quorum so the next sequence is locally durable on the leader but
    // not cluster-committed. Restart the leader from sim disk, heal followers,
    // then retry the unacked sequence (catch-up replication).
    c.followers[0].set_online(false);
    c.followers[1].set_online(false);
    let err = c.produce(1).expect_err("no quorum");
    assert_eq!(err.code, ErrorCode::Overloaded, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 1, "{}", c.ctx());

    c.restart_leader();
    c.followers[0].set_online(true);
    c.followers[1].set_online(true);
    c.replica_set.drain_all();

    let recovered = c
        .produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert!(recovered.outcomes[0].duplicate, "{}", c.ctx());
    assert_eq!(recovered.outcomes[0].offset, 1, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 2, "{}", c.ctx());

    let retry0 = c
        .produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert!(retry0.outcomes[0].duplicate, "{}", c.ctx());
    assert_eq!(retry0.outcomes[0].offset, 0, "{}", c.ctx());
    c.assert_invariants();
}

#[test]
fn follower_restart_and_reconnect_catch_up() {
    let mut c = boot_cluster(SEED ^ 0x50);
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));

    c.restart_follower(0);
    assert!(
        c.followers[0].local_committed_offset() >= 2,
        "{}: durable prefix must survive restart",
        c.ctx()
    );

    // Drop the next RPC to the restarted follower and take the peer offline so
    // the produce fails before HWM advances. Healing the network and retrying
    // then forces catch-up onto the restarted follower.
    c.replica_set.set_follower_fault(
        0,
        FollowerNetworkFault {
            drop_next: 1,
            ..FollowerNetworkFault::default()
        },
    );
    c.followers[1].set_online(false);
    let err = c.produce(2).expect_err("no quorum while partitioned");
    assert_eq!(err.code, ErrorCode::Overloaded, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 2, "{}", c.ctx());
    assert_eq!(c.followers[0].local_committed_offset(), 2, "{}", c.ctx());

    c.replica_set.clear_follower_fault(0);
    let caught = c
        .produce(2)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert!(caught.outcomes[0].duplicate, "{}", c.ctx());
    assert_eq!(c.followers[0].local_committed_offset(), 3, "{}", c.ctx());
    assert_eq!(c.cluster_committed.get(), 3, "{}", c.ctx());

    c.followers[1].set_online(true);
    c.produce(3)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    assert_eq!(c.followers[0].local_committed_offset(), 4, "{}", c.ctx());
    c.assert_invariants();
}

#[test]
fn consumer_fetch_during_leadership_change() {
    let mut c = boot_cluster(SEED ^ 0x60);
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));

    let before = c.fetch_follower(0, 0, 64);
    assert_eq!(before.records.len(), 2, "{}", c.ctx());
    assert_eq!(before.high_watermark, 2, "{}", c.ctx());

    // Leadership change via epoch steal; stale leader fetch must fence or
    // continue serving only the committed prefix without advancing HWM.
    c.meta.set(FENCING_EPOCH + 1);
    match c
        .leader()
        .handle(
            Role::Consumer,
            WireFrame {
                request_id: 99,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range: c.range.clone(),
                    fencing_epoch: FENCING_EPOCH,
                    start_offset: 0,
                    max_bytes: 1024,
                    max_records: 8,
                }),
            },
        )
        .message
    {
        Message::Error(ErrorResponse {
            code: ErrorCode::Fenced,
            ..
        }) => {}
        Message::FetchResponse(batch) => {
            assert!(
                batch
                    .records
                    .iter()
                    .all(|r| r.offset < batch.committed_high_watermark),
                "{}",
                c.ctx()
            );
            assert_eq!(batch.committed_high_watermark, 2, "{}", c.ctx());
        }
        other => panic!("{}: unexpected {other:?}", c.ctx()),
    }
    assert_eq!(c.cluster_committed.get(), 2, "{}", c.ctx());
}

#[test]
fn simultaneous_disk_fault_plan_and_network_drop() {
    let mut c = boot_cluster(SEED ^ 0x70);
    c.produce(0)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));

    // Disk: fail the next op on follower 0's sim. Network: drop one RPC to f1.
    // Independently configured — neither axis implies the other.
    c.sims[1].set_fault(FaultPlan::FailOp {
        op: c.sims[1].op_count(),
        kind: std::io::ErrorKind::Interrupted,
    });
    c.replica_set.set_follower_fault(
        1,
        FollowerNetworkFault {
            drop_next: 1,
            ..FollowerNetworkFault::default()
        },
    );
    // Produce may fail (no majority) depending on which follower still acks.
    let _ = c.produce(1);
    assert!(
        c.cluster_committed.get() <= 2,
        "{}: HWM must not jump past durable quorum",
        c.ctx()
    );

    c.sims[1].set_fault(FaultPlan::None);
    c.replica_set.clear_follower_fault(1);
    c.replica_set.drain_all();
    // Idempotent recovery: retry until quorum.
    c.produce(1)
        .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
    c.assert_invariants();
}

#[test]
fn seed_replay_is_deterministic() {
    fn oracle(seed: u64) -> (u64, Vec<u64>, u64) {
        let mut c = boot_cluster(seed);
        c.followers[1].set_online(false);
        c.replica_set.set_follower_fault(
            0,
            FollowerNetworkFault {
                delay_ticks: 1,
                ..FollowerNetworkFault::default()
            },
        );
        let _ = c.produce(0);
        c.replica_set.advance_tick(1);
        let _ = c.produce(1);
        c.followers[1].set_online(true);
        c.replica_set.clear_follower_fault(0);
        c.replica_set.drain_all();
        let _ = c.produce(2);
        (
            c.cluster_committed.get(),
            c.acked.clone(),
            c.replica_set.tick(),
        )
    }
    let a = oracle(SEED ^ 0x99);
    let b = oracle(SEED ^ 0x99);
    assert_eq!(a, b, "same seed must replay identically");
}

// ---------------------------------------------------------------------------
// Extended nightly-style suite
// ---------------------------------------------------------------------------

#[test]
#[ignore = "nightly-style extended matrix; run with -- --ignored"]
fn extended_multi_seed_smoke_matrix() {
    let seeds = [
        SEED,
        SEED ^ 0x1111,
        SEED ^ 0x2222,
        SEED ^ 0x3333,
        SEED ^ 0xabcd,
        0x5eed_0001,
        0x5eed_00aa,
        0xdead_beef,
    ];
    for seed in seeds {
        let mut c = boot_cluster(seed);
        for seq in 0..8u64 {
            match seq % 5 {
                0 => c.followers[1].set_online(false),
                1 => c.followers[1].set_online(true),
                2 => c.replica_set.set_follower_fault(
                    0,
                    FollowerNetworkFault {
                        drop_next: 1,
                        ..FollowerNetworkFault::default()
                    },
                ),
                3 => {
                    c.replica_set.clear_follower_fault(0);
                    c.followers[0].set_hold_fsync(true);
                }
                _ => {
                    let _ = c.followers[0].flush_held_fsync();
                    c.followers[0].set_hold_fsync(false);
                    c.replica_set.drain_all();
                }
            }
            let _ = c.produce(seq);
            c.replica_set.advance_tick(1);
        }
        c.followers[0].set_hold_fsync(false);
        let _ = c.followers[0].flush_held_fsync();
        c.followers[1].set_online(true);
        c.replica_set.drain_all();
        // Drain unacked with retries.
        let pending = c.unacked.clone();
        for seq in pending {
            let _ = c.produce(seq);
        }
        c.assert_invariants();
    }
}

#[test]
#[ignore = "nightly-style extended matrix; run with -- --ignored"]
fn extended_combined_disk_network_restart_sweep() {
    for bit in 0..16u64 {
        let seed = SEED ^ (0x1000 + bit);
        let mut c = boot_cluster(seed);
        c.produce(0)
            .unwrap_or_else(|e| panic!("{}: {e:?}", c.ctx()));
        if bit & 1 != 0 {
            c.followers[0].set_online(false);
        }
        if bit & 2 != 0 {
            c.replica_set.set_follower_fault(
                1,
                FollowerNetworkFault {
                    delay_ticks: 2,
                    ..FollowerNetworkFault::default()
                },
            );
        }
        if bit & 4 != 0 {
            c.followers[1.min(c.followers.len() - 1)].set_hold_fsync(true);
        }
        let _ = c.produce(1);
        c.replica_set.advance_tick(2);
        if bit & 8 != 0 {
            c.followers[0].set_online(true);
            c.restart_follower(0);
        }
        c.followers[0].set_online(true);
        c.followers[1].set_online(true);
        c.replica_set.clear_follower_fault(1);
        let _ = c.followers[1].flush_held_fsync();
        c.followers[1].set_hold_fsync(false);
        c.replica_set.drain_all();
        let _ = c.produce(1);
        let _ = c.produce(2);
        c.assert_invariants();
    }
}
