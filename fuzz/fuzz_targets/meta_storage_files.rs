//! Fuzz the metadata store's on-disk files (`vtop-meta`): the log chunks and
//! their entry frames (`VTOPMLG1`, `VTOPMLE1`), the snapshot file
//! (`VTOPMSN1`), the hard state (`VTOPMHS1`), the applied and purged
//! frontiers (`VTOPMAP1`, `VTOPMPG1`) and the membership log id
//! (`VTOPMML1`). They are read back after a crash and a disk fault, and a
//! snapshot file also arrives from a leader, so no content may panic a
//! reader or the recovery that replays them (#729).
//!
//! The input is a bundle of files (`SimDiskState::from_bundle`) restored
//! onto the simulated disk. Each file is opened by its own reader, then the
//! whole directory goes through `MetaStorage::open_in`, the node's startup
//! path. The metadata command and snapshot payload decoders behind them are
//! #706's `meta_decode` target; they run here only as recovery calls them.
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::path::Path;
use uuid::Uuid;
use vtop_log::sim::{SimDiskState, SimStorage};
use vtop_meta::storage::applied::AppliedFrontierFile;
use vtop_meta::storage::membership_log_id::MembershipLogIdFile;
use vtop_meta::storage::purged::PurgedFrontierFile;
use vtop_meta::{HardStateFile, MetaLog, MetaLogConfig, MetaSnapshots, MetaStorage};

const SEED: u64 = 0x5eed_0729;
/// The data directory and cluster the seeds were written under
/// (`crates/vtop-meta/tests/fuzz_seeds.rs`).
const ROOT: &str = "/meta";
const CLUSTER: u128 = 0xc1c1_c1c1_0000_4000_8000_0000_0000_0729;

fuzz_target!(|data: &[u8]| {
    let disk = SimDiskState::from_bundle(data);
    if disk.files.is_empty() {
        return;
    }
    let sim = SimStorage::new();
    sim.restore(&disk);
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let cluster = Uuid::from_u128(CLUSTER);
    let root = Path::new(ROOT);

    // Each reader on its own, so a bad file does not hide the ones after it.
    let _ = HardStateFile::open_in(&env, root.join("meta.hardstate"));
    let _ = AppliedFrontierFile::open_in(&env, root.join("meta.applied"));
    let _ = PurgedFrontierFile::open_in(&env, root.join("meta.purged"));
    let _ = MembershipLogIdFile::open_in(&env, root.join("meta.membership_log_id"));
    if let Ok(log) = MetaLog::open_in(&env, root, cluster, MetaLogConfig::default()) {
        if let (Some(first), Some(last)) = (log.first_index(), log.last_index()) {
            let _ = log.read_range(first, last.saturating_add(1));
        }
    }
    if let Ok(mut snapshots) = MetaSnapshots::open_in(&env, root, cluster) {
        if let Some(meta) = snapshots.newest() {
            let _ = snapshots.read(&meta);
        }
        // A snapshot a leader sends is installed from its bytes.
        for (path, bytes) in &disk.files {
            if path.extension().and_then(|value| value.to_str()) == Some("vmsnap") {
                let _ = snapshots.install(bytes);
            }
        }
    }

    // Startup recovery over the directory as it was.
    sim.restore(&disk);
    sim.create_dir_all(Path::new(ROOT));
    let _ = MetaStorage::open_in(&env, root, cluster);
});
