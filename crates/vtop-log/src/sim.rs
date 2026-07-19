//! Deterministic in-memory storage simulator for crash and fault sweeps.
//!
//! The simulator distinguishes durable state from volatile state the way a
//! POSIX filesystem does: file data becomes durable on `sync_data`, directory
//! entries (creates, renames, removals) become durable on `sync_dir`. A
//! simulated crash discards volatile state; sweep faults instead materialize
//! the volatile op log as an in-order prefix whose final write may be torn at
//! any byte, which is exactly the space of crash-consistent states the real
//! log must recover from.

use crate::env::{Clock, DirEntryInfo, Env, OpenMode, Rng, Storage, StorageFile};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// One deterministic fault, keyed by the global operation counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPlan {
    None,
    /// Crash before operation `op` executes; nothing volatile survives.
    CrashBefore(u64),
    /// Crash during write operation `op`: every earlier volatile op reaches
    /// disk in order and the write itself is truncated at `byte_cut`.
    CrashDuringWrite {
        op: u64,
        byte_cut: usize,
    },
    /// Operation `op` fails with `kind` without crashing.
    FailOp {
        op: u64,
        kind: io::ErrorKind,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceKind {
    Open,
    HandleRead,
    HandleWrite,
    HandleSeek,
    HandleSetLen,
    HandleSyncData,
    HandleLen,
    ReadFile,
    Rename,
    RemoveFile,
    Exists,
    ReadDir,
    SyncDir,
}

/// One executed storage operation, in global order. `len` is the payload
/// length for writes and zero otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    pub index: u64,
    pub kind: TraceKind,
    pub path: PathBuf,
    pub len: u64,
}

/// A snapshot of durable disk contents.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimDiskState {
    pub files: BTreeMap<PathBuf, Vec<u8>>,
    pub dirs: BTreeSet<PathBuf>,
}

#[derive(Clone, Debug)]
enum DataOp {
    Write { position: u64, bytes: Vec<u8> },
    SetLen { len: u64 },
}

#[derive(Clone, Debug)]
enum NsOp {
    Create { path: PathBuf, inode: usize },
    Rename { from: PathBuf, to: PathBuf },
    Remove { path: PathBuf },
}

#[derive(Clone, Debug)]
enum PendingOp {
    Data { inode: usize, op: DataOp },
    Namespace(NsOp),
}

#[derive(Clone, Debug, Default)]
struct Inode {
    durable: Vec<u8>,
    visible: Vec<u8>,
}

struct SimState {
    inodes: Vec<Inode>,
    ns_visible: BTreeMap<PathBuf, usize>,
    ns_durable: BTreeMap<PathBuf, usize>,
    /// Directories that exist. Modeled as pre-existing filesystem state (like
    /// a test's tempdir), durable immediately and outside the crash model.
    dirs: BTreeSet<PathBuf>,
    pending: Vec<PendingOp>,
    plan: FaultPlan,
    trace: Vec<TraceEntry>,
    next_op: u64,
    epoch: u64,
    crashed: bool,
}

impl SimState {
    fn new() -> Self {
        Self {
            inodes: Vec::new(),
            ns_visible: BTreeMap::new(),
            ns_durable: BTreeMap::new(),
            dirs: BTreeSet::new(),
            pending: Vec::new(),
            plan: FaultPlan::None,
            trace: Vec::new(),
            next_op: 0,
            epoch: 0,
            crashed: false,
        }
    }

    fn require_dir(&self, path: &Path) -> io::Result<()> {
        if self.dirs.contains(path) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no such simulated directory",
            ))
        }
    }

    /// Count the operation, record it, and apply any planned fault. `Err`
    /// means the operation must not take effect.
    fn begin_op(&mut self, kind: TraceKind, path: &Path, len: u64) -> io::Result<u64> {
        if self.crashed {
            return Err(io::Error::other("simulated storage is down until reboot"));
        }
        let index = self.next_op;
        self.next_op += 1;
        self.trace.push(TraceEntry {
            index,
            kind,
            path: path.to_path_buf(),
            len,
        });
        match self.plan {
            FaultPlan::CrashBefore(op) if op == index => {
                self.materialize_crash(0, None);
                Err(io::Error::other("simulated crash before operation"))
            }
            // A torn crash aimed at a non-write op degenerates to CrashBefore;
            // write operations intercept this case in `write` instead.
            FaultPlan::CrashDuringWrite { op, .. }
                if op == index && kind != TraceKind::HandleWrite =>
            {
                self.materialize_crash(0, None);
                Err(io::Error::other("simulated crash before operation"))
            }
            FaultPlan::FailOp { op, kind } if op == index => {
                Err(io::Error::new(kind, "injected storage failure"))
            }
            _ => Ok(index),
        }
    }

    /// Apply the first `prefix` volatile ops in issue order, optionally
    /// followed by one torn write, then discard the rest of volatile state.
    fn materialize_crash(&mut self, prefix: usize, torn: Option<(usize, u64, Vec<u8>)>) {
        let pending = std::mem::take(&mut self.pending);
        for op in pending.into_iter().take(prefix) {
            match op {
                PendingOp::Data { inode, op } => {
                    apply_data_op(&mut self.inodes[inode].durable, &op)
                }
                PendingOp::Namespace(op) => apply_ns_op(&mut self.ns_durable, &op),
            }
        }
        if let Some((inode, position, bytes)) = torn {
            apply_data_op(
                &mut self.inodes[inode].durable,
                &DataOp::Write { position, bytes },
            );
        }
        self.ns_visible = self.ns_durable.clone();
        for inode in &mut self.inodes {
            inode.visible = inode.durable.clone();
        }
        self.plan = FaultPlan::None;
        self.crashed = true;
        self.epoch += 1;
    }

    fn visible_inode(&self, path: &Path) -> io::Result<usize> {
        self.ns_visible
            .get(path)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such simulated file"))
    }

    fn promote_file(&mut self, inode: usize) {
        let retained = std::mem::take(&mut self.pending);
        for op in retained {
            match op {
                PendingOp::Data { inode: target, op } if target == inode => {
                    apply_data_op(&mut self.inodes[inode].durable, &op);
                }
                other => self.pending.push(other),
            }
        }
    }

    fn promote_dir(&mut self, dir: &Path) {
        let retained = std::mem::take(&mut self.pending);
        for op in retained {
            match op {
                PendingOp::Namespace(ns) if ns_op_in_dir(&ns, dir) => {
                    apply_ns_op(&mut self.ns_durable, &ns);
                }
                other => self.pending.push(other),
            }
        }
    }
}

fn apply_data_op(content: &mut Vec<u8>, op: &DataOp) {
    match op {
        DataOp::Write { position, bytes } => {
            let position = usize::try_from(*position).expect("simulated file fits in memory");
            let end = position + bytes.len();
            if content.len() < end {
                content.resize(end, 0);
            }
            content[position..end].copy_from_slice(bytes);
        }
        DataOp::SetLen { len } => {
            let len = usize::try_from(*len).expect("simulated file fits in memory");
            content.resize(len, 0);
        }
    }
}

fn apply_ns_op(namespace: &mut BTreeMap<PathBuf, usize>, op: &NsOp) {
    match op {
        NsOp::Create { path, inode } => {
            namespace.insert(path.clone(), *inode);
        }
        NsOp::Rename { from, to } => {
            if let Some(inode) = namespace.remove(from) {
                namespace.insert(to.clone(), inode);
            }
        }
        NsOp::Remove { path } => {
            namespace.remove(path);
        }
    }
}

fn ns_op_in_dir(op: &NsOp, dir: &Path) -> bool {
    let parent = |path: &Path| path.parent().unwrap_or_else(|| Path::new(".")) == dir;
    match op {
        NsOp::Create { path, .. } | NsOp::Remove { path } => parent(path),
        NsOp::Rename { from, to } => parent(from) || parent(to),
    }
}

/// Shared handle to one simulated disk. Clones observe the same state.
#[derive(Clone)]
pub struct SimStorage {
    state: Arc<Mutex<SimState>>,
}

impl Default for SimStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl SimStorage {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SimState::new())),
        }
    }

    /// Build an [`Env`] over this disk with a seeded deterministic RNG.
    pub fn env(&self, seed: u64) -> Env {
        Env {
            storage: Arc::new(self.clone()),
            clock: Arc::new(SimClock::new(0)),
            rng: Arc::new(SimRng::new(seed)),
        }
    }

    /// Declare a directory (and its ancestors) as existing, mirroring a
    /// tempdir created before the workload starts. Not a traced storage op.
    pub fn create_dir_all(&self, path: &Path) {
        let mut state = self.lock();
        let mut current = Some(path);
        while let Some(dir) = current {
            state.dirs.insert(dir.to_path_buf());
            current = dir.parent();
        }
    }

    pub fn set_fault(&self, plan: FaultPlan) {
        self.lock().plan = plan;
    }

    /// Crash now: volatile state after the last sync is dropped.
    pub fn crash(&self) {
        self.lock().materialize_crash(0, None);
    }

    pub fn reboot(&self) {
        self.lock().crashed = false;
    }

    pub fn has_crashed(&self) -> bool {
        self.lock().crashed
    }

    pub fn op_count(&self) -> u64 {
        self.lock().next_op
    }

    pub fn trace(&self) -> Vec<TraceEntry> {
        self.lock().trace.clone()
    }

    pub fn snapshot(&self) -> SimDiskState {
        let state = self.lock();
        SimDiskState {
            files: state
                .ns_durable
                .iter()
                .map(|(path, inode)| (path.clone(), state.inodes[*inode].durable.clone()))
                .collect(),
            dirs: state.dirs.clone(),
        }
    }

    /// Reset the whole simulation to a rebooted disk holding `disk`. The op
    /// counter, trace, and fault plan restart from zero.
    pub fn restore(&self, disk: &SimDiskState) {
        let mut state = self.lock();
        let epoch = state.epoch + 1;
        *state = SimState::new();
        state.epoch = epoch;
        state.dirs = disk.dirs.clone();
        for (path, bytes) in &disk.files {
            let inode = state.inodes.len();
            state.inodes.push(Inode {
                durable: bytes.clone(),
                visible: bytes.clone(),
            });
            state.ns_visible.insert(path.clone(), inode);
            state.ns_durable.insert(path.clone(), inode);
        }
    }

    /// Flip bits in one durable byte of a quiescent file.
    pub fn corrupt(&self, path: &Path, byte_index: usize, xor_mask: u8) {
        let mut state = self.lock();
        let inode = *state
            .ns_durable
            .get(path)
            .expect("corrupt target must be durable");
        let inode = &mut state.inodes[inode];
        inode.durable[byte_index] ^= xor_mask;
        inode.visible[byte_index] ^= xor_mask;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SimState> {
        self.state.lock().expect("sim state lock")
    }
}

impl Storage for SimStorage {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Box<dyn StorageFile>> {
        let mut state = self.lock();
        state.begin_op(TraceKind::Open, path, 0)?;
        let existing = state.ns_visible.get(path).copied();
        let inode = match (mode, existing) {
            (OpenMode::Read | OpenMode::ReadWrite, Some(inode)) => inode,
            (OpenMode::Read | OpenMode::ReadWrite, None) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no such simulated file",
                ));
            }
            (OpenMode::CreateNew, Some(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "simulated file already exists",
                ));
            }
            (OpenMode::CreateAppend, Some(inode)) => inode,
            (OpenMode::CreateNew | OpenMode::CreateAppend, None) => {
                state.require_dir(path.parent().unwrap_or_else(|| Path::new("/")))?;
                let inode = state.inodes.len();
                state.inodes.push(Inode::default());
                state.ns_visible.insert(path.to_path_buf(), inode);
                state.pending.push(PendingOp::Namespace(NsOp::Create {
                    path: path.to_path_buf(),
                    inode,
                }));
                inode
            }
        };
        Ok(Box::new(SimFile {
            state: Arc::clone(&self.state),
            path: path.to_path_buf(),
            inode,
            position: 0,
            epoch: state.epoch,
            writable: mode != OpenMode::Read,
        }))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut state = self.lock();
        state.begin_op(TraceKind::ReadFile, path, 0)?;
        let inode = state.visible_inode(path)?;
        Ok(state.inodes[inode].visible.clone())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.lock();
        state.begin_op(TraceKind::Rename, from, 0)?;
        let inode = state.visible_inode(from)?;
        state.require_dir(to.parent().unwrap_or_else(|| Path::new("/")))?;
        state.ns_visible.remove(from);
        state.ns_visible.insert(to.to_path_buf(), inode);
        state.pending.push(PendingOp::Namespace(NsOp::Rename {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        }));
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut state = self.lock();
        state.begin_op(TraceKind::RemoveFile, path, 0)?;
        state.visible_inode(path)?;
        state.ns_visible.remove(path);
        state.pending.push(PendingOp::Namespace(NsOp::Remove {
            path: path.to_path_buf(),
        }));
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let mut state = self.lock();
        state.begin_op(TraceKind::Exists, path, 0)?;
        Ok(state.ns_visible.contains_key(path) || state.dirs.contains(path))
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntryInfo>> {
        let mut state = self.lock();
        state.begin_op(TraceKind::ReadDir, path, 0)?;
        state.require_dir(path)?;
        Ok(state
            .ns_visible
            .keys()
            .filter(|entry| entry.parent() == Some(path))
            .map(|entry| DirEntryInfo {
                path: entry.clone(),
                is_regular_file: true,
            })
            .collect())
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        let mut state = self.lock();
        state.begin_op(TraceKind::SyncDir, path, 0)?;
        state.require_dir(path)?;
        state.promote_dir(path);
        Ok(())
    }
}

struct SimFile {
    state: Arc<Mutex<SimState>>,
    path: PathBuf,
    inode: usize,
    position: u64,
    epoch: u64,
    writable: bool,
}

fn lock_handle(
    state: &Arc<Mutex<SimState>>,
    epoch: u64,
) -> io::Result<std::sync::MutexGuard<'_, SimState>> {
    let state = state.lock().expect("sim state lock");
    if state.epoch != epoch {
        return Err(io::Error::other("file handle lost in simulated crash"));
    }
    Ok(state)
}

impl Read for SimFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut state = lock_handle(&self.state, self.epoch)?;
        state.begin_op(TraceKind::HandleRead, &self.path, 0)?;
        let content = &state.inodes[self.inode].visible;
        let position = usize::try_from(self.position).expect("simulated file fits in memory");
        if position >= content.len() {
            return Ok(0);
        }
        let count = buffer.len().min(content.len() - position);
        buffer[..count].copy_from_slice(&content[position..position + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Write for SimFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated file handle is read-only",
            ));
        }
        let mut state = lock_handle(&self.state, self.epoch)?;
        if state.crashed {
            return Err(io::Error::other("simulated storage is down until reboot"));
        }
        let index = state.next_op;
        state.next_op += 1;
        state.trace.push(TraceEntry {
            index,
            kind: TraceKind::HandleWrite,
            path: self.path.clone(),
            len: buffer.len() as u64,
        });
        match state.plan {
            FaultPlan::CrashBefore(op) if op == index => {
                state.materialize_crash(0, None);
                return Err(io::Error::other("simulated crash before write"));
            }
            FaultPlan::CrashDuringWrite { op, byte_cut } if op == index => {
                let torn = buffer[..byte_cut.min(buffer.len())].to_vec();
                let prefix = state.pending.len();
                state.materialize_crash(prefix, Some((self.inode, self.position, torn)));
                return Err(io::Error::other("simulated crash tore this write"));
            }
            FaultPlan::FailOp { op, kind } if op == index => {
                return Err(io::Error::new(kind, "injected storage failure"));
            }
            _ => {}
        }
        let op = DataOp::Write {
            position: self.position,
            bytes: buffer.to_vec(),
        };
        apply_data_op(&mut state.inodes[self.inode].visible, &op);
        state.pending.push(PendingOp::Data {
            inode: self.inode,
            op,
        });
        self.position += buffer.len() as u64;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for SimFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let mut state = lock_handle(&self.state, self.epoch)?;
        state.begin_op(TraceKind::HandleSeek, &self.path, 0)?;
        let length = state.inodes[self.inode].visible.len() as i128;
        let target = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::End(delta) => length + i128::from(delta),
            SeekFrom::Current(delta) => i128::from(self.position) + i128::from(delta),
        };
        let target = u64::try_from(target).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "seek before start of file")
        })?;
        self.position = target;
        Ok(target)
    }
}

impl StorageFile for SimFile {
    fn sync_data(&mut self) -> io::Result<()> {
        let mut state = lock_handle(&self.state, self.epoch)?;
        state.begin_op(TraceKind::HandleSyncData, &self.path, 0)?;
        state.promote_file(self.inode);
        Ok(())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated file handle is read-only",
            ));
        }
        let mut state = lock_handle(&self.state, self.epoch)?;
        state.begin_op(TraceKind::HandleSetLen, &self.path, 0)?;
        let op = DataOp::SetLen { len };
        apply_data_op(&mut state.inodes[self.inode].visible, &op);
        state.pending.push(PendingOp::Data {
            inode: self.inode,
            op,
        });
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        let mut state = lock_handle(&self.state, self.epoch)?;
        state.begin_op(TraceKind::HandleLen, &self.path, 0)?;
        Ok(state.inodes[self.inode].visible.len() as u64)
    }
}

/// Settable clock. Plumbing only: nothing in the crate consumes time yet.
pub struct SimClock {
    millis: AtomicI64,
}

impl SimClock {
    pub fn new(millis: i64) -> Self {
        Self {
            millis: AtomicI64::new(millis),
        }
    }

    pub fn set(&self, millis: i64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_millis: i64) {
        self.millis.fetch_add(delta_millis, Ordering::SeqCst);
    }
}

impl Clock for SimClock {
    fn now_millis(&self) -> i64 {
        self.millis.load(Ordering::SeqCst)
    }
}

/// SplitMix64. Print the seed on failure so a sweep replays exactly.
pub struct SimRng {
    state: Mutex<u64>,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: Mutex::new(seed),
        }
    }

    fn next_u64(&self) -> u64 {
        let mut state = self.state.lock().expect("sim rng lock");
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = *state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }
}

impl Rng for SimRng {
    fn next_u128(&self) -> u128 {
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActiveSegment, Durability, LogRecord, RangeLineage, SegmentConfig, SegmentDescriptor,
    };
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use uuid::Uuid;

    fn descriptor() -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: Uuid::from_u128(1),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: RangeLineage::root(Uuid::from_u128(2)),
            base_offset: 40,
        }
    }

    fn config() -> SegmentConfig {
        SegmentConfig {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
        }
    }

    fn record(sequence: u64, value: &[u8]) -> LogRecord {
        LogRecord {
            producer_id: Uuid::from_u128(3),
            producer_epoch: 0,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: value.to_vec(),
        }
    }

    fn run_workload(env: &Env, directory: &Path) {
        let path = directory.join("twin.active");
        let mut segment = ActiveSegment::create_in(env, &path, descriptor(), config()).unwrap();
        segment
            .append(record(0, b"alpha"), Durability::Fsync)
            .unwrap();
        segment
            .append(record(1, b"beta"), Durability::Buffered)
            .unwrap();
        segment
            .append(record(2, b"gamma"), Durability::Fsync)
            .unwrap();
        drop(segment.seal().unwrap());
    }

    fn by_file_name(files: BTreeMap<PathBuf, Vec<u8>>) -> BTreeMap<OsString, Vec<u8>> {
        files
            .into_iter()
            .map(|(path, bytes)| (path.file_name().expect("file name").to_owned(), bytes))
            .collect()
    }

    #[test]
    fn read_only_handles_and_missing_directories_are_rejected_like_the_real_fs() {
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/log"));
        let storage = sim.env(1).storage;

        // Creating a file under a directory that does not exist must fail.
        let missing_parent = storage
            .open(Path::new("/absent/file"), OpenMode::CreateAppend)
            .err()
            .expect("open under a missing directory must fail");
        assert_eq!(missing_parent.kind(), io::ErrorKind::NotFound);
        // Listing or syncing a missing directory must fail, not report empty.
        assert_eq!(
            storage.read_dir(Path::new("/absent")).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            storage.sync_dir(Path::new("/absent")).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );

        // A read-only handle must reject mutation.
        let mut writer = storage
            .open(Path::new("/log/file"), OpenMode::CreateNew)
            .unwrap();
        writer.write_all(b"bytes").unwrap();
        writer.sync_data().unwrap();
        let mut reader = storage
            .open(Path::new("/log/file"), OpenMode::Read)
            .unwrap();
        assert_eq!(
            reader.write(b"x").unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            reader.set_len(0).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn identical_workload_produces_byte_identical_durable_files_on_real_and_sim_storage() {
        let real_dir = tempfile::tempdir().unwrap();
        run_workload(&Env::real(), real_dir.path());
        let real_files: BTreeMap<OsString, Vec<u8>> = std::fs::read_dir(real_dir.path())
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect();

        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/log"));
        run_workload(&sim.env(0x5eed), Path::new("/log"));
        let sim_files = by_file_name(sim.snapshot().files);

        assert_eq!(sim_files, real_files);
    }

    #[test]
    fn unsynced_writes_and_renames_do_not_survive_a_plain_crash() {
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/log"));
        let env = sim.env(1);
        let root = Path::new("/log");
        let mut segment =
            ActiveSegment::create_in(&env, root.join("crash.active"), descriptor(), config())
                .unwrap();
        let durable_before = sim.snapshot();
        segment
            .append(record(0, b"buffered only"), Durability::Buffered)
            .unwrap();
        sim.crash();
        assert!(segment
            .append(record(1, b"after crash"), Durability::Fsync)
            .is_err());
        assert_eq!(sim.snapshot(), durable_before);

        sim.reboot();
        let recovered = ActiveSegment::recover_in(&env, root.join("crash.active")).unwrap();
        assert_eq!(recovered.next_offset(), 40);
    }

    #[test]
    fn snapshot_and_restore_round_trip_resets_ops_and_faults() {
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/log"));
        let env = sim.env(2);
        drop(ActiveSegment::create_in(&env, "/log/reset.active", descriptor(), config()).unwrap());
        let disk = sim.snapshot();
        sim.set_fault(FaultPlan::CrashBefore(sim.op_count() + 1));
        sim.restore(&disk);
        assert_eq!(sim.op_count(), 0);
        assert!(!sim.has_crashed());
        assert_eq!(sim.snapshot(), disk);
        ActiveSegment::recover_in(&env, "/log/reset.active").unwrap();
    }
}
