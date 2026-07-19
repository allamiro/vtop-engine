//! Injectable storage, clock, and randomness seam.
//!
//! Every durable byte the crate writes flows through these object-safe traits
//! so the same code paths can run against the real filesystem or the
//! deterministic in-memory simulator in [`crate::sim`]. The real
//! implementations wrap `std::fs` without changing any on-disk byte.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

/// How a storage file is opened. Covers exactly the `OpenOptions`
/// combinations the crate uses today.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    /// Read-only; the file must exist.
    Read,
    /// Read and write; the file must exist.
    ReadWrite,
    /// Read and write; fail if the file already exists.
    CreateNew,
    /// Read and write; create the file if it is missing, never truncate.
    CreateAppend,
}

/// An open file handle whose data durability is explicit.
#[allow(clippy::len_without_is_empty)]
pub trait StorageFile: Read + Write + Seek + Send {
    /// Make previously written data durable, as `File::sync_data` does.
    fn sync_data(&mut self) -> io::Result<()>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntryInfo {
    pub path: PathBuf,
    pub is_regular_file: bool,
}

pub trait Storage: Send + Sync {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Box<dyn StorageFile>>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn exists(&self, path: &Path) -> io::Result<bool>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntryInfo>>;
    /// Make directory entries (creates, renames, removals) durable.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
}

pub trait Clock: Send + Sync {
    fn now_millis(&self) -> i64;
}

pub trait Rng: Send + Sync {
    fn next_u128(&self) -> u128;
}

#[derive(Clone)]
pub struct Env {
    pub storage: Arc<dyn Storage>,
    pub clock: Arc<dyn Clock>,
    pub rng: Arc<dyn Rng>,
}

impl Env {
    pub fn real() -> Self {
        Self {
            storage: Arc::new(RealStorage),
            clock: Arc::new(RealClock),
            rng: Arc::new(RealRng),
        }
    }
}

pub struct RealStorage;

struct RealStorageFile(File);

impl Read for RealStorageFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

impl Write for RealStorageFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for RealStorageFile {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}

impl StorageFile for RealStorageFile {
    fn sync_data(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn len(&self) -> io::Result<u64> {
        self.0.metadata().map(|metadata| metadata.len())
    }
}

impl Storage for RealStorage {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Box<dyn StorageFile>> {
        let mut options = OpenOptions::new();
        match mode {
            OpenMode::Read => options.read(true),
            OpenMode::ReadWrite => options.read(true).write(true),
            OpenMode::CreateNew => options.read(true).write(true).create_new(true),
            OpenMode::CreateAppend => options.read(true).write(true).create(true).truncate(false),
        };
        Ok(Box::new(RealStorageFile(options.open(path)?)))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        Ok(path.exists())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntryInfo>> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            entries.push(DirEntryInfo {
                path: entry.path(),
                is_regular_file: entry.file_type()?.is_file(),
            });
        }
        Ok(entries)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }
}

pub struct RealClock;

impl Clock for RealClock {
    fn now_millis(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
            Err(before) => -i64::try_from(before.duration().as_millis()).unwrap_or(i64::MAX),
        }
    }
}

/// Backs atomic-write temp names, which must keep the exact
/// `.{name}.{uuid}.tmp` shape that startup classification pattern-matches.
pub struct RealRng;

impl Rng for RealRng {
    fn next_u128(&self) -> u128 {
        Uuid::new_v4().as_u128()
    }
}
