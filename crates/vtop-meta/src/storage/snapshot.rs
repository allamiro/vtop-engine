//! Durable state-machine snapshots with atomic publication.
//!
//! A snapshot file is `snapshot-{last_index:020}-{last_term:020}.vmsnap`:
//! a checksummed header (cluster, shard, coverage, membership, snapshot id),
//! the state-machine payload from [`crate::state`], and a BLAKE3 trailer over
//! everything. Publication is tmp + sync + rename + dir sync, and the
//! install path verifies the trailer *before* the rename, so a published
//! snapshot is always completely valid — recovery treats anything else as
//! corruption, never as a state to limp along with.

use super::{
    codec_corrupt, corrupt, io_error, is_atomic_temp_name, write_atomic, MetaStoreError,
    MetaStoreResult,
};
use crate::storage::log::MetaMembership;
use crate::wire::{put_u16, put_u32, put_u64, Reader};
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::env::Env;

pub(crate) const SNAPSHOT_MAGIC: &[u8; 8] = b"VTOPMSN1";
const SNAPSHOT_VERSION: u16 = 1;
const CHECKSUM_LEN: usize = 32;
const MAX_SNAPSHOT_ID_BYTES: usize = 64;
const MAX_MEMBERSHIP_BLOCK_BYTES: usize = 64 * 1024;
/// Explicit ceiling so a corrupt length field can never allocate the moon.
const MAX_SNAPSHOT_PAYLOAD_BYTES: u64 = 256 * 1024 * 1024;
/// How many published snapshots survive a successful publication.
const KEEP_NEWEST: usize = 2;

/// Identity and coverage of one published snapshot file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub last_index: u64,
    pub last_term: u64,
    pub membership: MetaMembership,
    pub snapshot_id: String,
    pub path: PathBuf,
}

fn snapshot_file_name(last_index: u64, last_term: u64) -> String {
    format!("snapshot-{last_index:020}-{last_term:020}.vmsnap")
}

fn parse_snapshot_file_name(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_prefix("snapshot-")?.strip_suffix(".vmsnap")?;
    let (index_digits, term_digits) = stem.split_once('-')?;
    for digits in [index_digits, term_digits] {
        if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    Some((index_digits.parse().ok()?, term_digits.parse().ok()?))
}

pub(crate) fn encode_snapshot_file(
    cluster_id: Uuid,
    last_index: u64,
    last_term: u64,
    membership: &MetaMembership,
    snapshot_id: &str,
    payload: &[u8],
) -> MetaStoreResult<Vec<u8>> {
    let invalid = |reason: String| MetaStoreError::InvalidConfig(reason);
    if snapshot_id.is_empty() || snapshot_id.len() > MAX_SNAPSHOT_ID_BYTES {
        return Err(invalid(format!(
            "snapshot id must be 1..={MAX_SNAPSHOT_ID_BYTES} bytes, got {}",
            snapshot_id.len()
        )));
    }
    let membership_bytes = membership
        .encode()
        .map_err(|error| invalid(format!("cannot encode membership: {error}")))?;
    if membership_bytes.len() > MAX_MEMBERSHIP_BLOCK_BYTES {
        return Err(invalid("membership block exceeds its bound".to_owned()));
    }
    if payload.len() as u64 > MAX_SNAPSHOT_PAYLOAD_BYTES {
        return Err(invalid("snapshot payload exceeds its bound".to_owned()));
    }
    let mut out = Vec::with_capacity(128 + membership_bytes.len() + payload.len());
    out.extend_from_slice(SNAPSHOT_MAGIC);
    put_u16(&mut out, SNAPSHOT_VERSION);
    out.extend_from_slice(cluster_id.as_bytes());
    put_u16(&mut out, crate::keys::META_SHARD_ID);
    put_u64(&mut out, last_index);
    put_u64(&mut out, last_term);
    put_u32(&mut out, membership_bytes.len() as u32);
    out.extend_from_slice(&membership_bytes);
    put_u16(&mut out, snapshot_id.len() as u16);
    out.extend_from_slice(snapshot_id.as_bytes());
    put_u64(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    Ok(out)
}

/// Fully validate snapshot bytes: trailer checksum first, then structure,
/// bounds, and trailing-byte rejection.
pub(crate) fn decode_snapshot_file(
    path: &Path,
    cluster_id: Uuid,
    bytes: &[u8],
) -> MetaStoreResult<(SnapshotMeta, Vec<u8>)> {
    if bytes.len() < CHECKSUM_LEN {
        return Err(corrupt(path, "snapshot is shorter than its checksum"));
    }
    let (body, stored_checksum) = bytes.split_at(bytes.len() - CHECKSUM_LEN);
    if blake3::hash(body).as_bytes() != stored_checksum {
        return Err(corrupt(path, "snapshot trailer checksum mismatch"));
    }
    let mut reader = Reader::new(body);
    let magic = reader
        .take(8, "snapshot magic")
        .map_err(|error| codec_corrupt(path, &error))?;
    if magic != SNAPSHOT_MAGIC {
        return Err(corrupt(path, "snapshot magic mismatch"));
    }
    let version = reader
        .u16("snapshot version")
        .map_err(|error| codec_corrupt(path, &error))?;
    if version != SNAPSHOT_VERSION {
        return Err(MetaStoreError::UnsupportedVersion {
            path: path.to_path_buf(),
            version,
        });
    }
    let stored_cluster = reader
        .uuid("snapshot cluster id")
        .map_err(|error| codec_corrupt(path, &error))?;
    if stored_cluster != cluster_id {
        return Err(corrupt(
            path,
            format!("snapshot belongs to cluster {stored_cluster}, not {cluster_id}"),
        ));
    }
    let shard = reader
        .u16("snapshot shard id")
        .map_err(|error| codec_corrupt(path, &error))?;
    if shard != crate::keys::META_SHARD_ID {
        return Err(corrupt(
            path,
            format!("snapshot belongs to foreign shard {shard}"),
        ));
    }
    let last_index = reader
        .u64("snapshot last index")
        .map_err(|error| codec_corrupt(path, &error))?;
    let last_term = reader
        .u64("snapshot last term")
        .map_err(|error| codec_corrupt(path, &error))?;
    let membership_len = reader
        .u32("membership block length")
        .map_err(|error| codec_corrupt(path, &error))? as usize;
    if membership_len > MAX_MEMBERSHIP_BLOCK_BYTES {
        return Err(corrupt(path, "membership block exceeds its bound"));
    }
    let membership_bytes = reader
        .take(membership_len, "membership block")
        .map_err(|error| codec_corrupt(path, &error))?;
    let membership =
        MetaMembership::decode(membership_bytes).map_err(|error| codec_corrupt(path, &error))?;
    let snapshot_id = reader
        .bounded_str(MAX_SNAPSHOT_ID_BYTES, "snapshot id")
        .map_err(|error| codec_corrupt(path, &error))?;
    if snapshot_id.is_empty() {
        return Err(corrupt(path, "snapshot id must not be empty"));
    }
    let payload_len = reader
        .u64("snapshot payload length")
        .map_err(|error| codec_corrupt(path, &error))?;
    if payload_len > MAX_SNAPSHOT_PAYLOAD_BYTES {
        return Err(corrupt(path, "snapshot payload exceeds its bound"));
    }
    let payload = reader
        .take(payload_len as usize, "snapshot payload")
        .map_err(|error| codec_corrupt(path, &error))?
        .to_vec();
    reader
        .finish()
        .map_err(|error| codec_corrupt(path, &error))?;
    Ok((
        SnapshotMeta {
            last_index,
            last_term,
            membership,
            snapshot_id,
            path: path.to_path_buf(),
        },
        payload,
    ))
}

/// Manager of the published snapshot set for one directory.
pub struct MetaSnapshots {
    env: Env,
    dir: PathBuf,
    cluster_id: Uuid,
    /// Published snapshots, ascending by (last_index, last_term).
    published: Vec<SnapshotMeta>,
}

impl MetaSnapshots {
    /// Scan the directory and fully validate every published snapshot.
    /// In-flight atomic temporaries are ignored; a published file that fails
    /// validation is an error, because the write and install paths both make
    /// full validity a precondition of publication.
    pub fn open_in(env: &Env, dir: impl AsRef<Path>, cluster_id: Uuid) -> MetaStoreResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        let mut published = Vec::new();
        for entry in env
            .storage
            .read_dir(&dir)
            .map_err(|source| io_error(&dir, source))?
        {
            if !entry.is_regular_file {
                continue;
            }
            let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if is_atomic_temp_name(name) {
                continue;
            }
            let Some((name_index, name_term)) = parse_snapshot_file_name(name) else {
                continue;
            };
            let bytes = env
                .storage
                .read(&entry.path)
                .map_err(|source| io_error(&entry.path, source))?;
            let (meta, _payload) = decode_snapshot_file(&entry.path, cluster_id, &bytes)?;
            if (meta.last_index, meta.last_term) != (name_index, name_term) {
                return Err(corrupt(
                    &entry.path,
                    format!(
                        "snapshot header covers ({}, {}) but the file name says \
                         ({name_index}, {name_term})",
                        meta.last_index, meta.last_term
                    ),
                ));
            }
            published.push(meta);
        }
        published.sort_by_key(|meta| (meta.last_index, meta.last_term));
        Ok(Self {
            env: env.clone(),
            dir,
            cluster_id,
            published,
        })
    }

    pub fn newest(&self) -> Option<SnapshotMeta> {
        self.published.last().cloned()
    }

    pub fn published_count(&self) -> usize {
        self.published.len()
    }

    /// Write and atomically publish a new snapshot, then retire everything
    /// but the newest [`KEEP_NEWEST`] published files.
    pub fn write(
        &mut self,
        last_index: u64,
        last_term: u64,
        membership: MetaMembership,
        snapshot_id: &str,
        payload: &[u8],
    ) -> MetaStoreResult<SnapshotMeta> {
        let bytes = encode_snapshot_file(
            self.cluster_id,
            last_index,
            last_term,
            &membership,
            snapshot_id,
            payload,
        )?;
        self.publish(last_index, last_term, membership, snapshot_id, &bytes)
    }

    /// Install snapshot bytes received from elsewhere (a leader, in PR 3).
    /// The trailer checksum and full structure are verified *before* the
    /// bytes are renamed into place; nothing invalid can ever be published.
    pub fn install(&mut self, bytes: &[u8]) -> MetaStoreResult<SnapshotMeta> {
        let probe = self.dir.join("snapshot-install.vmsnap");
        let (meta, _payload) = decode_snapshot_file(&probe, self.cluster_id, bytes)?;
        self.publish(
            meta.last_index,
            meta.last_term,
            meta.membership,
            &meta.snapshot_id,
            bytes,
        )
    }

    fn publish(
        &mut self,
        last_index: u64,
        last_term: u64,
        membership: MetaMembership,
        snapshot_id: &str,
        bytes: &[u8],
    ) -> MetaStoreResult<SnapshotMeta> {
        let path = self.dir.join(snapshot_file_name(last_index, last_term));
        write_atomic(&self.env, &path, bytes)?;
        let meta = SnapshotMeta {
            last_index,
            last_term,
            membership,
            snapshot_id: snapshot_id.to_owned(),
            path: path.clone(),
        };
        self.published.retain(|existing| existing.path != path);
        self.published.push(meta.clone());
        self.published
            .sort_by_key(|meta| (meta.last_index, meta.last_term));
        self.retire_old()?;
        Ok(meta)
    }

    fn retire_old(&mut self) -> MetaStoreResult<()> {
        let mut removed_any = false;
        while self.published.len() > KEEP_NEWEST {
            let old = self.published.remove(0);
            self.env
                .storage
                .remove_file(&old.path)
                .map_err(|source| io_error(&old.path, source))?;
            removed_any = true;
        }
        if removed_any {
            self.env
                .storage
                .sync_dir(&self.dir)
                .map_err(|source| io_error(&self.dir, source))?;
        }
        Ok(())
    }

    /// Re-read and re-validate a published snapshot, returning its payload.
    pub fn read(&self, meta: &SnapshotMeta) -> MetaStoreResult<Vec<u8>> {
        let bytes = self
            .env
            .storage
            .read(&meta.path)
            .map_err(|source| io_error(&meta.path, source))?;
        let (stored, payload) = decode_snapshot_file(&meta.path, self.cluster_id, &bytes)?;
        if (stored.last_index, stored.last_term) != (meta.last_index, meta.last_term) {
            return Err(corrupt(
                &meta.path,
                "snapshot coverage changed between scan and read",
            ));
        }
        Ok(payload)
    }
}
