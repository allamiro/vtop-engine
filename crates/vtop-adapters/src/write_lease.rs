//! Proof that no other process holds a file open, for `delete_after_commit`
//! (#619).
//!
//! WHY. Most appenders open a log once with `O_APPEND` and keep the
//! descriptor. Unlinking a file such a writer still holds sends every later
//! line into an inode no path names. #631 made the delete wait until the file
//! had stayed unwritten for a quiet period, which is a guess: a writer silent
//! for longer than the period, or one whose write lands between the delete's
//! last look at the file and the unlink, still lost its lines.
//!
//! HOW. A Linux write lease (`fcntl(F_SETLEASE, F_WRLCK)`) is granted only
//! when the caller's open file description is the ONLY one on the inode - no
//! other process, and no other descriptor of this one, has it open for
//! reading or writing, mapped included. While it is held, any `open` of the
//! inode, by any process, starts a lease break: the opener waits, and
//! `F_GETLEASE` stops answering `F_WRLCK`. So a granted lease proves nobody
//! holds the file, and [`WriteLease::intact`] proves nobody opened it since.
//! Leases are per inode in the kernel, so this sees a writer in another
//! container sharing the volume, which a scan of `/proc/*/fd` cannot.
//!
//! ONLY ON A LOCAL FILESYSTEM. A lease counts the openers THIS kernel sees.
//! On a network or FUSE filesystem (NFS, CIFS, CephFS, GlusterFS, virtiofs,
//! 9p, sshfs) a writer on another machine, or on the host behind a VM's
//! shared folder, is invisible to it - and kernels before leases became
//! opt-in per filesystem grant one there anyway (review of #689). So the
//! filesystem type is checked first, against a list of local ones
//! ([`LOCAL_FILESYSTEMS`]), and anything else answers [`Sole::Unknown`]
//! without asking for a lease.
//!
//! WHERE IT CANNOT. Elsewhere there is no such check: other platforms, a
//! filesystem not on that list, a file whose owner is not the engine's user
//! when the engine lacks an effective `CAP_LEASE` (`EACCES`), or
//! `fs.leases-enable = 0` (`EINVAL`). Those answer [`Sole::Unknown`], and the
//! caller refuses the delete unless the operator chose the quiet period alone
//! (`delete_without_open_check`). Any other error answers [`Sole::Failed`]:
//! not a verdict, so the delete is tried again later.
//!
//! UNSAFE. The only `unsafe` in this crate is here: `fcntl` with integer
//! arguments on a descriptor the borrowed `File` keeps open for the call. No
//! memory is passed or returned, so there is nothing for the kernel to read
//! or write on our side.

use std::fs::File;

/// What [`take`] found out about the other open file descriptions of a file.
// Off Linux only `Unknown` is ever answered.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) enum Sole<'a> {
    /// None exists: the lease is granted. Held until dropped.
    Proven(WriteLease<'a>),
    /// Another one exists: some process - possibly this one - has the file
    /// open.
    Open,
    /// There is no way to tell here; the reason, for the log.
    Unknown(String),
    /// The check itself failed for a reason that may pass (`ENOMEM`,
    /// `ENOLCK`, a failed `fstatfs`): no verdict either way.
    Failed(std::io::Error),
}

/// Filesystems on which a write lease sees every opener of a file: local
/// ones, by `statfs` `f_type` (`linux/magic.h`). An overlay counts: it is
/// the container's own root, and its lower and upper layers are local
/// directories.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const LOCAL_FILESYSTEMS: &[(u32, &str)] = &[
    (0xEF53, "ext2/ext3/ext4"),
    (0x5846_5342, "xfs"),
    (0x9123_683E, "btrfs"),
    (0x0102_1994, "tmpfs"),
    (0x794C_7630, "overlay"),
    (0xF2F5_2010, "f2fs"),
    (0x2FC1_2FC1, "zfs"),
    (0xCA45_1A4E, "bcachefs"),
];

/// Whether a filesystem of type `magic` is one a lease can be trusted on.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn is_local(magic: u32) -> bool {
    LOCAL_FILESYSTEMS.iter().any(|(m, _)| *m == magic)
}

/// A Linux write lease on a file, released when dropped.
pub(crate) struct WriteLease<'a> {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    file: &'a File,
}

#[cfg(target_os = "linux")]
mod imp {
    use super::{Sole, WriteLease};
    use std::fs::File;
    use std::os::fd::AsRawFd;

    /// `F_SETSIG` from `asm-generic/fcntl.h`, the value on every Linux
    /// architecture Rust builds std for; the `libc` crate does not export it
    /// for glibc targets.
    const F_SETSIG: libc::c_int = 10;

    fn fcntl(file: &File, cmd: libc::c_int, arg: libc::c_int) -> std::io::Result<libc::c_int> {
        // SAFETY: `file` is borrowed for the call, so its descriptor is open;
        // every command used here (F_SETSIG, F_SETLEASE, F_GETLEASE,
        // F_SETOWN) takes an integer argument, and no pointer is passed.
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), cmd, arg) };
        if ret == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(ret)
        }
    }

    pub(super) fn take(file: &File) -> Sole<'_> {
        let magic = match rustix::fs::fstatfs(file) {
            // `f_type` is a signed long on 64-bit targets and a u32 on some
            // others; the magic numbers are 32-bit either way.
            Ok(fs) => fs.f_type as u64 as u32,
            Err(error) => return Sole::Failed(error.into()),
        };
        if !super::is_local(magic) {
            return Sole::Unknown(format!(
                "the file is on a filesystem of type {magic:#x}, not a local one (ext2/3/4, \
                 xfs, btrfs, tmpfs, overlay, f2fs, zfs, bcachefs): a lease there cannot see \
                 a writer on another machine or on the host of a shared folder"
            ));
        }
        // A lease break signals the holder, and the default signal, SIGIO,
        // terminates the process. SIGURG's default action is to ignore it,
        // and nothing in the engine handles it, so a break can never stop the
        // engine. Set before the lease, since a break can start the moment
        // it is granted.
        if let Err(error) = fcntl(file, F_SETSIG, libc::SIGURG) {
            return Sole::Unknown(format!("the lease signal could not be set: {error}"));
        }
        match fcntl(file, libc::F_SETLEASE, libc::F_WRLCK) {
            Ok(_) => {}
            Err(error)
                if matches!(error.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EBUSY)) =>
            {
                return Sole::Open
            }
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EACCES)
                        | Some(libc::EPERM)
                        | Some(libc::EINVAL)
                        | Some(libc::ENOSYS)
                        | Some(libc::EOPNOTSUPP)
                ) =>
            {
                return Sole::Unknown(format!(
                    "a write lease, the engine's check that no process holds the file open, \
                     was refused ({error}): it needs the engine to own the file or to have \
                     CAP_LEASE in its effective set, and fs.leases-enable on"
                ))
            }
            Err(error) => return Sole::Failed(error),
        }
        // And no signal at all: with no owner, a break sends none. The lease
        // state, which is what the delete reads, is unaffected.
        let _ = fcntl(file, libc::F_SETOWN, 0);
        Sole::Proven(WriteLease { file })
    }

    pub(super) fn intact(lease: &WriteLease<'_>) -> bool {
        // During a break this answers the type the lease is being broken to
        // (F_UNLCK for an opener that writes, F_RDLCK for one that reads),
        // and F_UNLCK once the kernel has revoked it.
        fcntl(lease.file, libc::F_GETLEASE, 0).is_ok_and(|kind| kind == libc::F_WRLCK)
    }

    pub(super) fn release(lease: &WriteLease<'_>) {
        // Closing the descriptor releases it too; this lets a waiting opener
        // go as soon as the delete is decided, and covers a descriptor that
        // outlives the lease (a duplicate of one the adapter keeps).
        let _ = fcntl(lease.file, libc::F_SETLEASE, libc::F_UNLCK);
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{Sole, WriteLease};
    use std::fs::File;

    pub(super) fn take(_file: &File) -> Sole<'_> {
        Sole::Unknown(
            "this platform has no check that no process holds a file open (the engine \
             uses Linux write leases)"
                .to_owned(),
        )
    }

    pub(super) fn intact(_lease: &WriteLease<'_>) -> bool {
        false
    }

    pub(super) fn release(_lease: &WriteLease<'_>) {}
}

/// Take a write lease on `file`: proof, while it is held, that no other open
/// file description of the file exists.
pub(crate) fn take(file: &File) -> Sole<'_> {
    imp::take(file)
}

impl WriteLease<'_> {
    /// Whether the lease still stands: no process has opened the file since
    /// it was granted.
    pub(crate) fn intact(&self) -> bool {
        imp::intact(self)
    }
}

impl Drop for WriteLease<'_> {
    fn drop(&mut self) {
        imp::release(self);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Write;

    /// Review of #689: a lease is trusted only on a local filesystem.
    #[test]
    fn only_local_filesystems_are_trusted() {
        for magic in [0xEF53, 0x5846_5342, 0x0102_1994, 0x794C_7630] {
            assert!(is_local(magic), "{magic:#x}");
        }
        // nfs, cifs, fuse (sshfs, virtiofs on older kernels), 9p, ceph.
        for magic in [0x6969, 0xFF53_4D42, 0x6573_5546, 0x0102_1997, 0x00C3_6400] {
            assert!(!is_local(magic), "{magic:#x}");
        }
    }

    fn leased(file: &File) -> Option<WriteLease<'_>> {
        match take(file) {
            Sole::Proven(lease) => Some(lease),
            Sole::Open => None,
            Sole::Unknown(why) => panic!("no lease check on the test's filesystem: {why}"),
            Sole::Failed(error) => panic!("the lease check failed: {error}"),
        }
    }

    /// The lease is granted only when the asking descriptor is the file's
    /// only one, and an open while it is held is seen.
    #[test]
    fn a_lease_proves_no_one_else_has_the_file_open_and_sees_anyone_who_opens_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, "x").unwrap();
        let mine = File::open(&path).unwrap();

        let writer = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        assert!(
            leased(&mine).is_none(),
            "granted while a writer had it open"
        );
        drop(writer);
        let reader = File::open(&path).unwrap();
        assert!(
            leased(&mine).is_none(),
            "granted while a reader had it open"
        );
        drop(reader);

        let lease = leased(&mine).expect("refused with no other descriptor open");
        assert!(lease.intact());
        let opener = {
            let path = path.clone();
            std::thread::spawn(move || {
                // Waits for the lease to be released.
                let mut w = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap();
                w.write_all(b"y").unwrap();
            })
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while lease.intact() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            !lease.intact(),
            "an open of the file did not break the lease"
        );
        drop(lease);
        opener.join().unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "xy");
    }
}
