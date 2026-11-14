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
//! WHERE IT CANNOT. Elsewhere there is no such check: other platforms, file
//! systems without leases (NFS, most FUSE and 9p/virtiofs mounts, where the
//! kernel answers `EINVAL`), a file whose owner is not the engine's user when
//! the engine lacks `CAP_LEASE` (`EACCES`), or `fs.leases-enable = 0`. Those
//! answer [`Sole::Unknown`], and the caller refuses the delete unless the
//! operator chose the quiet period alone (`delete_without_open_check`).
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
            Err(error) => {
                return Sole::Unknown(format!(
                    "a write lease, the engine's check that no process holds the file open, \
                     was refused: {error}"
                ))
            }
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

    fn leased(file: &File) -> Option<WriteLease<'_>> {
        match take(file) {
            Sole::Proven(lease) => Some(lease),
            Sole::Open => None,
            Sole::Unknown(why) => panic!("no lease check on the test's filesystem: {why}"),
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
