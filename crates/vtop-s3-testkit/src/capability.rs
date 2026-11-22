//! The capabilities a live S3 test can need, and what a server is expected
//! to do for each (#722, decisions D2 and D5).

use std::fmt;
use std::str::FromStr;

/// One thing VTOP relies on an S3 store to do. A live case names the ones it
/// needs; a server's file says, for each, what that server does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Capability {
    /// PUT, GET, HEAD and DELETE of a plain object.
    PutGetHead,
    /// Path-style addressing (`http://host/bucket/key`), every lab server's.
    PathStyle,
    /// Virtual-host addressing (`http://bucket.host/key`), AWS's.
    VirtualHost,
    /// A PUT carrying a WRONG `x-amz-checksum-sha256` is rejected.
    ChecksumSha256Enforced,
    /// `HeadObject` with checksum mode returns the SHA-256, so verification
    /// is `store_checksum`; without it, `read_back`.
    ChecksumSha256Returned,
    /// The SDK's default CRC32 on PUT is accepted.
    ChecksumCrc32Default,
    /// CreateMultipartUpload, UploadPart, CompleteMultipartUpload.
    Multipart,
    /// AbortMultipartUpload.
    MultipartAbort,
    /// ListMultipartUploads (the tests use it to prove an abort).
    MultipartListUploads,
    /// A multipart upload resumed from a persisted session.
    MultipartResume,
    /// Version ids on PUT, HEAD and GET by `versionId`.
    Versioning,
    /// Object Lock: the configuration read, retention and legal hold written
    /// and read back.
    ObjectLock,
    /// A locked version cannot be deleted.
    ObjectLockEnforced,
    /// A governance retention can be cleared by a principal that bypasses
    /// governance (`PutObjectRetention` with an empty retention and the
    /// bypass header). VTOP never does this; the live tests do, to tamper
    /// with a receipt and see the scrub report it.
    ObjectLockRetentionClear,
    /// `If-None-Match: *` on PUT. Informational: VTOP does not use it (D14).
    ConditionalPut,
}

impl Capability {
    /// Every capability, in the order a server's file and the report list
    /// them.
    pub const ALL: [Capability; 15] = [
        Capability::PutGetHead,
        Capability::PathStyle,
        Capability::VirtualHost,
        Capability::ChecksumSha256Enforced,
        Capability::ChecksumSha256Returned,
        Capability::ChecksumCrc32Default,
        Capability::Multipart,
        Capability::MultipartAbort,
        Capability::MultipartListUploads,
        Capability::MultipartResume,
        Capability::Versioning,
        Capability::ObjectLock,
        Capability::ObjectLockEnforced,
        Capability::ObjectLockRetentionClear,
        Capability::ConditionalPut,
    ];

    /// The id a server's file and a results line use.
    pub fn as_str(self) -> &'static str {
        match self {
            Capability::PutGetHead => "put_get_head",
            Capability::PathStyle => "path_style",
            Capability::VirtualHost => "virtual_host",
            Capability::ChecksumSha256Enforced => "checksum_sha256_enforced",
            Capability::ChecksumSha256Returned => "checksum_sha256_returned",
            Capability::ChecksumCrc32Default => "checksum_crc32_default",
            Capability::Multipart => "multipart",
            Capability::MultipartAbort => "multipart_abort",
            Capability::MultipartListUploads => "multipart_list_uploads",
            Capability::MultipartResume => "multipart_resume",
            Capability::Versioning => "versioning",
            Capability::ObjectLock => "object_lock",
            Capability::ObjectLockEnforced => "object_lock_enforced",
            Capability::ObjectLockRetentionClear => "object_lock_retention_clear",
            Capability::ConditionalPut => "conditional_put",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Capability {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Capability::ALL
            .into_iter()
            .find(|c| c.as_str() == s)
            .ok_or_else(|| {
                format!(
                    "unknown capability {s:?} (expected one of {})",
                    Capability::ALL.map(Capability::as_str).join(", ")
                )
            })
    }
}

/// When VTOP refuses a capability the server lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalAt {
    /// Engine construction fails, before anything is read or written.
    Startup,
    /// The first batch fails and the source position does not advance.
    FirstBatch,
}

impl RefusalAt {
    pub fn as_str(self) -> &'static str {
        match self {
            RefusalAt::Startup => "startup",
            RefusalAt::FirstBatch => "first_batch",
        }
    }
}

/// What a server is expected to do for one capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expectation {
    /// The positive body passes.
    Pass,
    /// The server lacks it, and VTOP refuses at the stated moment: the
    /// refusal body runs and must pass.
    Unsupported { refusal: RefusalAt },
    /// A known failure, tracked by an issue: the positive body runs and must
    /// fail (a pass is an "unexpected pass").
    Xfail { issue: u32 },
    /// Not yet known (discovery only): the positive body runs and its
    /// outcome is recorded, never judged. Refused on a gating server.
    Unknown,
    /// The capability does not apply to this server, or has no refusal path:
    /// nothing runs, and the result says so. Never a silent pass.
    NotApplicable,
}

impl Expectation {
    /// How much the state constrains a case that needs it: the most
    /// constraining capability decides the case.
    pub(crate) fn rank(self) -> u8 {
        match self {
            Expectation::NotApplicable => 4,
            Expectation::Unsupported { .. } => 3,
            Expectation::Xfail { .. } => 2,
            Expectation::Unknown => 1,
            Expectation::Pass => 0,
        }
    }
}

impl fmt::Display for Expectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expectation::Pass => f.write_str("pass"),
            Expectation::Unsupported { refusal } => {
                write!(f, "unsupported (refused at {})", refusal.as_str())
            }
            Expectation::Xfail { issue } => write!(f, "xfail (#{issue})"),
            Expectation::Unknown => f.write_str("unknown"),
            Expectation::NotApplicable => f.write_str("not_applicable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_capability_round_trips_through_its_id() {
        for c in Capability::ALL {
            assert_eq!(c.as_str().parse::<Capability>().unwrap(), c);
        }
        let err = "object_locks".parse::<Capability>().unwrap_err();
        assert!(err.contains("object_lock,"), "{err}");
    }
}
