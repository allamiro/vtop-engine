//! Server-side encryption at rest on the native S3 backend
//! (`upload.server_side_encryption`).
//!
//! Three things live here, apart from the 4,000-line backend: the headers a
//! write carries, the rule that decides whether what the store reports back
//! is what was asked for, and the key comparison that rule needs.
//!
//! SSE-C is out of scope on purpose: it needs the client to hold an object
//! key and send it on every read as well as every write, and VTOP holds no
//! such key.

use crate::base::StoredEncryption;
use aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder;
use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
use aws_sdk_s3::types::ServerSideEncryption;
use vtop_core::config::{ServerSideEncryptionConfig, ServerSideEncryptionMode};

/// The same three settings on both builders, which share no trait. UploadPart
/// and CompleteMultipartUpload carry none of them: for SSE-S3 and SSE-KMS S3
/// takes the encryption from CreateMultipartUpload.
macro_rules! with_encryption {
    ($req:expr, $sse:expr) => {{
        let sse: &ServerSideEncryptionConfig = $sse;
        match sse.mode {
            ServerSideEncryptionMode::None => $req,
            ServerSideEncryptionMode::SseS3 => {
                $req.server_side_encryption(ServerSideEncryption::Aes256)
            }
            ServerSideEncryptionMode::SseKms => {
                let mut req = $req.server_side_encryption(ServerSideEncryption::AwsKms);
                // No key id is a request for the store's default KMS key
                // (on AWS, the account's AWS-managed `aws/s3` key): the
                // header names the algorithm and nothing else.
                if let Some(key) = &sse.kms_key_id {
                    req = req.ssekms_key_id(key);
                }
                if sse.bucket_key {
                    req = req.bucket_key_enabled(true);
                }
                req
            }
        }
    }};
}

/// An object or manifest PUT with the configured encryption on it.
pub(crate) fn on_put(
    req: PutObjectFluentBuilder,
    sse: &ServerSideEncryptionConfig,
) -> PutObjectFluentBuilder {
    with_encryption!(req, sse)
}

/// A CreateMultipartUpload with the configured encryption on it. S3 fixes a
/// multipart object's encryption here, not at complete.
pub(crate) fn on_create_multipart(
    req: CreateMultipartUploadFluentBuilder,
    sse: &ServerSideEncryptionConfig,
) -> CreateMultipartUploadFluentBuilder {
    with_encryption!(req, sse)
}

/// A KMS key named by alias. S3 never reports the alias a request used, only
/// the key's ARN, so a configured alias cannot be matched against a HEAD.
pub fn is_kms_alias(configured: &str) -> bool {
    configured.starts_with("alias/") || configured.contains(":alias/")
}

/// `long` ends with `short`, at a `/` or `:` boundary: a key id is the tail of
/// its ARN (`...:key/<id>`), and a store such as MinIO reports `arn:aws:kms:<id>`.
fn tail_of(long: &str, short: &str) -> bool {
    long.strip_suffix(short)
        .is_some_and(|head| head.ends_with(['/', ':']))
}

/// Whether the key a store reports is the one that was configured. The
/// configured value may be a key id or a key ARN and the store may report
/// either, so a match is the same string, or one being the other's tail at an
/// id boundary. A prefix or a substring never matches.
pub fn kms_key_matches(configured: &str, reported: &str) -> bool {
    configured == reported || tail_of(reported, configured) || tail_of(configured, reported)
}

/// Whether what the store reports for an object is the encryption that was
/// asked for. `Err` carries the operator-facing reason, which becomes a failed
/// verification: a write that should be encrypted and is not must never be
/// committed, the same as a checksum mismatch.
///
/// - `sse-s3`: the store must report `AES256`.
/// - `sse-kms` with no `kms_key_id`: it must report `aws:kms`; which key the
///   store used is its default and is not checked.
/// - `sse-kms` with a `kms_key_id`: `aws:kms` and a reported key that matches
///   by key id or key ARN. An alias cannot be matched (see [`is_kms_alias`]),
///   so for one only the algorithm is checked.
pub fn judge(
    expected: &ServerSideEncryptionConfig,
    reported: Option<&StoredEncryption>,
) -> Result<(), String> {
    let Some(want) = expected.mode.as_s3() else {
        return Ok(());
    };
    let mode = expected.mode;
    let Some(reported) = reported else {
        return Err(format!(
            "encryption at rest missing: upload.server_side_encryption asks for {mode} \
             ({want}) but the store reports no server-side encryption on the stored object. \
             The batch is not committed and will be replayed. Check that the store, and any \
             proxy in front of it, honours x-amz-server-side-encryption"
        ));
    };
    if reported.algorithm != want {
        return Err(format!(
            "encryption at rest differs: upload.server_side_encryption asks for {mode} \
             ({want}) but the store reports {}. The batch is not committed and will be \
             replayed. Check for a bucket policy or proxy that changes the encryption",
            reported.algorithm
        ));
    }
    let Some(configured) = expected.kms_key_id.as_deref() else {
        return Ok(());
    };
    if is_kms_alias(configured) {
        return Ok(());
    }
    match reported.kms_key_id.as_deref() {
        Some(key) if kms_key_matches(configured, key) => Ok(()),
        Some(key) => Err(format!(
            "encryption key differs: upload.server_side_encryption.kms_key_id is \
             {configured} but the store reports the object encrypted under {key}. The batch \
             is not committed and will be replayed. Check which key the store applied"
        )),
        None => Err(format!(
            "encryption key unconfirmed: upload.server_side_encryption.kms_key_id is \
             {configured} but the store reports the object as {want} without naming the key. \
             The batch is not committed and will be replayed; a store that does not report the \
             key id cannot be used with a named key"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};

    fn client() -> aws_sdk_s3::Client {
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("a", "b", None, None, "test"))
                .build(),
        )
    }

    fn sse(
        mode: ServerSideEncryptionMode,
        key: Option<&str>,
        bucket_key: bool,
    ) -> ServerSideEncryptionConfig {
        ServerSideEncryptionConfig {
            mode,
            kms_key_id: key.map(str::to_owned),
            bucket_key,
        }
    }

    /// What the PutObject and CreateMultipartUpload builders carry.
    fn on_both(
        cfg: &ServerSideEncryptionConfig,
    ) -> [(Option<ServerSideEncryption>, Option<String>, Option<bool>); 2] {
        let c = client();
        let put = on_put(c.put_object().bucket("b").key("k"), cfg);
        let multi = on_create_multipart(c.create_multipart_upload().bucket("b").key("k"), cfg);
        let put = put.as_input();
        let multi = multi.as_input();
        [
            (
                put.get_server_side_encryption().clone(),
                put.get_ssekms_key_id().clone(),
                *put.get_bucket_key_enabled(),
            ),
            (
                multi.get_server_side_encryption().clone(),
                multi.get_ssekms_key_id().clone(),
                *multi.get_bucket_key_enabled(),
            ),
        ]
    }

    #[test]
    fn sse_s3_sets_aes256_and_nothing_else_on_a_put_and_a_create() {
        for got in on_both(&sse(ServerSideEncryptionMode::SseS3, None, false)) {
            assert_eq!(got, (Some(ServerSideEncryption::Aes256), None, None));
        }
    }

    #[test]
    fn sse_kms_with_a_key_sets_the_algorithm_the_key_and_the_bucket_key() {
        for got in on_both(&sse(ServerSideEncryptionMode::SseKms, Some("key-1"), true)) {
            assert_eq!(
                got,
                (
                    Some(ServerSideEncryption::AwsKms),
                    Some("key-1".to_owned()),
                    Some(true)
                )
            );
        }
    }

    /// The request for the account's AWS-managed key: aws:kms and NO key id,
    /// so the store picks its default. Sending an empty or invented id here
    /// would name a key, and S3 would refuse it or use another.
    #[test]
    fn sse_kms_with_no_key_id_sends_aws_kms_and_no_key() {
        for got in on_both(&sse(ServerSideEncryptionMode::SseKms, None, false)) {
            assert_eq!(got, (Some(ServerSideEncryption::AwsKms), None, None));
        }
    }

    #[test]
    fn none_sets_no_encryption_header() {
        for got in on_both(&sse(ServerSideEncryptionMode::None, None, false)) {
            assert_eq!(got, (None, None, None));
        }
    }

    fn stored(alg: &str, key: Option<&str>) -> Option<StoredEncryption> {
        Some(StoredEncryption {
            algorithm: alg.to_owned(),
            kms_key_id: key.map(str::to_owned),
        })
    }

    const ARN: &str = "arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
    const ID: &str = "1234abcd-12ab-34cd-56ef-1234567890ab";

    #[test]
    fn a_key_matches_by_id_or_arn_and_only_at_an_id_boundary() {
        assert!(kms_key_matches(ARN, ARN));
        assert!(kms_key_matches(ID, ARN), "configured id, reported ARN");
        assert!(kms_key_matches(ARN, ID), "configured ARN, reported id");
        assert!(
            kms_key_matches("my-key", "arn:aws:kms:my-key"),
            "MinIO's spelling"
        );
        assert!(
            !kms_key_matches("abcd", ARN),
            "a suffix inside an id is not the id"
        );
        assert!(!kms_key_matches("1234abcd", ARN), "a prefix is not the id");
        assert!(!kms_key_matches(
            ID,
            "arn:aws:kms:eu-west-1:111122223333:key/ffffffff-12ab-34cd-56ef-1234567890ab"
        ));
    }

    #[test]
    fn the_verdict_follows_the_mode_the_key_and_what_the_store_reports() {
        let s3 = sse(ServerSideEncryptionMode::SseS3, None, false);
        assert!(judge(&s3, stored("AES256", None).as_ref()).is_ok());
        let e = judge(&s3, None).unwrap_err();
        assert!(e.contains("missing") && e.contains("sse-s3"), "{e}");
        let e = judge(&s3, stored("aws:kms", Some(ARN)).as_ref()).unwrap_err();
        assert!(e.contains("differs") && e.contains("aws:kms"), "{e}");

        // No key id: any aws:kms object passes, the store's default key
        // included; the algorithm is all that is checked.
        let managed = sse(ServerSideEncryptionMode::SseKms, None, false);
        assert!(judge(&managed, stored("aws:kms", Some(ARN)).as_ref()).is_ok());
        assert!(judge(&managed, stored("aws:kms", None).as_ref()).is_ok());
        assert!(judge(&managed, None).is_err());
        assert!(judge(&managed, stored("AES256", None).as_ref()).is_err());

        let named = sse(ServerSideEncryptionMode::SseKms, Some(ID), false);
        assert!(judge(&named, stored("aws:kms", Some(ARN)).as_ref()).is_ok());
        let e = judge(
            &named,
            stored(
                "aws:kms",
                Some("arn:aws:kms:eu-west-1:111122223333:key/other"),
            )
            .as_ref(),
        )
        .unwrap_err();
        assert!(
            e.contains("key differs") && e.contains(ID) && e.contains("other"),
            "{e}"
        );
        let e = judge(&named, stored("aws:kms", None).as_ref()).unwrap_err();
        assert!(e.contains("unconfirmed"), "{e}");
        assert!(judge(&named, None).is_err());

        // An alias is never reported back, so only the algorithm is checked.
        let alias = sse(ServerSideEncryptionMode::SseKms, Some("alias/vtop"), false);
        assert!(judge(&alias, stored("aws:kms", Some(ARN)).as_ref()).is_ok());
        assert!(judge(&alias, None).is_err());

        // Nothing asked for, nothing checked.
        let none = sse(ServerSideEncryptionMode::None, None, false);
        assert!(judge(&none, None).is_ok());
    }
}
