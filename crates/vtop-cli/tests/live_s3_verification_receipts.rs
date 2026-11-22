//! #671 release B against a real S3-compatible store: the method a
//! verification receipt names is the one the native S3 backend ran.
//!
//! The configured method for `s3_native` with SHA-256 is `store_checksum`,
//! and the 0.4 manifest's `verification_plan` says so. A single PUT gets a
//! whole-object SHA-256 from the service, so the receipt says
//! `store_checksum`; a multipart object gets none, the backend reads the
//! object back (#482), and the receipt says `read_back`. The mock reproduces
//! this; this test shows the real backend against a real store does too, and
//! that `vtopctl scrub` with receipts required passes the archive.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-cli
//!  --test live_s3_verification_receipts -- --ignored --test-threads=1`
//! It deletes every object it writes. CI runs it in the `s3-compat`
//! job. `VTOP_TEST_S3_SERVER` names the server and its capability file
//! (#722).
#![cfg(unix)]

use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use vtop_cli::scrub::{ReceiptResult, ScrubOptions};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::manifest::{ManifestWriteVersion, ParsedManifest, ParsedReceipt};
use vtop_core::partitioning::receipt_uri;
use vtop_core::types::{CompressionType, SourceType};
use vtop_s3_testkit::{record_method, Capability, LiveS3, S3Settings};

fn config(s: &S3Settings, dir: &Path, prefix: &str, bytes: usize) -> VtopConfig {
    let input = dir.join("app.log");
    let mut f = std::fs::File::create(&input).unwrap();
    let line = "receipt record payload abcdefghijklmnopqrstuvwxyz0123456789\n";
    for _ in 0..bytes.div_ceil(line.len()) {
        f.write_all(line.as_bytes()).unwrap();
    }
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "s3_native",
    );
    cfg.upload.bucket = s.bucket.clone();
    cfg.upload.prefix = prefix.into();
    cfg.upload.endpoint_url = s.endpoint.clone();
    cfg.upload.region = s.region.clone();
    cfg.upload.force_path_style = s.force_path_style;
    cfg.upload.verify_tls = s.verify_tls;
    cfg.upload.require_strong_verification = true;
    cfg.compression.kind = CompressionType::None;
    // One batch for the whole file (the default 10,000 records would cut a
    // 6 MiB file of short lines into several).
    cfg.batching.max_records = 1_000_000;
    // S3's smallest non-final part: a 6 MiB object is two parts.
    cfg.upload.multipart_part_size_bytes = 5 * 1024 * 1024;
    cfg.upload.multipart_threshold_bytes = 5 * 1024 * 1024;
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.manifest.verification_receipts = true;
    cfg
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_a_receipt_names_the_check_the_store_ran() {
    let live = LiveS3::from_env();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for (case, name, needs, bytes, expected) in [
        (
            "single put",
            "receipts.a_single_put_names_store_checksum",
            &[Capability::PutGetHead, Capability::ChecksumSha256Returned][..],
            4 * 1024,
            "store_checksum",
        ),
        (
            "multipart",
            "receipts.a_multipart_object_names_read_back",
            &[Capability::Multipart][..],
            6 * 1024 * 1024,
            "read_back",
        ),
    ] {
        live.case(name, needs, || async {
            let dir = tempfile::tempdir().unwrap();
            let prefix = format!("vtop-it-671-receipts-{nanos}-{}", bytes);
            let mut engine = Engine::new(
                config(&live.settings, dir.path(), &prefix, bytes),
                StreamsConfig { streams: vec![] },
            )
            .await
            .unwrap();
            let out = engine.process_once(SourceType::File).await.unwrap();
            assert_eq!(out.len(), 1, "{case}: {out:?}");
            assert!(out[0].committed, "{case}: {out:?}");
            let row = engine
                .store
                .get_batch(&out[0].batch_id)
                .await
                .unwrap()
                .unwrap();
            let manifest_uri = row.manifest_uri.clone().unwrap();
            let receipt_uri = receipt_uri(&manifest_uri).unwrap();

            let manifest = engine
                .backend
                .get_object_bounded(&manifest_uri, 1 << 20)
                .await
                .unwrap();
            let manifest = ParsedManifest::from_slice(&manifest).unwrap();
            assert_eq!(
                manifest.fields().verification_plan.as_ref().unwrap().method,
                "store_checksum",
                "{case}: the configured plan"
            );
            let receipt = engine
                .backend
                .get_object_bounded(&receipt_uri, 1 << 16)
                .await
                .unwrap();
            let receipt = ParsedReceipt::from_slice(&receipt).unwrap();
            record_method(&receipt.fields().verification.method);
            assert!(receipt.authenticate(&Default::default()).is_ok(), "{case}");
            assert_eq!(receipt.fields().verification.method, expected, "{case}");
            assert_eq!(receipt.fields().verification.class, "strong", "{case}");
            assert_eq!(
                Some(receipt.fields().manifest.sha256.as_str()),
                row.manifest_sha256.as_deref(),
                "{case}"
            );

            let rows = vtop_cli::scrub::committed_rows(
                engine.store.list_batches().await.unwrap(),
                Vec::new(),
            );
            let report = engine
                .scrub(
                    rows,
                    &ScrubOptions {
                        check_receipts: true,
                        require_receipts: true,
                        ..ScrubOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(report.passed, "{case}: {report:#?}");
            assert_eq!(
                report.receipts.get(ReceiptResult::Ok.as_str()),
                Some(&1),
                "{case}: {report:#?}"
            );

            for uri in [row.object_uri.clone().unwrap(), manifest_uri, receipt_uri] {
                engine
                    .backend
                    .delete_object(&uri)
                    .await
                    .unwrap_or_else(|e| panic!("cleaning up {uri}: {e:?}"));
            }
        })
        .await;
    }
}
