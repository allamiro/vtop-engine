//! Test support for VTOP's live S3 tests (#722). Not published.
//!
//! The same live test binaries run against every S3 server; which one is
//! named by `VTOP_TEST_S3_SERVER` (D1). That server's capability file,
//! `tests/s3-compat/servers/<id>.yaml`, says for every capability what the
//! server is expected to do (D2). A live case names the capabilities it
//! needs, and [`LiveS3::case`] runs its positive body, its refusal body or
//! nothing, judges the outcome against the file, and appends one results
//! line to `VTOP_TEST_S3_RESULTS` (D6). An unexpected pass and an unexpected
//! fail both fail the test, naming the file to edit (D4).
//!
//! ```no_run
//! # async fn example() {
//! use vtop_s3_testkit::{Capability, LiveS3};
//! let live = LiveS3::from_env();
//! live.case("verification.store_checksum", &[Capability::PutGetHead], || async {
//!     // ... the test body, against live.settings ...
//! })
//! .await;
//! # }
//! ```

mod capability;
mod case;
mod server;
mod settings;

pub use capability::{Capability, Expectation, RefusalAt};
pub use case::{
    record_method, LiveS3, Outcome, Recorder, ResultLine, COMMIT_ENV, IMAGE_DIGEST_ENV,
    RESULTS_ENV, SERVER_VERSION_ENV,
};
pub use server::{servers_dir, Server, DEFAULT_SERVER, SERVERS_DIR_ENV, SERVER_ENV};
pub use settings::{
    S3Settings, BUCKET_ENV, ENDPOINT_ENV, LOCKED_BUCKET_ENV, PATH_STYLE_ENV, REGION_ENV,
    VERIFY_TLS_ENV,
};
