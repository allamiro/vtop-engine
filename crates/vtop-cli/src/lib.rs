//! # vtop-cli
//!
//! The VTOP Engine runtime and the `vtopctl` command-line interface. The engine
//! logic lives in [`engine`] so it can be exercised directly by integration
//! tests; [`commands`] wires it to the CLI.

pub mod commands;
pub mod coverage;
pub mod engine;
pub mod meta_tools;
pub mod metrics_server;
pub mod node_tools;
#[cfg(feature = "otel")]
pub mod otel;
pub mod scrub;
pub mod segment_tools;
mod spans;
pub mod testkit;
pub mod tier_tools;
mod verification_mode;

pub use engine::{BatchOutcome, Engine, Pipeline, RecoverySummary};
