//! # vtop-cli
//!
//! The VTOP Engine runtime and the `vtopctl` command-line interface. The engine
//! logic lives in [`engine`] so it can be exercised directly by integration
//! tests; [`commands`] wires it to the CLI.

pub mod commands;
pub mod engine;
pub mod metrics_server;
pub mod testkit;

pub use engine::{BatchOutcome, Engine, Pipeline, RecoverySummary};
