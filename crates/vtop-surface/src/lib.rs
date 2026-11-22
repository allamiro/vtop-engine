//! The operator-facing surface of a VTOP process, as data (#719).
//!
//! Dashboards, alerts and scripts are written against metric names, their
//! label names, and the commands and flags of `vtopctl` and `vtop-node`. A
//! rename in any of them breaks those silently: the dashboard shows "no data",
//! the alert never fires, the script fails at 3 a.m. So the surface is
//! committed under `docs/surface/`, one item per line, and a test in each
//! binary's crate regenerates it from the code and fails when the two differ.
//! `scripts/check-surface-deprecations.py` then requires an entry in
//! `docs/deprecations.md` for every line a pull request removes.
//!
//! The surface is generated from the code, never from a scrape: a labelled
//! family has no series until something is counted, so a scrape of an idle
//! process leaves it out. [`Registry`] keeps every collector registered in it
//! and describes each family from its descriptor instead.

mod registry;
mod snapshot;

#[cfg(feature = "clap")]
mod commands;

#[cfg(feature = "clap")]
pub use commands::command_lines;
pub use registry::{Family, Registry};
pub use snapshot::{check, UPDATE_ENV};
