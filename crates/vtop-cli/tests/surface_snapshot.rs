//! The `vtopctl` command tree and the engine's metric families, against the
//! committed snapshots in `docs/surface/` (#719).
//!
//! Both are generated from the code: the command tree from clap's model of
//! `vtopctl`, the families from what the engine registers. A difference
//! fails with the lines on each side and the command that regenerates the
//! snapshot; `scripts/check-surface-deprecations.py` then asks a pull request
//! that removes a line for its `docs/deprecations.md` entry.

use clap::CommandFactory;
use std::path::PathBuf;

fn snapshot(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/surface")
        .join(name)
}

#[test]
fn the_vtopctl_command_tree_matches_its_snapshot() {
    let lines = vtop_surface::command_lines(&vtop_cli::commands::Cli::command());
    if let Err(message) = vtop_surface::check(&snapshot("vtopctl.txt"), lines) {
        panic!("{message}");
    }
}

/// Everything `vtopctl run` serves on `/metrics` (and exports over OTLP),
/// registered the way the engine registers it.
#[test]
fn the_engine_metric_families_match_their_snapshot() {
    let metrics = vtop_cli::metrics_server::init_registry().expect("the engine registry");
    let families = metrics
        .registry
        .families()
        .unwrap_or_else(|e| panic!("{e}"));
    let lines = families.iter().map(vtop_surface::Family::line);
    if let Err(message) = vtop_surface::check(&snapshot("metrics-engine.txt"), lines) {
        panic!("{message}");
    }
}
