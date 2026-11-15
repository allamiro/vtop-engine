# Golden manifest projections of progress markers

`ProgressMarker::for_manifest()` is the marker a manifest carries in its
`source_progress`. The ledger keeps more than that: the content and tail
fingerprints (#578, #620), the spool's `file_size`, and the file read's
`ctime` (#619). Those fields are ledger-only, and they must never reach a
manifest. If one did, the bytes a released `verify-manifest` re-encodes would
change.

Each file here is the projection of one marker variant: one JSON line,
byte for byte as `serde_json::to_string` writes it. Each is the shape that
manifest version 0.3 carries:

- **`file-0.3.json`** has the key set and order of `source_progress` in the
  released v0.8.0 fixtures (`../manifests/v0.8.0-*.json`, written by the
  released binary). The test checks that against those files.
- **`syslog_spool-0.3.json`** and **`kafka-0.3.json`** have the fields their
  variants serialize at the v0.9.0 tag (`crates/vtop-core/src/types.rs`), with
  every ledger-only field left out. No released manifest of these sources is
  committed.

The values are made up. `crates/vtop-core/tests/marker_projection.rs` sets
every optional field of each variant, the ledger-only ones included. It
projects the marker and requires these bytes exactly.

These files are written by hand. A change to one is a change to the manifest
format, so it needs a version decision (#743), not a regenerated golden.
