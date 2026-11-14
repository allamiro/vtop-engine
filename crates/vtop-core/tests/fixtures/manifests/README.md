# Released manifest fixtures

These files are unmodified output from the **released v0.7.0 Linux aarch64
`vtopctl` binary**, downloaded from:

https://github.com/allamiro/vtop-engine/releases/tag/v0.7.0

v0.7.0 is the newest published release (v0.8.0 was not tagged when these were
captured), so its archives are the oldest the next release must keep
reading. The asset `vtop-engine-0.7.0-aarch64-unknown-linux-gnu.tar.gz` has
SHA-256 `88ff2075dd1b2c1dcbc102df13739523dea91c50e8f244b29d1d1952350396f4`,
matching the release's published `SHA256SUMS`. That binary writes manifest
version 0.2 (`VTOP_VERSION` at the v0.7.0 tag).

Generation: run the released binary in a Linux container with
`process-once --source file --config config.yaml`, a file containing
`old release manifest fixture\n`, `compression: {type: none}`,
`checksum: {algorithm: sha256}`, and a localfs upload backend. Copy the
resulting `.manifest.json` directly, without parsing or rewriting it. Each
fixture's `object.checksum` is the SHA-256 of its input line.

The signed fixture uses the input `signed old release manifest fixture\n`
and `manifest_mac_key_env: VTOP_FIXTURE_MAC`, with the explicitly non-secret
fixture key `11` repeated 32 times (64 hexadecimal characters).
The fixture's paths, inode, timestamps and batch identifier are the values
that the released binary observed; tests must not normalize them.

`crates/vtop-core/tests/manifest_evolution.rs` verifies both files from their
stored bytes. The same files seed the `manifest_parse` fuzz corpus
(`fuzz/corpus/manifest_parse/v070-*.seed`). `scripts/upgrade-compat.sh` does
not cover manifests: its profiles open a native data directory with
`vtop-node`, not an engine archive.
