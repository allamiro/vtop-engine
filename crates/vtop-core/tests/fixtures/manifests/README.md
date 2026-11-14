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

## v0.8.0 (`v0.8.0-unsigned.json`, `v0.8.0-signed.json`)

Unmodified output from the **released v0.8.0 Linux aarch64 `vtopctl`**,
generated the same way as the v0.7.0 files (localfs backend, no compression,
SHA-256, the signed one with the fixture key `11` x 32 in
`VTOP_FIXTURE_MAC`). The inputs were `v0.8.0 manifest fixture\n` and
`signed v0.8.0 manifest fixture\n`. The asset
`vtop-engine-0.8.0-aarch64-unknown-linux-gnu.tar.gz` has SHA-256
`f766a40e57e8cdd643eb588fb7f650315e9b49cd603c36b065c2e64f76078e73`, matching
the release's `SHA256SUMS`. v0.8.0 writes manifest version 0.3.

`crates/vtop-core/tests/manifest_writer.rs` rebuilds each from the fields it
records with the default (`0.3`) writer and requires the bytes to be
identical: the guard that `manifest.write_version` left the default output
untouched (#671 release B).

## Release B 0.4 (`release-b-0.4-unsigned.json`, `release-b-0.4-signed.json`)

The first `0.4` manifests, written by the release-B `vtopctl` (#671) built
from this branch, with the same procedure plus `manifest: {write_version:
"0.4"}` and the inputs `release B 0.4 manifest fixture\n` and
`signed release B 0.4 manifest fixture\n`. The default
`upload.require_strong_verification: true` and the localfs backend give
`verification_plan: {method: read_back, require_strong: true}`. They are the
documents every later reader must keep reading. `manifest_writer.rs`
verifies them from their stored bytes; they also seed the fuzz corpus
(`v04-release-b-*.seed`). The released v0.8.0 `vtopctl verify-manifest`
passed the signed one in its localfs archive, and the released v0.7.0 one
reported its self-hash and MAC as failed, which is the documented cutoff.

## Key ids (`keyring-0.4-q3.json`, `keyring-0.4-q4.json`)

The first manifests that name their MAC key (#685), written by the keyring
release's `vtopctl process-once` built from this branch: the localfs
backend, no compression, SHA-256, `manifest: {write_version: "0.4"}` and a
keyring of two key files, `q3` (`11` x 32, legacy, the key every earlier
signed fixture uses) and `q4` (`22` x 32). One run had `active: q3` and the
input `keyring q3 0.4 manifest fixture\n`, the other `active: q4` and
`keyring q4 0.4 manifest fixture\n`. Each records `manifest.mac_key_id`, and
this build's `verify-manifest` passed both, naming the key.

The released v0.8.0 `vtopctl verify-manifest` (asset SHA-256 as above),
configured with `manifest_mac_key_env` holding the `q3` key, passed the `q3`
manifest (self-hash OK: it authenticates `mac_key_id` as an unknown field)
and reported the `q4` one as "MAC missing or invalid". That is the reader
gate of the rotation procedure: every reader must run the keyring release
before the active key changes.

`crates/vtop-core/tests/manifest_keyring.rs` verifies both under their ids
and refuses `q3` under a keyring that does not hold it; they also seed the
fuzz corpus (`v04-keyring-*.seed`).
