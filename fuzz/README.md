# Fuzzing

Every decoder that reads **untrusted bytes** has a fuzz target here: records
straight off Kafka, files and syslog; manifests and receipts read back from an
object store a tampering party may control; frames a producer, a consumer, a
replica peer or a metadata peer sends; Kafka requests from any client; and the
files the engine and the broker read back after a crash (segments, metadata
store files, the ledger's digests, upload sessions, audit records). The
contract each target enforces is that the decoder never **panics** on
arbitrary input: a parser that a crafted byte string can crash is a
denial-of-service surface, and on a recovery path a crash loop. Where a
format is written back, the targets also check that what decodes re-encodes
to something that decodes the same.

Built with [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz)
(libFuzzer), which requires a nightly toolchain. CI pins
`nightly-2026-07-22` (see the `fuzz` job in `.github/workflows/ci.yml`).

## Targets

| Target | Exercises | Contract |
|---|---|---|
| `detect_record` | `detect_record(&[u8])` | never panics on any byte string |
| `detect_batch` | `detect_batch(&[Vec<u8>])` (input split on newlines) | never panics on any set of records |
| `manifest_parse` | `ParsedManifest::from_slice` (version check first, strict 0.4 parse and canonical form) + `verify_self_hash` / `verify_authentication` / keyring `authenticate`; `serde_json::from_slice::<VtopManifest>` + `self_hash` / `verify_self_hash` / `to_json_bytes` | parsing returns `Err`, never panics; the verification helpers don't panic on a hostile manifest. Seeds include the released v0.7.0 and v0.8.0 manifests, the first 0.4 manifests release B wrote (signed and unsigned, with `verification_plan`), a 0.4 document with unknown fields, a 0.4 plan with an unknown `method` and field, the 0.4 manifests signed under key ids `q3` and `q4` (#685), and newer, duplicate-key and fractional-number cases. Each parsed manifest is also authenticated against a keyring, so a hostile `mac_key_id` goes through the key lookup. The same bytes go through `ParsedReceipt::from_slice` + `verify_self_hash` / `authenticate` (verification receipts, #671), with seeds for the first signed and unsigned receipts release B wrote and one with an unknown `method`, an unknown field and an oversized `mac_key_id` |
| `protocol_frame` | `vtop_protocol::decode_frame` and the streaming `read_frame` (VTPW), every message kind | never panics; a frame that decodes and re-encodes decodes to itself. The input is decoded as sent and with its checksum repaired, so the per-kind message decoders see hostile payloads. Seeds: one valid frame per message family |
| `kafka_request` | the Kafka gateway's request path: `RequestHeader::decode`, then the body decoder the header's api key and version pick (`decode_metadata`, `decode_produce`, `decode_fetch`, `decode_list_offsets`, `decode_init_producer_id`, the group APIs), whole-body consumption, and each batch of a produce request's RECORDS field (`RecordBatch::decode`); the raw input also as a RECORDS field, with each batch's CRC-32C repaired | never panics. Seeds: every served api at its lowest and highest served version |
| `segment_codec` | a segment directory restored onto the simulated disk: `SegmentReader::open_in` and a fetch, `verify_sealed_segment_in`, `chunk_proof_in`, `ActiveSegment::recover_in`, and the startup scan `SegmentSet::open_in`; so the v1 and v2 headers and record frames, the index, the `VTOPCHK1` chunk sidecar, the `VTOPCMT1` commit marker, the manifest and the producer sidecar | never panics. The input is a file bundle (below). Seeds: v1 and v2, sealed (keyed and unkeyed) and active |
| `meta_storage_files` | a metadata data directory restored onto the simulated disk: `HardStateFile` (`VTOPMHS1`), the applied, purged and membership frontiers (`VTOPMAP1`, `VTOPMPG1`, `VTOPMML1`), `MetaLog` chunks and entry frames (`VTOPMLG1`, `VTOPMLE1`), `MetaSnapshots` (`VTOPMSN1`, open and `install`), and `MetaStorage::open_in`, the recovery that replays them | never panics. The input is a file bundle. The command and snapshot payload decoders are `meta_decode`'s; here they run only as recovery calls them. Seeds: a log with a hard state, a directory holding every file, and one whose log and snapshot hold attributed commands (kind 40) and their tail (#706) |
| `meta_decode` | the metadata tier's payload decoders, selected by the input's first byte: `MetadataCommand::decode`, `MetadataResponse::decode`, `MetaMembership::decode`, `MetaStateMachine::decode_snapshot`, and the log entry frame through `MetaLog::open_in` (frames as written to a chunk, the first frame again with its checksum repaired, and an entry kind with its payload in a valid frame) | never panics. A value that decodes encodes, decodes to itself and encodes to the same bytes again. A log entry the log reads is the value its payload decoder gives, and a payload that decoder refuses is refused by the log as corruption or as a newer release's record (`NewerFormat`), never read and never an I/O error. Seeds: the golden vectors of `crates/vtop-meta/tests/codec_golden.rs` (`golden-*`), one value of every command and response kind, the membership shapes, snapshot payloads and entry frames, and an unknown command kind (`newer-*`) |
| `vtpm_frame` | `VtpmFrame::decode` and the streaming `read_frame` (the metadata peer and admin transport), then the payload decoder of the frame's kind | never panics; a decoded frame re-encodes to its own bytes. Decoded as sent and with the checksum repaired. Seeds: a frame of every kind, most with a real payload |
| `ledger_rows` | `vtop_cli::coverage::parse_digest` (the `ledger-digest-*.jsonl` prune digest), `BatchRecord` and `ProgressMarker` JSON per line, and the coverage audit (`audit`, `unit_ranges`, `rotation_unproven`) over what parses | never panics; a row and a marker that read write back to text that reads the same. Seeds: a digest `write_digest` wrote, its rows, and the markers written before #578 and before the tail fingerprint |
| `session_files` | the multipart upload session (`MultipartSession` JSON) and the scrub cursor (`vtop_cli::scrub::parse_cursor`) | never panics; a session round-trips. Seeds: the v0.8.0 session fixture, a completed session with an Object Lock retention (#684), two cursors |
| `audit_record` | `vtop_observe::audit::read_record` per line | never panics; a newer version or an unknown value stays `Unknown`, not-JSON stays `Malformed`, a v1 record round-trips. Seeds: `audit-v1.jsonl`, its lines, a newer version, an unknown action, a torn line |

`meta_decode` is selected by the first byte of its input: `0` command, `1`
response, `2` membership, `3` snapshot payload, `4` log entry frames, `5` an
entry kind and its payload. The frames of the metadata transport are
`vtpm_frame`'s, and the store's files as a whole (the log chunks, snapshot
file, hard state and frontiers, and the recovery that replays them) are
`meta_storage_files`'s. A metadata command or response kind a later release
adds is covered by `meta_decode` as soon as the encoder has a value of it:
`vtop-meta/src/fuzz_seeds.rs` writes a seed per entry of its `every_command`
and `every_response` lists, and a test fails when a committed seed is stale.

### File bundles

`segment_codec` and `meta_storage_files` read several files at once, so their
input is a bundle: for each file, a big-endian `u16` path length, the absolute
path, a big-endian `u32` length, the content (`vtop_log::sim::SimDiskState`'s
`to_bundle` and `from_bundle`). The fuzzer can damage one sidecar while the
rest of the directory stays valid.

### Checksums

Most binary formats here carry a checksum over their own bytes, and a random
mutation almost always breaks it. `protocol_frame`, `vtpm_frame` and
`kafka_request` therefore decode each input twice: as sent (so the checksum
check stays covered) and with the checksum recomputed (so the decoders behind
it see hostile bytes). The storage formats are fuzzed as sent.

## Run

```bash
rustup toolchain install nightly-2026-07-22
cargo install cargo-fuzz

# From the repo root. The fuzz crate has no committed lockfile; start it from
# the workspace's, as CI does, to build the versions the engine ships:
cp Cargo.lock fuzz/Cargo.lock

cargo +nightly-2026-07-22 fuzz list
cargo +nightly-2026-07-22 fuzz run protocol_frame        # until a crash or Ctrl-C
cargo +nightly-2026-07-22 fuzz run protocol_frame -- -max_total_time=60
```

CI passes `--target x86_64-unknown-linux-gnu` on Linux, because ASAN cannot
link a static libc.

## Corpus

`fuzz/corpus/<target>/*.seed` holds the committed seeds; the corpus the
fuzzer grows is git-ignored (only `*.seed` is tracked). The seeds of the
binary formats are written by tests in the crates that own the formats, so
they stay in step with the code:

| Target | Written by | Checked in the normal suite by |
|---|---|---|
| `protocol_frame` | `cargo test -p vtop-protocol --lib -- --ignored write_fuzz_seeds` | `the_committed_fuzz_seeds_decode` (vtop-protocol) |
| `kafka_request` | `cargo test -p vtop-kafka --lib -- --ignored write_fuzz_seeds` | `the_committed_fuzz_seeds_decode` (vtop-kafka) |
| `vtpm_frame` | `cargo test -p vtop-meta --lib -- --ignored write_fuzz_seeds` | `the_committed_fuzz_seeds_decode` (vtop-meta) |
| `segment_codec` | `cargo test -p vtop-log --test fuzz_seeds -- --ignored write_fuzz_seeds` | `the_committed_fuzz_seeds_open` (vtop-log) |
| `meta_storage_files` | `cargo test -p vtop-meta --test fuzz_seeds -- --ignored write_fuzz_seeds` | `the_committed_fuzz_seeds_open` (vtop-meta) |
| `meta_decode` | `cargo test -p vtop-meta --lib -- --ignored write_fuzz_seeds` (every command and response kind, memberships, snapshots, entry frames) and `cargo test -p vtop-meta --test codec_golden -- --ignored write_golden_fuzz_seeds` (the golden vectors) | `the_committed_fuzz_seeds_are_what_this_build_writes` (vtop-meta lib) and `the_committed_golden_fuzz_seeds_are_the_pinned_vectors_and_decode` (`codec_golden`) |
| `ledger_rows` | `cargo test -p vtop-cli --lib -- --ignored write_fuzz_seeds` (plus the hand-copied `marker-*` seeds) | the CI seed replay |

A format change that leaves a seed undecodable fails those tests, rather than
quietly starting the nightly from inputs rejected at the first byte: rerun
the writer and commit the result.

## CI

**Every PR** (`fuzz smoke (every target)` in `ci.yml`, when the fuzz crate or
any crate it fuzzes changes): builds every target, replays every committed
seed once (`-runs=0`), then runs each target for 30 s. A crash fails the
build and uploads `fuzz/artifacts/`.

**Every night** (`.github/workflows/fuzz-nightly.yml`, 02:37 UTC): the
targets are built once, then one job per target fuzzes for an hour
(`seconds` on a manual run), all in parallel.

- The corpus persists between nights in the Actions cache, keyed
  `fuzz-corpus-<target>-<run id>` and restored by the prefix
  `fuzz-corpus-<target>-`: each night restores the newest and saves a new
  one, minimized first (`-merge=1`, what `cargo fuzz cmin` runs) so it holds
  only inputs that add coverage. The committed seeds are copied in every
  night, so an evicted cache (seven days unused, or the repository's 10 GB
  cache limit) restarts from the seeds, not from nothing.
- A crash minimizes the input (`-minimize_crash=1`, what `cargo fuzz tmin`
  runs), uploads it as the artifact `fuzz-crash-<target>-<run id>` (kept 90
  days), and opens an issue titled `fuzz: <target> crashed` with the labels
  `security` and `testing`, or comments on the open issue with that title.
- A pull request that changes the workflow or `fuzz/` runs the same workflow
  for 30 s per target, with the issue step as a dry run (it searches for the
  open issue and prints what it would do). The real issue path runs only on
  `main`.

### Reproducing a nightly crash

```bash
gh run download <run id> -n fuzz-crash-<target>-<run id> -D /tmp/crash
cp Cargo.lock fuzz/Cargo.lock
cargo +nightly-2026-07-22 fuzz run <target> /tmp/crash/artifacts/crash-<hash>
```

The `.min` file beside it is the minimized input. **The fix commits that
input as a regression test in the crate that owns the decoder**: a `#[test]`
that feeds it to the decoder and expects an `Err`, not a panic, so the normal
suite keeps the bug fixed without a fuzzer.

## Mutation testing

`ci.yml` runs `cargo-mutants` over the commit path (`state_machine.rs` and
`replay.rs` in `vtop-core`) on every Rust PR and **gates** on the score
(`mutation testing (commit path)`): `scripts/mutants-gate.py` reads
`mutants.out/outcomes.json`, computes `caught / (caught + missed + timeout)`
(unviable mutants, which do not compile, are left out), lists every mutant
the tests missed, and fails below `MUTANTS_MIN_SCORE`, set in the job.
`detect.rs` and `batch.rs` stay in the report-only job.

The nightly workflow mutates the verify, recovery and file-continuity code in
`vtop-upload`, `vtop-adapters` and `vtop-state`, one job per crate:

| Leg | Files | Measured (nightly, then this change) | Nightly |
|---|---|---|---|
| `vtop-state` (recovery: claims, fences, the ledger) | `store.rs`, `fence.rs`, `sqlite_store.rs` | 0.786, 0.763, 0.831, then 0.929, 0.947, 0.944 | **gated** at 0.87, 0.89, 0.89 |
| `vtop-upload` (verify) | `base.rs`, `multipart.rs` | 0.641, 0.708, then 0.982, 1.000 | **gated** at 0.93, 0.95 |
| `vtop-adapters` (file continuity) | `continuity.rs`, `rotated.rs`, `file_source.rs` | 0.759, 0.765, 0.741, then 0.963, 0.971, 0.975 | **gated** at 0.91, 0.92, 0.92 |

A gated leg fails the nightly run when any of its files scores below its own
threshold (`scripts/mutants-gate.py --file-min-score FILE=SCORE`), and lists
every mutant the tests missed. A pull request that changes the nightly
workflow runs every gated leg in full.

The `vtop-adapters` mutants still alive after the tests (17 of 626 in the
last full run, none excluded) are ones no test can tell from the original:
a bound on an elapsed time (`<` for `<=`), a value assigned that was already
there, a buffer size, a guard that only skips a re-read of the same bytes, a
fallback that fails with the same error as the check it follows, an error
only a signal (`EINTR`) or a filesystem without a no-replace rename
produces, and a search whose candidates are proven again afterwards. Three
more fail an open of a file the tests run as root can always open; they are
caught when the job runs as the runner's own user. An exclusion would hide
them from the next change that makes one of them matter, so each threshold's
5 points leave room for them instead.

`base.rs` has two surviving mutants that no test can kill, because they do
not change behaviour, and they stay in the score rather than being excluded:

- `bytes_read > expected_size` to `>=` in `verify_reader_content`: the line
  is only reached when `bytes_read != expected_size`, so the two are the same
  there.
- `kept.len() > Self::MAX_BYTES` to `>=` in `ReadBackEvidence::collect`: at
  equality the excess to drain is zero, so draining or not is the same.

The tests of the two files run with the clock paused wherever a part is
retried, and bound every upload call, so a mutant that removes a retry bound
fails a test instead of timing the run out.

Thresholds (the maintainer's rule on #729): each gate is the file's measured
score minus 5 points, rounded down to two places, and never below 0.80. `state_machine.rs` and
`replay.rs` measured 22 of 22 caught (1.00) but are gated at 0.90 until the
nightly runs show no flaky timeouts, since a mutant that times out on a
loaded runner counts as not caught; then their gate goes to 1.00.

Run the gate locally after a mutation run:

```bash
cargo mutants --package vtop-core \
  --file crates/vtop-core/src/state_machine.rs --file crates/vtop-core/src/replay.rs
python3 scripts/mutants-gate.py --outcomes mutants.out/outcomes.json --min-score 0.90
python3 -m unittest discover -s scripts/tests -p 'test_mutants_gate.py'   # the gate's own tests
```
