# Fuzzing

`detect.rs` and manifest deserialization both parse **untrusted input bytes** —
records straight off Kafka / files / syslog, and manifests read back from an
object store that a tampering party may control. The contract these fuzz targets
enforce is that neither ever **panics** on arbitrary input: a classifier or
parser that can be crashed by a crafted byte string is a denial-of-service
surface.

Built with [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer),
which requires a nightly toolchain.

## Targets

| Target | Exercises | Contract |
|---|---|---|
| `detect_record` | `detect_record(&[u8])` | never panics on any byte string |
| `detect_batch` | `detect_batch(&[Vec<u8>])` (input split on newlines) | never panics on any set of records |
| `manifest_parse` | `serde_json::from_slice::<VtopManifest>` + `self_hash` / `verify_self_hash` / `to_json_bytes` on anything that parses | deserialization returns `Err`, never panics; downstream helpers don't panic on a hostile manifest |

## Run

```bash
rustup toolchain install nightly
cargo install cargo-fuzz

# From the repo root:
cargo +nightly fuzz run detect_record        # runs until a crash or Ctrl-C
cargo +nightly fuzz run detect_record -- -max_total_time=60   # bounded

cargo +nightly fuzz run detect_batch
cargo +nightly fuzz run manifest_parse
```

A crash writes the offending input to `fuzz/artifacts/<target>/` for replay:

```bash
cargo +nightly fuzz run detect_record fuzz/artifacts/detect_record/crash-<hash>
```

## Corpus

`fuzz/corpus/<target>/*.seed` holds committed seed inputs — one valid example of
each format (CEF, LEEF, JSON, syslog, raw, JSONL, an empty manifest). libFuzzer
grows the working corpus from these; the generated files are git-ignored (only
`*.seed` is tracked).

## CI

The `fuzz smoke` job runs each target for 60 s from the seed corpus on every PR
and fails the build if any target crashes. It is a regression guard, not a
substitute for long out-of-band campaigns.
