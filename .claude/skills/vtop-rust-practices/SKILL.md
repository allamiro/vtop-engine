---
name: vtop-rust-practices
description: Rust conventions and review checklist tuned to vtop-engine — edition 2021 with an MSRV floor, clippy -D warnings, unsafe policy, error handling split (thiserror in libraries, anyhow in the CLI), async hygiene (bounded work, no locks across await, deadlines, shutdown), test hygiene, and verification tools the repo does not use yet (loom/shuttle, turmoil, miri). Use when writing or reviewing any Rust in this workspace.
---

# Rust practices for vtop-engine

Ground truth for this repo, checked against the tree: workspace `edition = "2021"`, `rust-version = "1.94.1"` (a CI job builds on exactly that toolchain), CI runs `cargo clippy --workspace --all-targets --locked -- -D warnings`, `thiserror` in library crates and `anyhow` only in `vtop-cli`, `proptest` in `vtop-core`, `cargo-mutants` (report-only) and fuzz smoke in CI. Complements `vtop-contributing` (process) and the `ddia-*` skills (design). Part of the checklist is adapted from the ideas in the MIT-licensed `rust-systems` skill in [iliaal/ai-skills](https://github.com/iliaal/ai-skills), rewritten for this codebase.

## Toolchain: two compilers, one tree

The code must compile warning-free on the **MSRV (1.94.1)** *and* on **current stable**. Those disagree over time:
- A std API that is deprecated on stable may have its replacement only on a newer toolchain than the MSRV (`Atomic*::fetch_update` -> `try_update` was exactly this). Do not `#[allow(deprecated)]` it away and do not adopt the newer API; write the small portable form (a compare-exchange loop) and say why in a comment.
- Dependency bumps can change struct shapes (syn 3.0.6 moved `ItemTrait::auto_token` under `modifiers: TraitModifiers`, which broke `vtop-upload`'s trait-declaration guard on the lib-test build). Build the tests (`--all-targets`), not just the lib, on the MSRV.
- Before pushing Rust: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked` on the MSRV. With no local toolchain, run them in `rust:1.94.1` and `rust:latest` containers. CI and `docker/Dockerfile` install `cmake clang libssl-dev libsasl2-dev libcurl4-openssl-dev zlib1g-dev pkg-config`, which is more than a default build needs since #677 (next section); use that list in a container so the result matches CI.

## Native dependencies: portable by default (#677, #679)

A default build must not depend on what the host happens to have installed, and a released binary must not need a library the user may not have. The facts, each checked against the tree:

- **OpenSSL is compiled in.** The workspace pins `rdkafka = { version = "0.39", features = ["cmake-build", "ssl-vendored"] }` (`Cargo.toml`), so librdkafka is built from source against an OpenSSL built from source (`openssl-src`, 3.6.x in `Cargo.lock`). The binaries load no OpenSSL at run time, and the build works whatever the host ships (1.1.1 on EL8, 3.x on EL9 and EL10, 4.x on Fedora 45 and Homebrew). It needs `make` and Perl with its core modules (`perl-core` on a minimal RHEL-family image). `OPENSSL_NO_VENDOR=1` (with `OPENSSL_DIR` where needed) links the system OpenSSL instead, for packagers; that works only with OpenSSL 1.1.1 or 3.x, because librdkafka calls the ENGINE API that OpenSSL 4 removed (the symptom is undefined `_ENGINE_*` symbols at link time). The cost is that OpenSSL security fixes arrive as `openssl-src` bumps through dependabot's cargo group, not through distribution packages: treat an advisory on `openssl-src` as urgent (`vtop-ci-supply-chain`).
- **Kerberos is opt-in.** `kafka-gssapi` (`crates/vtop-adapters/Cargo.toml`, re-exported by `crates/vtop-cli/Cargo.toml`) turns on `rdkafka/sasl`, which links the system Cyrus SASL. The default build links none: SASL PLAIN, SCRAM and OAUTHBEARER are built into librdkafka. An engine configured with `sasl_mechanism: GSSAPI` and built without the feature fails at start with "No provider for SASL mechanism GSSAPI". Do not make the default build link a system library to add a feature; add an opt-in feature the way `kafka-gssapi` does.
- **macOS floor is 11.0.** `.cargo/config.toml` sets `MACOSX_DEPLOYMENT_TARGET = { value = "11.0", force = false }`, because C dependencies built by cmake or cc (librdkafka, OpenSSL, aws-lc) otherwise take the build host's SDK version as their minimum and flood the link with "built for newer 'macOS' version" warnings. `force = false` lets an explicit environment value win. `release.yml` exports the same value so the log shows it, and its macOS legs install only `cmake` (`brew install`).
- **The release job checks the binaries.** After the build, the macOS legs run `otool -L` over `vtop-node` and `vtopctl` and fail if any library is outside `/usr/lib` or `/System`, and print the `minos` version. A new dependency that links a Homebrew library therefore breaks the release, by design; vendor it or put it behind an opt-in feature. Reproduce locally with `otool -L target/release/vtopctl` and `otool -l target/release/vtopctl | grep -A3 LC_BUILD_VERSION`; on Linux, `ldd` must show no libssl, libcrypto, libsasl2 or libcurl.
- **The curl header quirk.** A Linux build needs the libcurl development package (`libcurl4-openssl-dev`, `libcurl-devel`), but only for the header: librdkafka's generated config always defines `WITH_OAUTHBEARER_OIDC` (as 0 or 1) and `rdkafka_conf.c` tests it with `#ifdef`, so `curl/curl.h` is included even with curl support switched off. Nothing links libcurl. `fatal error: curl/curl.h: No such file or directory` means the header package is missing; the fix is to install it, not to add a libcurl feature. The macOS SDK already ships the header.
- **Verify after touching any of this** (`rdkafka`, `openssl`, a feature, `.cargo/config.toml`): `cargo build --release --locked`; `cargo build -p vtop-cli --no-default-features` (no librdkafka and no OpenSSL at all; the file, spool and native paths still work); `cargo build --release --features kafka-gssapi` with `libsasl2-dev` installed; and the `ldd` or `otool` check above. A green `cargo test` proves none of it.

## Unsafe policy

- Production code (`crates/*/src`) is currently unsafe-free; the only `unsafe` lives in research/test harnesses (`libc` I/O calls, aligned allocation). Keep it that way: **no `unsafe` in `src/` without a design note**, a `// SAFETY:` comment per block stating why each invariant holds, and a safe wrapper at the module boundary.
- Candidate improvement: a `[workspace.lints.rust] unsafe_code = "deny"` with a targeted `#![allow(unsafe_code)]` on the harness test files, so the compiler enforces what is currently a habit.
- Prefer `bytes`, `zerocopy`, `bytemuck` over hand-written transmutes. If `unsafe` is ever added, run it under `cargo +nightly miri test`.
- In edition 2024, env-var writes are `unsafe`; today (2021) they are not, but treat `set_var` as process-global mutation: only in `main` before threads start, never in tests that run concurrently.

## Lints and suppressions

- There is currently no `[workspace.lints]` table; lint policy lives in CI flags. Consider centralising it.
- Prefer fixing over `#[allow(clippy::...)]`. When a suppression is warranted, write `#[expect(clippy::lint, reason = "...")]` (stable since 1.81): it warns when the lint stops firing, so a suppression that outlived its cause reports itself. The tree has ~26 `allow(clippy` and no `expect` — new suppressions should use `expect` with a reason.

## Errors

- Libraries return typed errors (`thiserror`), preserving variants; add operational context at application boundaries (`vtop-cli` with `anyhow`).
- A failure message names its cause: what was observed, how many times, what was expected. "timed out" alone is not an error message.
- Distinguish "config missing" from "config unreadable/invalid" before writing a replacement; never swallow with `let _ =` where the result decides a safety property (replay rewinds, commits, fsyncs).
- Beware panicking guards: `bool::then_some(expr)` evaluates `expr` eagerly — `(len >= 19).then_some(&v[..19])` panics on short input. Use `then(|| ...)`. A fixed-width slice is not a parse: it panics mid-character on non-ASCII; parse and normalise or reject.
- Decoders over untrusted bytes: bound every length before allocating, return errors, never `unwrap`/index. Add a fuzz target (`fuzz/fuzz_targets/`).

## Async hygiene (ties directly to the failure modes in `ddia-fault-testing`)

- **Every await on the network, a database, or disk has a deadline** (S3/Postgres clients configured with operation/statement timeouts, or `tokio::time::timeout` around the call). A call without one can wedge the engine with the process alive and no error.
- **Bound queues and spawned work** (semaphores, bounded channels); an unbounded `spawn` per input turns an outage into a memory incident.
- **Never hold a `std::sync` lock across `.await`**, and keep blocking work (fsync, hashing, compression) off async workers (`spawn_blocking`).
- **Define shutdown.** Handle SIGTERM as well as SIGINT; flush or abandon in-flight work deliberately; a cancelled future must leave state recoverable (assume it can be dropped at any `.await`).
- A deadline per call is not a deadline per round. When a round makes several awaited calls in series, give the round one budget that each call draws down, or the round can take several times the interval your pacing arithmetic assumes. Opening a store (connect plus the schema check) is network work too and needs the same bound as the requests after it.
- A timeout means *unknown outcome*. Retrying a non-idempotent call after a timeout can double-apply; retry only idempotent calls, with backoff and jitter, at one layer.
- Prefer monotonic `Instant` for local durations; wall-clock `SystemTime` only for values that cross machines or are stored.

## Tests

- A bug fix ships a test that fails on the old code; a test that cannot fail is worse than none.
- Do not mutate process-wide state (env vars, cwd, global tracing/metrics registries) in tests that run in parallel; exercise the real binary for CLI behaviour, and the relevant feature combinations (`--features postgres`).
- Property tests (`proptest`) for codecs and state machines; crash/fault sweeps via the `Env` seam for durability (see `ddia-storage-durability`).
- Doc-tests are not run by `nextest`; if nextest is ever adopted, keep `cargo test --doc`.

## Verification tools worth adding (not used today)

| Tool | What it would catch here |
|---|---|
| `loom` or `shuttle` | Interleavings in hand-rolled concurrency: group commit's mutex/condvar queue, the readiness gate, metrics gauges. Deterministic exploration instead of hoping a stress test hits the race. |
| `turmoil` (or similar deterministic network simulation for tokio) | Partitions, delays and reordering in the replica plane and lease agent without real sockets; pairs with the storage `sim`. |
| `miri` | UB in any future `unsafe`; cheap to run on the few crates that would contain it. |
| `cargo-mutants` (already report-only) | Promote from report-only on `vtop-core` state machine paths once the surviving-mutant list is short. |

## Review checklist

- [ ] Compiles warning-free on MSRV and stable, tests included?
- [ ] Any new `unsafe`, `allow`, `unwrap`/`expect`/indexing on external input?
- [ ] Every external await has a deadline; every spawn is bounded; shutdown path defined?
- [ ] Errors keep their variants and name their cause; no result silently dropped where it decides safety?
- [ ] Does a test fail on the old code?
