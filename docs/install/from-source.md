# Building from source

What you need to build `vtopctl` and `vtop-node` yourself, the optional build
variants, the checks CI runs, and what to do when a build fails. Prebuilt,
signed binaries are described in [binaries.md](binaries.md).

## Native prerequisites

The default build includes the Kafka source. Its `rdkafka` dependency compiles
librdkafka with CMake, and OpenSSL from source with Perl. OpenSSL is linked
into the binary, so the build never depends on the host's OpenSSL version, and
the binaries load no OpenSSL or SASL library at run time (#677). You need a
C/C++ toolchain, CMake, make and Perl, plus Rust from
[rustup](https://rustup.rs). The minimum Rust version is `rust-version` in
[`Cargo.toml`](../../Cargo.toml).

=== "Debian / Ubuntu"

    ```bash
    sudo apt-get install -y build-essential cmake perl pkg-config libcurl4-openssl-dev zlib1g-dev
    ```

=== "RHEL family"

    RHEL, AlmaLinux and Rocky Linux 8, 9 and 10:

    ```bash
    sudo dnf install -y gcc gcc-c++ make cmake perl-core libcurl-devel zlib-devel
    ```

=== "Fedora"

    ```bash
    sudo dnf install -y gcc gcc-c++ make cmake perl-core libcurl-devel zlib-devel
    ```

=== "macOS"

    macOS 11 or later, Apple silicon or Intel:

    ```bash
    xcode-select --install
    brew install cmake
    ```

The `dnf` line was checked on AlmaLinux 8.10, 9.8 and 10.2, Rocky Linux 8.10
and 9.8, and Fedora 45. A full build was run on AlmaLinux 8 (OpenSSL 1.1.1
installed) and on Fedora 45 (OpenSSL 4 installed); neither touches the system
OpenSSL. RHEL takes the same packages from its BaseOS and AppStream
repositories. On a minimal image, `perl-core` matters: the OpenSSL build needs
Perl modules (`FindBin`, `IPC::Cmd`) that bare `perl` doesn't carry. The curl
package is needed for its header only. librdkafka includes `curl/curl.h` even
with curl support switched off (its config always defines
`WITH_OAUTHBEARER_OIDC`, and one file tests it with `#ifdef`). Nothing links
libcurl. The macOS SDK ships that header already.

Then build:

```bash
cargo build --release --locked
./target/release/vtopctl --version
```

On macOS, `.cargo/config.toml` sets `MACOSX_DEPLOYMENT_TARGET=11.0`, so a
binary built on any macOS release runs on macOS 11 and later. To target a
different minimum, export that variable yourself.

## Optional builds

- **Without the Kafka source.** `cargo build -p vtop-cli --no-default-features`
  skips librdkafka and OpenSSL entirely. The file, spool and native paths work
  the same. Kafka is enabled by default for compatibility with existing
  deployments, but it is a feature-gated adapter: a native/file/syslog-only
  CLI build does not compile or link `rdkafka`.
- **Linking the system OpenSSL** instead of the built-in one. Packagers may want
  this to pick up distribution security updates. Install the OpenSSL
  development package (`libssl-dev`, `openssl-devel`, or Homebrew
  `openssl@3`), then:

  ```bash
  export OPENSSL_NO_VENDOR=1
  export OPENSSL_DIR="$(brew --prefix openssl@3)"   # macOS only
  ```

  Use OpenSSL 1.1.1 or 3.x. librdkafka calls the ENGINE API, which OpenSSL 4
  removed, so a system OpenSSL 4 (Fedora 45, Homebrew's default `openssl`)
  fails at the link step.
- **Kerberos (SASL GSSAPI)** for the Kafka source:
  `cargo build --release --features kafka-gssapi`. It links the system Cyrus
  SASL, so install `libsasl2-dev` (Debian/Ubuntu), `cyrus-sasl-devel`
  (RHEL family, Fedora) or Homebrew `cyrus-sasl`, and on macOS:

  ```bash
  export PKG_CONFIG_PATH="$(brew --prefix cyrus-sasl)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
  ```

  SASL PLAIN, SCRAM and OAUTHBEARER are built into librdkafka and need none of
  this.

## Checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

CI runs formatting, linting, tests, and release build on push and pull
request; the workflow is [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml).
[CONTRIBUTING.md](../../CONTRIBUTING.md) lists the rest of what CI expects
(shellcheck, bats, Helm).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Library not loaded: .../libsasl2.3.dylib` or `libssl.3.dylib` (macOS) | A v0.8.0 or earlier binary. Run `brew install openssl@3 cyrus-sasl`, or build from source. |
| `error while loading shared libraries: libsasl2.so.3` or `libssl.so.3` (Linux) | A v0.8.0 or earlier binary. Install the run-time packages listed in [binaries.md](binaries.md#run-time-libraries). |
| `Can't locate FindBin.pm` or `IPC/Cmd.pm` during the build | The OpenSSL build needs those Perl modules. Install `perl-core` (RHEL family, Fedora) or `perl` (Debian/Ubuntu). |
| `failed to run custom build command for rdkafka-sys` mentioning `cmake` | Install CMake (see [Native prerequisites](#native-prerequisites)). |
| `fatal error: curl/curl.h: No such file or directory` (Linux) | Install `libcurl4-openssl-dev` or `libcurl-devel` (header only; see above). |
| Undefined `_ENGINE_*` symbols, or the link line names `openssl@4` | `OPENSSL_NO_VENDOR` is set and found OpenSSL 4. Unset it, or point `OPENSSL_DIR` at an OpenSSL 3. |
| `No provider for SASL mechanism GSSAPI` when the engine starts | The binary was built without Kerberos. Rebuild with `--features kafka-gssapi`. |
| `ld: warning: ... was built for newer 'macOS' version` | Something set `MACOSX_DEPLOYMENT_TARGET` higher than the build defaults. Unset it, or set it to the oldest macOS you want to support. |
| A fix above changed nothing | Cargo cached the old build of the C libraries. Run `cargo clean -p openssl-sys -p rdkafka-sys` and build again. |
