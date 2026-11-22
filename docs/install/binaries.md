# Release binaries

Every [release](https://github.com/allamiro/vtop-engine/releases) ships
`vtop-node` (the node process) and `vtopctl` (the operator CLI) for Linux
(x86_64 and aarch64, each as a glibc and a static musl build) and macOS
(arm64, x86_64), a multi-arch container image
(`ghcr.io/allamiro/vtop-engine`), an SPDX SBOM, and a keyless Sigstore
signature for each artifact. Windows is not built. To build the binaries
yourself, see [from-source.md](from-source.md).

## Supported platforms

Each release has six archives, named
`vtop-engine-VERSION-TARGET.tar.gz`:

| Archive target | Runs on |
|---|---|
| `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` | Linux with **glibc 2.17 or newer** |
| `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` | Any Linux, Alpine and other musl distributions included: fully static |
| `aarch64-apple-darwin` (Apple silicon), `x86_64-apple-darwin` (Intel) | **macOS 11 or newer** |

### Which Linux archive to pick

- **glibc distributions** (Amazon Linux, RHEL and its rebuilds, Debian,
  Ubuntu, SUSE, Fedora): take the `-linux-gnu` archive. `ldd --version`
  prints `GNU libc` or `GLIBC` and the version on these systems.
- **musl distributions** such as Alpine, a glibc older than 2.17, or a host
  where you want a binary that loads no shared library at all: take the
  `-linux-musl` archive. On Alpine, `ldd --version` prints `musl libc`.

Where both run, prefer the `-linux-gnu` archive: CI's test suite runs on
glibc builds, while the musl archives are checked by the release smoke run
described below and nothing more. They use musl's own memory allocator,
whose performance under load has not been measured here.

### glibc floor

The `-linux-gnu` binaries of v0.10.2 and later need glibc 2.17. They are built
inside manylinux2014 (CentOS 7, glibc 2.17), and the release workflow fails
if either binary asks for a symbol version newer than glibc 2.17, or than the
`libgcc_s` and `libstdc++` of gcc 4.8 that the glibc 2.17 distributions ship,
or if it links a library a base install lacks. That covers RHEL and CentOS 7
and later (AlmaLinux and Rocky Linux 8 and later), Amazon Linux 2 and 2023,
Debian 8 and later and Ubuntu 14.04 and later. Rust's own floors apply too:
Linux kernel 3.2 on x86_64 and 4.1 on aarch64.

The `-linux-musl` binaries have no glibc floor: they are static executables
with no program interpreter and no shared-library dependency, which the
release workflow also checks.

### What the release runs before publishing

Both binaries from every Linux archive (x86_64 and aarch64, glibc and musl)
run in a container of each of these distributions, with nothing installed:
the version check, then a file-to-local-directory round trip, the coverage
audit and a scrub that reads every object back.

| Distribution | glibc | `-linux-gnu` | `-linux-musl` |
|---|---|---|---|
| Amazon Linux 2 | 2.26 | run | run |
| Amazon Linux 2023 | 2.34 | run | run |
| Debian 12 | 2.36 | run | run |
| Debian 13 | 2.41 | run | run |
| Ubuntu 22.04 | 2.35 | run | run |
| Ubuntu 24.04 | 2.39 | run | run |
| AlmaLinux 8 | 2.28 | run | run |
| AlmaLinux 9 | 2.34 | run | run |
| AlmaLinux 10 | 2.39 | run | run |
| Alpine 3.24 | none (musl) | cannot run | run |

The macOS archives run the same round trip on the macOS runners (macOS 15,
arm64 and x86_64). The other distributions and macOS releases named above are
expected to work by the same symbol versions and deployment target, but are
not run.

### The test suite on each platform

Besides the archives, the workspace's test suite (`cargo test --workspace`,
the tests that need no broker or store) runs nightly on each CPU and operating
system the archives target: macOS 15 on Apple silicon and on Intel, and Linux
on aarch64 (Ubuntu 24.04), in the
[platform tests](https://github.com/allamiro/vtop-engine/blob/main/.github/workflows/platform-tests.yml)
workflow. Every other test job runs on x86_64 Linux. The live tests, against
Kafka, MinIO and PostgreSQL, run on x86_64 Linux only.

### Earlier releases

- v0.10.1's Linux archives were built inside AlmaLinux 8 and need glibc 2.28,
  so they do not start on Amazon Linux 2 or RHEL 7. v0.10.1 has no musl
  archives.
- The Linux archives of v0.8.0 to v0.10.0 were built on glibc 2.39 hosts.
  Their `vtopctl` needs glibc 2.39 and stops with
  ``version `GLIBC_2.38' not found`` on Debian 12, Ubuntu 22.04 and RHEL 8 or
  9; their `vtop-node` needs glibc 2.34 (#808).

On those systems, use v0.10.2 or later, or the container image.

## Verify the download

Check both the checksum and the signature. `SHA256SUMS` detects a corrupted
or swapped download; the Sigstore bundle establishes that the archive came
from this project's release workflow.

```bash
archive=vtop-engine-VERSION-TARGET.tar.gz
sha256sum --check --ignore-missing SHA256SUMS
cosign verify-blob "$archive" \
  --bundle "${archive}.sigstore.json" \
  --certificate-identity "https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/vVERSION" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

The container image, its SBOM and provenance attestations, and the stricter
identity pin for a deployment are covered in
[RELEASE_VERIFICATION.md](../RELEASE_VERIFICATION.md), which is the full
procedure.

## Run-time libraries

From v0.9.0, the Linux and macOS binaries need no library beyond the base
system's (for the glibc version, see [Supported platforms](#supported-platforms)).
The v0.8.0 binaries and earlier still link OpenSSL 3 and Cyrus SASL
dynamically:

=== "Debian / Ubuntu"

    ```bash
    sudo apt-get install -y libssl3 libsasl2-2
    ```

=== "RHEL family"

    ```bash
    sudo dnf install -y openssl-libs cyrus-sasl-lib
    ```

=== "Fedora"

    ```bash
    sudo dnf install -y openssl-libs cyrus-sasl-lib
    ```

=== "macOS"

    ```bash
    brew install openssl@3 cyrus-sasl
    ```

The container image always carries what it needs.

A binary that fails to start with `Library not loaded` (macOS) or
`error while loading shared libraries` (Linux) is one of those older
binaries; the [troubleshooting table](from-source.md#troubleshooting) has the
fix for each message.
