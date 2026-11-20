# Release binaries

Every [release](https://github.com/allamiro/vtop-engine/releases) ships
`vtop-node` (the node process) and `vtopctl` (the operator CLI) for Linux
(x86_64, aarch64) and macOS (arm64, x86_64), a multi-arch container image
(`ghcr.io/allamiro/vtop-engine`), an SPDX SBOM, and a keyless Sigstore
signature for each artifact. Windows is not built. To build the binaries
yourself, see [from-source.md](from-source.md).

## Supported platforms

The Linux binaries of v0.10.1 and later need **glibc 2.28 or newer**. That
covers Debian 10 and later, Ubuntu 20.04 and later, and RHEL, AlmaLinux and
Rocky Linux 8 and later. Before publishing, the release workflow checks that
neither binary asks for a glibc symbol version newer than 2.28, then runs both
binaries from each Linux archive (x86_64 and aarch64) on Debian 12, Ubuntu
22.04 and AlmaLinux 8 (glibc 2.36, 2.35 and 2.28) with nothing installed. The
other releases in that list are expected to work by the same symbol versions
but are not run. Distributions built on musl instead of glibc, such as Alpine,
are not supported; use the container image there.

The Linux archives of v0.8.0 to v0.10.0 were built on glibc 2.39 hosts.
Their `vtopctl` needs glibc 2.39 and stops with
``version `GLIBC_2.38' not found`` on Debian 12, Ubuntu 22.04 and RHEL 8 or
9; their `vtop-node` needs glibc 2.34. On those systems, use v0.10.1 or later,
the first release built for the glibc 2.28 floor (#808), or the container
image.

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
