# Release binaries

Every [release](https://github.com/allamiro/vtop-engine/releases) ships
`vtop-node` (the node process) and `vtopctl` (the operator CLI) for Linux
(x86_64, aarch64) and macOS (arm64, x86_64), a multi-arch container image
(`ghcr.io/allamiro/vtop-engine`), an SPDX SBOM, and a keyless Sigstore
signature for each artifact. Windows is not built. To build the binaries
yourself, see [from-source.md](from-source.md).

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

From the release after v0.8.0, the Linux and macOS binaries need no extra
libraries. The v0.8.0 binaries and earlier still link OpenSSL 3 and Cyrus
SASL dynamically:

- **macOS:** `brew install openssl@3 cyrus-sasl`.
- **Debian/Ubuntu:** `sudo apt-get install -y libssl3 libsasl2-2`.
- **RHEL family and Fedora:** `sudo dnf install -y openssl-libs cyrus-sasl-lib`.

The container image always carries what it needs.

A binary that fails to start with `Library not loaded` (macOS) or
`error while loading shared libraries` (Linux) is one of those older
binaries; the [troubleshooting table](from-source.md#troubleshooting) has the
fix for each message.
