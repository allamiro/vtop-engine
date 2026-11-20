# Installing and verifying a release

Every VTOP Engine release is signed and attested. Nothing here is optional
ceremony: the point of publishing signatures is that somebody checks them, and
an unverified download from a release page is a download from whoever could
reach the release page.

Replace `VERSION` with the release you are installing, for example `0.10.1`.
(This is the software release version; the VTOP *protocol* version carried
inside manifests is separate — see
[VTOP_PROTOCOL_DRAFT.md §11.2](VTOP_PROTOCOL_DRAFT.md#112-field-reference).
Likewise, "verifying" here means release-artifact verification, distinct from
the protocol's object/manifest verification in §17.)

## Container image

```
docker pull ghcr.io/allamiro/vtop-engine:VERSION
```

Multi-arch (`linux/amd64`, `linux/arm64`), published with SBOM and provenance
attestations, signed with cosign (keyless).

```
cosign verify ghcr.io/allamiro/vtop-engine:VERSION \
  --certificate-identity-regexp='https://github.com/allamiro/vtop-engine/.*' \
  --certificate-oidc-issuer='https://token.actions.githubusercontent.com'
```

The identity regexp matches any workflow in this repository. Pin it to
`.../.github/workflows/release.yml@refs/tags/vVERSION` if you want to assert
that the image came from the release workflow at that exact tag, which is the
stricter and better check for a deployment.

## Binaries

Each archive contains `vtop-node` (the node process) and `vtopctl` (the
operator CLI), for `linux` (x86_64, aarch64) and `macOS` (arm64, x86_64).

Windows is not built. librdkafka is compiled from source via cmake and the
Windows path is not validated — an untested binary is worse than an absent one.

Every archive ships a keyless Sigstore bundle alongside it, named the same plus
`.sigstore.json`:

```
archive=vtop-engine-VERSION-TARGET.tar.gz
cosign verify-blob "$archive" \
  --bundle "${archive}.sigstore.json" \
  --certificate-identity "https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/vVERSION" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

Also check the archive against `SHA256SUMS`:

```
sha256sum --check --ignore-missing SHA256SUMS
```

`SHA256SUMS` detects a corrupted or swapped download; the Sigstore bundle is
what establishes it came from this project's release workflow. They answer
different questions, so check both.

## SBOM

An SPDX SBOM (`vtop-engine-sbom.spdx.json`) is attached to every release, and
the container image carries an SBOM attestation.

## Reproducing a build

Every release run (a `v*` tag, and a dry-run dispatch) also rebuilds each
binary archive target on a second, clean runner and compares the result with
the archive the first build made. The job is named `reproduce <target>
(report only)`. It is report only: it never blocks or delays a release, its
failures show as a red job inside a green run, and what it uploads
(`repro-report-<target>`, and `repro-binaries-<target>` when a build differs)
is kept out of the release assets.

What it compares is `vtop-node` and `vtopctl`, by SHA-256, against the copies
inside the archive. The tarball's own hash is printed but not compared, because
`tar` and `gzip` record file times, so two correct builds of one tree give two
archive hashes. To check an archive, use `SHA256SUMS` and the signatures above;
to ask whether the binaries inside it come from the tagged source, run the same
comparison yourself:

```
tar -xzf vtop-engine-VERSION-TARGET.tar.gz
sha256sum vtop-engine-VERSION-TARGET/vtop-node vtop-engine-VERSION-TARGET/vtopctl
```

Then build the tag the way `release.yml` does (the `build` steps of its
`binaries` job: AlmaLinux 8 for the Linux targets, the macOS runner for the
Apple ones) and hash `target/TARGET/release/vtop-node` and `vtopctl`.

### When a rebuild differs

A difference is documented, not hidden: the job summary lists both hashes, the
versions the rebuild used, and, for a Linux binary, the compiler version
recorded inside each copy. Three build inputs are not pinned yet, and each can
change a binary without any change to the source:

- **The Rust toolchain.** The workflow installs `stable`, which moves every six
  weeks. A rebuild after a new stable release uses a newer compiler.
- **The distribution packages in the Linux build image.** The AlmaLinux 8 image
  is pinned by digest; the compiler, cmake, perl and `-devel` packages installed
  into it are whatever the mirrors serve on the day.
- **The macOS runner image.** Xcode, the SDK and Homebrew's cmake change with
  the runner image.

Cargo dependencies are not on this list: `--locked` builds use the versions in
`Cargo.lock`. If a run shows drift, the fix is to pin the input in
`release.yml`, for both the `binaries` job and the `reproduce` job.

## Maturity

VTOP is a **prototype / reference implementation**. Each release is signed,
attested and published as a full release, not a GitHub pre-release; the 0.x
series means a minor version may still break compatibility, and the release
notes say where.

Read [Known limitations](PRODUCTION_HA.md#19-known-limitations-current-code) (§19)
before depending on this in production.
