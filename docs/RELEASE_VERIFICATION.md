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

The image also has a GitHub build provenance attestation (SLSA), which says
which workflow, tag and commit built it. Check it with the GitHub CLI
(`gh attestation`, version 2.68 or newer, the first with `--source-ref`), signed in to the registry if your
setup needs it:

```
gh attestation verify oci://ghcr.io/allamiro/vtop-engine:VERSION \
  --repo allamiro/vtop-engine \
  --signer-workflow allamiro/vtop-engine/.github/workflows/release.yml \
  --source-ref refs/tags/vVERSION \
  --deny-self-hosted-runners
```

It exits 0 and prints the verified attestation when the image was built by
`release.yml` at that tag on a GitHub-hosted runner, and non-zero otherwise.

## Binaries

Each archive contains `vtop-node` (the node process) and `vtopctl` (the
operator CLI), for `linux` (x86_64, aarch64) and `macOS` (arm64, x86_64).
Linux has two archives per architecture: `TARGET` is
`x86_64-unknown-linux-gnu` or `aarch64-unknown-linux-gnu` (glibc 2.17 or
newer), or `x86_64-unknown-linux-musl` or `aarch64-unknown-linux-musl`
(fully static, any Linux). On macOS it is `aarch64-apple-darwin` or
`x86_64-apple-darwin`. Six archives in all, each with its own Sigstore
bundle, build provenance attestation and line in `SHA256SUMS`;
[install/binaries.md](install/binaries.md#supported-platforms) says which to
pick.

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

Each archive also has a GitHub build provenance attestation (SLSA). It is not a
file in the release: it is stored with GitHub's attestations for the
repository, and `gh attestation verify` fetches it:

```
gh attestation verify "$archive" \
  --repo allamiro/vtop-engine \
  --signer-workflow allamiro/vtop-engine/.github/workflows/release.yml \
  --source-ref refs/tags/vVERSION \
  --deny-self-hosted-runners
```

It exits 0 and prints the verified attestation when the archive was built by
`release.yml` at that tag on a GitHub-hosted runner. The release workflow runs
this same command on every archive and on the image before it publishes the
release, so a release that cannot be verified is not published. A manual
dry-run of the workflow (`workflow_dispatch`) builds but publishes nothing and
creates no attestation.

Also check the archive against `SHA256SUMS`:

```
sha256sum --check --ignore-missing SHA256SUMS
```

`SHA256SUMS` detects a corrupted or swapped download; the Sigstore bundle and
the attestation are what establish it came from this project's release
workflow. They answer different questions, so check both.

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

Then build the tag the way `release.yml` does (the build steps of its
`binaries` job) and hash `target/TARGET/release/vtop-node` and `vtopctl`. The
inputs per target:

| Target | Build environment |
|---|---|
| `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` | The `quay.io/pypa/manylinux2014_<arch>` image named by digest in the `build_image` field of the `binaries` matrix (CentOS 7, glibc 2.17, gcc 10), with `perl-core` and `libcurl-devel` installed, run natively on that architecture |
| `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` | The `rust:1-alpine` image named by digest in the same field, with `build-base cmake perl linux-headers curl-dev` installed, `LIBZ_SYS_STATIC=1` set, and the Rust version the workflow resolved installed with `rustup`, run natively on that architecture |
| `aarch64-apple-darwin`, `x86_64-apple-darwin` | The `macos-15` and `macos-15-intel` runners, `cmake` from Homebrew, `MACOSX_DEPLOYMENT_TARGET=11.0` |

The Rust version is the `stable` the run installed; the job's `inputs.txt`
(in the `repro-report-<target>` artifact) and the log of the `build <target>`
job both print it, so build with that version, not with whatever `stable` is
today.

### When a rebuild differs

A difference is documented, not hidden: the job summary lists both hashes, the
versions the rebuild used, and, for a Linux binary, the compiler version
recorded inside each copy. Three build inputs are not pinned yet, and each can
change a binary without any change to the source:

- **The Rust toolchain.** The workflow installs `stable`, which moves every six
  weeks. A rebuild after a new stable release uses a newer compiler.
- **The distribution packages in the Linux build images.** The manylinux2014
  and Alpine images are pinned by digest; the packages installed into them
  (`perl-core` and `libcurl-devel` on CentOS 7, `build-base`, `cmake`, `perl`,
  `linux-headers` and `curl-dev` on Alpine) are whatever the mirrors serve on
  the day. The job prints each installed version in `inputs.txt`.
- **The macOS runner image.** Xcode, the SDK and Homebrew's cmake change with
  the runner image.

Cargo dependencies are not on this list: `--locked` builds use the versions in
`Cargo.lock`. If a run shows drift, the fix is to pin the input in
`release.yml`, for both the `binaries` job and the `reproduce` job.

### What the first run found

The first run of the job, the dry run of v0.10.3 on 2026-11-22
([run 44967350117](https://github.com/allamiro/vtop-engine/actions/runs/44967350117)),
found one difference, and it came from none of the inputs above:

| Target | `vtop-node` | `vtopctl` |
|---|---|---|
| `x86_64-unknown-linux-gnu` | identical | same size, 106,850 bytes differ |
| `aarch64-unknown-linux-gnu` | identical | same size, 25 bytes differ |
| `x86_64-unknown-linux-musl` | identical | same size, 25 bytes differ |
| `aarch64-unknown-linux-musl` | identical | same size, 25 bytes differ |
| `x86_64-apple-darwin` | identical | same size, 21 bytes differ |
| `aarch64-apple-darwin` | identical | same size, 82 bytes differ |

Both builds of each target used the same compiler (`rustc 1.100.0`), image
and packages. `vtop-node` came out byte for byte the same everywhere. Every
`vtopctl` differed in the same place: the OpenSSL it builds from source
(`ssl-vendored`, for the Kafka client) records the time it was compiled, as
`built on: <date> UTC`, and the two builds ran minutes apart. `strings` over
the two copies of each target differs in that one line and nowhere else. The
other differing bytes are the GNU build id or the Mach-O `LC_UUID` and code
signature, which are hashes of the contents, and, on
`x86_64-unknown-linux-gnu` only, about 4,000 pointers into the read-only data
that moved by a few bytes without changing the file's size: data laid out
differently around the one string that changed.

Since #731, both jobs set `SOURCE_DATE_EPOCH` to the tagged commit's time,
and OpenSSL's build records that instead of the clock, so a rebuild of the
tag gives the same `vtopctl`. Archives built before that change (v0.10.4 and
earlier) carry the build's own date in that string: their `vtopctl` does not
reproduce bit for bit, while their `vtop-node` does.

The archives themselves still differ between two builds, by the file times
`tar` and `gzip` record. Compare the binaries inside, as above.

## Maturity

VTOP is a **prototype / reference implementation**. Each release is signed,
attested and published as a full release, not a GitHub pre-release; the 0.x
series means a minor version may still break compatibility, and the release
notes say where.

Read [Known limitations](PRODUCTION_HA.md#19-known-limitations-current-code) (§19)
before depending on this in production.
