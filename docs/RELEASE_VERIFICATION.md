# Installing and verifying a release

Every VTOP Engine release is signed and attested. Nothing here is optional
ceremony: the point of publishing signatures is that somebody checks them, and
an unverified download from a release page is a download from whoever could
reach the release page.

Replace `VERSION` with the release you are installing, for example `0.2.0`.

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

## Maturity

VTOP is a **prototype / reference implementation**. The 0.x series signals that
the API may still change — not that a tag is a preview. Each release is signed,
attested, and intended for use.

Read [Known limitations](PRODUCTION_HA_PLAN.md) (§21) before depending on this
in production.
