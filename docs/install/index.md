# Install

Every release ships the same two binaries, `vtopctl` (the archive engine and
operator CLI) and `vtop-node` (the cluster-plane node), in four forms. Pick the
one that matches where you will run it.

| You want to | Use | Notes |
|---|---|---|
| Run on a Linux or macOS host | [Release binaries](binaries.md) | Linux x86_64 and aarch64, macOS arm64 and x86_64. Windows is not built. |
| Run in Docker or another container runtime | [Container image](container.md) | `ghcr.io/allamiro/vtop-engine`, `linux/amd64` and `linux/arm64`. |
| Run the cluster plane on Kubernetes | [Helm chart](helm.md) | Deploys `vtop-node`, not the archive engine. |
| Use PostgreSQL, Kerberos, or change the build | [Build from source](from-source.md) | The published binaries and image do not include the `postgres` feature. |

Every artifact is signed with keyless Sigstore and ships with an SPDX SBOM.
[Verify a release](../RELEASE_VERIFICATION.md) before you run it.

!!! warning "PostgreSQL needs a source build"

    The release binaries and the container image are built with the default
    features only, and a `postgres://` state store fails at startup with
    `postgres:// state store requires a build with --features postgres`. Build
    `vtopctl` with `--features postgres` for a PostgreSQL ledger; see
    [PostgreSQL deployment](../POSTGRES_DEPLOYMENT.md).
