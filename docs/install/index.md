# Install

Every release ships the same two binaries, `vtopctl` (the archive engine and
operator CLI) and `vtop-node` (the cluster-plane node), signed and with an
SBOM. Pick the form that matches where you will run them.

## Which install?

| You want to | Use | Notes |
|---|---|---|
| Run on a Linux or macOS host | [Release binaries](binaries.md) | Linux x86_64 and aarch64 (glibc 2.17 or newer, or a static musl build for any Linux), macOS 11 or newer on arm64 and x86_64. Windows is not built. |
| Run in Docker or another container runtime | [Container image](container.md) | `ghcr.io/allamiro/vtop-engine`, `linux/amd64` and `linux/arm64`. |
| Run the cluster plane on Kubernetes | [Helm chart](helm.md) | Deploys `vtop-node`, not the archive engine. |
| Use PostgreSQL, Kerberos, or change the build | [Container image](container.md) or [Build from source](from-source.md) | The container image includes the `postgres` feature from v0.11.0; the release binaries do not. Kerberos needs a source build. |

!!! warning "PostgreSQL: the container image, or a source build"

    The container image is built with the `postgres` feature from v0.11.0
    (v0.10.4 and earlier images are not). The release binaries
    are built with the default features only, and with them a `postgres://`
    state store fails at startup with `postgres:// state store requires a
    build with --features postgres`. For a PostgreSQL ledger, run the image,
    or build `vtopctl` with `--features postgres`.

<div class="grid cards" markdown>

-   **Release binaries**

    ---

    Download, verify, and run on Linux or macOS.

    [:octicons-arrow-right-24: Binaries](binaries.md)

-   **Container image**

    ---

    Tags, volumes, and running any `vtopctl` command.

    [:octicons-arrow-right-24: Container](container.md)

-   **Helm chart**

    ---

    The cluster plane on Kubernetes.

    [:octicons-arrow-right-24: Helm](helm.md)

-   **Build from source**

    ---

    Per-OS prerequisites, optional features, troubleshooting.

    [:octicons-arrow-right-24: From source](from-source.md)

</div>

Whichever you choose, [verify the release](../RELEASE_VERIFICATION.md) before
you run it. To read Kafka, check that your broker is in the
[tested Kafka releases](kafka.md).
