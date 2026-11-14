# Container image

Each release publishes a multi-arch image to the GitHub Container Registry.

```bash
docker pull ghcr.io/allamiro/vtop-engine:0.8.0
```

| Property | Value |
|---|---|
| Image | `ghcr.io/allamiro/vtop-engine` |
| Tags | The release version without the `v`, for example `0.8.0`. `latest` follows the newest release whose version has no pre-release suffix such as `-rc.1`. |
| Platforms | `linux/amd64`, `linux/arm64` |
| Binaries | `/usr/local/bin/vtopctl`, `/usr/local/bin/vtop-node` |
| Base | `debian:bookworm-slim`, pinned by digest |
| User | `vtop`, uid `10001` |
| Entrypoint | `docker/entrypoint.sh`, default command `run` |
| Features | Default build: Kafka source included, no PostgreSQL. |
| Signing | Keyless cosign signature, SBOM and provenance attestations. See [Verify a release](../RELEASE_VERIFICATION.md#container-image). |

Pin a version tag in anything you deploy. `latest` moves with each release.

The tags and platforms come from the `docker-manifest` job in
[`release.yml`](../../.github/workflows/release.yml); the image contents come
from [`docker/Dockerfile`](../../docker/Dockerfile).

## Run the engine

The entrypoint runs `vtopctl <command> --config "$VTOP_CONFIG"`, where
`VTOP_CONFIG` defaults to `/app/examples/config.yaml` and the command defaults
to `run`. The image does not contain a config, so mount one. A
`streams.yaml` in the same directory is read too.

The entrypoint exits unless `/data/work` and `/data/state` are writable by
uid 10001. A new named volume is owned by root, so set its ownership once:

```bash
docker volume create vtop-data
docker run --rm --user root -v vtop-data:/data --entrypoint sh \
  ghcr.io/allamiro/vtop-engine:0.8.0 \
  -c 'mkdir -p /data/input /data/spool /data/work /data/state && chown -R 10001 /data'
```

Then check the config and start the engine:

```bash
docker run --rm \
  -v "$PWD/config:/app/examples:ro" -v vtop-data:/data \
  ghcr.io/allamiro/vtop-engine:0.8.0 validate

docker run -d --name vtop-engine \
  -v "$PWD/config:/app/examples:ro" -v vtop-data:/data \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION \
  -e VTOP_METRICS_ADDR=0.0.0.0:9090 \
  ghcr.io/allamiro/vtop-engine:0.8.0
```

Credentials come from the environment or mounted secrets, never from the
config file. The variables the engine reads are listed in
[Config basics](../configure/basics.md#environment-variables).

`docker stop` sends SIGTERM, which is an orderly stop: the engine finishes the
current source pass and flushes what it has buffered. Give it enough time with
`docker stop --time`; see [Stopping `vtopctl run`](../reference/cli.md#stopping-vtopctl-run).

## Run another command

Any `vtopctl` subcommand that takes `--config` can replace `run`, for example
`status` or `list-batches`. The entrypoint adds `--config` itself, so do not
pass it again.

For commands that take no engine config (`vtopctl segment`, `vtopctl meta`),
for a read-only job such as a scheduled `vtopctl scrub`, or for `vtop-node`,
bypass the entrypoint:

```bash
docker run --rm --entrypoint vtopctl ghcr.io/allamiro/vtop-engine:0.8.0 --version
docker run --rm --entrypoint vtop-node ghcr.io/allamiro/vtop-engine:0.8.0 --help
```

## Tools in the image

Besides the runtime libraries, the image carries `curl`, `netcat-openbsd` and
`openssl` for troubleshooting from inside a container: reading `/metrics`,
`/healthz` and `/readyz`, checking that a port is open, and inspecting
certificates. `getent hosts` checks name resolution.
