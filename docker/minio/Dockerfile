# syntax=docker/dockerfile:1
#
# MinIO server and client for the LAB and BENCHMARK stacks, built from MinIO's
# own source at the releases those stacks have always pinned.
#
# Why built rather than pulled: MinIO withdrew its images from Docker Hub in
# September 2026 (#507), and then from quay.io as well. Every pinned tag, and
# `latest`, is gone from both registries, so `compose up` failed in its pull
# phase before any container existed - which is how the e2e smoke job went red
# with the engine blamed for it. The source is still published (the upstream
# repositories are archived, read-only), so the same releases are built here.
# Each is checked out by COMMIT, and the checkout is verified, so a tag that
# moves cannot change what is built.
#
# LAB ONLY. The upstream repositories are archived: these releases receive no
# security fixes. This is an S3 endpoint for local testing on loopback-bound
# ports with lab credentials, never a store for real data. Moving the lab to a
# maintained S3-compatible server is a separate decision.
#
# One image carries both binaries: the `minio` service runs the server, and the
# one-shot `minio-init` service runs `mc` from a shell to create the buckets.

ARG GO_IMAGE=golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

FROM ${GO_IMAGE} AS build
# The release names and the commits they point at. Bump them together; the
# build refuses a checkout that does not match.
ARG MINIO_RELEASE=2025-09-07T16:13:09Z
ARG MINIO_COMMIT=07c3a429bfed433e49018cb0f78a52145d4bedeb
ARG MC_RELEASE=2025-08-13T08:35:41Z
ARG MC_COMMIT=7394ce0dd2a80935aded936b09fa12cbb3cb8096
# Static binaries, the toolchain in the image (never a downloaded one), paths
# stripped from the binary.
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-trimpath
WORKDIR /src

# Each checkout is fetched by commit (GitHub serves any reachable commit) and
# verified. The version stamps are the ones MinIO's own release build sets
# (buildscripts/gen-ldflags.go), so `minio --version` names the release rather
# than DEVELOPMENT.
RUN git init -q minio \
 && git -C minio fetch -q --depth 1 https://github.com/minio/minio "${MINIO_COMMIT}" \
 && git -C minio checkout -q FETCH_HEAD \
 && test "$(git -C minio rev-parse HEAD)" = "${MINIO_COMMIT}" \
 && cd minio \
 && go build -tags kqueue \
      -ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${MINIO_RELEASE}")" \
      -o /out/minio .

RUN git init -q mc \
 && git -C mc fetch -q --depth 1 https://github.com/minio/mc "${MC_COMMIT}" \
 && git -C mc checkout -q FETCH_HEAD \
 && test "$(git -C mc rev-parse HEAD)" = "${MC_COMMIT}" \
 && cd mc \
 && go build -tags kqueue \
      -ldflags "$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${MC_RELEASE}")" \
      -o /out/mc .

FROM ${RUNTIME_IMAGE}
# curl for the compose healthcheck (/minio/health/live), certificates for an
# endpoint a lab user points at TLS.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/minio /out/mc /usr/local/bin/
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
