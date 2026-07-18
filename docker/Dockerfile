# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# rdkafka with the "cmake-build" feature compiles librdkafka from source, so
# the builder needs cmake + a C toolchain, SSL/SASL/zlib headers, and libcurl
# (the SASL OAUTHBEARER/OIDC path #includes <curl/curl.h>).
# Pinned by digest so a rebuild cannot silently pick up a different base than
# was tested (#80). To refresh: `docker buildx imagetools inspect rust:1-bookworm`
# (and the same for debian:bookworm-slim below), update BOTH digest and the
# comment date, and let CI rebuild. Digests captured 2026-07-18.
FROM rust:1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        clang \
        libssl-dev \
        libsasl2-dev \
        libcurl4-openssl-dev \
        zlib1g-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
# --locked: the image must build exactly the dependency set in Cargo.lock; a
# lockfile drift is a build FAILURE here, never a silent re-resolution.
RUN cargo build --release --locked --bin vtopctl

# ---- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libsasl2-2 \
        libcurl4 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home vtop

COPY --from=builder /build/target/release/vtopctl /usr/local/bin/vtopctl
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Data volumes are created by the entrypoint; run as an unprivileged user.
USER vtop
WORKDIR /app

ENV VTOP_CONFIG=/app/examples/config.yaml
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["run"]
