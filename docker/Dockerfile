# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# rdkafka with the "cmake-build" feature compiles librdkafka from source, so
# the builder needs cmake + a C toolchain, SSL/SASL/zlib headers, and libcurl
# (the SASL OAUTHBEARER/OIDC path #includes <curl/curl.h>).
FROM rust:1-bookworm AS builder

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
RUN cargo build --release --bin vtopctl

# ---- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim AS runtime

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
