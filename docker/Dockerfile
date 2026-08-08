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
# BOTH binaries, matching what the release archives ship. The image used to
# build only vtopctl, which made it a steering wheel without the car: the
# Helm chart runs `vtop-node node`, so every pod exec'd a binary that was not
# there. Anyone reaching for the container — which is what a Kubernetes
# deployment does — got the operator CLI and no engine.
RUN cargo build --release --locked --bin vtopctl --bin vtop-node

# ---- Runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime

# Two groups, kept separate on purpose.
#
# RUNTIME: what the binaries link against. Removing any of these breaks the
# engine.
#
# TROUBLESHOOTING: what an operator needs to answer "why can this node not
# reach that one". Deliberately included rather than left out, because the
# alternative was measured and it costs more than the bytes.
#
# `curl` is the one that matters. /metrics, /healthz and /readyz are plain HTTP
# and are the ONLY endpoints a liveness question is asked of — `vtopctl meta
# status` and `node status` speak VTPM and the replica plane over mTLS and
# cannot read them, so there is no Rust-native substitute in this image to
# point at instead. Without it, reading readiness inside a pod is
# `curl: not found`, and the workaround is a port-forward per pod per check —
# which is exactly how diagnosing #292 turned into chasing `tls handshake eof`
# and `connection refused` that had nothing to do with the bug. It also does
# the certificate work this project needs constantly: `--cacert`, `-v` for the
# negotiated suite and peer chain, and proxy handling.
#
# The cost is smaller than it looks: `libcurl4` is ALREADY here for
# librdkafka's SASL OAUTHBEARER path, so this adds the binary, not the stack.
#
# `netcat-openbsd` answers the second question in a peering failure: is that
# port open.
#
# The third — does this name resolve — needs NOTHING added. `getent hosts` is
# already here from libc, and it is the BETTER tool for this codebase: it
# resolves through the same nsswitch path the engine itself uses, so it answers
# the question actually being asked ("can this process resolve the peer") rather
# than "can a DNS client reach a DNS server". `host` and `dig` were measured and
# rejected: `bind9-host` drags in `libicu72`, which is 36 MB on its own — more
# than a third of the image — to answer a question libc already answers more
# faithfully.
#
# Hardening note: a debug binary in a production image is a real trade. The
# answer is the one SECURITY_MODEL.md already gives — run read-only with
# dropped capabilities as an unprivileged user, which this image does. A
# separate `-debug` tag was considered and rejected for now: its failure mode
# is the tools being absent at exactly the moment an incident makes you want
# them.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libsasl2-2 \
        libcurl4 \
        curl \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home vtop

COPY --from=builder /build/target/release/vtopctl /usr/local/bin/vtopctl
COPY --from=builder /build/target/release/vtop-node /usr/local/bin/vtop-node
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Data volumes are created by the entrypoint; run as an unprivileged user.
USER vtop
WORKDIR /app

ENV VTOP_CONFIG=/app/examples/config.yaml
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["run"]
