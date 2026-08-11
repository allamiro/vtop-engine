# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# rdkafka with the "cmake-build" feature compiles librdkafka from source, so
# the builder needs cmake + a C toolchain, SSL/SASL/zlib headers, and libcurl
# (the SASL OAUTHBEARER/OIDC path #includes <curl/curl.h>).
# Pinned by digest so a rebuild cannot silently pick up a different base than
# was tested (#80). To refresh: `docker buildx imagetools inspect rust:1-bookworm`
# (and the same for debian:bookworm-slim below), update BOTH digest and the
# comment date, and let CI rebuild. Digests captured 2026-07-18.
FROM rust:1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        clang \
        libssl-dev \
        libsasl2-dev \
        libcurl4-openssl-dev \
        zlib1g-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Pinned like every other input here: an unpinned `cargo install` re-resolves
# on every cache miss and would change the recipe format under us.
RUN cargo install cargo-chef --locked --version 0.1.68

WORKDIR /build

# ---- Dependency plan --------------------------------------------------------
# The recipe is derived from the manifests alone, so this stage's OUTPUT changes
# only when a dependency changes — even though its input is the whole tree.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Dependency build -------------------------------------------------------
# WHY THIS SPLIT EXISTS. `COPY . . && cargo build` puts every dependency in a
# layer keyed on the source tree, so editing one line of Rust rebuilt all of
# them — including librdkafka, which the "cmake-build" feature compiles FROM
# SOURCE with a C toolchain. That was the bulk of a ~6 minute image build, paid
# three times per CI run (the image job, the Helm job, and the compose lab),
# and it was the critical path.
#
# Keyed on the recipe instead, this layer survives every change that does not
# touch Cargo.toml or Cargo.lock — which is nearly every commit.
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

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
FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS runtime

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
        # DECLARED, not inherited. `/usr/bin/openssl` is in the image today only
        # because `ca-certificates` depends on it — `libssl3` above provides the
        # shared library, not the binary. CI asserts openssl is present, so
        # leaving it transitive means the assertion is backed by somebody else's
        # dependency: consolidate or swap the CA package and the check breaks at
        # the exact moment the toolset silently slips, which is the failure these
        # assertions exist to catch.
        openssl \
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
