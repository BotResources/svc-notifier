# Runtime-only image for svc-notifier.
#
# The binary is compiled OUTSIDE Docker (by scripts/publish.sh or CI) and
# copied in. This image contains only the runtime dependencies — no Rust
# toolchain, no source code, no build artifacts.
#
# Build args:
#   BIN_PATH — path to the pre-compiled Linux binary, REQUIRED.
#              Must point to an ELF binary matching the target --platform;
#              a host (e.g. Mach-O) binary will build silently and crash
#              at runtime with `exec format error`.
#
# Local usage: prefer `./scripts/publish.sh --local-image`, which cross-
# compiles for the host arch, invokes this Dockerfile with the correct
# BIN_PATH, and tags {image}:{version}-local. Reach for a direct
# `docker build` only when you already have a Linux binary on disk:
#
#   cross build --release --target x86_64-unknown-linux-gnu
#   docker build \
#     --platform linux/amd64 \
#     --build-arg BIN_PATH=target/x86_64-unknown-linux-gnu/release/svc-notifier \
#     -t br-svc-notifier .
#
# Multi-arch release goes through scripts/publish.sh.

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

ARG BIN_PATH

COPY ${BIN_PATH} /usr/local/bin/svc-notifier
COPY migrations/ /app/migrations/

WORKDIR /app
EXPOSE 8010
ENTRYPOINT ["/usr/local/bin/svc-notifier"]
