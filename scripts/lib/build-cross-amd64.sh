#!/usr/bin/env bash
# Static-musl cross-compile for linux/amd64 (x86_64-unknown-linux-musl).
#
# Uses cargo-zigbuild with Zig as the C compiler. We deliberately avoid
# `cross` (DinD bind-mount fails on ARC runners, cross-rs/cross#260) and
# the system `gcc-aarch64-linux-gnu` + glibc path (runner-vs-runtime
# GLIBC skew has shipped CrashLoop releases before, see
# br-graphql-gateway#29). Static-musl decouples the binary from the
# runtime image's libc entirely.
#
# Prerequisites (CI: installed in cd.yml; local Linux/macOS:
# `rustup target add x86_64-unknown-linux-musl`, install Zig + run
# `cargo install cargo-zigbuild`):
#
# Usage:
#   source scripts/lib/build-cross-amd64.sh
#   build_cross_amd64

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

build_cross_amd64() {
    local target="x86_64-unknown-linux-musl"
    cd "$REPO_ROOT"

    info "Static-musl cross-compiling ${CRATE_NAME} for $target"
    cargo zigbuild --release --locked --target "$target"

    local bin="target/$target/release/${CRATE_NAME}"
    if [ ! -f "$bin" ]; then
        error "${CRATE_NAME}: binary not found at $bin"
    fi
    info "[${CRATE_NAME}] Built $bin"
}
