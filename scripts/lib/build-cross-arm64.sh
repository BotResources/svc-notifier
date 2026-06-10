#!/usr/bin/env bash
# Static-musl cross-compile for linux/arm64 (aarch64-unknown-linux-musl).
#
# Uses cargo-zigbuild with Zig as the C compiler. We deliberately avoid
# `cross` (DinD bind-mount fails on ARC runners, cross-rs/cross#260) and
# the system `gcc-aarch64-linux-gnu` + glibc path (runner-vs-runtime
# GLIBC skew has shipped CrashLoop releases before, see
# br-graphql-gateway#29). Zig also handles `ring` + `aws-lc-sys`
# (transitively pulled by rustls everywhere) cleanly, unlike the apt
# musl-cross toolchain for aarch64.
#
# Prerequisites (CI: installed in cd.yml; local Linux/macOS:
# `rustup target add aarch64-unknown-linux-musl`, install Zig + run
# `cargo install cargo-zigbuild`):
#
# Usage:
#   source scripts/lib/build-cross-arm64.sh
#   build_cross_arm64

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

build_cross_arm64() {
    local target="aarch64-unknown-linux-musl"
    cd "$REPO_ROOT" || exit 1

    info "Static-musl cross-compiling ${CRATE_NAME} for $target"
    cargo zigbuild --release --locked --target "$target"

    local bin="target/$target/release/${CRATE_NAME}"
    if [ ! -f "$bin" ]; then
        error "${CRATE_NAME}: binary not found at $bin"
    fi
    info "[${CRATE_NAME}] Built $bin"
}
