#!/usr/bin/env bash
# Cross-compile for linux/amd64 (x86_64-unknown-linux-gnu).
#
# Prerequisites:
#   cargo install cross --git https://github.com/cross-rs/cross --locked
#   Docker running
#   SSH agent with a key that can fetch BotResources/br-rust-common (either
#   the default agent on a dev machine, or webfactory/ssh-agent in CI).
#   `CROSS_CONTAINER_OPTS` must mount the agent socket into the container
#   (see cd.yml and Cross.toml for the pattern).
#
# Usage:
#   source scripts/lib/build-cross-amd64.sh
#   build_cross_amd64

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

build_cross_amd64() {
    local target="x86_64-unknown-linux-gnu"
    cd "$REPO_ROOT"

    info "Cross-compiling ${CRATE_NAME} for $target"
    cross build --release --target "$target"

    local bin="target/$target/release/${CRATE_NAME}"
    if [ ! -f "$bin" ]; then
        error "${CRATE_NAME}: binary not found at $bin"
    fi
    info "[${CRATE_NAME}] Built $bin"
}
