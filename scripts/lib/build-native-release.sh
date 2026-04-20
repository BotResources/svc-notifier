#!/usr/bin/env bash
# Build in release mode for the local platform.
# Optimized binary. For local perf testing and dry-run verification.
#
# Usage:
#   source scripts/lib/build-native-release.sh
#   build_native_release

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

build_native_release() {
    cd "$REPO_ROOT"

    info "[${CRATE_NAME}] Building release (native)"
    cargo build --release
    info "[${CRATE_NAME}] Built target/release/${CRATE_NAME}"
}
