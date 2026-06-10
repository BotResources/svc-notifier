#!/usr/bin/env bash
# Unit tests. Integration tests need Postgres + NATS and live in tests/*.rs —
# they are gated by the `integration` job in ci.yml, not by publish.sh.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/test.sh
#   run_crate_tests

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

run_crate_tests() {
    cd "$REPO_ROOT" || exit 1

    info "[${CRATE_NAME}] Running unit tests"
    cargo test --lib
}
