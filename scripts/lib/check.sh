#!/usr/bin/env bash
# Crate checks: fmt, clippy, audit, helm lint.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/check.sh
#   run_crate_checks

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

run_crate_checks() {
    cd "$REPO_ROOT" || exit 1

    info "Running crate checks"

    info "  cargo fmt --check"
    cargo fmt --check

    info "  cargo clippy --all-targets"
    cargo clippy --all-targets -- -D warnings

    info "Running cargo audit"
    if command -v cargo-audit >/dev/null 2>&1; then
        cargo audit
    else
        warn "cargo-audit not installed — skipping (install with: cargo install cargo-audit)"
    fi

    if command -v helm >/dev/null 2>&1; then
        info "  helm lint charts/br-svc-notifier"
        helm lint "$REPO_ROOT/charts/br-svc-notifier"
    else
        warn "helm not installed — skipping chart lint"
    fi
}
