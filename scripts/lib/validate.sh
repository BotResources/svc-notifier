#!/usr/bin/env bash
# Validate the crate before any slow work: changelog entry for current version.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/validate.sh
#   validate_crate

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

validate_crate() {
    local version
    version="$(crate_version)"

    info "[${CRATE_NAME}] Validating $version"

    local changelog="$REPO_ROOT/CHANGELOG.md"
    if [ ! -f "$changelog" ]; then
        error "CHANGELOG.md missing"
    fi
    if ! grep -qE "^## ${version}( |$)" "$changelog"; then
        error "CHANGELOG.md has no '## $version' heading"
    fi

    info "[${CRATE_NAME}] Validation passed"
}
