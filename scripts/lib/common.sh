#!/usr/bin/env bash
# Shared helpers for all publish scripts.

info()  { echo "==> $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }
warn()  { echo "WARN: $*" >&2; }

# Read a simple TOML string value: key = "value"
toml_string() {
    local file="$1" key="$2"
    grep -E "^${key}\s*=" "$file" | head -1 | sed 's/.*"\(.*\)".*/\1/'
}

# Repo root — resolved once, available everywhere.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Service-crate metadata — the service crate lives at the workspace root.
export CRATE_NAME="svc-notifier"

crate_version() { toml_string "$REPO_ROOT/Cargo.toml" "version"; }

# Image name — `br-` prefix marks this as a portable BotResources service
# (cross-project reuse). Override via IMAGE env var for non-default registries.
crate_image()   { echo "${IMAGE:-ghcr.io/botresources/br-svc-notifier}"; }

# Check we're on main and up to date with origin.
# In CI (detached HEAD on tag checkout), verify the tag commit is on main.
ensure_main() {
    cd "$REPO_ROOT" || exit 1
    git fetch origin main --tags --quiet

    local branch
    branch="$(git rev-parse --abbrev-ref HEAD)"

    if [ "$branch" = "HEAD" ]; then
        # Detached HEAD (CI tag checkout) — verify commit is on main
        if ! git merge-base --is-ancestor HEAD origin/main; then
            error "Detached HEAD is not on main"
        fi
        info "Detached HEAD — commit is on main (CI mode)"
    elif [ "$branch" = "main" ]; then
        local local_sha remote_sha
        local_sha="$(git rev-parse HEAD)"
        remote_sha="$(git rev-parse origin/main)"
        if [ "$local_sha" != "$remote_sha" ]; then
            info "Local main is behind origin — pulling"
            git pull origin main --ff-only --quiet
        fi
    else
        error "Must be on main (currently on '$branch')"
    fi
}
