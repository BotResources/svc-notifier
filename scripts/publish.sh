#!/usr/bin/env bash
# publish.sh — check, build, and publish the svc-notifier container image
# + Helm chart to GHCR.
#
# Source of truth for publishing is a git tag: v{version} matching Cargo.toml.
# No tag = no publish.
#
# Modes:
#   (default)      — full publish: tag required, checks + build + push to GHCR
#   --dry-run      — checks + native build, NO docker, NO push, NO tag required
#   --local-image  — build a runnable Docker image for the host arch, NO push,
#                    NO tag required, NO main-branch required, checks skipped.
#                    Produces {image}:{version}-local.
#   --check-only   — checks only (fmt, clippy, tests, audit), no build
#   --skip-checks  — skip checks (CI already validated), build + push only
#
# Usage:
#   ./scripts/publish.sh                    # publish v{version} from Cargo.toml
#   ./scripts/publish.sh --local-image      # build local-runnable docker image
#   ./scripts/publish.sh --dry-run          # build binary only, no docker, no push
#   ./scripts/publish.sh --check-only       # checks only, no build
#   ./scripts/publish.sh --skip-checks      # CD mode: build + push (CI passed)
#
# Environment:
#   GHCR_TOKEN  — required for publish mode
#   GHCR_USER   — optional, defaults to git user name
#   IMAGE       — optional, override image name (default: ghcr.io/botresources/br-svc-notifier)
#   CHART_REPO  — optional, override chart OCI repo (default: oci://ghcr.io/botresources/charts)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/validate.sh"
source "$SCRIPT_DIR/lib/check.sh"
source "$SCRIPT_DIR/lib/test.sh"
source "$SCRIPT_DIR/lib/build-native-release.sh"
source "$SCRIPT_DIR/lib/build-cross-amd64.sh"
source "$SCRIPT_DIR/lib/build-cross-arm64.sh"

MODE="publish"
SKIP_CHECKS=false

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)      MODE="dry-run"; shift ;;
        --local-image)  MODE="local-image"; shift ;;
        --check-only)   MODE="check-only"; shift ;;
        --skip-checks)  SKIP_CHECKS=true; shift ;;
        --) shift; break ;;
        -*) echo "ERROR: unknown flag: $1" >&2; exit 2 ;;
        *)  echo "ERROR: unexpected argument: $1" >&2; exit 2 ;;
    esac
done

VERSION="$(crate_version)"
IMAGE_NAME="$(crate_image)"
TAG="v${VERSION}"

# ---------------------------------------------------------------------------
# --local-image — fast path for local iteration. Cross-compile for the host
# arch only, docker build, tag {image}:{version}-local, exit. Everything
# else is skipped: no main-branch requirement, no tag check, no changelog
# validation, no checks, no push.
# ---------------------------------------------------------------------------
if [ "$MODE" = "local-image" ]; then
    case "$(uname -m)" in
        arm64|aarch64)
            PLATFORM="linux/arm64"
            TARGET_TRIPLE="aarch64-unknown-linux-gnu"
            ;;
        x86_64|amd64)
            PLATFORM="linux/amd64"
            TARGET_TRIPLE="x86_64-unknown-linux-gnu"
            ;;
        *)
            error "Unsupported host arch: $(uname -m). Use --dry-run + manual docker build."
            ;;
    esac

    LOCAL_TAG="${IMAGE_NAME}:${VERSION}-local"

    info "${CRATE_NAME} ${VERSION} → ${LOCAL_TAG} (${PLATFORM})"

    cd "$REPO_ROOT"
    info "Cross-compiling for $TARGET_TRIPLE"
    cross build --release --target "$TARGET_TRIPLE"

    info "Packaging $PLATFORM image"
    docker build \
        --provenance=false \
        --platform "$PLATFORM" \
        --build-arg BIN_PATH="target/$TARGET_TRIPLE/release/${CRATE_NAME}" \
        --tag "$LOCAL_TAG" \
        "$REPO_ROOT"

    echo ""
    info "Built $LOCAL_TAG"
    info "Run it with: docker run --rm -p 8010:8010 --env-file .env $LOCAL_TAG"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 0: publish-only guards. The main-branch requirement and tag-existence
# check apply only to the actual publish path. --check-only and --dry-run
# are explicitly designed for local iteration on feature branches.
# ---------------------------------------------------------------------------
if [ "$MODE" = "publish" ]; then
    ensure_main

    if ! git tag -l "$TAG" | grep -q .; then
        error "Tag '$TAG' does not exist. Create it with: git tag $TAG && git push origin $TAG"
    fi

    # Tag must be on main
    TAG_COMMIT="$(git rev-list -n1 "$TAG")"
    if ! git merge-base --is-ancestor "$TAG_COMMIT" HEAD; then
        error "Tag '$TAG' points to a commit not on main"
    fi

    # Already published?
    if docker manifest inspect "${IMAGE_NAME}:${VERSION}" >/dev/null 2>&1; then
        info "${IMAGE_NAME}:${VERSION} already on GHCR — nothing to do"
        exit 0
    fi
fi

info "${CRATE_NAME} ${VERSION} → ${IMAGE_NAME}:${VERSION}"

# ---------------------------------------------------------------------------
# Step 2: fast validation (changelog)
# ---------------------------------------------------------------------------
validate_crate

# ---------------------------------------------------------------------------
# Step 3: checks + unit tests
# ---------------------------------------------------------------------------
if [ "$SKIP_CHECKS" = true ]; then
    info "Skipping checks (--skip-checks)"
else
    run_crate_checks
    run_crate_tests

    if [ "$MODE" = "check-only" ]; then
        info "Check-only complete — all checks passed"
        exit 0
    fi
fi

# ---------------------------------------------------------------------------
# Step 4: build binaries
# ---------------------------------------------------------------------------
if [ "$MODE" = "dry-run" ]; then
    build_native_release
else
    build_cross_amd64
    build_cross_arm64
fi

# ---------------------------------------------------------------------------
# Step 5: package into Docker images (publish mode only)
# ---------------------------------------------------------------------------
if [ "$MODE" != "dry-run" ]; then
    # --provenance=false disables attestation manifests that Docker Desktop
    # adds by default. Without this, each image becomes a manifest list
    # (instead of a plain manifest), and `docker manifest create` fails
    # with "is a manifest list".

    info "Packaging linux/amd64 image"
    docker build \
        --provenance=false \
        --platform linux/amd64 \
        --build-arg BIN_PATH="target/x86_64-unknown-linux-gnu/release/${CRATE_NAME}" \
        --tag "${IMAGE_NAME}:${VERSION}-amd64" \
        "$REPO_ROOT"

    info "Packaging linux/arm64 image"
    docker build \
        --provenance=false \
        --platform linux/arm64 \
        --build-arg BIN_PATH="target/aarch64-unknown-linux-gnu/release/${CRATE_NAME}" \
        --tag "${IMAGE_NAME}:${VERSION}-arm64" \
        "$REPO_ROOT"
fi

# ---------------------------------------------------------------------------
# Dry-run stops here
# ---------------------------------------------------------------------------
if [ "$MODE" = "dry-run" ]; then
    echo ""
    info "Dry-run complete — binary built at: target/release/${CRATE_NAME}"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 6: push to GHCR
# ---------------------------------------------------------------------------
info "Logging in to GHCR"
echo "${GHCR_TOKEN:?GHCR_TOKEN must be set}" \
    | docker login ghcr.io --username "${GHCR_USER:-$(git config user.name)}" --password-stdin

info "Pushing arch-specific images"
docker push "${IMAGE_NAME}:${VERSION}-amd64"
docker push "${IMAGE_NAME}:${VERSION}-arm64"

info "Creating multi-arch manifest"
docker manifest create "${IMAGE_NAME}:${VERSION}" \
    "${IMAGE_NAME}:${VERSION}-amd64" \
    "${IMAGE_NAME}:${VERSION}-arm64"
docker manifest push "${IMAGE_NAME}:${VERSION}"

# ---------------------------------------------------------------------------
# Step 7: package + push Helm chart
# ---------------------------------------------------------------------------
CHART_DIR="$REPO_ROOT/charts/br-svc-notifier"
CHART_REPO="${CHART_REPO:-oci://ghcr.io/botresources/charts}"

if [ ! -d "$CHART_DIR" ]; then
    error "Helm chart not found at $CHART_DIR"
fi
if ! command -v helm >/dev/null 2>&1; then
    error "helm not installed — required for chart publish"
fi

info "Logging helm into GHCR registry"
echo "$GHCR_TOKEN" \
    | helm registry login ghcr.io --username "${GHCR_USER:-$(git config user.name)}" --password-stdin

info "Packaging Helm chart"
CHART_OUT="$(mktemp -d)"
helm package "$CHART_DIR" \
    --version "$VERSION" \
    --app-version "$VERSION" \
    --destination "$CHART_OUT"

CHART_TGZ="$CHART_OUT/br-svc-notifier-${VERSION}.tgz"
[ -f "$CHART_TGZ" ] || error "chart tgz not produced at $CHART_TGZ"

info "Pushing chart to $CHART_REPO"
helm push "$CHART_TGZ" "$CHART_REPO"

echo ""
info "Published ${IMAGE_NAME}:${VERSION} (linux/amd64 + linux/arm64)"
info "Published chart ${CHART_REPO}/br-svc-notifier:${VERSION}"
