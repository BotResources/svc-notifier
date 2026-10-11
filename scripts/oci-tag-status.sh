#!/usr/bin/env bash
# oci-tag-status.sh <repository path> <tag>
#
# Asks the OCI registry (ghcr.io by default) whether a tag exists and
# prints exactly one word on stdout:
#   present   the registry answers HTTP 200 for the manifest
#   absent    the registry answers HTTP 404
# Any other answer (no token, 401, 5xx, network error) exits non-zero:
# an unknown state never counts as "absent".
#
# <repository path> is the path without the registry host, for example
#   botresources/br-svc-notifier          (the image)
#   botresources/charts/br-svc-notifier   (the Helm chart)
#
# Environment:
#   REGISTRY_TOKEN  a token that can pull (GITHUB_TOKEN in CD)   required
#   GITHUB_ACTOR    the user name for the token exchange          required
#   REGISTRY        the registry host (default ghcr.io)
#
# Same probe as br-graphql-gateway scripts/oci-tag-status.sh (gw#74).
# Wired in: .github/workflows/cd.yml (job `detect-chart`, job
# `publish-chart`, and the `publish` job's chart guard) and
# scripts/publish.sh (re-check just before `helm push`).

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <repository path> <tag>" >&2
    exit 2
fi
REPO_PATH="$1"
TAG="$2"
REGISTRY="${REGISTRY:-ghcr.io}"
: "${REGISTRY_TOKEN:?REGISTRY_TOKEN is required}"
: "${GITHUB_ACTOR:?GITHUB_ACTOR is required}"

TOKEN="$(curl -fsS -u "${GITHUB_ACTOR}:${REGISTRY_TOKEN}" \
    "https://${REGISTRY}/token?service=${REGISTRY}&scope=repository:${REPO_PATH}:pull" \
    | jq -r '.token')"
if [[ -z "${TOKEN}" || "${TOKEN}" == "null" ]]; then
    echo "::error::no registry token for ${REGISTRY}/${REPO_PATH}" >&2
    exit 1
fi

CODE="$(curl -sS -o /dev/null -w '%{http_code}' --head \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json' \
    "https://${REGISTRY}/v2/${REPO_PATH}/manifests/${TAG}")"

case "${CODE}" in
    200) echo "present" ;;
    404) echo "absent" ;;
    *)   echo "::error::${REGISTRY}/${REPO_PATH}:${TAG}: unexpected HTTP ${CODE}" >&2
         exit 1 ;;
esac
