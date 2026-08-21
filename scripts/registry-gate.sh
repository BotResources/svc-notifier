#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: $0 <service> <version> [<image-ref>]" >&2
    exit 2
fi

SERVICE="$1"
VERSION="$2"
IMAGE_REF="${3:-}"
BR_REGISTRY_URL="${BR_REGISTRY_URL:-https://botresources.ai/graphql}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/service-meta.sh
source scripts/service-meta.sh

if [[ -z "${BR_REGISTRY_KEY:-}" ]]; then
    echo "::error::registry gate: BR_REGISTRY_KEY is not set — the sealed-and-not-implemented release law cannot be verified for ${SERVICE} ${VERSION}. The gate refuses rather than passes: an unverifiable law is a broken one. Provision the BR_REGISTRY_KEY repository secret."
    exit 1
fi

if [[ ! "${VERSION}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "::error::registry gate: version '${VERSION}' is not a plain M.m.p semver — cannot map it to a registry PatchVersion."
    exit 1
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"

gql() {
    local payload
    payload=$(python3 -c 'import json,sys; print(json.dumps({"query": sys.argv[1], "variables": json.loads(sys.argv[2])}))' "$1" "$2")
    curl -sS --fail-with-body -X POST "${BR_REGISTRY_URL}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${BR_REGISTRY_KEY}" \
        -d "${payload}"
}

registry_ids "${SERVICE}" "${VERSION}"
SERVICE_ID="${SVC_REG_SERVICE_ID}"

VALIDATE_JSON=$(gql \
    'query($sid: UUID!, $maj: Int!, $min: Int!, $pat: Int!) { servicesValidateImplementationTarget(serviceId: $sid, major: $maj, minor: $min, patch: $pat) }' \
    "{\"sid\": \"${SERVICE_ID}\", \"maj\": ${MAJOR}, \"min\": ${MINOR}, \"pat\": ${PATCH}}")

VERDICT=$(python3 -c '
import json, sys
d = json.loads(sys.argv[1])
if d.get("errors"):
    e = d["errors"][0]
    ext = e.get("extensions") or {}
    reason = ext.get("reason") or ""
    code = ext.get("code") or ""
    msg = e.get("message") or ""
    if code == "OPERATION_VALIDATION_ERROR" and "servicesValidateImplementationTarget" in msg:
        print("UNSUPPORTED")
    else:
        print("REFUSED %s %s" % (reason or code, msg))
    sys.exit(0)
ok = (d.get("data") or {}).get("servicesValidateImplementationTarget")
print("ELIGIBLE" if ok is True else "REFUSED unexpected %r" % (ok,))
' "${VALIDATE_JSON}")

case "${VERDICT}" in
    ELIGIBLE)
        echo "OK   ${SERVICE} ${VERSION}: registry target sealed and not implemented — eligible for publication."
        exit 0
        ;;
    UNSUPPORTED)
        echo "::error::registry gate: ${BR_REGISTRY_URL} does not expose servicesValidateImplementationTarget — the sealed-and-not-implemented law cannot be verified for ${SERVICE} ${VERSION}. Either the registry was rolled back below the version that ships this query, or BR_REGISTRY_URL points somewhere that is not the production registry."
        exit 1
        ;;
    "REFUSED patch_already_implemented"*)
        if [[ -z "${IMAGE_REF}" ]]; then
            echo "::error::registry gate: ${SERVICE} ${VERSION} is already implemented in the registry. A published version is immutable — bump to the next registry-sealed version instead."
            exit 1
        fi
        echo "OK   ${SERVICE} ${VERSION}: already implemented in the registry — treating this as an idempotent re-run of the CD that shipped it (${IMAGE_REF}:${VERSION}). Whether that image exists is GHCR's answer, not the registry's: the publish step checks GHCR and skips the push when it is there."
        exit 0
        ;;
    "REFUSED not_found"*)
        echo "::error::registry gate: ${SERVICE} has no PatchVersion ${VERSION} in the Services registry. Create and seal ${VERSION} in the registry BEFORE bumping the crate — the registry version is the release authorization."
        exit 1
        ;;
    "REFUSED patch_not_sealed"*)
        echo "::error::registry gate: ${SERVICE} ${VERSION} exists in the Services registry but is NOT SEALED. Seal it before publishing."
        exit 1
        ;;
    *)
        echo "::error::registry gate: ${SERVICE} ${VERSION} refused — ${VERDICT#REFUSED }"
        exit 1
        ;;
esac
