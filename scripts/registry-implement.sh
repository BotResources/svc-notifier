#!/usr/bin/env bash

set -euo pipefail

usage() { echo "usage: $0 <service> <version> <image>" >&2; exit 2; }

[[ $# -eq 3 ]] || usage
SERVICE="$1"; VERSION="$2"; IMAGE="$3"

BR_REGISTRY_URL="${BR_REGISTRY_URL:-https://botresources.ai/graphql}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"
# shellcheck source=scripts/service-meta.sh
source scripts/service-meta.sh

if [[ -z "${BR_REGISTRY_KEY:-}" ]]; then
    echo "::error::registry implement: BR_REGISTRY_KEY is not set — the published image ${IMAGE} cannot be recorded in the Services registry, so ${SERVICE} ${VERSION} will never flip to implemented. Failing rather than skipping: a release the registry does not know about is exactly what the release law exists to prevent. Record it manually and re-run this script with the key."
    exit 1
fi

if [[ ! "${VERSION}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "::error::registry implement: version '${VERSION}' is not a plain M.m.p semver — cannot map it to a registry PatchVersion."
    exit 1
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"

registry_ids "${SERVICE}" "${VERSION}"
SERVICE_ID="${SVC_REG_SERVICE_ID}"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

gql() {
    python3 -c 'import json,sys; json.dump({"query": sys.argv[1], "variables": json.load(open(sys.argv[2]))}, open(sys.argv[3], "w"))' \
        "$1" "$2" "${WORK}/payload.json"
    curl -sS --fail-with-body -X POST "${BR_REGISTRY_URL}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${BR_REGISTRY_KEY}" \
        -d "@${WORK}/payload.json"
}

coord_vars() {
    python3 -c '
import json, sys
a = sys.argv[5:-1]
v = {"sid": sys.argv[1], "maj": int(sys.argv[2]), "min": int(sys.argv[3]), "pat": int(sys.argv[4])}
v.update({a[i]: a[i + 1] for i in range(0, len(a), 2)})
json.dump(v, open(sys.argv[-1], "w"))
' "${SERVICE_ID}" "${MAJOR}" "${MINOR}" "${PATCH}" "$@" "${WORK}/vars.json"
    echo "${WORK}/vars.json"
}

classify() {
    python3 -c '
import json, sys
d = json.loads(sys.argv[1])
if not d.get("errors"):
    print("OK")
    sys.exit(0)
e = d["errors"][0]
ext = e.get("extensions") or {}
reason = ext.get("reason") or ""
if reason == "patch_already_implemented":
    print("ALREADY")
    sys.exit(0)
print("REFUSED %s (%s)" % (reason or e.get("message"), ext.get("code")))
' "$1"
}

RESP=$(gql 'mutation($sid: UUID!, $maj: Int!, $min: Int!, $pat: Int!, $img: String!) { servicesRecordPatchImage(serviceId: $sid, major: $maj, minor: $min, patch: $pat, image: $img) { ok } }' \
    "$(coord_vars img "${IMAGE}")")
RECORD_VERDICT=$(classify "${RESP}")
case "${RECORD_VERDICT}" in
OK)
    echo "recorded image ${IMAGE} on ${SERVICE} ${MAJOR}.${MINOR}.${PATCH}"
    ;;
ALREADY)
    echo "OK   ${SERVICE} ${VERSION} is already implemented in the Services registry — the run that shipped it recorded the image, and an implemented patch is frozen. Nothing to record; this is the idempotent re-run path."
    exit 0
    ;;
*)
    echo "::error::registry implement: record image refused — ${RECORD_VERDICT#REFUSED }"
    exit 1
    ;;
esac

PROBE='query($sid: UUID!, $maj: Int!, $min: Int!, $pat: Int!) { servicesValidateImplementationTarget(serviceId: $sid, major: $maj, minor: $min, patch: $pat) }'
RESP=$(gql "${PROBE}" "$(coord_vars)" 2>/dev/null) || {
    echo "::warning::registry implement: could not reach the registry to confirm the implemented flip for ${SERVICE} ${VERSION}. The image write succeeded, so the flip is expected to have happened. Confirm manually: servicesValidateImplementationTarget(serviceId: \"${SERVICE_ID}\", major: ${MAJOR}, minor: ${MINOR}, patch: ${PATCH}) — a 'patch_already_implemented' refusal means it did."
    exit 0
}
python3 -c '
import json, sys
d = json.loads(sys.argv[1])
service, version, sid, maj, mnr, pat = sys.argv[2:8]
coords = "serviceId: \"%s\", major: %s, minor: %s, patch: %s" % (sid, maj, mnr, pat)
if d.get("errors"):
    e = d["errors"][0]
    ext = e.get("extensions") or {}
    reason = ext.get("reason") or ""
    if reason == "patch_already_implemented":
        print("OK   %s %s is now implemented in the Services registry — the target is no longer eligible, which is exactly what recording the image was meant to achieve." % (service, version))
        sys.exit(0)
    print("::warning::registry implement: the flip confirmation for %s %s is inconclusive — the registry refused the probe with %s (%s). The image write succeeded, so the flip is expected; confirm manually: servicesValidateImplementationTarget(%s) — a \"patch_already_implemented\" refusal means it happened."
          % (service, version, reason or e.get("message"), ext.get("code"), coords))
    sys.exit(0)
if (d.get("data") or {}).get("servicesValidateImplementationTarget") is True:
    print("::warning::registry implement: %s %s did NOT flip to implemented — it is still a valid implementation target, so the registry is still missing a piece. The image was recorded by the write above, which leaves the procedural SDL and the procedural DB schema as the candidates (the publish job poses both: scripts/registry-docs.sh). The image is published either way; this job does not gate it. Re-run the CD run for this version once the cause is fixed — every gesture is idempotent."
          % (service, version))
    sys.exit(0)
print("::warning::registry implement: the flip confirmation for %s %s is inconclusive — the probe returned an unexpected body. The image write succeeded, so the flip is expected; confirm manually: servicesValidateImplementationTarget(%s)."
      % (service, version, coords))
' "${RESP}" "${SERVICE}" "${VERSION}" "${SERVICE_ID}" "${MAJOR}" "${MINOR}" "${PATCH}"
