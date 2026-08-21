#!/usr/bin/env bash

set -euo pipefail

PG_MAJOR="17"

usage() {
    cat >&2 <<'EOF'
usage: registry-docs.sh <service> <version>
                        [--binary <path>] [--pg-port <port>] [--print-only]
EOF
    exit 2
}

[[ $# -ge 2 ]] || usage
SERVICE="$1"
VERSION="$2"
shift 2
BINARY="" PG_PORT="5499" PRINT_ONLY=0
while [[ $# -gt 0 ]]; do case "$1" in
    --binary) BINARY="$2"; shift 2;;
    --pg-port) PG_PORT="$2"; shift 2;;
    --print-only) PRINT_ONLY=1; shift;;
    -h | --help) usage;;
    *) echo "error: unknown argument '$1'" >&2; usage;;
esac; done

if [[ ! "${VERSION}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "::error::registry docs: version '${VERSION}' is not a plain M.m.p semver — cannot map it to a registry PatchVersion." >&2
    exit 1
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

# shellcheck source=scripts/service-meta.sh
source scripts/service-meta.sh

registry_ids "${SERVICE}" "${VERSION}"

BR_REGISTRY_URL="${BR_REGISTRY_URL:-https://botresources.ai/graphql}"

WORK="$(mktemp -d)"
PG_CONTAINER=""
cleanup() {
    if [[ -n "${PG_CONTAINER}" ]]; then
        docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
    fi
    rm -rf "${WORK}"
}
trap cleanup EXIT

SDL_DOC="${WORK}/patch.sdl"
DB_DOC="${WORK}/patch.db.sql"
: >"${SDL_DOC}"
: >"${DB_DOC}"

extract_sdl() {
    if [[ "${SVC_META_GRAPHQL}" != "yes" ]]; then
        echo "sdl: ${SERVICE} declares no GraphQL surface — posing the empty document."
        return 0
    fi

    if [[ -z "${BINARY}" ]]; then
        for candidate in \
            "target/x86_64-unknown-linux-musl/release/${SERVICE}" \
            "target/release/${SERVICE}" \
            "target/debug/${SERVICE}"; do
            [[ -x "${candidate}" ]] && { BINARY="${candidate}"; break; }
        done
    fi
    [[ -n "${BINARY}" && -x "${BINARY}" ]] || {
        echo "::error::registry docs: no ${SERVICE} binary to extract the SDL from (looked under target/{x86_64-unknown-linux-musl/release,release,debug}). Build it first, or pass --binary."
        exit 1
    }
    [[ "${BINARY}" == /* ]] || BINARY="${REPO_ROOT}/${BINARY#./}"

    echo "sdl: running '${BINARY} schema' ..."
    if ! env -i "${BINARY}" schema >"${SDL_DOC}" 2>"${WORK}/sdl.err"; then
        echo "::error::registry docs: '${BINARY} schema' failed for ${SERVICE}. Does its main handle the \`schema\` arg?"
        cat "${WORK}/sdl.err" >&2
        exit 1
    fi
    [[ -s "${SDL_DOC}" ]] || {
        echo "::error::registry docs: '${BINARY} schema' printed nothing, but ${SERVICE} declares a GraphQL surface (scripts/service-meta.sh). Refusing to pose an empty SDL for a service that has one."
        exit 1
    }
    echo "sdl: extracted ($(wc -l <"${SDL_DOC}" | tr -d ' ') lines)"
}

pg_start() {
    command -v docker >/dev/null || { echo "::error::registry docs: docker is required to recompose the DB schema." >&2; exit 1; }
    echo "db: starting ephemeral postgres:${PG_MAJOR} container on 127.0.0.1:${PG_PORT} ..."
    PG_CONTAINER=$(docker run --rm -d \
        -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=postgres \
        -p "127.0.0.1:${PG_PORT}:5432" "postgres:${PG_MAJOR}")
    for _ in $(seq 1 60); do
        docker exec "${PG_CONTAINER}" pg_isready -U postgres >/dev/null 2>&1 && return 0
        sleep 1
    done
    echo "::error::registry docs: the postgres container never became ready." >&2
    exit 1
}

pg_sql() {
    docker exec -i "${PG_CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d "$1" >/dev/null
}

pg_dump_schema() {
    docker exec "${PG_CONTAINER}" pg_dump --schema-only --no-owner -U postgres -d "$1"
}

recompose_db_schema() {
    if [[ -z "${SVC_META_DB_NAME}" ]]; then
        echo "db: ${SERVICE} owns no schema — posing the empty document."
        return 0
    fi

    command -v sqlx >/dev/null || {
        echo "::error::registry docs: sqlx-cli is required to recompose the DB schema — it is the migrator the service itself uses (\`sqlx::migrate!\`), so it is the only one that reproduces the \`_sqlx_migrations\` bookkeeping table the deployed schema carries. Install the version pinned in cd.yml." >&2
        exit 1
    }

    pg_start
    echo "CREATE DATABASE ${SVC_META_DB_NAME};" | pg_sql postgres

    for role in ${SVC_META_DB_ROLES}; do
        echo "db: ensuring prerequisite role ${role} ..."
        pg_sql "${SVC_META_DB_NAME}" <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN
        CREATE ROLE ${role} NOLOGIN;
    END IF;
END
\$\$;
SQL
    done

    for entry in ${SVC_META_MIGRATIONS}; do
        local search_path="${entry%%=*}" source_dir="${entry#*=}" dir url encoded
        dir="${REPO_ROOT}/${source_dir}"
        [[ -d "${dir}" ]] || {
            echo "::error::registry docs: migrations source '${source_dir}' for ${SERVICE} resolved to '${dir}', which is not a directory." >&2
            exit 1
        }
        encoded="${search_path//,/%2C}"
        url="postgres://postgres@127.0.0.1:${PG_PORT}/${SVC_META_DB_NAME}?options=-c%20search_path%3D${encoded}"
        echo "db: applying ${dir} with search_path '${search_path}' ..."
        DATABASE_URL="${url}" sqlx migrate run --source "${dir}" >&2
    done

    echo "db: pg_dump --schema-only --no-owner ..."
    pg_dump_schema "${SVC_META_DB_NAME}" >"${WORK}/raw.sql"

    python3 - "${WORK}/raw.sql" >"${DB_DOC}" <<'PY'
import sys

kept = []
for line in open(sys.argv[1]).read().splitlines():
    if line.startswith("SET "):
        continue
    if line.startswith("SELECT pg_catalog.set_config"):
        continue
    if line.startswith("--"):
        continue
    if line.startswith("\\"):
        continue
    kept.append(line.rstrip())

out = []
for line in kept:
    if line == "" and (not out or out[-1] == ""):
        continue
    out.append(line)
while out and out[0] == "":
    out.pop(0)
while out and out[-1] == "":
    out.pop()
sys.stdout.write("\n".join(out) + "\n")
PY

    [[ -s "${DB_DOC}" ]] || {
        echo "::error::registry docs: the recomposed schema is empty, but ${SERVICE} declares the database '${SVC_META_DB_NAME}'. Refusing to pose an empty DB document for a service that owns a schema." >&2
        exit 1
    }
    echo "db: recomposed ($(wc -l <"${DB_DOC}" | tr -d ' ') lines)"
}

extract_sdl
recompose_db_schema

if [[ "${PRINT_ONLY}" == 1 ]]; then
    echo "===== SDL (${SERVICE} ${VERSION}) ====="
    cat "${SDL_DOC}"
    echo "===== DB SCHEMA (${SERVICE} ${VERSION}) ====="
    cat "${DB_DOC}"
    exit 0
fi

if [[ -z "${BR_REGISTRY_KEY:-}" ]]; then
    echo "::error::registry docs: BR_REGISTRY_KEY is not set — the procedural SDL and DB schema for ${SERVICE} ${VERSION} cannot be posed. Failing rather than skipping: without these two documents the patch never flips to implemented, so a silent skip ships an image the registry does not know about. Use --print-only to exercise this script without a key."
    exit 1
fi

gql() {
    python3 -c 'import json,sys; json.dump({"query": sys.argv[1], "variables": json.load(open(sys.argv[2]))}, open(sys.argv[3], "w"))' \
        "$1" "$2" "${WORK}/payload.json"
    curl -sS --fail-with-body -X POST "${BR_REGISTRY_URL}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${BR_REGISTRY_KEY}" \
        -d "@${WORK}/payload.json"
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

FROZEN=0

pose() {
    python3 -c 'import json,sys; json.dump({"sid": sys.argv[1], "maj": int(sys.argv[2]), "min": int(sys.argv[3]), "pat": int(sys.argv[4]), "doc": open(sys.argv[5]).read()}, open(sys.argv[6], "w"))' \
        "${SVC_REG_SERVICE_ID}" "${MAJOR}" "${MINOR}" "${PATCH}" "$2" "${WORK}/doc-vars.json"
    local resp verdict
    resp=$(gql "$3" "${WORK}/doc-vars.json")
    verdict=$(classify "${resp}")
    case "${verdict}" in
    OK)
        echo "posed procedural $1 ($(wc -l <"$2" | tr -d ' ') lines)"
        ;;
    ALREADY)
        FROZEN=1
        echo "::warning::registry docs: ${SERVICE} ${VERSION} is already implemented in the registry, so its documents are frozen — the $1 was NOT re-posed. Reached only when a version recorded in the registry has to be rebuilt because its image is missing from GHCR; the document already on the patch was posed from this same commit, so it is already the right one. If you believe it is wrong, that needs a new patch version — a published version is immutable."
        ;;
    *)
        echo "::error::registry docs: set procedural $1 refused — ${verdict#REFUSED }"
        exit 1
        ;;
    esac
}

pose "SDL" "${SDL_DOC}" \
    'mutation($sid: UUID!, $maj: Int!, $min: Int!, $pat: Int!, $doc: String!) { servicesSetPatchSdl(serviceId: $sid, major: $maj, minor: $min, patch: $pat, sdl: $doc, procedural: true) { ok } }'
pose "DB schema" "${DB_DOC}" \
    'mutation($sid: UUID!, $maj: Int!, $min: Int!, $pat: Int!, $doc: String!) { servicesSetPatchDbSchema(serviceId: $sid, major: $maj, minor: $min, patch: $pat, dbSchema: $doc, procedural: true) { ok } }'

if [[ "${FROZEN}" == 1 ]]; then
    echo "OK   ${SERVICE} ${VERSION}: already implemented — the documents on the patch were posed by the run that shipped this version and were left untouched."
else
    echo "OK   ${SERVICE} ${VERSION}: both procedural documents posed on ${SERVICE} ${MAJOR}.${MINOR}.${PATCH} (service ${SVC_REG_SERVICE_ID})."
fi
