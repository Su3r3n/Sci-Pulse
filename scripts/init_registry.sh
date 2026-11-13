#!/usr/bin/env sh
# Idempotently initializes Confluent Schema Registry with all SciPulse v1 contracts
# and enforces BACKWARD compatibility globally and per subject.
# Note: raw.code.github uses TopicRecordNameStrategy (<topic>-<record_fullname>),
# while all other topics use TopicNameStrategy (<topic>-value).

set -eu

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL%/}"
MAX_RETRIES="${MAX_RETRIES:-60}"
RETRY_INTERVAL="${RETRY_INTERVAL:-2}"

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

if [ -z "${CONTRACTS_DIR:-}" ]; then
  if [ -d "$REPO_ROOT/contracts" ]; then
    CONTRACTS_DIR="$REPO_ROOT/contracts"
  elif [ -d "/contracts" ]; then
    CONTRACTS_DIR="/contracts"
  else
    echo "ERROR: contracts directory not found (set CONTRACTS_DIR)" >&2
    exit 1
  fi
fi

CONTENT_TYPE="Content-Type: application/vnd.schemaregistry.v1+json"

wait_for_registry() {
  attempt=1
  while [ "$attempt" -le "$MAX_RETRIES" ]; do
    if curl -fsS "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
      echo "Schema Registry is ready at ${SCHEMA_REGISTRY_URL}"
      return 0
    fi
    echo "Waiting for Schema Registry (${attempt}/${MAX_RETRIES})..."
    attempt=$((attempt + 1))
    sleep "$RETRY_INTERVAL"
  done
  echo "ERROR: Schema Registry did not become ready at ${SCHEMA_REGISTRY_URL}" >&2
  exit 1
}

build_schema_payload() {
  schema_file="$1"
  escaped=$(awk '{
    sub(/\r$/, "")
    gsub(/\\/, "\\\\")
    gsub(/"/, "\\\"")
    printf "%s\\n", $0
  }' "$schema_file")
  printf '{"schema":"%s"}' "$escaped"
}

set_global_compatibility() {
  curl -fsS -X PUT \
    -H "$CONTENT_TYPE" \
    --data '{"compatibility":"BACKWARD"}' \
    "${SCHEMA_REGISTRY_URL}/config" >/dev/null
  echo "Global compatibility set to BACKWARD"
}

register_subject() {
  subject="$1"
  schema_file="$2"
  full_path="${CONTRACTS_DIR}/${schema_file}"

  if [ ! -f "$full_path" ]; then
    echo "ERROR: schema file not found: ${full_path}" >&2
    exit 1
  fi

  payload=$(build_schema_payload "$full_path")

  # Check if subject already has at least one registered version.
  latest_code=$(curl -sS -o /dev/null -w '%{http_code}' \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest")

  if [ "$latest_code" = "200" ]; then
    compat_resp=$(curl -fsS -X POST \
      -H "$CONTENT_TYPE" \
      --data "$payload" \
      "${SCHEMA_REGISTRY_URL}/compatibility/subjects/${subject}/versions/latest")
    case "$compat_resp" in
      *'"is_compatible":true'*)
        ;;
      *)
        echo "ERROR: schema ${schema_file} is not BACKWARD compatible with ${subject}: ${compat_resp}" >&2
        exit 1
        ;;
    esac
  fi

  # Register schema idempotently (identical schema returns existing id).
  reg_resp=$(curl -fsS -X POST \
    -H "$CONTENT_TYPE" \
    --data "$payload" \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions")

  # Enforce BACKWARD compatibility explicitly on the subject.
  curl -fsS -X PUT \
    -H "$CONTENT_TYPE" \
    --data '{"compatibility":"BACKWARD"}' \
    "${SCHEMA_REGISTRY_URL}/config/${subject}" >/dev/null

  echo "Registered ${subject} <- ${schema_file} (${reg_resp})"
}

wait_for_registry
set_global_compatibility

# TopicNameStrategy subjects
register_subject "raw.papers.openalex-value" "paper.v1.avsc"
register_subject "raw.papers.arxiv-value" "paper.v1.avsc"
register_subject "raw.papers.inspire-value" "paper.v1.avsc"
register_subject "raw.code.events-value" "code_event.v1.avsc"
register_subject "raw.code.zenodo-value" "software_record.v1.avsc"
register_subject "dlq.papers-value" "dlq_envelope.v1.avsc"
register_subject "dlq.code-value" "dlq_envelope.v1.avsc"
register_subject "load.synthetic-value" "synthetic_event.v1.avsc"

# TopicRecordNameStrategy subjects for raw.code.github
register_subject "raw.code.github-org.scipulse.v1.RepoSnapshot" "repo_snapshot.v1.avsc"
register_subject "raw.code.github-org.scipulse.v1.Release" "release.v1.avsc"

echo "All 10 Schema Registry subjects initialized with BACKWARD compatibility."
