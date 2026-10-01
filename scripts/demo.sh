#!/usr/bin/env bash
# End-to-end smoke test against a running stack (`docker compose up`).
# Usage: ./scripts/demo.sh [base_url]      Requires: curl, jq
set -euo pipefail

BASE="${1:-http://localhost:8080}"
USER_ID="demo-$(date +%s)"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
H=(-H "X-User-Id: ${USER_ID}")

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

wait_for() { # $1=path  -> prints final JSON once status is terminal
  local path="$1" status
  for _ in $(seq 1 120); do
    body=$(curl -fsS "${H[@]}" "${BASE}${path}")
    status=$(jq -r .status <<<"$body")
    case "$status" in
      completed|completed_with_errors|failed) echo "$body"; return 0 ;;
    esac
    printf '   status=%s ...\n' "$status" >&2
    sleep 1
  done
  echo "timed out waiting for ${path}" >&2; return 1
}

step "Readiness"
curl -fsS "${BASE}/ready" | jq -c .

step "Upload samples/orders_sample.csv as user ${USER_ID}"
IMPORT_ID=$(curl -fsS -X POST "${H[@]}" -H "Idempotency-Key: demo-upload-1" \
  -F "file=@${DIR}/samples/orders_sample.csv" "${BASE}/imports" | tee /dev/stderr | jq -r .import_id)
echo

step "Same request again with the same Idempotency-Key -> same import id"
curl -fsS -X POST "${H[@]}" -H "Idempotency-Key: demo-upload-1" \
  -F "file=@${DIR}/samples/orders_sample.csv" "${BASE}/imports" | jq -c '{import_id}'

step "Wait for import ${IMPORT_ID}"
wait_for "/imports/${IMPORT_ID}" | jq '{status,total_rows,valid_rows,invalid_rows,attempts,attempt_history}'

step "Invalid rows"
curl -fsS "${H[@]}" "${BASE}/imports/${IMPORT_ID}/errors" | jq '.items'

step "Download the original upload back from object storage"
curl -fsS "${H[@]}" "${BASE}/imports/${IMPORT_ID}/file" | head -3

step "Upload a file with a bad header (fails permanently, no retries)"
BAD_ID=$(curl -fsS -X POST "${H[@]}" -F "file=@${DIR}/samples/orders_bad_header.csv" "${BASE}/imports" | jq -r .import_id)
wait_for "/imports/${BAD_ID}" | jq '{status,attempts,last_error}'

step "Request a report (returns immediately)"
REPORT_ID=$(curl -fsS -X POST "${H[@]}" -H 'Content-Type: application/json' \
  -d "{\"import_ids\": [\"${IMPORT_ID}\"]}" "${BASE}/reports" | tee /dev/stderr | jq -r .report_id)
echo

step "Wait for report ${REPORT_ID}"
wait_for "/reports/${REPORT_ID}" | jq '{status,attempts,totals: .summary.totals, files_available}'

step "Download report as CSV"
curl -fsS "${H[@]}" "${BASE}/reports/${REPORT_ID}/download?format=csv" | tee /tmp/report.csv

if diff -q /tmp/report.csv "${DIR}/samples/expected_report_sample.csv" >/dev/null; then
  printf '\n\033[1;32mReport matches samples/expected_report_sample.csv\033[0m\n'
else
  printf '\n\033[1;31mReport differs from expected!\033[0m\n'; exit 1
fi

printf '\nFiles stay in object storage until removed explicitly, e.g.:\n'
printf '  docker compose run --rm api storage-admin list\n'
printf '  docker compose run --rm api storage-admin delete-report %s\n' "${REPORT_ID}"
