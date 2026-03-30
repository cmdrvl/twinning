#!/bin/sh
set -eu

# UBS gate for twinning.
#
# Default behavior:
# - scans Rust sources only
# - fails on CRITICAL findings only
# - reports warning/info counts for trend tracking
# - skips only in local environments without UBS installed
#
# Optional strict mode:
#   UBS_FAIL_ON_WARNING=1 ./scripts/ubs_gate.sh
#
# Optional scope override:
#   ./scripts/ubs_gate.sh src tests

if ! command -v ubs >/dev/null 2>&1; then
  if [ "${CI:-}" = "true" ] || [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    echo "UBS not installed in CI; failing UBS gate." >&2
    exit 2
  fi
  echo "UBS not installed; skipping UBS gate." >&2
  exit 0
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for UBS gate summary parsing." >&2
  exit 2
fi

if [ "$#" -eq 0 ]; then
  set -- src
fi

report_file="${UBS_GATE_REPORT_FILE:-.ubs-gate-summary.json}"
if command -v mktemp >/dev/null 2>&1; then
  report_tmp=$(mktemp "${TMPDIR:-/tmp}/twinning-ubs-gate-report.XXXXXX")
else
  report_tmp="${TMPDIR:-/tmp}/twinning-ubs-gate-$$.json"
  : >"$report_tmp"
fi
if command -v mktemp >/dev/null 2>&1; then
  log_file=$(mktemp "${TMPDIR:-/tmp}/twinning-ubs-gate-log.XXXXXX")
else
  log_file="${TMPDIR:-/tmp}/twinning-ubs-gate-$$.log"
  : >"$log_file"
fi

cleanup() {
  rm -f "$log_file" "$report_tmp"
}
trap cleanup EXIT INT TERM

rm -f "$report_file"

set +e
ubs --ci --only=rust "$@" --report-json "$report_tmp" >"$log_file" 2>&1
ubs_exit=$?
set -e

if [ "$ubs_exit" -ne 0 ]; then
  echo "UBS gate failed: scanner exited with code $ubs_exit." >&2
  if [ -s "$log_file" ]; then
    tail -n 40 "$log_file" >&2
  fi
  exit "$ubs_exit"
fi

if [ ! -s "$report_tmp" ]; then
  echo "UBS gate failed: scanner did not produce a summary report." >&2
  if [ -s "$log_file" ]; then
    tail -n 40 "$log_file" >&2
  fi
  exit 2
fi

critical=$(jq -r '.totals.critical // 0' "$report_tmp")
warning=$(jq -r '.totals.warning // 0' "$report_tmp")
info=$(jq -r '.totals.info // 0' "$report_tmp")
files=$(jq -r '.totals.files // 0' "$report_tmp")

cp "$report_tmp" "$report_file"

printf 'UBS summary: files=%s critical=%s warning=%s info=%s\n' "$files" "$critical" "$warning" "$info"

if [ "$critical" -gt 0 ]; then
  echo "UBS gate failed: critical findings present." >&2
  exit 1
fi

if [ "${UBS_FAIL_ON_WARNING:-0}" = "1" ] && [ "$warning" -gt 0 ]; then
  echo "UBS gate failed: warnings present and UBS_FAIL_ON_WARNING=1." >&2
  exit 1
fi

echo "UBS gate passed."
