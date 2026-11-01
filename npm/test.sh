#!/bin/sh
set -eu

artifact_root="${TMPDIR:-/tmp}/twinning-mcp-tests"
mkdir -p "$artifact_root"
artifacts="$(mktemp -d "$artifact_root/npm-launcher-XXXXXX")"
printf 'ARTIFACTS: %s\n' "$artifacts"

if ! command -v node >/dev/null 2>&1; then
  printf '%s\n' 'SKIPPED: NPM01: Node.js is not installed' | tee "$artifacts/skips.txt"
  exit 0
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec node "$script_dir/test/launcher.test.js" "$artifacts"
