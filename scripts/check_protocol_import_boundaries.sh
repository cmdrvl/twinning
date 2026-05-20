#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

failures=0

check_forbidden_imports() {
  local source_dir="$1"
  local forbidden_protocol="$2"
  local label="$3"

  if [ ! -d "$source_dir" ]; then
    echo "error: expected protocol directory does not exist: $source_dir" >&2
    failures=1
    return
  fi

  local pattern
  pattern="(crate::|twinning::)?protocol::[[:space:]]*(::)?[[:space:]]*(${forbidden_protocol}|\\{[^}]*${forbidden_protocol})|super::[[:space:]]*${forbidden_protocol}"

  if grep -RInE --include='*.rs' "$pattern" "$source_dir"; then
    echo "error: $label must not import or reference protocol::$forbidden_protocol" >&2
    failures=1
  fi
}

check_forbidden_imports "crates/twinning-postgres/src" "rest" "Postgres protocol"
check_forbidden_imports "crates/twinning-rest/src" "postgres" "REST protocol"

if [ "$failures" -ne 0 ]; then
  cat >&2 <<'MSG'

Protocol modules must only depend on shared layers. Move shared behavior into a
shared module instead of importing one protocol implementation from another.
MSG
  exit 1
fi

echo "Protocol import boundaries are clean."
