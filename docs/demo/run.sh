#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo_root"

CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target/pane-c}
export CARGO_TARGET_DIR

cargo build --locked --offline --release -p twinning -p twinning-mcp
uv run --isolated --no-project --offline --with psycopg2-binary==2.9.13 \
  --python python3.11 -- python docs/demo/demo.py \
  --twinning-bin "$CARGO_TARGET_DIR/release/twinning" \
  --mcp-bin "$CARGO_TARGET_DIR/release/twinning-mcp"
