# twinning

<div align="center">

**Postgres-first database twin bootstrap. Parse real DDL, normalize the catalog, and emit deterministic reports and snapshots before the live wire server lands.**

</div>

---

`twinning` is the factory's speed layer: an in-memory, constraint-checked store that eventually speaks the real database wire protocol so extraction code can iterate in seconds instead of hours. The repo now has the real artifact surface and internal boundaries for that system:

- Parse schema DDL with `sqlparser-rs`
- Normalize tables, columns, keys, checks, foreign keys, and indexes into a deterministic catalog
- Load rule manifests and hash all bootstrap inputs
- Emit `twinning.v0` readiness reports and `twinning.snapshot.v0` snapshots
- Refuse unimplemented live-server paths explicitly instead of pretending to be a database

The wire protocol runtime is not implemented yet. This first cut is intentionally honest: it gives the factory a strong artifact contract and a clean Rust architecture now, without faking pgwire support.

## Quick Start

```bash
# Validate a schema and inspect the bootstrap state
twinning postgres --schema schema.sql --json

# Write the bootstrap report and an empty deterministic snapshot
twinning postgres --schema schema.sql --rules rules.json \
  --report out/bootstrap.json \
  --snapshot out/bootstrap.twin \
  --json

# Restore a prior snapshot and re-emit status
twinning postgres --restore out/bootstrap.twin --json
```

## Current Runtime Contract

`twinning <ENGINE> [OPTIONS]`

Arguments:
- `<ENGINE>`: `postgres`, `mysql`, or `oracle`

Options:
- `--schema <FILE>`: SQL DDL file defining tables, constraints, and indexes
- `--rules <FILE>`: verify-rule manifest for later coverage scoring
- `--host <HOST>`: bind host (default `127.0.0.1`)
- `--port <PORT>`: bind port (default engine-specific port)
- `--run <COMMAND>`: planned live one-shot mode; currently refused
- `--report <FILE>`: write the `twinning.v0` readiness report as JSON
- `--snapshot <FILE>`: write the deterministic `twinning.snapshot.v0` bootstrap snapshot
- `--restore <FILE>`: restore a prior `twinning.snapshot.v0` snapshot
- `--json`: emit structured JSON status instead of human-readable text
- `--describe`: print `operator.json`

Exit codes:
- `0`: clean bootstrap
- `1`: reserved for future rule violations
- `2`: refusal

## What Exists vs What Is Deferred

Implemented now:
- CLI surface aligned with the plan
- Deterministic catalog parsing for `CREATE TABLE` and `CREATE INDEX`
- Snapshot hashing and verification
- Report generation for the factory/orchestration layer

Deferred:
- pgwire listener
- SQL execution engine
- row storage and constraint enforcement
- `--run` live orchestration
- rule evaluation against in-memory state

## Repository Plan

- Plan: [docs/PLAN_TWINNING.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING.md)
- Factory master plan: [PLAN_FACTORY.md](/Users/zac/Source/cmdrvl/cmdrvl-context/docs/09-plans/epistemic-spine/PLAN_FACTORY.md)

## Quality Gate

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
