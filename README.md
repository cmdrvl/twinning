# twinning

<div align="center">

**Postgres-first interface twin bootstrap. Parse real DDL, normalize the catalog, and emit deterministic reports and snapshots before the live wire server lands.**

</div>

---

`twinning` is the factory's speed layer: a protocol-faithful twin that keeps the hot working set in memory for tournament iteration and leaves room for heavier replay/proof backends when the corpus gets large. The repo now has the real artifact surface and internal boundaries for that system:

- Parse schema DDL with `sqlparser-rs`
- Normalize tables, columns, keys, checks, foreign keys, and indexes into a deterministic catalog
- Load compiled `verify.constraint.v1` artifacts and hash all bootstrap inputs
- Emit `twinning.v0` readiness reports and `twinning.snapshot.v0` snapshots
- Refuse unimplemented live-server paths explicitly instead of pretending to be a database
- Establish the bounded-memory tournament / heavier replay split in the artifact surface

The wire protocol runtime is not implemented yet. This first cut is intentionally honest: it gives the factory a strong artifact contract and a clean Rust architecture now, without faking pgwire support.

Per `PLAN_FACTORY.md`, live `twinning` is a later scale-phase layer. The core
decode loop is still meant to be proven against real Postgres first. This repo
exists now so the twin contract is explicit before the wire/runtime work lands.

## Quick Start

```bash
# Validate a schema and inspect the bootstrap state
twinning postgres --schema schema.sql --json

# Write the bootstrap report and an empty deterministic snapshot
twinning postgres --schema schema.sql --verify schema.verify.json \
  --report out/bootstrap.json \
  --snapshot out/bootstrap.twin \
  --json

# Restore a prior snapshot and re-emit status
twinning postgres --restore out/bootstrap.twin --json
```

## Current Runtime Contract

`twinning postgres [OPTIONS]`

Options:
- `--schema <FILE>`: SQL DDL file defining tables, constraints, and indexes
- `--verify <FILE>`: compiled verify constraint artifact (`verify.constraint.v1`)
- `--host <HOST>`: bind host (default `127.0.0.1`)
- `--port <PORT>`: bind port (default `5432`)
- `--run <COMMAND>`: planned live one-shot mode; currently refused
- `--report <FILE>`: write the `twinning.v0` readiness report as JSON
- `--snapshot <FILE>`: write the deterministic `twinning.snapshot.v0` bootstrap snapshot
- `--restore <FILE>`: restore a prior `twinning.snapshot.v0` snapshot
- `--json`: emit structured JSON status instead of human-readable text
- `--describe`: print `operator.json`

V0 scope:
- Postgres wire format only
- non-Postgres adapters are future work and should not shape the current kernel

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
- Storage-boundary reporting for tournament mode vs replay/proof mode
- Verify-artifact loading and bootstrap attachment metadata
- Checked-in JSON Schemas for `twinning.v0` and `twinning.snapshot.v0`

Deferred:
- pgwire listener
- SQL execution engine
- bounded-memory overlay backend
- heavier replay/proof backends
- row storage and constraint enforcement
- `--run` live orchestration
- compiled-verify evaluation against materialized twin state

Boundary note:
- `twinning` owns runtime/session behavior, snapshots, and raw twin-native
  metrics.
- `verify` owns constraint semantics and `verify.report.v1`.
- `benchmark` owns gold-set correctness scoring.
- `assess` owns proceed/escalate/block policy decisions.
- The live twin should call embedded `verify`, not export state and shell out to
  batch `verify`.

Implementation note:
- `asupersync` is a plausible runtime substrate for the live protocol shell
  once it lands: session orchestration, cancellation correctness, deterministic
  protocol-race testing, and snapshot lifecycle.
- It is not the semantic kernel. SQL behavior, row-store semantics,
  constraint enforcement, and verify integration remain `twinning`'s job.

## Repository Plan

- Plan: [docs/PLAN_TWINNING.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING.md)
- Futures: [docs/PLAN_TWINNING_FUTURES.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING_FUTURES.md)
- Report schema: [schemas/twinning.v0.schema.json](/Users/zac/Source/cmdrvl/twinning/schemas/twinning.v0.schema.json)
- Snapshot schema: [schemas/twinning.snapshot.v0.schema.json](/Users/zac/Source/cmdrvl/twinning/schemas/twinning.snapshot.v0.schema.json)
- Factory master plan: [PLAN_FACTORY.md](/Users/zac/Source/cmdrvl/cmdrvl-context/docs/09-plans/epistemic-spine/PLAN_FACTORY.md)

## Quality Gate

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
