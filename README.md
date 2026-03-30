# twinning

**Protocol-faithful interface twins for fast extractor iteration and later migration proof.**

`twinning` is the spine and factory's runtime twin layer. It materializes
already-decided state behind a real protocol boundary so existing client code
can run against a fast disposable twin instead of a large production database.

It answers one narrow question:

**Can this client or extractor run against a protocol-faithful twin for the declared subset, and what committed-state artifacts does that twin produce?**

Current status:

- repository status: Phase 0 bootstrap-only implementation
- source of truth: [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md)
- current repo contents: plan + execution graph + Rust bootstrap crate + report/snapshot schemas
- first deferred direction after the v0 center: twin-pair migration proof

The live wire/runtime path is not implemented yet. The current repo is
intentionally honest: it defines the artifact contract and refusal behavior now
instead of pretending to be a working database.

---

## Current Quickstart

```bash
# Validate a schema and inspect the bootstrap state
cargo run -- postgres --schema schema.sql --json

# Write the bootstrap report and deterministic bootstrap snapshot
cargo run -- postgres --schema schema.sql --verify schema.verify.json \
  --report out/bootstrap.json \
  --snapshot out/bootstrap.twin \
  --json

# Restore a prior snapshot and re-emit bootstrap status
cargo run -- postgres --restore out/bootstrap.twin --json

# Print the operator manifest
cargo run -- --describe
```

The examples above describe the implemented Phase 0 bootstrap contract only.

### Example Output

**Human mode** (default):

```text
twinning postgres bootstrap ready
endpoint: 127.0.0.1:5432
schema: schema.sql (4 tables, 28 columns, 3 indexes, hash sha256:a1b2c3...)
storage: tournament=bounded-memory hot working set with per-twin overlay | replay=disk-backed, snapshot-backed, or delegated real-database backend
verify: constraints.verify.json (12 loaded, hash sha256:d4e5f6...)
snapshot: out/bootstrap.twin (sha256:7a8b9c...)
next: Live pgwire execution is not implemented yet. Use this build to validate schema assets, emit deterministic bootstrap artifacts, and stage the runtime boundary cleanly.
```

**JSON mode** (`--json`):

```json
{
  "version": "twinning.v0",
  "outcome": "READY",
  "mode": "bootstrap",
  "engine": "postgres",
  "host": "127.0.0.1",
  "port": 5432,
  "wire_protocol": "planned.pgwire",
  "schema": {
    "source": "schema.sql",
    "hash": "sha256:a1b2c3...",
    "table_count": 4,
    "column_count": 28,
    "index_count": 3,
    "constraint_count": 9
  },
  "verify_artifact": {
    "source": "constraints.verify.json",
    "hash": "sha256:d4e5f6...",
    "loaded": 12
  },
  "catalog": {
    "dialect": "postgres",
    "table_count": 4,
    "column_count": 28,
    "index_count": 3,
    "constraint_count": 9
  },
  "storage": {
    "tournament_mode": "bounded-memory hot working set with per-twin overlay",
    "replay_mode": "disk-backed, snapshot-backed, or delegated real-database backend",
    "hot_working_set": "memory",
    "cold_state": "shared snapshot or pluggable backing store"
  },
  "tables": {
    "public.deals": { "rows": 0, "columns": 10, "indexes": 2, "constraints": 4 },
    "public.tenants": { "rows": 0, "columns": 6, "indexes": 1, "constraints": 3 }
  },
  "constraints": {
    "not_null_violations": 0,
    "fk_violations": 0,
    "check_violations": 0,
    "unique_violations": 0
  },
  "snapshot": {
    "written_to": "out/bootstrap.twin",
    "snapshot_hash": "sha256:7a8b9c..."
  },
  "next_step": "Live pgwire execution is not implemented yet. Use this build to validate schema assets, emit deterministic bootstrap artifacts, and stage the runtime boundary cleanly."
}
```

**Refusal mode** (unimplemented path):

```json
{
  "version": "twinning.v0",
  "outcome": "REFUSAL",
  "refusal": {
    "code": "E_RUN_MODE_UNIMPLEMENTED",
    "message": "Live command execution depends on the wire-protocol server, which is not implemented in this build.",
    "detail": { "engine": "postgres", "host": "127.0.0.1", "port": 5432 },
    "next_command": "twinning postgres --schema schema.sql --report bootstrap.json --json"
  }
}
```

---

## Why twinning exists

Two loops need the same primitive:

- **Extractor iteration.** Agent-written extractors need a fast Postgres-behaving
  target that fails quickly on type coercion, constraint violations, and
  unsupported SQL shapes.
- **Migration proof.** Later, the same kernel and snapshot contract should make
  historical-query replay and twin-pair migration proof possible without
  pointing legacy workloads at production.

You provide:

- a schema DDL file or a prior snapshot
- optionally a compiled `verify.constraint.v1` artifact
- later, a real client or extractor pointed at the twin

`twinning` returns:

- one deterministic `twinning.v0` report
- one deterministic `twinning.snapshot.v0` snapshot
- explicit refusal behavior instead of silent partial semantics
- later, protocol-visible runtime behavior for the canary-defined Postgres subset

---

## What Exists Now

Implemented now:

- Postgres DDL parsing with `sqlparser-rs`
- deterministic normalized catalog construction
- verify-artifact loading and hashing
- bootstrap report generation
- bootstrap snapshot hashing and restore verification
- storage-boundary reporting for tournament mode vs replay/proof mode
- refusal envelopes for unimplemented live paths

Not implemented yet:

- pgwire listener
- SQL execution engine
- row materialization and constraint enforcement
- bounded-memory overlay backend
- live `--run` orchestration
- embedded verify execution over materialized twin state

This means the repo can currently validate bootstrap assets, but it cannot yet
run an agent's script against a live twin.

---

## Runtime Contract

Command surface:

```text
twinning postgres [OPTIONS]
```

Current options:

- `--schema <FILE>`: SQL DDL file defining tables, constraints, and indexes
- `--verify <FILE>`: compiled `verify.constraint.v1` artifact
- `--host <HOST>`: bind host (default `127.0.0.1`)
- `--port <PORT>`: bind port (default `5432`)
- `--run <COMMAND>`: planned one-shot live mode; currently refused
- `--report <FILE>`: write `twinning.v0`
- `--snapshot <FILE>`: write `twinning.snapshot.v0`
- `--restore <FILE>`: restore a prior `twinning.snapshot.v0`
- `--json`: emit machine-readable status
- `--describe`: print `operator.json`

Exit codes:

| Exit | Meaning |
|------|---------|
| `0` | clean bootstrap |
| `1` | reserved for future live verify-violation exits |
| `2` | refusal / bootstrap failure / CLI error |

Live-shape discipline when runtime lands:

- bootstrap/configuration failures stay process-level refusals
- unsupported live protocol or SQL shapes become client-visible errors
- the twin stays up after unsupported live traffic
- reports and snapshots see committed state only

---

## Spine Boundaries

`twinning` owns:

- runtime/session behavior
- materialized twin state
- snapshots
- raw twin-native metrics

`twinning` does not own:

- claim resolution or canonicalization (`decoding`, `canon`)
- constraint semantics (`verify`)
- gold-set scoring (`benchmark`)
- proceed/escalate/block decisions (`assess`)
- evidence sealing (`pack`)

Critical boundary:

- the live twin must execute embedded `verify`
- it must attach raw `verify.report.v1` payloads
- it must not export state and shell out to batch `verify`

---

## Where twinning fits

Factory role:

```text
decoding / materialize -> twinning -> embedded verify -> assess / pack
```

Current development sequencing:

```text
real Postgres proof loop first -> twinning tournament wedge -> later replay/proof modes
```

Related tools:

| If you need... | Use |
|----------------|-----|
| Claim resolution and canonical mutation selection | `decoding` |
| Constraint semantics and reports | `verify` |
| Gold-set scoring | `benchmark` |
| Policy decisions | `assess` |
| Evidence sealing | `pack` |

`twinning` only answers:

**Can we materialize and exercise candidate state behind a protocol-faithful twin boundary for the declared subset?**

---

## Artifacts

Current artifact contracts:

- `twinning.v0` — bootstrap or later runtime report
- `twinning.snapshot.v0` — content-addressed snapshot
- `twinning.canary-manifest.v0` — normative compatibility manifest for the supported subset

Checked-in manifest:

- [canaries/manifest.v0.json](./canaries/manifest.v0.json)

Schemas:

- [schemas/twinning.v0.schema.json](./schemas/twinning.v0.schema.json)
- [schemas/twinning.snapshot.v0.schema.json](./schemas/twinning.snapshot.v0.schema.json)
- [schemas/twinning.canary-manifest.v0.schema.json](./schemas/twinning.canary-manifest.v0.schema.json)

Snapshot contract highlights:

- hashes include committed-state identity and canonical relation ordering
- hashes exclude timestamps, live sessions, warnings, and debug strings
- bootstrap snapshots today are catalog-only; live committed relation contents come later

---

## Refusal Codes

Refusals are structured errors with exit code `2`. Each includes a code, message, detail, and (when applicable) a `next_command` suggesting what to run instead.

| Code | When | Next step |
|------|------|-----------|
| `E_BOOTSTRAP_SOURCE_REQUIRED` | Neither `--schema` nor `--restore` provided | Add `--schema schema.sql` |
| `E_AMBIGUOUS_BOOTSTRAP_SOURCE` | Both `--schema` and `--restore` provided | Use exactly one |
| `E_ENGINE_UNIMPLEMENTED` | Non-Postgres engine requested | Use `twinning postgres ...` |
| `E_RUN_MODE_UNIMPLEMENTED` | `--run` requested (pgwire not built yet) | Use bootstrap mode without `--run` |
| `E_IO_READ` | Input file not readable | Check path and permissions |
| `E_IO_WRITE` | Output file not writable | Check path and permissions |
| `E_SCHEMA_PARSE` | DDL parsing failed | Fix SQL syntax in schema file |
| `E_VERIFY_ARTIFACT_PARSE` | Verify artifact malformed | Regenerate with `verify` |
| `E_SNAPSHOT_VERIFY` | Snapshot hash mismatch or version error | Re-emit from schema source |
| `E_SERIALIZATION` | Internal JSON rendering failure | Report bug |

Refusals are never silent. If twinning cannot do what you asked, it tells you why and what to try instead.

---

## What twinning Is Not

| If you need... | Use | Not twinning |
|----------------|-----|--------------|
| Claim resolution, canonical mutation selection | `decoding` | twinning does not decide truth |
| Constraint semantics and validation rules | `verify` | twinning executes verify, it does not define constraints |
| Gold-set scoring | `benchmark` | twinning materializes state, it does not score it |
| Policy proceed/block decisions | `assess` | twinning reports, it does not adjudicate |
| Evidence sealing | `pack` | twinning is runtime, not archive |
| A real Postgres instance | `docker run postgres` | twinning speaks the protocol for a narrow subset, it is not a database |
| A general SQL engine | DuckDB, SQLite | twinning only supports canary-defined shapes |

twinning only answers: **Can this client run against a protocol-faithful twin for the declared subset, and what committed-state artifacts does that twin produce?**

---

## Installation

### From source (current)

```bash
git clone https://github.com/cmdrvl/twinning.git
cd twinning
cargo build --release
# Binary at target/release/twinning
```

No Homebrew tap or pre-built binaries yet. These will be added with the v0.1.0 release once the pgwire runtime is functional.

---

## Agent / CI Integration

twinning is designed for agent-driven pipelines. Parse JSON output programmatically:

```bash
# Check bootstrap readiness
result=$(cargo run -- postgres --schema schema.sql --json 2>/dev/null)
outcome=$(echo "$result" | jq -r '.outcome')

case $outcome in
  READY)    echo "Twin ready for live runtime (when implemented)" ;;
  REFUSAL)  echo "Refused: $(echo "$result" | jq -r '.refusal.code')" ;;
esac

# Exit code routing
cargo run -- postgres --schema schema.sql --json
case $? in
  0) echo "clean bootstrap" ;;
  1) echo "verify violations (future)" ;;
  2) echo "refusal or error" ;;
esac
```

---

## Troubleshooting

**`E_RUN_MODE_UNIMPLEMENTED` when using `--run`**

The pgwire server is not implemented yet. Use bootstrap mode to validate schema assets and emit artifacts. Live twin execution is the next major milestone.

**`E_SCHEMA_PARSE` on valid-looking SQL**

twinning uses `sqlparser-rs` for DDL parsing. Ensure your schema uses standard Postgres CREATE TABLE syntax. Extensions, custom types, and procedural SQL are not supported in v0.

**`E_SNAPSHOT_VERIFY` hash mismatch**

The snapshot was modified after creation, or was produced by a different twinning version. Re-emit from the original schema source with `--schema` and `--snapshot`.

**`E_ENGINE_UNIMPLEMENTED` for mysql or oracle**

Only the Postgres engine is implemented. MySQL and Oracle are declared in the CLI for forward compatibility but currently refused. See [docs/PLAN_TWINNING_FUTURES.md](./docs/PLAN_TWINNING_FUTURES.md) for the roadmap.

---

## Limitations (v0)

- **Postgres only.** MySQL, Oracle, VSAM, IMS are deferred.
- **Bootstrap only.** No live pgwire server, no SQL execution, no row materialization.
- **Canary-defined subset.** Only SQL shapes named in the [canary manifest](./canaries/manifest.v0.json) will be supported.
- **No concurrent writers.** The intended live model is single-writer admission with explicit contention refusal.
- **Tournament mode only.** Replay/proof backends are deferred to the first post-v0 artifact.

---

## Repository Plan

- Main plan: [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md)
- Futures: [docs/PLAN_TWINNING_FUTURES.md](./docs/PLAN_TWINNING_FUTURES.md)
- Agent guidance: [AGENTS.md](./AGENTS.md)
- Canary manifest: [canaries/manifest.v0.json](./canaries/manifest.v0.json)
- Crucible master plan: [PLAN_FACTORY.md](../cmdrvl-context/docs/09-plans/epistemic-spine/PLAN_FACTORY.md)

If the code, README, and plan disagree, the plan wins.

---

## Quality Gate

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
./scripts/ubs_gate.sh
```
