# twinning

**Protocol-faithful interface twins for fast extractor iteration and later migration proof.**

`twinning` is the spine and factory's runtime twin layer. It materializes
already-decided state behind a real protocol boundary so existing client code
can run against a fast disposable twin instead of a large production database.

It answers one narrow question:

**Can this client or extractor run against a protocol-faithful twin for the declared subset, and what committed-state artifacts does that twin produce?**

Current status:

- repository status: Phase 0 bootstrap + live `run_once` and `--serve` shells for the proven Postgres subset, plus REST, MCP, and Snowflake wire protocol twins
- source of truth: [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md)
- workspace: `crates/twinning-kernel`, `crates/twinning-postgres`, `crates/twinning-rest`, `crates/twinning-snowflake`
- first deferred direction after the v0 center: twin-pair migration proof

Protocol twins available:

| Subcommand | Feature flag | Protocol |
|-----------|-------------|---------|
| `twinning postgres` | (default) | pgwire — canary-defined SQL subset |
| `twinning rest` | `rest` | OpenAPI-spec-driven HTTP REST |
| `twinning mcp` | `mcp` | Model Context Protocol (JSON-RPC 2.0 over HTTP + stdio) |
| `twinning snowflake` | `snowflake` | Snowflake HTTP wire protocol (Arrow IPC results) |
| `twinning port` | `rest` | Dual REST twins for client migration proof |

Build with all twins: `cargo build --features all`

---

## Current Quickstart

```bash
# Build with all protocol twins enabled
cargo build --features all

# Validate a schema and inspect the bootstrap state
cargo run -- postgres --schema schema.sql --json

# Write the bootstrap report and deterministic bootstrap snapshot
cargo run -- postgres --schema schema.sql --verify schema.verify.json \
  --report out/bootstrap.json \
  --snapshot out/bootstrap.twin \
  --json

# Capture declared rows from a disposable/source Postgres database into a snapshot
cargo run -- postgres --schema schema.sql \
  --materialize-source-url "$SOURCE_DATABASE_URL" \
  --report out/materialized.json \
  --snapshot out/materialized.twin \
  --json

# Export a seed contract for an external agent to fill, then import seed rows
cargo run -- postgres --schema schema.sql \
  --export-seed-contract out/seed-contract.jsonl \
  --json
cargo run -- postgres --schema schema.sql \
  --seed out/seed-data.jsonl \
  --snapshot out/seeded.twin \
  --json

# Restore a prior snapshot and re-emit bootstrap status
cargo run -- postgres --restore out/bootstrap.twin --json

# Compare two committed-state snapshots over one proof query fixture
cargo run -- --json proof twin-pair \
  --left snapshots/legacy.twin \
  --right snapshots/candidate.twin \
  --queries tests/fixtures/differential/twin_pair_migration_proof/cases.json \
  --report out/twin-pair-proof.json

# Run manifest-first twin-pair proof orchestration over restore-backed or schema-load endpoints
cargo run -- --json proof twin-pair orchestrate \
  --manifest proof-run.json \
  --report out/twin-pair-proof.json \
  --bundle-dir out/twin-pair-proof/

# Run one child command against the live run_once shell
# The child must connect to the configured --host/--port.
cargo run -- postgres --schema schema.sql --run 'your-client-command' --json

# REST twin — serve OpenAPI-spec-driven HTTP endpoints
cargo run --features rest -- rest --spec openapi.yaml --serve --report out/rest.json

# REST response stubs — deterministic contract responses declared in x-twinning
cargo run --features rest -- rest \
  --spec tests/fixtures/rest/openfigi_v2_v3/response-stub-schema.yaml \
  --server-variable basePath=v3 \
  --auth-mode shape \
  --serve

# MCP twin — serve Model Context Protocol over HTTP (or stdio)
cargo run --features mcp -- mcp --server 'npx @scope/mcp-server' --report out/mcp.json
cargo run --features mcp -- mcp --manifest manifest.json --serve

# Snowflake wire twin — serve Snowflake HTTP protocol with Arrow IPC results
cargo run --features snowflake -- snowflake --schema schema.sql --serve --report out/sf.json

# Run a standalone interactive twin until SIGINT/SIGTERM
cargo run -- postgres --schema schema.sql \
  --serve \
  --report out/interactive.json \
  --snapshot out/interactive.twin \
  --json

# Opt in to a redacted query trace artifact for a live run_once session
cargo run -- postgres --schema schema.sql \
  --run 'your-client-command' \
  --query-trace out/query-trace.json \
  --json

# Print the operator manifest
cargo run -- --describe

# Inspect the read-only doctor surface
cargo run -- doctor health --json
cargo run -- doctor capabilities --json
cargo run -- doctor --robot-triage
```

The first materialization example shells out to `psql` and captures only the
tables declared by the schema. Use it against a source account/database that is
safe for deterministic reads; `TWINNING_PSQL_BIN` can point at a specific
`psql` binary in test harnesses. The `--run` and `--serve` examples use the
current live pgwire shell and final artifact path.

Seed JSONL is a contract/data pair, not a generator. `--export-seed-contract`
writes a deterministic schema-shaped template for another agent to fill.
`--seed` imports filled rows through the Postgres kernel and constraint executor
before bootstrap finalization or live startup, so seeded rows become committed
state in reports, snapshots, and the first `run_once` client session. V1 is
schema-backed only: seed import/export cannot be combined with `--restore`, and
`--seed` cannot be combined with `--materialize-source-url`.

### Example Output

**Human mode** (default):

```text
twinning postgres bootstrap ready
endpoint: 127.0.0.1:5432
schema: schema.sql (4 tables, 28 columns, 3 indexes, hash sha256:a1b2c3...)
storage: tournament=bounded-memory hot working set with per-twin overlay | replay=disk-backed, snapshot-backed, or delegated real-database backend
verify: constraints.verify.json (12 loaded, hash sha256:d4e5f6...)
snapshot: out/bootstrap.twin (sha256:7a8b9c...)
next: Bootstrap mode validated the schema assets and deterministic artifact path. Use --run or --serve to exercise the declared live Postgres subset, or stay in bootstrap mode while broader protocol and SQL coverage lands.
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
  "next_step": "Bootstrap mode validated the schema assets and deterministic artifact path. Use --run or --serve to exercise the declared live Postgres subset, or stay in bootstrap mode while broader protocol and SQL coverage lands."
}
```

Bootstrap reports show the validation-oriented `next_step` string above. When
`--run` is present, the report switches to `mode: "run_once"` and points
operators at the final `run` metadata instead.

**Run-once mode** (`--run <COMMAND>`):

```json
{
  "version": "twinning.v0",
  "outcome": "FAIL",
  "mode": "run_once",
  "engine": "postgres",
  "host": "127.0.0.1",
  "port": 5432,
  "run": {
    "command": "exit 7",
    "exit_code": 7,
    "timed_out": false
  },
  "next_step": "Inspect the run metadata, fix the child failure or drift, and rerun the candidate against the twin."
}
```

**Interactive mode** (`--serve`):

```json
{
  "version": "twinning.v0",
  "outcome": "READY",
  "mode": "interactive",
  "engine": "postgres",
  "host": "127.0.0.1",
  "port": 5432,
  "next_step": "Interactive mode finalized committed-state artifacts after shutdown. Inspect the snapshot and embedded verify payload before reusing this twin state."
}
```

**Refusal mode** (bootstrap/configuration failure):

```json
{
  "version": "twinning.v0",
  "outcome": "REFUSAL",
  "refusal": {
    "code": "E_AMBIGUOUS_BOOTSTRAP_SOURCE",
    "message": "Use exactly one bootstrap source: --schema or --restore, not both.",
    "detail": { "engine": "postgres" },
    "next_command": "twinning postgres --schema schema.sql --json"
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

- one deterministic `twinning.v0` report over the declared committed state
- one content-addressed `twinning.snapshot.v0` snapshot with canonical
  committed-state bytes; envelope metadata such as creation time and restore
  lineage remains explicit but is not the equality surface
- explicit refusal behavior instead of silent partial semantics
- later, protocol-visible runtime behavior for the canary-defined Postgres subset

---

## What Exists Now

Implemented now:

**Postgres twin** (default):
- DDL parsing with `sqlparser-rs`, deterministic normalized catalog construction
- verify-artifact loading, bootstrap report and snapshot generation
- pgwire listener + startup/session shell for the declared live subset
- normalized read/mutation IR, row-store execution for canary-defined SQL shapes
- live `--run` and `--serve` modes with SIGINT/SIGTERM finalization
- embedded verify execution, twin-pair migration proof orchestration

**REST twin** (`--features rest`):
- OpenAPI-spec-driven HTTP endpoint generation
- auth shape enforcement (bearer/apiKey), chaos injection, routing modes
- deterministic `x-twinning.response-stubs` for fixture-backed contract responses
- canary manifest validation, startup report, `port` dual-twin migration proof

**MCP twin** (`--features mcp`):
- JSON-RPC 2.0 dispatcher over HTTP and stdio transports
- Catalog from live MCP server introspection or static manifest
- All 7 MCP methods: `initialize`, `tools/list`, `tools/call`, `resources/list`, `resources/read`, `prompts/list`, `prompts/get`
- Typed stub responses for tools with output schemas; `unsupported_shape` for stateful/schema-less tools
- Auth enforcement, session log, startup report

**Snowflake wire twin** (`--features snowflake`):
- Snowflake HTTP REST protocol (5 core endpoints)
- Arrow IPC `rowsetBase64` result encoding with full Snowflake type fidelity
- `SHOW` and `DESCRIBE` responses for JDBC/DBeaver/dbt compatibility
- DDL catalog ingestion, query dispatch with `SqlRoute` classification
- Session lifecycle, startup report, materialize path

Not implemented yet:

- SQL/session shapes outside the checked-in canary manifests
- Joined reads (explicit protocol-visible refusal in current manifest)
- Concurrent writers or multi-writer semantics
- Heavier replay/proof backends beyond the in-memory snapshot-backed proof path

This means the repo can validate bootstrap assets, run one child command, or
serve interactive clients for the proven subset, but it still refuses broader
live traffic instead of pretending to be a complete database.

---

## Runtime Contract

Command surface:

```text
twinning postgres [OPTIONS]           # pgwire twin (default)
twinning rest [OPTIONS]               # REST twin (--features rest)
twinning mcp [OPTIONS]                # MCP twin (--features mcp)
twinning snowflake [OPTIONS]          # Snowflake wire twin (--features snowflake)
twinning port [OPTIONS]               # dual REST twins (--features rest)
twinning proof twin-pair [OPTIONS]    # twin-pair migration proof
twinning doctor [SUBCOMMAND]          # read-only health/capabilities
```

Current options:

- `--schema <FILE>`: SQL DDL file defining tables, constraints, and indexes
- `--verify <FILE>`: compiled `verify.constraint.v1` artifact
- `--declaration <FILE>`: optional `twinning.catalog-declaration.v0` parent catalog subset identity
- `--host <HOST>`: bind host (default `127.0.0.1`)
- `--port <PORT>`: bind port (default `5432`)
- `--run <COMMAND>`: run one child command against the live pgwire shell, then freeze final artifacts
- `--serve`: run a standalone interactive pgwire shell until SIGINT/SIGTERM, then freeze final artifacts
- `--report <FILE>`: write `twinning.v0`
- `--snapshot <FILE>`: write `twinning.snapshot.v0`
- `--query-trace <FILE>`: write a redacted live query trace artifact in `--run` or `--serve`
- `--restore <FILE>`: restore a prior `twinning.snapshot.v0`
- `--export-seed-contract <FILE>`: write `twinning.seed-contract.v0` JSONL for the schema-loaded Postgres catalog
- `--seed <FILE>`: import filled `twinning.seed-data.v0` JSONL as committed Postgres state
- `--materialize-source-url <URL>`: capture declared source rows into the final report/snapshot
- `--json`: emit machine-readable status
- `--describe`: print `operator.json`

REST response stubs are declared inside the OpenAPI document under
`x-twinning.response-stubs` and are consumed through the existing
`twinning rest --spec <FILE>` path. They are deterministic contract fixtures:
the runtime matches method, mounted path, and optional canonical JSON request
body equality, then returns the declared status, headers, and JSON, text, or
file-backed body after auth and chaos checks. File-backed bodies use
`body-file` and are validated at startup. They are not generated dummy data, not
a live provider simulator, and not a separate REST `--seed` mode.

Use separate specs or thin spec overlays to model separate API scenarios for the
same upstream service, such as happy path, no-match, malformed/error, and
ambiguity twins. The OpenFIGI fixture at
`tests/fixtures/rest/openfigi_v2_v3/response-stub-schema.yaml` demonstrates a
local `/v3/mapping` response that returns the OpenFIGI top-level batch array
instead of the generic `{"data":null,"warning":null}` fallback.

Managed paths:

- Canonical config location: `~/.cmdrvl/config/twinning/config.toml`
- Canonical tool-managed state directory: `~/.cmdrvl/state/twinning/`
- Canonical rebuildable cache directory: `~/.cmdrvl/cache/twinning/`
- Migration ledger and deprecation notices: `~/.cmdrvl/migrations/applied.jsonl`
  and `~/.cmdrvl/notices/deprecated-paths.jsonl`
- Current inventory has no legacy twinning-managed home or repo config path to
  copy. Reports, snapshots, query traces, seed contracts, proof bundles, and
  protocol session reports are written only when the operator supplies an
  explicit output path.

Read-only doctor surface:

- `doctor health --json`: emit `twinning.v0` health diagnostics without reading
  schema, snapshot, or verify files
- `doctor capabilities --json`: list machine-readable commands, output
  contracts, detector fixtures, and safety boundaries
- `doctor robot-docs`: print concise agent-facing command notes
- `doctor --robot-triage`: emit structured follow-up findings

`doctor --fix` is intentionally not available. The read-only doctor detector
catalog is fixture-backed, and any future fix mode needs verbatim backups,
explicit inverses, and undo tests before it is exposed.

Exit codes:

| Exit | Meaning |
|------|---------|
| `0` | clean bootstrap, `run_once`, or `--serve` completion without embedded verify failure |
| `1` | live finalization completed but embedded verify reported `FAIL` |
| `2` | refusal / bootstrap failure / CLI error |

Live-shape discipline:

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
- twin-pair proof reports may reference raw `verify`/`benchmark`/`assess`
  artifact identities, but must not score or reinterpret them
- twin-pair proof cases include replay-result diff inputs with snapshot
  provenance, result hashes, and SQLSTATE parity, not timing-derived fields

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
- `twinning.query-trace.v0` — opt-in, redacted live query lineage artifact
- `twinning.twin-pair-proof.v0` — interface-equivalence receipt for two Postgres twins
- `twinning.twin-pair-replay-result.v0` — per-case replay diff inputs nested in twin-pair proof reports
- `twinning.twin-pair-replay-manifest.v0` — declared Postgres-subset replay manifest for twin-pair proof queries
- `twinning.twin-pair-orchestration-manifest.v0` — manifest-first twin-pair proof orchestration contract
- `twinning.canary-manifest.v0` — normative compatibility manifest for the supported subset

Checked-in manifest:

- [canaries/manifest.v0.json](./canaries/manifest.v0.json)

Schemas:

- [schemas/twinning.v0.schema.json](./schemas/twinning.v0.schema.json)
- [schemas/twinning.snapshot.v0.schema.json](./schemas/twinning.snapshot.v0.schema.json)
- [schemas/twinning.query-trace.v0.schema.json](./schemas/twinning.query-trace.v0.schema.json)
- [schemas/twinning.twin-pair-proof.v0.schema.json](./schemas/twinning.twin-pair-proof.v0.schema.json)
- [schemas/twinning.twin-pair-replay-manifest.v0.schema.json](./schemas/twinning.twin-pair-replay-manifest.v0.schema.json)
- [schemas/twinning.canary-manifest.v0.schema.json](./schemas/twinning.canary-manifest.v0.schema.json)

Snapshot contract highlights:

- hashes include committed-state identity and canonical relation ordering
- hashes exclude timestamps, live sessions, warnings, and debug strings
- bootstrap snapshots remain catalog-only; `run_once` snapshots freeze committed relation contents for the executed subset

---

## Refusal Codes

Refusals are structured errors with exit code `2`. Each includes a code, message, detail, and (when applicable) a `next_command` suggesting what to run instead.

| Code | When | Next step |
|------|------|-----------|
| `E_BOOTSTRAP_SOURCE_REQUIRED` | Neither `--schema` nor `--restore` provided | Add `--schema schema.sql` |
| `E_AMBIGUOUS_BOOTSTRAP_SOURCE` | Both `--schema` and `--restore` provided | Use exactly one |
| `E_ENGINE_UNIMPLEMENTED` | Non-Postgres engine requested | Use `twinning postgres ...` |
| `E_IO_READ` | Input file not readable | Check path and permissions |
| `E_IO_WRITE` | Output file not writable | Check path and permissions |
| `E_SCHEMA_PARSE` | DDL parsing failed | Fix SQL syntax in schema file |
| `E_DECLARATION_PARSE` | Catalog declaration malformed or mismatched | Regenerate the declaration for the selected schema |
| `E_VERIFY_ARTIFACT_PARSE` | Verify artifact malformed | Regenerate with `verify` |
| `E_SNAPSHOT_VERIFY` | Snapshot hash mismatch or version error | Re-emit from schema source |
| `E_SEED_BOOTSTRAP_SOURCE` | Seed import/export requested with `--restore` | Use `--schema` |
| `E_SEED_SOURCE_COMPOSITION` | `--seed` combined with `--materialize-source-url` | Pick one source for v1 |
| `E_SEED_JSONL` | Seed JSONL is malformed or violates catalog/constraint rules | Regenerate or fix seed rows |
| `E_REST_INVALID_X_TWINNING` | REST `x-twinning` response stubs are malformed or target no mounted route | Fix the OpenAPI `x-twinning` extension |
| `E_TWIN_PAIR_PROOF` | Twin-pair proof inputs or query fixture are incompatible | Use snapshots with the same schema/catalog/declaration and a supported query fixture |
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

### Homebrew (tagged releases)

Tagged releases publish `twinning` to `cmdrvl/tap`:

```bash
brew install cmdrvl/tap/twinning
```

Until the first tagged release is cut, use the source build below.

### From source (current)

```bash
git clone https://github.com/cmdrvl/twinning.git
git clone https://github.com/cmdrvl/verify.git
cd twinning
cargo build --release
# Binary at target/release/twinning
```

Current source builds expect a sibling `verify/` checkout because `twinning`
links against `verify-core` and `verify-engine` via path dependencies.

---

## Agent / CI Integration

twinning is designed for agent-driven pipelines. Parse JSON output programmatically:

```bash
# Check bootstrap readiness
result=$(cargo run -- postgres --schema schema.sql --json 2>/dev/null)
outcome=$(echo "$result" | jq -r '.outcome')

case $outcome in
  READY)    echo "Twin ready for bootstrap or live mode" ;;
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

**`FAIL` outcome in `run_once` mode**

The child command completed with a non-success exit, timeout, signal, or an
embedded verify failure. Inspect the `run` metadata and any attached `verify`
payload in `twinning.v0`, fix the candidate or supported-shape drift, and rerun.

**`E_SCHEMA_PARSE` on valid-looking SQL**

twinning uses `sqlparser-rs` for DDL parsing. Ensure your schema uses standard Postgres CREATE TABLE syntax. Extensions, custom types, and procedural SQL are not supported in v0.

**`E_SNAPSHOT_VERIFY` hash mismatch**

The snapshot was modified after creation, or was produced by a different twinning version. Re-emit from the original schema source with `--schema` and `--snapshot`.

**`E_ENGINE_UNIMPLEMENTED` for mysql or oracle**

Only the Postgres engine is implemented. MySQL and Oracle are declared in the CLI for forward compatibility but currently refused. See [docs/PLAN_TWINNING_FUTURES.md](./docs/PLAN_TWINNING_FUTURES.md) for the roadmap.

---

## Limitations (v0)

- **Postgres only.** MySQL, Oracle, VSAM, IMS are deferred.
- **Canary-defined live shells only.** `--run` and `--serve` bind pgwire for the manifest-backed subset; unsupported SQL and protocol shapes remain explicit live errors.
- **Canary-defined subset.** Only SQL shapes named in the [canary manifest](./canaries/manifest.v0.json) will be supported.
- **No concurrent writers.** The intended live model is single-writer admission with explicit contention refusal.
- **In-memory snapshot-backed proof orchestration only.** Restore-backed and schema-plus-load proof endpoints are supported when the committed state fits the current memory budget; heavier replay/proof backends remain deferred behind the [backend policy](./docs/REPLAY_PROOF_BACKEND_POLICY.md).

---

## Repository Plan

- Main plan: [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md)
- Futures: [docs/PLAN_TWINNING_FUTURES.md](./docs/PLAN_TWINNING_FUTURES.md)
- Replay/proof backend policy: [docs/REPLAY_PROOF_BACKEND_POLICY.md](./docs/REPLAY_PROOF_BACKEND_POLICY.md)
- Agent guidance: [AGENTS.md](./AGENTS.md)
- Harness notes: [CODEX.md](./CODEX.md), [CLAUDE.md](./CLAUDE.md), [GEMINI.md](./GEMINI.md)
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
