# twinning — Interface Twin

## One-line promise
**Impersonate an interface — speak the real protocol, enforce schema or record contracts, keep the hot working set in memory, and use pluggable state backends when full replay demands it — so extraction code iterates in seconds, not hours, and legacy migrations prove equivalence without touching production.**

---

## Problem

Two versions of the same problem:

**Extractor development.** You're building extractors that write to Postgres (or MySQL, or Oracle). Against real Postgres with 150M existing rows, each test takes minutes to hours. Schema changes require migrations. Constraint violations are buried in database logs. The feedback loop is too slow for agent-driven iteration. The twin compresses the loop from days to seconds.

**Legacy database migration.** You're retiring a 1000-table Oracle database. You need to prove that migrated data is correct — not just structurally valid, but *behaviorally equivalent*. The same queries that ran against Oracle for 20 years must return the same results against the migrated data. You can't run those queries against production during migration testing. The twin lets you replay them against candidate state without pointing the legacy program at production.

Both use cases need the same thing: a fast, ephemeral, constraint-checked behavioral twin that speaks the real protocol. Existing client code — SQLAlchemy, psycopg2, JDBC, COBOL file I/O, IMS calls — connects to it and can't tell the difference for the subset the use case requires. The hot working set lives in memory for speed; the full corpus does not need to.

### Core insight

This is the Digital Twin Universe insight from StrongDM applied to databases instead of SaaS APIs. An agent iterates 20 times per hour instead of once per day. A migration team replays 12 months of production queries in minutes. The twin speaks the real wire protocol; existing client code can't tell the difference.

### Crucible sequencing

Per the Crucible plan, `twinning` is a scale-phase speed layer, not the first
thing Crucible must prove. The core decode loop is proven against real
Postgres first. `twinning` becomes worth building when iteration speed and
swarm economics become the bottleneck.

### Boundary with `decoding` and `crucible`

`twinning` only makes sense if the truth/runtime split stays explicit.

- `crucible` orchestrates the overall loop and decides when a candidate state is
  ready to materialize
- `decoding` resolves claims into canonical mutations or canonical archaeology
  entries
- `twinning` materializes that already-decided state behind a protocol-faithful
  runtime boundary

So:

1. the first proof path is deterministic artifacts plus real Postgres
2. `twinning` is added when protocol fidelity and iteration speed become the
   bottleneck
3. `twinning` never resolves claims, canonicalizes entities, or substitutes for
   decode policy

## V0 scope discipline

V0 is intentionally narrow:

- Postgres v3 wire protocol only
- tournament mode first
- one protocol-faithful twin for the canary-defined SQL subset
- one twin-side validation path that consumes compiled
  `verify.constraint.v1`
- one snapshot / restore contract

Deferred beyond v0:

- MySQL protocol support
- Oracle TNS support
- VSAM / IMS / CICS adapters
- OpenSearch or other non-SQL twins
- replay/proof mode as a primary implementation target

---

## Non-goals

`twinning` is NOT:
- A database (no persistence, no WAL, no crash recovery)
- A replacement for the customer's production database (no application points at the twin)
- A truth oracle (truth is determined by decode policy + gold set + evidence chain)
- A concurrent multi-writer system (single writer per instance)
- A query translator (it runs SQL verbatim — schema must match the client's expectations)
- A promise that every twin keeps the entire universe resident in RAM

No application points at the twin. The customer's web app, API, and reports
continue to run against their production database. The twin exists in two loops:
the extractor development loop (agents iterate fast) and the migration proof
loop (replay historical queries, diff results).

### Storage boundary

The requirement is **stateful**, not **memory-only**.

The twin must keep enough behavioral state to answer the next operation correctly. For tournament mode, that means the hot working set plus protocol/session state live in memory. For replay/proof mode, the backing state can be disk-backed, snapshot-backed, or delegated to a real database while the twin still owns the protocol boundary and behavioral contract.

Short version: **memory-resident behavior, not memory-resident universe**.

---

## CLI

```
twinning postgres [OPTIONS]

Options:
  --schema <FILE>        SQL DDL file defining tables, constraints, indexes
  --verify <FILE>        Compiled verify constraint artifact (`verify.constraint.v1`) for twin-side validation
  --port <PORT>          Listen port (default: 5432)
  --host <HOST>          Listen address (default: 127.0.0.1)
  --run <COMMAND>        Run command against the twin, then report and exit
  --report <FILE>        Write twin validation / metrics report as JSON on exit
  --snapshot <FILE>      Dump twin state to content-addressed snapshot on exit
  --restore <FILE>       Restore twin state from a snapshot before accepting connections
  --json                 JSON output for status messages
```

### Exit codes

`0` clean (all verify checks pass, or no verify artifact is provided) | `1` violations (verify artifact provided, some checks failed) | `2` refusal

### Usage modes

**Interactive:** Start the twin, connect with psycopg2/SQLAlchemy, iterate manually.
```bash
twinning postgres --schema schema.sql --port 5433
```

**With twin-side validation:** Start with compiled verify constraints and emit a
validation / metrics report.
```bash
twinning postgres --schema schema.sql --verify schema.verify.json --port 5433
```

**One-shot:** Start, run extraction, get a twin report plus attached verify
results, then exit.
```bash
twinning postgres --schema schema.sql --verify schema.verify.json --port 5433 \
  --run "python extract.py" --report twin-report.json
```

### Runtime lifecycle semantics

The CLI contract is only stable if startup, shutdown, and report boundaries are
explicit.

Startup preflight order:

1. Parse CLI and select exactly one bootstrap source (`--schema` or
   `--restore`).
2. Load the normalized catalog / restored snapshot and derive the bound runtime
   config.
3. If `--verify` is provided, load the compiled artifact and refuse immediately
   if it contains any batch-only rule.
4. Bind report / snapshot output targets.
5. Only then start accepting pgwire connections.

Interactive mode semantics:

- The twin remains up until orderly shutdown (`SIGINT`, `SIGTERM`, or explicit
  operator stop).
- Unsupported statements are client-visible refusals / SQLSTATEs; they do not
  terminate the twin process.
- If `--report` is set, `twinning` emits exactly one final report on orderly
  shutdown over the final committed state.
- If `--snapshot` is set, `twinning` emits exactly one final snapshot at the
  same boundary.
- Exit code is determined only when the process exits: `0` clean, `1` verify
  violations in the final report, `2` bootstrap/refusal failure.

`--run` mode semantics:

- `twinning` starts the twin, launches the child command, waits for it to exit,
  freezes the final committed state, runs verify if configured, writes the
  report/snapshot outputs, and then exits.
- Child-command failure does not create a second exit-code taxonomy for
  `twinning`; it is reported in the final artifact metadata, while `twinning`
  still exits via the `0` / `1` / `2` domain contract above.

Visibility rules:

- Verification and snapshotting see only committed twin state.
- Rolled-back mutations, transient session variables, live sockets, and
  prepared-statement caches are outside the report/snapshot semantic surface.

### Unsupported-shape mapping

The twin needs one explicit mapping from "unsupported" to user-visible
behavior.

| Situation | Surface | Outcome |
|------|---------|---------|
| Invalid bootstrap input, malformed snapshot, batch-only verify artifact in live mode | process/bootstrap | process-level `REFUSAL`, exit `2`, no live traffic accepted |
| Unsupported pgwire/session behavior before a statement can execute | client protocol | protocol error / SQLSTATE, twin stays up |
| Unsupported SQL shape from a live client after startup | client protocol | protocol error / SQLSTATE, twin stays up |
| Unsupported historical query shape in replay/proof evaluation | harness/reporting | `SKIP` in corpus/reporting outputs, not a live-process refusal |

Rules:

- `SKIP` is not a live pgwire concept in v0. It exists only in corpus-level
  replay/proof or compatibility reporting.
- Live client traffic receives protocol-visible failure, never silent partial
  semantics and never a process-wide refusal after startup.
- Process-level `REFUSAL` is reserved for bootstrap/configuration boundaries.

Recommended SQLSTATE for unsupported live features:

- `0A000` (`feature_not_supported`) unless the canary contract explicitly
  requires a narrower Postgres code.

---

## Architecture

```
+----------------------------------------------------------+
|                     twinning                               |
|                                                            |
|  +-------------+   +-------------+   +---------------+    |
|  | Protocol    |   | Operation   |   | Validation /  |    |
|  | Adapter     |   | Parser /    |   | Metrics       |    |
|  |             |   | Router      |   |               |    |
|  | Postgres v3 |   | INSERT      |   | row counts    |    |
|  | (future     |   | UPSERT      |   | null rates    |    |
|  | adapters)   |   | SELECT      |   | FK coverage   |    |
|  |             |   | READ/WRITE  |   | attached      |    |
|  |             |   |             |   | verify report |    |
|  +------+------+   +------+------+   +-------+-------+    |
|         |                 |                   |            |
|  +------v-----------------v-------------------v---------+  |
|  |              Behavioral Kernel                      |  |
|  |                                                      |  |
|  |  Interface semantics + constraint engine + replay    |  |
|  |  state + cursor state + deterministic snapshots      |  |
|  |                                                      |  |
|  |  State backends:                                     |  |
|  |    - bounded-memory hot working set                  |  |
|  |    - copy-on-write overlay per twin                  |  |
|  |    - snapshot-backed / disk-backed replay backend    |  |
|  +------------------------------------------------------+  |
+------------------------------------------------------------+
```

### Storage modes

**Tournament mode.** This is the swarm mode. One agent, one cheap twin. The twin keeps protocol state, the hot working set, and a copy-on-write overlay in memory. It loads only the slice under test and can evict or reset aggressively. The target footprint is MBs to low hundreds of MBs, not GBs.

**Replay / proof mode.** This is the heavy mode. The goal is full-corpus equivalence or broad historical query replay. The twin still owns the protocol boundary and replay semantics, but the backing state can be snapshot-backed, disk-backed, or a real Postgres instance with the translated schema. This mode is not one twin per agent.

### Swarm economics

If 40 agents each need a twin, the architecture cannot assume a multi-GB resident dataset per instance. The only economically sane model is:
- shared base snapshot or backing store
- per-agent bounded-memory overlay
- lazy hydration of touched keys or ranges
- rapid teardown and rebuild

---

## Deferred futures

Twin-pair migration proof and the broader non-SQL interface-emulator roadmap
are intentionally out of the v0 implementation center. They now live in
[PLAN_TWINNING_FUTURES.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING_FUTURES.md).

The stable v0 primitive remains:

- one Postgres tournament twin
- one canary-defined client/session subset
- one embedded `verify` validation attachment path
- one bounded-memory tournament / heavier-backend storage boundary

---

## Client canary subset

The supported SQL and session behavior are defined only by the canary corpus and
its manifest. Prose summaries are explanatory; they are not the source of
truth.

For v0, the normative canaries are:

- `psql_smoke`
- `psycopg2_params`
- `sqlalchemy_core`
- `extractor_canary`

The v0 subset should therefore be described in terms of those canaries:

- startup, auth, and the parameter-status/session flow those clients require
- `SET`, `BEGIN`, `COMMIT`, and `ROLLBACK` behavior required to keep those
  clients alive
- write shapes exercised by the canaries: `INSERT`, `INSERT ... ON CONFLICT`,
  and the exact coercion / SQLSTATE behavior those writes rely on
- read shapes exercised by the canaries: point lookups, simple predicates, and
  only the aggregates or grouping shapes explicitly present in the manifest

Anything outside the canary-defined subset is not "partially supported." It is
either:

- explicitly refused
- classified as SKIP in replay/proof mode
- or deferred until a new canary proves it

This is the only stable way to stop the SQL surface from drifting into
wish-casting.

### Canary manifest artifact

The manifest itself must be a checked-in artifact, not an implicit test folder.

Recommended location:

- `canaries/manifest.v0.json`

Recommended schema:

| Field | Meaning |
|------|---------|
| `version` | Manifest version, e.g. `twinning.canary-manifest.v0` |
| `engine` | `postgres` for v0 |
| `canaries[]` | Named compatibility claims |
| `canaries[].id` | Stable canary ID such as `psql_smoke` |
| `canaries[].client` | Client family: `psql`, `psycopg2`, `sqlalchemy_core`, `factory_extractor` |
| `canaries[].session_shapes[]` | Required startup/session behaviors |
| `canaries[].write_shapes[]` | Required mutation shapes |
| `canaries[].read_shapes[]` | Required read/query shapes |
| `canaries[].required_sqlstates[]` | SQLSTATEs the canary depends on |
| `canaries[].unsupported_policy` | `refusal` or `skip` for shapes outside the declared subset |

Claim rule:

- If a session or SQL shape is not named in the manifest, v0 does not claim to
  support it.
- The manifest, fixtures, and harness names must line up one-to-one.
- A compatibility claim is valid only if the corresponding canary passes.

Controlled vocabulary for v0:

| Field | Allowed values in v0 |
|------|-----------------------|
| `session_shapes[]` | `startup_auth_v3`, `parameter_status_baseline`, `set_application_name`, `tx_begin`, `tx_commit`, `tx_rollback`, `simple_query`, `extended_query_parse_bind_execute_sync` |
| `write_shapes[]` | `insert_values`, `insert_returning`, `upsert_pk`, `upsert_unique`, `update_by_predicate`, `delete_by_predicate` |
| `read_shapes[]` | `select_by_pk`, `select_filtered_scan`, `select_is_null`, `select_in_list`, `select_between`, `aggregate_count`, `aggregate_basic_group_by` |

If a future canary needs a new shape token, the manifest schema must be extended
explicitly rather than inventing ad hoc shape names in fixtures.

---

## What it must get right

### Type coercion

Agents send strings from Excel. The twin coerces to the declared column type (int, float, numeric, date, timestamp, text, boolean) and rejects bad values with a clear error. This is how the twin catches "agent parsed DSCR as string 'N/A' instead of NULL" — the column type is `numeric`, the insert fails, the agent fixes its extractor.

### Postgres error codes

psycopg2 and SQLAlchemy catch specific error codes to distinguish constraint violations from parse errors from connection failures. The twin must return the correct SQLSTATE codes:

| Code | Meaning | When |
|------|---------|------|
| `23505` | unique_violation | PK or UNIQUE constraint violated |
| `23503` | foreign_key_violation | FK references nonexistent parent row |
| `23502` | not_null_violation | NULL in NOT NULL column |
| `23514` | check_violation | CHECK constraint failed |
| `22P02` | invalid_text_representation | Type coercion failed |
| `42P01` | undefined_table | Table doesn't exist |
| `42703` | undefined_column | Column doesn't exist |

Without correct error codes, SQLAlchemy's `IntegrityError` / `DataError` exception hierarchy breaks and extraction code can't handle errors properly.

### ON CONFLICT target matching

Upsert is the primary write pattern. The twin must correctly identify the conflict target (PK or named UNIQUE constraint), match incoming rows against existing keys, and apply the SET clause on conflict. Composite keys (e.g., `(property_id, period)`) must work.

### NULL semantics

`WHERE column = NULL` returns no rows (must use `IS NULL`). Three-valued logic in boolean expressions. Agents occasionally get this wrong — the twin must behave like Postgres so the bug surfaces during testing, not production.

### Session and single-writer semantics

V0 is single-writer, not single-session.

Rules:

- Multiple concurrent sessions may connect.
- Multiple read-only sessions may execute concurrently against the last committed
  state.
- At most one session may hold the mutable write transaction at a time.
- Uncommitted writes are visible only to the owning session.
- Other sessions see last committed state only; v0 does not expose dirty reads.
- `ROLLBACK` discards the owning session's uncommitted changes completely.
- Auto-commit writes are treated as single-statement write transactions.

Admission behavior:

- If a second session attempts to enter a write transaction while another write
  transaction is active, the twin returns a protocol-visible error and leaves
  both sessions alive.
- Recommended SQLSTATE for this v0 writer-admission failure: `55P03`
  (`lock_not_available`).

---

## Performance

The throughput table below is explanatory only. The real implementation contract
is the acceptance-budget table later in this document.

| Operation | Mechanism | Throughput |
|-----------|-----------|------------|
| INSERT (single row) | Hash + constraint checks | ~2-5M rows/sec |
| INSERT (multi-row batch) | Same, amortized alloc | ~3-5M rows/sec |
| UPSERT (ON CONFLICT) | Hash lookup + conditional insert/update | ~1-3M rows/sec |
| SELECT COUNT(*) | Table length | Instant |
| SELECT ... WHERE key = X | Hash lookup | Instant |
| SELECT ... WHERE col > X | Scan (no index) | ~10-50M rows/sec (memory bandwidth) |
| FK check | Hash lookup into parent PK map | O(1) per row |

### Full load throughput (optional full-RAM profile)

| Scale | Time | Notes |
|-------|------|-------|
| 200K rows (per-agent iteration) | <1 second | The iteration loop |
| 10M rows (single deal, all tables) | 3-10 seconds | Per-deal validation |
| 150M rows (full corpus, all tables) | 1-3 minutes | Optional full-RAM validation profile, not per-agent default |

These numbers assume Rust HashMap with FxHash or similar fast hasher, pre-allocated capacity, and minimal allocation during insert. Memory usage: ~50-100 bytes per row typical (depends on column count and types), so 150M rows ~ 8-15 GB RAM. That is an optional whole-corpus benchmark for a single heavyweight validation run, not the baseline tournament shape.

For day-to-day swarm iteration, the expected shape is much smaller: per-deal, per-job, per-template, or per-partition slices with bounded-memory overlays. If you need whole-corpus replay, a disk-backed or real-Postgres backend is acceptable — the important boundary is protocol fidelity and behavioral equivalence, not proving everything can fit in RAM at once.

---

## Validation and twin reporting

The twin should not invent its own rule language.

It consumes a compiled `verify.constraint.v1` artifact through `--verify` and
reports two families of signals:

- twin-native structural metrics such as row counts, null rates, FK coverage,
  and snapshot provenance
- an attached `verify` execution report over the materialized twin state

`twinning` may aggregate those signals into one report for iteration speed, but
the constraint semantics belong to `verify`, the gold-set semantics belong to
`benchmark`, and proceed / escalate / block decisions belong to `assess`.

The stable boundary is:

- `twinning` owns runtime/session behavior, materialized state, snapshots, and
  raw twin-native metrics
- `verify` owns constraint meaning and report semantics
- `benchmark` owns correctness against gold data
- `assess` owns policy decisions

So the `twinning` report should surface raw twin-native metrics and attach a
`verify.report.v1` artifact or embedded equivalent. It should not collapse those
signals into a new pseudo-score.

For v0, `twinning` should execute `verify` through the embedded library surface,
not by exporting state and shelling out to batch `verify`. If a provided
constraint artifact contains batch-only rules, live `twinning` must refuse them
explicitly instead of silently degrading into a second execution path.

### Verify execution contract

`twinning` needs one exact bridge into `verify`.

Execution timing:

- In `--run` mode, verify runs once after the child command exits and before the
  final report/snapshot is written.
- In interactive mode, verify runs once at the final report boundary during
  orderly shutdown.
- v0 does not run verify after every statement or transaction.

Binding surface:

- Every materialized table is exposed to embedded `verify` as a named relation
  using the canonical catalog name `schema.table` with `public` explicit.
- The compiled verify artifact used with `twinning` v0 must bind directly to
  those canonical relation names.
- `twinning` does not invent a second alias or file-binding layer for verify.

Outcome rules:

- The attached verify payload in a twin report must be a valid
  `verify.report.v1` artifact or an embedded-equivalent structure with the same
  semantics.
- If the artifact contains any batch-only rule, `twinning` refuses the run
  before accepting live traffic.
- If no verify artifact is supplied, the twin report omits the attached verify
  section rather than fabricating a PASS.

### Twin report contract

The report can no longer be example-only. v0 needs one explicit artifact shape.

Artifact identity:

- current artifact ID: `twinning.v0`
- authoritative machine schema:
  [schemas/twinning.v0.schema.json](/Users/zac/Source/cmdrvl/twinning/schemas/twinning.v0.schema.json)

Required top-level fields:

| Field | Meaning |
|------|---------|
| `version` | Report artifact ID |
| `outcome` | `READY`, `PASS`, `FAIL`, or `REFUSAL` at the twin-report boundary |
| `mode` | `bootstrap`, `interactive`, or `run_once` |
| `engine` | `postgres` in v0 |
| `host` | bind host for the twin boundary |
| `port` | bind port for the twin boundary |
| `wire_protocol` | declared wire/runtime contract, e.g. `planned.pgwire` |
| `schema` | normalized schema identity and hash |
| `catalog` | normalized catalog summary for the loaded schema |
| `storage` | declared tournament / replay storage boundary |
| `tables` | per-table structural/runtime metrics |
| `constraints` | twin-native constraint counters |
| `snapshot` | final snapshot metadata if written or restored |
| `next_step` | operator-facing summary of the next live/runtime step |

Optional top-level fields:

| Field | Present when |
|------|---------------|
| `verify_artifact` | `--verify` was supplied |
| `verify` | embedded `verify.report.v1` is attached |
| `run` | `--run` was used |
| `null_rates` | metrics collection included null-rate output |
| `fk_coverage` | metrics collection included FK-coverage output |
| `warnings` | non-fatal operator warnings exist |

`run` object contract:

| Field | Meaning |
|------|---------|
| `command` | exact child command string |
| `exit_code` | child exit code if it exited normally |
| `signal` | terminating signal if applicable |
| `timed_out` | whether twinning terminated the child for timeout |

Report rules:

- The report must not include wall-clock timestamps or unbounded stdout/stderr
  blobs from the child command.
- If `--run` is not used, the `run` object is omitted entirely.
- If `--verify` is not used, both `verify_artifact` and `verify` are omitted.
- Optional sections are omitted when absent; they are not serialized as `null`
  placeholders or empty warning arrays.
- The embedded `verify` payload preserves `verify.report.v1` field semantics and
  ordering; `twinning` may wrap it, but not reinterpret it.

Example combined report:

```json
{
  "version": "twinning.v0",
  "outcome": "FAIL",
  "mode": "run_once",
  "engine": "postgres",
  "host": "127.0.0.1",
  "port": 5433,
  "wire_protocol": "planned.pgwire",
  "schema": {
    "source": "schemas/cmbs.sql",
    "hash": "sha256:...",
    "table_count": 3,
    "column_count": 54,
    "index_count": 9,
    "constraint_count": 11
  },
  "catalog": {
    "dialect": "postgres",
    "table_count": 3,
    "column_count": 54,
    "index_count": 9,
    "constraint_count": 11
  },
  "storage": {
    "tournament_mode": "bounded-memory hot working set with per-twin overlay",
    "replay_mode": "snapshot-backed or delegated real-database backend",
    "hot_working_set": "memory",
    "cold_state": "shared snapshot or pluggable backing store"
  },
  "tables": {
    "public.deals": {
      "rows": 3500,
      "columns": 8,
      "indexes": 2,
      "constraints": 3
    },
    "public.loans": {
      "rows": 412000,
      "columns": 19,
      "indexes": 4,
      "constraints": 5
    },
    "public.properties": {
      "rows": 389000,
      "columns": 27,
      "indexes": 3,
      "constraints": 3
    }
  },
  "constraints": {
    "not_null_violations": 0,
    "fk_violations": 0,
    "check_violations": 0,
    "unique_violations": 0
  },
  "verify_artifact": {
    "source": "loan-perf.verify.json",
    "hash": "sha256:...",
    "loaded": 14
  },
  "verify": {
    "version": "verify.report.v1",
    "outcome": "FAIL",
    "summary": {
      "total_rules": 14,
      "passed_rules": 12,
      "failed_rules": 2
    }
  },
  "run": {
    "command": "python extract.py",
    "exit_code": 0,
    "timed_out": false
  },
  "null_rates": {
    "financials.noi": 0.02,
    "financials.occupancy": 0.08,
    "financials.dscr": 0.03
  },
  "fk_coverage": {
    "loans.deal_id -> deals.deal_id": 1.0,
    "financials.property_id -> properties.property_id": 0.97
  },
  "next_step": "Read the attached verify report, fix extractor drift, and rerun the candidate against the twin."
}
```

Anchored coverage is not a first-class `twinning` claim in v0. The twin may
surface raw anchor-query outputs or local expected-vs-observed counters when a
caller provides them, but global anchored-coverage interpretation belongs at the
Crucible/reporting layer above `twinning`.

---

## Snapshots

```bash
# Dump current state
twinning postgres --schema schema.sql --port 5433 \
  --snapshot snapshots/2025-12-full.twin

# Restore and continue
twinning postgres --restore snapshots/2025-12-full.twin --port 5433
```

A snapshot is a content-addressed dump of the twin state (schema + materialized data + constraint metadata + overlay state). Snapshots enable:
- Fast twin recovery without replaying claim/mutation events
- Diffing twin states across time periods (export to CSV, run `rvl`)
- Evidence sealing (snapshot hash in evidence packs)

### Snapshot contract

The snapshot contract must be explicit enough to support the determinism claim.

Authoritative machine schema:

- [schemas/twinning.snapshot.v0.schema.json](/Users/zac/Source/cmdrvl/twinning/schemas/twinning.snapshot.v0.schema.json)

A live `twinning.snapshot.v0` hash surface must include:

- snapshot format version
- engine and twin mode
- schema hash and normalized catalog identity
- base snapshot hash if the current twin was restored from one
- verify artifact hash if one is attached
- committed relation contents serialized in one canonical representation
- deterministic row counts per relation

A live `twinning.snapshot.v0` hash surface must exclude:

- wall-clock timestamps
- active connection IDs
- transient session variables
- prepared-statement cache contents
- warnings, logs, witness receipts, or operator-facing display strings

If timestamps or debug metadata are present in the serialized snapshot, they
must live outside the content-addressed hash surface.

Phase-0 bootstrap note:

- The current bootstrap build emits normalized catalog identity plus deterministic
  `table_rows` counts and optional restore lineage.
- Live committed relation contents may be attached later through the optional
  `relations` section declared in the schema, but only if the canonical ordering
  rules below are preserved.
- If a future live implementation cannot preserve those ordering rules under the
  existing schema surface, it must bump the snapshot version instead of silently
  changing the meaning of `twinning.snapshot.v0`.

Canonical representation rule for v0:

- v0 snapshots hash full committed relation contents, not overlay deltas.
- Relations serialize in lexicographic `schema.table` order.
- Rows serialize in primary-key order when a PK exists; otherwise by full row
  tuple order over the normalized column list.
- Column order is always the normalized catalog column order.

Overlay-delta snapshots are a later optimization, not a second valid v0
encoding.

---

## Postgres-only v0

V0 is Postgres-only on purpose.

Why Postgres first:

- `pgwire` is mature enough to make protocol fidelity plausible
- `psycopg2` and SQLAlchemy Core are the primary client canaries
- Crucible's immediate target environment is Postgres
- the hard part is not "many engines"; it is one honest, compatible twin

Future interface support belongs to later phases only after the Postgres kernel,
snapshot contract, and tournament economics are proven.

---

## Usage examples

### Extractor development loop

```bash
# Start a Postgres twin on port 5433 with a schema file
twinning postgres --schema schema.sql --port 5433

# One-shot: run extraction, get a twin report plus attached verify results, exit
twinning postgres --schema schema.sql --verify schema.verify.json --port 5433 \
  --run "python extract.py" --report twin-report.json

# Agent iteration loop (typical Crucible usage)
# 1. Start fresh twin
twinning postgres --schema schema.sql --verify schema.verify.json --port 5433 \
  --run "python extract_deal_42.py" --report deal_42_report.json
# 2. Agent reads the twin report and attached verify output, fixes extractor, re-runs in seconds
# 3. Repeat 20x per hour until the twin and verify signals stabilize
```

### Tournament (multiple candidates)

```bash
# Score 3 assembly strategies for the same data product
for strategy in oracle-direct doc-reparse hybrid; do
  twinning postgres --schema loan-perf-schema.sql --verify loan-perf.verify.json --port 5433 \
    --run "python assemble_${strategy}.py" \
    --report "results/${strategy}_report.json" \
    --snapshot "snapshots/${strategy}.twin"
done

# Compare: which strategy scored highest?
# assess picks the winner based on policy
```

---

## Relationship to other tools

| Tool | Relationship |
|------|-------------|
| **crucible** | Crucible uses `twinning` as a later speed/protocol layer for tournament iteration after the first proof loop exists on deterministic artifacts plus real Postgres. It may later orchestrate twin-pair migration proof on top of the same kernel. |
| **decoding** | Decoding resolves claims into canonical mutations or canonical archaeology outputs; the twin materializes that state and enforces runtime/constraint behavior without resolving claims itself. |
| **verify** | `verify` owns the constraint protocol and report semantics. `twinning` consumes compiled `verify.constraint.v1` artifacts and attaches `verify` results over materialized state. |
| **shape** | Twin's schema DDL is the structural contract; `shape` checks CSV inputs before they reach the twin |
| **benchmark** | `benchmark` scores correctness against gold data; `twinning` does not replace it |
| **assess** | `assess` consumes `twinning`, `verify`, and `benchmark` outputs as policy inputs; `twinning` does not make the decision |
| **pack** | Twin snapshots can be included in evidence packs |
| **rvl** | Diffing twin states across time: export to CSV, run `rvl` |

---

## Implementation notes

### Implementation scope

| Component | Source | LOC estimate |
|-----------|--------|-------------|
| Wire protocol server | **pgwire** crate (Postgres v3) | ~500 (glue code) |
| SQL parser + router | **sqlparser-rs** crate (Postgres dialect) | ~500 (route parsed AST to store) |
| Schema catalog | Custom | ~1-2K |
| State backend trait + overlay model | Custom | ~500-1K |
| Bounded-memory in-memory backend | Custom | ~2-3K |
| Replay / proof backend (disk-backed or real Postgres) | Custom / delegated | ~1-2K |
| Constraint checker (NOT NULL, CHECK, UNIQUE, FK, types) | Custom | ~1-2K |
| Upsert logic (ON CONFLICT) | Custom | ~500 |
| Basic SELECT executor | Custom | ~2-3K |
| Twin metrics + verify integration | Custom | ~1-2K |
| Snapshot dump/restore | Custom | ~500-1K |
| Error code mapping (SQLSTATE) | Custom | ~300 |
| **Total** | | **~10-15K lines of Rust** |

### Swarm-safe module map

The runtime work should converge on a file layout that lets multiple agents work
without constant collisions.

Recommended module ownership:

| Path | Responsibility |
|------|----------------|
| `src/cli.rs` | Clap surface only |
| `src/runtime.rs` | Process lifecycle, startup/shutdown, signal handling, report/snapshot boundaries |
| `src/protocol/postgres/{mod,startup,simple_query,extended_query,error}.rs` | pgwire adapter and protocol framing |
| `src/ir.rs` | `Operation IR` definitions and normalization invariants |
| `src/kernel/{mod,coerce,write,read,sqlstate}.rs` | Semantic kernel |
| `src/backend/{mod,base,memory,overlay}.rs` | Base snapshot access and tournament backend behavior |
| `src/verify_bridge.rs` | Embedded verify binding and attachment logic |
| `src/snapshot.rs` | Snapshot serialization, hashing, restore |
| `src/report.rs` | Twin report schema and rendering |
| `tests/canaries/*.rs` | Client compatibility harnesses |
| `tests/differential/*.rs` | Real-Postgres parity harnesses |
| `tests/storage/*.rs` | Memory/reset/startup budget checks |

The exact filenames can differ slightly, but v0 work should preserve this
separation of concerns.

### Candidate crates

| Need | Crate | Notes |
|------|-------|-------|
| Postgres wire protocol | `pgwire` | Production-grade, Rust |
| SQL parsing | `sqlparser-rs` | Postgres dialect, mature |
| Fast hashing | `rustc-hash` (FxHash) | HashMap performance |
| Content hashing | `sha2` | Snapshot content addressing |

### Ideal role for `asupersync`

`asupersync` is a plausible runtime substrate for the live twin shell, but not
for the semantic center.

Good fit:

- connection/session task orchestration
- cancellation-correct protocol handling
- per-twin overlay lifecycle and teardown
- deterministic replay of protocol races in compatibility tests
- snapshot/export/restore orchestration

Not its job:

- SQL parsing
- row-store semantics
- constraint enforcement
- SQLSTATE parity
- `verify` semantics

Short version: if adopted, `asupersync` should sit around the protocol adapter
and runtime shell, not replace the semantic kernel or state model.

Follows the same implementation standards as protocol tools: `#![forbid(unsafe_code)]`, clap derive CLI, MIT license, CI (fmt -> clippy -> test), cross-platform release builds.

---

## Phased implementation roadmap

### Delivery doctrine

- One wedge at a time. v1 is **Postgres tournament mode**, not "all interfaces."
- Decode-first before twin-first. If the deterministic decode/materialization
  loop is not proven on real Postgres, the twin is premature.
- Protocol fidelity comes before SQL breadth. If clients cannot connect cleanly, new SQL support is wasted work.
- Capability growth must be explicit. Expand the supported subset by growing the canary corpus and manifest, never by vague claims.
- Backend abstraction comes before the second interface. Do not hard-code SQL-shaped storage into the kernel.
- Tournament mode ships before replay/proof mode. Fast agent iteration is the first value.
- Whole-corpus replay does not need to be memory-only. Use a heavier backend when the economics demand it.
- Heavy backends may delegate storage, but they must not change the protocol-facing contract.
- Expansion beyond Postgres tournament mode is tracked in the futures doc, not in this execution plan.

### What "v1 working" means

`twinning v1` means an unmodified Postgres client can connect to a tournament twin, restore or load a bounded base snapshot, execute the declared SQL subset, and receive either Postgres-matching results / SQLSTATEs or an explicit refusal.

In scope for v1:

- Postgres-only tournament mode
- Startup/auth/session behavior required by `psql`, `psycopg2`, and SQLAlchemy Core
- Simple query and minimal extended-query flow (`Parse`, `Bind`, `Describe`, `Execute`, `Sync`) for parameterized statements
- Schema supplied by input DDL and/or restored snapshots rather than arbitrary runtime DDL
- DML subset: `INSERT`, `INSERT ... ON CONFLICT`, `UPDATE`, `DELETE`
- Read subset limited to the exact `SELECT` shapes exercised by the canary corpus
- Shared base snapshot plus per-agent copy-on-write overlay with deterministic reset

Not v1:

- `COPY`, `LISTEN/NOTIFY`, replication, logical decoding, advisory locks
- Broad catalog emulation or ORM reflection completeness
- Arbitrary runtime DDL, stored procedures, triggers, or broad SQL compatibility claims
- Oracle TNS, VSAM, IMS, or CICS adapters
- Full-corpus replay in RAM

### Kernel boundaries

The repo stays honest only if the center is adapter-agnostic.

| Layer | Responsibility | Must not know |
|------|----------------|---------------|
| **Protocol adapter** | Wire/session framing, auth, transaction lifecycle, mapping client messages into semantic requests | Row layout, snapshot format, backend residency policy |
| **Operation IR** | Normalized operations such as session control, parameterized statements, point lookups, scans, mutations, and explicit refusals | Protocol frames, engine-specific storage details |
| **Semantic kernel** | Type coercion, constraint checks, conflict handling, SQLSTATE mapping, read/write semantics | pgwire details, VSAM call shapes, storage-medium specifics |
| **State backend** | Base snapshot access, uniqueness probes, scans, FK existence checks, overlay writes, deterministic reads | SQL text, client protocol, copybook or parser internals |
| **Overlay / snapshot manager** | Restore, branch, reset, export, content hashing, isolation guarantees | Query parsing, protocol details |

Every new interface must terminate at the same `Operation IR` and `Semantic kernel`. If an adapter needs to bypass those layers to work, the architecture is wrong.

### Operation IR contract

The IR can no longer stay implicit. v0 needs one normalized operation surface
between pgwire parsing and kernel execution.

| IR op | Required fields | Meaning |
|------|------------------|---------|
| `SessionOp` | `session_id`, `op`, `tracked_params` | Session lifecycle and acknowledged settings such as `BEGIN`, `COMMIT`, `ROLLBACK`, and tracked `SET` operations |
| `PrepareOp` | `session_id`, `statement_id`, `sql_hash`, `param_types` | Minimal extended-query preparation state for parameterized execution |
| `MutationOp` | `session_id`, `table`, `kind`, `columns`, `rows`, `conflict_target`, `predicate`, `returning` | Normalized write operation for `INSERT`, `UPSERT`, `UPDATE`, and `DELETE` |
| `ReadOp` | `session_id`, `table`, `shape`, `projection`, `predicate`, `aggregate`, `group_by`, `limit` | Normalized read operation for the canary-defined `SELECT` subset |
| `RefusalOp` | `scope`, `code`, `detail` | Explicit unsupported-shape or invalid-operation refusal before backend execution |

IR invariants:

- All table and column references are resolved against the normalized catalog
  before backend execution.
- The IR contains no protocol frames, socket state, or backend residency hints.
- Unsupported syntax becomes `RefusalOp` before the backend sees it.
- Equivalent supported SQL shapes normalize to the same IR shape regardless of
  client library.

Controlled IR vocab for v0:

| Field | Allowed values in v0 |
|------|-----------------------|
| `SessionOp.op` | `set_param`, `begin`, `commit`, `rollback`, `sync` |
| `MutationOp.kind` | `insert`, `upsert`, `update`, `delete` |
| `ReadOp.shape` | `point_lookup`, `filtered_scan`, `aggregate_scan` |
| `ReadOp.aggregate` | `none`, `count`, `sum`, `avg`, `min`, `max` |

`predicate` normalization for v0:

- conjunction / disjunction over column comparisons only
- operators limited to `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `is_null`,
  `in_list`, and `between`
- no arbitrary expression trees beyond the canary-defined subset

### Kernel result contract

The adapter also needs one normalized result surface back from the kernel.

| Result kind | Required fields | Meaning |
|------------|------------------|---------|
| `AckResult` | `tag`, `rows_affected` | Session or mutation acknowledgement |
| `ReadResult` | `columns`, `rows` | Deterministic rowset for the normalized read |
| `MutationResult` | `tag`, `rows_affected`, `returning_rows` | Write result for DML and `RETURNING` |
| `RefusalResult` | `code`, `message`, `sqlstate`, `detail` | Live protocol-visible failure |

The pgwire adapter maps only these normalized results into protocol frames. It
must not re-interpret backend state directly.

### Phases

| Phase | Goal | Deliverables | Hard gate to continue | Stop / redirect if |
|------|------|--------------|------------------------|--------------------|
| **0** | Bootstrap the artifact surface | CLI, DDL/catalog parsing, verify-artifact loading, deterministic report, snapshot hashing | Done in the current repo | n/a |
| **1** | Make common Postgres clients connect and execute a first parameterized round trip | Startup/auth handshake, backend key data, parameter status, simple query path, minimal extended-query path, `SET`/`BEGIN`/`COMMIT`/`ROLLBACK` ACKs, minimal session state, correct protocol/error framing | `psql` smoke, `psycopg2` parameterized smoke, and SQLAlchemy Core smoke all pass without app-side hacks | If common clients cannot complete the parameterized canaries through `pgwire` after a bounded spike, stop adding SQL features and reassess the protocol strategy |
| **2** | Make the write path correct | `INSERT`, `ON CONFLICT`, PK/UNIQUE/FK/NOT NULL/CHECK, type coercion, SQLSTATE mapping, deterministic snapshot/restore, overlay-safe mutations | Differential tests vs real Postgres pass for the declared write subset with exact SQLSTATE parity; extractor canaries can write unchanged | If error codes, coercion, or upsert behavior drift from Postgres on repeated gold cases, stop and fix the kernel before adding reads |
| **3** | Support the read subset extractors actually use | Declared `SELECT` subset, predicates, joins only if the canary corpus demands them, basic aggregates / `GROUP BY` only if demanded, `UPDATE`, `DELETE`, minimal catalog stubs, explicit SKIP reporting | The curated query corpus meets the acceptance budgets below, and unsupported features are classified explicitly rather than guessed | If unsupported-query rate stays high for the real corpus, narrow the supported subset and stop claiming broader compatibility |
| **4** | Make tournament mode swarm-safe | Backend trait, shared base snapshot, bounded-memory hot working set, per-agent copy-on-write overlay, lazy hydration, fast reset, memory-budget reporting | Tournament twins meet the startup/reset/private-RSS budgets below on the reference canaries | If per-agent twins still need large resident state, stop interface expansion and fix storage economics first |

Replay/proof mode and non-SQL expansion continue in
[PLAN_TWINNING_FUTURES.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING_FUTURES.md)
once the v0 center above is real.

### Concrete acceptance budgets

These are the default go / no-go budgets for v1 on a reference developer machine. They are not portability promises; they are implementation gates.

| Metric | Target | Red line |
|------|--------|----------|
| Cold startup from schema | <= 1.0s | > 2.0s |
| Warm restore from base snapshot | <= 2.0s | > 5.0s |
| Reset to clean overlay | <= 200ms | > 500ms |
| Idle private RSS per tournament twin | <= 128 MiB | > 256 MiB |
| Private RSS under the reference canary workload | <= 256 MiB | > 512 MiB |
| Gold write-corpus differential parity | 100% | Any silent mismatch |
| Unsupported-statement handling | 100% explicit refusal or SKIP | Any silent partial semantics |
| Snapshot determinism | 20/20 identical hashes for identical state | Any hash drift |

### First canary corpus

The first harnesses are not optional. They define the real subset.

- `psql_smoke`: connect, authenticate, `SET application_name`, `BEGIN`, `SELECT`, `ROLLBACK`, restore-ready snapshot load
- `psycopg2_params`: parameterized `INSERT`, `SELECT`, `ON CONFLICT`, and a known unique-violation SQLSTATE
- `sqlalchemy_core`: engine connect, transaction begin/commit, parameterized execute, row fetch, no reflection requirement in v1
- `extractor_canary`: one real Crucible extractor script, run unchanged against the twin
The rule is simple: no new feature claim lands without a canary or differential fixture that proves it.

### Immediate work order

The next sequence should match the current Beads queue:

1. `bd-399`: build the compatibility and differential harness first; every later phase depends on it.
2. `bd-cry`: define the shared Operation IR before the protocol adapter and kernel drift into incompatible local shapes.
3. `bd-1jd`: land the pgwire listener only far enough to satisfy the phase-1 canaries.
4. `bd-372`: implement the row store and constraint executor to satisfy the phase-2 gold corpus.
5. `bd-28r`: add bounded-memory overlays and the replay/proof backend boundary before widening the query surface or adding another interface.
6. `bd-wij`: layer embedded `verify` execution and raw twin metrics reporting on top once semantics and storage behavior are trustworthy.

### Test strategy

The implementation lives or dies by the harnesses, not the prose.

- **Client compatibility suite:** real `psql`, `psycopg2`, SQLAlchemy, and later `asyncpg`, running startup, session, transaction, and error-path canaries.
- **Differential semantics suite:** the same DDL/DML/query corpus executed against real Postgres and the twin, with result and SQLSTATE comparison.
- **Extractor canary suite:** a small set of real extractor scripts from Crucible use cases, run unchanged against the twin.
- **Storage-economics suite:** startup time, reset time, overlay size, hot-working-set growth, and concurrent-twin memory budgets.
- **Snapshot suite:** restore fidelity, content-address determinism, and overlay isolation checks.

### Initial success criteria

`twinning` is credible when all of the following are true:

- A Python extractor using `psycopg2` or SQLAlchemy Core can point at the twin and run unchanged for the declared subset.
- Unsupported operations are explicit refusals or SKIPs, never silent wrong answers.
- A per-agent tournament twin stays inside the private-RSS and reset budgets defined above.
- The implementation can later grow into replay/proof and non-SQL modes without rewriting the kernel boundary.

### What does not block v1

- 150M rows fitting in RAM
- `COPY` support
- `LISTEN/NOTIFY`
- Prepared-statement caching sophistication beyond the minimal extended-query flow
- SQLAlchemy reflection completeness
- Oracle TNS support
- CICS support
- Full SQL compatibility
- Multi-writer semantics
- A universal replay backend for every interface

### Go / no-go checkpoints

- If the Postgres handshake is brittle, pause feature work and fix the protocol surface first.
- If the write-path semantics differ from real Postgres on gold cases, do not move on to reads.
- If tournament twins are not bounded-memory, do not move on to the next interface.
- Future replay/proof and non-SQL expansion should reuse the same kernel boundary instead of reopening the v0 center.

---

## Determinism

Same schema + same operation stream + same base snapshot = same twin state. No randomness, no side effects beyond the selected backend semantics. Snapshots are content-addressed — same state produces the same hash.
