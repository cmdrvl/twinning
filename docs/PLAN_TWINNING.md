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

### Factory sequencing

Per the factory plan, `twinning` is a scale-phase speed layer, not the first
thing the factory must prove. The core decode loop is proven against real
Postgres first. `twinning` becomes worth building when iteration speed and
swarm economics become the bottleneck.

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

## Later mode: twin-pair migration proof

Twin-pair migration proof is real, but it is not the v0 center.

The first stable `twinning` primitive is one Postgres tournament twin for
extractor iteration. Legacy migration proof becomes a later mode built on the
same kernel and snapshot contract.

In that later mode, each data product may use two twins:

- **Twin A — Legacy schema.** Same tables, columns, and relationships as the
  source database so historical queries can replay verbatim.
- **Twin B — Target schema.** The clean target schema, with attached
  `verify`/`benchmark`/`assess` outputs proving the new product on its own
  terms.

That later mode is intentionally deferred because it pulls in replay harnesses,
result diffing, and broader factory evidence flows that are not required to
stabilize the v0 tournament boundary.

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

---

## Performance

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

Example combined report:

```json
{
  "version": "twinning.v0",
  "engine": "postgres",
  "schema": "cmbs.v1",
  "tables": {
    "deals": { "rows": 3500, "columns": 8, "indexes": 2 },
    "loans": { "rows": 412000, "columns": 19, "indexes": 4 },
    "properties": { "rows": 389000, "columns": 27, "indexes": 3 }
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
  "null_rates": {
    "financials.noi": 0.02,
    "financials.occupancy": 0.08,
    "financials.dscr": 0.03
  },
  "fk_coverage": {
    "loans.deal_id -> deals.deal_id": 1.0,
    "financials.property_id -> properties.property_id": 0.97
  }
}
```

Anchored coverage is not a first-class `twinning` claim in v0. The twin may
surface raw anchor-query outputs or local expected-vs-observed counters when a
caller provides them, but global anchored-coverage interpretation belongs at the
factory/reporting layer above `twinning`.

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

---

## Postgres-only v0

V0 is Postgres-only on purpose.

Why Postgres first:

- `pgwire` is mature enough to make protocol fidelity plausible
- `psycopg2` and SQLAlchemy Core are the primary client canaries
- the factory's immediate target environment is Postgres
- the hard part is not "many engines"; it is one honest, compatible twin

Future interface support belongs to later phases only after the Postgres kernel,
snapshot contract, and tournament economics are proven.

---

## Beyond v0: the generalized interface model

The twin's core abstraction is not "a SQL database emulator." It's **an interface emulator**: speak the protocol the client expects, enforce the schema's constraints, and keep enough behavioral state to answer correctly. SQL wire protocols are one interface. There are others — and the architecture supports them without changing the factory, the convergence model, or the tournament.

### Non-SQL twin types (future)

| Interface | Protocol | What programs expect | Twin implementation | Complexity vs Postgres twin |
|-----------|----------|---------------------|--------------------|-----------------------------|
| **VSAM** | COBOL file I/O (`OPEN`, `READ`, `WRITE`, `REWRITE`, `DELETE`, `START`, `CLOSE`) | Keyed or sequential access to fixed-format record files | Keyed byte-array store with hot pages in RAM and optional snapshot/disk backing | **Simpler** — no SQL parsing, no query planning, ~3-4K LOC |
| **IMS/DL/I** | Hierarchical navigation (`GU`, `GN`, `GNP`, `ISRT`, `REPL`, `DLET`) | Tree traversal over segments defined by a DBD | Hierarchical store with hot segments in RAM and pluggable persistence | **Harder** — navigational semantics are subtle, ~5-8K LOC |
| **Flat files** | Sequential I/O (`READ`/`WRITE` with copybook layout) | Fixed-length records in EBCDIC with packed decimal fields | Byte-stream twin with hot windows in memory and copybook-defined field offsets | **Simplest** — no indexing, no constraints, ~1-2K LOC |
| **CICS** | Transaction dispatch (`EXEC CICS` commands) | Screen input → program → DB/file updates → screen output | Transaction router + CICS API surface emulation | **Hardest** — hundreds of commands; pragmatic path is mock top 50 or use Micro Focus commercially |

Each twin type follows the same contract:
- Speak the interface the program expects
- Keep enough behavioral state to answer correctly
- Keep the hot working set in memory; use snapshots, overlays, or heavier backends for the rest
- Enforce constraints from the schema definition (DDL, DBD, copybook)
- Support content-addressed snapshots
- Report raw twin-native metrics

The VSAM twin is the highest-value addition. COBOL batch programs that read VSAM datasets are ~30% of typical mainframe workloads, and the VSAM access pattern (keyed byte-array store) is simpler than SQL. A VSAM twin + GnuCOBOL (open-source COBOL compiler) enables off-mainframe batch job replay: compile the COBOL program, point its file I/O at the VSAM twin, run it, capture output, compare against known-good output from the mainframe.

### Schema definitions by twin type

| Twin type | Schema source | What it declares |
|-----------|--------------|-----------------|
| SQL (Postgres/MySQL/Oracle) | DDL file | Tables, columns, types, PK, FK, CHECK, UNIQUE, NOT NULL |
| VSAM | COBOL copybook | Record layout: field offsets, lengths, PIC clauses (exact byte-level types) |
| IMS | DBD (Database Description) | Segment hierarchy, field definitions, search fields |
| Flat file | COBOL copybook | Same as VSAM — record layout at byte level |

The copybook IS the schema for non-SQL twins. `PIC S9(7)V99 COMP-3` declares a signed 7.2 packed decimal at a specific byte offset — no ambiguity, no inference. Copybook parsing produces the schema, a conversion codec (EBCDIC/packed decimal to native types), and a `shape` definition, all from one artifact.

---

## Usage examples

### Extractor development loop

```bash
# Start a Postgres twin on port 5433 with a schema file
twinning postgres --schema schema.sql --port 5433

# One-shot: run extraction, get a twin report plus attached verify results, exit
twinning postgres --schema schema.sql --verify schema.verify.json --port 5433 \
  --run "python extract.py" --report twin-report.json

# Agent iteration loop (typical factory usage)
# 1. Start fresh twin
twinning postgres --schema schema.sql --verify schema.verify.json --port 5433 \
  --run "python extract_deal_42.py" --report deal_42_report.json
# 2. Agent reads the twin report and attached verify output, fixes extractor, re-runs in seconds
# 3. Repeat 20x per hour until the twin and verify signals stabilize
```

### Later-mode migration proof

```bash
# Boot Twin A (legacy schema) and Twin B (target schema) for loan performance mart
twinning postgres --schema oracle-loan-tables.sql --port 5433 &
twinning postgres --schema loan-perf-schema.sql --verify loan-perf.verify.json --port 5434 &

# Load migrated data into Twin A
psql -p 5433 -f load_oracle_slice.sql

# Load transformed data into Twin B
psql -p 5434 -f load_loan_perf_data.sql

# Replay historical queries against Twin A (factory handles comparison)
factory replay --queries scan-results/queries/risk-app.sql \
  --oracle oracle://prod-readonly \
  --twin localhost:5433 \
  --output replay-results/

# Score Twin B against spine
benchmark loan_perf_export.csv --assertions gold.jsonl --key loan_id --json > benchmark.json
verify run loan-perf.verify.json --bind loan_perf=loan_perf_export.csv --json > verify.json
assess benchmark.json verify.json --policy migration.v1 > decision.json

# Seal evidence for both proofs
pack seal replay-results/ benchmark.json verify.json decision.json \
  --output evidence/loan-performance-mart/
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
| **factory** | Factory uses `twinning` as a scale-phase speed layer for tournament iteration and may later orchestrate twin-pair migration proof on top of the same kernel. |
| **decoding** | Decoding resolves claims into canonical mutations; the twin enforces constraints on those mutations |
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
- Protocol fidelity comes before SQL breadth. If clients cannot connect cleanly, new SQL support is wasted work.
- Capability growth must be explicit. Expand the supported subset by growing the canary corpus and manifest, never by vague claims.
- Backend abstraction comes before the second interface. Do not hard-code SQL-shaped storage into the kernel.
- Tournament mode ships before replay/proof mode. Fast agent iteration is the first value.
- Whole-corpus replay does not need to be memory-only. Use a heavier backend when the economics demand it.
- Heavy backends may delegate storage, but they must not change the protocol-facing contract.
- VSAM is the first non-SQL target. Oracle TNS and CICS are explicitly deferred.

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

### Phases

| Phase | Goal | Deliverables | Hard gate to continue | Stop / redirect if |
|------|------|--------------|------------------------|--------------------|
| **0** | Bootstrap the artifact surface | CLI, DDL/catalog parsing, verify-artifact loading, deterministic report, snapshot hashing | Done in the current repo | n/a |
| **1** | Make common Postgres clients connect and execute a first parameterized round trip | Startup/auth handshake, backend key data, parameter status, simple query path, minimal extended-query path, `SET`/`BEGIN`/`COMMIT`/`ROLLBACK` ACKs, minimal session state, correct protocol/error framing | `psql` smoke, `psycopg2` parameterized smoke, and SQLAlchemy Core smoke all pass without app-side hacks | If common clients cannot complete the parameterized canaries through `pgwire` after a bounded spike, stop adding SQL features and reassess the protocol strategy |
| **2** | Make the write path correct | `INSERT`, `ON CONFLICT`, PK/UNIQUE/FK/NOT NULL/CHECK, type coercion, SQLSTATE mapping, deterministic snapshot/restore, overlay-safe mutations | Differential tests vs real Postgres pass for the declared write subset with exact SQLSTATE parity; extractor canaries can write unchanged | If error codes, coercion, or upsert behavior drift from Postgres on repeated gold cases, stop and fix the kernel before adding reads |
| **3** | Support the read subset extractors actually use | Declared `SELECT` subset, predicates, joins only if the canary corpus demands them, basic aggregates / `GROUP BY` only if demanded, `UPDATE`, `DELETE`, minimal catalog stubs, explicit SKIP reporting | The curated query corpus meets the acceptance budgets below, and unsupported features are classified explicitly rather than guessed | If unsupported-query rate stays high for the real corpus, narrow the supported subset and stop claiming broader compatibility |
| **4** | Make tournament mode swarm-safe | Backend trait, shared base snapshot, bounded-memory hot working set, per-agent copy-on-write overlay, lazy hydration, fast reset, memory-budget reporting | Tournament twins meet the startup/reset/private-RSS budgets below on the reference canaries | If per-agent twins still need large resident state, stop interface expansion and fix storage economics first |
| **5** | Add replay / proof mode | Snapshot-backed or disk-backed backend, optional delegation to real Postgres for full-corpus replay, replay harness, result diffing, evidence outputs | Historical query replay works against the heavy backend with reproducible reports and unchanged protocol-facing behavior | If full replay only works in full-RAM mode, do not scale that design; redirect to a heavier backend |
| **6** | Prove the interface-emulator model beyond SQL | VSAM adapter, copybook parser, keyed store semantics, GnuCOBOL harness, batch replay proof | At least one real COBOL/VSAM program runs against the twin unchanged for the supported operation subset | If VSAM requires special-case hacks that bypass the kernel abstraction, refactor the kernel before attempting IMS/CICS |

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
- `extractor_canary`: one real factory extractor script, run unchanged against the twin
- `heavy_backend_canary`: the same operation stream executed through the replay/proof backend to prove backend swap does not change observable behavior

The rule is simple: no new feature claim lands without a canary or differential fixture that proves it.

### Immediate work order

The next sequence should match the current Beads queue:

1. `bd-399`: build the compatibility and differential harness first; every later phase depends on it.
2. `bd-1jd`: land the pgwire listener only far enough to satisfy the phase-1 canaries.
3. `bd-372`: implement the row store and constraint executor to satisfy the phase-2 gold corpus.
4. `bd-28r`: add bounded-memory overlays and the replay/proof backend boundary before widening the query surface or adding another interface.
5. `bd-wij`: layer embedded `verify` execution and raw twin metrics reporting on top once semantics and storage behavior are trustworthy.

### Test strategy

The implementation lives or dies by the harnesses, not the prose.

- **Client compatibility suite:** real `psql`, `psycopg2`, SQLAlchemy, and later `asyncpg`, running startup, session, transaction, and error-path canaries.
- **Differential semantics suite:** the same DDL/DML/query corpus executed against real Postgres and the twin, with result and SQLSTATE comparison.
- **Extractor canary suite:** a small set of real extractor scripts from factory use cases, run unchanged against the twin.
- **Storage-economics suite:** startup time, reset time, overlay size, hot-working-set growth, and concurrent-twin memory budgets.
- **Replay suite:** historical query corpora and expected result packs for heavy replay/proof mode.
- **Snapshot suite:** restore fidelity, content-address determinism, and overlay isolation checks.

### Initial success criteria

`twinning` is credible when all of the following are true:

- A Python extractor using `psycopg2` or SQLAlchemy Core can point at the twin and run unchanged for the declared subset.
- Unsupported operations are explicit refusals or SKIPs, never silent wrong answers.
- A per-agent tournament twin stays inside the private-RSS and reset budgets defined above.
- Heavy replay can use a non-RAM-only backend without changing the protocol-facing contract.
- The first non-SQL adapter can reuse the same kernel/backend shape instead of forcing a rewrite.

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
- If whole-corpus replay pressures the architecture toward huge per-agent RAM footprints, switch to the heavy backend instead of compromising swarm mode.
- If VSAM cannot reuse the kernel/backend abstraction, the abstraction is wrong.

---

## Determinism

Same schema + same operation stream + same base snapshot = same twin state. No randomness, no side effects beyond the selected backend semantics. Snapshots are content-addressed — same state produces the same hash.
