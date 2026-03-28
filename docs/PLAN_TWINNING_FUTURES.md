# twinning futures

This document holds the real `twinning` directions that are intentionally
outside the v0 implementation wedge in
[PLAN_TWINNING.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING.md).

The main plan stays focused on:

- one Postgres tournament twin
- one canary-defined client/session subset
- one embedded `verify` attachment path
- one bounded-memory tournament / heavier-backend boundary

Everything here is valuable, but deferred until that center is real.

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
result diffing, and broader Crucible evidence flows that are not required to
stabilize the v0 tournament boundary.

### Later-mode migration proof example

```bash
# Boot Twin A (legacy schema) and Twin B (target schema) for loan performance mart
twinning postgres --schema oracle-loan-tables.sql --port 5433 &
twinning postgres --schema loan-perf-schema.sql --verify loan-perf.verify.json --port 5434 &

# Load migrated data into Twin A
psql -p 5433 -f load_oracle_slice.sql

# Load transformed data into Twin B
psql -p 5434 -f load_loan_perf_data.sql

# Replay historical queries against Twin A (Crucible handles comparison)
crucible replay --queries scan-results/queries/risk-app.sql \
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

---

## Beyond v0: the generalized interface model

The twin's core abstraction is not "a SQL database emulator." It's **an
interface emulator**: speak the protocol the client expects, enforce the
schema's constraints, and keep enough behavioral state to answer correctly. SQL
wire protocols are one interface. There are others, and the architecture should
eventually support them without changing Crucible, the convergence model, or
the tournament.

### Non-SQL twin types

| Interface | Protocol | What programs expect | Twin implementation | Complexity vs Postgres twin |
|-----------|----------|---------------------|--------------------|-----------------------------|
| **VSAM** | COBOL file I/O (`OPEN`, `READ`, `WRITE`, `REWRITE`, `DELETE`, `START`, `CLOSE`) | Keyed or sequential access to fixed-format record files | Keyed byte-array store with hot pages in RAM and optional snapshot/disk backing | **Simpler** — no SQL parsing, no query planning, ~3-4K LOC |
| **IMS/DL/I** | Hierarchical navigation (`GU`, `GN`, `GNP`, `ISRT`, `REPL`, `DLET`) | Tree traversal over segments defined by a DBD | Hierarchical store with hot segments in RAM and pluggable persistence | **Harder** — navigational semantics are subtle, ~5-8K LOC |
| **Flat files** | Sequential I/O (`READ`/`WRITE` with copybook layout) | Fixed-length records in EBCDIC with packed decimal fields | Byte-stream twin with hot windows in memory and copybook-defined field offsets | **Simplest** — no indexing, no constraints, ~1-2K LOC |
| **CICS** | Transaction dispatch (`EXEC CICS` commands) | Screen input -> program -> DB/file updates -> screen output | Transaction router + CICS API surface emulation | **Hardest** — hundreds of commands; pragmatic path is mock top 50 or use Micro Focus commercially |

Each twin type follows the same contract:

- Speak the interface the program expects
- Keep enough behavioral state to answer correctly
- Keep the hot working set in memory; use snapshots, overlays, or heavier
  backends for the rest
- Enforce constraints from the schema definition (DDL, DBD, copybook)
- Support content-addressed snapshots
- Report raw twin-native metrics

The VSAM twin is the highest-value addition. COBOL batch programs that read
VSAM datasets are a large share of typical mainframe workloads, and the VSAM
access pattern is simpler than SQL. A VSAM twin plus GnuCOBOL enables
off-mainframe batch job replay: compile the COBOL program, point its file I/O
at the VSAM twin, run it, capture output, and diff it against known-good output
from the mainframe using whatever audit surface Crucible standardizes
(`rvl --exhaustive`, `pack diff`, or a twin-specific result comparator).

### Schema definitions by twin type

| Twin type | Schema source | What it declares |
|-----------|--------------|-----------------|
| SQL (Postgres/MySQL/Oracle) | DDL file | Tables, columns, types, PK, FK, CHECK, UNIQUE, NOT NULL |
| VSAM | COBOL copybook | Record layout: field offsets, lengths, PIC clauses (exact byte-level types) |
| IMS | DBD (Database Description) | Segment hierarchy, field definitions, search fields |
| Flat file | COBOL copybook | Same as VSAM — record layout at byte level |

The copybook is the schema for non-SQL twins. `PIC S9(7)V99 COMP-3` declares a
signed 7.2 packed decimal at a specific byte offset. Copybook parsing should
produce the schema, a conversion codec, and a `shape` definition from one
artifact.

---

## Why this stays separate

This material belongs in the repository, but not in the v0 implementation doc.

Reasons:

- it is easy for agents to overbuild toward migration proof or non-SQL adapters
  if the main plan keeps them in the center
- the Postgres tournament wedge needs clean rubric-grade boundaries
- the futures are still real design input for the kernel, but they should shape
  abstractions without widening the first implementation target
