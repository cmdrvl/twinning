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

## Priority among deferred artifacts

If `twinning` expands beyond the v0 center in exactly one direction first, it
should be twin-pair migration proof.

Why this comes before the other deferred artifacts:

- it reuses the same Postgres kernel, snapshot contract, and verify/report
  boundaries as the v0 center
- it creates new migration-proof value without opening a new protocol family
- it forces the replay/proof and evidence boundaries that the main plan already
  sketches
- it keeps VSAM / IMS / CICS as true later expansions instead of widening the
  first post-v0 implementation front

Non-SQL interface twins remain real future work, but they should not preempt
the first migration-proof mode.

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

### Legacy-query replay boundary

The first migration-proof cut is **translated Postgres-compatible replay** over
the catalog-declared subset. Historical query families may be derived from a
legacy workload, but they must enter `twinning` as declared Postgres-compatible
queries against the legacy-schema twin. True Oracle/TNS protocol fidelity is
deferred to a later adapter and must not be implied by Twin A.

This boundary is deliberate:

- the v0 kernel and protocol surface remain Postgres-only
- every replay shape must be named in the replay manifest as PASS, FAIL, or
  SKIP
- unsupported joins, introspection, Oracle syntax, and non-Postgres protocol
  claims stay SKIP or process-level proof refusals until there is a manifest
  and canary-backed implementation
- successful proof means interface equivalence for the declared translated
  Postgres subset, not whole legacy-database compatibility

### Later-mode migration proof example

```bash
# Boot Twin A (legacy schema) and Twin B (target schema) for loan performance mart
twinning postgres --schema oracle-loan-tables.sql --port 5433 &
twinning postgres --schema loan-perf-schema.sql --verify loan-perf.verify.json --port 5434 &

# Load migrated data into Twin A
psql -p 5433 -f load_oracle_slice.sql

# Load transformed data into Twin B
psql -p 5434 -f load_loan_perf_data.sql

# Replay translated Postgres-compatible historical query families against Twin A
crucible replay --queries replay-manifest.translated-postgres.json \
  --reference postgres://legacy-readonly \
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

### Production twin-pair orchestration surface

The current `twinning proof twin-pair` command is a snapshot-pair prototype: it
compares two already-frozen `twinning.snapshot.v0` artifacts over one query
fixture. Production migration proof should add a manifest-first orchestration
surface instead of stretching the prototype flags into a second runtime mode.

Proposed later command:

```bash
twinning proof twin-pair orchestrate \
  --manifest proof-run.json \
  --report out/twin-pair-proof.json \
  --bundle-dir out/twin-pair-proof/
```

The manifest should be the single operator contract for a paired proof run:

| Field | Owner | Contract |
|-------|-------|----------|
| `proof_id` | operator / orchestrator | Stable run identity used in reports and evidence bundles |
| `catalog_declaration` | catalog / declaration layer | Same subset identity both twins must share |
| `left_endpoint` | twinning | Legacy/source twin bootstrap or restore input, endpoint id, role, engine |
| `right_endpoint` | twinning | Candidate/target twin bootstrap or restore input, endpoint id, role, engine |
| `replay_manifest` | replay harness | Query families, PASS / FAIL / SKIP policy, expected SQLSTATE parity |
| `target_evidence` | verify / benchmark / assess | Raw artifact identities only; no policy meaning is interpreted by twinning |
| `artifact_outputs` | pack / operator | Report path, snapshot paths, bundle directory, optional seal handoff |

The orchestrator phases should be deterministic and restartable:

1. Preflight both endpoint specs before binding any listener.
2. Materialize or restore each endpoint and freeze committed-state snapshots.
3. Verify both snapshots share the declared schema/catalog/declaration identity.
4. Replay the manifest against both twins through the declared protocol surface.
5. Emit `twinning.twin-pair-proof.v0` with endpoint identities, result parity,
   target evidence identities, and no pseudo-score aggregation.
6. Hand report/snapshot paths to `pack`; sealing remains outside `twinning`.

Refusal boundaries:

- incompatible schema hashes, normalized catalogs, declarations, engines, or
  snapshot versions are process-level proof refusals
- malformed manifests or target evidence identity records are process-level
  proof refusals
- unsupported replay shapes inside a declared SKIP row are accounting entries,
  not success
- unsupported replay shapes that are executed must surface as protocol-visible
  errors and be recorded as observations, not process exits
- `verify`, `benchmark`, and `assess` outcomes never change the twin-pair proof
  verdict; their identities are attached so downstream policy can consume them

### Gaps from current repo to first twin-pair proof

Current repo status now has the Postgres `run_once` shell, committed-state
snapshot hashing, catalog declaration identity, a prototype fixture-backed
`twinning.twin-pair-proof.v0` receipt, and a typed parser for the proposed
orchestration manifest. The shortest path to a production twin-pair proof is:

1. finish the v0 center from
   [PLAN_TWINNING.md](/Users/zac/Source/cmdrvl/twinning/docs/PLAN_TWINNING.md)
   phases 1-4
2. add the migration-proof-specific surfaces below

The remaining blocking gaps are:

- **Live dual-endpoint orchestration runner gap.** The proposed manifest-first
  operator surface above names how production proof should boot, load, name,
  and hand off paired twins, and the manifest parser exists. The code still
  needs to implement the runner without widening the v0 Postgres subset.
- **Replay corpus gap.** The current proof fixture covers translated
  Postgres-compatible point lookup, filtered scan, aggregate count, intentional
  divergence, and SQLSTATE parity. Its replay matrix now explicitly records
  PASS / FAIL / SKIP coverage, including SKIP accounting for join,
  introspection, and historical-query families. Twin A still needs executable
  fixtures for those broader replay families once the declared subset widens.
- **Legacy-query breadth gap.** The first cut is explicitly translated
  Postgres-compatible replay. True Oracle/TNS fidelity is deferred to a later
  adapter. The remaining work is breadth: more translated replay families and
  promotion of current SKIP/refusal accounting into proved replay rows as the
  subset expands.
- **Replay-result breadth gap.** `twinning.twin-pair-proof.v0` now includes a
  per-case `twinning.twin-pair-replay-result.v0` section with result hashes,
  SQLSTATE parity, and endpoint snapshot provenance. Broader live replay still
  needs more query-family coverage and pack-facing bundle layout.
- **Heavier-backend gap.** Twin-pair migration proof is the first consumer that
  may genuinely need the snapshot-backed, disk-backed, or delegated backend
  path. The backend boundary exists in prose, but the first migration-proof cut
  still needs an explicit policy for when Twin A can delegate storage without
  changing protocol-visible behavior.
- **Target-side evidence orchestration gap.** The prototype proof report can
  attach raw `verify`, `benchmark`, and `assess` artifact identities to the
  target endpoint without interpreting their policy meaning. Production proof
  still needs operator orchestration for collecting, sealing, and linking those
  artifacts back to the twin snapshots and replay outputs.

None of those gaps justify widening the v0 center itself. They are the bridge
to build immediately after the main Postgres tournament wedge is real.

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
