# twinning — In-Memory Database Twin

## One-line promise
**Impersonate a database — speak the real wire protocol, enforce schema constraints, store everything in memory — so extraction code iterates in seconds, not hours.**

---

## Problem

You're building extractors that write to Postgres (or MySQL, or Oracle). Against real Postgres with 150M existing rows, each test takes minutes to hours. Schema changes require migrations. Constraint violations are buried in database logs. The feedback loop is too slow for agent-driven iteration.

`twinning` compresses the feedback loop from days to seconds. It speaks the real wire protocol (Postgres v3, MySQL, eventually Oracle TNS), accepts the same SQL, enforces the same constraints, and stores everything in memory. Existing client code — SQLAlchemy, psycopg2, JDBC — connects to it and can't tell the difference, for the subset of SQL that extraction code actually uses.

It is not a database. It is a **fast, ephemeral, constraint-checked store that impersonates a database well enough for extraction code to develop and test against.** The fidelity target is the client SDK, not the full database engine. If the client can't tell the difference for the SQL subset that extraction code uses, the twin is good enough.

### Core insight

This is the Digital Twin Universe insight from StrongDM applied to databases instead of SaaS APIs. An agent iterates 20 times per hour instead of once per day. The twin speaks the real wire protocol; existing client code can't tell the difference.

---

## Non-goals

`twinning` is NOT:
- A database (no persistence, no WAL, no crash recovery)
- A replacement for the customer's production database (no application points at the twin)
- A truth oracle (truth is determined by decode policy + gold set + evidence chain)
- A concurrent multi-writer system (single writer per instance)

No application points at the twin. The customer's web app, API, and reports continue to run against their production database. The twin exists only in the extractor development loop: agents write extraction code, test it against the twin (fast), prove it against the twin (constraints + verify rules + gold set), then deploy the proven code against real Postgres. The twin is a scorekeeper — it tells you whether your extractor works.

---

## CLI

```
twinning <ENGINE> [OPTIONS]

Arguments:
  <ENGINE>               Target database engine to impersonate (postgres, mysql, oracle)

Options:
  --schema <FILE>        SQL DDL file defining tables, constraints, indexes
  --rules <FILE>         Verify rules (JSON) for coverage scoring
  --port <PORT>          Listen port (default: engine-specific default)
  --host <HOST>          Listen address (default: 127.0.0.1)
  --run <COMMAND>        Run command against the twin, then report and exit
  --report <FILE>        Write coverage/quality report as JSON on exit
  --snapshot <FILE>      Dump twin state to content-addressed snapshot on exit
  --restore <FILE>       Restore twin state from a snapshot before accepting connections
  --json                 JSON output for status messages
```

### Exit codes

`0` clean (all rules pass, or no rules provided) | `1` violations (rules provided, some failed) | `2` refusal

### Usage modes

**Interactive:** Start the twin, connect with psycopg2/SQLAlchemy, iterate manually.
```bash
twinning postgres --schema schema.sql --port 5433
```

**With coverage scoring:** Start with verify rules, get a coverage report.
```bash
twinning postgres --schema schema.sql --rules rules.json --port 5433
```

**One-shot:** Start, run extraction, get coverage report, exit.
```bash
twinning postgres --schema schema.sql --rules rules.json --port 5433 \
  --run "python extract.py" --report coverage.json
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     twinning                             │
│                                                          │
│  ┌─────────────┐   ┌─────────────┐   ┌───────────────┐  │
│  │ Wire Proto  │   │ SQL Parser  │   │ Coverage      │  │
│  │ (pgwire)    │   │ (sqlparser) │   │ Scorer        │  │
│  │             │   │             │   │               │  │
│  │ Postgres v3 │   │ INSERT      │   │ row counts    │  │
│  │ MySQL proto │   │ UPSERT      │   │ null rates    │  │
│  │ Oracle TNS  │   │ SELECT      │   │ FK coverage   │  │
│  │             │   │ CREATE TABLE│   │ verify rules  │  │
│  └──────┬──────┘   └──────┬──────┘   └───────┬───────┘  │
│         │                 │                   │          │
│  ┌──────▼─────────────────▼───────────────────▼───────┐  │
│  │              In-Memory Store                       │  │
│  │                                                    │  │
│  │  Table = HashMap<PrimaryKey, Row>                  │  │
│  │        + Vec<Column> (metadata, types)             │  │
│  │        + HashSet per UNIQUE constraint             │  │
│  │        + FK references (PK lookup into other table)│  │
│  │                                                    │  │
│  │  Constraint checker (inline, per-row):             │  │
│  │    NOT NULL · CHECK expr · UNIQUE · FK · type coerce│ │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## SQL support

The subset of SQL that extraction code uses:

| SQL | Support | Notes |
|-----|---------|-------|
| `CREATE TABLE` | Full DDL | columns, types, PK, UNIQUE, NOT NULL, CHECK, FK, DEFAULT |
| `INSERT INTO` | Full | single-row and multi-row, RETURNING |
| `INSERT ... ON CONFLICT DO UPDATE` | Full | composite keys, SET clause |
| `INSERT ... ON CONFLICT DO NOTHING` | Full | |
| `SELECT ... WHERE` | Basic | equality, comparison, AND/OR, IS NULL, IN, BETWEEN |
| `SELECT COUNT/SUM/AVG/MIN/MAX` | Full | aggregate queries for coverage scoring |
| `SELECT ... GROUP BY` | Basic | single-level grouping |
| `UPDATE ... WHERE` | Basic | for correction workflows |
| `DELETE ... WHERE` | Basic | for cleanup workflows |
| `BEGIN/COMMIT/ROLLBACK` | Acknowledged | single-writer, no real isolation needed — ACK and proceed |
| `SET` / session variables | Acknowledged | SQLAlchemy sends these on connect — ACK and ignore |

### What it skips

- MVCC / transaction isolation (single writer per instance)
- WAL / crash recovery (in-memory, ephemeral)
- Vacuum / dead tuples (upsert overwrites in the HashMap)
- Cost-based query optimizer (simple scan/hash-lookup is sufficient)
- JOINs across large tables (coverage queries don't need them; add later if needed)
- Window functions, CTEs, recursive queries, subqueries
- LISTEN/NOTIFY, advisory locks, cursors, prepared statements beyond basic
- Replication, roles, permissions, tablespaces, extensions
- TOAST / large objects
- Full system catalog (only schema definitions)

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

### Full load throughput

| Scale | Time | Notes |
|-------|------|-------|
| 200K rows (per-agent iteration) | <1 second | The iteration loop |
| 10M rows (single deal, all tables) | 3-10 seconds | Per-deal validation |
| 150M rows (full corpus, all tables) | 1-3 minutes | Full twin validation run |

These numbers assume Rust HashMap with FxHash or similar fast hasher, pre-allocated capacity, and minimal allocation during insert. Memory usage: ~50-100 bytes per row typical (depends on column count and types), so 150M rows ~ 8-15 GB RAM. Fits in a single machine.

---

## Coverage scoring

The twin has a built-in coverage scorer that runs `verify` rules against its in-memory state plus additional structural checks:

```json
{
  "version": "twinning.v0",
  "engine": "postgres",
  "schema": "cmbs.v1",
  "tables": {
    "deals":        { "rows": 3500, "expected": 3500, "coverage": 1.0 },
    "loans":        { "rows": 412000, "expected": null, "coverage": null },
    "properties":   { "rows": 389000, "expected": null, "coverage": null },
    "financials":   { "rows": 48200000, "expected": null, "coverage": null },
    "payments":     { "rows": 67100000, "expected": null, "coverage": null },
    "modifications": { "rows": 2100000, "expected": null, "coverage": null }
  },
  "constraints": {
    "not_null_violations": 0,
    "fk_violations": 0,
    "check_violations": 0,
    "unique_violations": 0
  },
  "verify_rules": {
    "pass": 12,
    "fail": 2,
    "violations": [
      { "rule_id": "NOI_CALC", "count": 47, "sample": { "property_id": "P-123", "period": "2024-12" } },
      { "rule_id": "DSCR_POSITIVE", "count": 3, "sample": { "property_id": "P-456", "period": "2024-06" } }
    ]
  },
  "null_rates": {
    "financials.noi": 0.02,
    "financials.occupancy": 0.08,
    "financials.dscr": 0.03
  },
  "fk_coverage": {
    "loans.deal_id → deals.deal_id": 1.0,
    "financials.property_id → properties.property_id": 0.97
  }
}
```

### Anchored vs unanchored coverage

`expected` is populated from anchor points when available (deal documents, declared loan counts, trustee summary statistics). When `expected` is null, the twin can't measure completeness — only internal consistency.

This distinction is a design principle. Anchored coverage ("3,211 of 3,500 deals have financials data, verified against trustee-declared deal counts") is a real, auditable number. Unanchored coverage ("48.2M financial rows exist and pass all constraints") proves internal consistency and policy compliance — valuable, but a different claim. The twin never reports a coverage percentage without specifying whether it's anchored.

---

## Snapshots

```bash
# Dump current state
twinning postgres --schema schema.sql --port 5433 \
  --snapshot snapshots/2025-12-full.twin

# Restore and continue
twinning postgres --restore snapshots/2025-12-full.twin --port 5433
```

A snapshot is a content-addressed binary dump of the in-memory state (schema + all table data + constraint metadata). Snapshots enable:
- Fast twin recovery without replaying claim/mutation events
- Diffing twin states across time periods (export to CSV, run `rvl`)
- Evidence sealing (snapshot hash in evidence packs)

---

## Engine support roadmap

| Engine | Wire protocol | Existing crate | Priority |
|--------|--------------|----------------|----------|
| **Postgres** | Postgres v3 (FE/BE message protocol) | **pgwire** (Rust, production-grade) | v0 — ship first |
| **MySQL** | MySQL client/server protocol | **opensrv-mysql** (Rust) | v0.2 |
| **Oracle** | Oracle TNS/Net8 | None in Rust (would need custom impl or OCI shim) | Defer |
| **OpenSearch** | REST API (HTTP/JSON) | Hyper/Axum (Rust HTTP) | v0.3 — different architecture (REST, not wire protocol) |

Postgres first because: pgwire crate is mature, SQLAlchemy/psycopg2 is the most common client in Python data engineering, and the target database for the CMBS use case is Postgres.

Oracle is the hardest — TNS protocol is proprietary and poorly documented. The pragmatic path: if the production target is Oracle, validate the extraction logic against a Postgres twin (same SQL semantics for INSERT/UPSERT), then run a final conformance check against real Oracle.

---

## Usage examples

```bash
# Start a Postgres twin on port 5433 with a schema file
twinning postgres --schema schema.sql --port 5433

# Start with verify rules for coverage scoring
twinning postgres --schema schema.sql --rules rules.json --port 5433

# One-shot: run extraction, get coverage report, exit
twinning postgres --schema schema.sql --rules rules.json --port 5433 \
  --run "python extract.py" --report coverage.json

# Save state for later
twinning postgres --schema schema.sql --port 5433 \
  --snapshot snapshots/2025-12-full.twin

# Restore and continue from snapshot
twinning postgres --restore snapshots/2025-12-full.twin --port 5433

# Agent iteration loop (typical factory usage)
# 1. Start fresh twin
twinning postgres --schema schema.sql --rules rules.json --port 5433 \
  --run "python extract_deal_42.py" --report deal_42_coverage.json
# 2. Agent reads coverage report, fixes extractor, re-runs in seconds
# 3. Repeat 20x per hour until coverage targets met

# Full twin validation (all extractors, all files)
twinning postgres --schema schema.sql --rules rules.json --port 5433 \
  --run "python run_all_extractors.py" \
  --report full_coverage.json \
  --snapshot snapshots/full-run.twin
```

---

## Relationship to other tools

| Tool | Relationship |
|------|-------------|
| **verify** | Twin runs `verify` rules against its in-memory state for coverage scoring |
| **shape** | Twin's schema DDL is the structural contract; `shape` checks CSV inputs before they reach the twin |
| **benchmark** | Gold set assertions can be checked against twin state (export to CSV, run `benchmark`) |
| **assess** | Twin coverage report feeds `assess` for go/no-go decisions |
| **pack** | Twin snapshots can be included in evidence packs |
| **rvl** | Diffing twin states across time: export to CSV, run `rvl` |
| **decoding** | Decoding resolves claims into canonical mutations; the twin enforces constraints on those mutations |

---

## Implementation notes

### Implementation scope

| Component | Source | LOC estimate |
|-----------|--------|-------------|
| Wire protocol server | **pgwire** crate (Postgres v3) | ~500 (glue code) |
| SQL parser + router | **sqlparser-rs** crate (Postgres dialect) | ~500 (route parsed AST to store) |
| Schema catalog | Custom | ~1-2K |
| In-memory store (HashMap per table) | Custom | ~2-3K |
| Constraint checker (NOT NULL, CHECK, UNIQUE, FK, types) | Custom | ~1-2K |
| Upsert logic (ON CONFLICT) | Custom | ~500 |
| Basic SELECT executor | Custom | ~2-3K |
| Coverage scorer + verify integration | Custom | ~1-2K |
| Snapshot dump/restore | Custom | ~500-1K |
| Error code mapping (SQLSTATE) | Custom | ~300 |
| **Total** | | **~10-15K lines of Rust** |

### Candidate crates

| Need | Crate | Notes |
|------|-------|-------|
| Postgres wire protocol | `pgwire` | Production-grade, Rust |
| MySQL wire protocol | `opensrv-mysql` | Rust |
| SQL parsing | `sqlparser-rs` | Postgres dialect, mature |
| Fast hashing | `rustc-hash` (FxHash) | HashMap performance |
| Content hashing | `sha2` | Snapshot content addressing |

Follows the same implementation standards as protocol tools: `#![forbid(unsafe_code)]`, clap derive CLI, MIT license, CI (fmt -> clippy -> test), cross-platform release builds.

---

## Determinism

Same schema + same SQL statements in same order = same twin state. No randomness, no side effects beyond the in-memory store. Snapshots are content-addressed — same state produces the same hash.
