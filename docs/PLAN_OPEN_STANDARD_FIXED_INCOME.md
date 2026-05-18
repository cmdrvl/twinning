# Open Fixed Income Data Standard — Strategic Plan

**Hub:** tranchelist.com  
**Engine:** twinning (Postgres wire twin + semantic inference + spine tools)  
**First participant:** cmdrvl-curves  
**First domain:** US Treasury bond reference data

---

## The Problem

Fixed income data is the most fragmented major asset class in financial markets.
Every participant — fund managers, primary dealers, risk systems, clearinghouses,
regulators — maintains custom mappings between their internal schema and every
counterparty or vendor they exchange data with. This costs billions annually in
integration work, data quality remediation, and reconciliation overhead.

The fragmentation is not for lack of effort. Existing initiatives have failed to
close it:

| Initiative | What It Covers | Why It Hasn't Solved This |
|-----------|----------------|--------------------------|
| **FIGI** | Security identifiers only | Identity, not schema or content |
| **LEI** | Legal entity identifiers | Same — identity layer only |
| **ACTUS** | Algorithmic contract types | Theoretically elegant, near-zero production adoption |
| **ISDA CDM** | Derivatives, regulatory reporting | Focused on derivatives; bond reference data out of scope |
| **ICMA Bond Data Taxonomy** | Issuance workflows | Nascent; XML-based; no production implementation |
| **FIX Protocol** | Order messaging | Pre-trade only; no reference data coverage |

The specific gap that none of these address: **a machine-readable open schema for
the terms and conditions of a fixed income instrument** — coupon, maturity,
day-count convention, call/put features, issuer identity, issue date, currency —
in a format that different systems can exchange without writing a custom mapping.

Every firm has this data. Every firm stores it differently. When two firms need to
exchange it, someone writes an ETL. This has happened millions of times and will
happen millions more times unless a neutral standard exists.

---

## The Opportunity

The first wedge is **US Treasury bond reference data**. Reasons:

- Simpler structure than corporate bonds — no complex embedded optionality, no
  covenant packages, no rating complexity
- The underlying data is public (TreasuryDirect, FiscalData.treasury.gov) but
  unharmonized — no single machine-readable schema exists
- Enormous constituency: treasuries traders, asset managers, pension funds,
  academics, central banks, risk systems
- No regulatory mandate forcing a proprietary solution, so no incumbent owns this
- A standard here becomes the template for agency bonds, then investment-grade
  corporates, then high yield

Approximately 40 core fields cover the vast majority of use cases:

```
instrument_id (CUSIP + ISIN)    coupon_rate
maturity_date                    coupon_frequency
issue_date                       day_count_convention
first_coupon_date                issuer_lei
original_face_value              currency
call_schedule (structured)       put_schedule (structured)
reopening_flag                   auction_date
```

---

## The Methodology — Usage-Derived, Not Committee-Imposed

Every existing fixed income standard was designed by a working group and then
participants were asked to adopt it. This one works in reverse: the standard is
derived from what participants actually do, and it is proved sufficient for their
declared workloads before anyone is asked to adopt it.

This is the differentiated claim. It is also verifiable — every participant
receives a cryptographic equivalence proof that the federated schema serves their
actual query patterns.

### How It Works

**Each participant runs locally, in their own environment:**

1. `twinning postgres --schema their_schema.sql` — bootstrap a twin from their
   DDL; no data leaves
2. Export `pg_stat_statements` — already normalized by Postgres to `$1/$2`
   placeholders, no actual values present; participant reviews and optionally
   redacts before sharing
3. Semantic inference runs locally — agent reads schema and query patterns,
   produces an abstract semantic model: entity names, relationships, access
   patterns; no table names or column names unless the participant opts in
4. Participant reviews the output and approves what to share
5. One file is shared with the federation: the abstract semantic model

**The federation proposes schema increments:**

6. The federated agent synthesizes across all participants' semantic models and
   proposes the next DDL increment — one table, one field, one constraint at a
   time
7. Each participant receives the proposed DDL and runs locally:
   `twinning federate validate --increment N` — applies the proposed DDL to
   their local twin, runs their declared query patterns against it, reports
   pass/fail in seconds
8. The increment is accepted when all participants pass; failing participants
   report which specific query patterns fail, which drives the next revision

**Each participant receives back:**

- The proposed federated DDL (the standard schema)
- Generated SQL views that map their internal schema to the standard —
  `CPNRT → coupon_rate`, `BOND_REF_DATA → bond` — produced automatically from
  the semantic mapping
- An equivalence proof: running queries through the generated views against the
  federated schema returns equivalent results to their internal schema directly

**What participants never share:** actual data rows, values, credentials,
customer identifiers, position sizes, proprietary business logic. Only the
abstract shape of their data model and how they use it.

---

## The Tooling

### What Exists Today (twinning)

- `twinning postgres --schema <file>` — DDL file in, bootstrapped Postgres twin
  out; structural validation and catalog normalization working
- `--materialize-source-url <url>` — pulls actual data from a live Postgres via
  `psql COPY` into the twin; requires schema file first
- `--restore <snapshot>` — restore from content-addressed snapshot
- `sqlparser` already a dependency — SQL parsing without a new library

### What Needs to Be Built

**`--extract-schema-from <url>`** — pulls DDL directly from a live Postgres via
`information_schema` instead of requiring a file export. This is the frictionless
onboarding path: one connection string, no manual `pg_dump --schema-only` step.

**View generator** — takes the semantic field mapping (internal schema →
federated schema) and emits the SQL views that bridge them. The participant
installs these views in their own database; consumers query via the views; the
internal tables are never exposed.

**Federation validation loop** — `twinning federate validate --increment N`:
applies a proposed federated schema increment to the participant's local twin,
runs the declared query workload against it, reports pass/fail with the specific
failing queries annotated. Runs entirely locally; no data leaves.

### The Spine Connection

For data sharing scenarios where participants want to verify data quality beyond
structural compatibility, the existing Crucible spine tools apply:

- **`benchmark`** — scores a data provider's sample against a consumer's declared
  gold set; measures coverage
- **`verify`** — validates data against declared constraints (value ranges,
  referential integrity, freshness)
- **`assess`** — policy decision: is this data provider a viable source for this
  use case?
- **`pack`** — sealed evidence bundle for compliance, audit, or board-level
  decision artifacts

---

## Data Sharing

The federation standardizes the schema. It does not move data. Data sharing
between participants happens through whatever channel works for both parties.
The standard is the common language; the transport is a separate decision.

**Snowflake native sharing.** Both parties on Snowflake → share the federated
views zero-copy via Snowflake's native sharing mechanism. No data leaves either
account. The consumer queries views in the provider's account. This is the same
mechanism the Snowflake Marketplace uses, now pointed at a privately shared
standard-schema dataset.

**Twin as a data service.** A participant runs a Postgres twin bootstrapped with
their data in the federated schema and exposes it to counterparties. Consumers
connect with any Postgres-compatible client and query standard SQL. The twin is
the service endpoint; the internal database is invisible.

**REST endpoint.** Derive an OpenAPI spec from the federated DDL (table/column
structure maps directly to REST endpoint shapes) and run a REST twin. Consumers
call HTTP endpoints that return data in the standard schema. No custom API code
needed.

**Files.** Generate CSV, JSON, or Parquet exports using standard field names.
Immediately consumable without mapping by any participant who has adopted the
schema.

---

## tranchelist.com as the Hub

tranchelist.com is the right home for this initiative for one reason: it is not
owned by a vendor. Bloomberg, Refinitiv, FactSet, and the large banks all have
strong incentives to keep fixed income data fragmented — proprietary data formats
are the source of their moat. ISDA and ICMA working groups are dominated by the
same institutions. A neutral standard requires a neutral host.

**What tranchelist.com publishes:**

- The current federated schema DDL — versioned, MIT licensed, JSON and XML
  serializations, GitHub-backed
- The methodology: how the schema was derived, participant count, which
  increments came from which participant feedback (anonymized)
- Equivalence proofs for each schema version: any participant who contributed
  has a proof that the current schema serves their declared query workload
- A public Postgres twin of FiscalData.treasury.gov data in the federated schema
  — the reference implementation anyone can query before participating

**What tranchelist.com does not do:**

- Hold participant data
- Require identity disclosure
- Charge for the schema
- Govern the standard through a committee — schema evolution is driven by
  participant validation results, not votes

---

## Participant Journey

### Onboarding (one afternoon)

```bash
# Install
brew install twinning

# Point at your schema (today: file; soon: live connection)
pg_dump --schema-only your_db > schema.sql
twinning federate init --schema schema.sql

# Point at your query log
twinning federate analyze --pg-stat-statements postgresql://your-host/your-db

# Review what will be shared — nothing leaves yet
twinning federate preview

# Share the semantic model
twinning federate submit --endpoint tranchelist.com/federate
```

### Receiving a Proposed Increment

```bash
# Validate locally — does this increment serve your query patterns?
twinning federate validate --increment 7

# Output:
# Increment 7 validation: PASS
# 847 query patterns tested against proposed schema
# 831 optimal plans, 14 degraded, 2 missing index suggestions
# Missing indexes: bond(maturity_date), bond(issuer_lei)
# Suggestion attached: CREATE INDEX ...

# Submit result
twinning federate report --increment 7
```

### Getting Value Back

```bash
# Generate the views that bridge your internal schema to the standard
twinning federate views --output ./db/views/treasury_standard.sql

# Output: CREATE VIEW bond AS SELECT CPNRT AS coupon_rate, ...
# Install in your database or use as materialization source for a twin

# Verify the views are equivalent to your internal queries
twinning federate verify-views --source postgresql://your-host/your-db
```

---

## Sequence

### Phase 1: Seed (cmdrvl-curves goes first)

1. Export cmdrvl-curves schema DDL
2. Export `pg_stat_statements` from cmdrvl-curves Postgres
3. Run semantic inference against both — Claude does this step today with
   existing twinning catalog output; no new tooling required for this step
4. Produce v0.1 anonymous semantic model and v0.1 federated Treasury schema
5. Stand up public Postgres twin of FiscalData.treasury.gov data in the v0.1
   schema at tranchelist.com
6. Publish: schema DDL, methodology disclosure, equivalence proof for participant
   zero (cmdrvl-curves), instructions for joining

**The key disclosure at launch:** "This standard was derived from one
participant's production database. Here is the anonymous semantic model it came
from. Here is the equivalence proof that the v0.1 schema serves every query
pattern that database declared. We are seeking a second participant."

### Phase 2: Second Participant (one warm introduction)

One relationship — not a campaign. Someone at CMD+RVL knows someone at a fund,
a desk, or a data vendor who is frustrated by this fragmentation. The ask: "We
are participant zero. We've published our methodology and our equivalence proof.
Would you be participant one?"

When the second participant's semantic model causes the federated schema to
update, publish the change at tranchelist.com with both equivalence proofs
attached. "The standard evolved to serve two production databases. Here is the
proof it still serves both."

### Phase 3: Reference Implementation and Tooling

- `--extract-schema-from <url>` flag in twinning (frictionless onboarding)
- View generator (automatic SQL view creation from semantic mapping)
- Federation validation loop (local pass/fail against proposed increments)
- CLI commands documented at tranchelist.com

### Phase 4: Expand the Domain

- Agency bonds (Freddie, Fannie, FHLB) — same core schema, minor extensions
- Investment-grade corporate bonds — add covenant and rating fields
- High-yield — embedded options, PIK features, covenant packages

Each expansion is driven by participant demand: if two participants have a query
pattern that the current schema cannot serve, that is the signal to extend it.

---

## The Marketing Claim

*The only fixed income data standard derived from what the industry actually does,
not from what a committee decided it should do.*

*Every field in the schema exists because at least one production database needed
it. Every version of the schema is proved sufficient for every participant's
declared query workload. The proof is public. The schema is free.*

This claim is:
- **Differentiated** — no existing standard can make it
- **Verifiable** — the equivalence proofs are public artifacts
- **Defensible** — it gets stronger with every participant, not weaker

---

## Connection to Cairn

The fixed income open standard is the foundation layer for Cairn as a data and
intelligence agent in this market. Once the standard exists:

- Cairn can compare any dataset against the standard schema structurally —
  the twin handles structural qualification
- Cairn can score data providers via `benchmark` against a participant's declared
  gold set — the spine handles coverage and quality
- Cairn can generate, validate, and optimize SQL against any participant's
  standard-compliant database — the SQL tournament and query log replay apply
  directly
- Cairn can run the Snowflake Marketplace optimizer against the catalog of
  providers who have published standard-compliant listings
- The industry schema federation gives Cairn cross-company intelligence that
  no single-tenant agent can have

The standard is the moat. Cairn is the agent that operates on top of it. The
combination is the product.

---

## What This Is Not

- A data vendor — tranchelist.com does not sell data
- A committee — schema evolution is driven by validation results, not votes
- A replacement for Bloomberg or Refinitiv — those provide market data, pricing,
  analytics; this provides the schema to exchange reference data between systems
- A Snowflake-specific initiative — the schema is database-agnostic; Snowflake
  native sharing is one transport among several
- Complete from day one — v0.1 covers US Treasuries; the rest is earned by
  demonstrating that the methodology works
