# twinning — alien artifact directions

The most ambitious ideas available to this repo, across the full twin type family:
Postgres wire, REST/HTTP, MCP/JSON-RPC, VSAM, IMS, flat files, CICS. Each idea
is developed at full ambition. None of these are near-term implementation targets;
all of them are architecturally reachable from the existing kernel abstractions.

The unifying framing: **twinning is not a mock framework. It is a protocol
reconstruction engine.** Any interface that can be described by a schema artifact
(DDL, OpenAPI spec, COBOL copybook, DBD, JSON Schema) can be twinned. Once you
accept that, the following become possible.

---

## 1. The temporal Postgres twin — time-travel debugging at the wire level

**The idea.** Every write the twin accepts is versioned internally with a
monotonic transaction ID. The in-memory store is an MVCC log, not a mutable row
table. The twin exposes a custom session variable:

```sql
SET twinning.as_of = '2025-11-03 14:22:07';
SELECT * FROM orders WHERE customer_id = 4491;
```

The twin answers as if it is November 3rd, 2025, at 14:22:07 — pulling the
committed state of every row as it existed at that moment. `SET twinning.as_of =
DEFAULT` returns to head. Any client that speaks pgwire — psql, pgAdmin, any ORM,
any language driver — gets time travel for free, without a single code change.

**The incident replay.** A production bug occurred at 14:22:07 on November 3rd.
Load the twin from a WAL-derived snapshot at that timestamp. Run the failing query.
Reproduce the exact database state that triggered the bug. Vary the query
interactively — as if you were at a Postgres REPL attached to the past.

**The MVCC architecture.** The twin's row store already needs MVCC semantics for
tournament-mode concurrent readers. The temporal query surface is the same
structure — retain all committed versions instead of GC-ing old ones. The storage
cost is bounded by the hot working set size plus a configurable retention window.
Beyond the retention window, versions are snapshotted and evicted to a content-
addressed snapshot chain: `twinning.snapshot.temporal.v0`. Any point in the
retention window is live-queryable; any point beyond it is restorable from the
snapshot chain.

**Differential time travel.** `SET twinning.as_of_left` and
`twinning.as_of_right` to run a query at two different timestamps and diff the
results. The twin emits both result sets with a structural diff annotation. This
is the incident analysis primitive: "what changed between the deployment at 14:00
and the incident at 14:22?"

**Migration safety.** Load the schema twin at the pre-migration state. Apply the
migration (DDL changes are applied as versioned schema transitions). Query the
post-migration state. Any query in the canonical corpus that returns different
results before and after the migration is a migration regression. This is not a
diff of the schema — it is a diff of the query results, which is the only thing
that matters to clients.

---

## 2. The universal protocol bridge — any client language to any backend via twin

**The idea.** The twin speaks multiple protocols: pgwire (Postgres), HTTP+REST,
JSON-RPC (MCP), VSAM file I/O, IMS DL/I navigation. Internally, all of these map
to the same kernel operation model: read, write, scan, upsert, delete against a
keyed in-memory store. The operations are protocol-agnostic; the protocol
adapters are the twin's surface.

Now wire the output of one twin to the input of another. A VSAM twin receives
COBOL file I/O calls, translates them to kernel operations, and instead of
answering from an in-memory store, routes the operations to a Postgres twin that
answers them via SQL. The COBOL program thinks it is reading a VSAM file. The data
lives in Postgres. No code in the COBOL program changes.

```
COBOL program
    → VSAM file I/O (OPEN, READ, WRITE)
    → VSAM twin (copybook codec, key translation)
    → Postgres twin (SQL generation from key operations)
    → in-memory row store
```

**Reverse direction: Postgres → REST.** A Python SQLAlchemy ORM speaks pgwire to
the Postgres twin. The twin translates SQL SELECT statements into REST API calls
to an upstream service. The ORM thinks it is querying a database. The upstream is
a microservice with an OpenAPI spec.

```
SQLAlchemy ORM (SQL over pgwire)
    → Postgres twin (SQL parser, query planner)
    → REST twin (HTTP calls against upstream)
    → upstream service
```

**The Rosetta Stone.** This is universal protocol translation. Any protocol the
twin speaks on the client side can be bridged to any protocol the twin speaks on
the backend side. The bridge is the twin's kernel — the same operation model,
with different adapters on each end.

**Why this matters at scale.** The canonical enterprise integration problem is: a
legacy system speaks protocol A; the modern system speaks protocol B; writing a
permanent protocol bridge is expensive and fragile. The twin provides both
adapters. The bridge is configuration, not code: declare the source schema (VSAM
copybook), declare the target schema (OpenAPI spec), declare the field mapping
between them. The twin handles the rest.

**Immediate application.** Every CMD+RVL integration point is a potential protocol
bridge. The aibuildout pipeline calls multiple REST APIs in sequence. The metadata
API speaks HTTP+REST. The cmdrvl-cli speaks the same HTTP+REST. The twin can sit
in the middle, translating between them, enforcing contracts on both sides
simultaneously — the upstream gets a validated request; the downstream gets a
contract-enforced response.

---

## 3. Adversarial equivalence prover — find the witness that proves two implementations disagree

**The idea.** Spin up two twins of the same spec on different ports: Twin A is
the reference implementation; Twin B is the candidate (new version, migration
target, alternative engine). Run the behavioral corpus against both simultaneously.
For every (input, output_A, output_B) triple where output_A ≠ output_B, record
the divergence with the exact input that caused it.

This is differential testing. But go further: use a coverage-guided fuzzer to
actively search for inputs that maximize divergence. Not random mutation — guided
mutation toward the parts of the input space where the corpus found divergences.
The fuzzer finds the *minimal* input that causes the *maximal* divergence.

The output is not "here are some differences." It is a **witness set**: the
smallest set of inputs that proves the two implementations are not equivalent, with
the exact nature of the non-equivalence annotated for each witness.

**For database migration proof.** Twin A speaks Postgres wire against the source
schema. Twin B speaks Postgres wire against the target schema with the migration
applied. Run the canonical query corpus against both. Find the queries that return
different results. Those queries are the migration regression surface — not
inferred from the schema diff but proved from actual query execution.

**For API version equivalence.** Twin A runs against `stripe-v2023.yaml`. Twin B
runs against `stripe-v2024.yaml`. The fuzzer generates requests across the full
parameter space and finds inputs where the two versions behave differently. The
witness set is the minimal set of requests that proves v2024 is not a backward-
compatible superset of v2023 — before any client code is run.

**The SAT/SMT extension.** For schemas with declared constraints (CHECK
constraints in DDL, pattern constraints in OpenAPI schemas), the twin knows the
constraint structure. Encode the constraints as a SAT formula and use a solver to
find inputs that are valid under constraint set A but invalid under constraint set
B. These are the algebraically derived witnesses — the fuzzer finds them
empirically; the solver finds them by proof. The two approaches complement each
other: the fuzzer finds common divergences fast; the solver finds rare divergences
with certainty.

**For agent swarms.** This is the ideal swarm task. Each agent in the swarm takes
a different region of the input space and runs its corpus against both twins. The
swarm coordinator collects divergences and dispatches the most interesting regions
(highest divergence density) to more agents. The witness set emerges from the
collective exploration. An agent swarm of 50 codex instances running this over a
weekend finds more divergences than a manual test suite built over months.

---

## 4. The mainframe regulatory sandbox — twin an entire core banking system for stress testing

**The idea.** A large bank runs core banking on IBM mainframes. The data lives in
VSAM datasets and IMS hierarchical databases. The business logic lives in COBOL
batch jobs. The transaction layer is CICS. Regulators require periodic stress
tests: "model a 300bp rate shock to your loan portfolio and demonstrate capital
adequacy." Running this on the real mainframe requires months of setup, mainframe
compute time, and careful coordination to avoid touching production data.

With twinning, the full interface surface is reconstructable from schema artifacts
that already exist:

```
VSAM datasets        → VSAM twins (reconstructed from COBOL copybooks)
IMS databases        → IMS twins (reconstructed from DBDs)
CICS transactions    → CICS twin (reconstructed from top-50 EXEC CICS commands)
Flat file outputs    → flat file twin (reconstructed from output copybooks)
```

Load the stress test scenario data into the twin fleet. Compile the COBOL batch
jobs with GnuCOBOL and point their file I/O at the VSAM twins. Execute the COBOL
programs. Collect the output flat files from the flat file twin. The stress test
result is produced in hours, on commodity hardware, in a completely isolated
environment.

**The attestation chain.** Every twin produces signed reports and content-
addressed snapshots. Every COBOL batch job's input state and output state is
captured. The entire execution is reproducible from the attestation chain alone:
restore the input snapshots, re-run the batch jobs, verify the output snapshots
match. The regulator can verify the computation independently.

**The dual-run validator.** Run the batch jobs against the twin fleet AND against
the real mainframe (in a non-production copy). Compare the output flat files byte
by byte. Any divergence is a twin fidelity gap — file it, fix it. When the twin
output consistently matches the mainframe output, the twin is certified for
regulatory stress testing purposes.

**Commercial impact.** Banks pay tens of millions of dollars annually for mainframe
stress test infrastructure. The regulatory sandbox collapses this to commodity
hardware costs. The twin fleet is the infrastructure. The attestation chain is the
audit trail. The GnuCOBOL compilation is the portability layer. All of these
components exist or are buildable; only the integration has not been done.

---

## 5. The chaos organism — evolving fault discovery via genetic search over twin configurations

**The idea.** Traditional chaos engineering requires human intuition about which
failure scenarios matter. The chaos organism eliminates that requirement by
evolving failure scenarios automatically.

A genetic algorithm operates over the space of (chaos configuration ×
request ordering × timing parameters). Each "genome" is a specification of:
which twins fail, in what patterns, at what rates, in what sequence. Each
generation, spawn a twin fleet with the current generation's genomes, run the
system under test against each fleet configuration, and score the results.

The scoring function is not "how many errors were induced" — that trivially
maximizes at 100% failure rate. The scoring function measures *interesting*
failures: responses that are wrong but not obviously wrong (200 OK with corrupted
data), failures that propagate incorrectly (service A fails, service B should
degrade gracefully but instead crashes), failures that expose undeclared state
dependencies (service B's behavior changes based on undocumented coupling to
service A's state).

**The interesting failure detector.** A twin in observation mode records every
response. The chaos organism's fitness function compares observed responses
against the contracts declared in the specs. A response that is spec-valid but
semantically wrong (empty array when the spec says non-empty, wrong count field)
scores higher than a response that is simply an error. These are the bugs that
production monitoring misses: the system *appears* to be working but is silently
producing wrong answers.

**The minimal witness genome.** Selection pressure toward minimal genomes: the
chaos organism prefers smaller, simpler failure configurations that produce the
same interesting failures as larger ones. The evolved genome is the minimal set
of conditions that maximally stresses the system. This is the adversarial test
scenario that human intuition would never construct — too subtle, too specific,
too dependent on the interaction of multiple services failing in a precise
sequence.

**Agent swarm implementation.** Each agent in the swarm evaluates one genome per
generation. A swarm of 50 agents evaluates 50 genomes per generation. With 100
generations, the swarm has evaluated 5,000 chaos configurations in the time a
human engineer would review one. The swarm coordinator runs the selection and
mutation steps; agents run the evaluation steps. The chaos organism is a natural
agent swarm application — embarrassingly parallel across genomes, naturally
convergent toward interesting failures.

---

## 6. The contract mesh — test an entire microservices architecture without running any services

**The idea.** Every service in a microservices architecture publishes an OpenAPI
spec. Spin up one twin per service. Configure the twins to call each other
according to the declared dependency graph: service A calls service B's twin for
user data; service B's twin calls service C's twin for payment data. No real
service runs. The entire architecture is running in memory, speaking real HTTP,
enforcing real contracts.

```
load balancer → service A twin (port 8081)
                    → service B twin (port 8082)
                            → service C twin (port 8083)
                    → service D twin (port 8084)
```

**What becomes testable.** Run service A's integration test suite against the
mesh. Service A makes real HTTP calls; the calls traverse the twin mesh; responses
come back contract-valid. Service A's behavior under the full system context is
testable in CI, without staging infrastructure, without other teams' services,
without network dependencies.

**Chaos injection into the mesh.** Apply chaos to specific twins in the mesh.
Service B fails with 503 at 30% rate. What does service A do? Does it degrade
gracefully? Does it propagate the error correctly? Does it cache a stale response
and return 200 with wrong data? These failure propagation behaviors are testable
because the twin mesh enforces contracts on every response, including error
responses — a 503 from service B's twin is spec-valid; the question is whether
service A handles it correctly.

**Contract drift detection.** When service B ships a new version, its spec
changes. Update service B's twin in the mesh to the new spec. Re-run service A's
tests. Tests that now fail mean service A has a dependency on service B behavior
that the new spec does not guarantee. This is backward-compatibility regression
detection across service boundaries — without deploying either service.

**The mesh as a development environment.** A developer working on service A runs
`twinning mesh --config mesh.yaml` and gets the entire architecture running
locally in under a second. No Docker Compose with 12 containers. No shared staging
environment. No "is service C deployed?" questions. The mesh is the local
development environment for distributed systems.

---

## 7. The protocol time capsule — run 1985 COBOL against 2026 data through a reconstructed interface

**The idea.** A COBOL program written in 1985 is still the legal system of record
for certain actuarial calculations. The VSAM files it reads no longer exist — the
data has been migrated through three systems since then. The COBOL program cannot
be rewritten because nobody understands the actuarial logic well enough to
translate it correctly, and it is legally certified.

The COBOL program is compiled with GnuCOBOL. It expects to read a VSAM KSDS
(key-sequenced dataset) with the layout defined by a 1985 COBOL copybook that
still exists in the source repository. That copybook is the schema artifact.

The VSAM twin is constructed from the copybook: record lengths, field offsets,
PIC clause types, key structure, access mode. The current data — living in Postgres
— is extracted, translated through the copybook codec (converting modern types to
EBCDIC packed decimal, truncating to fixed field widths, handling null-to-blank
translation), and loaded into the VSAM twin.

The 1985 COBOL program runs. It opens the VSAM file via its standard COBOL file
I/O verbs. The twin speaks VSAM. The COBOL program reads 2026 data through a 1985
interface that has not physically existed since 1992. It produces its output — the
actuarially certified calculation, running on current data, without a single line
of the COBOL program being changed.

**The fidelity proof.** The twin produces a signed attestation of the input data,
the VSAM access sequence (every OPEN, READ, REWRITE, CLOSE recorded), and the
output records. The attestation chain proves what data the program read, in what
order, and what it wrote. This is the audit trail for a legally certified
calculation running in a reconstructed environment.

**The broader pattern.** Every legacy system with a copybook or DBD schema
artifact can be time-capsuled. The schema artifact is the key. The twin is the
lock. The legacy program is the computation that could not be replicated any other
way. The protocol time capsule is the enabling infrastructure that makes legacy
program execution possible on modern hardware without emulation, without
mainframe access, without the original operating system.

---

## 8. The living digital twin — WAL-driven in-memory replica at RAM bandwidth

**The idea.** A production Postgres database streams its write-ahead log to the
twin via logical replication. The twin maintains a complete in-memory replica of
the declared subset, updated continuously. Queries that would be expensive on
production (full table scans, large aggregations, expensive joins on a 500GB
table) are served from the twin at memory bandwidth — not disk I/O, not buffer
pool contention, not lock waits.

This is not a read replica. A read replica has the same performance
characteristics as the primary because it is also a full Postgres instance with
the same disk-based storage engine. The twin stores everything in memory with its
own in-memory operation model; it serves queries at RAM speed against a hot
working set that fits entirely in L3 cache for the declared subset.

**The performance tier.** For the right workload — hot dataset that fits in
available RAM, read-heavy access patterns, latency-sensitive queries — the living
digital twin is a fundamentally different performance tier than any disk-based
replica. Analytical queries that take 800ms on production take 2ms on the twin.
The delta is not optimization — it is the absence of disk.

**The tournament mode connection.** The twin already supports tournament-mode
overlay semantics: a shared base snapshot plus lightweight per-session overlays.
In the living digital twin, the WAL stream applies to the shared base. Session
overlays handle read-your-writes semantics for sessions that have written but
whose writes have not yet replicated. The tournament model was designed for
exactly this use: cheap session forks on top of a shared committed base.

**The staleness bound.** The twin lags production by at most the replication
latency (typically <100ms for same-datacenter). Queries that can tolerate bounded
staleness — analytics, reporting, non-transactional reads — route to the twin.
Queries that require read-after-write consistency route to production. The routing
decision is declared in the session: `SET twinning.staleness_ok = true`.

**CMD+RVL angle.** `aibuildout`'s enrichment pipeline runs expensive analytical
queries against the metadata database to identify companies matching specific
criteria. These queries run against a living digital twin instead of production —
same data (within replication lag), RAM-speed query execution, no impact on
production query latency or lock contention.

---

## 9. The sovereign inference twin — OpenAI-wire-compatible router to optimal local models

**The idea.** The OpenAI API has a published OpenAPI spec. The twin speaks it:
`POST /v1/chat/completions`, `GET /v1/models`, `POST /v1/embeddings`. Any client
built against the OpenAI API — LangChain, LlamaIndex, aibuildout, any custom
agent — works against the twin without code changes.

But the twin does not return schema-valid placeholder responses. It routes each
request to a local model backend (llama.cpp, ollama, MLX Whisper, vLLM) and
returns the model's actual output in the OpenAI response format.

**The routing layer.** The twin inspects each request and routes to the optimal
local model based on a declared policy spec:

```yaml
routes:
  - match: { model: "gpt-4*", temperature: { lt: 0.3 } }
    backend: deepseek-r1:8b     # deterministic reasoning tasks
  - match: { model: "gpt-4*", tools: { present: true } }
    backend: qwen2.5-coder:7b   # tool use / code generation
  - match: { model: "gpt-3.5*" }
    backend: llama3.2:3b        # fast, cheap completions
  - match: { context_length: { gt: 32000 } }
    backend: gemma3:27b         # long context
  - fallback:
    backend: llama3.1:8b
```

The routing policy is a schema artifact. The twin enforces it. The client never
sees the routing — it makes an OpenAI API call and gets an OpenAI API response.

**The observability layer.** Because every request passes through the twin, every
inference call is observable: latency, token counts, model selected, cost
(estimated against local compute), cache hit rate. The twin emits this as
structured metrics alongside the response. The aibuildout pipeline gets a complete
inference audit trail — what was asked, which model answered, how long it took —
for every LLM call in the enrichment pipeline, without instrumenting the client
code.

**The cost arbitrage.** The twin tracks which requests actually required the
capability of a large model and which were satisfied by a small model at lower
cost. Over time, this is a dataset: (request characteristics, model selected,
response quality assessment). Train a routing model on this dataset. The routing
model improves its own training data — the twin learns to route more efficiently
by observing the outcomes of its own routing decisions.

**The contract enforcement angle.** The OpenAI spec declares which parameters are
valid. The twin rejects invalid requests before they reach any model backend —
malformed tool definitions, out-of-range temperature values, contradictory stream
settings. This is the spec-enforcement primitive applied to LLM API clients:
invalid requests fail fast at the protocol layer, not deep in the model's
tokenization pipeline.

---

## 10. SQL query tournament — schema-grounded generation with twin-validated execution

**The idea.** A data agent receives an ad hoc SQL query request. Instead of
returning a single LLM-generated SQL statement and hoping it works:

1. Pull the schema DDL from the metadata catalog.
2. Bootstrap a Postgres twin from that DDL in milliseconds — no data needed.
   The schema alone is enough for the twin to enforce structural correctness.
3. Spawn N agents in parallel, each generating a SQL candidate for the same user
   intent using a different approach: different JOIN strategies, CTE vs subquery,
   different filter orderings, different aggregation shapes.
4. Execute all candidates against the twin simultaneously.
5. Candidates that error are eliminated. Survivors are ranked and the best one
   is returned to the user, validated as syntactically and schematically correct
   against the real schema.

**Why the twin changes the economics.** Running 20 SQL candidates against a twin
costs nothing and takes milliseconds. Running one bad query against production can
cause a full table scan, lock rows, or return garbage silently. Without the twin,
SQL generation is a best guess. With the twin it is a tournament: only queries
that are actually correct against the actual schema advance to the user.

**EXPLAIN scoring.** Run `EXPLAIN` on each surviving candidate. The query plan is
available even without real data. Candidates with sequential scans on large tables
rank lower than candidates using declared indexes. Candidates with nested loops on
unindexed foreign keys rank lower than candidates that use hash joins. The twin
produces the query plan; the agent scores it. The user gets the syntactically
correct, schematically sound, and plan-efficient query.

**Incremental construction.** Build the query one clause at a time — FROM, then
JOIN, then WHERE, then GROUP BY — validating against the twin after each addition.
Catch schema errors before the query is complete. If a JOIN references a column
that doesn't exist, the twin errors at the JOIN step, not after the full query is
assembled. The agent backtracks and tries a different JOIN shape. This is
structured search over query space with the twin as the validity oracle at each
step.

**Safety gate before production.** Before any ad hoc query touches production, it
runs against the twin. The twin reports: does it reference tables annotated as
sensitive in the catalog, does it lack a WHERE clause on a large table, does it
produce a full scan without an index. These are schema-layer detectable properties
that don't require real data. The gate surfaces dangerous queries before they run,
not after.

**CMD+RVL angle.** A Cairn data agent generating SQL against the metadata catalog
schema is the primary use case. The metadata catalog already knows the schema —
it is the schema. Bootstrapping a twin from it takes the same DDL export the
catalog already produces. The agent gets a free execution sandbox for every SQL
generation attempt, grounded in the exact schema the user will run against.
aibuildout enrichment queries against the subscriber database are the second use
case: generate, tournament, validate, ship.

---

## 11. SQL codebase audit — find schema drift and performance regressions across an entire repo

**The idea.** Crawl a codebase — local or GitHub — for every SQL statement it
contains: `.sql` files, migration files, embedded SQL strings in Python/Ruby/
JS/Go source, ORM raw query calls. For each statement, extract exactly which
tables and columns it references using `sqlparser` (already a Cargo dependency).
Pull DDL for only those tables from the metadata catalog — targeted, per-query
extraction, not the whole schema. Bootstrap a Postgres twin from that minimal
DDL. Execute the statement. Classify the result.

**Schema drift findings.**

- `column "foo" does not exist` → column was renamed or dropped; this query is
  silently broken or will break at the next deployment
- `relation "bar" does not exist` → table dropped or renamed
- `operator does not exist: text = integer` → column type changed upstream

These are not hypothetical. Every long-lived codebase accumulates SQL statements
that were correct when written and have silently drifted out of sync with the
schema. The audit finds them exhaustively, with exact file path and line number,
before they surface as production errors.

**Performance findings.**

A query that executes without error against the twin still exposes its query plan
via `EXPLAIN`. Without any real data, the plan reveals:

- Sequential scans on tables where the DDL declares an index
- Nested loops on unindexed foreign keys
- Missing index usage on commonly filtered columns
- Query shapes that are structurally expensive regardless of data volume

Severity scores each finding: a seq scan on a table declared with a primary key
is critical; a seq scan on a small reference table is informational.

**Rewrite pass.** For each performance finding, hand the query to the SQL
tournament (bd-gwlk): generate N alternative formulations against the same twin,
score by EXPLAIN plan quality, emit a side-by-side plan comparison. The original
query and the optimized candidate both appear in the report, with their respective
plan costs annotated.

**The pre-migration safety check — the killer use case.** Before applying a
schema migration, run every SQL statement in the codebase against the
*post-migration* twin. Every failure is a query that will break in production
after the migration ships. Every plan regression is a query that will slow down.
The full impact surface is known before a single byte of schema change lands in
production.

```bash
# Before applying migration 0047_rename_subscriber_columns.sql:
twinning postgres --schema schema_post_migration.sql --port 5433
sql-audit crawl ./src --twin localhost:5433 --report audit-pre-deploy.json
# Every failure in the report = production breakage after migration
```

**CI gate variant.** On every PR that touches SQL or migrations, extract only
the SQL that changed or that references tables touched by changed DDL. Run through
the audit pipeline. Fail the PR if schema drift or severe plan regressions are
found. Drift and regressions are caught at review time, not at deployment time.

**CMD+RVL angle.** `cmdrvl-cli`, `aibuildout`, and any repo with SQL against the
metadata catalog schema gets audited automatically on every metadata migration.
The audit runs in CI and blocks the migration PR if any existing SQL in any
downstream repo would break.

`sqlparser` is already at version 0.53 in `Cargo.toml`. The parsing layer is not
a new dependency — it exists. The SQL extraction, table reference analysis, and
DDL-targeted bootstrapping are the new pieces.

---

## 12. Production query log replay — actual execution history replayed against the twin

**The idea.** Pull the production query log from `pg_stat_statements`:

```sql
SELECT query, calls, mean_exec_time, total_exec_time, rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC;
```

This gives every distinct query shape that actually ran, how many times, and
its total cost — already priority-ranked by production impact.
`pg_stat_statements` normalizes parameters to `$1`, `$2` placeholders, which
is exactly right for twin replay: you are testing query structure against the
schema, not testing values against data. Bootstrap a Postgres twin from the
current schema DDL. Replay the log in cost order.

**Why this is better than the codebase audit for finding what matters.**

The codebase audit (bd-2ndf) finds queries that *exist in source files*. The
log replay finds queries that *actually ran* — ORM-generated SQL, ad-hoc queries
from the analytics team, dynamic string-built queries, anything that was executed
but never appears in source code. Every long-lived production database has a large
population of these. The codebase audit and the log replay are complementary:
one tests what is written; the other tests what is executed. Together they give
complete coverage.

**Pre-migration safety — the sharpest use.** Before shipping a schema migration,
replay the production query log against the *post-migration twin*. Every error
is a query that will break in production, ranked by call count — the
highest-traffic broken queries appear first. Every plan change is a performance
regression or improvement. The full production query distribution, validated
against the new schema, before any user sees the migration.

**Optimization targeting.** The top-N queries by `total_exec_time` are the ones
consuming the most database time. Hand each one to the SQL tournament (bd-gwlk):
generate rewrite candidates, score by EXPLAIN plan quality against the same twin,
emit a side-by-side plan comparison. The optimization effort is directed by actual
production cost, not by guesses about what might be slow.

**Plan regression detection.** Compare the EXPLAIN output from the twin against
the `mean_exec_time` from the log. If the twin's plan looks expensive but the real
database was fast, a relevant index exists that the twin's DDL does not declare —
that is a schema artifact gap. If both the twin's plan and the real database are
slow, that is a genuine optimization target. If the twin's plan looks fast but the
real database is slow, something outside the query is the bottleneck (lock
contention, connection overhead, buffer eviction) — the twin surfaces this by
exclusion.

**CMD+RVL angle.** Run against the metadata database before every migration in
CI. `aibuildout`'s enrichment queries are the highest-cost targets for the
optimization pass — they run frequently against a growing dataset and are the
most likely to have plan regressions as the schema evolves.

---

## 13. Incremental spec synthesis — building new APIs one twin-verified increment at a time

**The idea inverts the premise of every other entry in this document.** Every
previous idea uses the twin to validate something that already exists. This uses
the twin as the design oracle while the API is being *created*.

An AI agent does domain research — reads documentation, studies similar APIs,
understands the problem space. It proposes the next increment to the OpenAPI
spec: a new endpoint, a new field on a response schema, a new error code, a new
security scheme. Before proposing the next increment, it twins the *current*
spec and validates that the addition is coherent and usable. The twin is the
grounding mechanism at each step. The spec grows one verified increment at a
time. You cannot propose an addition that produces an incoherent API surface
because the twin tells you immediately.

**What the twin reveals at each increment that reading YAML cannot.**

*Structural coherence.* Does the new schema reference types that are already
declared, or did this increment introduce an orphan? Does the authentication
declared on this endpoint match the scheme on adjacent endpoints, or did you
accidentally create a consistency gap? Does the new path parameter conflict with
an existing route pattern?

**The stranger test.** After each addition, spawn a fresh agent that has seen
only the current spec — not the design session, not the rationale, only the
YAML. Ask it to exercise the new endpoint. If the agent cannot construct a valid
request from the spec alone, the endpoint is under-determined. The twin is the
execution oracle: does the request work? The naive agent is the usability oracle:
can someone figure out how to make the request from the spec alone? Both run at
every increment. If either fails, the increment is revised before the next is
proposed.

**The backward compatibility gate.** Every proposed addition is replayed against
simulated clients written against earlier spec versions. If adding a required
response field breaks a v1 client, the gate rejects the addition. If making a
parameter required breaks clients that omit it, the gate rejects it. The spec
can only grow in directions that preserve compatibility with every previously
validated client. The spec's version history is a chain of verified increments,
each proved backward-compatible with all predecessors.

**Multi-stakeholder negotiation.** Different agents represent different consumer
perspectives: mobile client (bandwidth-sensitive, prefers flat response shapes),
server-to-server integration (cares about idempotency, retry semantics, bulk
endpoints), analytics consumer (wants cursor-based pagination, filter parameters,
response field projection). Each agent proposes additions and objections. Every
proposal is twinned immediately. The spec that emerges has been validated from
all declared consumer angles, not just the producer's perspective.

**The key distinction from spec-first development (bd-32e5).** Spec-first says:
write the full spec, then both sides develop against it. Incremental synthesis
says: the spec is an artifact that grows one verified increment at a time, with
a research loop proposing each increment and the twin grounding each proposal in
executable reality. The difference is the same as the difference between writing
a proof and discovering a proof — one is transcription, the other is search.

**CMD+RVL angle.** Designing the metadata v2 API, the Cairn data agent API
surface, any new internal service. Instead of writing a spec and discovering
incoherence during implementation, each design decision is verified before the
next decision is made. The twin makes design iteration cheap enough that you can
afford to explore.

---

## 14. Incremental data model synthesis — emerge a new Postgres schema one verified DDL step at a time

**The Postgres-wire companion to incremental spec synthesis (section 13).** The
schema DDL is the artifact under construction. The Postgres twin is the grounding
oracle at each increment. An AI agent does domain research — understands the
entities, relationships, cardinalities, and access patterns of the problem space
— and proposes the next DDL increment: a new table, a new column, a constraint,
an index, a foreign key. Before proposing the next increment, it applies the DDL
to the twin and validates it.

**The empty INSERT test.** After each increment, attempt to INSERT a single row
into each table using only the required fields. If the INSERT fails, a constraint
is unsatisfiable: a NOT NULL column has no DEFAULT and no obvious population path,
a circular FK makes it impossible to populate both tables simultaneously, or a
CHECK constraint is unsatisfiable given the declared types. This surfaces broken
constraint designs before anything is built on top of them — not during the first
integration test six weeks later.

**The query workload oracle.** Declare the access patterns this model must serve
alongside the schema design: these are the N queries this data model is being
built to answer. After every DDL increment, run EXPLAIN on all declared queries
against the twin. If adding a table causes a join to produce a seq scan because
the FK index was not declared, the twin says so now, not when the table has ten
million rows. If an increment causes a plan regression on a query that previously
had a good plan, the increment is revised before it is locked in. The data model
converges toward one that is provably capable of serving its declared workload.

**Constraint satisfiability across the whole schema.** Circular foreign keys,
overlapping unique constraints, CHECK constraints that conflict with each other.
The twin attempts to satisfy all declared constraints simultaneously. Constraint
sets that are individually valid but collectively impossible fail at the twin,
not in a production migration.

**The migration path falls out for free.** Each increment is a verified DDL
delta — it was applied to the twin and proved correct before the next increment
was proposed. The full migration from schema v0 to vN is the ordered sequence of
those verified deltas. Migrations are not written after the fact and hoped to be
correct; they are the design steps, each one already twin-proved at the moment
it was accepted.

**The normalization lens.** After each increment the agent analyzes the schema
for normalization issues: repeating groups in a single table, partial dependencies
on composite keys, transitive dependencies between non-key columns. Not to mandate
normal form — sometimes denormalization is the right call — but to make the
trade-off explicit at design time, when it costs nothing to fix, rather than after
the ORM is built around the wrong shape.

**What makes this distinct from migration-proof work.** The migration-proof
direction (PLAN_TWINNING_FUTURES.md) proves that an existing migration is correct
— it validates a transition between two known schemas. Incremental data model
synthesis is about discovering the schema itself, with the twin as the feedback
mechanism at each design decision. The artifact being produced is the schema, not
the proof.

**CMD+RVL angle.** Designing new tables for the metadata database, the Cairn data
model, any new data product. Instead of sketching a schema in a whiteboard session
and discovering constraint problems when the first migration runs, each design
decision is verified before the next is made. The twin makes data model iteration
cheap enough to explore alternatives: propose a normalization, verify it, propose
a denormalization, verify it, choose based on query plan evidence rather than
intuition.

---

## 15. Legacy schema modernization — discover the ideal schema from application usage, emerge it, prove equivalence, migrate

**The core insight.** The applications are the specification for the legacy
database. They encode, through decades of usage patterns, what the database is
actually supposed to do — even when the schema itself is a mess of accumulated
decisions, bad names, and organic growth. You do not have to guess what the ideal
schema should be. The answer is already written in the query logs and the
application code. You have to read it.

This synthesizes four earlier ideas — query log replay (12), codebase audit (11),
incremental data model synthesis (14), and the adversarial equivalence prover (3)
— into a complete legacy database modernization pipeline.

---

**Stage 1: Schema archaeology.**

Bootstrap a Postgres twin from the legacy DDL. Run the query log replay and the
codebase audit against it simultaneously. The result is a complete usage map:
every table, every column, every join, every filter, ranked by frequency and
cost. This is the legacy schema as it is *actually used*, stripped of everything
that was added and never touched. Columns that appear in zero queries in six
months of logs are candidates for removal. Tables referenced only by one
application are candidates for ownership consolidation.

**Stage 2: Semantic inference.**

An agent reads the usage map and infers what the legacy schema *means*,
independent of what it *says*. The naming is usually terrible — `ACCT_TYP_CD`,
`CUST_REL_TBL`, numeric magic codes — but usage reveals semantics:

- A column where the application always filters `IN ('01', '02', '03')` is an
  undeclared enum. The values are the enum members. Name them.
- A column that always appears in JOINs to one specific other table is an
  undeclared foreign key. Declare it.
- Columns that are always NULL for certain row types reveal single-table
  inheritance: one table is doing the job of two. Split it.
- Columns that are always read together are a candidate entity that was never
  given its own table.
- A column that is written exactly once per row and never updated is an immutable
  fact. It belongs in a different modeling pattern than mutable state.

The agent produces a **semantic model**: the real entities, the real
relationships, the real constraints, expressed in modern terms — independent of
the legacy naming scheme.

**Stage 3: Ideal schema emergence.**

Feed the semantic model into the incremental data model synthesis loop (section
14). The agent proposes the clean schema one verified increment at a time. The
query workload oracle uses the access patterns from stage 1 — the schema that
emerges is not designed from first principles but from actual evidence. Every
increment is twin-validated: empty INSERT test, EXPLAIN on all declared queries,
constraint satisfiability. The schema that emerges is provably capable of serving
every access pattern the legacy system actually served.

**Stage 4: Equivalence proof.**

Translate all queries from the usage map to work against the new schema. Run the
adversarial equivalence prover (section 3): legacy twin on the left, new schema
twin on the right, full query corpus against both. Any query where the results
differ is either a translation error or a genuine semantic difference that
requires human review. The witness set is exact — not "something might differ"
but "this specific query with these specific parameters returns different results,
here is the minimal input that proves it." Prove equivalence before migrating a
single byte of data.

**Stage 5: Migration generation.**

The sequence of verified DDL deltas from stage 3 is the schema migration script —
already proved correct because each delta was twin-validated at the moment it was
accepted. The data migration ETL is verified by replaying the query log against
the post-migration new-schema twin. Any query that succeeded before the migration
but fails or returns different results after is caught before production.

---

The full pipeline closes the loop: legacy schema → usage archaeology → semantic
inference → ideal schema emergence → equivalence proof → verified migration. The
twin is the oracle at every stage. No stage requires running any of this against
the production database until the final migration, which arrives having already
been proved correct.

**CMD+RVL angle.** Any legacy data source being onboarded into the metadata
catalog. This is the onboarding pipeline for data products with messy inherited
schemas — which is most of them.

---

## 16. Snowflake Marketplace vendor replacement optimizer — twin every listing, prove the optimal replacement bundle

**The new primitive.** A Snowflake wire protocol twin — a new protocol family
alongside Postgres, REST, MCP, VSAM, and IMS. Snowflake has a proprietary SQL
dialect but a REST API and JDBC surface. The twin speaks that surface, pulls
schema and sample data from any Snowflake instance or Marketplace listing, and
presents it as a locally queryable dataset. Same pattern as every other twin
type: schema artifact in, executable interface out.

**Boundary correction: the twin has schema, not data.**

The twin can tell you whether a Marketplace listing *structurally* satisfies your
requirements. It cannot tell you whether the listing actually contains the
securities or entities you care about — that requires real data. Coverage and
value-level quality are outside the twin's scope. This shapes how the pipeline
is structured: the twin does structural qualification first, cheaply eliminating
listings that cannot possibly work; real data closes the evaluation for survivors.

**Phase 1: Structural qualification via twin.**

Twin the schema of every Marketplace listing and the current vendor feed. For
each listing, the twin answers:

- *Field compatibility* — does this listing have the fields the vendor feed
  has, with compatible types? `SECURITY_PRICE_CLOSE`, `px_close`, and
  `close_price_usd` are the same thing; semantic field mapping from names,
  descriptions, and type signatures without touching any values.
- *Query structural validity* — can the canonical analytical queries execute
  against this listing's schema without modification? If not, the listing is
  eliminated without spending a single Snowflake credit on it.
- *EXPLAIN analysis* — given the listing's declared indexes and table structure,
  will the required queries be efficient? A listing whose schema forces full
  scans on the primary analytical pattern is a poor candidate regardless of
  data quality.

The twin phase narrows hundreds of listings to a small survivor set — cheaply,
instantly, without any data access.

**Phase 2: Sample pull.**

Request free sample data from structural survivors only — most Marketplace
providers offer this. This is the only phase that touches real data, and it
touches only the listings that passed structural qualification.

**Phase 3: Spine evaluation.**

This is not custom evaluation logic. It is the existing Crucible spine workflow
— `benchmark`, `verify`, `assess`, `pack` — with Marketplace data as input.
The same tools that prove migration quality today, applied to vendor replacement.

- **`benchmark`** — the vendor feed sample is the gold set; the Marketplace
  listing sample is the candidate. `benchmark` scores exactly how much of the
  gold set the listing covers and where it diverges. This is the coverage tool.
  No custom coverage logic needed.
- **`verify`** — does the Marketplace data satisfy declared constraints: value
  ranges, referential integrity, freshness rules, data quality assertions?
- **`assess`** — policy decision given `benchmark` score and `verify` results:
  viable replacement or not, with quantified scores attached.
- **`pack`** — sealed evidence bundle with all `benchmark`, `verify`, and
  `assess` artifacts. This is the board-level decision artifact, signed and
  reproducible.

**Phase 4: Bundle optimization.**

Model the optimal combination of `assess`-scored listings against the vendor
feed cost. Output is a proof with a price tag.

**Phase 5: Continuous discovery.**

Auto-twin new listings for structural qualification immediately on publication.
Sample pull and full spine evaluation only if structural qualification passes.

**CMD+RVL angle.** `cmdrvl-curves` is already in financial data — SOFR, Treasury
curves, rate data. CMD+RVL's clients pay for vendor data feeds. This tool runs
against their Snowflake instance, catalogs every relevant Marketplace listing,
and produces a vendor replacement proof with cost modeling attached. The output
is a board-level decision artifact: here is what you are paying, here is what
you could pay, here is the quality delta, here are the gaps, signed and
reproducible. That is a product CMD+RVL can sell.

---

## The unifying pattern

Every idea here follows the same structure:

1. **Identify a schema artifact** that describes the interface (DDL, OpenAPI spec,
   COBOL copybook, DBD, JSON Schema for a model API).
2. **Reconstruct the interface** using the twin kernel — speak the protocol, enforce
   the constraints, maintain behavioral state.
3. **Add a dimension** that was previously impossible: time travel, cross-protocol
   bridging, adversarial equivalence search, evolutionary fault discovery, WAL-
   driven live replication, or intelligent request routing.
4. **Emit artifacts** — attestations, proofs, metrics, witness sets — that have
   value independent of the system under test.

The twin is not a testing tool. It is an interface reconstruction engine. The
schema artifact is the source code. The twin is the compiled output. What you do
with a running interface is limited only by what operations you can express over
the protocol — and the ambition of the agent swarms building the extensions.
