# Goal

First read ALL of AGENTS.md and README.md super carefully and
understand ALL of both, then read docs/PLAN_TWINNING.md cover to cover -
that file is the source of truth for what twinning is supposed to become.
Then use your code investigation agent mode to fully understand the
current state of the Rust bootstrap crate, the report/snapshot schemas,
the `run_once` pgwire live shell, and the Beads execution graph for this
repo. Hold the central design contract in your head as you work: twinning
is a PROTOCOL-FAITHFUL INTERFACE TWIN for a DECLARED SUBSET - the client
speaks real Postgres (pgwire), the twin answers real Postgres against
content-addressed committed state, and out-of-subset queries are refused
cleanly as ordinary protocol errors. The twin is not a cache, not a mock,
and not faster-than-production by design - it is subset-honest, frozen,
and reproducible. Every change you make should preserve that contract.

Here is the use case to keep in your head as you build, end to end.

An extractor - say, one of our CMD+RVL pipeline extractors that
queries the metadata catalog to harvest evidence for a specific
outcome - needs to be iterated on. Maybe debugged, maybe extended,
maybe rewritten. The catalog is Postgres-backed and lives in
production. Pointing the extractor at production for every dev cycle
is slow, risky, and leaks more raw catalog content into the
developer's environment than belongs there. Spinning up a full clone
of production for every iteration costs minutes per turn and is
wasteful when the extractor only touches three tables for one
outcome.

The operator starts at the metadata catalog. They search for the
tables relevant to the outcome - for example, the three tables that
serve the procurement outcome - and pick them out. The catalog
already knows their schemas, their primary keys, their foreign-key
relationships, their source deployment, and the outcome tags they
serve. The operator exports the catalog metadata for those tables
and produces twinning's expected input format from it - by hand
today, by a small shim or skill tomorrow, by a `cmdrvl metadata
twinning-export` subcommand someday. Twinning consumes that input
format. The catalog stays the declaration layer; twinning stays the
materialization layer. They compose without coupling.

Twinning, given the declared subset and the live source connection,
walks the named tables, captures the committed rows at a moment in
time, and seals the result into a content-addressed bootstrap.twin
snapshot. The snapshot embeds the declared schema, the captured rows,
and a parent reference back to the catalog selection (catalog table
keys plus catalog version plus source deployment fingerprint). The
snapshot is a few megabytes, sha256-named, and reproducible: same
declared subset plus same source state plus same twinning version
equals byte-identical snapshot, every time.

The operator stands up a `run_once` pgwire listener bound to
localhost. The extractor connects to the twin exactly the way it
would connect to production Postgres. It runs SELECT, JOIN, WHERE,
even information_schema introspection. The twin advertises the same
schema the catalog declared and answers from the frozen rows.
For the declared subset, verified by the parity ledger against the
source.

When the extractor accidentally references a table outside the
declared subset - say `audit_log` when only the procurement tables
are in scope - the twin returns a structured pgwire error: ERROR
42P01, relation not in declared subset, with the declared subset
listed in the error detail. The extractor's existing "relation not
found" handling fires normally. The twin is not lying about what it
has; it is refusing cleanly with a protocol-faithful response that
maps to a real Postgres error code.

The developer iterates. Each iteration is seconds, not minutes.
Every test run is reproducible - same snapshot bytes plus same
extractor bytes equal same output bytes. When the extractor
graduates, the same bootstrap snapshot can be sealed into a pack
alongside the extractor's evidence outputs, and any operator on any
machine can re-run the entire iteration against the same frozen
subset and verify the work, right down to the schema version that
produced it.

Later, when the catalog backend itself migrates (one Postgres
deployment to another, or to a Postgres-compatible backend), the
twin-pair migration proof kicks in. Bind a twin to each side using
the same subset declaration. Run the same queries. Prove that for
the declared subset, both backends answer identically at the wire.
The migration is justified by a parity receipt against a
catalog-declared truth, not by hope.

The composition arc: catalog declares the subset, twinning
materializes the frozen substrate, the extractor iterates against
the twin, pack seals the run, verify gates declared constraints over
the snapshot, assess decides whether the result clears policy. Each
layer narrow and honest. Each layer composable. Twinning is the
materialization layer in that chain - protocol-faithful, subset-
honest, frozen, reproducible.

Catalog is the declaration. Twinning is the materialization. Pack
is the seal. That is the story twinning tells inside this practice.
Every feature you build, every test you write, every bead you close
should make that story easier to tell and harder to break.

Then proceed meticulously through the open beads. Use the `bv` tool
mentioned in AGENTS.md to prioritize what to work on next; pick the
next bead you can usefully work on and get started. Work systematically,
close beads as you finish them, and capture any new work you discover as
new beads using `br` (see $beads-workflow). Look for beads that are
clearly "stalled out" - marked as in progress with no recent work - and
mark them open again before picking them up. Don't get stuck in planning
purgatory; when the next move is clear, start. When it isn't, surface
the design question explicitly in the bead and pick a different one.

*CRITICAL*: All cargo builds, all cargo tests, and any other
CPU-intensive operations MUST be offloaded using $rch - see AGENTS.md
for the exact mechanics. Do not run cargo build or cargo test directly
on this machine.

As you work the beads, the priority order for "what counts as twinning
being complete" is:

  1. Bootstrap mode is rock solid for the proven Postgres subset
     (schema validation, deterministic bootstrap snapshot, restore from
     snapshot, report/snapshot schema parity).
  2. The live `run_once` pgwire shell is reliable end-to-end: bind on
     --host/--port, run one child command against the twin, shut down
     cleanly, and freeze committed-state artifacts.
  3. Parity ledger: every client query that runs against the twin can
     be verified - same query against production should produce
     byte-identical responses for the declared subset, and out-of-subset
     queries return a protocol-faithful error citing the declared
     subset.
  4. Twin-pair migration proof (the deferred direction in the README):
     a twin can be bound to two backing implementations and prove they
     answer identically, giving operators a deterministic
     interface-equivalence check across a migration cutover.

Make sure the receipt artifacts (bootstrap report, deterministic
bootstrap snapshot, run report, refusal envelopes) stay
content-addressed, schema-versioned, and reproducible. If you change a
schema, bump the version and update the fixtures.

If you run out of useful beads to work on, you can also use the various
skills with names beginning with "testing-" to improve our testing
posture - $testing-real-service-e2e-no-mocks (this matters a lot for
twinning since the testing oracle is pgwire-faithful behavior, not
mock-shaped values), $testing-conformance-harnesses (perfect fit for
twinning's parity contract: real Postgres vs. twin should be
indistinguishable for the declared subset), $testing-golden-artifacts
(for the bootstrap snapshot determinism), $testing-metamorphic (for
verifying that the same query against the same subset always returns
identical responses), $testing-fuzzing (for the pgwire frame parser
and the schema validator). For twinning specifically, conformance
harnesses are the highest-leverage testing investment - every
conformance test that passes against both production Postgres and the
twin is a permanent receipt that protocol faithfulness is preserved.

When the open and stalled beads are worked through and the review
rounds are starting to converge and appear saturated (i.e., not many
new bugs being found relative to the effort and token usage), then
start applying these skills to find the next layer of work:

  - especially important for twinning where mocks would directly
    violate the protocol-faithfulness contract)
  - $deadlock-finder-and-fixer (pgwire is concurrent; the `run_once`
    shell binds a port and accepts a connection, so deadlocks in the
    shutdown path or the child-process wait are real risks)
  - $reality-check-for-project (compare the implemented state against
    PLAN_TWINNING.md - surface every gap)
  - $modes-of-reasoning-project-analysis (does the design hold up
    under multiple analytical lenses?)
  - $profiling-software-performance (the twin should be FAST for the
    declared subset; bootstrap snapshot freeze and restore times
    matter; pgwire response latency matters)
  - $security-audit-for-saas (only to the extent applicable - pgwire
    bind, child-process spawn, snapshot path traversal, manifest
    forgery surfaces)

Use the bugs and gaps these skills surface to create new beads via
`br` and work them through the same systematic loop.

You can also apply $extreme-software-optimization to push the existing code
to the next level of sophistication and performance - twinning needs
to be compelling and accretive for clients evaluating it against
production databases for extractor iteration loops and migration
proofs, which means the twin needs world-class responsiveness and
deterministic resource utilization. Take full advantage of the
underlying machine.

I also want you to use $idea-wizard through all phases (end to end)
to come up with additional features and functionality in furtherance
of the four-step completion order above - especially anything that
strengthens the protocol-faithfulness contract, sharpens the
subset-honesty refusal envelope, or makes the twin-pair migration
proof use case more compelling.

End state: when twinning hits step 4 (twin-pair migration proof) with
a clean conformance harness against real Postgres for the declared
subset, plus a freeze/restore round trip that is byte-identical across
runs, the repo can graduate from Phase 0 to a public release. Until
then, every commit should move at least one of the four completion
criteria forward.
