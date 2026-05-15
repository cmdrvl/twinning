# differential corpora

These modules are the checked-in differential corpus harness for the
real-Postgres parity lane named in the plan.

Current state:

- read and write corpus entry tests both run by default against the landed
  twin-side normalization and kernel surfaces; the read corpus now pins `IN`,
  `BETWEEN`, and basic `COUNT ... GROUP BY` behavior plus adjacent refusals
- the parity ledger test runs the twinning binary in `run_once` mode with a
  child pgwire client and records declared reference-vs-twin observations; by
  default it uses pinned Postgres observations, and with
  `TWINNING_DIFF_POSTGRES_URL` it resets a disposable Postgres `public` schema
  through `psql` and captures live reference observations
- the migration proof test restores two committed-state snapshots over the same
  catalog declaration, runs the same Postgres-kernel reads against both, and
  emits a `twinning.twin-pair-proof.v0` receipt for point lookup,
  filtered-scan, aggregate-count, divergence, SQLSTATE-refusal cases, and raw
  target-side `verify`/`benchmark`/`assess` evidence identities; its coverage
  matrix also pins explicit SKIP accounting for join, introspection, and
  historical workload families
- the shared runner still carries `TWINNING_DIFF_POSTGRES_URL` so live-target
  comparison paths can reuse the same checked-in corpora
- fixture directories are checked in and exercised so layout drift shows up
  early
- `tests/manifest_coverage_gate.rs` checks the canary manifest against
  `tests/fixtures/canaries/manifest_coverage_matrix.json`, so a manifest token
  cannot be added without executable evidence in the same test run

The purpose of this layout is to keep the checked-in corpus contract active now
instead of letting the differential suite become an afterthought.
