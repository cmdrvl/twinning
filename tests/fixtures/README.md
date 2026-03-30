# test fixtures

This tree is the checked-in fixture surface for the first v0 compatibility and
differential suites.

Subtrees:

- `canaries/` — per-canary fixture directories matching `canaries/manifest.v0.json`
- `differential/` — read and write parity corpora for real-Postgres comparison

The current files are active contract inputs:

- `canaries/` pins client-facing canary inputs, extracted SQL/IR fixtures, and
  runtime-backed harness assets
- `differential/` pins read/write corpus schemas and expectations used by the
  twin-side differential harnesses today, with room for future live-Postgres
  comparisons
