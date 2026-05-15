# test fixtures

This tree is the checked-in fixture surface for the first v0 compatibility and
differential suites.

Subtrees:

- `canaries/` — per-canary fixture directories matching `canaries/manifest.v0.json`
- `declarations/` — catalog-selected subset declarations that twinning imports
- `differential/` — read and write parity corpora for real-Postgres comparison
- `doctor_detectors/` — failure-mode fixtures backing the read-only doctor detector catalog
- `snapshots/` — committed-row snapshot fixtures for deterministic freeze/restore checks
- `storage/` — tournament-mode budget gate notes and storage-economics fixtures

The current files are active contract inputs:

- `canaries/` pins client-facing canary inputs, extracted SQL/IR fixtures, and
  runtime-backed harness assets
- `declarations/` pins parent catalog identity fixtures without coupling
  twinning to live catalog services
- `differential/` pins read/write corpus schemas and expectations used by the
  twin-side differential harnesses today, with room for future live-Postgres
  comparisons
- `doctor_detectors/` pins the refusal/report signals that must exist before
  any future doctor fix mode can be exposed
- `snapshots/` pins canonical committed-state bytes independently of artifact
  metadata such as creation time and restore lineage
- `storage/` documents which budget metrics are mandatory in CI and which can
  be explicitly platform-skipped when RSS measurement is unavailable
