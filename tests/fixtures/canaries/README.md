# canary fixtures

Each subdirectory here matches one canary ID from `canaries/manifest.v0.json`.

The checked-in fixture files pin the declared bootstrap-era contract for that
canary. Current fixture types include:

- `ir_equivalence.json` for cross-client expected-IR equivalence and refusal
  near-miss coverage
- `fixture.json` / companion assets for canaries that already have a frozen
  corpus, such as `extractor_canary`

`metadata_join_conformance` is a synthetic conformance probe rather than a
client library fixture: it pins the exact supported `information_schema.tables`
public-base-table query and the explicit join refusal boundary.
