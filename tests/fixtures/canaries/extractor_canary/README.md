# extractor_canary

This directory pins the representative extractor target for the runtime-backed
`extractor_canary` compatibility test.

Frozen fixture surface:

- `representative_extractor.py` — the pinned extractor entrypoint path that the
  active runtime-backed harness runs unchanged
- `fixture.json` — manifest-aligned description of the extractor canary subset
- `input_rows.json` — deterministic checked-in mutation and read corpus for that
  pinned extractor path

Pinned subset boundaries:

- session flow: startup/auth baseline, parameter status, `BEGIN`, `COMMIT`, and
  extended query parse/bind/execute/sync
- write shapes: `insert_values`, `upsert_pk`, `upsert_unique`
- read shapes: `select_filtered_scan`, `select_is_null`
- required SQLSTATE coverage: `23502`, `23503`, `23505`, `23514`, `22P02`

Current contract:

- this fixture freeze describes the currently running `extractor_canary` lane
- broader extractor traffic stays outside the supported subset until separate
  canaries prove it
