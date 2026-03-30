# canaries

This directory holds the normative compatibility manifest for the v0 Postgres
subset.

- [manifest.v0.json](./manifest.v0.json) names the canaries and the exact
  session, write, read, and SQLSTATE claims the repo intends to support.
- [../schemas/twinning.canary-manifest.v0.schema.json](../schemas/twinning.canary-manifest.v0.schema.json)
  is the machine schema for that manifest.

Current state:

- the manifest is checked in now
- the corresponding harness layout exists under `tests/canaries/`
- the live pgwire runtime is not implemented yet, so those harnesses are still
  scaffolded and ignored by default

The manifest is still normative. A compatibility claim is only valid once the
corresponding harness passes.
