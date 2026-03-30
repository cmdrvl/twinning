# canary harnesses

These modules are the scaffold for the v0 client compatibility suite named in
`canaries/manifest.v0.json`.

Current state:

- the manifest is checked in
- the harness module names line up one-to-one with manifest canary IDs
- the runtime-backed canary tests execute in `tests/compatibility_suite.rs`
- unsupported shapes stay explicit through the checked-in canary assertions and fixtures

Future canary work should extend these harnesses and fixtures directly instead of
creating a parallel naming scheme elsewhere in the repo.
