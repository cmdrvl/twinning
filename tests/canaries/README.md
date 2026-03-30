# canary harnesses

These modules are the scaffold for the v0 client compatibility suite named in
`canaries/manifest.v0.json`.

Current state:

- the manifest is checked in
- the harness module names line up one-to-one with manifest canary IDs
- the tests are marked `#[ignore]` until the live pgwire runtime exists

When the runtime work starts, these harnesses should grow into real compatibility
tests instead of creating a parallel naming scheme elsewhere in the repo.
