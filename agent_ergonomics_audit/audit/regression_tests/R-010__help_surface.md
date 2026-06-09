# R-010 — help-surface completeness regression test

The actual test lives in the repo's Cargo integration-test tree (so it runs under
`cargo test`), per this repo's convention that golden tests live in `tests/`:

    tests/help_surface.rs   (7 tests)

It asserts:
- every long flag on every subcommand carries a description in `--help`
  (robust to both clap layouts — compact inline and top-level expanded);
- the top-level `--help` advertises the read-only discovery surfaces
  (`--describe`, `doctor capabilities --json`, `doctor --robot-triage`);
- the exit-code dictionary appears in top-level `--help`;
- `postgres --help` includes a worked example and the `--schema`/`--restore`
  exclusivity note.

Run: `cargo test --features all --test help_surface`

Fails against the pre-apply (blank-flag) binary: pre-apply `--help` had bare
`--json`/`--describe` with no descriptions and no discovery footer, so both
`assert_all_flags_documented` and the `contains("machine-readable JSON")` /
discovery-needle assertions fail.
