# agent-ergonomics pass 1 — HANDOFF

**Target.** `twinning` on branch `main` (no new branch created).
**Workspace.** `agent_ergonomics_audit/` (in-tree, committed with the code).
**Mode.** `full`.

## What shipped (1 source commit + tests)

A single coherent change — **complete the CLI self-documentation surface** —
applied directly in `src/cli.rs`, pinned by `tests/help_surface.rs`:

- Global `--json` / `--describe` now carry descriptions (were blank).
- Top-level `--help` gained a `long_about` and an `AFTER_HELP` footer that
  advertises the read-only discovery surfaces (`--describe`,
  `doctor capabilities/health/robot-docs --json`, `doctor --robot-triage`), the
  stdout/stderr + `next_command` output contract, the exit-code dictionary, and
  a canonical example.
- Every flag on every subcommand (`postgres`/`mysql`/`oracle` `TwinArgs`,
  `rest`, `port`, `mcp`, `snowflake`, `doctor`, `proof twin-pair[/orchestrate]`)
  now has a description. Required flags marked `[required]`; mutual-exclusion and
  composition rules described in prose.
- `postgres` subcommand gained 5 worked `after_help` examples for the canonical
  task plus the `--schema`/`--restore` exclusivity note.
- New `tests/help_surface.rs` (7 tests) asserts per-flag documentation across
  every command in both clap layouts, plus discovery-surface/exit-code/example
  presence.

## Key design constraint honored

Mutual-exclusion / required-source validation lives in `src/config.rs` and emits
the structured refusal envelopes (`code` + `message` + `next_command`). Adding
clap-level `required`/`conflicts_with` would short-circuit those envelopes with
generic clap errors — a regression in `error_pedagogy`. **All changes are
help-text only; constraints are described, never enforced at the clap layer.**
Verified post-change: `E_BOOTSTRAP_SOURCE_REQUIRED`, `E_ENGINE_UNIMPLEMENTED`,
`--describe`, `doctor capabilities`, and clap typo-suggestion all unchanged.

## Verification

- `cargo fmt --check` — clean
- `cargo clippy --all-targets --features all -- -D warnings` — clean
- `cargo test --features all` — all pass (incl. new 7-test `help_surface`)
- `./scripts/ubs_gate.sh` — passed (critical=0)
- skill verifiers (`verify-determinism`, `verify-stdout-stderr-split`,
  `verify-non-tty-discipline`) on `doctor health`/`capabilities` — all PASS

## Scorecard delta (audited dimensions)

| Dimension | Pre | Post |
|-----------|-----|------|
| agent_ease_of_use | 430 | ~830 |
| self_documentation | 440 | ~840 |
| agent_intuitiveness | 720 | ~860 |
| regression_resistance | 600 | ~860 |

Output-contract dimensions (output_parseability 900, error_pedagogy 880,
determinism 900, composability 880, safety 820) were already strong and were
preserved, not changed.

## Ambition Bar

The tool already had the mega-command, capabilities/robot-docs, `--json`
everywhere, `next_command` errors, and clap typo-inference — the "add when
missing" change types were pre-existing and strong. The genuine, pervasive gap
was undocumented `--help`, now closed across the entire flag surface and pinned.
Adding further changes in already-strong areas would be churn (skill: "convergence
beats churn; don't inflate the count"). 4 dimensions moved; no self-prompt
re-entry warranted because the work is comprehensive within the real gap, not a
polite scorecard.

## Queued for a future pass (low priority, not gaps today)

- Consider mirroring the per-flag help into `operator.json` descriptions if they
  ever drift (currently consistent; an `audit-readme-vs-help` drift test could
  pin this).
- `doctor health`/`capabilities` subcommand `about` strings could mention `--json`
  inline; currently surfaced via the top-level footer.
