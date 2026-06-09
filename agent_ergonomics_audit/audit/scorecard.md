# twinning — agent-ergonomics scorecard (pass 1)

Target SHA at audit start: `800808a` · branch: `main` · mode: `full`
Binary: `cargo build --features all` → `target/debug/twinning`

## Method

Surfaces enumerated from `src/cli.rs` clap definitions + runtime `--help`,
`--json`, refusal, and `doctor` output. Output-contract dimensions verified
with the skill's `verify-determinism.sh`, `verify-stdout-stderr-split.sh`, and
`verify-non-tty-discipline.sh` against `doctor health`/`doctor capabilities`
(all PASS).

## Dimension summary (0–1000)

| Dimension | Pre | Evidence |
|-----------|-----|----------|
| agent_intuitiveness | 720 | `arg_required_else_help`; bare invocation prints help; clap typo suggestion (`--jsno`→`--json`). Loses points: `--help` flags are undocumented so a guessed flag's purpose is opaque. |
| agent_ergonomics | 760 | Single-call bootstrap; `doctor --robot-triage` mega-command exists. |
| agent_ease_of_use | **430** | `doctor capabilities`/`robot-docs`/`--describe` exist, BUT every `--help` flag is blank → an agent cannot learn a flag's purpose, required-ness, or exclusivity without reading source or operator.json. |
| output_parseability | 900 | `--json` everywhere; clean stdout(data)/stderr(diag) split; exit-code dictionary 0/1/2. Verified. |
| error_pedagogy | 880 | Structured refusals carry `code`+`message`+`detail`+`next_command`. Verified live. |
| intent_inference | 700 | clap Levenshtein suggestion on unknown flags; config-layer refusals redirect ambiguous/missing-source invocations with `next_command`. |
| safety_with_recovery | 820 | Read-only `doctor`; `doctor --fix` intentionally absent with rationale; refusals non-destructive. |
| determinism_and_reproducibility | 900 | Snapshot hashing is content-addressed; `--json` byte-identical across runs under `SOURCE_DATE_EPOCH`. Verified. |
| self_documentation | **440** | `capabilities`/`robot-docs`/operator.json strong, BUT `--help` itself documents nothing per-flag and never points agents at the discovery surfaces (Axiom 🧭 fail). |
| composability | 880 | Honors non-TTY; JSON pipes cleanly; documented exit codes. |
| regression_resistance | 600 | Rich JSON/refusal/snapshot test suite, but no test pins `--help`/flag-doc completeness, so help text can silently rot back to blank. |

## Worst surfaces (Phase 4 targets)

The two lowest dimensions — `agent_ease_of_use` (430) and `self_documentation`
(440) — share a single root cause: **clap `help`/`long_about`/`after_help` are
empty on the global flags and on every subcommand argument**. One coordinated
fix in `src/cli.rs` lifts both dimensions across all 9 subcommands plus the
top-level command. `regression_resistance` (600) is lifted by pinning the new
help text with a test.

Critical constraint discovered: mutual-exclusion / required-source validation
lives in `src/config.rs` and emits the structured refusal envelopes with
`next_command`. Adding clap-level `required`/`conflicts_with` would short-circuit
those envelopes with generic clap errors — a regression in `error_pedagogy`.
**Fix is help-text only; constraints are described in prose, never enforced at
the clap layer.**
