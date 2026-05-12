# CLAUDE.md - twinning

Claude Code-specific notes for this repo. Read [AGENTS.md](./AGENTS.md)
first; it is the shared policy and source of truth for scope, quality gates,
and coordination.

## Commands

Use the same copy-pasteable commands as other agents:

```bash
bv --robot-next
br ready --json
br show <id> --json
br update <id> --status in_progress --json

cargo run -- doctor health --json
cargo run -- doctor capabilities --json
cargo run -- doctor --robot-triage

cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
./scripts/ubs_gate.sh
```

For docs-only changes, `cargo fmt --check` is the required local gate. For
routine code, run fmt, clippy, and tests. Add UBS for runtime-sensitive or
stop-ship work.

## Permissions

Treat shell access as a write-capable local development environment. Do not run
destructive git commands, delete files, or perform network installs unless the
user explicitly asks or the harness requests approval. If `rch` is available it
can be used as an accelerator, but direct `cargo` commands remain the source of
truth on machines where `rch` is not configured.

## Hooks

There is no repo-local `.claude/` hook configuration checked in. Do not assume
Claude Code hooks have run. The enforced repository gates are the Cargo and UBS
commands above plus CI in [.github/workflows/ci.yml](./.github/workflows/ci.yml).

## Environment

Current source builds expect a sibling `verify/` checkout because
`Cargo.toml` uses local path dependencies for `verify-core` and
`verify-engine`. The active implementation is Postgres-first: bootstrap plus
the live `run_once` shell for the proven canary subset.

## Session Caveats

Use Beads as the execution graph, not an internal todo list alone. If Agent
Mail tools are exposed, reserve only exact files you will edit and use the bead
ID as the thread. If Agent Mail is not exposed, continue with `br`, narrow file
ownership, and short handoff notes.
