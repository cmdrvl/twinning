# CODEX.md - twinning

Codex-specific notes for this repo. Read [AGENTS.md](./AGENTS.md) first; this
file only records Codex harness caveats.

## Commands

Prefer robot-safe issue commands and direct Cargo gates:

```bash
bv --robot-next
br ready --json
br show <id> --json
br update <id> --status in_progress --json
br close <id> --reason "Completed" --json
br sync --flush-only

cargo run -- doctor health --json
cargo run -- doctor capabilities --json
cargo run -- doctor robot-docs
cargo run -- doctor --robot-triage

cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
./scripts/ubs_gate.sh
```

Never run bare `bv`; it starts an interactive TUI. After Beads state changes,
run `br sync --flush-only` and include `.beads/` in the commit.

## Permissions

Use `apply_patch` for manual file edits. Do not revert user changes or use
destructive git commands unless the user explicitly requests them. Network and
outside-worktree writes may require harness approval. `rch` is optional in this
repo; use direct `cargo` when the remote build runner is absent.

## Hooks

There is no repo-local Codex hook configuration. Do not assume external hooks
will format, test, sync Beads, or run UBS for you. Run the appropriate gate
before closing a bead.

## Environment

The expected working directory is the repository root. Current builds expect a
sibling `verify/` checkout for the local path dependencies in `Cargo.toml`.
The supported product lane is Postgres only: bootstrap artifacts plus the live
`run_once` shell for canary-defined SQL shapes.

## Session Caveats

Direct MCP Agent Mail tools may not be available in Codex. If they are absent,
do not block; rely on Beads, exact file ownership, and precise commits. Keep
`src/main.rs` thin, avoid broad refactors, and do not widen runtime support
without manifest-backed canaries.
