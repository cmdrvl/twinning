# GEMINI.md - twinning

Gemini-specific notes for this repo. Read [AGENTS.md](./AGENTS.md) first; it
defines the shared scope, safety rules, and quality gates.

## Commands

Use non-interactive, copy-pasteable commands:

```bash
bv --robot-next
br ready --json
br show <id> --json
br update <id> --status in_progress --json

cargo run -- --describe
cargo run -- doctor health --json
cargo run -- doctor robot-docs

cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
./scripts/ubs_gate.sh
```

For docs-only changes, `cargo fmt --check` is enough. For code, match the gate
tier in [AGENTS.md](./AGENTS.md).

## Permissions

Assume the repository is a shared worktree. Do not discard uncommitted changes
you did not make. Avoid destructive shell or git operations unless the user
explicitly requested them. If a Gemini harness has network or filesystem
approval prompts, use them rather than working around the sandbox.

## Hooks

There is no repo-local Gemini hook or config file checked in. CI is the
authoritative automated hook surface and runs fmt, clippy, tests, UBS, and
release smoke checks.

## Environment

The crate is a Rust binary and library with local path dependencies on a
sibling `verify/` checkout. `rch` may not be installed on every machine; direct
`cargo` commands are valid. The current runtime surface is still narrow:
Postgres bootstrap plus one-child `run_once`, not a long-lived general database
server.

## Session Caveats

Use the Beads graph before choosing work. If Agent Mail or reservation tools
are unavailable, keep edits narrow and mention touched files in the handoff.
Do not claim MySQL, Oracle, or non-SQL runtime support unless the manifest and
canaries prove it.
