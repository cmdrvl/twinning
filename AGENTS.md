# AGENTS.md — twinning

> Repo-specific guidance for AI coding agents working in `twinning`.

This file adds repo-specific instructions on top of the shared monorepo rules
when you are working inside the full `cmdrvl` workspace. In the standalone
`twinning` repo, treat this file and
[docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md) as the local source of truth.

---

## twinning — What This Project Does

`twinning` is the epistemic spine and factory's **interface twin primitive**.

It materializes already-decided state behind a protocol-faithful boundary so
existing client code can run against a fast, disposable twin instead of a large
real database.

Pipeline / factory position:

```text
decoding / materialize -> twinning -> embedded verify -> assess / pack
```

What `twinning` owns:

- protocol/runtime behavior for the declared client subset
- normalized schema/catalog bootstrap
- deterministic twin reports and snapshots
- row-store semantics and constraint enforcement for the supported subset
- tournament-mode storage behavior and reset economics
- raw twin-native metrics

What `twinning` does not own:

- claim resolution or canonicalization (`decoding`, `canon`)
- constraint semantics (`verify`)
- gold-set scoring (`benchmark`)
- policy decisions (`assess`)
- evidence sealing (`pack`)
- broad multi-engine or non-SQL compatibility in the v0 center

---

## Current Repository State

The current repo is **Phase 0 bootstrap-only**.

Implemented now:

- Postgres DDL parsing and deterministic catalog normalization
- `twinning.v0` bootstrap reports
- `twinning.snapshot.v0` bootstrap snapshots with hash verification
- verify-artifact loading and hashing
- explicit refusal behavior for unimplemented live runtime paths

Not implemented yet:

- pgwire listener
- SQL execution
- row materialization and constraint enforcement
- bounded-memory overlays
- live `--run` orchestration
- embedded verify execution over materialized state

Key references:

- [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md) — implementation-direction spec
- [docs/PLAN_TWINNING_FUTURES.md](./docs/PLAN_TWINNING_FUTURES.md) — deferred futures, with twin-pair migration proof first
- [.beads/issues.jsonl](./.beads/issues.jsonl) — execution graph
- [README.md](./README.md) — operator-facing contract and quickstart

Implication for new work:

- do not pretend the live twin exists when it does not
- do not widen scope beyond the canary-defined Postgres subset
- do not start non-SQL or second-engine work before the Postgres tournament wedge is real

---

## Quick Reference

```bash
# Read the main plan first
sed -n '1,260p' docs/PLAN_TWINNING.md

# See the execution graph
br ready
br blocked
br show <id>

# Current bootstrap path
cargo run -- postgres --schema schema.sql --json
cargo run -- postgres --schema schema.sql --verify schema.verify.json \
  --report out/bootstrap.json --snapshot out/bootstrap.twin --json
cargo run -- --describe

# Quality gates
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
./scripts/ubs_gate.sh

# Beads workflow
br update <id> --status in_progress
# ... implement ...
br close <id> --reason "Completed"
br sync --flush-only
git add .beads/
git commit -m "sync beads"

# Session search
cass search "twinning" --robot --limit 5
```

---

## Source of Truth

- **Spec:** [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md)
- **Deferred direction:** [docs/PLAN_TWINNING_FUTURES.md](./docs/PLAN_TWINNING_FUTURES.md)
- **Execution graph:** [.beads/issues.jsonl](./.beads/issues.jsonl)

If code, README, and plan disagree, the plan wins.

When there is tension between the current bootstrap code and the intended live
runtime, keep the current code honest and move the implementation toward the
plan instead of silently changing the contract.

---

## File Map

| Path | Purpose |
|------|---------|
| `src/main.rs` | thin binary entrypoint only |
| `src/lib.rs` | module root and CLI/runtime handoff |
| `src/cli.rs` | clap surface and engine/flag definitions |
| `src/config.rs` | validated runtime/bootstrap config |
| `src/catalog.rs` | Postgres DDL parsing and normalized catalog construction |
| `src/runtime.rs` | bootstrap lifecycle, load/restore, report/snapshot emission |
| `src/report.rs` | `twinning.v0` report model and rendering |
| `src/snapshot.rs` | `twinning.snapshot.v0` model, hashing, write/restore |
| `src/refusal.rs` | refusal taxonomy and rendering |
| `schemas/twinning.v0.schema.json` | report schema contract |
| `schemas/twinning.snapshot.v0.schema.json` | snapshot schema contract |
| `schemas/twinning.canary-manifest.v0.schema.json` | canary-manifest schema contract |
| `canaries/manifest.v0.json` | checked-in normative v0 compatibility manifest |
| `docs/PLAN_TWINNING.md` | main implementation plan |
| `docs/PLAN_TWINNING_FUTURES.md` | deferred futures and twin-pair migration path |
| `operator.json` | operator manifest surface |
| `.beads/issues.jsonl` | execution graph for next implementation steps |
| `tests/canary_manifest.rs` | manifest-to-layout contract checks |
| `tests/canaries.rs` | integration-test root for compatibility canaries |
| `tests/canaries/**` | scaffolded per-canary harness modules |
| `tests/differential.rs` | integration-test root for differential corpora |
| `tests/differential/**` | scaffolded differential corpus modules |
| `tests/fixtures/**` | placeholder canary and corpus fixture layout |

Critical structural rules:

- `src/main.rs` stays thin
- bootstrap/report/snapshot protocol types stay out of ad hoc CLI strings
- future pgwire code belongs under a dedicated protocol module, not inside `runtime.rs`
- future semantic kernel and backend work should follow the plan's separation of concerns rather than accreting into one file

---

## Output Contract (Critical)

Current exit domain:

| Exit | Meaning |
|------|---------|
| `0` | clean bootstrap |
| `1` | reserved for future live verify-violation exits |
| `2` | refusal / bootstrap failure / CLI error |

Current artifacts:

- `twinning.v0` report
- `twinning.snapshot.v0` snapshot

Runtime-facing rules that must survive implementation:

- bootstrap/configuration failures are process-level refusals
- unsupported live protocol or SQL shapes are client-visible errors, not process exits
- snapshots and reports see committed state only
- same schema + same committed state must produce the same snapshot hash

Do not replace refusal envelopes with ad hoc text. Do not silently degrade into
partial semantics.

---

## Core Invariants (Do Not Break)

### 1. One narrow Postgres wedge first

The v0 center is one Postgres tournament twin for the canary-defined subset.

Do not:

- add MySQL runtime support
- add Oracle/TNS support
- start VSAM / IMS / CICS work in this repo's main lane
- broaden SQL claims beyond the manifest and canaries

### 2. Bootstrap honesty matters

The current repo is bootstrap-only.

Do not:

- imply `--run` works when it still refuses
- fake pgwire compatibility
- ship placeholder execution paths that look like success

### 3. Canary-defined support only

The supported session and SQL subset is defined by the checked-in canary
manifest and companion harnesses, not by prose optimism.

If a shape is not named and proven, it is unsupported.

### 4. Clean spine boundaries

`twinning` materializes and enforces runtime behavior.

It does not:

- decide truth
- score against gold data
- invent its own constraint language
- make proceed/block decisions

### 5. Snapshot determinism is mandatory

Snapshot hashes must be stable over the canonical hash surface.

Do not let timestamps, session state, warnings, or debug strings leak into the
content-addressed meaning of a snapshot.

### 6. Verify stays embedded and raw

The live twin must execute embedded `verify` over materialized twin state and
attach raw `verify.report.v1` payloads.

Do not:

- shell out to batch `verify` from live runtime mode
- reinterpret verify semantics
- collapse twin-native metrics and verify results into a pseudo-score

### 7. Session semantics stay explicit

The intended live v0 model is single-writer, not single-session.

Do not smuggle in:

- dirty reads
- multi-writer semantics
- silent writer contention

### 8. Tournament economics are part of correctness

Bounded-memory overlays, fast reset, and shared base snapshots are not optional
performance polish. They are part of the v0 contract.

If storage economics fail, stop widening the interface surface.

---

## Immediate Work Order

Follow the order in the plan and execution graph:

1. canary + differential harnesses
2. shared Operation IR
3. pgwire/session compatibility
4. row store and constraint executor
5. bounded-memory overlay/backend work
6. embedded verify bridge

Do not reorder this just because a lower layer looks interesting.

Twin-pair migration proof is the first deferred artifact direction after the v0
center. It should reuse the same Postgres kernel and snapshot contract rather
than reopening the architecture.

---

## Quality Gate Tiers

Match the gate to the scope of your change:

| Scope | Gate |
|-------|------|
| docs-only | `cargo fmt --check` |
| routine code | `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` |
| runtime-sensitive (report/snapshot/refusal paths) | routine + verify snapshot determinism (same schema bytes produce same snapshot hash) |
| stop-ship | all of the above + `./scripts/ubs_gate.sh` + manual review of artifact contract changes |

The CI workflow at `.github/workflows/ci.yml` runs fmt, clippy, test, and UBS on every push and PR.

---

## MCP Agent Mail — Multi-Agent Coordination

Agent Mail is the coordination layer for multi-agent sessions in this repo: identities, inbox/outbox, thread history, and advisory file reservations.

### Session Baseline

1. If direct MCP Agent Mail tools are available in this harness, ensure project and reuse your identity:
   - `ensure_project(project_key=<abs-path>)`
   - `whois(project_key, agent_name)` or `register_agent(...)` only if identity does not exist
2. Reserve only exact files you will edit:
   - Allowed: `src/catalog.rs`, `tests/canaries/psql_smoke.rs`
   - Not allowed: `src/**`, `tests/**`, whole directories
3. Send a short start message and finish message for each bead, reusing the bead ID as the thread when practical.
4. Check inbox at moderate cadence (roughly every 2-5 minutes), not continuously.

### Important `ntm` Boundary

When this repo is worked via `ntm`, the session may be connected to Agent Mail even if the spawned harness does **not** expose direct `mcp__mcp-agent-mail__...` tools.

If direct MCP Agent Mail tools are unavailable:

- do **not** stop working just because mail tools are absent
- continue with `br`, exact file reservations via the available coordination surface, and overseer instructions
- treat Beads + narrow file ownership as the minimum coordination contract

### Stability Rules

- Do not run retry loops for `register_agent`, `create_agent_identity`, or `macro_start_session`.
- If a call fails with a transient DB/SQLite lock error, back off for 90 seconds before retrying.
- Continue bead work while waiting for retry windows; do not block all progress on mail retries.

### Communication Rules

- If a message has `ack_required=true`, acknowledge it promptly.
- Keep bead updates short and explicit: start message, finish message, blocker message.
- Reuse a stable bead thread when possible for searchable history.

### Reservation Rules

- Reserve only specific files you are actively editing.
- Never reserve entire directories or broad patterns.
- If a reservation conflict appears, pick another unblocked bead or a non-overlapping file.

---

## br (beads_rust) — Dependency-Aware Issue Tracking

**Note:** `br` is non-invasive and never executes git commands. After `br sync --flush-only`, you must manually run `git add .beads/` and `git commit`.

Beads is the execution source of truth in this repo.

- Beads = task graph, state, priorities, dependencies
- Agent Mail = coordination, reservations, audit trail

```bash
br ready
br show <id>
br update <id> --status in_progress
br close <id> --reason "Completed"
br sync --flush-only
git add .beads/
git commit -m "sync beads"
```

Conventions:

- include bead IDs in coordination subjects, for example `[<bead-id>] Start pgwire session startup`
- use the bead ID in reservation reasons when the tool supports it
- prefer concrete ready beads over the epic tracker

Workflow:

1. Start with `br ready`.
2. Mark the bead `in_progress` before editing.
3. Reserve exact files and send a short start update when coordination tools are available.
4. Implement and run the right quality gate.
5. Close the bead, send a completion note, and release reservations.

Repo-specific graph shape:

```text
canary harnesses + differential corpora
  -> operation IR
  -> pgwire / session startup
  -> row store + constraint executor
  -> bounded-memory overlay / backend
  -> embedded verify bridge
  -> shutdown + final report/snapshot
```

Important:

- the epic is tracker noise; prefer concrete ready beads
- do not start blocked feature work just because the file seems obvious
- if a bead is `in_progress` with no assignee, no comments, and no active reservation, reopen it before picking it up

---

## File Reservation Guidance

This repo is being prepared for parallel agent implementation. Reserve exact files only.

Per-lane target surfaces:

| Lane | Expected files |
|------|----------------|
| foundation | `Cargo.toml`, `src/lib.rs`, `src/main.rs` |
| cli | `src/cli.rs`, `src/config.rs`, `src/refusal.rs` |
| catalog | `src/catalog.rs` |
| runtime | `src/runtime.rs` |
| report | `src/report.rs` |
| snapshot | `src/snapshot.rs` |
| canary harnesses | `tests/canaries/<canary>.rs`, `tests/fixtures/canaries/<canary>/` |
| differential corpora | `tests/differential/<corpus>.rs`, `tests/fixtures/differential/<corpus>/` |
| protocol (future) | `src/protocol/*.rs` (when pgwire lands) |
| kernel (future) | `src/kernel/*.rs` (when execution lands) |

Do not reserve broad globs like `src/**` or `tests/**`.

---

## Editing Rules

- do not widen v0 scope beyond the canary-defined Postgres subset
- do not hide plan disagreements in implementation details
- do not fake pgwire compatibility or ship placeholder execution paths that look like success
- do not make `main.rs` the real implementation layer
- do not optimize runtime by changing semantics
- do not collapse twin-native metrics and verify results into a pseudo-score
- do not smuggle in dirty reads or multi-writer semantics
- do add tests when runtime work could change behavior
- do add refusal paths for unimplemented shapes before adding the shapes themselves

---

## Multi-Agent Notes

This repo is explicitly being prepared for parallel agent work.

That means:

1. Keep changes granular.
2. Prefer one or two file touches per bead.
3. Use existing module boundaries instead of introducing cross-cutting helpers early.
4. Future pgwire code belongs under a dedicated protocol module, not inside `runtime.rs`.
5. Future kernel and backend work should follow the plan's separation of concerns.

---

## CI / Release Status

Current repo reality:

- Rust crate exists and is locally runnable with `cargo run -- ...`
- CI workflow exists at `.github/workflows/ci.yml`
- release workflow exists at `.github/workflows/release.yml`
- tagged releases are wired to publish cross-target artifacts and update `cmdrvl/tap`
- no published binary yet until the first release is cut

Do not add README badges or install claims until they are real.

Release discipline follows the spine pattern:

- `fmt` / `clippy` / `test` before publish
- `./scripts/ubs_gate.sh` in CI
- deterministic artifacts
- `main` as primary branch

---

## Session Completion

Before ending a session in this repo:

1. verify plan alignment with [docs/PLAN_TWINNING.md](./docs/PLAN_TWINNING.md)
2. run the right quality gate for the current repo state
3. sync Beads if you changed issue state: `br sync --flush-only && git add .beads/`
4. confirm any file reservations or bead comments reflect the actual handoff state
5. if you were explicitly asked to commit or push, do so with a precise message
6. confirm `git status` accurately reflects what remains uncommitted
