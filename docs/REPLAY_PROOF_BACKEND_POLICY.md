# Replay/Proof Backend Policy

Policy version: `twinning.replay-backend-policy.v0`

This policy applies to twin-pair migration proof and other replay/proof modes.
It does not widen the v0 tournament twin. The tournament lane remains the
bounded-memory Postgres twin with explicit single-writer overlay semantics.

## Current State

The implemented proof runner is restore-backed:

- endpoints are materialized from `twinning.snapshot.v0` artifacts
- replay runs through the declared Postgres kernel subset
- reports and emitted snapshots cover committed state only
- schema bootstraps with load scripts are refused until live materialization is
  implemented

No snapshot-backed, disk-backed, or delegated replay/proof backend is enabled
yet.

## Backend Classes

| Backend class | Status | Allowed use |
|---|---:|---|
| `in_memory_snapshot` | implemented | Current tournament, snapshot-pair proof, and restore-backed orchestration when committed state fits the memory budget |
| `snapshot_backed` | deferred | Large read-mostly replay sets if reset starts from a content-addressed snapshot and emits the same canonical committed-state surface |
| `disk_backed` | deferred | Large Twin A or Twin B proof runs if SQLSTATEs, row encoding, ordering, constraints, and final snapshots are indistinguishable from the in-memory kernel |
| `delegated_postgres` | deferred | Twin A read-only historical replay only, when the delegated source is Postgres-compatible for the declared replay subset and every observation remains deterministic |

## Non-Negotiable Invariants

- Protocol-visible behavior must be equivalent for the declared subset:
  columns, rows, ordering, command tags, coercions, and SQLSTATEs cannot change
  because a heavier backend was chosen.
- Unsupported shapes remain SKIP rows or protocol-visible refusals, never
  successful proof observations.
- Reports and snapshots see committed state only.
- Snapshot hashes are over the canonical committed-state surface, not backend
  implementation details.
- Backend choice must not introduce scores, timing measurements, policy
  decisions, or `verify` reinterpretation into `twinning.twin-pair-proof.v0`.
- Pack sealing stays outside `twinning`; orchestration may hand off report and
  snapshot paths, but does not seal evidence.

## Delegation Rule

Twin A may delegate storage only when all of these are true:

- replay is read-only
- the replay manifest is translated Postgres-compatible
- the delegated source is pinned by an operator-supplied identity
- result observations are hashed and reported through the same proof surface
- a content-addressed committed-state snapshot or reference can be emitted for
  the proof bundle
- any unsupported query shape is recorded as a SKIP row or protocol-visible
  refusal

Twin B must remain a `twinning` materialization for target proof until a later
bead explicitly proves delegated target behavior without changing the proof
contract.

## Enablement Gates

A heavier backend can move from deferred to implemented only after a focused
slice proves:

- deterministic reset from the same base identity
- identical replay observations against the checked-in corpus
- identical committed-state snapshot hashes for equivalent state
- explicit backend provenance in operator-facing artifacts
- no new multi-writer or dirty-read semantics

Until those gates exist, restore-backed orchestration is the production
replay/proof boundary.
