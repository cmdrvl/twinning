# twin-pair migration proof fixture

Prototype fixture for the first Postgres-kernel twin-pair migration proof.

The first migration-proof cut uses translated Postgres-compatible replay. True
Oracle/TNS protocol fidelity is explicitly deferred; non-Postgres protocol
claims must be SKIP entries or process-level proof refusals, never successful
proof rows.

The fixture compares two endpoints over one catalog-declared subset. The pass
case gives both endpoints byte-identical committed state. The divergence case
keeps the same schema/declaration but changes one committed row so the proof
report records an intentional `query_result` mismatch. A refusal case proves
that both endpoints surface the same protocol-visible SQLSTATE for an
out-of-subset relation.

The replay matrix currently exercises translated Postgres-compatible point
lookup, filtered scan, aggregate count, intentional divergence, and SQLSTATE
refusal-parity rows. Join replay, catalog introspection, and historical workload
families remain explicit SKIP entries until the live v0 center declares and
proves those shapes.

The fixture also attaches target-side evidence identities for raw `verify`,
`benchmark`, and `assess` artifacts. Those identities are references only: the
proof report does not read, score, or reinterpret the target artifacts.

Each proof case also emits a `twinning.twin-pair-replay-result.v0` replay-result
artifact section with timing-independent diff inputs: endpoint snapshot hashes,
left/right result hashes, and SQLSTATE parity. It intentionally excludes scores
and timing measurements.
