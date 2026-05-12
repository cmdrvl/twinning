# twin-pair migration proof fixture

Prototype fixture for the first Postgres-kernel twin-pair migration proof.

The fixture compares two endpoints over one catalog-declared subset. The pass
case gives both endpoints byte-identical committed state. The divergence case
keeps the same schema/declaration but changes one committed row so the proof
report records an intentional `query_result` mismatch. A refusal case proves
that both endpoints surface the same protocol-visible SQLSTATE for an
out-of-subset relation.

The replay matrix currently exercises translated Postgres-compatible point
lookup, filtered scan, and aggregate count reads. Join and introspection replay
remain explicit SKIP entries until the live v0 center declares and proves those
shapes.

The fixture also attaches target-side evidence identities for raw `verify`,
`benchmark`, and `assess` artifacts. Those identities are references only: the
proof report does not read, score, or reinterpret the target artifacts.
