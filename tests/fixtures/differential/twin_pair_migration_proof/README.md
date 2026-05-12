# twin-pair migration proof fixture

Prototype fixture for the first Postgres-kernel twin-pair migration proof.

The fixture compares two endpoints over one catalog-declared subset. The pass
case gives both endpoints byte-identical committed state. The divergence case
keeps the same schema/declaration but changes one committed row so the proof
report records an intentional `query_result` mismatch. A refusal case proves
that both endpoints surface the same protocol-visible SQLSTATE for an
out-of-subset relation.
