# psycopg2_params

Pinned fixtures for the `psycopg2_params` compatibility canary.

- `ir_equivalence.json` freezes the declared parameterized mutation subset:
  `insert_values`, `insert_returning`, `upsert_pk`, the companion
  `select_by_pk` read, and a session-control near-miss refusal.
