# differential corpora

These modules are the scaffold for the real-Postgres parity suite named in the
plan.

Current state:

- write and read corpus modules exist
- both are ignored by default until the live runtime and real-Postgres harness
  are implemented
- fixture directories exist so layout drift shows up early

The purpose of this layout is to prevent the differential suite from becoming an
afterthought once protocol work starts.
