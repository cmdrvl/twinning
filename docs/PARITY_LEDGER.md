# Parity Ledger

The parity ledger is the receipt shape for criterion 3 in `docs/goal.md`:
declared Postgres-subset queries should have an explicit reference observation,
a twin observation, and a pass/fail verdict.

Current command:

```bash
cargo test --test differential_suite parity_ledger
```

The default gate is CI-friendly and does not require a local Postgres service.
It launches `twinning postgres --run`, drives a child process over pgwire, and
compares the twin observations to the pinned Postgres-reference observations in
`tests/fixtures/differential/parity_ledger/cases.json`.

Live Postgres reference execution is reserved for scratch databases only. Use
`TWINNING_DIFF_POSTGRES_URL` for that lane; the database may be reset by the
future live reference runner.

Requirement matrix:

| Requirement | Completion criterion | Current proof |
| --- | --- | --- |
| `process_run_once_twin` | run_once bind, child execution, shutdown, and report emission | `run_once_parity_ledger_records_success_and_protocol_refusal_entries` |
| `declared_success_parity` | at least one declared read and one declared write are ledgered | `write_insert_basic`, `read_select_by_pk` |
| `subset_refusal_parity` | out-of-subset relation references produce protocol-visible `42P01` | `outside_subset_relation` |

Ledger entries include:

- `query_id`
- `client_surface`
- `sql`
- `expected`
- `postgres_observed`
- `twin_observed`
- `verdict`
