# Parity Ledger

The parity ledger is the receipt shape for criterion 3 in `docs/goal.md`:
declared Postgres-subset queries should have an explicit reference observation,
a twin observation, and a pass/fail verdict.

Default command:

```bash
cargo test --test differential_suite parity_ledger
```

The default gate is CI-friendly and does not require a local Postgres service.
It launches `twinning postgres --run`, drives a child process over pgwire, and
compares the twin observations to the pinned Postgres-reference observations in
`tests/fixtures/differential/parity_ledger/cases.json`.

Live Postgres reference command:

```bash
TWINNING_DIFF_POSTGRES_URL='postgres://...' \
  cargo test --test differential_suite parity_ledger -- --nocapture
```

Live reference execution is reserved for disposable databases only. When
`TWINNING_DIFF_POSTGRES_URL` is set, the test uses `psql`, drops and recreates
the target database's `public` schema, loads the fixture schema, executes the
same ledger cases against real Postgres, and feeds those actual observations
into the twin comparison. Ledger metadata records `reference.mode` as either
`pinned_fixture` or `live_postgres`; live mode records a hashed source identity
instead of the raw connection URL.

Requirement matrix:

| Requirement | Completion criterion | Current proof |
| --- | --- | --- |
| `process_run_once_twin` | run_once bind, child execution, shutdown, and report emission | `run_once_parity_ledger_records_success_and_protocol_refusal_entries` |
| `declared_success_parity` | at least one declared read and one declared write are ledgered | `write_insert_basic`, `read_select_by_pk`, `metadata_public_base_tables` |
| `catalog_metadata_parity` | exact declared catalog-introspection behavior has a Postgres-reference observation | `metadata_public_base_tables` |
| `subset_refusal_parity` | out-of-subset relation references produce protocol-visible `42P01` | `outside_subset_relation` |

Ledger entries include:

- `reference.mode`
- `reference.source_identity`
- `reference.reset_scope`
- `query_id`
- `client_surface`
- `sql`
- `expected`
- `postgres_observed`
- `twin_observed`
- `verdict`
