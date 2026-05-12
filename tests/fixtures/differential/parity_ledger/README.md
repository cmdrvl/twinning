# parity ledger fixture

This fixture is the first process-based parity ledger for the declared
Postgres subset.

Default CI runs the fixture through `twinning postgres --run` and records the
twin-side pgwire observations. The `postgres_observed` fields are the pinned
reference observations for the same schema and SQL against a scratch Postgres
database. A live Postgres executor can replace those pinned observations when
`TWINNING_DIFF_POSTGRES_URL` points at a disposable database.

Live mode shells out to `psql`, resets the target database's `public` schema,
loads `schema.sql`, and executes the same cases before the twin-side run. Use it
only with a scratch database.

Requirement matrix:

| Requirement | Completion criterion | Fixture cases |
| --- | --- | --- |
| `process_run_once_twin` | live `run_once` pgwire shell is reliable end-to-end | all cases |
| `declared_success_parity` | declared subset queries are ledgered against reference outcomes | `write_insert_basic`, `read_select_by_pk`, `metadata_public_base_tables` |
| `catalog_metadata_parity` | exact catalog-introspection behavior is ledgered against reference outcomes | `metadata_public_base_tables` |
| `subset_refusal_parity` | out-of-subset relations return protocol-faithful errors | `outside_subset_relation` |

Run:

```bash
cargo test --test differential_suite parity_ledger
TWINNING_DIFF_POSTGRES_URL='postgres://...' cargo test --test differential_suite parity_ledger -- --nocapture
```
