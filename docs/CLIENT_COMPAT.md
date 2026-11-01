# Client compatibility observations

These observations come from runs against the live `twinning postgres --serve`
listener. Twinning is a declared-subset Postgres twin, not a general-purpose
Postgres server. The passing R05/R06 real-driver run below covers only the
listed operations.

| Client | Version tested | Statements exercised | Observed result | Final report |
| --- | --- | --- | --- | --- |
| `psql` | 14.24 | `BEGIN` and `ROLLBACK` sent as two `-c` arguments using libpq's default SSL mode (M0b, commit `f9d536a`) | Both statements succeeded with no SQLSTATE. | Server exited cleanly after SIGTERM and emitted an `outcome=READY` report. |
| psycopg2 | 2.9.13 (`dt dec pq3 ext lo64`) | Parameterized valid insert, CHECK-violating insert, duplicate primary-key insert, SELECT by primary key using D10, and SELECT by primary key using default libpq SSL mode | Valid insert and both SELECTs succeeded without SQLSTATE; CHECK violation returned `23514`; duplicate primary key returned `23505`. | `outcome=READY`; `public.deals.rows=2`; `not_null_violations=0`, `fk_violations=0`, `check_violations=1`, `unique_violations=1`. Trace SQLSTATEs: `23514`, `23505`. |
| SQLAlchemy Core via psycopg2 | 2.1.6 | Startup `hstore` OID/array-type probe, Core insert, and SELECT by primary key using D10 | The exact startup probe succeeded with zero rows; Core insert and SELECT succeeded without SQLSTATE. | Same final report: `public.deals.rows=2`; `check_violations=1`, `unique_violations=1`. |

The psycopg2 and SQLAlchemy results are from the R05/R06 run on 2026-11-01 at
commit `fa5570d791d98e696e24eac253b81b68be2cdb10`, exercised by
`CARGO_TARGET_DIR=target/pane-a cargo test --offline --test compat_drivers -- --nocapture`.
It passed all 7 declared driver cases; 0 cases were skipped. The run used uv
0.13.5 with the approved invocation
`uv run --with psycopg2-binary --with sqlalchemy python <absolute script path>`
from a scratch directory under the test artifacts directory. The live server
exited 0 and wrote the final report after SIGTERM.

## Not supported / not tested

The passing run does not establish general Postgres compatibility. The
following boundaries are explicit in current protocol tests and canaries:

- `SHOW ALL` returns SQLSTATE `0A000` in the raw-TCP simple-query test. A
  subsequent `SELECT version()` succeeds on that connection.
- A multi-statement simple query such as `BEGIN; ROLLBACK` returns `0A000` in
  the simple-query protocol test. Sending `BEGIN` and `ROLLBACK` as separate
  client commands is covered by the `psql` observation above.
- The broader reflection query
  `SELECT table_name FROM information_schema.tables` returns `0A000` in the
  simple-query test; an extended-query describe test also refuses that shape.
  The separate declared query for public base-table names is covered by the
  metadata conformance canary.
- The declared `select_inner_join_eq_refusal` canary returns `0A000` for the
  tested joined read:
  `SELECT deals.deal_id, tenants.tenant_name FROM public.deals JOIN public.tenants ON deals.tenant_id = tenants.tenant_id WHERE deals.deal_id = $1`.
- SQLAlchemy's tested `hstore` startup probe is answered with zero rows. This
  does not establish support for the hstore extension or general `pg_type`,
  `pg_catalog`, or ORM reflection queries.

Other catalog and reflection queries, join forms, SQL statements, drivers, and
Postgres protocol behaviors outside the checked-in canaries have not been
tested here. No compatibility claim is made for them, and this record does not
assert that every untested shape returns a particular SQLSTATE.
