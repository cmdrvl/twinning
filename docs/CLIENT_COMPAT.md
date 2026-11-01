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
`cargo test --offline --test compat_drivers -- --nocapture`.
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

## M5 real MCP end-to-end gate results (2026-11-01)

The R01-R08 tests ran the committed `twinning-mcp` binary over stdio. Each case
started and stopped its twin through MCP tool calls, passed an explicit real
`twinning` binary with `--twinning-bin`, and checked the child command line for
`--host 127.0.0.1`. No fake twin was used for these cases.

The focused command was
`cargo test --locked --offline -p twinning-mcp --test e2e_real -- --nocapture --test-threads=1`.
It passed R01-R08: 8 passed, 0 failed, 0 ignored; `SKIPPED:` count 0. The run
used psql 14.24 (Homebrew), psycopg2 2.9.13 (`dt dec pq3 ext lo64`), SQLAlchemy
2.1.6, and uv 0.13.5 (Homebrew 2026-10-22, x86_64-apple-darwin). R05-R07 each
ran the M0c `tests/compat/driver_cases.py` using the approved
`uv run --with psycopg2-binary --with sqlalchemy python <absolute script path>`
form from a scratch directory under the test artifacts directory.

| Case | Live operation | Observed result |
| --- | --- | --- |
| R01 | MCP `twin_start`, listener readiness, non-loopback connection attempt, `twin_stop`, `twin_report` | The real child argv used `--host 127.0.0.1`. psql attempted `192.168.86.29` on the allocated port and exited 2 with `timeout expired`; the twin remained ready, stopped with exit 0, and reported `READY`. |
| R02 | psql sent `BEGIN` and `ROLLBACK` as separate `-c` commands through the returned connection string | Both commands succeeded without SQLSTATE; stop report was `READY`. |
| R03 | psql `SELECT version()` with libpq's default SSL mode | Query succeeded and returned `PostgreSQL 16.0 (twinning)`; stop report was `READY`. |
| R04 | Malformed startup disconnect, successful psql query, `BEGIN` followed by disconnect, then another `BEGIN` and `ROLLBACK` | The final stop report was `READY`; the trace had no SQLSTATE `55P03`. |
| R05 | Real psycopg2 inserts and primary-key reads from the M0c driver script | Valid insert and reads succeeded. CHECK violation returned `23514`; duplicate primary key returned `23505`. Report: 2 rows, `check_violations=1`, `unique_violations=1`, `fk_violations=0`, `not_null_violations=0`. |
| R06 | Real SQLAlchemy Core insert and select by primary key from the M0c driver script | Insert and select succeeded. The script's hstore startup probe returned zero rows. Report counters matched R05. |
| R07 | Full stdio MCP session with real driver cases, `twin_stop`, then `twin_report` | The report showed the CHECK violation (`23514`), `check_violations=1`, `unique_violations=1`, and 2 rows. |
| R08 | `twin_seed_contract`, filled seed, `twin_start` with the seed, then `twin_stop` | Stop summary showed 1 row in `public.deals` and `outcome=READY`. |

G3 and the full workspace suite were also run from a fresh clone at source
revision `d6bdf1f` with an initially absent `target` directory. In that clone,
`cargo build --locked -p twinning`, `cargo fmt --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --locked --offline -- --nocapture` all exited 0. The
workspace suite had `SKIPPED:` count 0.

For G5, the process snapshot before and after the full suite was identical.
Both `pgrep -fl '[t]winning|fake_[t]winning'` snapshots contained only the
pre-existing NTM monitor process, PID 51566; `diff -u` exited 0. No process
started by the suite remained.

## REST real-client gate results (2026-11-01)

The RR01-RR08 tests started `twinning-mcp` and its real `twinning rest` child
through MCP tool calls, copied the checked-in OpenFIGI response-stub fixture
into each temporary workdir, and used curl for live HTTP requests. The tested
curl was `8.7.1` on `x86_64-apple-darwin24.0`, using SecureTransport and
LibreSSL `3.3.6`.

| Case | Request or boundary | Observed result |
| --- | --- | --- |
| RR01 | `POST /v3/mapping` with `X-OPENFIGI-APIKEY` and `[ {"idType":"ID_CUSIP","idValue":"037833100"} ]` | HTTP 200; response contained FIGI `BBG000B9XRY4` and ticker `AAPL`. |
| RR02 | Same POST without the API key | HTTP 401; response code `unauthorized`. |
| RR03 | Same POST with an object body instead of an array | HTTP 422; code `schema_validation_failed`, path `$`. |
| RR04 | `GET /v3/no-such-route` | HTTP 404; response code `not_found`. |
| RR05 | The four HTTP calls above, then `twin_stop` and `twin_report` | Report outcome `PASS`, `request_count=4`, `auth_refusal=1`, `schema_validation_refusal=1`, `response_stub=1`, `unmatched=1`; refusals were `unauthorized=1`, `schema_validation_failed=1`, `not_found=1`. `endpoints_exercised` contained `POST /v3/mapping`; `endpoints_not_exercised` was empty. |
| RR06 | Curl to the allocated port using host address `192.168.86.29` while the child argv bound `127.0.0.1` | Curl exited 28 after a connection timeout. The twin then stopped with exit 0 and request count 0. |
| RR07 | A spec containing a remote `$ref` to a local TCP sentinel | MCP returned `E_ARGUMENT_INVALID` before starting a child; the sentinel received no connection. |
| RR08 | Shutdown of active REST twins by MCP SIGTERM and stdin EOF | Both children exited, both reports had outcome `PASS`, and the before/after filtered process snapshots were empty. |

The focused command
`cargo test --offline -p twinning-mcp --test e2e_rest -- --nocapture --test-threads=1`
passed all eight cases with 0 failures and 0 ignored tests. No case printed
`SKIPPED:`. The demo in `docs/demo/rest/run.sh` repeated the four HTTP calls
with release binaries and produced a `PASS` report with four requests and the
same outcome and refusal counts.

This records a declared-subset REST twin run against one checked-in response
stub. It does not establish compatibility with the OpenFIGI service or other
specifications.

## Not supported / not tested

- Chaos or fault-injection behavior was not exercised.
- Authentication behavior beyond the scheme declared by this fixture was not
  tested. The observed missing-key request returned HTTP 401.
- Specifications other than
  `tests/fixtures/rest/openfigi_v2_v3/response-stub-schema.yaml` were not
  tested.
- Request bodies beyond the fixture schema were not tested. The observed
  object body was rejected with HTTP 422 at path `$`.
- Remote references are refused with `E_ARGUMENT_INVALID` before a child starts
  or a fetch is attempted.
- Routes and response behavior beyond the four requests listed above were not
  tested.
