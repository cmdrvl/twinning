# Client compatibility observations

This record covers the declared Postgres client subset tested against the live
`twinning postgres --serve` listener. M0c's real-driver run did not pass its
compatibility gate; these results are evidence of the current behavior, not a
compatibility claim.

| Client | Version tested | Statements exercised | Observed result | Final report |
| --- | --- | --- | --- | --- |
| `psql` | 14.24 | `BEGIN`, `ROLLBACK` using two `-c` arguments and libpq's default SSL mode (M0b, commit `f9d536a`) | Both statements succeeded; no SQLSTATE was returned. | Server exited cleanly after SIGTERM and emitted an `outcome=READY` report. |
| psycopg2 | 2.9.13 (`dt dec pq3 ext lo64`) | Parameterized valid insert, CHECK-violating insert, duplicate primary-key insert, SELECT by primary key using D10; SELECT by primary key once more with default libpq SSL mode | Every case returned SQLSTATE `0A000` (`unsupported_statement`). The query trace marks the requests as `simple_query`; expected `23514` and `23505` were not observed. | `check_violations=0`, `unique_violations=0`, and `public.deals.rows=0`. The trace contains seven `0A000` results and no `23514` or `23505`. |
| SQLAlchemy Core | 2.1.6 | Core insert and SELECT by primary key using D10 through the psycopg2 dialect | The dialect's startup `hstore` catalog probe was refused with `0A000`; the requested insert and SELECT did not succeed. | Same final report: `check_violations=0`, `unique_violations=0`, `public.deals.rows=0`. |

M0c used uv 0.13.5 with the approved invocation
`uv run --with psycopg2-binary --with sqlalchemy python <absolute script path>`
from a scratch directory below the test artifacts directory. The harness removed
inherited Conda activation variables so uv selected the available isolated
Python instead of reading malformed metadata from the ambient Anaconda 3.8
environment. No compatibility cases were skipped.

The M0c STOP RULE is active: real-driver insert violations are not represented
in the final report because the real-driver statements were refused before
constraint evaluation. Do not claim psycopg2 or SQLAlchemy compatibility until
the driver run returns the expected results and the final report records both
violation counters.
