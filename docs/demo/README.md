# Declared-subset Postgres MCP demo

This demo starts the real `twinning-mcp` server and `twinning` binary, then uses
the real psycopg2 driver for one parameterized insert and one CHECK violation.
Those operations match the narrow real-driver observation in
[CLIENT_COMPAT.md](../CLIENT_COMPAT.md). The demo does not establish support for
other psycopg2 statements or clients.

## Run from a clean clone

Prerequisites are Cargo, Python 3.11, and `uv`. From the repository root, run:

```bash
./docs/demo/run.sh
```

The wrapper builds both release executables with Cargo offline into
`target/pane-c` by default, then runs `demo.py` in an isolated `uv` environment
with cached psycopg2-binary 2.9.13. Set `CARGO_TARGET_DIR` to choose another
target directory. Both Cargo and `uv` run offline; no package download is
attempted.

## Steps

1. `demo.py` starts the real MCP stdio server with the repository as its
   workdir and sends `initialize`.
2. It calls `twin_start` with `schema.sql`.
3. `driver.py` uses psycopg2 parameter binding to insert one valid row.
4. `driver.py` attempts a negative amount. The CHECK violation must return
   SQLSTATE `23514`.
5. The script calls `twin_stop`, then `twin_report`. The report must show
   `outcome=READY`, one row in `public.deals`, and `check_violations=1`.

The schema and driver operations use the same table and statement forms covered
by the R05 and R07 compatibility runs. The final run output is printed by the
demo; MCP server diagnostics remain visible on stderr.

## Go-live checklist

- [ ] Record a GIF of the demo manually for release materials.
