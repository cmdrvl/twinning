# Postgres twin-pair migration proof

`twin_pair_proof` compares two committed-state Postgres snapshots by replaying
the queries in a supplied file. The snapshots come from `twin_start` requests
with `snapshot_path`; `twin_stop` writes each requested snapshot. The tool reads
the versioned proof report produced by `twinning` and returns its `PASS` or
`FAIL` outcome with the case verdicts and mismatches. A `FAIL` proof can still
have process exit code 0, so callers must read the report outcome.

This is a declared-subset twin. It is not general Postgres compatibility.

## Recorded real run

The R4 live MCP run used `twinning 0.6.0`, `twinning-mcp 0.1.0`, and real
`psql (PostgreSQL) 14.24 (Homebrew)`. The MCP started two real Postgres twins
with the same schema and catalog declaration. Real `psql` inserted two tenant
rows and two deal rows into each twin. The legacy row named `deal-002` as
`Beta`; the candidate row named it `Beta migrated`. Both twins stopped with
`READY`, exit code 0, and wrote the requested snapshots. The MCP then ran
`twin_pair_proof` on those snapshot files and the bundled query list.

The recorded report outcome was `FAIL`:

| Case | Verdict | Mismatches |
| --- | --- | --- |
| `deal_lookup` | fail | `result_hash` |
| `outside_subset_lookup` | pass | none |
| `tenant_b_filtered_scan` | fail | `result_hash` |
| `tenant_b_deal_count` | pass | none |
| `join_shape_deferred` | skip | none |

The candidate change affected the point lookup and filtered scan that read the
changed row. The aggregate count stayed equal. The out-of-subset relation had
matching refusal parity. A separate real-snapshot refusal case recorded
SQLSTATE `42P01` on both sides. The identical-state run returned `PASS` for all
four non-skip cases and `skip` for the join case.

```text
proof outcome=FAIL
case_id=deal_lookup verdict=fail mismatches=result_hash
case_id=outside_subset_lookup verdict=pass mismatches=none
case_id=tenant_b_filtered_scan verdict=fail mismatches=result_hash
case_id=tenant_b_deal_count verdict=pass mismatches=none
case_id=join_shape_deferred verdict=skip mismatches=none
```

The test evidence is in the `PR01` through `PR04` events from
`cargo test -p twinning-mcp --test e2e_real pr -- --nocapture`. Each event log
records the `psql` version, commands, twin ports and PIDs, stop summaries,
proof outcome, case verdicts, report paths, and child process cleanup.

## Not supported or not tested

- Join execution is not supported by this proof slice. A join query is reported
  as `skip` and is not executed.
- Oracle/TNS protocol fidelity is deferred. The proof compares Postgres twin
  snapshots and does not connect to an Oracle service.
- Catalog reflection, `information_schema`, and queries beyond the supplied
  query file have not been proven by this run.
- Only the supplied point lookup, filtered scan, aggregate count, matching
  refusal, and join skip cases were exercised. This does not establish support
  for arbitrary SQL, sessions, or client startup behavior.
- The report compares committed snapshot state. It does not compare live
  database servers or certify that the candidate schema is equivalent for
  queries absent from the supplied file.

## Runnable demo

The demo starts real `twinning-mcp` and `twinning` binaries, creates both twins
through MCP, inserts rows with the local `psql`, stops the twins, then runs the
proof tool. Build release binaries first, then run:

```bash
cargo build --release -p twinning -p twinning-mcp
./docs/demo/migration/run.sh
```

The demo uses a temporary work directory and removes it at exit. Set
`TWINNING_PSQL_BIN` if `psql` is not on `PATH`. Set `TWINNING_BIN` or
`TWINNING_MCP_BIN` to select release binaries outside the repository's default
`target/release` directory.
