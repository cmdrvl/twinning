# Twinning MCP Server: Build Spec (v0)

Companion to [PLAN_TWINNING_MCP.md](./PLAN_TWINNING_MCP.md), which holds the rationale, landscape and listing plan. This file holds what an implementing agent needs without making design decisions: scope fences, files, invariants, errors, tool contracts, tests, threats, gates and commands. Beads cite items here by ID (I##, E_*, U##, S##, R##, T##, G##, N##). If this file and the plan disagree, this file wins for implementation and the plan gets corrected.

Date: 2026-11-01. Status: ready for a one-shot implementation session after Zac's go.

## 1. Scope

v0 bar, in one sentence: an MCP client can start a Postgres twin from a DDL file, get a connection string that real drivers use, run statements from the supported subset, stop the twin, and read a report that shows what happened, installed by one binary download and one `setup` command on macOS and Linux.

In scope:

- `twinning-mcp` binary with six tools: `twin_start`, `twin_list`, `twin_stop`, `twin_report`, `twin_seed_contract`, `twin_doctor`. Every tool takes or implies `kind`; only `postgres` works.
- Subcommands: `twinning-mcp` (serve over stdio, default), `setup`, `doctor`.
- `setup` writes config for Claude Desktop and Cursor, and has `--print` for any other client.
- Unix only (macOS arm64 and x64, Linux x64 and arm64).
- TTL (default 60 minutes), twin cap (5), path confinement, stale-child sweep.
- twinning fixes found by the smoke test (plan section 2.1): SSL-denied handshake, per-connection error isolation, stale strings.
- Real-driver verification (psycopg2, SQLAlchemy) and a client-compat record.
- Release assets, npm launcher files, `server.json` and registry-auth file drafts, README.

Out of scope for v0 (do not build, do not stub in code):

- REST, MCP and Snowflake twin kinds. `twin_run_once`, `twin_query`, `twin_reset`, `twin_snapshot`, live report of a running twin, Receipts pointer, telemetry, hosted mode, Streamable HTTP transport, Windows, OCI image, Cline/VS Code/Codex `setup` targets, refactoring `twinning-rest` to share the new core crate.

Approval-gated, never done autonomously (each needs Zac's explicit yes in chat): pushing commits, tagging a release, publishing to npm, publishing to the MCP registry, directory submissions, deploying the website well-known file, installing a Python driver environment.

## 2. Design decisions already made

| # | Decision |
| --- | --- |
| D1 | Synchronous Rust with `std::thread`, `std::process`, `signal-hook` (already a dependency). No tokio in `twinning-mcp`. |
| D2 | `twinning-mcp-core` is created by copying the JSON-RPC types and `negotiate_initialize_protocol` from `crates/twinning-rest/src/mcp/{dispatcher,version}.rs` and the newline-framed stdio loop idea from `stdio.rs`, with their own tests. `twinning-rest` is not modified. De-duplication is a later bead. The existing `dispatch` is tied to `McpCatalog` and is not reusable as is. |
| D3 | The control server finds the `twinning` binary in this order: `--twinning-bin`, env `TWINNING_BIN`, a sibling of its own executable, then `PATH`. |
| D4 | Working directory: `--workdir` or env `TWINNING_MCP_WORKDIR`, else the launch directory. If the resolved workdir is `/` or the user's home directory itself and `--workdir` was not given explicitly, refuse to start with `E_WORKDIR_UNSAFE`. (Claude Desktop launches servers with cwd `/`.) `setup` always writes an explicit `--workdir`. |
| D5 | State directory: `<temp_dir>/twinning-mcp/<server_pid>/<twin_id>/` mode 0700, holding `report.json`, `trace.json`, `stderr.log`, `child.pid`. Callers never choose output paths. |
| D6 | Port: server binds `127.0.0.1:0`, reads the port, drops the socket, passes `--port N`. If the child exits within 1 s with an address-in-use message, retry up to 3 times. |
| D7 | Readiness: poll `TcpStream::connect` every 50 ms up to 10 s (env `TWINNING_MCP_READY_TIMEOUT_MS` overrides, for tests). The twin prints nothing while serving. |
| D8 | Stop: SIGTERM, wait up to 10 s (env override), then SIGKILL and `E_STOP_TIMEOUT`. Exit code 0 or 1 with a report file is a normal stop. Exit code 2 or no report file is `E_REPORT_UNREADABLE` with stderr tail. |
| D9 | `twin_report` works only for stopped twins. For a running twin it returns `E_REPORT_NOT_READY` with the hint to call `twin_stop` (twinning writes its report only at shutdown). Stopped records are kept for the server's lifetime, at most 50. |
| D10 | The connection string is `postgresql://u@127.0.0.1:<port>/db?sslmode=disable&gssencmode=disable`. No password, no secret. |
| D11 | Tool results are MCP `tools/call` results: `content: [{"type":"text","text":"<compact JSON>"}]`, plus `isError: true` and body `{"error":{"code","message","hint"}}` on failure. Protocol methods: `initialize`, `notifications/initialized`, `tools/list`, `tools/call`, `ping`. Server name `twinning-mcp`, version from Cargo. |
| D12 | `twin_start` passes `--query-trace <state>/trace.json` so the stop result can summarize request history. |
| D13 | Twin id: `tw_` plus 6 lowercase hex characters from `/dev/urandom`-seeded randomness, regenerated on collision. |

## 3. Module skeleton

New workspace members (add to root `Cargo.toml` `members`).

`crates/twinning-mcp-core` (library, no process logic):

| File | Responsibility |
| --- | --- |
| `src/lib.rs` | Re-exports |
| `src/jsonrpc.rs` | `JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`, parse, error constructors (copied, D2) |
| `src/version.rs` | `negotiate_initialize_protocol` and supported-version list (copied, D2) |
| `src/stdio.rs` | Read newline-delimited requests from a `BufRead`, write responses to a `Write`, flush per message; nothing else writes to stdout |

`crates/twinning-mcp` (binary `twinning-mcp`):

| File | Responsibility |
| --- | --- |
| `src/main.rs` | Argument parsing (clap), subcommand routing, signal setup, exit codes |
| `src/server.rs` | Stdio loop, method routing, `initialize`/`tools/list`/`tools/call`/`ping` |
| `src/tools.rs` | Tool descriptors (name, description, JSON schema, annotations) and argument decoding |
| `src/errors.rs` | `ToolError` with the codes in section 5 and conversion to the D11 error body |
| `src/registry.rs` | Twin records, id generation, cap, stopped-record eviction |
| `src/supervisor.rs` | Spawn, readiness, stop, TTL timer thread, shutdown-all, process-group handling |
| `src/argv.rs` | The only place that builds the `twinning` argument list from validated input (I07) |
| `src/paths.rs` | Workdir resolution, canonicalization, confinement, file-size limit |
| `src/locate.rs` | `twinning` binary resolution (D3) |
| `src/stale.rs` | Startup sweep of dead servers' state directories and orphan children |
| `src/report.rs` | Parse `twinning.v0` report, build the stop summary, truncate to 256 KiB |
| `src/setup.rs` | Client config detection and merge for `setup` |
| `src/doctor.rs` | Checks shared by `twin_doctor` and `twinning-mcp doctor` |
| `tests/` | `stub_integration.rs`, `e2e_real.rs`, `setup_configs.rs`, `golden/tools_list.json` |
| `tests/bin/fake_twinning.rs` | Test-only stub binary with modes `ok`, `exit_early`, `never_listen`, `ignore_sigterm`, `crash_on_stop` (env `FAKE_TWINNING_MODE`) |

Dependency direction: `twinning-mcp` depends on `twinning-mcp-core`; neither depends on `twinning-kernel` or any twin crate. The server talks to twinning only through the binary.

## 4. Invariants

| ID | Invariant |
| --- | --- |
| I01 | Stdout carries only JSON-RPC responses. Logs go to stderr. |
| I02 | Live twin count never exceeds the cap (default 5, env or flag to change). |
| I03 | On stdin EOF, SIGTERM or SIGINT, every child gets SIGTERM, then SIGKILL after 5 s; no child outlives a graceful exit. |
| I04 | Twin ids are unique within a server process. |
| I05 | The server only ever passes `--host 127.0.0.1`. |
| I06 | Every caller path is canonicalized (symlinks resolved) and must be inside the workdir; otherwise `E_PATH_OUTSIDE_WORKDIR`. |
| I07 | The `twinning` argument list is built in `argv.rs` from a fixed template. `--run`, `--materialize-source-url`, `--host`, `--report` and `--snapshot` values never come from callers. No shell is involved. |
| I08 | `kind` other than `postgres` is rejected before any process is spawned. |
| I09 | TTL expiry runs the same code path as `twin_stop` and retains the report. |
| I10 | The server makes no network calls other than connecting to its own twins on 127.0.0.1, and sends no telemetry. |
| I11 | `tools/list` is deterministic: sorted by name, every schema has `additionalProperties: false`. |
| I12 | `twin_start` returns a connection string only after a successful TCP connect to the port. |
| I13 | Input files larger than 5 MiB are refused with `E_ARGUMENT_INVALID`. |
| I14 | A server in `--restricted` mode refuses `seed`, `restore` and `verify` paths (only `schema` is accepted). |

## 5. Tool contracts and error taxonomy

Tools (all schemas `additionalProperties: false`):

| Tool | Input | Success body | Annotations |
| --- | --- | --- | --- |
| `twin_start` | `kind` (required, `postgres`), `schema_path` (required), `verify_path`, `seed_path`, `restore_path`, `ttl_minutes` (1 to 1440, default 60) | `{twin_id, kind, port, pid, connection_string, started_at, expires_at}` | not read-only, not idempotent |
| `twin_list` | none | `{twins:[{twin_id, kind, status, port, started_at, expires_at}]}`, status in `running`, `stopped`, `expired` | read-only |
| `twin_stop` | `twin_id` | `{twin_id, status, outcome, exit_code, constraints, tables, request_count, report_path, trace_path}` where `constraints` and `tables` come from the twinning report | destructive hint false |
| `twin_report` | `twin_id` | The full `twinning.v0` report JSON, plus `trace_summary`; truncated at 256 KiB with `truncated: true` and `report_path` | read-only |
| `twin_seed_contract` | `kind`, `schema_path` | `{contract_path, table_count}`; the file is written in the twin's state area via `--export-seed-contract` | not read-only |
| `twin_doctor` | none | `{twinning_bin, twinning_version, health, capabilities, workdir, cap, restricted}` | read-only |

Error codes (body `{"error":{"code","message","hint"}}`, `isError: true`):

| Code | When | Hint |
| --- | --- | --- |
| `E_ARGUMENT_INVALID` | Missing or wrong-type argument, unknown field, TTL out of range, file over 5 MiB, `--restricted` violation | Name the field |
| `E_KIND_UNSUPPORTED` | `kind` is not `postgres` | "Only postgres is available in v0" |
| `E_TWINNING_NOT_FOUND` | D3 search fails or the binary does not run `--describe` | Install twinning or set `TWINNING_BIN` |
| `E_WORKDIR_UNSAFE` | D4 | Pass `--workdir` |
| `E_PATH_NOT_FOUND` | A named file does not exist | |
| `E_PATH_OUTSIDE_WORKDIR` | I06 | Show the workdir |
| `E_TWIN_CAP_REACHED` | I02 | Stop a twin |
| `E_TWIN_NOT_FOUND` | Unknown `twin_id` | |
| `E_START_FAILED` | Child exited before readiness; include exit code, parsed refusal code if stdout holds a `twinning.v0` refusal, last 2 KiB of stderr | |
| `E_READY_TIMEOUT` | D7; the child is killed first | |
| `E_STOP_TIMEOUT` | D8 | |
| `E_REPORT_NOT_READY` | D9 | Call `twin_stop` |
| `E_REPORT_UNREADABLE` | D8 | |
| `E_SEED_CONTRACT_FAILED` | `--export-seed-contract` run failed; include stderr tail | |
| `E_INTERNAL` | Anything else; never include file contents | |

## 6. Sequence

| Step | Work | Depends on | Stop rule |
| --- | --- | --- | --- |
| M0a | Fix SSL-denied handshake so default-libpq clients connect; add raw-TCP regression test | none | |
| M0b | Isolate per-connection errors in the accept loop; regression test | none | |
| M0c | Real-driver run (psycopg2, SQLAlchemy) and constraint-report check; write `docs/CLIENT_COMPAT.md` | M0a, M0b, driver-env approval | If reports do not capture violations from real drivers, STOP and report to Zac. The differentiator premise fails. |
| M0d | Stale `wire_protocol` string, `AGENTS.md` and README server-mode wording | none | |
| M1 | `verify-core` and `verify-engine` as git dependencies pinned to a pushed `cmdrvl/verify` commit; fresh-clone build without the sibling checkout; simplify `release.yml` and `ci.yml` | none | If the pinned commit is not on GitHub or git fetch is denied, STOP and ask |
| M2 | Add `twinning-mcp-core` and `twinning-mcp` skeleton; JSON-RPC, `initialize`, `tools/list`, golden test | M1 | |
| M3 | Registry, supervisor, argv, paths, locate, stale sweep, fake-twinning stub | M2 | |
| M4 | The six tools and error taxonomy, stub integration tests S01 to S15 | M3 | |
| M5 | Real e2e R01 to R08 (needs M0 done) | M4, M0c | |
| M6 | `setup` and `doctor` subcommands, config tests | M2 | |
| M7 | README, `docs/demo/` script, `docs/CLIENT_COMPAT.md` linked, claims audit G8 | M5, M6 | |
| M8 | Release workflow assets and checksums, npm launcher files, `server.json` and `mcp-registry-auth` drafts, Homebrew formula bump text (nothing published) | M7 | |
| G-1..G-6 | Go-live gates, each needs Zac's explicit yes: push and tag, Homebrew formula update, npm publish, registry publish, website well-known file deploy, directory submissions | M8 | One approval per gate |

## 7. Test matrix

Unit (`cargo test -p twinning-mcp -p twinning-mcp-core`):

| ID | Case | Expected |
| --- | --- | --- |
| U01 | JSON-RPC request and response round trip, batch not supported | Parse OK; batch gives -32600 |
| U02 | Protocol negotiation with older, current, unknown client version | Server picks supported version |
| U03 | Each tool with missing, wrong-type and extra arguments | `E_ARGUMENT_INVALID` naming the field |
| U04 | Path `../x`, absolute outside, symlink pointing outside, path starting with `-` | `E_PATH_OUTSIDE_WORKDIR` or canonical absolute only |
| U05 | Workdir `/` and home without explicit flag | `E_WORKDIR_UNSAFE` |
| U06 | Argv builder over generated inputs | Never contains `--run`, `--materialize-source-url`, non-loopback host; caller strings only as values of `--schema`, `--verify`, `--seed`, `--restore` |
| U07 | TTL 0, 1, 1440, 1441, non-integer | 0 and 1441 rejected |
| U08 | Id format and 10 000 generated ids | Unique, `tw_` plus 6 hex |
| U09 | Registry cap at 5, eviction of stopped records beyond 50 | Matches I02, D9 |
| U10 | Connection string builder | Exactly the D10 form |
| U11 | `locate` order with env and flag set | D3 order |
| U12 | Report parser on READY, VIOLATIONS and REFUSAL fixtures | Summary fields; refusal becomes `E_REPORT_UNREADABLE` |
| U13 | Report over 256 KiB | Truncated with `truncated: true` |

Stub integration (`tests/stub_integration.rs`, uses `fake_twinning`):

| ID | Case | Expected |
| --- | --- | --- |
| S01 | start, list, stop, report happy path | Ready, DSN returned, report parsed |
| S02 | `exit_early` | `E_START_FAILED` with stderr tail |
| S03 | `never_listen` | `E_READY_TIMEOUT`, child gone |
| S04 | `ignore_sigterm` | `E_STOP_TIMEOUT`, child killed |
| S05 | Sixth start with cap 5 | `E_TWIN_CAP_REACHED` |
| S06 | TTL expiry (test hook sets seconds) | Twin stopped, report retained, status `expired` |
| S07 | Server stdin EOF | All child pids dead within 6 s |
| S08 | Server SIGTERM | Same |
| S09 | Server SIGKILL, then a new server starts | Stale sweep kills the orphan and cleans its directory |
| S10 | `twin_report` on a running twin | `E_REPORT_NOT_READY` |
| S11 | Unknown id | `E_TWIN_NOT_FOUND` |
| S12 | `kind: rest` | `E_KIND_UNSUPPORTED`, no process spawned |
| S13 | `tools/list` golden | Matches `golden/tools_list.json` |
| S14 | Stdout purity over a full session | Every stdout line is a JSON-RPC message |
| S15 | Three parallel `twin_start` | Distinct ports, all ready |

Real binary (`tests/e2e_real.rs`, needs `twinning` built; psql optional and skipped with a printed notice if absent; driver tests need the approved `uv` environment and are skipped with a loud notice otherwise, never silently passed):

| ID | Case | Expected |
| --- | --- | --- |
| R01 | Start, TCP ready, stop; report outcome READY | Matches D8 |
| R02 | psql with the returned connection string runs `begin` and `rollback` (psql_smoke shapes) | Succeeds |
| R03 | psql with default sslmode (no `sslmode=disable`) connects after M0a | Succeeds |
| R04 | A client that connects and drops mid-startup, then a good client connects | Server survives (M0b) |
| R05 | psycopg2: valid insert, CHECK violation, duplicate key | SQLSTATE 23514 and 23505 returned; report counts them, or STOP rule in M0c |
| R06 | SQLAlchemy Core insert and select by primary key | Succeeds |
| R07 | Full MCP session over stdio: initialize, tools/list, twin_start, driver inserts, twin_stop, twin_report | Report shows the violation |
| R08 | `twin_seed_contract` then `twin_start` with a filled seed | Rows present in stop summary |

Setup (`tests/setup_configs.rs`, temp HOME):

| ID | Case | Expected |
| --- | --- | --- |
| C01 | Claude Desktop config missing | Created with `mcpServers.twins` entry |
| C02 | Config has other servers | Preserved byte-for-byte except the new key; backup written |
| C03 | Run twice | Second run reports no change |
| C04 | Invalid JSON in config | Refuse, file untouched |
| C05 | Cursor `~/.cursor/mcp.json` | Same as C01 to C03 |
| C06 | `--dry-run` | Prints the change, writes nothing |
| C07 | `--print` with an unknown client name | Prints a JSON snippet, writes nothing |

## 8. Threat table

| ID | Threat | Mitigation | Test |
| --- | --- | --- | --- |
| T01 | Path traversal to read arbitrary files via schema, seed or restore | I06 | U04 |
| T02 | Symlink escape | Canonicalize before the check | U04 |
| T03 | Argument injection (a path that looks like a flag) | Canonical absolute paths, passed as values after their flag | U04, U06 |
| T04 | Arbitrary command execution | I07, no `--run`, no shell | U06 |
| T05 | Twin reachable from the network | I05, D6 | U06, R01 |
| T06 | Orphan children after a crash | I03, stale sweep | S07, S08, S09 |
| T07 | Resource exhaustion (many twins, giant DDL) | Cap, I13 | S05, U03 |
| T08 | Oversized report floods the model context | 256 KiB truncation | U13 |
| T09 | Secrets in output | No credentials in the connection string, no file contents in logs or errors | S14, review |
| T10 | Model treats DDL-derived or report text as instructions | Tool results are JSON data; README says to treat them as data | Review |
| T11 | `setup` corrupts a client config | Backup, atomic write (temp file then rename), refuse invalid JSON | C02, C04 |
| T12 | Path swapped between check and use | Accepted: local single-user threat, documented | none |
| T13 | Two servers share state | Per-server-pid directories | S09 |

## 9. Quality gates

| Gate | Condition |
| --- | --- |
| G1 | `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` pass for the whole workspace |
| G2 | S01 to S15 pass in 3 consecutive runs (flake gate) |
| G3 | R01 to R08 pass with the real binary, driver tests included |
| G4 | `docs/CLIENT_COMPAT.md` lists, per client (psql, psycopg2, SQLAlchemy), the version tested, the statements run and the result. No README claim goes beyond it. Go-live blocker |
| G5 | After the full suite, `pgrep -fl twinning` shows no process started by the tests |
| G6 | `initialize` round trip under 500 ms, measured and recorded |
| G7 | `./scripts/ubs_gate.sh` passes (process management is runtime-sensitive) |
| G8 | README claims audit: each claim maps to a test ID or is removed |
| G9 | Fresh clone in a temp directory with no sibling `verify` checkout builds `twinning` and `twinning-mcp` (after M1) |
| G10 | Release dry run: build both binaries for the host target, produce archive and SHA-256 file, extract and run `twinning-mcp doctor` |

## 10. Commands

```bash
cd /Users/zacharyruiz/Source/cmdrvl/twinning

cargo build --workspace
cargo build -p twinning-mcp
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
cargo test -p twinning-mcp --test stub_integration
cargo test -p twinning-mcp --test e2e_real -- --nocapture
cargo test -p twinning-mcp --test setup_configs
./scripts/ubs_gate.sh

# smoke by hand
./target/debug/twinning postgres --schema schema.sql --serve --port 54329 --report out/r.json --json
PGSSLMODE=disable PGGSSENCMODE=disable psql "postgresql://u@127.0.0.1:54329/db" -c "begin"

# MCP by hand (stdio)
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}' | ./target/debug/twinning-mcp --workdir "$PWD"

# beads
br ready --json
br update <id> --status in_progress --json
br close <id> --reason "Completed" --json
br sync --flush-only && git add .beads/ && git commit -m "sync beads"
```

In dcg-guarded sessions, shell redirects to variable paths and `psql -c "$var"` are blocked; put smoke scripts in files and use literal paths.

## 11. Non-negotiables

| ID | Rule |
| --- | --- |
| N01 | The server never runs a caller-supplied command and never exposes a shell. |
| N02 | Twins listen on 127.0.0.1 only. |
| N03 | No telemetry, no outbound network calls. |
| N04 | Nothing is published, tagged, pushed or installed from the network without Zac's explicit yes in chat. |
| N05 | The README and listing claim only what `docs/CLIENT_COMPAT.md` and the test matrix show. No "works with psycopg2" without G4. |
| N06 | Refusals and errors are explicit with codes; no silent fallback to partial behavior. |
| N07 | A failing test is reported as failing. A skipped test prints why. No test is weakened to pass. |
| N08 | No em dashes in authored docs or the README. |
| N09 | Beads implemented by an agent stay open for an independent verifier with an evidence comment; the agent does not close its own work as verified. |

## 12. Session kickoff prompt (one-shot)

```
Onboard: read AGENTS.md, CLAUDE.md, docs/PLAN_TWINNING_MCP.md and docs/PLAN_TWINNING_MCP_SPEC.md in the twinning repo. The spec wins on implementation details. Epic: see `br show` for the epic titled "Twinning MCP v0". Work beads in `br ready` order, M0a through M8. Use cass/ubs/testing-real-service-e2e-no-mocks where they fit.

Rules: no worktrees; br not bd; commit locally after each bead; do NOT push, tag, publish, or install network packages without Zac's explicit yes (spec N04). Ask once at the start for the throwaway `uv` driver environment (M0c). Stop rules in spec section 6 are binding: if M0c shows reports miss real-driver violations, stop and report. Leave implemented beads open for an independent verifier with an evidence comment (N09). Never-idle ladder when beads run out: re-run G1 to G10, then testing-* skills against the new crate, then the README claims audit (G8), then report. Done means: M0 to M8 beads have evidence comments, G1 to G10 recorded in docs/CLIENT_COMPAT.md or the epic, and a go-live checklist for Zac listing each approval-gated step.
```
