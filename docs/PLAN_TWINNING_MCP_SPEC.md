# Twinning MCP Manager: Initial Build Spec (v0)

Companion to [PLAN_TWINNING_MCP.md](./PLAN_TWINNING_MCP.md), which describes the manager's purpose and design context. This file records the initial Postgres manager design: scope, decisions, invariants, errors, tool contracts, tests, threats and quality gates. IDs in the tables provide stable cross references. If this file and the plan disagree on the initial manager design, this file is the implementation reference.

Date: 2026-11-01. This spec describes the initial Postgres manager baseline. The current manager kinds are `postgres` and `rest`; an MCP twin kind is not shipped. Current run evidence is recorded in [CLIENT_COMPAT.md](./CLIENT_COMPAT.md) and [MIGRATION_PROOF.md](./MIGRATION_PROOF.md).

## 1. Scope

v0 bar, in one sentence: an MCP client can start a Postgres twin from a DDL file, get a connection string that real drivers use, run statements from the supported subset, stop the twin, and read a report that shows what happened, installed by one binary download and one `setup` command on macOS and Linux.

The v0 bar is met only when each clause below is backed by a named test or recorded gate (this table is the acceptance definition; nothing else counts):

| Clause of the v0 bar | Verified by |
| --- | --- |
| Start a Postgres twin from a DDL file | S01 (stub), R01 (real) |
| Connection string real drivers use | R02, R03, R05, R06 (psql, psycopg2, SQLAlchemy), G4 |
| Run statements from the supported subset | R05, R06, R07 |
| Stop the twin | S01, R01, R04 |
| Report shows what happened | R05, R07 (violation counted in the report), R08 |
| One binary download plus one `setup` command | C01 to C08, G10 (archive extracts and runs `doctor`) |
| macOS and Linux | This spec validates the host target; other release targets are outside its test coverage |

Initial Postgres baseline:

- `twinning-mcp` binary with six lifecycle and inspection tools: `twin_start`, `twin_list`, `twin_stop`, `twin_report`, `twin_seed_contract`, `twin_doctor`. In this initial baseline, only `postgres` works.
- Subcommands: `twinning-mcp` (serve over stdio, default; a bare invocation on a terminal prints usage), `setup`, `doctor`, `capabilities`. CLI rules: section 5b.
- `setup` writes config for Claude Desktop and Cursor, and has `--print` for any other client.
- Unix only (macOS arm64 and x64, Linux x64 and arm64).
- TTL (default 60 minutes), twin cap (5), path confinement, stale-child sweep.
- twinning fixes found by the smoke test (plan section 2.1): SSL-denied handshake, per-connection error isolation, stale strings.
- Real-driver verification (psycopg2, SQLAlchemy) and a client-compat record.
- Release assets, npm launcher files, `registry/server.json` and registry-auth README drafts, README.

Extensions outside this initial baseline (not general twin kinds):

- MCP and Snowflake twin kinds. `twin_run_once`, `twin_query`, `twin_reset`, `twin_snapshot`, live report of a running twin, telemetry, hosted mode, Streamable HTTP transport, Windows, OCI image, Cline/VS Code/Codex `setup` targets, refactoring `twinning-rest` to share the new core crate.

These boundaries describe the original implementation plan. REST support and twin-pair proof were added later and are documented with their observed results in the current compatibility and migration records.

## 2. Design decisions already made

| # | Decision |
| --- | --- |
| D1 | Synchronous Rust with `std::thread`, `std::process`, `signal-hook` (already a dependency). No tokio in `twinning-mcp`. |
| D2 | `twinning-mcp-core` is created by copying the JSON-RPC types and `negotiate_initialize_protocol` from `crates/twinning-rest/src/mcp/{dispatcher,version}.rs` and the newline-framed stdio loop idea from `stdio.rs`, with their own tests. `twinning-rest` is not modified. De-duplication is deferred. The existing `dispatch` is tied to `McpCatalog` and is not reusable as is. |
| D3 | The control server finds the `twinning` binary in this order: `--twinning-bin`, env `TWINNING_BIN`, a sibling of its own executable (directory of `current_exe()` after canonicalizing; true for `target/debug`, the flat release archive, the npm cache and Homebrew `bin`), then `PATH`. A candidate counts only if `twinning --describe` exits 0 (verified: it prints `operator.v0` JSON with `name` and `version`, no engine subcommand needed). |
| D4 | Working directory: `--workdir` or env `TWINNING_MCP_WORKDIR`, else the launch directory. If the resolved workdir is `/` or the user's home directory itself and `--workdir` was not given explicitly, the server still starts (a hard exit shows the client only a generic failure) but every tool except `twin_doctor` returns `E_WORKDIR_UNSAFE`, and `twin_doctor` reports it. (Claude Desktop launches servers with cwd `/`.) `setup` always writes an explicit `--workdir`. |
| D5 | State directory: `<temp_dir>/twinning-mcp/<server_pid>/<twin_id>/` mode 0700, holding `report.json`, `trace.json`, `stdout.json`, `stderr.log`, `child.pid`. Child stdout is redirected to `stdout.json` (with `--json` the twin prints the final report there at shutdown, and prints a `twinning.v0` REFUSAL there on startup failures; stderr stays empty). Callers never choose output paths. |
| D6 | Port: server binds `127.0.0.1:0`, reads the port, drops the socket, passes `--port N`. If the child exits within 1 s and its `stdout.json` holds a REFUSAL with `detail.stage` = `listener_bind` and an `Address already in use` error (verified: it is on stdout, not stderr, exit 2), retry up to 3 times. |
| D7 | Readiness: poll `TcpStream::connect` every 50 ms up to 10 s (env `TWINNING_MCP_READY_TIMEOUT_MS` overrides, for tests). The twin prints nothing while serving. |
| D8 | Stop: SIGTERM, wait up to 10 s (env override), then SIGKILL and `E_STOP_TIMEOUT`. Exit code 0 or 1 with a report file is a normal stop. Exit code 2 or no report file is `E_REPORT_UNREADABLE` with stderr tail. |
| D9 | `twin_report` works only for stopped twins. For a running twin it returns `E_REPORT_NOT_READY` with the hint to call `twin_stop` (twinning writes its report only at shutdown). Stopped records are kept for the server's lifetime, at most 50. |
| D10 | The connection string is `postgresql://u@127.0.0.1:<port>/db?sslmode=disable&gssencmode=disable`. No password, no secret. |
| D11 | Tool results are MCP `tools/call` results: `content: [{"type":"text","text":"<compact JSON>"}]`, plus `isError: true` and body `{"error":{"code","message","hint"}}` on failure. Protocol methods: `initialize`, `notifications/initialized`, `tools/list`, `tools/call`, `ping`. Server name `twinning-mcp`, version from Cargo. |
| D12 | `twin_start` passes `--query-trace <state>/trace.json` so the stop result can summarize request history. The trace is written only at shutdown (like the report) as `{version: "twinning.query-trace.v0", policy, events: [...]}`; SQL text and bind values are redacted to hashes. `request_count` is `events.len()`. |
| D13 | Twin id: `tw_` plus 6 lowercase hex characters from `/dev/urandom`-seeded randomness, regenerated on collision. |
| D14 | `twin_stop` is idempotent: on a twin already `stopped` or `expired` it returns the retained stop summary unchanged and sends no signal. It is never `E_TWIN_NOT_FOUND` unless the record was evicted (D9). |
| D15 | `twin_start` validates in this order, first failure wins, and nothing is spawned before step 8: (1) argument shape and TTL range, `E_ARGUMENT_INVALID`; (2) `kind`, `E_KIND_UNSUPPORTED`; (3) workdir safety, `E_WORKDIR_UNSAFE`; (4) `--restricted` fields, `E_ARGUMENT_INVALID`; (5) each path: exists (`E_PATH_NOT_FOUND`), inside workdir (`E_PATH_OUTSIDE_WORKDIR`), size (`E_ARGUMENT_INVALID`); (6) `twinning` located, `E_TWINNING_NOT_FOUND`; (7) cap, `E_TWIN_CAP_REACHED`; (8) state directory creatable, else `E_INTERNAL`; then spawn. |
| D16 | Protocol errors are JSON-RPC errors; tool failures are `isError` results. Malformed JSON: -32700 with `id` null. Batch or non-object request: -32600. Unknown method: -32601. Unknown tool name, or `arguments` not an object: -32602. Notifications get no response. Anything that reaches a tool handler and fails is an `E_*` result, never a JSON-RPC error. |
| D17 | `twin_doctor` never fails because of what it diagnoses: a missing or non-running `twinning` gives a success body with `twinning_bin: null`, `twinning_version: null`, `health: "unavailable"`; an unsafe workdir is reported in `workdir`. It errors only for D16 protocol problems. |
| D18 | `twin_seed_contract` writes `seed-contract.jsonl` into a fresh `contract_<6 hex>` directory (mode 0700) under the server state directory, spawns no listener, and does not count against the cap. The sweep removes it with the rest of the server directory. |
| D19 | The `twinning-mcp` command line follows section 5b: usage on a bare terminal run, `--json` on `doctor` and `setup`, the exit-code dictionary, `capabilities`, did-you-mean hints, no ANSI ever. |

### 2a. Pinned implementation choices

| Topic | Choice |
| --- | --- |
| Manifests | Both new crates: version `0.1.0`, edition 2024, `license = "MIT"`, `publish = false`. Dependency ranges copy the root `Cargo.toml`: `clap = "4"` (derive), `serde = "1"` (derive), `serde_json = "1"`, `thiserror = "2"`, `signal-hook = "0.3"`, `chrono = "0.4"`. Also `libc = "0.2"` (only `kill`/`killpg`). Dev: `tempfile = "3"`, `sha2 = "0.10"`. All already in `Cargo.lock`. |
| Windows | `#[cfg(not(unix))] compile_error!("twinning-mcp supports Unix only")` in `main.rs`. |
| Stub binary | `[[bin]] name = "fake_twinning" path = "tests/bin/fake_twinning.rs"`; tests locate it with `env!("CARGO_BIN_EXE_fake_twinning")`. |
| Finding `twinning` in tests | Env `TWINNING_BIN`, else `$CARGO_TARGET_DIR/debug/twinning`, else `target/debug/twinning`; build it first with `cargo build -p twinning` under the same `CARGO_TARGET_DIR`. Never hard-code `target/debug`. |
| Test layout | U tests are `#[cfg(test)]` modules beside the code, except U14 and U16 to U19, which are integration tests in `crates/twinning-mcp/tests/cli.rs` (U16 to U19) and `tests/audit.rs` (U14). S, R and C tests are in the files of section 3. Name each test function after its ID (`fn u04_path_traversal()`), so `cargo test u04` selects it. |
| Fixtures | U12 fixtures (`report_ready.json`, `report_violations.json`, `refusal.json`) are captured once from real `twinning` runs into `crates/twinning-mcp/tests/fixtures/`, never hand-written. The `fake_twinning` report is a copy of `report_ready.json` with the port and paths substituted. |
| Golden files | `golden/tools_list.json` and `golden/capabilities.json` are captured from server output after a read-through, then edited by hand with a reason. A test never rewrites a golden. |
| Error strings | `message` is one sentence, `<what failed>: <detail>`. `hint` is per the section 5 table and names the field or the exact command. Tests assert the code and the named field, not full message text. |
| Ids and process groups | Twin ids read 3 bytes from `/dev/urandom` (no `rand` crate). Children start with `CommandExt::process_group(0)`; signals use `libc::kill` and `libc::killpg`. |
| Env names | `TWINNING_MCP_MAX_TWINS` (default 5, flag `--max-twins`), `TWINNING_MCP_RESTRICTED=1` (flag `--restricted`), `TWINNING_MCP_WORKDIR`, `TWINNING_BIN`, and the test-only timeout overrides. |
| Probe timeouts | Each `twinning --describe` or `doctor` probe made by the server has a 5 s timeout; a timeout counts as the probe failing. |
| Real-driver harness | `tests/compat/driver_cases.py` runs each driver case and prints one JSON line per case (case, SQLSTATE, rows); `tests/compat_drivers.rs` starts `twinning`, runs the cases, sends SIGTERM and checks the report. |
| `setup` text | Backup file `<config>.bak-<UTC yyyymmddThhmmssZ>` in the same directory. Second run prints exactly `no change`. Success ends with `Restart <client> to load the server.` |

## 3. Module skeleton

New workspace members (add to root `Cargo.toml` `members`).

`crates/twinning-mcp-core` (library, no process logic):

| File | Responsibility |
| --- | --- |
| `src/lib.rs` | Re-exports |
| `src/jsonrpc.rs` | `JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`, parse, error constructors (copied, D2) |
| `src/version.rs` | `negotiate_initialize_protocol` and supported-version list (copied, D2; the source list is `2024-11-05`, `2025-03-26`, and unknown client versions fall back to the last entry; add newer versions only deliberately, with a test) |
| `src/stdio.rs` | Read newline-delimited requests from a `BufRead`, write responses to a `Write`, flush per message; nothing else writes to stdout |

`crates/twinning-mcp` (binary `twinning-mcp`):

| File | Responsibility |
| --- | --- |
| `src/main.rs` | Argument parsing (clap), subcommand routing, signal setup, exit codes (section 5b) |
| `src/cli.rs` | Usage text, `capabilities` output, did-you-mean text, exit-code constants (section 5b) |
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
| `src/log.rs` | Server stderr logger in the format of section 7b |
| `src/doctor.rs` | Checks shared by `twin_doctor` and `twinning-mcp doctor` |
| `tests/` | `cli.rs`, `audit.rs`, `stub_integration.rs`, `e2e_real.rs`, `setup_configs.rs`, `golden/tools_list.json`, `golden/capabilities.json`, `common/mod.rs` (shared helpers of section 7b) |
| `tests/bin/fake_twinning.rs` | Test-only stub binary with modes `ok`, `exit_early`, `never_listen`, `ignore_sigterm`, `crash_on_stop` (exit 2, no report), `exit1_with_report`, `listen_late` (listens after `FAKE_TWINNING_LISTEN_DELAY_MS`), `addr_in_use_once` (first spawn writes a `listener_bind` REFUSAL to stdout and exits 2; a counter file in the state dir makes the next spawn succeed), `contract_fail` (env `FAKE_TWINNING_MODE`). In `ok` mode it also writes a valid `trace.json` and, for `--export-seed-contract`, a contract file |

Dependency direction: `twinning-mcp` depends on `twinning-mcp-core`; neither depends on `twinning-kernel` or any twin crate. The server talks to twinning only through the binary. Allowed dependencies: `serde`, `serde_json`, `clap`, `thiserror`, `signal-hook`, `chrono` (log timestamps; already in the root package), and `libc` (`kill` and `killpg` only). Dev-dependencies: `tempfile`, `sha2`. Exact ranges: section 2a.

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
| I08 | In the initial Postgres baseline, `kind` other than `postgres` is rejected before any process is spawned. |
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
| `twin_stop` | `twin_id` | `{twin_id, status, outcome, exit_code, constraints, tables, request_count, report_path, trace_path}` where `constraints` (keys `not_null_violations`, `fk_violations`, `check_violations`, `unique_violations`) and `tables` (object keyed by table name, each with `rows`, `columns`, `indexes`, `constraints`) come from the twinning report | destructive hint false |
| `twin_report` | `twin_id` | The full `twinning.v0` report JSON, plus `trace_summary`; truncated at 256 KiB with `truncated: true` and `report_path` | read-only |
| `twin_seed_contract` | `kind`, `schema_path` | `{contract_path, table_count}`; the file is written in the twin's state area via `--export-seed-contract` | not read-only |
| `twin_doctor` | none | `{twinning_bin, twinning_version, health, capabilities, workdir, cap, restricted}`; `twinning_version` is `version` from `--describe`, `health` is the `status` and `outcome` of `doctor health --json`, `capabilities` is the `commands` list of `doctor capabilities --json` (both verified to exit 0 and print `twinning.v0` JSON) | read-only |

Error codes (body `{"error":{"code","message","hint"}}`, `isError: true`):

| Code | When | Hint | Raised in |
| --- | --- | --- | --- |
| `E_ARGUMENT_INVALID` | Missing or wrong-type argument, unknown field, TTL out of range, file over 5 MiB, `--restricted` violation | Name the field | tools.rs |
| `E_KIND_UNSUPPORTED` | `kind` is not `postgres` | "Only postgres is available in v0" | tools.rs |
| `E_TWINNING_NOT_FOUND` | D3 search fails or the binary does not run `--describe` | Install twinning or set `TWINNING_BIN` | locate.rs |
| `E_WORKDIR_UNSAFE` | D4 | Pass `--workdir` | paths.rs |
| `E_PATH_NOT_FOUND` | A named file does not exist | Check the path; relative paths resolve against the workdir | paths.rs |
| `E_PATH_OUTSIDE_WORKDIR` | I06 | Show the workdir | paths.rs |
| `E_TWIN_CAP_REACHED` | I02 | Call `twin_list`, then `twin_stop` on one twin | registry.rs |
| `E_TWIN_NOT_FOUND` | Unknown `twin_id` | Call `twin_list` for valid ids | registry.rs |
| `E_START_FAILED` | Child exited before readiness; include exit code, parsed refusal code if stdout holds a `twinning.v0` refusal, last 2 KiB of stderr | Read `stderr_tail`; run `twinning-mcp doctor` | supervisor.rs |
| `E_READY_TIMEOUT` | D7; the child is killed first | Retry `twin_start`; raise `TWINNING_MCP_READY_TIMEOUT_MS` | supervisor.rs |
| `E_STOP_TIMEOUT` | D8 | The child was killed; call `twin_start` again | supervisor.rs |
| `E_REPORT_NOT_READY` | D9 | Call `twin_stop` | report.rs |
| `E_REPORT_UNREADABLE` | D8 | Read `stderr_tail`; start a new twin | report.rs, supervisor.rs |
| `E_SEED_CONTRACT_FAILED` | `--export-seed-contract` run failed; include stderr tail | Check `schema_path`; run `twinning-mcp doctor` | supervisor.rs |
| `E_INTERNAL` | Anything else; never include file contents | Run `twinning-mcp doctor` | any (catch-all) |

Every `hint` is non-empty and names the field or the exact command to run next; S18 asserts it.

JSON-RPC protocol errors (-32700, -32600, -32601, -32602) follow D16 and are raised in `server.rs`; they are not `E_*` codes.

Tool behavior contracts (each is checkable; the tests cite them):

| ID | Contract | Tests |
| --- | --- | --- |
| K01 | `twin_start` success returns exactly the fields of section 5, and the connection string only after a successful TCP connect (I12, D10) | S01, S03, U10 |
| K02 | `twin_start` validates in the D15 order; the first failure wins | U03, U05, S12, S18 |
| K03 | No validation failure spawns a process | S12, S18, S20 |
| K04 | `twin_list` returns every retained record in start order; status is `running` until stop, `stopped` after `twin_stop`, `expired` after TTL (I09) | S01, S06 |
| K05 | `twin_stop` on a running twin signals, waits (D8) and returns the summary; on a `stopped` or `expired` twin it returns the retained summary with no signal (D14) | S01, S06, S21 |
| K06 | Exit code 0 or 1 with a readable report is a normal stop; anything else is `E_REPORT_UNREADABLE` (D8) | S17, U12 |
| K07 | `twin_report` works only after stop (D9) and truncates at 256 KiB with `truncated: true` | S10, U13 |
| K08 | `twin_seed_contract` follows D18 and returns `{contract_path, table_count}`; a failure is `E_SEED_CONTRACT_FAILED` | S19 |
| K09 | `twin_doctor` follows D17 and works when twinning is missing or the workdir is unsafe | S21, U05 |
| K10 | Tool failures are `isError` results with the section 5 body; protocol failures are JSON-RPC errors (D16) | U01, S21 |
| K11 | Shutdown (stdin EOF, SIGTERM, SIGINT) and TTL use the same stop path as `twin_stop` (I03, I09) | S06, S07, S08 |
| K12 | Unknown or extra argument fields are rejected with the field named | U03 |

## 5b. Command-line surface (D19)

CLI behavior includes usage, `capabilities`, typo hints, ANSI handling, exit codes, `doctor`, `setup`, and `--json` output.

- Bare `twinning-mcp` with stdin a terminal prints usage to stdout and exits 0: each subcommand in one line, then `next: twinning-mcp setup` and `next: twinning-mcp doctor`. With stdin a pipe or file it serves. Test hook: env `TWINNING_MCP_ASSUME_TTY=1` forces the terminal branch.
- `doctor --json` and `setup --json` print one JSON object on stdout and nothing else; diagnostics, progress and warnings go to stderr. `doctor --json` is the `twin_doctor` body plus `checks: [{name, ok, detail, next}]`. `setup --json` is `{client, config_path, action, backup_path, restart_required}` with `action` in `created`, `updated`, `no_change`, `dry_run`, `printed`. Without `--json` the same facts are printed as plain text.
- `capabilities --json` (also the default output of `capabilities`) prints `{name, version, protocol_versions, kinds, subcommands, tools, error_codes, exit_codes}`; `tools` and `error_codes` come from the same tables the server uses, so they cannot drift. Pinned by `golden/capabilities.json` (the `version` field is compared with `CARGO_PKG_VERSION`, not the golden).
- Exit codes (CLI only; the stdio server exits 0 after EOF or a signal):

| Code | Meaning | Examples |
| --- | --- | --- |
| 0 | OK | success, `no change`, usage on a terminal, help |
| 1 | User input error | unknown flag or subcommand, missing value, bad `--client` for a file write |
| 2 | Safety block | unsafe default workdir for `setup`, config file is not valid JSON, `/` or home as workdir |
| 3 | Environment error | `twinning` not found or not running (`doctor`), cannot read or write the config directory |
| 4 | Upstream or twin failure | `twinning doctor health` reports failure, `twinning --describe` exits non-zero |

- Clap usage errors are mapped to exit 1 (clap's own default is 2); `--help` and `--version` exit 0. A mistyped subcommand or flag prints one line to stderr naming the exact corrected command, for example `error: unknown subcommand 'setpu'. Did you mean: twinning-mcp setup`, built from clap's suggestion (`ContextKind::SuggestedSubcommand`, `SuggestedArg`) plus this text; without a suggestion it prints `next: twinning-mcp --help`.
- Output is never colored: clap `ColorChoice::Never`, and neither the server nor the CLI writes any ESC byte, so `NO_COLOR` and non-TTY need no special case.
- Every CLI error ends with a `next:` line holding the exact command to run (`next: twinning-mcp doctor`, `next: twinning-mcp setup --dry-run`, `next: set TWINNING_BIN or pass --twinning-bin <path>`). Tool errors carry the same through `hint` (section 5).

## 6. Implementation overview

The manager validates requests before spawning a local twin process. It owns
the child lifecycle, readiness checks, time limits, shutdown, and report
parsing. The twin binary owns protocol handling and SQL or route behavior.
The test matrix below covers JSON-RPC behavior, process lifecycle, real client
traffic, and client configuration.

The tables in this spec describe the initial Postgres manager baseline. Later
REST and twin-pair proof support has separate observed evidence in
`docs/CLIENT_COMPAT.md` and `docs/MIGRATION_PROOF.md`.

## 7. Test matrix

Unit (`cargo test -p twinning-mcp -p twinning-mcp-core`):

| ID | Case | Expected |
| --- | --- | --- |
| U01 | JSON-RPC request and response round trip, malformed JSON, batch not supported, unknown method, notification, `ping` (D16) | Parse OK; malformed JSON -32700 with null id; batch -32600; unknown method -32601; notification gets no response; ping returns `{}` |
| U02 | Protocol negotiation with older, current, unknown client version | Server picks supported version |
| U03 | Each tool with missing, wrong-type and extra arguments; input file of exactly 5 MiB and of 5 MiB plus 1 byte (I13) | `E_ARGUMENT_INVALID` naming the field; 5 MiB accepted, +1 refused |
| U04 | Path `../x`, absolute outside, symlink pointing outside, path starting with `-` | `E_PATH_OUTSIDE_WORKDIR` or canonical absolute only |
| U05 | Workdir `/` and home without explicit flag; same paths with explicit `--workdir` | `E_WORKDIR_UNSAFE` (a running server answers every tool except `twin_doctor` with it, and `twin_doctor` reports it); explicit flag accepted |
| U06 | Argv builder over generated inputs | Never contains `--run`, `--materialize-source-url`, non-loopback host; caller strings only as values of `--schema`, `--verify`, `--seed`, `--restore` |
| U07 | TTL 0, 1, 1440, 1441, non-integer | 0 and 1441 rejected |
| U08 | Id format and 10 000 generated ids | Unique, `tw_` plus 6 hex |
| U09 | Registry cap at 5, eviction of stopped records beyond 50 | Matches I02, D9 |
| U10 | Connection string builder | Exactly the D10 form |
| U11 | `locate` order with env and flag set; a candidate whose `--describe` fails is skipped; none valid | D3 order; `E_TWINNING_NOT_FOUND` |
| U12 | Report parser on READY, VIOLATIONS and REFUSAL fixtures | Summary fields; refusal becomes `E_REPORT_UNREADABLE` |
| U13 | Report over 256 KiB | Truncated with `truncated: true` |
| U14 | Static audit: `cargo metadata` for `twinning-mcp` and `twinning-mcp-core` lists no async runtime or HTTP client (`tokio`, `reqwest`, `hyper`, `ureq`, `curl`, `isahc`); source grep finds `TcpStream::connect` and `TcpListener::bind` only with `127.0.0.1` (D1, I05, I10, N03) | No match outside the allowed set |
| U15 | `--restricted`: `seed_path`, `restore_path`, `verify_path` refused, `schema_path` accepted (I14) | `E_ARGUMENT_INVALID` naming the field |
| U16 | Bare `twinning-mcp` with `TWINNING_MCP_ASSUME_TTY=1` and with stdin a pipe carrying an `initialize` line (section 5b) | Terminal: usage on stdout containing `next: twinning-mcp setup`, exit 0, does not block. Pipe: serves the response |
| U17 | `twinning-mcp capabilities --json` against `golden/capabilities.json`; `tools` equals the `tools/list` names, `error_codes` equals the section 5 list, `exit_codes` equals the section 5b table, `version` equals `CARGO_PKG_VERSION` | Equal; stdout is one JSON object |
| U18 | Typos: `setpu`, `docter`, `setup --dryrun`, `--workdr X`, and an unknown flag with no close match | Exit 1; stderr names the corrected full command (`twinning-mcp setup`, `twinning-mcp setup --dry-run`, `twinning-mcp --workdir`); no-match case prints `next: twinning-mcp --help` |
| U19 | Run serve (one `initialize`), `--help`, `capabilities`, a typo error and usage with `NO_COLOR` unset, set, and stdout a pipe | No ESC byte (0x1b) in any stdout or stderr |

Stub integration (`tests/stub_integration.rs`, uses `fake_twinning`):

| ID | Case | Expected |
| --- | --- | --- |
| S01 | start, list, stop, report happy path, run with `listen_late` | Ready, DSN returned, report parsed; DSN is returned only after the port accepts, and a probe connect right after the result succeeds (I12); state dir mode 0700 with `child.pid`, `stdout.json`, `stderr.log` present (D5); `request_count` equals `events` length in the stub trace (D12) |
| S02 | `exit_early` | `E_START_FAILED` with stderr tail |
| S03 | `never_listen` | `E_READY_TIMEOUT`, child gone, no connection string in any output (I12, D7) |
| S04 | `ignore_sigterm` | `E_STOP_TIMEOUT`, child killed |
| S05 | Sixth start with cap 5 | `E_TWIN_CAP_REACHED` |
| S06 | TTL expiry (test hook sets seconds) | Twin stopped, report retained, status `expired` |
| S07 | Server stdin EOF | All child pids dead within 6 s |
| S08 | Server SIGTERM | Same |
| S09 | Server SIGKILL, then a new server starts; also a second server started while the first is alive | Sweep kills the orphan and cleans its directory; the live server's twins and directory are untouched (T13) |
| S10 | `twin_report` on a running twin | `E_REPORT_NOT_READY` |
| S11 | Unknown id | `E_TWIN_NOT_FOUND` |
| S12 | `kind: rest` | `E_KIND_UNSUPPORTED`, no process spawned |
| S13 | `tools/list` golden | Matches `golden/tools_list.json`; also asserts sorted by name and `additionalProperties: false` on every schema (I11) |
| S14 | Stdout purity and secrets over a full session; the DDL contains the marker `SECRET_MARKER_7f3a` | Every stdout line is a JSON-RPC message (I01); every stderr line matches the section 7b format; the marker appears in no response, error or stderr line; DSN has no password (T09) |
| S15 | Three parallel `twin_start` | Distinct ports, all ready |
| S16 | `addr_in_use_once`, then a stub that always reports address in use | Second spawn succeeds (D6); after 3 retries `E_START_FAILED` carrying the refusal code |
| S17 | `crash_on_stop`; `exit1_with_report` | `E_REPORT_UNREADABLE` with stderr tail (D8); exit 1 with a report is a normal stop |
| S18 | Every error body has a non-empty `hint` (section 5). Pre-spawn errors: bad `--twinning-bin`, missing schema file, schema outside workdir, workdir `/` without flag, state dir unwritable (`TMPDIR` read-only) | `E_TWINNING_NOT_FOUND`, `E_PATH_NOT_FOUND`, `E_PATH_OUTSIDE_WORKDIR`, `E_WORKDIR_UNSAFE`, `E_INTERNAL` with no file contents; no process spawned |
| S19 | `twin_seed_contract` with `ok`; with `contract_fail` | `{contract_path, table_count}`; `E_SEED_CONTRACT_FAILED` with stderr tail |
| S20 | Server started with `--restricted`: `twin_start` with `seed_path` | `E_ARGUMENT_INVALID`, no process spawned (I14) |
| S21 | `twin_stop` twice on one twin, and on an expired twin; `tools/call` with an unknown tool name; `arguments` as an array; `twin_doctor` with a bad `--twinning-bin` and with workdir `/` | Second stop returns the same summary and sends no signal (D14); -32602 for the unknown tool and for non-object arguments (D16); `twin_doctor` succeeds with `twinning_bin: null` and `health: "unavailable"`, and reports the unsafe workdir (D17) |

Real binary (`tests/e2e_real.rs`, needs `twinning` built; psql optional and skipped with a printed notice if absent; driver tests use the approved `uv` command form of section 1 and are skipped with a loud notice only if `uv` is absent, never silently passed):

| ID | Case | Expected |
| --- | --- | --- |
| R01 | Start, TCP ready, stop; report outcome READY; the port refuses a connect to the host's non-loopback address (skip with reason if the host has none) | Matches D8; listener is loopback only (I05, N02, T05) |
| R02 | psql with the returned connection string runs `begin` and `rollback` (psql_smoke shapes) | Succeeds |
| R03 | psql with default sslmode (without `sslmode=disable`) connects | Succeeds |
| R04 | A client that connects and drops mid-startup, then a good client; a client that runs `begin` and disconnects, then another `begin`; then `twin_stop` | Server survives; no 55P03; stop exits 0 with a report, not exit 2 |
| R05 | psycopg2: valid insert, CHECK violation, duplicate key | SQLSTATE 23514 and 23505 returned; the report counts the violations |
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
| C08 | Write into a read-only config directory; successful write | Original untouched, no temp file left; on success the write is temp-then-rename and the file mode is kept (T11) |
| C09 | `doctor --json` and `setup --json --dry-run` (also created, updated, `--print`) with stdout and stderr captured separately | stdout parses as one JSON object with the section 5b keys; diagnostics only on stderr; plain-text mode prints the same facts |
| C10 | Exit codes: unknown flag (1); invalid-JSON config and `setup` with workdir `/` (2); `doctor` with `--twinning-bin` pointing nowhere (3); `doctor` against a script in the temp dir that answers `--describe` and exits 1 on `doctor health` (4); success (0). Each failure prints a `next:` line | Codes match the section 5b table (the script is created by the test; `fake_twinning.rs` is not edited) |

## 7a. Coverage map

Every item below has an automated test unless marked review.

| Items | Test |
| --- | --- |
| I01 | S14 |
| I02, I04, D13, D9 (cap, ids, eviction) | U08, U09, S05 |
| D14, D16, D17, K05, K09, K10 | S21, U01 |
| D15, K01 to K04, K06 to K08, K11, K12 | U03, U05, S01, S06, S12, S17, S18, S19, U13 (as listed in section 5) |
| D18 | S19 |
| I03, T06 | S07, S08, S09 |
| I05, N02, T05, N01, T04, T03 | U06, R01 |
| I06, T01, T02 | U04, S18 |
| I07, I08 | U06, S12 |
| I09 | S06 |
| I10, N03, D1 | U14 |
| I11 | S13 |
| D19, section 5b (usage, capabilities, typo hints, no ANSI, exit codes, `--json`) | U16 to U19, C09, C10, S18 (hints) |
| I12, D7 | S01, S03 |
| I13, T07 | U03, S05 |
| I14 | U15, S20 |
| D2 | U01, U02 |
| D3 | U11, S18 |
| D4 | U05, S18 |
| D5, D12 | S01 |
| D6 | S16 |
| D8 | S04, S17, U12 |
| D10 | U10 |
| D11 | S01, S13, S14 |
| E_ARGUMENT_INVALID, E_KIND_UNSUPPORTED | U03, S12 |
| E_TWINNING_NOT_FOUND, E_WORKDIR_UNSAFE, E_PATH_NOT_FOUND, E_PATH_OUTSIDE_WORKDIR, E_INTERNAL | S18 |
| E_TWIN_CAP_REACHED, E_TWIN_NOT_FOUND, E_START_FAILED, E_READY_TIMEOUT, E_STOP_TIMEOUT | S05, S11, S02 and S16, S03, S04 |
| E_REPORT_NOT_READY, E_REPORT_UNREADABLE, E_SEED_CONTRACT_FAILED | S10, S17 and U12, S19 |
| T08 | U13 |
| T09 | S14, S18 |
| T10 | Review only (README treats tool output as data) |
| T11 | C02, C04, C08 |
| T12 | Accepted risk, no test |
| T13 | S09 |
| N05, N06 | G8 claims audit; E_* tests |
| N07 | Section 7b skip rule |
| N08 | G8 grep |
| G1 to G10 | Conditions in section 9 |

## 7b. Test logging, flake and skip discipline

Applies to every test in section 7. The raw-TCP regression tests also log the command line, port, bytes sent and received, and exit code.

**Where.** Each test binary creates `<temp_dir>/twinning-mcp-tests/<binary>-<pid>-<unix_ms>/` once (helper in `tests/common/mod.rs`) and prints `ARTIFACTS: <absolute path>` as its first output line. Per test it keeps `<ID>.log` (the same lines as stdout), plus copies of each twin's `report.json`, `trace.json`, `stdout.json`, `stderr.log` and the server's stderr. All lines go to stdout under `--nocapture`, prefixed `[<ID>]` (for example `[S04]`) so parallel output stays attributable. Artifacts are never deleted by the tests.

**What each class logs.**

| Class | Log lines (one per event, key=value) |
| --- | --- |
| U | The input and the expected/actual pair on mismatch; for generated inputs, the seed |
| S | Server command line and pid; fake mode and env overrides; each JSON-RPC request id and method, and the error code or result keys of the response; child pids and ports; elapsed ms of every poll; tail (last 20 lines) of server stderr on failure; for kill checks, the pid and the `kill -0` result |
| R | All of S, plus the `twinning` command line and port, the psql/driver command line and version, each SQLSTATE observed, the stop summary excerpt (`outcome`, `constraints`, `request_count`), and on failure the last 2 KiB of the twin's `stdout.json` and `stderr.log` |
| C | Temp HOME path, config file before and after (sizes and sha256, not contents), the CLI stdout |

Never log file contents of caller-supplied DDL, seed or config beyond sizes and hashes.

**Server stderr format (all binaries, section 4 I01).** One line per event: `<RFC3339 UTC with ms, via the chrono crate the root package already uses> <LEVEL> <twin_id or -> <event> key=value ...`, for example `2026-11-01T14:51:17.479Z INFO tw_a1b2c3 child_spawned pid=4242 port=54329`. Levels `INFO`, `WARN`, `ERROR`. Events at least: `server_start`, `workdir`, `twin_started`, `ready`, `stop_requested`, `child_exited` (exit code), `ttl_expired`, `sweep_killed`, `shutdown_all`, `tool_error` (code only). Values contain no newlines, no file contents, no secrets.

**No fixed sleeps.** Waiting on a process or file uses `poll_until(deadline, 20 ms, what)`: it logs elapsed time on success and the last observation on timeout, and panics with both. Deadlines scale with env `TWINNING_TEST_TIMEOUT_SCALE` (default 1). A bare `thread::sleep` outside the poll loop is a review failure. Asserts on absence (child gone) poll to the deadline, then assert.

**Ports.** Never hard-coded: bind `127.0.0.1:0`, read the port, release it (the D6 retry covers the race).

**Cleanup on panic.** Every spawned process (server, twin, psql, driver) sits in a drop guard that sends SIGKILL to its process group and waits; twins started by a server are also killed from the `child.pid` files in its state dir. Guards run on panic and on timeout.

**Skips.** A skip prints a line starting `SKIPPED: <ID>: <reason>` and appends it to `skips.txt` in the artifacts directory; it never panics and never returns silently. A skipped test does not satisfy a gate that requires that test to pass.

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
| T09 | Secrets in output | No credentials in the connection string, no file contents in logs or errors | S14, S18 |
| T10 | Model treats DDL-derived or report text as instructions | Tool results are JSON data; README says to treat them as data | Review only |
| T11 | `setup` corrupts a client config | Backup, atomic write (temp file then rename), refuse invalid JSON | C02, C04, C08 |
| T12 | Path swapped between check and use | Accepted: local single-user threat, documented | none |
| T13 | Two servers share state | Per-server-pid directories | S09 (second server leaves the first alone) |

## 9. Quality gates

| Gate | Condition |
| --- | --- |
| G1 | `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --workspace` exit 0 for the whole workspace |
| G2 | S01 to S21 pass in 3 consecutive runs |
| G3 | R01 to R08 pass with the real binary, driver tests included |
| G4 | `docs/CLIENT_COMPAT.md` lists, per client (psql, psycopg2, SQLAlchemy), the version tested, the statements run and the result. Every README claim that names a client, driver or version has a matching row in that file |
| G5 | After the full suite (S and R), `pgrep -fl 'twinning|fake_twinning'` compared with a before-snapshot shows no process started by the tests |
| G6 | `initialize` round trip under 500 ms, measured from process spawn to first response line in three runs; use the worst value |
| G7 | `./scripts/ubs_gate.sh` passes (process management is runtime-sensitive) |
| G8 | README claims map to test IDs or recorded results. New README lines contain no em dash. Tool output is presented as data (T10) |
| G9 | A fresh clone with no sibling `verify` checkout builds `twinning` and `twinning-mcp` |
| G10 | Release dry run: build both binaries for the host target, produce archive and SHA-256 file, extract and run `twinning-mcp doctor` |

## 10. Non-negotiables

These are the contract, not aspirations. If any is violated, twinning-mcp is not shippable and the work is not done, whatever else passes. "Verified by" names the check that fails when the rule is broken.

| ID | Rule | Verified by |
| --- | --- | --- |
| N01 | The server never runs a caller-supplied command and never exposes a shell. | U06, I07 |
| N02 | Twins listen on 127.0.0.1 only. | U06, R01 |
| N03 | No telemetry, no outbound network calls. | U14 |
| N05 | The README and listing claim only what `docs/CLIENT_COMPAT.md` and the test matrix show. No "works with psycopg2" without G4. | G8 claims table |
| N06 | Refusals and errors are explicit with codes; no silent fallback to partial behavior. | E_* tests in 7a, S21 |
| N07 | A failing test is reported as failing. A skipped test prints why. No test is weakened to pass. | `SKIPPED:` rule in 7b |
| N08 | No em dashes in authored docs or the README. | G8 grep |
