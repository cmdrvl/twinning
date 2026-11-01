# Twinning MCP Server: Design Doc

Status: v0 implemented in this repo (`crates/twinning-mcp`), not yet released. Date: 2026-11-01.

Implementation details (files, invariants, errors, tests, gates) live in [PLAN_TWINNING_MCP_SPEC.md](./PLAN_TWINNING_MCP_SPEC.md). Where the two differ, the spec wins. Notable differences from earlier drafts: process management is synchronous std threads, not tokio; `twin_report` works on stopped twins only (twinning writes its report at shutdown); `setup` covers Claude Desktop and Cursor plus `--print`; the OCI image and Windows are deferred; `twinning-rest` is not refactored; `twinning-mcp-core` is a copy of the JSON-RPC and version code, not an extraction; Streamable HTTP is out of v0.

## 1. Overview

Disposable, constraint-checked twins of datastores that an agent can create, use and tear down on the local machine, with a JSON report of what happened and which constraints were violated. v0 ships the Postgres twin. Tools take a `kind` argument (v0 accepts only `postgres`), so later kinds need no redesign.

For agent builders and data engineers who want to test agents and extractors against a throwaway Postgres that speaks pgwire for a declared SQL subset, without touching production.

What it is not: a full Postgres. The twin serves a canary-defined SQL subset and refuses everything else with an explicit refusal and SQLSTATE. The README says this up front, and [docs/CLIENT_COMPAT.md](./CLIENT_COMPAT.md) records exactly which client operations were run and what happened.

The value is the report, not the database: constraint violations and request history in a JSON report that a plain throwaway Postgres does not give you.

## 2. State of the twinning repo

- Public, MIT, workspace of `twinning-kernel`, `-postgres`, `-rest`, `-snowflake`, plus the new `twinning-mcp-core` and `twinning-mcp`.
- Postgres is the proven subset, defined by the canary manifest. Real-driver runs (psql, psycopg2, SQLAlchemy Core) and their limits are recorded in `docs/CLIENT_COMPAT.md`.
- Exit codes of `twinning`: 0 READY, 1 VIOLATIONS, 2 REFUSAL or CLI error.
- Postgres CLI used by the MCP server: `twinning postgres --schema <DDL> [--verify <artifact>] [--host 127.0.0.1] [--port N] --serve [--report FILE] [--snapshot FILE] [--restore FILE] [--seed FILE] [--json]`, `--export-seed-contract out.jsonl`, `twinning doctor health|capabilities --json`.
- `crates/twinning-rest/src/mcp` is the MCP twin (it mocks other servers). The control server is separate code that reuses its transport shape.

## 3. Tools (Postgres lifecycle)

The server is a local process. It spawns `twinning postgres ... --serve` child processes, tracks them by `twin_id`, and reads their report output. Data stays on the machine.

| Tool | Behavior |
| --- | --- |
| `twin_start` | Input: `kind` (`postgres` only), schema path; optional verify artifact, seed file, restore snapshot, `ttl_minutes` (default 60; the twin stops and writes its report when it expires). Returns `twin_id` and a connection string for `127.0.0.1` with `sslmode=disable&gssencmode=disable`, only after a TCP connect to the port succeeds. On failure the child is killed and its stderr is returned |
| `twin_list` | Retained twins with id, port, status and uptime |
| `twin_stop` | Stops the twin and returns its run summary; idempotent |
| `twin_report` | Session report JSON for a stopped twin: constraint violations, refusals, metrics |
| `twin_seed_contract` | Writes the seed contract an agent fills in |
| `twin_doctor` | Health and capabilities of the binary and workdir |

Deferred past v0: `twin_reset` and `twin_snapshot`.

The agent uses its own Postgres client with the returned connection string. That is the "use" step. Transport is stdio.

## 4. Scope

In v0: the Postgres twin lifecycle (create, use, tear down), with seed, restore, verify violations and reports.

Deferred: REST twin and MCP twin as MCP-controlled kinds; Snowflake twin (thinner coverage, would be labeled beta); `twin_run_once` and `--run CMD` (arbitrary-command surface); `twin_query` (agents already have Postgres tools); twin-pair migration proof, `--materialize-source-url` and `twinning port`; broader SQL coverage such as joins.

## 5. Implementation

- Rust, in this workspace: `crates/twinning-mcp` produces the `twinning-mcp` binary. It is a thin controller over the `twinning` binary, so flag and report changes move together and one release pipeline serves both.
- JSON-RPC, stdio and version negotiation code is copied into `twinning-mcp-core`.
- Process management uses std threads and `signal-hook`. Cap of 5 concurrent twins. Children are cleaned up on exit, and stale children are swept at startup.
- `verify-core` and `verify-engine` are git dependencies, so the repo builds standalone.
- Packaging: `twinning-mcp` ships beside `twinning` in the release archive with SHA-256 checksums; an npm launcher in `npm/` downloads the matching release binary, verifies its checksum and runs it.
- `twinning-mcp setup` writes the MCP config entry for Claude Desktop and Cursor (other clients via `--print`), with `--dry-run`, a backup of the edited file, and no changes to other servers' entries. `twinning-mcp doctor` runs the same checks as `twin_doctor`.
- `--restricted` limits optional file inputs to the configured working directory.

## 6. Safety

- Local only: twins bind `127.0.0.1`. No telemetry and no network calls from the server.
- v0 starts only the Postgres twin from a schema file the caller names. It does not run caller-supplied commands.
- Schema, seed, verify and restore paths must resolve inside the configured working directory.
- Cap on concurrent twins, with cleanup on exit.
- Tool output is data, not instructions.

## 7. Milestones

The work order and test gates are in the spec (milestones M0a to M8, quality gates G1 to G10). In short: twinning fixes so real clients connect and violations are counted; standalone build; MCP crates and the six tools; `setup` and `doctor`; real end-to-end tests with psql, psycopg2 and SQLAlchemy; README and demo; release assets and packaging drafts.
