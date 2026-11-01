# Twinning MCP Server: Design Doc (v1 ships Postgres, built as a twin manager)

Audience: Cairn. Status: revised draft for Zac's review. Date: 2026-11-01.
Revises the 2026-11-01 draft. Source checked: github.com/cmdrvl/twinning at HEAD (operator.json 0.6.0, v0.6.0 released 2026-06-25).

Implementation details live in [PLAN_TWINNING_MCP_SPEC.md](./PLAN_TWINNING_MCP_SPEC.md) (files, invariants, errors, tests, gates). Where the two differ, the spec wins, and these amendments apply: process management is synchronous std threads, not tokio; `twin_report` works on stopped twins only (twinning writes its report at shutdown); `setup` ships for Claude Desktop and Cursor plus `--print`, other clients later; the OCI image and Windows are deferred past v0; `twinning-rest` is not refactored in v0.

This is CMD+RVL's first MCP server to list in public directories. It is separate from the spine (loan tape) local MCP server, which is a later phase.

## 1. Pitch

One-liner: disposable, constraint-checked twins of your datastores, APIs and MCP servers that your agent can create, use and tear down on your own machine, with a JSON report of what happened and which constraints were violated. v1 ships the Postgres twin; Snowflake, REST and MCP twins follow behind the same tools.

For: agent builders and data engineers who want to test agents and extractors against a throwaway Postgres that speaks pgwire for a declared SQL subset, without touching production. Compatibility with real client libraries (psycopg2, SQLAlchemy, psql) is a go-live gate (milestones 0 and 7), not yet an established fact; see section 2.1.

What it is not: a full Postgres. The twin serves a canary-defined SQL subset and refuses what it cannot handle, with an explicit refusal and SQLSTATE. The README says this up front.

The differentiator is the report, not the database: constraint violations and request history in a JSON report that a plain throwaway Postgres does not give you. The wider position, no tool found covers datastore, API and MCP twins together with violation reports, is in section 10.

Naming: the listing name and README lead with "twins", not "Postgres". Tools take a `kind` argument (v1 accepts only `postgres`), so later kinds need no redesign.

## 2. Verified state of the twinning repo

- Public, MIT, workspace of `twinning-kernel`, `-postgres`, `-rest`, `-snowflake`. Release workflow (`release.yml`) builds Linux x64/arm64, macOS x64/arm64 and Windows. `homebrew-tap/Formula/twinning.rb` exists.
- Postgres is the proven subset. Canary manifest covers `psql`, `psycopg2`, `sqlalchemy_core`, a metadata-join probe and the factory extractor; 55 test files reference Postgres. The compatibility suite passes with the `postgres` feature.
- Exit codes: 0 READY, 1 VIOLATIONS, 2 REFUSAL / CLI error.
- Build gap: `verify-core` and `verify-engine` are path dependencies in two places, the root `Cargo.toml` (lines 39-40) and `crates/twinning-kernel/Cargo.toml` (lines 17-18), pointing at the sibling `verify` repo. `cmdrvl/verify` is already public and MIT.
- Postgres CLI used by this server: `twinning postgres --schema <DDL> [--verify <artifact>] [--host 127.0.0.1] [--port N] --serve [--report FILE] [--snapshot FILE] [--restore FILE] [--seed FILE] [--json]`, `--export-seed-contract out.jsonl`, `twinning doctor health|capabilities --json`.
- No MCP server that controls twinning exists yet. `crates/twinning-rest/src/mcp` is the MCP twin (it mocks other servers). The control server is new code that reuses its transport.

### 2.1 Smoke-test findings (2026-11-01, `target/debug/twinning`, psql 14.24)

Ran `twinning postgres --schema loans.sql --serve --port 54329 --report r.json --json` and drove it with psql. Findings that change this plan:

1. **Stop works.** SIGTERM stops the server, exit code 0, and the `--report` file is written (outcome READY, `twinning.v0`). This confirms the `twin_stop` mapping.
2. **No ready signal.** While serving, stdout and stderr are empty. The JSON report prints only at shutdown. The control server must detect readiness by polling a TCP connect to the port, with a timeout.
3. **Default libpq settings fail.** psql with the default `sslmode=prefer` hangs until timeout, and the twin's listener then exits with REFUSAL `E_RUNTIME_IO` ("connection closed while reading frame", stage `listener_accept_loop`), exit code 2, and no report file. With `PGSSLMODE=disable` the same session works. Unit test `ssl_request_is_denied_without_fatal_refusal` exists, so the SSL-denied reply needs a real-client check. Consequences: the connection string the MCP returns carries `sslmode=disable` and `gssencmode=disable`, and a twinning fix is a prerequisite (milestone 0).
4. **One bad connection ends the server.** The accept loop treats a mid-frame disconnect as fatal for the whole listener. A long-lived twin managed by an agent needs per-connection errors isolated. Seen in one case only; milestone 0 reproduces it with a test.
5. **Simple-query writes are refused.** `INSERT INTO loans (id, balance) VALUES (1, 100)` through `psql -c` returns "simple query shape `unsupported_statement` is outside the declared session subset". The canary manifest lists writes only for extended-query clients (`psycopg2_params`, `sqlalchemy_core`); `psql_smoke` has `write_shapes: []`. The demo and the e2e tests must use an extended-protocol client, not `psql -c`.
6. **The canaries are frozen fixtures, not real driver runs.** `tests/fixtures/canaries/*/ir_equivalence.json` and the Rust test harness pin the statement shapes. Nothing in the repo runs real psycopg2 or SQLAlchemy against the server. The first real-driver run is milestone 0, and installing a driver needs Zac's approval (a throwaway `uv` environment).
7. **Report details.** `wire_protocol` reads `planned.pgwire` (stale string). The `constraints` block has `not_null`, `fk`, `check` and `unique` counters; whether violations raised through extended-query inserts increment them is untested and is checked in milestone 0.
8. **AGENTS.md is stale.** It says a long-lived standalone server is not implemented, but `--serve` exists (README and CLI help agree). Fix in milestone 0.

## 3. v1 tools (Postgres lifecycle)

The server is a local process. It spawns `twinning postgres ... --serve` child processes, tracks them by `twin_id`, and reads their `--report` and `--json` output. Data stays on the machine.

| Tool | Behavior | Maps to |
| --- | --- | --- |
| `twin_start` | Input: `kind` (v1: `postgres` only), DDL path; optional verify artifact, seed file, restore snapshot, `ttl_minutes` (default 60; the twin stops and writes its report when it expires). Returns `twin_id` and `postgresql://u@127.0.0.1:<port>/db?sslmode=disable&gssencmode=disable`, only after a TCP connect to the port succeeds (10 s timeout; on failure the child is killed and its stderr returned) | `postgres --schema --serve --port` (free port by default) |
| `twin_list` | Running twins: id, port, uptime, schema | server state |
| `twin_stop` | Stop the twin; write the report and optionally a snapshot | SIGTERM to child, `--report`, `--snapshot` |
| `twin_report` | Session report JSON: constraint violations, refusals, metrics | `--report` file |
| `twin_seed_contract` | Export the seed contract an agent fills in | `--export-seed-contract` |
| `twin_doctor` | Health and capabilities | `doctor health --json`, `doctor capabilities --json` |

Deferred past v1 (restart-based, no in-place reset exists): `twin_reset` (stop and start from the original schema or a snapshot) and `twin_snapshot`.

The agent uses its own Postgres tool or client with the returned connection string. That is the "use" step.

Transport: stdio by default. Optional Streamable HTTP bound to 127.0.0.1 only. Twin ports default to free ports on 127.0.0.1.

## 4. Scope

Ship in v1:

- Postgres twin lifecycle: create, use, tear down, with seed, snapshot/restore, verify violations and reports.

Deferred (not in v1):

- REST twin, MCP twin (beta candidates for v2).
- Snowflake twin. Tests pass (20-test compatibility suite green, 5 config unit tests), but coverage is thinner than Postgres: one smoke canary run through a Go-style HTTP sequence, not in the canary manifest, no multi-client canary. Label "beta" when it ships. Bar for promoting it: one more client canary (for example dbt or JDBC) in the manifest.
- `twin_run_once` and `--run CMD`: runs a caller-supplied command. Out of v1, which also removes the arbitrary-command surface (and `mcp --server <CMD>`).
- `twin_query`: would make the server a SQL client with its own connection handling. Not a twinning feature today; agents already have Postgres tools.
- Twin-pair migration proof, `--materialize-source-url` (touches a real database), `twinning port`.
- Receipts funnel hint in `twin_report` (no CMD+RVL pointers in tool output for v1).

## 5. Implementation

- Rust, a new workspace crate and binary in the twinning repo: `crates/twinning-mcp` producing `twinning-mcp`. Same repo because it is a thin controller over the twinning binary (flag and report changes move together), reuses workspace code, and shares one release pipeline. Split out only if its release cadence or contributors diverge.
- Reuse from `crates/twinning-rest/src/mcp`: `stdio.rs`, `dispatcher.rs` (`JsonRpcRequest`/`Response`/`Error`), `version.rs`. Extract into a small shared crate (`twinning-mcp-core`) so the spine server can reuse it.
- Process management with std threads and `signal-hook` (no tokio; spec D1). Default cap of 5 concurrent twins. Clean up children on exit and on crash (children must not survive a killed parent: process group or parent-death handling; sweep stale children at startup).
- Fix the build gap first: switch `verify-core` and `verify-engine` to git dependencies in both places (root `Cargo.toml` and `twinning-kernel`). Crates.io publishing is optional later. Once fixed, remove the vendoring step in `release.yml`.
- Packaging: extend the existing `release.yml` and Homebrew formula (`brew install cmdrvl/tap/twinning` already works). Add:
  - the `twinning-mcp` binary to release assets, with SHA-256 checksums and build provenance;
  - an npm launcher `@cmdrvl/twinning-mcp` in an `npm/` subfolder that downloads the matching release binary, verifies its checksum and runs it (pin the version in the package);
  - an OCI image (needed for the Docker MCP catalog): deferred past v0.
- `twinning-mcp setup`: one command that writes the MCP config entry for Claude Desktop and Cursor (other clients via `--print`, more later), with `--dry-run` to print the change and `--client <name>` to target one. Idempotent, backs up the file it edits, never touches other servers' entries. Also `twinning-mcp doctor` (same checks as `twin_doctor`) so a user can verify the install from a shell. Idea taken from PGSandbox (section 11).
- Read-only/restricted mode: `twinning-mcp --restricted` (or an env var) disables `twin_start` options that read caller-supplied seed or snapshot paths outside the configured working directory. Idea taken from Neon and Postgres MCP Pro access modes.
- Document `cargo install --git ...` as a secondary route. Prebuilt binaries are the default install; source is public for review and fallback.

## 6. Safety

- Local only: twins bind 127.0.0.1. No telemetry and no network calls from the server.
- v1 starts only the Postgres twin from a DDL file the caller names. It does not run caller-supplied commands (`--run` and `mcp --server <CMD>` are out of v1).
- File paths: restrict DDL, seed, snapshot and report paths to a working directory the user configures (default: current directory).
- Cap on concurrent twins, with cleanup on exit.
- README safety section states all of the above plus the SQL-subset refusal behavior.

## 7. Listing plan (nothing is published without Zac's approval)

1. Official MCP Registry first (preview; no uptime guarantees). `server.json` name `com.cmdrvl/twinning` with a `packages` entry for npm (`@cmdrvl/twinning-mcp`, stdio); OCI is added later.
   - Registry verification: the npm `package.json` must contain `"mcpName": "com.cmdrvl/twinning"`, matching `server.json`. When the OCI image is added, confirm the registry's matching label name from its docs first.
   - Domain proof for the `com.cmdrvl` namespace by DNS TXT (`v=MCPv1; k=ed25519; p=<key>`) or the HTTP file at `https://cmdrvl.com/.well-known/mcp-registry-auth` (served from the website's `static/`; no DNS access needed). Without domain proof, GitHub auth would force `io.github.cmdrvl/twinning`.
   - Check `repository.subfolder` in the schema if the package lives in a subfolder.
2. Repo discoverability: add topics `mcp` and `mcp-server` (current topics are CLI-flavored); lead the README with a "Use with an agent (MCP)" section.
3. README sections directories expect: what it is, install (brew, npx, docker), config snippets for Claude Desktop, Cursor and Cline, tool list with args, example prompts, safety, license (MIT), demo GIF.
4. Demo GIF is the v1 loop: start a twin from a DDL file, run a bad insert, read the constraint violation in the report, stop.
5. Then, as each fits: punkpeye/awesome-mcp-servers (local, public repo with code: qualifies), Docker MCP catalog, Cline marketplace, LobeHub, mcp.so, mcpservers.org. Glama and PulseMCP: PulseMCP is paused and says it will pick up official-registry entries automatically on reopening; Glama has no official submission guide I found, so confirm on their site. Smithery wants a remote Streamable HTTP URL, so a local stdio server may not fit; confirm before spending time.
6. Add an entry in the planned public `cmdrvl/mcp-servers` catalog repo (docs-only index linking here and to Receipts).

## 8. Funnel

None in v1. Revisit an opt-in `next_actions` hint in a later release.

## 9. Milestones

0. Twinning preflight: fix the SSL-denied handshake with real libpq clients, isolate per-connection errors so one bad client cannot end the server, run real psycopg2 and SQLAlchemy through a throwaway `uv` environment (needs Zac's approval to install) and record the result, fix the stale `wire_protocol` string and AGENTS.md. Go-live is blocked until this passes.
1. Switch `verify-core` and `verify-engine` to git dependencies (root and `twinning-kernel`); confirm `cargo build` works standalone and simplify `release.yml`.
2. Extract `twinning-mcp-core` from `twinning-rest`.
3. Control server skeleton with `twin_start`, `twin_list`, `twin_stop`, `twin_report`.
4. Add `twin_seed_contract`, `twin_doctor`, TTL expiry. Child cleanup and path restrictions.
5. `twinning-mcp setup` and `twinning-mcp doctor` (client config writer, dry-run, backup).
6. Integration tests in Claude Desktop, Cursor and Cline, using `setup` to install. Demo GIF.
7. Release: binaries with checksums and provenance, npm launcher with checksum verification, README (OCI later).
8. `server.json`, `mcpName`, domain proof, then registry and directory submissions (after Zac approves).

## 10. Landscape

Searched 2026-11-01 from public pages only. Nothing below was run or benchmarked by us, and popularity figures are as reported on the cited pages. Gaps are noted. Two lists: tools that give an agent a real database, and tools that give an agent a twin. Only the second list is competition. The first is context for what directory users will also see.

### 10.1 Real-database tools (not competition)

| Tool | What it gives the agent | Notes |
| --- | --- | --- |
| PGSandbox / `pgsandbox-mcp` (LVTD-LLC) | Create, inspect, use, clone and delete disposable real Postgres databases through an stdio MCP server and CLI | Local-first Rust, managed cluster under `~/.pgsandbox/`, no Docker, Homebrew, a `setup` command that writes MCP client config, TTL on create. Latest release seen: pgsandbox-mcp v0.4.7. Closest in packaging and UX; the source of the `setup` and TTL ideas |
| sbx (aryanmehrotra) | Per-agent isolated real Postgres, Redis or browser sandboxes | Go, MIT, v0.9.1, Docker/Kubernetes providers, about 19 MCP tools, wake-on-connect. No star or download figures found |
| Neon MCP | Hosted branching: `create_branch`, `delete_branch`, `run_sql`, temporary-branch migration and tuning flows | 659 stars, MIT, hosted at mcp.neon.tech, OAuth or API key, `readonly=true` mode, analytics on by default (opt out by env var) |
| Supabase MCP | Hosted; branching on a paid plan | New branches start without data by default |
| Postgres MCP Pro (crystaldba) | Configurable read/write access and performance analysis against an existing database | Roughly 2.5-2.8K stars across directory snapshots (sources vary) |
| Ardent | Commercial copy-on-write clone of a Postgres database for agents | Commercial service |
| Microsoft, pgEdge, DBHub, MCP Toolbox | Access to an existing database | Access servers, not disposable environments |

Not found by search: anything for PGlite MCP or Testcontainers MCP.

### 10.2 Twin and simulator tools (competition)

| Tool | Scope | Notes |
| --- | --- | --- |
| Arga Labs (commercial) | Stateful twins of about 33 SaaS apps (Slack, GitHub, Gmail, Stripe, Notion, Jira, Salesforce, Twilio...), API, MCP and CLI | Switch base URLs; runs last 60 minutes or are permanent; seeded from a natural-language description or production data; every call, response and side effect logged and graded per commit or PR. No database twins on the page. SOC 2 and HIPAA "in progress". No fidelity metrics published |
| WonderTwin AI (open core) | Local behavioral twins of third-party APIs, one MCP server per twin | `wt install stripe@latest` gives a compiled Go binary; latest twin versions free forever, historical versions paid, chaos testing "coming soon". Only Stripe named on the page; fidelity claim is SDK compliance for the latest release. HN post had 2 points |
| Pome digital-twins | Stripe (26 MCP tools) and Slack (18) twins that log requests and state changes | Seen in search results; the GitHub repo now returns 404, so status is unknown |
| StrongDM Digital Twin Universe | Internal clones of Okta, Jira, Slack, Google Docs/Drive/Sheets used to test agents | The origin of the idea (Willison write-up, Feb 2026), an approach and not a product |
| WireMock (7.4K stars) | API mocking; its CLI ships an MCP server so agents create and update mocks | Not a stateful twin of a named service |
| mockd | Multi-protocol mock server with 18 MCP tools | One third-party listing only; unverified |
| mcptest | pytest-style mocking of MCP servers with YAML fixtures and trajectory assertions | MCP-server mocks only |
| fakesnow (216 stars, Apache-2.0) | DuckDB-backed Snowflake fake that the official drivers connect to unchanged | Active (pushed 2026-10-25). SQL-level, not the HTTP wire protocol |
| LocalStack for Snowflake | Container emulator; basic warehouse, database, schema and table operations | Needs an auth token; narrower feature set |

Not found: a Postgres wire-protocol fake that reports constraint violations (PGlite is real Postgres compiled to WASM; pg-mem and pgmock could not be verified from search). pgwire fakes may exist that search did not surface.

### 10.3 Where twinning stands

- Ahead on breadth: per its README, one tool with Postgres (pgwire), Snowflake (HTTP and Arrow), REST from an OpenAPI spec and MCP twins, with constraint-violation reports, local and with no account. No tool above covers datastore, API and MCP twins together. This comes from the README, not from comparative testing.
- Ahead on the report: constraint violations, refusals and request history in one JSON report with exit codes (READY, VIOLATIONS, REFUSAL).
- Behind on SaaS coverage: Arga ships prebuilt behavioral twins for dozens of named products. Twinning's REST twin is generated from a spec, with one worked example seen (OpenFIGI).
- Behind on MCP-first packaging: WonderTwin and Arga already ship MCP access per twin. Twinning has no MCP server yet; this doc is the plan to fix that.
- Against fakesnow: a toss-up that depends on the user. fakesnow is established at the SQL level; twinning's Snowflake twin is wire-level with thinner canary coverage.

## 11. Ideas adopted from the landscape

| Idea | From | How it lands here |
| --- | --- | --- |
| `setup` subcommand that writes the MCP client config | PGSandbox | `twinning-mcp setup` with `--dry-run`, `--client`, backup of edited files (section 5, milestone 5) |
| Time-to-live on every sandbox | PGSandbox, Arga (60-minute runs) | `ttl_minutes` on `twin_start`, default 60; expiry stops the twin and writes its report |
| Same operations from the CLI and over MCP | PGSandbox | Already true: the MCP tools drive the existing `twinning` CLI. State it in the README |
| Read-only or restricted access mode | Neon (`readonly=true`), Postgres MCP Pro | `--restricted` mode that limits file paths to the working directory (section 5) |
| Disclose telemetry, make opt-out obvious | Neon (analytics on, Sentry not opt-out) | No telemetry at all in v1, stated in the README as a feature |
| Per-call logging graded against criteria | Arga | The twin report already holds the request log. Later: assertions on the report (for example "zero violations") so a CI job can fail on it |
| Fidelity claims tied to something checkable | WonderTwin (SDK compliance), Arga (no published metrics, a gap to avoid) | Publish the canary manifest and its pass results per twin kind in the README. Say "tested clients: psql, psycopg2, SQLAlchemy Core" and nothing wider |
| Prebuilt twins installed by one command, free for the latest | WonderTwin | Homebrew, npm and Docker installs for the binary. No account |
| An agent skill that builds a new twin | WonderTwin | Later: a skill that drafts a twin spec for a given OpenAPI file; not v1 |
| Chaos and failure modes as a feature | WonderTwin ("coming soon"), Truto guide (429s) | The REST twin already has chaos injection. Mention it when REST ships; not v1 |
| Driver compatibility as the headline test | fakesnow (official drivers unmodified) | README states which drivers connect unchanged, per twin |
| Trajectory assertions on tool calls | mcptest | For the MCP twin kind later: the session log exposed as a report an agent test can assert on |

Not adopted: hosted accounts and OAuth (Neon, Arga), because v1 is local only; per-vendor SaaS twin catalogs (Arga), because twinning is spec-driven and that is a different business; cloning real databases (Ardent), which is `--materialize-source-url` and deferred.

## Open questions for Zac

None. All resolved below.

## Resolved from the earlier draft

- Separate binary vs subcommand: separate `twinning-mcp` binary, same repo.
- REST twin in v1: deferred.
- `twin_query`: out of v1.
- crates.io vs git for verify: git dependencies now (verify is public).
- Snowflake: deferred; beta when it ships, after one more client canary.
- `twinning-mcp setup` and `ttl_minutes` (default 60) are in v1 (Zac, 2026-11-01).
- Listing leads with "twins" and tools take `kind`; v1 accepts only `postgres` and refuses other kinds clearly. The README says Postgres only up front (Zac, 2026-11-01).
- Registry name `com.cmdrvl/twinning` confirmed. Domain proof by the HTTP file `https://cmdrvl.com/.well-known/mcp-registry-auth` (website `static/`), not DNS (Zac, 2026-11-01).
- `twin_reset` and `twin_snapshot`: deferred past v1. v1 ships the six core tools (Zac, 2026-11-01).
- Twin cap 5 and file paths restricted to the current directory by default (Zac, 2026-11-01).
- Receipts pointer: out of v1 entirely, not even in the README (Zac, 2026-11-01).
