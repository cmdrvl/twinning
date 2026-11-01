#!/usr/bin/env bash
set -euo pipefail

TWINNING_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TWINNING_BIN_PATH="${TWINNING_BIN:-$TWINNING_ROOT/target/release/twinning}"
TWINNING_MCP_BIN_PATH="${TWINNING_MCP_BIN:-$TWINNING_ROOT/target/release/twinning-mcp}"
PSQL_BIN_PATH="${TWINNING_PSQL_BIN:-$(command -v psql || true)}"

for required_path in "$TWINNING_BIN_PATH" "$TWINNING_MCP_BIN_PATH"; do
    if [[ ! -x "$required_path" ]]; then
        printf 'required executable is missing: %s\n' "$required_path" >&2
        exit 1
    fi
done
if [[ -z "$PSQL_BIN_PATH" || ! -x "$PSQL_BIN_PATH" ]]; then
    printf 'psql executable is missing; set TWINNING_PSQL_BIN\n' >&2
    exit 1
fi

printf 'twinning: %s\n' "$("$TWINNING_BIN_PATH" --version)"
printf 'twinning-mcp: %s\n' "$("$TWINNING_MCP_BIN_PATH" --version)"
printf 'psql: %s\n' "$("$PSQL_BIN_PATH" --version)"

python3 -u - "$TWINNING_ROOT" "$TWINNING_BIN_PATH" "$TWINNING_MCP_BIN_PATH" "$PSQL_BIN_PATH" <<'PY'
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

root = Path(sys.argv[1])
twinning = Path(sys.argv[2])
mcp_binary = Path(sys.argv[3])
psql = Path(sys.argv[4])
demo = root / "docs/demo/migration"

with tempfile.TemporaryDirectory(prefix="twinning-migration-demo-") as scratch:
    scratch_path = Path(scratch)
    workdir = scratch_path / "workdir"
    tempdir = scratch_path / "tmp"
    workdir.mkdir()
    tempdir.mkdir()
    for name in ("schema.sql", "declaration.json", "queries.json"):
        shutil.copyfile(demo / name, workdir / name)

    environment = os.environ.copy()
    environment["TMPDIR"] = str(tempdir)
    server = subprocess.Popen(
        [str(mcp_binary), "--workdir", str(workdir), "--twinning-bin", str(twinning)],
        cwd=workdir,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    next_id = 1
    live_twins = []

    def rpc(method, params=None):
        global next_id
        request_id = next_id
        next_id += 1
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        server.stdin.write(json.dumps(message) + "\n")
        server.stdin.flush()
        line = server.stdout.readline()
        if not line:
            raise RuntimeError(f"MCP server closed stdout during {method}")
        response = json.loads(line)
        if "error" in response:
            raise RuntimeError(f"MCP protocol error: {response['error']}")
        return response["result"]

    def tool(name, arguments):
        result = rpc("tools/call", {"name": name, "arguments": arguments})
        if result.get("isError"):
            raise RuntimeError(result["content"][0]["text"])
        return json.loads(result["content"][0]["text"])

    def start(label, snapshot):
        started = tool(
            "twin_start",
            {
                "kind": "postgres",
                "schema_path": str(workdir / "schema.sql"),
                "declaration_path": str(workdir / "declaration.json"),
                "snapshot_path": str(workdir / snapshot),
            },
        )
        live_twins.append(started["twin_id"])
        print(f"twin started role={label} port={started['port']}")
        return started

    def load_rows(label, connection, candidate=False):
        deal_name = "Beta migrated" if candidate else "Beta"
        statements = [
            "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ('tenant-a', 'Tenant A')",
            "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ('tenant-b', 'Tenant B')",
            f"INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-002', 'tenant-b', '{deal_name}', 120)",
            "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-001', 'tenant-a', 'Alpha', 100)",
        ]
        print(f"psql load role={label} statement_count={len(statements)}")
        subprocess.run(
            [
                str(psql),
                "--no-psqlrc",
                "--set=ON_ERROR_STOP=1",
                connection,
                *sum((["--command", statement] for statement in statements), []),
            ],
            check=True,
        )

    try:
        rpc(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "twinning-migration-demo", "version": "1"},
            },
        )
        server.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        server.stdin.flush()
        names = [entry["name"] for entry in rpc("tools/list")["tools"]]
        if "twin_pair_proof" not in names:
            raise RuntimeError("twin_pair_proof is absent from tools/list")

        legacy = start("legacy", "legacy.snapshot")
        candidate = start("candidate", "candidate.snapshot")
        load_rows("legacy", legacy["connection_string"])
        load_rows("candidate", candidate["connection_string"], candidate=True)
        for twin in (legacy, candidate):
            stopped = tool("twin_stop", {"twin_id": twin["twin_id"]})
            if stopped["exit_code"] != 0:
                raise RuntimeError(f"twin_stop failed: {stopped}")
            live_twins.remove(twin["twin_id"])
            print(f"twin stopped outcome={stopped['outcome']} exit_code={stopped['exit_code']}")

        proof = tool(
            "twin_pair_proof",
            {
                "left_snapshot_path": str(workdir / "legacy.snapshot"),
                "right_snapshot_path": str(workdir / "candidate.snapshot"),
                "queries_path": str(workdir / "queries.json"),
                "report_path": str(workdir / "migration-proof-report.json"),
            },
        )
        print(f"proof outcome={proof['outcome']} report_file=migration-proof-report.json")
        for case in proof["cases"]:
            mismatches = ",".join(case["mismatches"]) or "none"
            print(
                f"case_id={case['case_id']} verdict={case['verdict']} mismatches={mismatches}"
            )
        if not (workdir / "migration-proof-report.json").is_file():
            raise RuntimeError("proof report was not written")
    finally:
        for twin_id in live_twins:
            try:
                tool("twin_stop", {"twin_id": twin_id})
            except Exception as error:
                print(f"twin_stop cleanup failed twin_id={twin_id}: {error}", file=sys.stderr)
        if server.poll() is None:
            try:
                server.stdin.close()
                server.wait(timeout=15)
            except Exception:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait()
        if server.returncode not in (0, None):
            raise RuntimeError(f"twinning-mcp exited {server.returncode}")
PY
