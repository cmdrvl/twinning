#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cd "$repo_root"

CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target}
export CARGO_TARGET_DIR
case "$CARGO_TARGET_DIR" in
  /*) target_root=$CARGO_TARGET_DIR ;;
  *) target_root=$repo_root/$CARGO_TARGET_DIR ;;
esac

cargo build --locked --release -p twinning -p twinning-mcp

workdir=$(mktemp -d "${TMPDIR:-/tmp}/twinning-rest-demo.XXXXXX")
trap 'rm -rf "$workdir"' EXIT HUP INT TERM
cp docs/demo/rest/response-stub-schema.yaml "$workdir/response-stub-schema.yaml"

python3 - "$target_root/release/twinning" "$target_root/release/twinning-mcp" "$workdir" <<'PY'
import json
import signal
import subprocess
import sys
from pathlib import Path

twinning_bin, mcp_bin, workdir_arg = sys.argv[1:]
workdir = Path(workdir_arg).resolve()
spec_path = workdir / "response-stub-schema.yaml"
stderr_path = workdir / "mcp-stderr.log"
stderr_file = stderr_path.open("w")
server = subprocess.Popen(
    [mcp_bin, "--workdir", str(workdir), "--twinning-bin", twinning_bin],
    cwd=workdir,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=stderr_file,
    text=True,
    bufsize=1,
    start_new_session=True,
)
next_id = 1
twin_id = None


def stop_on_signal(signum, _frame):
    raise KeyboardInterrupt(f"received signal {signum}")


signal.signal(signal.SIGINT, stop_on_signal)
signal.signal(signal.SIGTERM, stop_on_signal)


def rpc(method, params=None):
    global next_id
    request_id = next_id
    next_id += 1
    request = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        request["params"] = params
    server.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
    server.stdin.flush()
    line = server.stdout.readline()
    if not line:
        raise RuntimeError(f"MCP server closed stdout during {method}")
    response = json.loads(line)
    if response.get("id") != request_id or "error" in response:
        raise RuntimeError(f"MCP request {method} failed: {response}")
    return response["result"]


def tool(name, arguments):
    response = rpc("tools/call", {"name": name, "arguments": arguments})
    if response.get("isError"):
        raise RuntimeError(f"MCP tool {name} failed: {response}")
    return json.loads(response["content"][0]["text"])


def curl_request(label, method, url, expected_status, body=None, api_key=False):
    response_path = workdir / f"{label}.response.json"
    command = [
        "curl", "--silent", "--show-error", "--noproxy", "*",
        "--connect-timeout", "2", "--max-time", "10",
        "--request", method,
    ]
    if body is not None:
        command.extend(["--header", "Content-Type: application/json"])
    if api_key:
        command.extend(["--header", "X-OPENFIGI-APIKEY: demo-key"])
    if body is not None:
        body_path = workdir / f"{label}.request.json"
        body_path.write_text(json.dumps(body, separators=(",", ":")))
        command.extend(["--data-binary", f"@{body_path}"])
    command.extend([
        "--output", str(response_path),
        "--write-out", "\\n%{http_code}",
        url,
    ])
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    status_text = result.stdout.strip().splitlines()[-1]
    status = int(status_text)
    response_body = response_path.read_text()
    response_json = json.loads(response_body)
    if status != expected_status:
        raise RuntimeError(f"{label}: expected HTTP {expected_status}, got {status}: {response_body}")
    print(f"curl {label}: {method} {url} -> HTTP {status}")
    print(json.dumps(response_json, sort_keys=True, separators=(",", ":")))
    return response_json


try:
    initialized = rpc("initialize", {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "twinning-rest-demo", "version": "1"},
    })
    print(f"MCP initialized: {initialized['protocolVersion']}")
    server.stdin.write(json.dumps({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
    }) + "\n")
    server.stdin.flush()
    tools = rpc("tools/list")["tools"]
    names = {item["name"] for item in tools}
    if not {"twin_start", "twin_stop", "twin_report"}.issubset(names):
        raise RuntimeError(f"Required MCP tools are absent: {sorted(names)}")

    started = tool("twin_start", {
        "kind": "rest",
        "spec_path": str(spec_path),
        "server_variables": {"basePath": "v3"},
    })
    twin_id = started["twin_id"]
    base_url = started["base_url"]
    print(f"REST twin started through MCP: id={twin_id} base_url={base_url}")
    mapping_body = [{"idType": "ID_CUSIP", "idValue": "037833100"}]
    curl_request("matching", "POST", f"{base_url}/v3/mapping", 200, mapping_body, True)
    curl_request("missing-key", "POST", f"{base_url}/v3/mapping", 401, mapping_body)
    curl_request(
        "object-body", "POST", f"{base_url}/v3/mapping", 422,
        {"idType": "ID_CUSIP", "idValue": "037833100"}, True,
    )
    curl_request("unknown-route", "GET", f"{base_url}/v3/no-such-route", 404)

    stopped = tool("twin_stop", {"twin_id": twin_id})
    print(f"REST twin stopped through MCP: status={stopped['status']} exit_code={stopped['exit_code']}")
    report = tool("twin_report", {"twin_id": twin_id})
    summary = {
        key: report[key]
        for key in (
            "version", "outcome", "request_count", "outcomes", "refusals",
            "endpoints_exercised", "endpoints_not_exercised",
        )
    }
    print("REST report summary: " + json.dumps(summary, sort_keys=True, separators=(",", ":")))
    if report["request_count"] != 4 or report["outcome"] != "PASS":
        raise RuntimeError(f"Unexpected REST report: {summary}")
    server.stdin.close()
    exit_code = server.wait(timeout=20)
    print(f"twinning-mcp exit: {exit_code}")
    if exit_code != 0:
        raise RuntimeError(f"twinning-mcp exited {exit_code}; see {stderr_path}")
finally:
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    stderr_file.close()
    for line in stderr_path.read_text().splitlines():
        print(f"MCP stderr: {line}")
PY
