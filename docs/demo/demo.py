"""Run the real stdio MCP manager and psycopg2 driver for this demo."""

from __future__ import annotations

import argparse
import json
import os
import select
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


class McpClient:
    def __init__(self, process: subprocess.Popen[str]) -> None:
        self.process = process
        self.next_id = 1

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id = self.next_id
        self.next_id += 1
        request: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            request["params"] = params
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("MCP server pipes are unavailable")

        self.process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        ready, _, _ = select.select([self.process.stdout], [], [], 30)
        if not ready:
            raise TimeoutError(f"MCP server did not answer {method!r} within 30 seconds")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(
                f"MCP server closed stdout during {method!r}; "
                f"exit={self.process.poll()}"
            )
        response = json.loads(line)
        if response.get("id") != request_id:
            raise RuntimeError(f"unexpected MCP response id: {response!r}")
        if "error" in response:
            raise RuntimeError(f"MCP request failed: {response['error']!r}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"MCP response has no result object: {response!r}")
        return result

    def notify(self, method: str) -> None:
        if self.process.stdin is None:
            raise RuntimeError("MCP server stdin is unavailable")
        notification = {"jsonrpc": "2.0", "method": method}
        self.process.stdin.write(json.dumps(notification, separators=(",", ":")) + "\n")
        self.process.stdin.flush()

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self.request("tools/call", {"name": name, "arguments": arguments})
        if result.get("isError"):
            raise RuntimeError(f"tool {name!r} returned an error: {result!r}")
        content = result.get("content")
        if not isinstance(content, list) or not content or not isinstance(content[0], dict):
            raise RuntimeError(f"tool {name!r} returned no text content: {result!r}")
        text = content[0].get("text")
        if not isinstance(text, str):
            raise RuntimeError(f"tool {name!r} returned non-text content: {result!r}")
        value = json.loads(text)
        if not isinstance(value, dict):
            raise RuntimeError(f"tool {name!r} returned a non-object value: {value!r}")
        return value


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    default_target = Path(os.environ.get("CARGO_TARGET_DIR", "target/pane-c"))
    if not default_target.is_absolute():
        default_target = root / default_target
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--twinning-bin", type=Path, default=default_target / "release" / "twinning"
    )
    parser.add_argument(
        "--mcp-bin", type=Path, default=default_target / "release" / "twinning-mcp"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    twinning_bin = args.twinning_bin.resolve(strict=True)
    mcp_bin = args.mcp_bin.resolve(strict=True)
    schema = root / "docs" / "demo" / "schema.sql"
    driver = root / "docs" / "demo" / "driver.py"

    with tempfile.TemporaryDirectory(prefix="twinning-mcp-demo-") as temporary:
        environment = os.environ.copy()
        environment["TMPDIR"] = temporary
        process = subprocess.Popen(
            [
                str(mcp_bin),
                "--workdir",
                str(root),
                "--twinning-bin",
                str(twinning_bin),
            ],
            cwd=root,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
            bufsize=1,
        )
        client = McpClient(process)
        twin_id: str | None = None
        stopped = False
        try:
            initialized = client.request(
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "twinning-demo", "version": "1"},
                },
            )
            client.notify("notifications/initialized")
            server_info = initialized.get("serverInfo", {})
            print(
                "MCP initialize: "
                f"{server_info.get('name', 'unknown')} {server_info.get('version', 'unknown')}",
                flush=True,
            )

            twin = client.call_tool(
                "twin_start",
                {"kind": "postgres", "schema_path": str(schema.relative_to(root))},
            )
            twin_id = twin["twin_id"]
            connection_string = twin["connection_string"]
            print(
                "twin_start: "
                f"kind={twin['kind']} host=127.0.0.1 port={twin['port']}",
                flush=True,
            )

            driver_result = subprocess.run(
                [sys.executable, str(driver), connection_string],
                cwd=root,
                check=False,
            )

            stopped_summary = client.call_tool("twin_stop", {"twin_id": twin_id})
            stopped = True
            report = client.call_tool("twin_report", {"twin_id": twin_id})
            rows = report["tables"]["public.deals"]["rows"]
            check_violations = report["constraints"]["check_violations"]
            print(
                "twin_stop: "
                f"status={stopped_summary['status']} outcome={stopped_summary['outcome']}",
                flush=True,
            )
            print(
                "twin_report: "
                f"outcome={report['outcome']} public.deals.rows={rows} "
                f"check_violations={check_violations}",
                flush=True,
            )
            if driver_result.returncode != 0:
                raise RuntimeError(f"driver exited with {driver_result.returncode}")
            if rows != 1 or check_violations != 1 or report["outcome"] != "READY":
                raise RuntimeError(f"unexpected final report values: {report!r}")
        finally:
            if twin_id is not None and not stopped and process.poll() is None:
                try:
                    client.call_tool("twin_stop", {"twin_id": twin_id})
                except Exception as error:
                    print(f"demo cleanup: twin_stop failed: {error}", file=sys.stderr)
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            if process.poll() is None:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait()
                    raise RuntimeError("MCP server did not stop after stdin closed")
            if process.returncode != 0:
                raise RuntimeError(f"MCP server exited with {process.returncode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
