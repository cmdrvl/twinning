#![forbid(unsafe_code)]
#![cfg(feature = "mcp")]

use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Output, Stdio},
};

use serde_json::Value;
use tempfile::tempdir;

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("mcp")
        .join("http_manifest.json")
}

fn run_twinning_mcp_run(report_path: &std::path::Path) -> Output {
    Command::new(twinning_bin())
        .arg("mcp")
        .arg("--manifest")
        .arg(fixture_path())
        .arg("--port")
        .arg("0")
        .arg("--auth-mode")
        .arg("bypass")
        .arg("--run")
        .arg(
            r#"curl -fsS -X POST -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"lookup"}}' "$MCP_BASE_URL/" >/dev/null"#,
        )
        .arg("--report")
        .arg(report_path)
        .arg("--json")
        .output()
        .expect("run twinning mcp")
}

#[test]
fn mcp_run_mode_dispatches_jsonrpc_and_writes_report() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("mcp-report.json");

    let output = run_twinning_mcp_run(&report_path);

    assert!(
        output.status.success(),
        "twinning mcp should exit 0: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: Value = serde_json::from_slice(&output.stdout).expect("mcp run JSON stdout");
    assert_eq!(stdout["version"], "twinning.mcp-run.v0");
    assert_eq!(stdout["outcome"], "PASS");
    assert_eq!(stdout["child"]["exit_code"], 0);

    let report_raw = fs::read_to_string(&report_path).expect("MCP report should be written");
    let report: Value = serde_json::from_str(&report_raw).expect("MCP report JSON");
    assert_eq!(report["version"], "twinning.mcp-report.v0");
    assert_eq!(report["catalog"]["source"], "manifest");
    assert_eq!(report["catalog"]["server_name"], "fixture-mcp");
    assert_eq!(report["catalog"]["tool_count"], 2);
    assert_eq!(report["catalog"]["tools_stubbable"], 1);
    assert_eq!(report["catalog"]["tools_unsupported"], 1);
    assert_eq!(report["session"]["request_count"], 1);
    assert_eq!(report["session"]["tool_calls"]["stubbable"], 1);
}

#[test]
fn mcp_stdio_dispatches_newline_jsonrpc() {
    let mut child = Command::new(twinning_bin())
        .arg("mcp")
        .arg("--manifest")
        .arg(fixture_path())
        .arg("--stdio")
        .arg("--json")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn twinning mcp stdio");

    {
        let mut stdin = child.stdin.take().expect("stdio stdin");
        writeln!(
            &mut stdin,
            r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2024-11-05"}}}}"#
        )
        .expect("write initialize");
        writeln!(
            &mut stdin,
            r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list"}}"#
        )
        .expect("write tools/list");
    }

    let output = child.wait_with_output().expect("wait for stdio child");
    assert!(
        output.status.success(),
        "stdio command should exit 0: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = String::from_utf8(output.stdout)
        .expect("stdout UTF-8")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("JSON-RPC line"))
        .collect::<Vec<_>>();

    assert_eq!(2, lines.len());
    assert_eq!("2024-11-05", lines[0]["result"]["protocolVersion"]);
    assert_eq!("lookup", lines[1]["result"]["tools"][0]["name"]);
}
