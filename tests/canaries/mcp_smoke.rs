use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::Value;
use twinning::protocol::rest::{
    auth::RestAuthMode,
    mcp::{
        catalog::load_mcp_catalog_from_manifest,
        listener::{McpCatalogInput, McpConfig, start_test_server},
    },
};

#[derive(Debug, Deserialize)]
struct McpSmokeCase {
    name: String,
    request: Value,
    expect: Value,
}

#[test]
fn mcp_smoke() {
    let fixture_dir = mcp_fixture_dir();
    let manifest_path = fixture_dir.join("manifest.json");
    let cases_path = fixture_dir.join("cases.json");
    let report_dir = tempfile::tempdir().expect("report tempdir");
    let report_path = report_dir.path().join("mcp-report.json");

    let first = load_mcp_catalog_from_manifest(&manifest_path).expect("first catalog load");
    let second = load_mcp_catalog_from_manifest(&manifest_path).expect("second catalog load");
    assert_eq!(first.catalog_hash, second.catalog_hash);

    let (addr, shutdown_tx) = start_test_server(McpConfig {
        source: McpCatalogInput::Manifest {
            path: manifest_path.clone(),
        },
        host: "127.0.0.1".to_owned(),
        port: 0,
        auth_mode: RestAuthMode::Bypass,
        stdio: false,
        run_command: None,
        report_path: Some(report_path.clone()),
        json: true,
    });
    let cases: Vec<McpSmokeCase> =
        serde_json::from_str(&fs::read_to_string(cases_path).expect("read MCP cases"))
            .expect("parse MCP cases");

    for case in cases {
        let actual = post_jsonrpc(addr, &case.request);
        assert_json_subset(&case.expect, &actual, &case.name);
    }

    shutdown_tx.send(()).expect("shutdown MCP server");
    let report = wait_for_report(&report_path);
    assert_eq!("twinning.mcp-report.v0", report["version"]);
    assert_eq!(6, report["session"]["request_count"]);
    assert_eq!(1, report["session"]["tool_calls"]["stubbable"]);
    assert_eq!(1, report["session"]["tool_calls"]["unsupported"]);
    assert_eq!(
        1,
        report["session"]["protocol_versions"]["unsupported_requested"]["2099-01-01"]
    );
    assert_eq!(1, report["warnings"].as_array().expect("warnings").len());
}

fn mcp_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join("mcp_smoke")
}

fn post_jsonrpc(addr: SocketAddr, request: &Value) -> Value {
    let body = request.to_string();
    let mut stream = TcpStream::connect(addr).expect("connect to MCP twin");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");
    let raw = format!(
        "POST / HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(raw.as_bytes()).expect("write request");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "expected HTTP 200 JSON-RPC response, got {response}"
    );
    let (_, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response should contain a body separator");
    serde_json::from_str(body).expect("parse JSON-RPC response")
}

fn assert_json_subset(expected: &Value, actual: &Value, context: &str) {
    assert_json_subset_at(expected, actual, context, "$");
}

fn assert_json_subset_at(expected: &Value, actual: &Value, context: &str, path: &str) {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            for (key, expected_value) in expected {
                let actual_value = actual
                    .get(key)
                    .unwrap_or_else(|| panic!("{context}: missing key `{key}` at {path}"));
                assert_json_subset_at(
                    expected_value,
                    actual_value,
                    context,
                    &format!("{path}.{key}"),
                );
            }
        }
        (Value::Array(expected), Value::Array(actual)) => {
            assert!(
                actual.len() >= expected.len(),
                "{context}: expected at least {} array entries at {path}, got {}",
                expected.len(),
                actual.len()
            );
            for (index, expected_value) in expected.iter().enumerate() {
                assert_json_subset_at(
                    expected_value,
                    &actual[index],
                    context,
                    &format!("{path}[{index}]"),
                );
            }
        }
        _ => assert_eq!(expected, actual, "{context}: mismatch at {path}"),
    }
}

fn wait_for_report(path: &Path) -> Value {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(raw) = fs::read_to_string(path) {
            return serde_json::from_str(&raw).expect("parse MCP report");
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for MCP report at {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}
