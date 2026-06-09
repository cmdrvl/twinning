#![forbid(unsafe_code)]
#![cfg(feature = "rest")]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
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
        .join("rest")
        .join("minimal-api.yaml")
}

fn openfigi_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("openfigi_v2_v3")
        .join("schema.json")
}

fn run_twinning(args: &[&str]) -> Output {
    Command::new(twinning_bin())
        .args(args)
        .output()
        .expect("run twinning")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, got status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn run_port(client_cmd: &str, report_path: &Path) -> Output {
    let spec = fixture_path();
    run_twinning(&[
        "port",
        "--from-spec",
        spec.to_str().expect("spec path"),
        "--to-spec",
        spec.to_str().expect("spec path"),
        "--client-cmd",
        client_cmd,
        "--report",
        report_path.to_str().expect("report path"),
        "--json",
    ])
}

fn run_openfigi_v2_v3_port(client_cmd: &str, report_path: &Path) -> Output {
    let spec = openfigi_fixture_path();
    run_twinning(&[
        "port",
        "--from-spec",
        spec.to_str().expect("spec path"),
        "--to-spec",
        spec.to_str().expect("spec path"),
        "--from-server-variable",
        "basePath=v2",
        "--to-server-variable",
        "basePath=v3",
        "--client-cmd",
        client_cmd,
        "--report",
        report_path.to_str().expect("report path"),
        "--json",
    ])
}

#[test]
fn port_cli_reports_equivalent_when_sessions_match() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("port-proof.json");
    let client_cmd = r#"case "$TWIN_FROM_URL" in http://127.0.0.1:*) ;; *) exit 41;; esac; case "$TWIN_TO_URL" in http://127.0.0.1:*) ;; *) exit 42;; esac; curl -fsS -H 'Authorization: Bearer test-token' "$TWIN_FROM_URL/files" >/dev/null; curl -fsS -H 'Authorization: Bearer test-token' "$TWIN_TO_URL/files" >/dev/null"#;

    let output = run_port(client_cmd, &report_path);

    assert_success(&output);
    let stdout = parse_stdout(&output);
    assert_eq!(stdout["version"], "twinning.port-proof.v0");
    assert_eq!(stdout["state_mode"], "independent");
    assert_eq!(stdout["verdict"], "EQUIVALENT");
    assert_eq!(stdout["source_session"]["request_count"], 1);
    assert_eq!(stdout["target_session"]["request_count"], 1);
    assert_eq!(stdout["missing_operations"], serde_json::json!([]));
    assert_eq!(stdout["missing_response_handling"], serde_json::json!([]));
    assert_eq!(stdout["missing_error_paths"], serde_json::json!([]));

    let report: Value =
        serde_json::from_str(&fs::read_to_string(report_path).expect("report file"))
            .expect("report JSON");
    assert_eq!(report, stdout);
}

#[test]
fn port_cli_reports_incomplete_when_target_misses_operation() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("port-proof.json");
    let output = run_port(
        r#"curl -fsS -H 'Authorization: Bearer test-token' "$TWIN_FROM_URL/files" >/dev/null"#,
        &report_path,
    );

    assert_success(&output);
    let stdout = parse_stdout(&output);
    assert_eq!(stdout["verdict"], "INCOMPLETE");
    assert_eq!(stdout["source_session"]["request_count"], 1);
    assert_eq!(stdout["target_session"]["request_count"], 0);
    assert!(
        stdout["missing_operations"]
            .as_array()
            .expect("missing operations")
            .iter()
            .any(|operation| operation == "GET /files"),
        "missing operation should identify the target gap: {stdout}"
    );
}

#[test]
fn port_cli_can_compare_openfigi_v2_and_v3_server_variable_twins() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("openfigi-port-proof.json");
    let client_cmd = r#"curl -fsS -H 'X-OPENFIGI-APIKEY: test-token' "$TWIN_FROM_URL/v2/mapping/values/idType" >/dev/null; curl -fsS -H 'X-OPENFIGI-APIKEY: test-token' "$TWIN_TO_URL/v3/mapping/values/idType" >/dev/null"#;

    let output = run_openfigi_v2_v3_port(client_cmd, &report_path);

    assert_success(&output);
    let stdout = parse_stdout(&output);
    assert_eq!(stdout["verdict"], "EQUIVALENT");
    assert_eq!(stdout["source_session"]["request_count"], 1);
    assert_eq!(stdout["target_session"]["request_count"], 1);
    assert_eq!(stdout["missing_operations"], serde_json::json!([]));
}

#[test]
fn port_cli_refuses_snapshot_flags_before_file_io() {
    let spec = fixture_path();
    let output = run_twinning(&[
        "port",
        "--from-spec",
        spec.to_str().expect("spec path"),
        "--to-spec",
        spec.to_str().expect("spec path"),
        "--client-cmd",
        "true",
        "--shared-snapshot",
        "missing-shared.twin",
        "--from-snapshot",
        "missing-from.twin",
        "--json",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let stdout = parse_stdout(&output);
    assert_eq!(stdout["outcome"], "REFUSAL");
    assert_eq!(
        stdout["refusal"]["code"],
        "E_PORT_SNAPSHOT_RESTORE_UNIMPLEMENTED"
    );
    assert!(
        stdout["refusal"]["message"]
            .as_str()
            .expect("message")
            .contains("snapshot restore is not implemented in v0")
    );
}

#[test]
fn port_proof_schema_file_is_checked_in_and_versioned() {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("twinning.port-proof.v0.schema.json");
    let schema: Value =
        serde_json::from_str(&fs::read_to_string(schema_path).expect("schema file"))
            .expect("schema JSON");

    assert_eq!(schema["title"], "twinning.port-proof.v0");
    assert_eq!(
        schema["properties"]["version"]["const"],
        "twinning.port-proof.v0"
    );
    assert_eq!(
        schema["properties"]["verdict"]["enum"],
        serde_json::json!(["EQUIVALENT", "PARTIAL", "INCOMPLETE"])
    );
}
