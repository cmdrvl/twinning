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

fn lineage_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("lineage-bulk.yaml")
}

fn run_twinning_rest_run(report_path: &std::path::Path) -> Output {
    run_twinning_rest_run_with_spec(
        fixture_path(),
        report_path,
        r#"curl -fsS -H 'Authorization: Bearer test-token' "$TWIN_BASE_URL/files" >/dev/null"#,
    )
}

fn run_twinning_rest_run_with_spec(
    spec_path: PathBuf,
    report_path: &Path,
    command: &str,
) -> Output {
    Command::new(twinning_bin())
        .arg("rest")
        .arg("--spec")
        .arg(spec_path)
        .arg("--port")
        .arg("0")
        .arg("--run")
        .arg(command)
        .arg("--report")
        .arg(report_path)
        .arg("--json")
        .output()
        .expect("run twinning rest")
}

#[test]
fn rest_run_mode_injects_base_url_and_writes_report() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("rest-run-report.json");

    let output = run_twinning_rest_run(&report_path);

    assert!(
        output.status.success(),
        "twinning rest should exit 0: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: Value = serde_json::from_slice(&output.stdout).expect("rest run JSON stdout");
    assert_eq!(stdout["version"], "twinning.rest-run.v0");
    assert_eq!(stdout["outcome"], "PASS");
    assert_eq!(stdout["child"]["exit_code"], 0);
    assert_eq!(
        stdout["child"]["command"],
        r#"curl -fsS -H 'Authorization: Bearer test-token' "$TWIN_BASE_URL/files" >/dev/null"#
    );

    let report_raw = fs::read_to_string(&report_path).expect("REST run report should be written");
    let report: Value = serde_json::from_str(&report_raw).expect("REST report JSON");
    assert_eq!(report["version"], "twinning.rest-report.v0");
    assert_eq!(report["session"]["request_count"], 1);
    assert!(
        report["session"]["endpoints_exercised"]
            .as_array()
            .expect("endpoints array")
            .iter()
            .any(|endpoint| endpoint == "GET /files"),
        "GET /files should be recorded as exercised: {report_raw}"
    );
}

#[test]
fn rest_run_mode_validates_lineage_request_body_before_unsupported_shape() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("lineage-rest-run-report.json");
    let valid_body_path = dir.path().join("valid-lineage.json");
    let invalid_body_path = dir.path().join("invalid-lineage.json");
    let valid_response_path = dir.path().join("valid-response.json");
    let invalid_response_path = dir.path().join("invalid-response.json");

    fs::write(
        &valid_body_path,
        r#"{"edges":[{"upstream_key":"table://warehouse.raw/orders","downstream_key":"table://warehouse.mart/orders","source":"sql_lineage","confidence":null}]}"#,
    )
    .expect("write valid body");
    fs::write(&invalid_body_path, r#"{"edges":"not-array"}"#).expect("write invalid body");

    let command = format!(
        "curl -sS -H 'Content-Type: application/json' --data-binary @{} -o {} \"$TWIN_BASE_URL/metadata-api/v2/lineage/bulk\" && curl -sS -H 'Content-Type: application/json' --data-binary @{} -o {} \"$TWIN_BASE_URL/metadata-api/v2/lineage/bulk\"",
        shell_path(&valid_body_path),
        shell_path(&valid_response_path),
        shell_path(&invalid_body_path),
        shell_path(&invalid_response_path),
    );
    let output = run_twinning_rest_run_with_spec(lineage_fixture_path(), &report_path, &command);

    assert!(
        output.status.success(),
        "twinning rest lineage run should exit 0: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let valid_response: Value = serde_json::from_str(
        &fs::read_to_string(&valid_response_path).expect("valid response file"),
    )
    .expect("valid response JSON");
    assert_eq!(
        valid_response,
        serde_json::json!({ "request_valid": true, "stubbed": true })
    );

    let invalid_response: Value = serde_json::from_str(
        &fs::read_to_string(&invalid_response_path).expect("invalid response file"),
    )
    .expect("invalid response JSON");
    assert_eq!(invalid_response["code"], "schema_validation_failed");
    assert_eq!(invalid_response["path"], "$.edges");
    assert_eq!(invalid_response["expected"], "array");

    let report_raw = fs::read_to_string(&report_path).expect("REST run report should be written");
    let report: Value = serde_json::from_str(&report_raw).expect("REST report JSON");
    assert_eq!(report["session"]["request_count"], 2);
    assert_eq!(report["session"]["response_stubs"]["lineage_bulk_ok"], 1);
    assert_eq!(report["session"]["outcomes"]["response_stub"], 1);
    assert_eq!(
        report["session"]["outcomes"]["schema_validation_refusal"],
        1
    );
}

fn shell_path(path: &Path) -> String {
    format!("'{}'", path.display())
}
