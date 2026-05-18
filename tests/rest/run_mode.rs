use std::{
    fs,
    path::PathBuf,
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

fn run_twinning_rest_run(report_path: &std::path::Path) -> Output {
    Command::new(twinning_bin())
        .arg("rest")
        .arg("--spec")
        .arg(fixture_path())
        .arg("--port")
        .arg("0")
        .arg("--run")
        .arg(r#"curl -fsS -H 'Authorization: Bearer test-token' "$TWIN_BASE_URL/files" >/dev/null"#)
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
