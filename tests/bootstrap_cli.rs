#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::Value;
use tempfile::tempdir;

const NEXT_STEP: &str = "Bootstrap mode validated the schema assets and deterministic artifact path. Use --run to exercise the declared live Postgres subset, or stay in bootstrap mode while broader protocol and SQL coverage lands.";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fixture_path(name: &str) -> PathBuf {
    repo_root()
        .join("tests")
        .join("fixtures")
        .join("bootstrap_cli")
        .join(name)
}

fn twinning_binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_twinning")
        .map(PathBuf::from)
        .expect("cargo should provide the compiled twinning binary path")
}

fn run_twinning(args: &[&str]) -> Output {
    Command::new(twinning_binary())
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

fn parse_json(path: &Path) -> Value {
    let raw = fs::read_to_string(path).expect("read json file");
    serde_json::from_str(&raw).expect("parse json")
}

#[test]
fn bootstrap_cli_writes_expected_json_report() {
    let schema_path = fixture_path("schema.sql");
    let verify_path = fixture_path("verify.json");
    let tempdir = tempdir().expect("tempdir");
    let report_path = tempdir.path().join("out").join("bootstrap.json");
    let snapshot_path = tempdir.path().join("out").join("bootstrap.twin");

    let output = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path utf-8"),
        "--verify",
        verify_path.to_str().expect("verify path utf-8"),
        "--report",
        report_path.to_str().expect("report path utf-8"),
        "--snapshot",
        snapshot_path.to_str().expect("snapshot path utf-8"),
        "--json",
    ]);
    assert_success(&output);

    let stdout_json: Value =
        serde_json::from_slice(&output.stdout).expect("stdout should contain report json");
    let report_json = parse_json(&report_path);

    assert_eq!(stdout_json, report_json);
    assert_eq!(report_json["version"], "twinning.v0");
    assert_eq!(report_json["outcome"], "READY");
    assert_eq!(report_json["mode"], "bootstrap");
    assert_eq!(report_json["engine"], "postgres");
    assert_eq!(report_json["host"], "127.0.0.1");
    assert_eq!(report_json["port"], 5432);
    assert_eq!(report_json["wire_protocol"], "planned.pgwire");
    assert_eq!(
        report_json["schema"]["source"],
        schema_path.display().to_string()
    );
    assert_eq!(report_json["schema"]["table_count"], 2);
    assert_eq!(report_json["schema"]["column_count"], 6);
    assert_eq!(report_json["schema"]["index_count"], 1);
    assert_eq!(report_json["schema"]["constraint_count"], 5);
    assert_eq!(
        report_json["verify_artifact"]["source"],
        verify_path.display().to_string()
    );
    assert_eq!(report_json["verify_artifact"]["loaded"], 2);
    assert_eq!(report_json["catalog"]["dialect"], "postgres");
    assert_eq!(report_json["tables"]["public.deals"]["columns"], 4);
    assert_eq!(report_json["tables"]["public.deals"]["indexes"], 1);
    assert_eq!(report_json["tables"]["public.deals"]["constraints"], 3);
    assert_eq!(report_json["tables"]["public.tenants"]["columns"], 2);
    assert_eq!(report_json["tables"]["public.tenants"]["indexes"], 0);
    assert_eq!(report_json["tables"]["public.tenants"]["constraints"], 2);
    assert_eq!(report_json["constraints"]["not_null_violations"], 0);
    assert_eq!(
        report_json["snapshot"]["written_to"],
        snapshot_path.display().to_string()
    );
    assert!(snapshot_path.exists(), "snapshot file should be written");
    assert!(
        report_json.get("warnings").is_none(),
        "warnings should be omitted when absent"
    );
    assert!(
        report_json.get("verify").is_none(),
        "verify payload should be omitted in Phase-0 bootstrap mode"
    );
    assert_eq!(report_json["next_step"], NEXT_STEP);
}

#[test]
fn bootstrap_cli_human_output_matches_operator_next_step() {
    let schema_path = fixture_path("schema.sql");
    let verify_path = fixture_path("verify.json");

    let output = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path utf-8"),
        "--verify",
        verify_path.to_str().expect("verify path utf-8"),
    ]);
    assert_success(&output);

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    assert!(stdout.starts_with("twinning postgres bootstrap ready\n"));
    assert!(stdout.contains("endpoint: 127.0.0.1:5432\n"));
    assert!(stdout.contains(&format!(
        "schema: {} (2 tables, 6 columns, 1 indexes, hash sha256:",
        schema_path.display()
    )));
    assert!(stdout.contains(&format!(
        "verify: {} (2 loaded, hash sha256:",
        verify_path.display()
    )));
    assert!(stdout.contains(&format!("next: {NEXT_STEP}\n")));
}
