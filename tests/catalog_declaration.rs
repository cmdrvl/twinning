#![forbid(unsafe_code)]

use std::{fs, path::PathBuf, process::Command};

use serde_json::{Value, json};
use tempfile::tempdir;
use twinning::snapshot::read_snapshot;

const FIXTURE_ROOT: &str = "tests/fixtures/declarations/procurement_subset";

#[test]
fn procurement_declaration_is_preserved_in_report_and_snapshot() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("report.json");
    let snapshot_path = dir.path().join("bootstrap.twin");

    let output = run_twinning([
        "postgres",
        "--schema",
        &path_arg(schema_path()),
        "--declaration",
        &path_arg(declaration_path()),
        "--report",
        &path_arg(&report_path),
        "--snapshot",
        &path_arg(&snapshot_path),
        "--json",
    ]);
    assert_eq!(output.status.code(), Some(0), "stdout={}", output.stdout);

    let stdout: Value = serde_json::from_str(&output.stdout).expect("parse stdout report");
    let written_report: Value =
        serde_json::from_str(&fs::read_to_string(&report_path).expect("read report"))
            .expect("parse written report");
    let snapshot = read_snapshot(&snapshot_path).expect("read snapshot");
    let declaration = stdout["catalog_declaration"].clone();

    assert_eq!(stdout["outcome"], "READY");
    assert_eq!(declaration, written_report["catalog_declaration"]);
    assert_eq!(declaration["version"], "twinning.catalog-declaration.v0");
    assert_eq!(
        declaration["declaration_id"],
        "metadata-catalog:procurement-outcome:v0"
    );
    assert_eq!(declaration["outcome_tags"], json!(["procurement"]));
    assert_eq!(
        declaration["table_keys"]["public.procurement_evidence"],
        "metadata.tables.public.procurement_evidence"
    );

    let snapshot_declaration = serde_json::to_value(
        snapshot
            .catalog_declaration
            .expect("snapshot should preserve declaration identity"),
    )
    .expect("serialize snapshot declaration");
    assert_eq!(snapshot_declaration, declaration);
}

#[test]
fn declaration_loader_refuses_malformed_json_and_schema_hash_drift() {
    let dir = tempdir().expect("tempdir");
    let malformed = dir.path().join("declaration-malformed.json");
    fs::write(&malformed, "{ not-json").expect("write malformed declaration");
    assert_declaration_refuses(&malformed, "E_DECLARATION_PARSE");

    let drifted = dir.path().join("declaration-drifted.json");
    let mut declaration = fixture_declaration();
    declaration["schema_hash"] = Value::String(format!("sha256:{}", "2".repeat(64)));
    write_declaration(&drifted, &declaration);
    assert_declaration_refuses(&drifted, "E_DECLARATION_PARSE");

    let bad_fingerprint = dir.path().join("declaration-bad-fingerprint.json");
    let mut declaration = fixture_declaration();
    declaration["source_deployment_fingerprint"] = Value::String("sha256:ABC".to_owned());
    write_declaration(&bad_fingerprint, &declaration);
    assert_declaration_refuses(&bad_fingerprint, "E_DECLARATION_PARSE");

    let fk_drifted = dir.path().join("declaration-fk-drifted.json");
    let mut declaration = fixture_declaration();
    declaration["tables"][2]["foreign_keys"][0]["references_columns"] = json!(["source_id"]);
    write_declaration(&fk_drifted, &declaration);
    assert_declaration_refuses(&fk_drifted, "E_DECLARATION_PARSE");
}

fn assert_declaration_refuses(declaration_path: &PathBuf, code: &str) {
    let output = run_twinning([
        "postgres",
        "--schema",
        &path_arg(schema_path()),
        "--declaration",
        &path_arg(declaration_path),
        "--json",
    ]);
    assert_refusal(&output, code);
}

fn assert_refusal(output: &TwinningOutput, code: &str) {
    assert_eq!(output.status.code(), Some(2), "stdout={}", output.stdout);
    let json: Value = serde_json::from_str(&output.stdout).expect("parse refusal");
    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], code);
}

fn fixture_declaration() -> Value {
    serde_json::from_str(&fs::read_to_string(declaration_path()).expect("read declaration"))
        .expect("parse declaration")
}

fn write_declaration(path: &PathBuf, declaration: &Value) {
    fs::write(
        path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(declaration).expect("render declaration")
        ),
    )
    .expect("write declaration");
}

struct TwinningOutput {
    status: std::process::ExitStatus,
    stdout: String,
}

fn run_twinning<const N: usize>(args: [&str; N]) -> TwinningOutput {
    let output = Command::new(twinning_bin())
        .args(args)
        .output()
        .expect("run twinning");

    assert!(
        output.stderr.is_empty(),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    TwinningOutput {
        status: output.status,
        stdout: String::from_utf8(output.stdout).expect("stdout utf-8"),
    }
}

fn schema_path() -> PathBuf {
    repo_root().join(FIXTURE_ROOT).join("schema.sql")
}

fn declaration_path() -> PathBuf {
    repo_root().join(FIXTURE_ROOT).join("declaration.json")
}

fn path_arg(path: impl Into<PathBuf>) -> String {
    path.into().display().to_string()
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
