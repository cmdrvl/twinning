#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use twinning::{
    catalog::parse_postgres_schema,
    cli::Engine,
    declaration::load_catalog_declaration,
    snapshot::{SnapshotRelations, TwinSnapshot, write_snapshot},
};

const FIXTURE_ROOT: &str = "tests/fixtures/differential/twin_pair_migration_proof";

#[test]
fn proof_cli_writes_report_for_matching_snapshots() {
    let workspace = tempdir().expect("workspace");
    let left_snapshot = workspace.path().join("left.twin");
    let right_snapshot = workspace.path().join("right.twin");
    let report_path = workspace.path().join("proof.json");
    write_fixture_snapshot(&left_snapshot, "relations-pass-left.json", true);
    write_fixture_snapshot(&right_snapshot, "relations-pass-right.json", true);

    let output = Command::new(twinning_bin())
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("--left")
        .arg(&left_snapshot)
        .arg("--right")
        .arg(&right_snapshot)
        .arg("--queries")
        .arg(fixture_dir().join("cases.json"))
        .arg("--report")
        .arg(&report_path)
        .output()
        .expect("run proof cli");

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    assert!(
        output.status.success(),
        "proof CLI failed: stdout={stdout}; stderr={stderr}"
    );
    assert!(stderr.is_empty(), "unexpected stderr: {stderr}");

    let report: Value = serde_json::from_str(&stdout).expect("parse proof stdout");
    let written_report: Value =
        serde_json::from_str(&fs::read_to_string(&report_path).expect("read proof report"))
            .expect("parse written report");
    assert_eq!(report, written_report);
    assert_eq!(report["version"], "twinning.twin-pair-proof.v0");
    assert_eq!(report["outcome"], "PASS");
    assert!(report.get("score").is_none());
    assert_eq!(report["endpoints"].as_array().expect("endpoints").len(), 2);
    assert_eq!(
        report["endpoints"][0]["evidence_identities"]
            .as_array()
            .map(Vec::len),
        None
    );
    assert_eq!(
        report["endpoints"][1]["evidence_identities"]
            .as_array()
            .expect("right endpoint evidence identities")
            .len(),
        3
    );
    assert_eq!(
        report["endpoints"][1]["evidence_identities"][0]["artifact_kind"],
        "verify"
    );
    let verify_identity = report["endpoints"][1]["evidence_identities"][0]
        .as_object()
        .expect("verify evidence identity");
    assert!(!verify_identity.contains_key("score"));
    assert!(!verify_identity.contains_key("outcome"));
    assert_eq!(
        report["endpoints"][1]["evidence_identities"][1]["artifact_kind"],
        "benchmark"
    );
    assert_eq!(
        report["endpoints"][1]["evidence_identities"][2]["artifact_kind"],
        "assess"
    );
    let cases = report["cases"].as_array().expect("cases");
    assert_eq!(cases.len(), 4);

    let sqlstate_case = cases
        .iter()
        .find(|case| case["case_id"] == "outside_subset_lookup")
        .expect("outside-subset SQLSTATE case");
    assert_eq!(sqlstate_case["left"]["result"]["sqlstate"], "42P01");

    let filtered_case = cases
        .iter()
        .find(|case| case["case_id"] == "tenant_b_filtered_scan")
        .expect("filtered-scan replay case");
    assert_eq!(
        filtered_case["left"]["result"]["rows"][0][0]["text"],
        "deal-002"
    );

    let aggregate_case = cases
        .iter()
        .find(|case| case["case_id"] == "tenant_b_deal_count")
        .expect("aggregate-count replay case");
    assert_eq!(aggregate_case["left"]["result"]["columns"][0], "deal_count");
    assert_eq!(aggregate_case["left"]["result"]["rows"][0][0]["integer"], 1);
}

#[test]
fn proof_cli_refuses_incompatible_snapshot_declarations() {
    let workspace = tempdir().expect("workspace");
    let left_snapshot = workspace.path().join("left.twin");
    let right_snapshot = workspace.path().join("right.twin");
    write_fixture_snapshot(&left_snapshot, "relations-pass-left.json", true);
    write_fixture_snapshot(&right_snapshot, "relations-pass-right.json", false);

    let output = Command::new(twinning_bin())
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("--left")
        .arg(&left_snapshot)
        .arg("--right")
        .arg(&right_snapshot)
        .arg("--queries")
        .arg(fixture_dir().join("cases.json"))
        .output()
        .expect("run proof cli");

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    assert_eq!(output.status.code(), Some(2), "stdout={stdout}");
    assert!(stderr.is_empty(), "unexpected stderr: {stderr}");

    let refusal: Value = serde_json::from_str(&stdout).expect("parse refusal");
    assert_eq!(refusal["outcome"], "REFUSAL");
    assert_eq!(refusal["refusal"]["code"], "E_TWIN_PAIR_PROOF");
}

fn write_fixture_snapshot(path: &Path, relations_file: &str, attach_declaration: bool) {
    let schema_bytes = fs::read(schema_path()).expect("read schema");
    let schema_hash = sha256_prefixed(&schema_bytes);
    let catalog = parse_postgres_schema(&String::from_utf8(schema_bytes).expect("schema utf-8"))
        .expect("parse schema");
    let declaration = attach_declaration
        .then(|| load_catalog_declaration(&declaration_path(), &schema_hash, &catalog))
        .transpose()
        .expect("load declaration");
    let relations: SnapshotRelations =
        serde_json::from_str(&fs::read_to_string(fixture_dir().join(relations_file)).unwrap())
            .expect("parse relations");

    let snapshot = TwinSnapshot::new(
        Engine::Postgres,
        schema_path().display().to_string(),
        schema_hash,
        None,
        None,
        catalog,
    )
    .expect("snapshot")
    .with_catalog_declaration(declaration)
    .expect("attach declaration")
    .with_relations(relations)
    .expect("attach relations");
    write_snapshot(path, &snapshot).expect("write snapshot");
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn schema_path() -> PathBuf {
    fixture_dir().join("schema.sql")
}

fn declaration_path() -> PathBuf {
    fixture_dir().join("declaration.json")
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}
