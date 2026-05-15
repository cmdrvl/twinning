#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::{Value, json};
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
    assert_eq!(cases.len(), 5);

    let sqlstate_case = cases
        .iter()
        .find(|case| case["case_id"] == "outside_subset_lookup")
        .expect("outside-subset SQLSTATE case");
    assert_eq!(sqlstate_case["left"]["result"]["sqlstate"], "42P01");
    assert!(sqlstate_case["left"]["result"].get("rows").is_none());
    assert!(
        sqlstate_case["left"]["result"]["refusal_hash"]
            .as_str()
            .expect("refusal hash")
            .starts_with("sha256:")
    );
    assert_eq!(
        sqlstate_case["replay_result"]["sqlstate_parity"]["left"],
        "42P01"
    );
    assert_eq!(
        sqlstate_case["replay_result"]["sqlstate_parity"]["right"],
        "42P01"
    );
    assert_eq!(
        sqlstate_case["replay_result"]["sqlstate_parity"]["matches"],
        true
    );
    assert_eq!(
        sqlstate_case["replay_result"]["refusal_parity"]["matches"],
        true
    );
    assert_eq!(
        sqlstate_case["replay_result"]["left_snapshot_hash"],
        report["endpoints"][0]["snapshot_hash"]
    );
    assert_eq!(
        sqlstate_case["replay_result"]["right_snapshot_hash"],
        report["endpoints"][1]["snapshot_hash"]
    );
    assert!(sqlstate_case["replay_result"].get("duration_ms").is_none());
    assert!(sqlstate_case["replay_result"].get("score").is_none());

    let filtered_case = cases
        .iter()
        .find(|case| case["case_id"] == "tenant_b_filtered_scan")
        .expect("filtered-scan replay case");
    assert_eq!(filtered_case["left"]["result"]["row_count"], 1);
    assert_eq!(filtered_case["left"]["result"]["command_tag"], "SELECT 1");
    assert!(filtered_case["left"]["result"].get("rows").is_none());
    assert!(
        filtered_case["left"]["result"]["rows_hash"]
            .as_str()
            .expect("rows hash")
            .starts_with("sha256:")
    );

    let aggregate_case = cases
        .iter()
        .find(|case| case["case_id"] == "tenant_b_deal_count")
        .expect("aggregate-count replay case");
    assert_eq!(aggregate_case["left"]["result"]["columns"][0], "deal_count");
    assert_eq!(aggregate_case["left"]["result"]["row_count"], 1);
    assert!(aggregate_case["left"]["result"].get("rows").is_none());

    let skip_case = cases
        .iter()
        .find(|case| case["case_id"] == "join_shape_deferred")
        .expect("skip replay case");
    assert_eq!(skip_case["verdict"], "skip");
    assert_eq!(skip_case["left"]["result"]["outcome_class"], "skip");
    assert_eq!(
        skip_case["replay_result"]["skip_reason_code"],
        "join_replay_deferred"
    );
    assert_eq!(
        skip_case["replay_result"]["ordering_policy_parity"]["matches"],
        true
    );

    let second_output = Command::new(twinning_bin())
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
        .expect("rerun proof cli");
    let second_stdout = String::from_utf8(second_output.stdout).expect("stdout utf-8");
    let second_stderr = String::from_utf8(second_output.stderr).expect("stderr utf-8");
    assert!(
        second_output.status.success(),
        "proof CLI rerun failed: stdout={second_stdout}; stderr={second_stderr}"
    );
    assert!(
        second_stderr.is_empty(),
        "unexpected stderr: {second_stderr}"
    );
    let second_report: Value = serde_json::from_str(&second_stdout).expect("parse rerun report");
    assert_eq!(second_report, report);
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

#[test]
fn proof_cli_orchestrates_restore_manifest_and_writes_outputs() {
    let workspace = tempdir().expect("workspace");
    let left_input = workspace.path().join("input-left.twin");
    let right_input = workspace.path().join("input-right.twin");
    let left_output = workspace.path().join("out").join("legacy.twin");
    let right_output = workspace.path().join("out").join("candidate.twin");
    let report_path = workspace.path().join("out").join("proof.json");
    let bundle_dir = workspace.path().join("bundle");
    let manifest_path = workspace.path().join("manifest.json");
    write_fixture_snapshot(&left_input, "relations-pass-left.json", true);
    write_fixture_snapshot(&right_input, "relations-pass-right.json", true);
    write_json(
        &manifest_path,
        &restore_orchestration_manifest(
            &left_input,
            &right_input,
            &report_path,
            &left_output,
            &right_output,
            &bundle_dir,
        ),
    );

    let output = Command::new(twinning_bin())
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("orchestrate")
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .expect("run proof orchestrator");

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    assert!(
        output.status.success(),
        "proof orchestrator failed: stdout={stdout}; stderr={stderr}"
    );
    assert!(stderr.is_empty(), "unexpected stderr: {stderr}");
    assert!(bundle_dir.is_dir());

    let report: Value = serde_json::from_str(&stdout).expect("parse proof stdout");
    let written_report: Value =
        serde_json::from_str(&fs::read_to_string(&report_path).expect("read proof report"))
            .expect("parse written report");
    assert_eq!(report, written_report);
    assert_eq!(report["proof_id"], "loan-performance-migration-proof:test");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["endpoints"][0]["endpoint_id"], "legacy");
    assert_eq!(report["endpoints"][0]["role"], "legacy-source");
    assert_eq!(report["endpoints"][1]["endpoint_id"], "candidate");
    assert_eq!(report["endpoints"][1]["role"], "candidate-target");
    assert_eq!(
        report["endpoints"][1]["evidence_identities"]
            .as_array()
            .expect("target evidence")
            .len(),
        3
    );

    let left_snapshot: Value =
        serde_json::from_str(&fs::read_to_string(&left_output).expect("read left output"))
            .expect("parse left output");
    let right_snapshot: Value =
        serde_json::from_str(&fs::read_to_string(&right_output).expect("read right output"))
            .expect("parse right output");
    assert_eq!(
        left_snapshot["snapshot_hash"],
        report["endpoints"][0]["snapshot_hash"]
    );
    assert_eq!(
        right_snapshot["snapshot_hash"],
        report["endpoints"][1]["snapshot_hash"]
    );
    assert!(report.get("score").is_none());
}

#[test]
fn proof_cli_orchestrates_schema_load_materialized_endpoint_and_keeps_hashes_stable() {
    let workspace = tempdir().expect("workspace");
    let left_input = workspace.path().join("input-left.twin");
    let left_output = workspace.path().join("out").join("legacy.twin");
    let right_output = workspace.path().join("out").join("candidate.twin");
    let report_path = workspace.path().join("out").join("proof.json");
    let bundle_dir = workspace.path().join("bundle");
    let manifest_path = workspace.path().join("manifest.json");
    write_fixture_snapshot(&left_input, "relations-pass-left.json", true);

    let mut manifest = restore_orchestration_manifest(
        &left_input,
        &workspace.path().join("unused-right.twin"),
        &report_path,
        &left_output,
        &right_output,
        &bundle_dir,
    );
    manifest["right_endpoint"]["bootstrap"] = json!({
        "kind": "schema",
        "schema": schema_path().display().to_string(),
        "declaration": declaration_path().display().to_string(),
        "load": [load_path().display().to_string()]
    });
    write_json(&manifest_path, &manifest);

    let first_output = Command::new(twinning_bin())
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("orchestrate")
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .expect("run proof orchestrator");
    let first_stdout = String::from_utf8(first_output.stdout).expect("stdout utf-8");
    let first_stderr = String::from_utf8(first_output.stderr).expect("stderr utf-8");
    assert!(
        first_output.status.success(),
        "schema-load proof orchestrator failed: stdout={first_stdout}; stderr={first_stderr}"
    );
    assert!(first_stderr.is_empty(), "unexpected stderr: {first_stderr}");

    let first_report: Value = serde_json::from_str(&first_stdout).expect("parse first report");
    assert_eq!(first_report["outcome"], "PASS");
    assert_eq!(first_report["endpoints"][0]["endpoint_id"], "legacy");
    assert_eq!(first_report["endpoints"][1]["endpoint_id"], "candidate");
    assert_eq!(
        first_report["endpoints"][1]["source_materialization"]["method"],
        "manifest_sql_load"
    );
    assert_eq!(
        first_report["endpoints"][1]["source_materialization"]["row_count"],
        4
    );
    assert_eq!(
        first_report["endpoints"][1]["source_materialization"]["tables"]["public.deals"],
        2
    );
    assert!(
        first_report["endpoints"][1]["source_materialization"]["source_identity"]
            .as_str()
            .expect("source identity")
            .starts_with("sha256:")
    );
    assert!(
        first_report["endpoints"][0]
            .get("source_materialization")
            .is_none()
    );

    let right_snapshot: Value =
        serde_json::from_str(&fs::read_to_string(&right_output).expect("read right output"))
            .expect("parse right output");
    assert_eq!(
        right_snapshot["source_materialization"],
        first_report["endpoints"][1]["source_materialization"]
    );
    assert_eq!(right_snapshot["table_rows"]["public.deals"], 2);
    assert_eq!(right_snapshot["table_rows"]["public.tenants"], 2);
    assert_eq!(
        right_snapshot["snapshot_hash"],
        first_report["endpoints"][1]["snapshot_hash"]
    );

    let second_output = Command::new(twinning_bin())
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("orchestrate")
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .expect("rerun proof orchestrator");
    let second_stdout = String::from_utf8(second_output.stdout).expect("stdout utf-8");
    let second_stderr = String::from_utf8(second_output.stderr).expect("stderr utf-8");
    assert!(
        second_output.status.success(),
        "schema-load proof orchestrator rerun failed: stdout={second_stdout}; stderr={second_stderr}"
    );
    assert!(
        second_stderr.is_empty(),
        "unexpected stderr: {second_stderr}"
    );
    let second_report: Value = serde_json::from_str(&second_stdout).expect("parse second report");
    assert_eq!(
        second_report["endpoints"][1]["snapshot_hash"],
        first_report["endpoints"][1]["snapshot_hash"],
        "same schema and load data must produce the same materialized endpoint snapshot hash"
    );
    assert_eq!(second_report, first_report);
}

#[test]
fn proof_cli_orchestrate_refuses_unsupported_schema_load_statement() {
    let workspace = tempdir().expect("workspace");
    let left_input = workspace.path().join("input-left.twin");
    let left_output = workspace.path().join("out").join("legacy.twin");
    let right_output = workspace.path().join("out").join("candidate.twin");
    let report_path = workspace.path().join("out").join("proof.json");
    let bundle_dir = workspace.path().join("bundle");
    let manifest_path = workspace.path().join("manifest.json");
    let unsupported_load = workspace.path().join("unsupported-load.sql");
    write_fixture_snapshot(&left_input, "relations-pass-left.json", true);
    fs::write(&unsupported_load, "SELECT * FROM public.deals;\n").expect("write bad load");

    let mut manifest = restore_orchestration_manifest(
        &left_input,
        &workspace.path().join("unused-right.twin"),
        &report_path,
        &left_output,
        &right_output,
        &bundle_dir,
    );
    manifest["right_endpoint"]["bootstrap"] = json!({
        "kind": "schema",
        "schema": schema_path().display().to_string(),
        "declaration": declaration_path().display().to_string(),
        "load": [unsupported_load.display().to_string()]
    });
    write_json(&manifest_path, &manifest);

    let output = Command::new(twinning_bin())
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("orchestrate")
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .expect("run proof orchestrator");
    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    assert_eq!(output.status.code(), Some(2), "stdout={stdout}");
    assert!(stderr.is_empty(), "unexpected stderr: {stderr}");

    let refusal: Value = serde_json::from_str(&stdout).expect("parse refusal");
    assert_eq!(refusal["outcome"], "REFUSAL");
    assert_eq!(refusal["refusal"]["code"], "E_TWIN_PAIR_PROOF");
    assert_eq!(refusal["refusal"]["detail"]["endpoint_id"], "candidate");
    assert_eq!(
        refusal["refusal"]["detail"]["policy"],
        "manifest_sql_load_supports_declared_mutations_only"
    );
    assert!(!report_path.exists());
    assert!(!right_output.exists());
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

fn restore_orchestration_manifest(
    left_snapshot: &Path,
    right_snapshot: &Path,
    report_path: &Path,
    left_output: &Path,
    right_output: &Path,
    bundle_dir: &Path,
) -> Value {
    json!({
        "version": "twinning.twin-pair-orchestration-manifest.v0",
        "proof_id": "loan-performance-migration-proof:test",
        "catalog_declaration": {
            "path": declaration_path().display().to_string(),
            "hash": file_hash(&declaration_path())
        },
        "left_endpoint": {
            "endpoint_id": "legacy",
            "role": "legacy-source",
            "engine": "postgres",
            "bootstrap": {
                "kind": "restore",
                "snapshot": left_snapshot.display().to_string()
            }
        },
        "right_endpoint": {
            "endpoint_id": "candidate",
            "role": "candidate-target",
            "engine": "postgres",
            "bootstrap": {
                "kind": "restore",
                "snapshot": right_snapshot.display().to_string()
            }
        },
        "replay_manifest": {
            "path": fixture_dir().join("cases.json").display().to_string(),
            "hash": file_hash(&fixture_dir().join("cases.json"))
        },
        "target_evidence": [
            {
                "artifact_kind": "verify",
                "artifact_id": "verify:loan-performance-candidate:v1",
                "version": "verify.report.v1",
                "hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            },
            {
                "artifact_kind": "benchmark",
                "artifact_id": "benchmark:loan-performance-candidate:v1",
                "version": "benchmark.report.v1",
                "hash": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
            {
                "artifact_kind": "assess",
                "artifact_id": "assess:loan-performance-candidate:v1",
                "version": "assess.decision.v1",
                "hash": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            }
        ],
        "artifact_outputs": {
            "report": report_path.display().to_string(),
            "bundle_dir": bundle_dir.display().to_string(),
            "left_snapshot": left_output.display().to_string(),
            "right_snapshot": right_output.display().to_string()
        }
    })
}

fn write_json(path: &Path, value: &Value) {
    fs::write(
        path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(value).expect("render json")
        ),
    )
    .expect("write json");
}

fn file_hash(path: &Path) -> String {
    sha256_prefixed(&fs::read(path).expect("read hash input"))
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

fn load_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("differential")
        .join("twin_pair_orchestration")
        .join("load")
        .join("candidate.sql")
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}
