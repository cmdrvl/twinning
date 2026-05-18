#![forbid(unsafe_code)]
#![cfg(feature = "postgres")]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde::Deserialize;
use serde_json::Value;
use tempfile::tempdir;
use twinning::{
    catalog::parse_postgres_schema,
    ir::{Operation, normalize_read_sql},
};

const FIXTURE_ROOT: &str = "tests/fixtures/doctor_detectors";

#[derive(Debug, Deserialize)]
struct DetectorManifest {
    version: String,
    required_before_fix: Vec<String>,
    detectors: Vec<ManifestDetector>,
}

#[derive(Debug, Deserialize)]
struct ManifestDetector {
    id: String,
    fixture: String,
    expected: ExpectedSignal,
}

#[derive(Debug, Deserialize)]
struct ExpectedSignal {
    kind: String,
    code: String,
    sqlstate: Option<String>,
    json_path: Option<String>,
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn repo_path(path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn path_arg(path: impl AsRef<Path>) -> String {
    path.as_ref().display().to_string()
}

fn run_twinning<I, S>(args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    Command::new(twinning_bin())
        .args(args)
        .output()
        .expect("run twinning")
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should be json")
}

fn assert_success(output: &Output) -> Result<(), String> {
    if output.status.success() {
        return Ok(());
    }

    Err(format!(
        "expected success, got status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn assert_refusal(output: &Output, expected_code: &str) -> Result<Value, String> {
    if output.status.code() != Some(2) {
        return Err(format!(
            "expected refusal exit 2, got status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let json = stdout_json(output);
    assert_eq!(json["outcome"], "REFUSAL");
    assert_eq!(json["refusal"]["code"], expected_code);
    Ok(json)
}

fn load_manifest() -> DetectorManifest {
    let path = repo_path(format!("{FIXTURE_ROOT}/manifest.json"));
    serde_json::from_str(&fs::read_to_string(path).expect("read detector manifest"))
        .expect("parse detector manifest")
}

fn detector_map(json: &Value) -> Result<BTreeMap<String, Value>, String> {
    let detectors = json["detectors"]
        .as_array()
        .ok_or_else(|| "doctor output should include detectors array".to_owned())?;

    Ok(detectors
        .iter()
        .map(|detector| {
            let id = detector["id"]
                .as_str()
                .expect("detector id should be string")
                .to_owned();
            (id, detector.clone())
        })
        .collect())
}

#[test]
fn doctor_capabilities_detector_catalog_matches_fixture_manifest() -> Result<(), String> {
    let manifest = load_manifest();
    assert_eq!(manifest.version, "twinning.doctor-detector-fixtures.v0");

    let output = run_twinning(["doctor", "capabilities", "--json"]);
    assert_success(&output)?;
    let capabilities_json = stdout_json(&output);
    let capability_detectors = detector_map(&capabilities_json)?;

    let triage_output = run_twinning(["doctor", "--robot-triage"]);
    assert_success(&triage_output)?;
    let triage_json = stdout_json(&triage_output);
    let triage_detectors = detector_map(&triage_json)?;

    let manifest_ids = manifest
        .detectors
        .iter()
        .map(|detector| detector.id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        capability_detectors
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        manifest_ids
    );
    assert_eq!(
        triage_detectors.keys().cloned().collect::<BTreeSet<_>>(),
        manifest_ids
    );

    for manifest_detector in &manifest.detectors {
        let detector = capability_detectors
            .get(&manifest_detector.id)
            .ok_or_else(|| format!("missing detector {}", manifest_detector.id))?;
        assert_eq!(detector["fixture"], manifest_detector.fixture);
        assert_eq!(detector["signal"]["kind"], manifest_detector.expected.kind);
        assert_eq!(detector["signal"]["code"], manifest_detector.expected.code);
        assert_eq!(
            detector["signal"]["sqlstate"]
                .as_str()
                .map(ToOwned::to_owned),
            manifest_detector.expected.sqlstate
        );
        assert_eq!(
            detector["signal"]["json_path"]
                .as_str()
                .map(ToOwned::to_owned),
            manifest_detector.expected.json_path
        );
        assert_eq!(detector["fix_available"], false);

        let required_before_fix = detector["required_before_fix"]
            .as_array()
            .ok_or_else(|| format!("{} missing fix requirements", manifest_detector.id))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .expect("fix requirement should be string")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(required_before_fix, manifest.required_before_fix);
        assert!(
            repo_path(&manifest_detector.fixture).exists(),
            "detector fixture should exist: {}",
            manifest_detector.fixture
        );
    }

    Ok(())
}

#[test]
fn malformed_postgres_ddl_fixture_refuses_schema_parse() -> Result<(), String> {
    let schema = repo_path(format!("{FIXTURE_ROOT}/malformed_postgres_ddl/schema.sql"));
    let output = run_twinning(["postgres", "--schema", &path_arg(schema), "--json"]);

    assert_refusal(&output, "E_SCHEMA_PARSE")?;
    Ok(())
}

#[test]
fn unsupported_sql_shape_fixture_stays_manifest_backed_refusal() -> Result<(), String> {
    let schema = fs::read_to_string(repo_path(format!(
        "{FIXTURE_ROOT}/catalog_drift/before.sql"
    )))
    .map_err(|error| error.to_string())?;
    let query = fs::read_to_string(repo_path(format!(
        "{FIXTURE_ROOT}/unsupported_sql_shape/query.sql"
    )))
    .map_err(|error| error.to_string())?;
    let catalog = parse_postgres_schema(&schema).map_err(|error| error.to_string())?;

    let operation = normalize_read_sql(&catalog, "doctor-detector", &query);
    let Operation::Refusal(refusal) = operation else {
        return Err(format!(
            "unsupported SQL fixture should normalize to refusal, got {operation:?}"
        ));
    };

    assert_eq!(refusal.code, "unsupported_shape");
    assert_eq!(
        load_manifest()
            .detectors
            .iter()
            .find(|detector| detector.id == "unsupported_sql_shape")
            .and_then(|detector| detector.expected.sqlstate.as_deref()),
        Some("0A000")
    );
    Ok(())
}

#[test]
fn catalog_drift_fixture_changes_normalized_catalog() -> Result<(), String> {
    let before = fs::read_to_string(repo_path(format!(
        "{FIXTURE_ROOT}/catalog_drift/before.sql"
    )))
    .map_err(|error| error.to_string())?;
    let after = fs::read_to_string(repo_path(format!("{FIXTURE_ROOT}/catalog_drift/after.sql")))
        .map_err(|error| error.to_string())?;

    let before_catalog = parse_postgres_schema(&before).map_err(|error| error.to_string())?;
    let after_catalog = parse_postgres_schema(&after).map_err(|error| error.to_string())?;

    assert_ne!(before_catalog, after_catalog);
    assert_eq!(before_catalog.table_count, after_catalog.table_count);
    assert_ne!(before_catalog.column_count, after_catalog.column_count);
    Ok(())
}

#[test]
fn snapshot_detector_fixture_catches_tampered_hash() -> Result<(), String> {
    let dir = tempdir().map_err(|error| error.to_string())?;
    let schema = repo_path(format!("{FIXTURE_ROOT}/tampered_snapshot/schema.sql"));
    let snapshot = dir.path().join("bootstrap.twin");

    let write_output = run_twinning([
        "postgres",
        "--schema",
        &path_arg(schema),
        "--snapshot",
        &path_arg(&snapshot),
        "--json",
    ]);
    assert_success(&write_output)?;

    let mut snapshot_json: Value =
        serde_json::from_str(&fs::read_to_string(&snapshot).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    snapshot_json["snapshot_hash"] = Value::String("sha256:tampered".to_owned());
    fs::write(
        &snapshot,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&snapshot_json).map_err(|error| error.to_string())?
        ),
    )
    .map_err(|error| error.to_string())?;

    let restore_output = run_twinning(["postgres", "--restore", &path_arg(snapshot), "--json"]);
    assert_refusal(&restore_output, "E_SNAPSHOT_VERIFY")?;
    Ok(())
}

#[test]
fn batch_only_verify_fixture_refuses_live_mode() -> Result<(), String> {
    let schema = repo_path(format!(
        "{FIXTURE_ROOT}/batch_only_verify_artifact/schema.sql"
    ));
    let verify = repo_path(format!(
        "{FIXTURE_ROOT}/batch_only_verify_artifact/constraints.verify.json"
    ));

    let output = run_twinning([
        "postgres",
        "--schema",
        &path_arg(schema),
        "--verify",
        &path_arg(verify),
        "--run",
        "true",
        "--json",
    ]);
    let json = assert_refusal(&output, "E_BATCH_ONLY_RULE")?;
    assert_eq!(
        json["refusal"]["detail"]["rule_id"],
        "doctor_batch_only_query"
    );
    Ok(())
}

#[test]
fn pgwire_bind_failure_fixture_refuses_listener_collision() -> Result<(), String> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|error| error.to_string())?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port()
        .to_string();
    let schema = repo_path(format!("{FIXTURE_ROOT}/pgwire_bind_failure/schema.sql"));

    let output = run_twinning([
        "postgres",
        "--schema",
        &path_arg(schema),
        "--host",
        "127.0.0.1",
        "--port",
        &port,
        "--run",
        "true",
        "--json",
    ]);
    let json = assert_refusal(&output, "E_RUNTIME_IO")?;
    assert_eq!(json["refusal"]["detail"]["stage"], "listener_bind");
    Ok(())
}

#[test]
fn run_once_child_failure_fixture_preserves_metadata() -> Result<(), String> {
    let schema = repo_path(format!("{FIXTURE_ROOT}/run_once_child_failure/schema.sql"));
    let output = run_twinning([
        "postgres",
        "--schema",
        &path_arg(schema),
        "--host",
        "127.0.0.1",
        "--port",
        "0",
        "--run",
        "exit 7",
        "--json",
    ]);
    assert_success(&output)?;

    let json = stdout_json(&output);
    assert_eq!(json["outcome"], "FAIL");
    assert_eq!(json["mode"], "run_once");
    assert_eq!(json["run"]["exit_code"], 7);
    assert_eq!(json["run"]["timed_out"], false);
    assert_eq!(json["run"]["command"], "exit 7");
    Ok(())
}
