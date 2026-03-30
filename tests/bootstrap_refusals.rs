#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use tempfile::tempdir;

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn write_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("schema.sql");
    fs::write(
        &schema_path,
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            deal_name TEXT NOT NULL
        );
        "#,
    )
    .expect("write schema");
    schema_path
}

struct CommandResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
    json: Value,
}

fn run_twinning(args: &[&str]) -> CommandResult {
    let output = Command::new(twinning_bin())
        .args(args)
        .output()
        .expect("run twinning");
    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    let json = serde_json::from_str(&stdout).expect("parse twinning json output");

    CommandResult {
        exit_code: output.status.code().expect("process exit code"),
        stdout,
        stderr,
        json,
    }
}

fn assert_refusal(result: &CommandResult, code: &str) {
    assert_eq!(
        result.exit_code, 2,
        "expected refusal exit code 2, got stdout={}, stderr={}",
        result.stdout, result.stderr
    );
    assert!(
        result.stderr.is_empty(),
        "process-level refusal should render on stdout without stderr noise: {}",
        result.stderr
    );
    assert_eq!(result.json["version"], "twinning.v0");
    assert_eq!(result.json["outcome"], "REFUSAL");
    assert_eq!(result.json["refusal"]["code"], code);
    assert!(
        result.json["refusal"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "refusal message should be present: {}",
        result.stdout
    );
    assert!(
        result.json["refusal"]["detail"].is_object(),
        "refusal detail should stay structured: {}",
        result.stdout
    );
    assert!(
        result.json["refusal"].get("next_command").is_some(),
        "refusal envelope should keep the next_command field: {}",
        result.stdout
    );
}

#[test]
fn bootstrap_exit_domain_stays_zero_for_ready_and_two_for_current_bootstrap_refusals() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let schema = schema_path.to_str().expect("schema path");

    let ready = run_twinning(&["postgres", "--schema", schema, "--json"]);
    assert_eq!(
        ready.exit_code, 0,
        "implemented bootstrap path should exit cleanly: stdout={}, stderr={}",
        ready.stdout, ready.stderr
    );
    assert!(
        ready.stderr.is_empty(),
        "unexpected stderr: {}",
        ready.stderr
    );
    assert_eq!(ready.json["outcome"], "READY");

    let missing_source = run_twinning(&["postgres", "--json"]);
    assert_refusal(&missing_source, "E_BOOTSTRAP_SOURCE_REQUIRED");

    let engine_unimplemented = run_twinning(&["mysql", "--schema", schema, "--json"]);
    assert_refusal(&engine_unimplemented, "E_ENGINE_UNIMPLEMENTED");
}

#[test]
fn run_mode_keeps_child_failures_in_report_but_process_exit_stays_zero() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let schema = schema_path.to_str().expect("schema path");

    let run_mode = run_twinning(&["postgres", "--schema", schema, "--run", "exit 7", "--json"]);

    assert_eq!(
        run_mode.exit_code, 0,
        "child failure should stay in run metadata, not become a process-level refusal: stdout={}, stderr={}",
        run_mode.stdout, run_mode.stderr
    );
    assert!(
        run_mode.stderr.is_empty(),
        "unexpected stderr noise: {}",
        run_mode.stderr
    );
    assert_eq!(run_mode.json["version"], "twinning.v0");
    assert_eq!(run_mode.json["mode"], "run_once");
    assert_eq!(run_mode.json["outcome"], "FAIL");
    assert_eq!(run_mode.json["run"]["command"], "exit 7");
    assert_eq!(run_mode.json["run"]["exit_code"], 7);
    assert_eq!(run_mode.json["run"]["timed_out"], false);
}

#[test]
fn invalid_bootstrap_source_combinations_fail_before_file_io() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let restore_path = dir.path().join("missing.twin");

    let result = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--restore",
        restore_path.to_str().expect("restore path"),
        "--json",
    ]);

    assert_refusal(&result, "E_AMBIGUOUS_BOOTSTRAP_SOURCE");
    assert_eq!(
        result.json["refusal"]["next_command"],
        Value::String("twinning postgres --schema schema.sql --json".to_owned())
    );
}

#[test]
fn malformed_schema_and_verify_inputs_stay_process_level_refusals() {
    let dir = tempdir().expect("tempdir");
    let invalid_schema_path = dir.path().join("invalid.sql");
    let valid_schema_path = write_schema(dir.path());
    let invalid_verify_path = dir.path().join("invalid.verify.json");

    fs::write(&invalid_schema_path, "CREATE TABLE public.deals (").expect("write invalid schema");
    fs::write(&invalid_verify_path, "{ not-json").expect("write invalid verify");

    let schema_result = run_twinning(&[
        "postgres",
        "--schema",
        invalid_schema_path.to_str().expect("invalid schema path"),
        "--json",
    ]);
    assert_refusal(&schema_result, "E_SCHEMA_PARSE");

    let verify_result = run_twinning(&[
        "postgres",
        "--schema",
        valid_schema_path.to_str().expect("valid schema path"),
        "--verify",
        invalid_verify_path.to_str().expect("invalid verify path"),
        "--json",
    ]);
    assert_refusal(&verify_result, "E_VERIFY_ARTIFACT_PARSE");
}

#[test]
fn malformed_snapshots_exit_two_with_snapshot_refusals() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let valid_snapshot_path = dir.path().join("bootstrap.twin");
    let invalid_snapshot_path = dir.path().join("invalid.twin");

    fs::write(&invalid_snapshot_path, "{ not-json").expect("write invalid snapshot");

    let invalid_json_result = run_twinning(&[
        "postgres",
        "--restore",
        invalid_snapshot_path
            .to_str()
            .expect("invalid snapshot path"),
        "--json",
    ]);
    assert_refusal(&invalid_json_result, "E_SNAPSHOT_VERIFY");

    let written_snapshot = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--snapshot",
        valid_snapshot_path.to_str().expect("valid snapshot path"),
        "--json",
    ]);
    assert_eq!(written_snapshot.exit_code, 0);

    let mut tampered_snapshot: Value =
        serde_json::from_str(&fs::read_to_string(&valid_snapshot_path).expect("read snapshot"))
            .expect("parse snapshot json");
    tampered_snapshot["snapshot_hash"] = Value::String("sha256:tampered".to_owned());
    fs::write(
        &valid_snapshot_path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&tampered_snapshot).expect("render tampered snapshot")
        ),
    )
    .expect("rewrite snapshot");

    let tampered_result = run_twinning(&[
        "postgres",
        "--restore",
        valid_snapshot_path
            .to_str()
            .expect("tampered snapshot path"),
        "--json",
    ]);
    assert_refusal(&tampered_result, "E_SNAPSHOT_VERIFY");
}
