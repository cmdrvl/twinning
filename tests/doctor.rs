#![forbid(unsafe_code)]
#![cfg(feature = "rest")]

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::Value;
use tempfile::tempdir;

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn run_twinning(args: &[&str], cwd: &Path) -> Output {
    Command::new(twinning_bin())
        .args(args)
        .current_dir(cwd)
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
    assert!(
        output.stderr.is_empty(),
        "doctor commands should not write stderr on success: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should be json")
}

#[test]
fn doctor_health_is_read_only_twinning_v0_json() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["doctor", "health", "--json"], dir.path());
    assert_success(&output);
    assert!(
        !dir.path().join(".doctor").exists(),
        "read-only doctor must not create .doctor artifacts"
    );

    let json = parse_stdout_json(&output);
    assert_eq!(json["version"], "twinning.v0");
    assert_eq!(json["outcome"], "READY");
    assert_eq!(json["mode"], "doctor_health");
    assert_eq!(json["tool"], "twinning");
    assert_eq!(json["package_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(json["read_only"], true);
    assert_eq!(json["side_effects"]["reads_schema_files"], false);
    assert_eq!(json["side_effects"]["reads_snapshot_files"], false);
    assert_eq!(json["side_effects"]["reads_verify_artifacts"], false);
    assert_eq!(json["side_effects"]["binds_network_socket"], false);
    assert_eq!(json["side_effects"]["runs_child_command"], false);
    assert_eq!(json["side_effects"]["writes_doctor_artifacts"], false);
    assert_eq!(json["checks"][0]["id"], "doctor_surface");
    assert_eq!(json["checks"][3]["id"], "fix_mode");
    assert_eq!(json["checks"][3]["status"], "not_available");
}

#[test]
fn doctor_health_human_output_lists_rest_protocol() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["doctor", "health"], dir.path());
    assert_success(&output);

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    assert!(stdout.contains("REST protocol: twinning rest --spec <openapi.yaml> [OPTIONS]\n"));
    assert!(stdout.contains("twinning rest --spec openapi.yaml --json"));
}

#[test]
fn doctor_capabilities_reflect_agent_commands_and_no_fix_mode() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["doctor", "capabilities", "--json"], dir.path());
    assert_success(&output);

    let json = parse_stdout_json(&output);
    assert_eq!(json["version"], "twinning.v0");
    assert_eq!(json["mode"], "doctor_capabilities");
    assert_eq!(json["read_only"], true);
    assert_eq!(json["fix_mode"]["available"], false);

    let commands = json["commands"].as_array().expect("commands array");
    assert!(
        commands
            .iter()
            .any(|command| command["command"] == "twinning doctor --robot-triage")
    );
    assert!(
        commands
            .iter()
            .any(|command| command["command"] == "twinning --describe")
    );
    assert!(commands.iter().any(|command| {
        command["command"] == "twinning rest --spec <FILE> --json"
            && command["output"] == "twinning.rest-report.v0 JSON"
            && command["description"].as_str().is_some_and(|description| {
                description.contains("--port (default 8080)")
                    && description.contains("adaptive routing")
                    && description.contains("remote $ref resolution")
                    && description.contains("auth shape compliance")
            })
    }));

    let output_contracts = json["output_contracts"]
        .as_array()
        .expect("output contracts array");
    assert!(output_contracts.iter().any(|contract| {
        contract["name"] == "REST session report"
            && contract["version"] == "twinning.rest-report.v0"
    }));
}

#[test]
fn doctor_robot_triage_is_json_without_global_json_flag() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["doctor", "--robot-triage"], dir.path());
    assert_success(&output);

    let json = parse_stdout_json(&output);
    assert_eq!(json["version"], "twinning.v0");
    assert_eq!(json["mode"], "doctor_triage");
    assert_eq!(json["summary"]["doctor_surface"], "read_only_available");
    assert_eq!(json["summary"]["fix_mode"], "absent_by_design");
    assert!(
        json["recommended_next_work"]
            .as_array()
            .expect("recommended work array")
            .iter()
            .any(|item| item["id"] == "snapshot_detector_fixtures")
    );
}

#[test]
fn doctor_robot_docs_is_plain_agent_guidance() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["doctor", "robot-docs"], dir.path());
    assert_success(&output);

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    assert!(stdout.starts_with("twinning doctor robot-docs\n"));
    assert!(stdout.contains("twinning doctor health --json\n"));
    assert!(stdout.contains("twinning rest --spec <openapi.yaml> --json\n"));
    assert!(stdout.contains("doctor --fix is intentionally unavailable"));
}

#[test]
fn doctor_fix_is_not_exposed() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["doctor", "--fix"], dir.path());
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--fix'"),
        "stderr should explain the rejected fix flag: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !dir.path().join(".doctor").exists(),
        "rejected fix flag must not create .doctor artifacts"
    );
}

#[test]
fn describe_works_without_engine_subcommand() {
    let dir = tempdir().expect("tempdir");

    let output = run_twinning(&["--describe"], dir.path());
    assert_success(&output);

    let json = parse_stdout_json(&output);
    assert_eq!(json["schema_version"], "operator.v0");
    assert_eq!(json["name"], "twinning");
    assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(json["doctor"]["read_only"], true);

    let usage = json["invocation"]["usage"]
        .as_array()
        .expect("usage array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(usage.contains("twinning rest --spec <FILE>"));

    let options = json["options"].as_array().expect("options array");
    assert!(options.iter().any(|option| {
        option["name"] == "spec"
            && option["flag"] == "--spec"
            && option["description"]
                .as_str()
                .is_some_and(|description| description.contains("OpenAPI 3.x YAML or JSON"))
    }));

    let implemented_surface = json["current_runtime_behavior"]["implemented_surface"]
        .as_array()
        .expect("implemented surface")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(implemented_surface.contains("REST twin: OpenAPI 3.x YAML/JSON"));
    assert!(implemented_surface.contains("ResourceTopology with confidence scoring"));

    let artifacts = json["artifacts"].as_array().expect("artifacts array");
    assert!(artifacts.iter().any(|artifact| {
        artifact["name"] == "twinning.rest-report.v0" && artifact["kind"] == "session_report"
    }));
}
