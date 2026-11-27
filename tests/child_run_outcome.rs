//! bd-98je, bd-w32d: `rest --run`, `snowflake --run` and `mcp --run` must not say PASS when
//! the child failed.
//!
//! As the Postgres twin does, the outcome becomes FAIL and the child's result is kept, while
//! twinning's own exit code stays in its usual domain: a failed child alone still exits 0
//! (docs/PLAN_TWINNING.md, `--run` mode semantics). None of the three has a child timeout, so
//! `timed_out` has no true case to test here.

#![cfg(all(unix, any(feature = "rest", feature = "snowflake", feature = "mcp")))]

use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};
use tempfile::tempdir;

/// How the child ends: the shell command, its exit code and its signal.
const FAILED_CHILDREN: [(&str, Option<i32>, Option<i32>); 3] = [
    ("exit 7", Some(7), None),
    ("definitely-not-a-command-bd98je", Some(127), None),
    ("kill -KILL $$", None, Some(9)),
];

struct Finished {
    exit_code: Option<i32>,
    stdout: String,
    report: Value,
}

impl Finished {
    /// The JSON object on stdout (REST prints a startup table before it).
    fn summary(&self) -> Value {
        let start = self.stdout.find("{\n").expect("JSON on stdout");
        serde_json::from_str(&self.stdout[start..]).expect("run summary JSON")
    }
}

fn run(mode: &str, child: &str, json_output: bool) -> Finished {
    run_with_canary(mode, child, json_output, None)
}

fn run_with_canary(mode: &str, child: &str, json_output: bool, canary: Option<&Value>) -> Finished {
    let dir = tempdir().expect("tempdir");
    let report = dir.path().join("report.json");
    let mut command = twin_command(mode, dir.path());
    command
        .args(["--port", "0", "--run", child, "--report"])
        .arg(&report);
    if json_output {
        command.arg("--json");
    }
    if let Some(canary) = canary {
        command
            .arg("--canary")
            .arg(write(dir.path(), "canary.json", &canary.to_string()));
    }
    let output = command.output().expect("run twinning");
    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let written = fs::read_to_string(&report);
    assert!(written.is_ok(), "no report: {written:?}: {stdout}");
    Finished {
        exit_code: output.status.code(),
        stdout,
        report: serde_json::from_str(&written.expect("checked above")).expect("report JSON"),
    }
}

/// `twinning <mode>` with the least it needs to start.
fn twin_command(mode: &str, dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_twinning"));
    command.arg(mode);
    match mode {
        "rest" => command.arg("--spec").arg(write(
            dir,
            "api.yaml",
            "openapi: 3.0.3\ninfo: {title: t, version: \"1\"}\npaths:\n  /things:\n    get:\n      \
             responses:\n        \"200\": {description: ok}\n",
        )),
        "mcp" => command
            .args(["--auth-mode", "bypass", "--manifest"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/mcp/http_manifest.json"
            )),
        _ => command
            .arg("--schema")
            .arg(write(dir, "schema.sql", "CREATE TABLE t (id INT);\n")),
    };
    command
}

fn write(dir: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    fs::write(&path, text).expect("write input");
    path
}

fn assert_failed_child_is_reported(mode: &str, summary_version: &str, report_version: &str) {
    for (child, exit_code, signal) in FAILED_CHILDREN {
        let finished = run(mode, child, true);

        // Unchanged, as for the Postgres twin: the child does not move twinning's exit code.
        assert_eq!(finished.exit_code, Some(0), "{child}: {}", finished.stdout);
        let summary = finished.summary();
        assert_eq!(summary["version"], summary_version);
        assert_eq!(summary["outcome"], "FAIL", "{child}: {summary}");
        assert_eq!(
            summary["child"],
            json!({"command": child, "exit_code": exit_code, "signal": signal,
                   "timed_out": false}),
            "{child}"
        );

        // The report is what is kept and passed on, so it must say it too, and why. `run` has
        // the Postgres report's shape: an absent exit code or signal is left out.
        assert_eq!(finished.report["version"], report_version);
        assert_eq!(finished.report["outcome"], "FAIL", "{child}");
        let mut expected = json!({"command": child, "timed_out": false});
        if let Some(code) = exit_code {
            expected["exit_code"] = json!(code);
        }
        if let Some(signal) = signal {
            expected["signal"] = json!(signal);
        }
        assert_eq!(finished.report["run"], expected, "{child}");
    }
}

/// Guard, true before and after: a successful child is PASS and the report gains nothing.
fn assert_successful_child_is_unchanged(mode: &str) {
    let finished = run(mode, "true", true);

    assert_eq!(finished.exit_code, Some(0), "{}", finished.stdout);
    assert_eq!(finished.summary()["outcome"], "PASS");
    assert_eq!(finished.report["outcome"], "PASS");
    assert!(finished.report.get("run").is_none(), "{}", finished.report);
}

#[cfg(feature = "rest")]
#[test]
fn rest_run_reports_a_failed_child() {
    assert_failed_child_is_reported("rest", "twinning.rest-run.v0", "twinning.rest-report.v0");
}

#[cfg(feature = "rest")]
#[test]
fn rest_run_with_a_successful_child_is_unchanged() {
    assert_successful_child_is_unchanged("rest");
}

/// With no report there is nothing but the child to say it failed.
#[cfg(feature = "rest")]
#[test]
fn rest_run_without_a_report_still_says_fail_for_a_failed_child() {
    let dir = tempdir().expect("tempdir");
    let spec = write(
        dir.path(),
        "api.yaml",
        "openapi: 3.0.3\ninfo: {title: t, version: \"1\"}\npaths:\n  /things:\n    get:\n      \
         responses:\n        \"200\": {description: ok}\n",
    );
    let output = Command::new(env!("CARGO_BIN_EXE_twinning"))
        .arg("rest")
        .arg("--spec")
        .arg(spec)
        .args(["--port", "0", "--run", "exit 7", "--json"])
        .output()
        .expect("run twinning");
    let finished = Finished {
        exit_code: output.status.code(),
        stdout: String::from_utf8(output.stdout).expect("stdout utf-8"),
        report: Value::Null,
    };

    assert_eq!(finished.exit_code, Some(0), "{}", finished.stdout);
    assert_eq!(finished.summary()["outcome"], "FAIL", "{}", finished.stdout);
}

/// Met only by a child that calls the one route.
#[cfg(feature = "rest")]
fn things_was_called() -> Value {
    json!({"assertions": [{"kind": "endpoint_called", "method": "GET", "path": "/things"}]})
}

/// A failed canary fails the report, so the summary must not say PASS, whatever the child did.
/// Since bd-ocu0 a failed canary also exits 1; a failed child alone still does not.
#[cfg(feature = "rest")]
#[test]
fn rest_run_summary_says_fail_when_a_canary_fails_the_report() {
    for (child, child_exit) in [("true", 0), ("exit 7", 7)] {
        let finished = run_with_canary("rest", child, true, Some(&things_was_called()));

        assert_eq!(finished.exit_code, Some(1), "{child}: {}", finished.stdout);
        assert_eq!(finished.report["canary"]["failed"], 1, "{child}");
        assert_eq!(finished.report["outcome"], "FAIL", "{child}");
        assert_eq!(finished.report.get("run").is_some(), child_exit != 0);
        let summary = finished.summary();
        assert_eq!(summary["outcome"], "FAIL", "{child}: {summary}");
        assert_eq!(summary["child"]["exit_code"], child_exit);
    }
}

/// Guard, true before and after: a passing canary leaves a successful run PASS.
#[cfg(feature = "rest")]
#[test]
fn rest_run_with_a_passing_canary_is_pass() {
    // curl exits 0 whatever the status, and the canary only asks that the call was made.
    let child = "curl -s \"$TWIN_BASE_URL/things\"";
    let finished = run_with_canary("rest", child, true, Some(&things_was_called()));

    assert_eq!(finished.exit_code, Some(0), "{}", finished.stdout);
    assert_eq!(
        finished.report["canary"]["passed"], 1,
        "{}",
        finished.report
    );
    assert_eq!(finished.report["outcome"], "PASS");
    assert_eq!(finished.summary()["outcome"], "PASS");
    assert_eq!(finished.summary()["child"]["exit_code"], 0);
}

/// The human output never says PASS for a failed canary, and since bd-ocu0 names the
/// failure and exits 1.
#[cfg(feature = "rest")]
#[test]
fn rest_run_human_output_never_says_pass_for_a_failed_canary() {
    let finished = run_with_canary("rest", "true", false, Some(&things_was_called()));

    assert_eq!(finished.exit_code, Some(1), "{}", finished.stdout);
    assert!(
        finished
            .stdout
            .contains("REST canary FAILED: 1 of 1 assertions"),
        "{}",
        finished.stdout
    );
    assert_eq!(finished.report["outcome"], "FAIL");
    assert!(
        finished.stdout.contains("child command completed"),
        "{}",
        finished.stdout
    );
    assert!(!finished.stdout.contains("PASS"), "{}", finished.stdout);
}

/// The published schema allows no unknown property, so it has to know `run`.
#[cfg(feature = "rest")]
#[test]
fn rest_report_with_a_failed_child_stays_within_its_schema() {
    let schema: Value = serde_json::from_str(include_str!(
        "../schemas/twinning.rest-report.v0.schema.json"
    ))
    .expect("schema JSON");
    assert_eq!(schema["additionalProperties"], false);
    for child in ["exit 7", "kill -KILL $$"] {
        let report = run("rest", child, true).report;
        for name in report.as_object().expect("report object").keys() {
            assert!(
                schema["properties"].get(name).is_some(),
                "`{name}` is not in the schema"
            );
        }
        let allowed = &schema["properties"]["run"];
        assert_eq!(allowed["additionalProperties"], false);
        let section = report["run"].as_object().expect("run section");
        for name in section.keys() {
            assert!(allowed["properties"].get(name).is_some(), "run.{name}");
        }
        for name in allowed["required"].as_array().expect("required") {
            assert!(section.contains_key(name.as_str().expect("name")), "{name}");
        }
        assert!(
            schema["properties"]["outcome"]["enum"]
                .as_array()
                .expect("outcomes")
                .contains(&report["outcome"])
        );
    }
}

#[cfg(feature = "snowflake")]
#[test]
fn snowflake_run_reports_a_failed_child() {
    assert_failed_child_is_reported(
        "snowflake",
        "twinning.snowflake-run.v0",
        "twinning.snowflake-report.v0",
    );
}

#[cfg(feature = "snowflake")]
#[test]
fn snowflake_run_with_a_successful_child_is_unchanged() {
    assert_successful_child_is_unchanged("snowflake");
}

#[cfg(feature = "mcp")]
#[test]
fn mcp_run_reports_a_failed_child() {
    assert_failed_child_is_reported("mcp", "twinning.mcp-run.v0", "twinning.mcp-report.v0");
}

#[cfg(feature = "mcp")]
#[test]
fn mcp_run_with_a_successful_child_is_unchanged() {
    assert_successful_child_is_unchanged("mcp");
}

/// With no report there is nothing but the child to say it failed.
#[cfg(feature = "mcp")]
#[test]
fn mcp_run_without_a_report_still_says_fail_for_a_failed_child() {
    for (child, exit_code, signal) in FAILED_CHILDREN.into_iter().chain([("true", Some(0), None)]) {
        let dir = tempdir().expect("tempdir");
        let output = twin_command("mcp", dir.path())
            .args(["--port", "0", "--run", child, "--json"])
            .output()
            .expect("run twinning");
        let finished = Finished {
            exit_code: output.status.code(),
            stdout: String::from_utf8(output.stdout).expect("stdout utf-8"),
            report: Value::Null,
        };

        assert_eq!(finished.exit_code, Some(0), "{child}: {}", finished.stdout);
        let summary = finished.summary();
        let expected = if exit_code == Some(0) { "PASS" } else { "FAIL" };
        assert_eq!(summary["outcome"], expected, "{child}: {summary}");
        assert_eq!(summary["child"]["exit_code"], json!(exit_code), "{child}");
        assert_eq!(summary["child"]["signal"], json!(signal), "{child}");
    }
}
