#![forbid(unsafe_code)]
#![cfg(feature = "postgres")]

use std::{
    fs,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::tempdir;

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

#[test]
fn run_once_verify_fails_on_child_created_committed_violation() {
    let result = run_verify_case("__NULL__");

    assert_eq!(
        result.exit_code, 1,
        "child-created verify violation should drive process exit 1: stdout={}; stderr={}; diagnostics={}",
        result.stdout, result.stderr, result.diagnostics
    );
    assert!(
        result.stderr.is_empty(),
        "unexpected stderr: {}",
        result.stderr
    );
    assert_eq!(result.json["mode"], "run_once");
    assert_eq!(result.json["outcome"], "FAIL");
    assert_eq!(result.json["run"]["exit_code"], 0);
    assert_eq!(result.json["tables"]["public.deals"]["rows"], 1);
    assert_eq!(result.json["verify"]["version"], "verify.report.v1");
    assert_eq!(result.json["verify"]["execution_mode"], "embedded");
    assert_eq!(result.json["verify"]["outcome"], "FAIL");
    assert_eq!(result.json["verify"]["summary"]["failed_rules"], 1);
    assert_eq!(
        result.json["verify"]["results"][0]["rule_id"],
        "DEAL_NAME_PRESENT"
    );
    // This asserted the raw key `deal-from-child` before bd-uk5d: it recorded the leak. The
    // raw key is now asserted under --explicit, below.
    assert_eq!(
        result.json["verify"]["results"][0]["affected"][0]["key"]["deal_id"],
        "redacted:ref:k1"
    );
}

#[test]
fn run_once_verify_passes_on_child_created_clean_state() {
    let result = run_verify_case("Alpha");

    assert_eq!(
        result.exit_code, 0,
        "child-created clean state should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        result.stdout, result.stderr, result.diagnostics
    );
    assert!(
        result.stderr.is_empty(),
        "unexpected stderr: {}",
        result.stderr
    );
    assert_eq!(result.json["mode"], "run_once");
    assert_eq!(result.json["outcome"], "PASS");
    assert_eq!(result.json["run"]["exit_code"], 0);
    assert_eq!(result.json["tables"]["public.deals"]["rows"], 1);
    assert_eq!(result.json["verify"]["version"], "verify.report.v1");
    assert_eq!(result.json["verify"]["execution_mode"], "embedded");
    assert_eq!(result.json["verify"]["outcome"], "PASS");
    assert_eq!(result.json["verify"]["summary"]["failed_rules"], 0);
}

struct VerifyCaseResult {
    port: u16,
    exit_code: i32,
    stdout: String,
    stderr: String,
    diagnostics: String,
    report_file: String,
    json: Value,
}

/// bd-uk5d: the report is what gets sealed and passed on, so by default it must carry neither
/// the failing row's key nor its cell value. `deal-from-child` is the key the child commits and
/// `CANARY_NAME` the cell value. Only `verify` is inspected: `run.command` repeats the caller's
/// own command line.
#[test]
fn run_once_verify_default_report_redacts_affected_keys_and_values() {
    let result = run_verify_case_with(
        CANARY_NAME,
        r#"{ "op": "predicate", "binding": "public.deals",
             "expr": { "eq": [ { "column": "deal_name" }, "Alpha" ] } }"#,
    );

    assert_eq!(result.exit_code, 1, "stdout={}", result.stdout);
    assert_eq!(result.json["verify"]["outcome"], "FAIL");
    assert_eq!(result.json["verify"]["results"][0]["violation_count"], 1);
    assert_eq!(
        result.json["verify"]["results"][0]["affected"][0],
        serde_json::json!({
            "binding": "public.deals",
            "key": { "deal_id": "redacted:ref:k1" },
            "field": "deal_name",
            "value": "redacted:ref:v1"
        })
    );
    assert_no_row_data(&result.json["verify"]);
}

/// bd-uk5d, second channel: a rule the engine refuses to evaluate names the row it stopped on.
#[test]
fn run_once_verify_default_refusal_redacts_the_row_key() {
    let result = run_verify_case_with(
        CANARY_NAME,
        r#"{ "op": "predicate", "binding": "public.deals",
             "expr": { "gt": [ { "column": "deal_name" }, 5 ] } }"#,
    );

    assert_eq!(result.json["verify"]["outcome"], "REFUSAL");
    assert_eq!(result.json["verify"]["refusal"]["code"], "E_BAD_EXPR");
    assert_eq!(
        result.json["verify"]["refusal"]["detail"]["key"],
        serde_json::json!({ "deal_id": "redacted:ref:k1" })
    );
    assert_no_row_data(&result.json["verify"]);
}

#[test]
fn run_once_verify_explicit_report_keeps_the_raw_key_and_value() {
    let case = Case::new(FAILING_PREDICATE);
    let result = case.run(CANARY_NAME, Run::explicit());

    assert_eq!(result.exit_code, 1, "stdout={}", result.stdout);
    assert_eq!(
        result.json["verify"]["results"][0]["affected"][0],
        serde_json::json!({
            "binding": "public.deals",
            "key": { "deal_id": "deal-from-child" },
            "field": "deal_name",
            "value": CANARY_NAME
        })
    );
    let refused = Case::new(REFUSING_PREDICATE).run(CANARY_NAME, Run::explicit());
    assert_eq!(
        refused.json["verify"]["refusal"]["detail"]["key"],
        serde_json::json!({ "deal_id": "deal-from-child" })
    );
}

/// The expected markers were computed outside this code:
/// printf '%s' '"<value>"' | openssl dgst -sha256 -hmac 0123456789abcdef   (first 32 hex)
#[test]
fn run_once_verify_keyed_markers_replace_ordinals() {
    let keyed = Run {
        key: Some(GOOD_KEY),
        ..Run::default()
    };
    let result = Case::new(FAILING_PREDICATE).run(CANARY_NAME, keyed);

    assert_eq!(
        result.json["verify"]["results"][0]["affected"][0],
        serde_json::json!({
            "binding": "public.deals",
            "key": { "deal_id": "redacted:hmac-sha256:331f011f6f161861912efb933e020309" },
            "field": "deal_name",
            "value": "redacted:hmac-sha256:093eb9bcb1cd46c75bbdd17a2ea859dd"
        })
    );
    assert_no_row_data(&result.json["verify"]);
    assert!(!result.stdout.contains(GOOD_KEY) && !result.stderr.contains(GOOD_KEY));
}

/// Same state, with and without --explicit: the whole report, the exit code and so every
/// verdict, count and rule status are identical except at the listed paths.
#[test]
fn redaction_changes_only_row_keys_and_cells() {
    let cases: [(&str, &str, &str, i32, &[&str]); 4] = [
        (
            "passing",
            r#"{ "op": "not_null", "binding": "public.deals", "columns": ["deal_id"] }"#,
            "PASS",
            0,
            &[],
        ),
        (
            "failing predicate",
            FAILING_PREDICATE,
            "FAIL",
            1,
            &[
                "/verify/results/0/affected/0/key/deal_id",
                "/verify/results/0/affected/0/value",
            ],
        ),
        (
            // The failing cell is null; verify marks it too.
            "failing not_null",
            r#"{ "op": "not_null", "binding": "public.deals", "columns": ["deal_name"] }"#,
            "FAIL",
            1,
            &[
                "/verify/results/0/affected/0/key/deal_id",
                "/verify/results/0/affected/0/value",
            ],
        ),
        (
            "refusing",
            REFUSING_PREDICATE,
            "REFUSAL",
            // 0 before bd-0yjb: this recorded a refused check passing as success.
            2,
            &["/verify/refusal/detail/key/deal_id"],
        ),
    ];

    for (name, check, outcome, exit_code, expected_paths) in cases {
        let case = Case::new(check);
        let deal_name = if name == "failing not_null" {
            "__NULL__"
        } else {
            CANARY_NAME
        };
        let mut default = case.run(deal_name, Run::default());
        let mut explicit = case.run(deal_name, Run::explicit());
        normalize_port(&mut default);
        normalize_port(&mut explicit);

        assert_eq!(default.json["verify"]["outcome"], outcome, "{name}");
        assert_eq!(
            (default.exit_code, explicit.exit_code),
            (exit_code, exit_code),
            "{name}: {} / {}",
            default.stdout,
            explicit.stdout
        );
        let mut paths = Vec::new();
        differing_paths("", &default.json, &explicit.json, &mut paths);
        assert_eq!(paths, expected_paths, "{name}");
        // Byte for byte: put the raw values back and the two documents are one.
        let mut restored = default.json.clone();
        for path in expected_paths {
            *restored.pointer_mut(path).expect("path in default") = explicit
                .json
                .pointer(path)
                .expect("path in explicit")
                .clone();
        }
        assert_eq!(
            serde_json::to_string(&restored).expect("serializes"),
            serde_json::to_string(&explicit.json).expect("serializes"),
            "{name}"
        );
    }
}

/// Every place the report leaves the process. The positive control for each is the same
/// surface of an --explicit run, which must contain the row's key and cell.
#[test]
fn no_default_output_surface_carries_row_data() {
    for check in [FAILING_PREDICATE, REFUSING_PREDICATE] {
        let case = Case::new(check);
        let default = case.run(CANARY_NAME, Run::default());
        let explicit = case.run(CANARY_NAME, Run::explicit());
        let raw: &[&str] = if check == FAILING_PREDICATE {
            &["deal-from-child", CANARY_NAME]
        } else {
            // A refusal names the row's key only.
            &["deal-from-child"]
        };
        let verify_text = |report: &str| {
            serde_json::from_str::<Value>(report).expect("report json")["verify"].to_string()
        };
        for (surface, quiet, loud) in [
            (
                "stdout verify section",
                verify_text(&default.stdout),
                verify_text(&explicit.stdout),
            ),
            (
                "--report file verify section",
                verify_text(&default.report_file),
                verify_text(&explicit.report_file),
            ),
        ] {
            for value in raw {
                assert!(
                    loud.contains(value),
                    "{surface}: control did not fire: {loud}"
                );
                assert!(!quiet.contains(value), "{surface}: `{value}` in {quiet}");
            }
        }
        // stderr carries nothing in either mode.
        assert_eq!(
            (default.stderr.as_str(), explicit.stderr.as_str()),
            ("", "")
        );
    }
}

/// The human view prints no verify payload in either mode. The control shows the channel is
/// read: it does print the verify artifact's path, given here a canary name.
#[test]
fn human_output_carries_no_row_data() {
    let case = Case::with_artifact_name(FAILING_PREDICATE, "ZQXCANARY-artifact.verify.json");
    for explicit in [false, true] {
        let run = Run {
            human: true,
            explicit,
            ..Run::default()
        };
        let result = case.run(CANARY_NAME, run);
        assert!(
            result.stdout.contains("ZQXCANARY-artifact"),
            "control did not fire: {}",
            result.stdout
        );
        let without_path = result.stdout.replace("ZQXCANARY-artifact", "");
        for raw in ["deal-from-child", CANARY_NAME] {
            // The human view repeats the caller's --run command, which names the cell value.
            let printed = without_path
                .lines()
                .filter(|line| !line.contains("verify_child.py"))
                .any(|line| line.contains(raw));
            assert!(!printed, "`{raw}` in human output: {}", result.stdout);
        }
    }
}

#[test]
fn unusable_redaction_key_is_refused_before_reading_or_listening() {
    // The port is already taken and the schema does not exist: any attempt to read or to
    // listen would refuse with a different code.
    let taken = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let port = taken.local_addr().expect("addr").port().to_string();
    let dir = tempdir().expect("tempdir");
    let marker = dir.path().join("child-ran");
    for (key, reason) in [("", "empty"), ("0123456789abcde", "too_short")] {
        for explicit in [false, true] {
            let mut command = Command::new(twinning_bin());
            command
                .args(["postgres", "--schema"])
                .arg(dir.path().join("no-such-schema.sql"))
                .arg("--verify")
                .arg(dir.path().join("no-such-verify.json"))
                .args(["--port", &port, "--run"])
                .arg(format!("touch {}", shell_quote(&marker)))
                .arg("--json")
                .env("VERIFY_REDACTION_KEY", key);
            if explicit {
                command.arg("--explicit");
            }
            let output = command.output().expect("run twinning");
            let stdout = String::from_utf8(output.stdout).expect("utf-8");
            let json: Value = serde_json::from_str(&stdout).expect("refusal json");

            assert_eq!(output.status.code(), Some(2), "{stdout}");
            assert_eq!(json["outcome"], "REFUSAL");
            assert_eq!(json["refusal"]["code"], "E_REDACTION_KEY", "{stdout}");
            assert_eq!(
                json["refusal"]["detail"],
                serde_json::json!({
                    "env_var": "VERIFY_REDACTION_KEY",
                    "reason": reason,
                    "min_bytes": 16
                })
            );
            assert!(key.is_empty() || !stdout.contains(key));
            assert!(output.stderr.is_empty());
            assert!(!marker.exists(), "the child ran");
        }
    }
}

/// Without --verify there is nothing to redact, so the variable is not read.
#[test]
fn a_short_key_does_not_matter_without_verify() {
    let dir = tempdir().expect("tempdir");
    let output = Command::new(twinning_bin())
        .args(["postgres", "--schema"])
        .arg(write_schema(dir.path()))
        .args(["--port", "0", "--run", "true", "--json"])
        .env("VERIFY_REDACTION_KEY", "short")
        .output()
        .expect("run twinning");

    assert_eq!(output.status.code(), Some(0), "{:?}", output);
}

/// bd-0yjb: a check the engine refused was never evaluated, so the run must not read as a
/// success. As standalone verify, it is a refusal with exit 2; the report is still written.
#[test]
fn a_refused_check_is_a_top_level_refusal_with_exit_2() {
    for serve in [false, true] {
        let mode = if serve { "interactive" } else { "run_once" };
        let run = Run {
            serve,
            ..Run::default()
        };
        let result = Case::new(REFUSING_PREDICATE).run(CANARY_NAME, run);

        assert_eq!(
            result.exit_code, 2,
            "{mode}: {} {}",
            result.stdout, result.diagnostics
        );
        assert_eq!(result.json["outcome"], "REFUSAL", "{mode}");
        assert_eq!(result.json["mode"], mode);
        // A twin report, not a process refusal: the evidence stays.
        assert!(result.json["refusal"].is_null(), "{mode}");
        assert_eq!(result.json["tables"]["public.deals"]["rows"], 1, "{mode}");
        assert_eq!(result.json["verify"]["outcome"], "REFUSAL", "{mode}");
        assert_eq!(result.json["verify"]["refusal"]["code"], "E_BAD_EXPR");
        assert_eq!(
            result.json["verify"]["refusal"]["detail"]["key"],
            serde_json::json!({ "deal_id": "redacted:ref:k1" })
        );
        assert_eq!(result.report_file, result.stdout, "{mode}");
    }
}

/// The refusal outranks a failed child, and the child's result stays in the report.
#[test]
fn a_refused_check_outranks_a_failed_child() {
    for (then, exit_code, signal) in [
        ("; exit 7", Some(7), None),
        ("; kill -KILL $$", None, Some(9)),
    ] {
        let run = Run {
            then,
            ..Run::default()
        };
        let result = Case::new(REFUSING_PREDICATE).run(CANARY_NAME, run);

        assert_eq!(result.exit_code, 2, "{then}: {}", result.stdout);
        assert_eq!(result.json["outcome"], "REFUSAL", "{then}");
        assert_eq!(
            result.json["run"]["exit_code"],
            serde_json::json!(exit_code)
        );
        assert_eq!(result.json["run"]["signal"], serde_json::json!(signal));
        assert_eq!(result.json["verify"]["outcome"], "REFUSAL", "{then}");
    }
}

/// One refused rule makes the whole verify report a refusal (the engine keeps no results, as
/// standalone verify), whether another rule would have failed before it or after it.
#[test]
fn a_refused_check_outranks_a_failing_one_in_either_order() {
    for (first, second) in [
        (FAILING_PREDICATE, REFUSING_PREDICATE),
        (REFUSING_PREDICATE, FAILING_PREDICATE),
    ] {
        let both = format!(
            r#"{first} }}, {{ "id": "SECOND", "severity": "error", "portability": "portable",
                "check": {second}"#
        );
        let result = Case::new(&both).run(CANARY_NAME, Run::default());

        assert_eq!(result.exit_code, 2, "{}", result.stdout);
        assert_eq!(result.json["outcome"], "REFUSAL");
        assert_eq!(result.json["verify"]["outcome"], "REFUSAL");
        assert_eq!(result.json["verify"]["results"], serde_json::json!([]));
    }
}

#[test]
fn the_human_view_says_refusal() {
    let run = Run {
        human: true,
        ..Run::default()
    };
    let result = Case::new(REFUSING_PREDICATE).run(CANARY_NAME, run);

    assert_eq!(result.exit_code, 2, "{}", result.stdout);
    assert_eq!(
        result.stdout.lines().next(),
        Some("twinning postgres run_once refusal")
    );
}

/// Guard, true before and after bd-0yjb: passing and failing checks behave as they did,
/// including that a failed child alone does not change the exit code.
#[test]
fn passing_and_failing_checks_keep_their_outcomes_and_exit_codes() {
    let passing = r#"{ "op": "not_null", "binding": "public.deals", "columns": ["deal_id"] }"#;
    for (check, serve, then, outcome, exit_code) in [
        (passing, false, "", "PASS", 0),
        (passing, true, "", "PASS", 0),
        (passing, false, "; exit 7", "FAIL", 0),
        (FAILING_PREDICATE, false, "", "FAIL", 1),
        (FAILING_PREDICATE, true, "", "FAIL", 1),
        (FAILING_PREDICATE, false, "; exit 7", "FAIL", 1),
    ] {
        let run = Run {
            serve,
            then,
            ..Run::default()
        };
        let result = Case::new(check).run(CANARY_NAME, run);

        assert_eq!(
            (result.exit_code, &result.json["outcome"]),
            (exit_code, &Value::from(outcome)),
            "serve={serve} then={then:?}: {} {}",
            result.stdout,
            result.diagnostics
        );
    }
}

const CANARY_NAME: &str = "ZQXCANARY-deal-name-5521";
const GOOD_KEY: &str = "0123456789abcdef";
const FAILING_PREDICATE: &str = r#"{ "op": "predicate", "binding": "public.deals",
    "expr": { "eq": [ { "column": "deal_name" }, "Alpha" ] } }"#;
const REFUSING_PREDICATE: &str = r#"{ "op": "predicate", "binding": "public.deals",
    "expr": { "gt": [ { "column": "deal_name" }, 5 ] } }"#;

/// Each run takes its own port. This is the only thing normalized before two reports are
/// compared: `port`, and the same number inside `run.command`, each checked first.
fn normalize_port(result: &mut VerifyCaseResult) {
    assert_eq!(result.json["port"], result.port, "{}", result.stdout);
    result.json["port"] = Value::from(0);
    let command = result.json["run"]["command"].as_str().expect("run command");
    let own = format!(" 127.0.0.1 {} ", result.port);
    assert_eq!(command.matches(&own).count(), 1, "{command}");
    result.json["run"]["command"] = Value::from(command.replace(&own, " 127.0.0.1 PORT "));
}

/// JSON pointers of every leaf that differs.
fn differing_paths(path: &str, left: &Value, right: &Value, out: &mut Vec<String>) {
    match (left, right) {
        (Value::Object(a), Value::Object(b)) if a.keys().eq(b.keys()) => {
            for (name, child) in a {
                differing_paths(&format!("{path}/{name}"), child, &b[name], out);
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (index, (x, y)) in a.iter().zip(b).enumerate() {
                differing_paths(&format!("{path}/{index}"), x, y, out);
            }
        }
        _ if left != right => out.push(path.to_owned()),
        _ => {}
    }
}

fn assert_no_row_data(verify: &Value) {
    let text = verify.to_string();
    for raw in [CANARY_NAME, "deal-from-child"] {
        assert!(
            !text.contains(raw),
            "`{raw}` is in the default verify report: {text}"
        );
    }
}

fn run_verify_case(deal_name_arg: &str) -> VerifyCaseResult {
    run_verify_case_with(
        deal_name_arg,
        r#"{ "op": "not_null", "binding": "public.deals", "columns": ["deal_name"] }"#,
    )
}

fn run_verify_case_with(deal_name_arg: &str, check: &str) -> VerifyCaseResult {
    Case::new(check).run(deal_name_arg, Run::default())
}

/// How one run differs from the default one.
#[derive(Clone, Copy, Default)]
struct Run {
    explicit: bool,
    human: bool,
    key: Option<&'static str>,
    /// `--serve` instead of `--run`: the same client runs against it, then SIGINT.
    serve: bool,
    /// Appended to the `--run` command: how the child ends after committing its row.
    then: &'static str,
}

impl Run {
    fn explicit() -> Self {
        Self {
            explicit: true,
            ..Self::default()
        }
    }
}

/// One directory, so that two runs of a case see the same input paths.
struct Case {
    dir: tempfile::TempDir,
    schema_path: PathBuf,
    verify_path: PathBuf,
    child_path: PathBuf,
}

impl Case {
    fn new(check: &str) -> Self {
        Self::with_artifact_name(check, "verify.constraint.json")
    }

    fn with_artifact_name(check: &str, artifact_name: &str) -> Self {
        let dir = tempdir().expect("tempdir");
        let schema_path = write_schema(dir.path());
        let written = write_verify_artifact(dir.path(), check);
        let verify_path = dir.path().join(artifact_name);
        fs::rename(written, &verify_path).expect("name verify artifact");
        let child_path = write_child(dir.path());
        Self {
            dir,
            schema_path,
            verify_path,
            child_path,
        }
    }

    fn run(&self, deal_name_arg: &str, run: Run) -> VerifyCaseResult {
        // A reserved port is free again before twinning binds it, and with tests running in
        // parallel something else can take it first. Only that refusal is tried again.
        (0..10)
            .find_map(|_| self.run_on(reserve_local_port(), deal_name_arg, run))
            .expect("ten reserved ports in a row were taken before twinning bound them")
    }

    /// `None` when twinning could not bind `port`.
    fn run_on(&self, port: u16, deal_name_arg: &str, run: Run) -> Option<VerifyCaseResult> {
        let diagnostics_path = self.dir.path().join("client.err");
        // Named for the run, so that a run cannot be judged by an earlier run's report.
        let report_path = self.dir.path().join(format!("report-{port}.json"));
        let client_command = format!(
            "python3 {} 127.0.0.1 {} {} {}",
            shell_quote(&self.child_path),
            port,
            shell_quote_str(deal_name_arg),
            shell_quote(&diagnostics_path),
        );

        let mut command = Command::new(twinning_bin());
        command
            .arg("postgres")
            .arg("--schema")
            .arg(&self.schema_path)
            .arg("--verify")
            .arg(&self.verify_path)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--report")
            .arg(&report_path)
            // The caller's environment must not decide what a test sees.
            .env_remove("VERIFY_REDACTION_KEY");
        if run.serve {
            command.arg("--serve");
        } else {
            command
                .arg("--run")
                .arg(format!("{client_command}{}", run.then));
        }
        if !run.human {
            command.arg("--json");
        }
        if run.explicit {
            command.arg("--explicit");
        }
        if let Some(key) = run.key {
            command.env("VERIFY_REDACTION_KEY", key);
        }
        let output = if run.serve {
            let mut client = Command::new("python3");
            client
                .arg(&self.child_path)
                .args(["127.0.0.1", &port.to_string(), deal_name_arg])
                .arg(&diagnostics_path);
            serve_one_client(command, port, client)
        } else {
            command.output().expect("run twinning")
        };
        let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
        let diagnostics = fs::read_to_string(diagnostics_path).unwrap_or_default();
        if stdout.contains("listener_bind") && stdout.contains("Address already in use") {
            return None;
        }
        let report_file = fs::read_to_string(&report_path);
        assert!(
            report_file.is_ok(),
            "no report: {report_file:?}: {stdout} {diagnostics}"
        );
        let report_file = report_file.expect("checked above");
        let json = if run.human {
            Value::Null
        } else {
            serde_json::from_str(&stdout).expect("parse twinning json")
        };

        Some(VerifyCaseResult {
            port,
            exit_code: output.status.code().expect("process exit code"),
            stdout,
            stderr,
            diagnostics,
            report_file,
            json,
        })
    }
}

/// Start a `--serve` twin, run the client once against it, then SIGINT it and collect what
/// it printed. If it never listens, its own output says why.
fn serve_one_client(mut command: Command, port: u16, mut client: Command) -> Output {
    let mut twin = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start twinning --serve");
    let deadline = Instant::now() + Duration::from_secs(30);
    let listening = loop {
        if twin.try_wait().expect("twin status").is_some() {
            break false;
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    if listening {
        // The client reports its own failure to the diagnostics file.
        let _ = client.output();
    }
    if twin.try_wait().expect("twin status").is_none() {
        let signal = if listening { "-INT" } else { "-KILL" };
        Command::new("kill")
            .args([signal, &twin.id().to_string()])
            .status()
            .expect("signal twin");
    }
    twin.wait_with_output().expect("twin output")
}

fn write_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("schema.sql");
    fs::write(
        &schema_path,
        "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY, deal_name TEXT);",
    )
    .expect("write schema");
    schema_path
}

fn write_verify_artifact(dir: &Path, check: &str) -> PathBuf {
    let verify_path = dir.join("verify.constraint.json");
    fs::write(
        &verify_path,
        r#"{
          "version": "verify.constraint.v1",
          "constraint_set_id": "fixtures.public.deals.not_null",
          "bindings": [
            { "name": "public.deals", "kind": "relation", "key_fields": ["deal_id"] }
          ],
          "rules": [
            {
              "id": "DEAL_NAME_PRESENT",
              "severity": "error",
              "portability": "portable",
              "check": __CHECK__
            }
          ]
        }"#
        .replace("__CHECK__", check),
    )
    .expect("write verify artifact");
    verify_path
}

fn write_child(dir: &Path) -> PathBuf {
    let child_path = dir.join("verify_child.py");
    fs::write(&child_path, VERIFY_CHILD).expect("write child");
    child_path
}

fn reserve_local_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local port");
    let port = listener.local_addr().expect("listener addr").port();
    drop(listener);
    port
}

fn shell_quote(path: &Path) -> String {
    shell_quote_str(&path.display().to_string())
}

fn shell_quote_str(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

const VERIFY_CHILD: &str = r#"
import socket
import struct
import sys

TEXT_OID = 25

host = sys.argv[1]
port = int(sys.argv[2])
deal_name_arg = sys.argv[3]
diagnostics_path = sys.argv[4]
deal_name = None if deal_name_arg == "__NULL__" else deal_name_arg


def fail(message):
    with open(diagnostics_path, "w", encoding="utf-8") as diagnostics:
        diagnostics.write(message + "\n")
    raise SystemExit(message)


sock = socket.create_connection((host, port), timeout=2)
sock.settimeout(2)


def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)


def read_exact(size):
    body = b""
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        if not chunk:
            fail("connection closed while reading backend frame")
        body += chunk
    return body


def read_frame():
    header = read_exact(5)
    tag = header[:1]
    length = struct.unpack("!I", header[1:])[0]
    return tag, read_exact(length - 4)


def read_until_ready():
    frames = []
    while True:
        frame = read_frame()
        frames.append(frame)
        if frame[0] == b"Z":
            return frames


def expect_tag(frame, expected):
    tag, body = frame
    if tag != expected:
        fail(f"expected backend frame {expected!r}, got {tag!r} body={body!r}")


def expect_ready(frames, status):
    tag, body = frames[-1]
    if tag != b"Z" or body != status:
        fail(f"expected ReadyForQuery {status!r}, got tag={tag!r} body={body!r}")


def startup():
    body = struct.pack("!I", 196608)
    for name, value in ((b"user", b"postgres"), (b"database", b"postgres")):
        body += name + b"\x00" + value + b"\x00"
    body += b"\x00"
    sock.sendall(struct.pack("!I", len(body) + 4) + body)
    expect_ready(read_until_ready(), b"I")


def query(sql, ready_status):
    framed(b"Q", sql.encode("utf-8") + b"\x00")
    frames = read_until_ready()
    expect_tag(frames[0], b"C")
    expect_ready(frames, ready_status)


def insert_deal():
    statement = "insert_deal"
    sql = "INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2)"
    body = statement.encode("utf-8") + b"\x00" + sql.encode("utf-8") + b"\x00"
    body += struct.pack("!h", 2)
    body += struct.pack("!I", TEXT_OID)
    body += struct.pack("!I", TEXT_OID)
    framed(b"P", body)

    portal = "insert_deal_portal"
    body = portal.encode("utf-8") + b"\x00" + statement.encode("utf-8") + b"\x00"
    body += struct.pack("!h", 0)
    body += struct.pack("!h", 2)
    for value in ("deal-from-child", deal_name):
        if value is None:
            body += struct.pack("!i", -1)
            continue
        encoded = value.encode("utf-8")
        body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\x00" + struct.pack("!i", 0))
    framed(b"S")

    frames = read_until_ready()
    expect_tag(frames[0], b"1")
    expect_tag(frames[1], b"2")
    expect_tag(frames[2], b"C")
    expect_ready(frames, b"T")


startup()
query("BEGIN", b"T")
insert_deal()
query("COMMIT", b"I")
framed(b"X")
sock.close()
"#;
