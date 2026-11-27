//! bd-ocu0: `rest --canary` is checked whether or not a report is written.
//!
//! A failed canary exits 1 and is named on stdout, in `--run` and `--serve`. The
//! manifest is read once, before the port opens or the child runs: one that cannot
//! be read, asserts nothing or has an unknown top-level key refuses with exit 2.

#![cfg(all(unix, feature = "rest"))]

use std::{
    fs,
    io::Read,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

const SPEC: &str = "openapi: 3.0.3\ninfo: {title: t, version: \"1\"}\npaths:\n  /things:\n    get:\n      \
                    responses:\n        \"200\": {description: ok}\n";
const CALL: &str = "curl -s -o /dev/null \"$TWIN_BASE_URL/things\"";

fn endpoint(path: &str) -> String {
    json!({"assertions": [{"kind": "endpoint_called", "method": "GET", "path": path}]}).to_string()
}

fn passing() -> String {
    endpoint("/things")
}

fn failing() -> String {
    endpoint("/never")
}

/// Manifests that must refuse, with the words the refusal must carry. `None` is a
/// path with no file; `Some("")` is made a directory.
fn refused_manifests() -> Vec<(&'static str, Option<String>, &'static str)> {
    let typo = failing().replace("\"assertions\"", "\"assertion\"");
    let extra = format!("{{\"version\":1,{}", &passing()[1..]);
    vec![
        ("missing", None, "failed to read"),
        ("directory", Some(String::new()), "failed to read"),
        (
            "not json",
            Some("not a manifest\n".into()),
            "failed to parse",
        ),
        (
            "unknown kind",
            Some(r#"{"assertions":[{"kind":"nope"}]}"#.into()),
            "unknown variant",
        ),
        ("wrong shape", Some("[1, 2]".into()), "failed to parse"),
        ("empty object", Some("{}".into()), "no assertions"),
        (
            "empty list",
            Some(r#"{"assertions":[]}"#.into()),
            "no assertions",
        ),
        ("misspelled key", Some(typo), "unknown field `assertion`"),
        ("extra key", Some(extra), "unknown field `version`"),
    ]
}

struct Twin {
    dir: TempDir,
}

impl Twin {
    fn new() -> Self {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("api.yaml"), SPEC).expect("spec");
        Self { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn canary(&self, content: Option<&str>) -> &Self {
        match content {
            None => {}
            Some("") => fs::create_dir(self.path("canary.json")).expect("directory"),
            Some(text) => fs::write(self.path("canary.json"), text).expect("canary"),
        }
        self
    }

    fn command(&self, port: u16, report: bool, json_output: bool) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_twinning"));
        command
            .current_dir(self.dir.path())
            .args(["rest", "--spec", "api.yaml", "--canary", "canary.json"])
            .args(["--port", &port.to_string()]);
        if report {
            command.args(["--report", "report.json"]);
        }
        if json_output {
            command.arg("--json");
        }
        command
    }

    fn run(&self, child: &str, report: bool, json_output: bool) -> Finished {
        let output = self
            .command(0, report, json_output)
            .args(["--run", child])
            .output()
            .expect("run twinning");
        Finished::new(output.status.code(), output.stdout, self)
    }

    /// Serves, calls the one route, then stops the twin with `signal`.
    fn serve(&self, report: bool, json_output: bool, signal: &str) -> Finished {
        let port = free_port();
        let mut child = self
            .command(port, report, json_output)
            .arg("--serve")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn twinning");
        wait_for_port(port, &mut child);
        let called = Command::new("curl")
            .args([
                "-s",
                "-o",
                "/dev/null",
                &format!("http://127.0.0.1:{port}/things"),
            ])
            .status()
            .expect("curl");
        assert!(called.success());
        assert!(child.try_wait().expect("wait").is_none(), "still serving");
        let killed = Command::new("kill")
            .args([signal, &child.id().to_string()])
            .status()
            .expect("kill");
        assert!(killed.success());
        self.finish(child)
    }

    fn finish(&self, mut child: Child) -> Finished {
        let deadline = Instant::now() + Duration::from_secs(30);
        while child.try_wait().expect("wait").is_none() {
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Finished::new(None, b"twin did not exit".to_vec(), self);
            }
            thread::sleep(Duration::from_millis(20));
        }
        let mut stdout = Vec::new();
        child
            .stdout
            .take()
            .expect("piped")
            .read_to_end(&mut stdout)
            .expect("stdout");
        let status = child.wait().expect("status");
        Finished::new(status.code(), stdout, self)
    }
}

#[derive(Debug)]
struct Finished {
    code: Option<i32>,
    stdout: String,
    report: Option<Value>,
}

impl Finished {
    fn new(code: Option<i32>, stdout: Vec<u8>, twin: &Twin) -> Self {
        let report = fs::read(twin.path("report.json"))
            .ok()
            .map(|bytes| serde_json::from_slice(&bytes).expect("report JSON"));
        Self {
            code,
            stdout: String::from_utf8(stdout).expect("utf-8"),
            report,
        }
    }

    fn json(&self) -> Value {
        let start = self.stdout.find('{').expect("JSON on stdout");
        serde_json::from_str(&self.stdout[start..]).expect("stdout JSON")
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
        .port()
}

fn wait_for_port(port: u16, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            child.try_wait().expect("wait").is_none(),
            "the twin ended before opening its port"
        );
        assert!(Instant::now() < deadline, "port never opened");
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_names_the_failure(stdout: &str) {
    assert!(
        stdout.contains("REST canary FAILED: 1 of 1 assertions"),
        "{stdout}"
    );
    assert!(
        stdout.contains("endpoint_called GET /never: endpoint was never called"),
        "{stdout}"
    );
}

#[test]
fn a_failed_canary_exits_1_and_is_named_in_run_mode() {
    for report in [true, false] {
        for json_output in [true, false] {
            let twin = Twin::new();
            let finished = twin.canary(Some(&failing())).run(CALL, report, json_output);
            assert_eq!(
                finished.code,
                Some(1),
                "{report} {json_output}: {finished:?}"
            );
            if json_output {
                let summary = finished.json();
                assert_eq!(summary["outcome"], "FAIL");
                assert_eq!(summary["canary"]["failed"], 1);
                assert_eq!(summary["canary"]["failures"][0]["expected"], "GET /never");
            } else {
                assert_names_the_failure(&finished.stdout);
            }
            assert_eq!(finished.report.is_some(), report);
            if let Some(written) = finished.report {
                assert_eq!(written["outcome"], "FAIL");
                assert_eq!(written["canary"]["failed"], 1);
            }
        }
    }
}

#[test]
fn a_failed_canary_exits_1_and_is_named_when_serve_stops() {
    for (report, json_output, signal) in [
        (true, true, "-TERM"),
        (true, false, "-INT"),
        (false, true, "-INT"),
        (false, false, "-TERM"),
    ] {
        let twin = Twin::new();
        let finished = twin
            .canary(Some(&failing()))
            .serve(report, json_output, signal);
        assert_eq!(
            finished.code,
            Some(1),
            "{report} {json_output} {signal}: {finished:?}"
        );
        assert!(finished.stdout.starts_with("REST twin stopped.\n"));
        assert_names_the_failure(&finished.stdout);
        assert_eq!(finished.report.is_some(), report);
        if let Some(written) = finished.report {
            assert_eq!(written["outcome"], "FAIL");
        }
    }
}

#[test]
fn a_passing_canary_leaves_the_output_as_it_was() {
    for report in [true, false] {
        let run = Twin::new();
        let finished = run.canary(Some(&passing())).run(CALL, report, true);
        assert_eq!(finished.code, Some(0), "{finished:?}");
        let summary = finished.json();
        assert_eq!(summary["outcome"], "PASS");
        assert!(summary.get("canary").is_none(), "{summary}");

        let serve = Twin::new();
        let finished = serve.canary(Some(&passing())).serve(report, true, "-TERM");
        assert_eq!(finished.code, Some(0), "{finished:?}");
        assert_eq!(finished.stdout, "REST twin stopped.\n");
        assert_eq!(finished.report.is_some(), report);
    }
}

/// The exit code of a failed child alone is not part of this change.
#[test]
fn a_failed_child_alone_still_exits_0_and_a_failed_canary_still_exits_1() {
    for (canary, code) in [(passing(), 0), (failing(), 1)] {
        for report in [true, false] {
            let twin = Twin::new();
            let child = format!("{CALL}; exit 7");
            let finished = twin.canary(Some(&canary)).run(&child, report, true);
            assert_eq!(finished.code, Some(code), "{finished:?}");
            assert_eq!(finished.json()["outcome"], "FAIL");
            assert_eq!(finished.json()["child"]["exit_code"], 7);
        }
    }
}

#[test]
fn a_manifest_that_cannot_assert_refuses_before_the_child_runs() {
    for (label, content, words) in refused_manifests() {
        for report in [true, false] {
            let twin = Twin::new();
            let finished = twin
                .canary(content.as_deref())
                .run(": > child-ran", report, true);
            assert_eq!(finished.code, Some(2), "{label} {report}: {finished:?}");
            let refusal = finished.json();
            assert_eq!(refusal["outcome"], "REFUSAL", "{label}");
            assert_eq!(refusal["refusal"]["code"], "E_RUNTIME_IO", "{label}");
            assert_eq!(refusal["refusal"]["detail"]["stage"], "rest_canary");
            let error = refusal["refusal"]["detail"]["error"]
                .as_str()
                .expect("error");
            assert!(error.contains(words), "{label}: {error}");
            assert!(!twin.path("child-ran").exists(), "{label}: child ran");
            assert!(finished.report.is_none(), "{label}: report written");
        }
    }
}

/// With the port already taken, a twin that tried to listen would report that
/// instead. The canary refusal shows the manifest is read first.
#[test]
fn a_manifest_that_cannot_assert_refuses_before_serve_opens_its_port() {
    for (label, content, words) in refused_manifests() {
        for report in [true, false] {
            let twin = Twin::new();
            twin.canary(content.as_deref());
            let taken = TcpListener::bind("127.0.0.1:0").expect("bind");
            let port = taken.local_addr().expect("address").port();
            let child = twin
                .command(port, report, true)
                .arg("--serve")
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn twinning");
            let finished = twin.finish(child);
            assert_eq!(finished.code, Some(2), "{label} {report}: {finished:?}");
            let refusal = finished.json();
            assert_eq!(
                refusal["refusal"]["detail"]["stage"], "rest_canary",
                "{label}: {refusal}"
            );
            let error = refusal["refusal"]["detail"]["error"]
                .as_str()
                .expect("error");
            assert!(error.contains(words), "{label}: {error}");
            assert!(finished.report.is_none(), "{label}: report written");
        }
    }
}

#[test]
fn the_manifest_is_read_once_before_the_session() {
    for (first, code) in [(passing(), 0), (failing(), 1)] {
        let twin = Twin::new();
        // The child replaces the manifest with one that could not even be read.
        let child = format!(
            "{CALL}; printf 'not a manifest' > '{}'",
            twin.path("canary.json").display()
        );
        let finished = twin.canary(Some(&first)).run(&child, true, true);
        assert_eq!(finished.code, Some(code), "{finished:?}");
        assert_eq!(
            fs::read_to_string(twin.path("canary.json")).expect("canary"),
            "not a manifest"
        );
    }
}
