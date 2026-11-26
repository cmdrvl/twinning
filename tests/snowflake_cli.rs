#![forbid(unsafe_code)]
#![cfg(all(feature = "snowflake", unix))]

//! bd-91hu: `twinning snowflake` follows the same flag contract as `twinning postgres`.
//! Neither `--serve` nor `--run`: validate, write `--report`, print the readiness summary, exit 0
//! without binding a port. `--serve`: serve until SIGINT/SIGTERM. `--run <cmd>`: one child, exit.
//!
//! Every case is a child process under a hard timeout, and a guard kills the child if the test
//! ends early, so a hang fails the test instead of leaving a process or a bound port behind.

use std::{
    fs,
    io::Read,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::tempdir;

const HARD_TIMEOUT: Duration = Duration::from_secs(15);

fn twinning_bin() -> PathBuf {
    std::env::var_os("TWINNING_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_twinning")))
}

/// Kills the child on drop so no test can leave a process (or its port) behind.
struct Guarded(Child);

impl Drop for Guarded {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn write_schema(dir: &Path) -> PathBuf {
    let path = dir.join("schema.sql");
    fs::write(
        &path,
        "CREATE TABLE observations (id NUMBER(38,0), label VARCHAR(32));\n",
    )
    .expect("write schema");
    path
}

fn spawn(args: &[&str]) -> Guarded {
    Guarded(
        Command::new(twinning_bin())
            .arg("snowflake")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn twinning snowflake"),
    )
}

/// Waits for exit up to the hard timeout. `None` = still running (the caller's guard kills it).
fn wait_with_timeout(child: &mut Guarded, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.0.try_wait().expect("try_wait") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn read_stdout(child: &mut Guarded) -> String {
    let mut out = String::new();
    child
        .0
        .stdout
        .take()
        .expect("piped stdout")
        .read_to_string(&mut out)
        .expect("read stdout");
    out
}

#[test]
fn snowflake_default_mode_validates_writes_report_and_exits_without_binding() {
    let dir = tempdir().expect("tempdir");
    let schema = write_schema(dir.path());
    let report = dir.path().join("out.json");

    // Hold the port: a twin that tried to bind it would fail with a listener error (exit 2).
    let held = TcpListener::bind(("127.0.0.1", 0)).expect("hold a port");
    let port = held.local_addr().expect("addr").port().to_string();

    let mut child = spawn(&[
        "--schema",
        schema.to_str().expect("path"),
        "--port",
        &port,
        "--report",
        report.to_str().expect("path"),
        "--json",
    ]);
    let status = wait_with_timeout(&mut child, HARD_TIMEOUT)
        .expect("default mode must exit on its own, not block serving");
    assert_eq!(status.code(), Some(0), "default mode exits 0");

    let stdout = read_stdout(&mut child);
    let summary: Value = serde_json::from_str(&stdout).expect("JSON readiness summary");
    assert_eq!(summary["version"], "twinning.snowflake-report.v0");
    assert_eq!(summary["outcome"], "PASS");
    assert_eq!(summary["catalog"]["table_count"], 1);
    let written: Value =
        serde_json::from_str(&fs::read_to_string(&report).expect("report written"))
            .expect("report JSON");
    assert_eq!(written["version"], "twinning.snowflake-report.v0");
    assert_eq!(written["catalog"]["table_count"], 1);
}

#[test]
fn snowflake_default_mode_human_output_and_refusal_still_work() {
    let dir = tempdir().expect("tempdir");
    let schema = write_schema(dir.path());

    let mut child = spawn(&["--schema", schema.to_str().expect("path")]);
    let status = wait_with_timeout(&mut child, HARD_TIMEOUT).expect("must exit");
    assert_eq!(status.code(), Some(0));
    let stdout = read_stdout(&mut child);
    assert!(
        stdout.contains("Snowflake twin ready - 1 tables"),
        "human readiness summary: {stdout}"
    );

    // A schema that does not exist is still a refusal (exit 2), not a hang.
    let mut refused = spawn(&["--schema", "/nonexistent/schema.sql", "--json"]);
    let status = wait_with_timeout(&mut refused, HARD_TIMEOUT).expect("refusal must exit");
    assert_eq!(status.code(), Some(2));
}

#[test]
fn snowflake_run_mode_runs_one_child_and_exits() {
    let dir = tempdir().expect("tempdir");
    let schema = write_schema(dir.path());

    let mut child = spawn(&[
        "--schema",
        schema.to_str().expect("path"),
        "--port",
        "0",
        "--run",
        "true",
        "--json",
    ]);
    let status = wait_with_timeout(&mut child, HARD_TIMEOUT).expect("--run must exit");
    assert_eq!(status.code(), Some(0));
    let run: Value = serde_json::from_str(&read_stdout(&mut child)).expect("run JSON");
    assert_eq!(run["version"], "twinning.snowflake-run.v0");
    assert_eq!(run["child"]["exit_code"], 0);
}

#[test]
fn snowflake_serve_mode_keeps_serving_until_sigterm_then_writes_the_report() {
    let dir = tempdir().expect("tempdir");
    let schema = write_schema(dir.path());
    let report = dir.path().join("serve.json");
    let port = {
        let probe = TcpListener::bind(("127.0.0.1", 0)).expect("probe port");
        probe.local_addr().expect("addr").port()
    };

    let mut child = spawn(&[
        "--schema",
        schema.to_str().expect("path"),
        "--serve",
        "--port",
        &port.to_string(),
        "--report",
        report.to_str().expect("path"),
        "--json",
    ]);

    // It must come up and keep running: connect, then confirm it has not exited.
    let deadline = Instant::now() + HARD_TIMEOUT;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        assert!(
            child.0.try_wait().expect("try_wait").is_none(),
            "--serve exited instead of serving"
        );
        assert!(Instant::now() < deadline, "--serve never started listening");
        thread::sleep(Duration::from_millis(50));
    }
    thread::sleep(Duration::from_millis(300));
    assert!(
        child.0.try_wait().expect("try_wait").is_none(),
        "--serve must keep running until a signal"
    );
    assert!(!report.exists(), "the report is written at shutdown");

    let status = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("spawn kill");
    assert!(status.success(), "kill -TERM: {status}");
    let status = wait_with_timeout(&mut child, HARD_TIMEOUT).expect("SIGTERM must stop --serve");
    assert_eq!(status.code(), Some(0), "clean shutdown exits 0");
    let written: Value =
        serde_json::from_str(&fs::read_to_string(&report).expect("report at shutdown"))
            .expect("report JSON");
    assert_eq!(written["version"], "twinning.snowflake-report.v0");
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "the port must be released after shutdown"
    );
}
