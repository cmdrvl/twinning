#![forbid(unsafe_code)]
#![cfg(all(feature = "postgres", unix))]

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

const TEST_ID: &str = "R05-R06";
const SCHEMA: &str = "CREATE TABLE public.deals (\n    deal_id TEXT PRIMARY KEY,\n    deal_name TEXT NOT NULL,\n    amount NUMERIC CHECK (amount >= 0)\n);\n";

struct ChildOutput {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

struct ProcessGuard {
    child: Option<Child>,
}

impl ProcessGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("server child")
    }

    fn id(&mut self) -> u32 {
        self.child_mut().id()
    }

    fn finish(mut self, timeout: Duration) -> ChildOutput {
        let mut child = self.child.take().expect("server child");
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll server child") {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().expect("kill server after shutdown timeout");
                let status = child.wait().expect("wait for killed server");
                panic!("R05/R06 server failed to exit after SIGTERM: {status}");
            }
            thread::sleep(Duration::from_millis(20));
        };
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .expect("server stdout pipe")
            .read_to_string(&mut stdout)
            .expect("read server stdout");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("server stderr pipe")
            .read_to_string(&mut stderr)
            .expect("read server stderr");
        ChildOutput {
            status,
            stdout,
            stderr,
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().expect("check server status").is_none() {
                child.kill().expect("kill server during cleanup");
            }
            let _ = child.wait();
        }
    }
}

#[test]
fn r05_r06_real_drivers_return_constraint_sqlstates_and_report_them() {
    let artifacts = artifacts_dir();
    fs::create_dir_all(&artifacts).expect("create test artifacts dir");
    println!("ARTIFACTS: {}", artifacts.display());
    let log_path = artifacts.join(format!("{TEST_ID}.log"));
    let mut log = ArtifactLog::new(&log_path);
    log.line(format!("[{TEST_ID}] event=artifacts path={:?}", artifacts));

    let schema_path = artifacts.join("schema.sql");
    let report_path = artifacts.join("report.json");
    let snapshot_path = artifacts.join("snapshot.twin");
    let trace_path = artifacts.join("trace.json");
    let scratch = artifacts.join("uv-scratch");
    fs::create_dir_all(&scratch).expect("create uv scratch directory");
    fs::write(&schema_path, SCHEMA).expect("write schema");

    let port = reserve_local_port();
    let twinning = find_twinning();
    let server_args = [
        "postgres".to_owned(),
        "--schema".to_owned(),
        schema_path.display().to_string(),
        "--serve".to_owned(),
        "--host".to_owned(),
        "127.0.0.1".to_owned(),
        "--port".to_owned(),
        port.to_string(),
        "--report".to_owned(),
        report_path.display().to_string(),
        "--snapshot".to_owned(),
        snapshot_path.display().to_string(),
        "--query-trace".to_owned(),
        trace_path.display().to_string(),
        "--json".to_owned(),
    ];
    let server_command = format!("{:?} {:?}", twinning, server_args);
    let server_child = Command::new(&twinning)
        .args(&server_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn twinning postgres --serve");
    let mut server = ProcessGuard::new(server_child);
    let server_pid = server.id();
    log.line(format!(
        "[{TEST_ID}] event=server_started command={server_command:?} pid={server_pid} port={port}"
    ));
    wait_until_listener_bound(port, server.child_mut(), &mut log);

    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/driver_cases.py");
    let driver_args = [
        "run".to_owned(),
        "--with".to_owned(),
        "psycopg2-binary".to_owned(),
        "--with".to_owned(),
        "sqlalchemy".to_owned(),
        "python".to_owned(),
        script.display().to_string(),
        "--port".to_owned(),
        port.to_string(),
    ];
    let driver_command = format!("uv {:?}", driver_args);
    log.line(format!(
        "[{TEST_ID}] event=drivers_started command={driver_command:?} cwd={:?}",
        scratch
    ));
    let driver_output = Command::new("uv")
        .args(&driver_args)
        .current_dir(&scratch)
        .env_remove("PGSSLMODE")
        .env_remove("PGGSSENCMODE")
        .env_remove("CONDA_DEFAULT_ENV")
        .env_remove("CONDA_EXE")
        .env_remove("CONDA_PREFIX")
        .env_remove("CONDA_PROMPT_MODIFIER")
        .env_remove("CONDA_PYTHON_EXE")
        .env_remove("CONDA_SHLVL")
        .output()
        .expect("run approved uv real-driver command");

    // Signal the server as soon as the real driver process exits.
    send_sigterm(server_pid);
    log.line(format!(
        "[{TEST_ID}] event=drivers_finished command={driver_command:?} exit_code={:?} stdout={:?} stderr={:?}",
        driver_output.status.code(),
        tail(&String::from_utf8_lossy(&driver_output.stdout), 4096),
        tail(&String::from_utf8_lossy(&driver_output.stderr), 4096),
    ));
    let server_output = server.finish(Duration::from_secs(15));
    log.line(format!(
        "[{TEST_ID}] event=server_finished pid={server_pid} port={port} exit_code={:?} stdout_tail={:?} stderr_tail={:?}",
        server_output.status.code(),
        tail(&server_output.stdout, 2048),
        tail(&server_output.stderr, 2048),
    ));

    assert!(
        report_path.exists(),
        "R05/R06 final report missing; server_exit={:?} stdout_tail={} stderr_tail={}",
        server_output.status.code(),
        tail(&server_output.stdout, 2048),
        tail(&server_output.stderr, 2048)
    );
    assert!(trace_path.exists(), "R05/R06 final query trace missing");
    let report: Value = serde_json::from_slice(&fs::read(&report_path).expect("read report"))
        .expect("parse report JSON");
    let trace: Value = serde_json::from_slice(&fs::read(&trace_path).expect("read query trace"))
        .expect("parse query trace JSON");
    let events = trace["events"].as_array().expect("query trace events");
    let traced_sqlstates = events
        .iter()
        .filter_map(|event| event["result"]["sqlstate"].as_str())
        .collect::<Vec<_>>();
    log.line(format!(
        "[{TEST_ID}] event=stop_summary outcome={} constraints={} request_count={} sqlstates={traced_sqlstates:?}",
        report["outcome"],
        report["constraints"],
        events.len(),
    ));

    let driver_stdout = String::from_utf8_lossy(&driver_output.stdout);
    let versions = driver_stdout
        .lines()
        .find_map(|line| line.strip_prefix("VERSIONS: "))
        .map(str::to_owned)
        .expect("driver script version line");
    let versions: Value = serde_json::from_str(&versions).expect("parse driver versions");
    log.line(format!("[{TEST_ID}] event=versions value={versions}"));

    let case_results = driver_stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str::<Value>(line).expect("parse driver case JSON"))
        .collect::<Vec<_>>();
    for case in &case_results {
        log.line(format!(
            "[{TEST_ID}] event=driver_case case={} sqlstate={} rows={} row_count={}",
            case["case"], case["sqlstate"], case["rows"], case["row_count"],
        ));
    }

    assert!(
        server_output.status.success(),
        "R05/R06 server exit was not clean: exit={:?} stdout_tail={} stderr_tail={}",
        server_output.status.code(),
        tail(&server_output.stdout, 2048),
        tail(&server_output.stderr, 2048)
    );
    assert!(
        driver_output.status.success(),
        "real driver cases failed: exit={:?}; versions={versions}; stdout={}; stderr={}",
        driver_output.status.code(),
        String::from_utf8_lossy(&driver_output.stdout),
        String::from_utf8_lossy(&driver_output.stderr)
    );
    assert_eq!(report["outcome"], "READY");
    assert_eq!(trace["version"], "twinning.query-trace.v0");

    for (case_name, expected_sqlstate) in [
        ("psycopg2_valid_insert", Value::Null),
        (
            "psycopg2_check_violation",
            Value::String("23514".to_owned()),
        ),
        (
            "psycopg2_duplicate_primary_key",
            Value::String("23505".to_owned()),
        ),
        ("psycopg2_select_by_pk", Value::Null),
        ("psycopg2_default_sslmode_select_by_pk", Value::Null),
        ("sqlalchemy_core_insert", Value::Null),
        ("sqlalchemy_core_select_by_pk", Value::Null),
    ] {
        let case = case_results
            .iter()
            .find(|case| case["case"] == case_name)
            .unwrap_or_else(|| panic!("missing real-driver case {case_name}"));
        assert_eq!(case["sqlstate"], expected_sqlstate, "case={case_name}");
    }
    assert_eq!(
        case_results
            .iter()
            .find(|case| case["case"] == "psycopg2_select_by_pk")
            .expect("psycopg2 SELECT case")["rows"],
        serde_json::json!([["psycopg2-valid-1", "Alpha"]])
    );
    assert_eq!(
        case_results
            .iter()
            .find(|case| case["case"] == "sqlalchemy_core_select_by_pk")
            .expect("SQLAlchemy SELECT case")["rows"],
        serde_json::json!([["sqlalchemy-valid-1", "Beta"]])
    );
    assert!(
        traced_sqlstates.contains(&"23514") && traced_sqlstates.contains(&"23505"),
        "query trace missed real-driver SQLSTATEs 23514/23505: {traced_sqlstates:?}"
    );

    // Binding M0c STOP RULE: do not proceed or describe compatibility as proven
    // unless live driver violations are included in the final report counters.
    assert_eq!(
        report["constraints"]["check_violations"], 1,
        "M0c STOP RULE: report did not count the real psycopg2 CHECK violation; report excerpt={} trace_sqlstates={traced_sqlstates:?}",
        report["constraints"]
    );
    assert_eq!(
        report["constraints"]["unique_violations"], 1,
        "M0c STOP RULE: report did not count the real psycopg2 duplicate-key violation; report excerpt={} trace_sqlstates={traced_sqlstates:?}",
        report["constraints"]
    );
}

struct ArtifactLog {
    file: File,
}

impl ArtifactLog {
    fn new(path: &Path) -> Self {
        Self {
            file: OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("open R05/R06 log"),
        }
    }

    fn line(&mut self, line: String) {
        println!("{line}");
        writeln!(self.file, "{line}").expect("write R05/R06 log");
        self.file.flush().expect("flush R05/R06 log");
    }
}

fn artifacts_dir() -> PathBuf {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_millis();
    std::env::temp_dir()
        .join("twinning-test-artifacts")
        .join(format!("{TEST_ID}-{}-{now_ms}", std::process::id()))
}

fn find_twinning() -> PathBuf {
    std::env::var_os("TWINNING_BIN")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("CARGO_TARGET_DIR").map(|target| {
                PathBuf::from(target).join("debug").join(if cfg!(windows) {
                    "twinning.exe"
                } else {
                    "twinning"
                })
            })
        })
        .unwrap_or_else(|| {
            PathBuf::from("target/debug").join(if cfg!(windows) {
                "twinning.exe"
            } else {
                "twinning"
            })
        })
}

fn reserve_local_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve free localhost port");
    let port = listener.local_addr().expect("read reserved port").port();
    drop(listener);
    port
}

fn wait_until_listener_bound(port: u16, child: &mut Child, log: &mut ArtifactLog) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().expect("poll twinning startup") {
            panic!("twinning exited before listen readiness: {status}");
        }
        match TcpListener::bind(("127.0.0.1", port)) {
            Ok(listener) => drop(listener),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                log.line(format!(
                    "[{TEST_ID}] event=ready port={port} readiness=listener_bind elapsed_ms={}",
                    started.elapsed().as_millis()
                ));
                return;
            }
            Err(error) => panic!("probe listener port {port}: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "twinning did not bind port {port} within 15 seconds"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn send_sigterm(pid: u32) {
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM to twinning");
    assert!(status.success(), "kill -TERM should succeed: {status}");
}

fn tail(text: &str, max_bytes: usize) -> String {
    let start = text.len().saturating_sub(max_bytes);
    let start = text
        .char_indices()
        .find(|(index, _)| *index >= start)
        .map_or(0, |(index, _)| index);
    text[start..].to_owned()
}
