pub mod common;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use common::scaled_deadline;
use serde_json::{Value, json};

const SERVER_BIN: &str = env!("CARGO_BIN_EXE_twinning-mcp");
const FAKE_TWINNING: &str = env!("CARGO_BIN_EXE_fake_twinning");
const GOLDEN_TOOLS: &str = include_str!("golden/tools_list.json");

#[derive(Default)]
struct LaunchOptions {
    workdir: Option<PathBuf>,
    current_dir: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
    server_bin: Option<PathBuf>,
    twinning_bin: Option<PathBuf>,
    fake_mode: Option<&'static str>,
    max_twins: Option<usize>,
    restricted: bool,
    empty_path: bool,
    env: Vec<(String, String)>,
}

struct TestServer {
    child: Child,
    pid: u32,
    stdin: Option<ChildStdin>,
    responses: Receiver<String>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<String>>,
    input_log: Vec<Value>,
    raw_stdout: String,
    next_id: u64,
    temp_dir: PathBuf,
    command_line: String,
    fake_mode: String,
    env_overrides: Vec<(String, String)>,
    finished: bool,
}

struct FinishedServer {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
    state_root: PathBuf,
    command_line: String,
    fake_mode: String,
    env_overrides: Vec<(String, String)>,
}

impl TestServer {
    fn new(temp: &tempfile::TempDir, options: LaunchOptions) -> Self {
        let fake_mode = options.fake_mode.unwrap_or("ok").to_owned();
        let workdir = options.workdir.clone().unwrap_or_else(|| {
            create_workdir(
                temp.path(),
                "CREATE TABLE public.deals (id INT PRIMARY KEY, amount INT CHECK (amount > 0));",
            )
        });
        let temp_dir = options
            .temp_dir
            .clone()
            .unwrap_or_else(|| temp.path().join("tmp"));
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir).unwrap();
        }

        let mut command = Command::new(
            options
                .server_bin
                .as_deref()
                .unwrap_or(Path::new(SERVER_BIN)),
        );
        if let Some(path) = options.workdir.as_deref() {
            command.args(["--workdir", path.to_str().unwrap()]);
        } else if options.current_dir.is_none() {
            command.args(["--workdir", workdir.to_str().unwrap()]);
        }
        if let Some(path) = options.twinning_bin.as_deref() {
            command.args(["--twinning-bin", path.to_str().unwrap()]);
        } else {
            command.args(["--twinning-bin", FAKE_TWINNING]);
        }
        if let Some(count) = options.max_twins {
            command.args(["--max-twins", &count.to_string()]);
        }
        if options.restricted {
            command.arg("--restricted");
        }
        command
            .current_dir(options.current_dir.as_deref().unwrap_or(&workdir))
            .env("TMPDIR", &temp_dir)
            .env_remove("TWINNING_BIN")
            .env_remove("TWINNING_MCP_WORKDIR")
            .env_remove("TWINNING_MCP_MAX_TWINS")
            .env_remove("TWINNING_MCP_RESTRICTED")
            .env("FAKE_TWINNING_MODE", &fake_mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        if options.empty_path {
            command.env("PATH", temp_dir.join("empty-path"));
        }
        let mut env_overrides = vec![("FAKE_TWINNING_MODE".into(), fake_mode.clone())];
        if options.empty_path {
            env_overrides.push((
                "PATH".into(),
                temp_dir.join("empty-path").display().to_string(),
            ));
        }
        for (name, value) in &options.env {
            command.env(name, value);
            env_overrides.push((name.clone(), value.clone()));
        }
        let command_line = std::iter::once(command.get_program().to_string_lossy().into_owned())
            .chain(
                command
                    .get_args()
                    .map(|argument| argument.to_string_lossy().into_owned()),
            )
            .collect::<Vec<_>>()
            .join(" ");

        let mut child = command.spawn().expect("twinning-mcp starts");
        let pid = child.id();
        let stdout = child.stdout.take().expect("server stdout is piped");
        let (sender, responses) = mpsc::channel();
        let stdout_reader = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if sender.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let stderr = child.stderr.take().expect("server stderr is piped");
        let stderr_reader = thread::spawn(move || {
            let mut reader = stderr;
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).expect("read server stderr");
            String::from_utf8_lossy(&bytes).into_owned()
        });
        Self {
            stdin: child.stdin.take(),
            child,
            pid,
            responses,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            input_log: Vec::new(),
            raw_stdout: String::new(),
            next_id: 1,
            temp_dir,
            command_line,
            fake_mode,
            env_overrides,
            finished: false,
        }
    }

    fn call(&mut self, method: &str, params: Option<Value>) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut request = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            request["params"] = params;
        }
        let rendered = serde_json::to_string(&request).unwrap();
        let stdin = self.stdin.as_mut().expect("server input remains open");
        writeln!(stdin, "{rendered}").expect("request writes");
        stdin.flush().expect("request flushes");
        self.input_log.push(request);
        let line = poll_until(
            scaled_deadline(Duration::from_secs(15)),
            Duration::from_millis(20),
            "JSON-RPC response",
            || self.responses.try_recv().ok(),
        );
        self.raw_stdout.push_str(&line);
        serde_json::from_str(&line).expect("server emits a JSON-RPC response line")
    }

    fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        self.call(
            "tools/call",
            Some(json!({"name":name,"arguments":arguments})),
        )
    }

    fn state_root(&self) -> PathBuf {
        self.temp_dir
            .join("twinning-mcp")
            .join(self.pid.to_string())
    }

    fn state_dir(&self, twin_id: &str) -> PathBuf {
        self.state_root().join(twin_id)
    }

    fn signal(&self, signal: i32) {
        // SAFETY: this PID is the test-owned twinning-mcp child.
        let result = unsafe { libc::kill(self.pid as libc::pid_t, signal) };
        assert_eq!(0, result, "signal server pid {}", self.pid);
    }

    fn finish(mut self, test_id: &str) -> FinishedServer {
        self.stdin.take();
        let status = self
            .child
            .wait()
            .expect("server exits after stdin EOF or signal");
        let stdout_reader = self.stdout_reader.take().unwrap();
        stdout_reader.join().expect("stdout reader exits");
        let stderr_reader = self.stderr_reader.take().unwrap();
        let stderr = stderr_reader.join().expect("stderr reader exits");
        let output = FinishedServer {
            status,
            stdout: self.raw_stdout.clone(),
            stderr,
            state_root: self.state_root(),
            command_line: self.command_line.clone(),
            fake_mode: self.fake_mode.clone(),
            env_overrides: self.env_overrides.clone(),
        };
        record_session(test_id, self.pid, &self.input_log, &output);
        self.finished = true;
        output
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if self.child.try_wait().ok().flatten().is_none() {
            // SAFETY: the test server was started in a fresh process group.
            let _ = unsafe { libc::killpg(self.pid as libc::pid_t, libc::SIGKILL) };
        }
        if let Ok(entries) = fs::read_dir(self.state_root()) {
            for entry in entries.flatten() {
                if let Ok(pid) = fs::read_to_string(entry.path().join("child.pid"))
                    && let Ok(pid) = pid.trim().parse::<libc::pid_t>()
                {
                    // SAFETY: each child is created in a fresh process group with pgid == pid.
                    let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
                }
            }
        }
        let _ = self.child.wait();
    }
}

fn test_tempdir() -> tempfile::TempDir {
    let _ = common::artifacts_dir("twinning_mcp_stub_integration");
    tempfile::tempdir().unwrap()
}

fn poll_until<T>(
    deadline: std::time::Instant,
    interval: Duration,
    what: &str,
    mut observe: impl FnMut() -> Option<T>,
) -> T {
    let start = std::time::Instant::now();
    let label = std::thread::current()
        .name()
        .and_then(|name| {
            let prefix = if name.starts_with("rs") {
                name.get(..4)
            } else {
                name.get(..3)
            }?;
            let digits = prefix
                .strip_prefix("rs")
                .or_else(|| prefix.strip_prefix('s'))?;
            digits
                .bytes()
                .all(|byte| byte.is_ascii_digit())
                .then(|| prefix.to_ascii_uppercase())
        })
        .unwrap_or_else(|| "TEST".into());
    loop {
        if let Some(value) = observe() {
            println!(
                "[{label}] poll what={what:?} elapsed_ms={}",
                start.elapsed().as_millis()
            );
            return value;
        }
        let last = format!("not observed after {} ms", start.elapsed().as_millis());
        if std::time::Instant::now() >= deadline {
            panic!(
                "[{label}] poll timeout what={what:?} elapsed_ms={} last={last}",
                start.elapsed().as_millis()
            );
        }
        std::thread::sleep(interval);
    }
}

fn create_workdir(root: &Path, schema: &str) -> PathBuf {
    let workdir = root.join("work");
    fs::create_dir_all(&workdir).unwrap();
    fs::write(workdir.join("schema.sql"), schema).unwrap();
    workdir
}

fn default_launch(workdir: &Path, mode: &'static str) -> LaunchOptions {
    LaunchOptions {
        workdir: Some(workdir.to_path_buf()),
        fake_mode: Some(mode),
        ..LaunchOptions::default()
    }
}

fn start_args(workdir: &Path) -> Value {
    json!({"kind":"postgres","schema_path":workdir.join("schema.sql")})
}

fn rest_start_args(spec_path: &Path) -> Value {
    json!({"kind":"rest","spec_path":spec_path})
}

fn write_rest_spec(workdir: &Path, name: &str, description: &str) -> PathBuf {
    let path = workdir.join(name);
    fs::write(
        &path,
        format!(
            "openapi: 3.0.3\ninfo:\n  title: Test REST twin\n  version: '1.0'\n  description: {description:?}\nservers:\n  - url: https://api.example.invalid/{{basePath}}\n    variables:\n      basePath:\n        default: v1\npaths:\n  /items:\n    get:\n      responses:\n        '200':\n          description: items\n          content:\n            application/json:\n              schema:\n                $ref: '#/components/schemas/Item'\ncomponents:\n  schemas:\n    Item:\n      type: object\n      properties:\n        id:\n          type: integer\n"
        ),
    )
    .unwrap();
    path
}

fn successful_tool(response: &Value) -> Value {
    assert!(
        response.get("error").is_none(),
        "protocol error: {response}"
    );
    assert_eq!(
        Some(false),
        response["result"]["isError"].as_bool(),
        "tool call should succeed: {response}"
    );
    serde_json::from_str(
        response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool result content is text JSON"),
    )
    .expect("tool success body is JSON")
}

fn failed_tool(response: &Value, code: &str) -> Value {
    assert!(
        response.get("error").is_none(),
        "expected tool error, got {response}"
    );
    assert_eq!(Some(true), response["result"]["isError"].as_bool());
    let body: Value = serde_json::from_str(
        response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool error content is text JSON"),
    )
    .expect("tool error body is JSON");
    assert_eq!(code, body["error"]["code"]);
    assert!(
        body["error"]["hint"]
            .as_str()
            .is_some_and(|hint| !hint.trim().is_empty())
    );
    body
}

fn record_session(test_id: &str, pid: u32, requests: &[Value], output: &FinishedServer) {
    let directory = common::artifacts_dir("twinning_mcp_stub_integration");
    fs::write(
        directory.join(format!("{test_id}.stdout.json")),
        &output.stdout,
    )
    .expect("stdout artifact writes");
    fs::write(
        directory.join(format!("{test_id}.stderr.log")),
        &output.stderr,
    )
    .expect("stderr artifact writes");
    let mut log = format!(
        "[{test_id}] command={} pid={pid} fake_mode={} exit={:?}\n",
        output.command_line,
        output.fake_mode,
        output.status.code()
    );
    for (name, value) in &output.env_overrides {
        log.push_str(&format!("[{test_id}] env {name}={value}\n"));
    }
    copy_state_artifacts(test_id, &output.state_root, directory, &mut log);
    for request in requests {
        log.push_str(&format!(
            "[{test_id}] request id={} method={}\n",
            request.get("id").unwrap_or(&Value::Null),
            request["method"].as_str().unwrap_or("<invalid>")
        ));
    }
    for line in output.stdout.lines() {
        if let Ok(response) = serde_json::from_str::<Value>(line) {
            let keys = response["result"]
                .as_object()
                .map(|object| object.keys().cloned().collect::<Vec<_>>().join(","))
                .unwrap_or_default();
            let tool_error = response["result"]["content"][0]["text"]
                .as_str()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .and_then(|body| body["error"]["code"].as_str().map(str::to_owned));
            let error_code = response["error"]["code"]
                .as_i64()
                .map(|code| code.to_string())
                .or(tool_error)
                .unwrap_or_else(|| "-".into());
            log.push_str(&format!(
                "[{test_id}] response id={} error_code={} result_keys={}\n",
                response.get("id").unwrap_or(&Value::Null),
                error_code,
                keys
            ));
        }
    }
    fs::write(directory.join(format!("{test_id}.log")), &log).expect("session log writes");
    print!("{log}");
}

fn copy_state_artifacts(test_id: &str, state_root: &Path, artifacts: &Path, log: &mut String) {
    let Ok(entries) = fs::read_dir(state_root) else {
        return;
    };
    let case_dir = artifacts.join(test_id);
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let twin_id = entry.file_name().to_string_lossy().into_owned();
        let destination = case_dir.join(&twin_id);
        fs::create_dir_all(&destination).expect("create per-twin artifacts directory");
        for file in fs::read_dir(&path)
            .expect("read twin state directory")
            .flatten()
        {
            let source = file.path();
            if source.is_file() {
                fs::copy(&source, destination.join(file.file_name()))
                    .expect("copy twin state artifact");
            }
        }
        let child = fs::read_to_string(path.join("child.pid")).unwrap_or_default();
        let report = fs::read(path.join("report.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        let port = report
            .as_ref()
            .and_then(|value| value["port"].as_u64())
            .or_else(|| {
                fs::read(path.join("argv_summary.json"))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .and_then(|value| value["port"].as_u64())
            })
            .map(|port| port.to_string())
            .unwrap_or_else(|| "unknown".into());
        if !child.trim().is_empty() {
            log.push_str(&format!(
                "[{test_id}] child twin_id={twin_id} pid={} port={port}\n",
                child.trim()
            ));
        }
    }
}

fn child_pid(server: &TestServer, twin_id: &str) -> u32 {
    fs::read_to_string(server.state_dir(twin_id).join("child.pid"))
        .expect("child pid file exists")
        .trim()
        .parse()
        .expect("child pid is numeric")
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: signal zero checks process existence without sending a signal.
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn assert_process_gone(pid: u32, what: &str) {
    poll_until(
        scaled_deadline(Duration::from_secs(6)),
        Duration::from_millis(20),
        what,
        || (!process_alive(pid)).then_some(()),
    );
    assert!(!process_alive(pid), "{what}: process {pid} is still alive");
}

fn list_twins(server: &mut TestServer) -> Vec<Value> {
    let body = successful_tool(&server.call_tool("twin_list", json!({})));
    body["twins"].as_array().expect("twins array").clone()
}

#[test]
fn s01_start_list_stop_report_and_delayed_readiness() {
    let temp = test_tempdir();
    let workdir = create_workdir(
        temp.path(),
        "CREATE TABLE public.deals (id INT PRIMARY KEY);",
    );
    let mut server = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            fake_mode: Some("listen_late"),
            env: vec![("FAKE_TWINNING_LISTEN_DELAY_MS".into(), "150".into())],
            ..LaunchOptions::default()
        },
    );
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    let twin_id = started["twin_id"].as_str().unwrap();
    assert_eq!("postgres", started["kind"]);
    assert!(started["port"].as_u64().unwrap() > 0);
    let pid = started["pid"].as_u64().unwrap() as u32;
    assert!(
        started["connection_string"]
            .as_str()
            .unwrap()
            .starts_with("postgresql://u@127.0.0.1:")
    );
    assert!(started["started_at"].as_str().is_some());
    assert!(started["expires_at"].as_str().is_some());
    assert!(TcpStream::connect(("127.0.0.1", started["port"].as_u64().unwrap() as u16)).is_ok());

    let state = server.state_dir(twin_id);
    assert_eq!(
        0o700,
        fs::metadata(&state).unwrap().permissions().mode() & 0o777
    );
    for name in ["child.pid", "stdout.json", "stderr.log"] {
        assert!(state.join(name).is_file(), "missing {name}");
    }
    assert_eq!(pid, child_pid(&server, twin_id));
    let listed = list_twins(&mut server);
    assert_eq!(1, listed.len());
    assert_eq!("running", listed[0]["status"]);
    failed_tool(
        &server.call_tool("twin_report", json!({"twin_id":twin_id})),
        "E_REPORT_NOT_READY",
    );

    let stopped = successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin_id})));
    assert_eq!(twin_id, stopped["twin_id"]);
    assert_eq!("stopped", stopped["status"]);
    assert_eq!("stopped", list_twins(&mut server)[0]["status"]);
    assert!(stopped["constraints"].is_object());
    assert!(stopped["tables"].is_object());
    let trace: Value =
        serde_json::from_slice(&fs::read(state.join("trace.json")).unwrap()).unwrap();
    assert_eq!(
        trace["events"].as_array().unwrap().len(),
        stopped["request_count"].as_u64().unwrap() as usize
    );
    assert!(
        stopped["report_path"]
            .as_str()
            .unwrap()
            .ends_with("report.json")
    );
    assert!(
        stopped["trace_path"]
            .as_str()
            .unwrap()
            .ends_with("trace.json")
    );

    let report = successful_tool(&server.call_tool("twin_report", json!({"twin_id":twin_id})));
    assert_eq!("twinning.v0", report["version"]);
    assert!(report["trace_summary"].is_object());
    let finished = server.finish("S01");
    assert!(finished.status.success());
    assert_process_gone(pid, "S01 twin stop");
}

#[test]
fn s02_exit_early_reports_code_and_stderr_tail() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "exit_early"));
    let error = failed_tool(
        &server.call_tool("twin_start", start_args(&workdir)),
        "E_START_FAILED",
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exit code 3")
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("stderr_tail=fake exited early")
    );
    let finished = server.finish("S02");
    assert!(finished.status.success());
}

#[test]
fn s03_never_listen_times_out_kills_child_and_returns_no_dsn() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut options = default_launch(&workdir, "never_listen");
    options
        .env
        .push(("TWINNING_MCP_READY_TIMEOUT_MS".into(), "250".into()));
    let mut server = TestServer::new(&temp, options);
    let response = server.call_tool("twin_start", start_args(&workdir));
    let error = failed_tool(&response, "E_READY_TIMEOUT");
    assert!(
        !serde_json::to_string(&response)
            .unwrap()
            .contains("postgresql://")
    );
    assert!(
        !serde_json::to_string(&error)
            .unwrap()
            .contains("postgresql://")
    );
    let mut pids = Vec::new();
    if let Ok(entries) = fs::read_dir(server.state_root()) {
        for entry in entries.flatten() {
            if let Ok(text) = fs::read_to_string(entry.path().join("child.pid"))
                && let Ok(pid) = text.trim().parse::<u32>()
            {
                pids.push(pid);
            }
        }
    }
    let finished = server.finish("S03");
    assert!(finished.status.success());
    for pid in pids {
        assert_process_gone(pid, "S03 readiness timeout child");
    }
}

#[test]
fn s04_ignore_sigterm_is_killed_after_stop_timeout() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut options = default_launch(&workdir, "ignore_sigterm");
    options
        .env
        .push(("TWINNING_MCP_STOP_TIMEOUT_MS".into(), "250".into()));
    let mut server = TestServer::new(&temp, options);
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    let pid = started["pid"].as_u64().unwrap() as u32;
    let error = failed_tool(
        &server.call_tool("twin_stop", json!({"twin_id":started["twin_id"]})),
        "E_STOP_TIMEOUT",
    );
    assert!(error["error"]["hint"].as_str().unwrap().contains("killed"));
    let finished = server.finish("S04");
    assert!(finished.status.success());
    assert_process_gone(pid, "S04 child after SIGKILL");
}

#[test]
fn s05_sixth_start_hits_default_live_cap() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let mut started = Vec::new();
    for _ in 0..5 {
        started.push(successful_tool(
            &server.call_tool("twin_start", start_args(&workdir)),
        ));
    }
    failed_tool(
        &server.call_tool("twin_start", start_args(&workdir)),
        "E_TWIN_CAP_REACHED",
    );
    assert_eq!(5, list_twins(&mut server).len());
    let finished = server.finish("S05");
    assert!(finished.status.success());
    for twin in started {
        assert_process_gone(twin["pid"].as_u64().unwrap() as u32, "S05 EOF cleanup");
    }
}

#[test]
fn s06_ttl_expiry_stops_twin_and_retains_report() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut options = default_launch(&workdir, "ok");
    options
        .env
        .push(("TWINNING_MCP_TTL_SECONDS_TEST".into(), "1".into()));
    let mut server = TestServer::new(&temp, options);
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    let twin_id = started["twin_id"].as_str().unwrap().to_owned();
    let expired = poll_until(
        scaled_deadline(Duration::from_secs(5)),
        Duration::from_millis(20),
        "TTL status expired",
        || {
            let twins = list_twins(&mut server);
            twins
                .into_iter()
                .find(|twin| twin["twin_id"] == twin_id && twin["status"] == "expired")
        },
    );
    assert_eq!("expired", expired["status"]);
    let stopped = successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin_id})));
    assert_eq!("expired", stopped["status"]);
    let finished = server.finish("S06");
    assert!(finished.status.success());
}

#[test]
fn s07_stdin_eof_stops_all_child_pids() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    let pid = started["pid"].as_u64().unwrap() as u32;
    let finished = server.finish("S07");
    assert!(finished.status.success());
    assert_process_gone(pid, "S07 child after stdin EOF");
}

#[test]
fn s08_sigterm_stops_all_child_pids() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    let pid = started["pid"].as_u64().unwrap() as u32;
    server.signal(libc::SIGTERM);
    let finished = server.finish("S08");
    assert!(finished.status.success());
    assert_process_gone(pid, "S08 child after server SIGTERM");
}

#[test]
fn s09_stale_sweep_kills_orphan_and_preserves_live_server() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let shared_tmp = temp.path().join("shared-tmp");
    fs::create_dir_all(&shared_tmp).unwrap();

    let mut crashed = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(shared_tmp.clone()),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    let orphan = successful_tool(&crashed.call_tool("twin_start", start_args(&workdir)));
    let orphan_pid = orphan["pid"].as_u64().unwrap() as u32;
    let crashed_root = crashed.state_root();

    let mut live = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(shared_tmp.clone()),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    let live_twin = successful_tool(&live.call_tool("twin_start", start_args(&workdir)));
    let live_pid = live_twin["pid"].as_u64().unwrap() as u32;
    let live_root = live.state_root();

    crashed.signal(libc::SIGKILL);
    let killed_server = crashed.finish("S09-crashed-server");
    assert!(!killed_server.status.success());
    assert!(
        process_alive(orphan_pid),
        "crash should leave the fake child for stale sweep"
    );

    let mut sweeper = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(shared_tmp.clone()),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    let sweep_twin = successful_tool(&sweeper.call_tool("twin_start", start_args(&workdir)));
    assert_process_gone(orphan_pid, "S09 stale orphan child");
    assert!(!crashed_root.exists(), "dead server directory is removed");
    assert!(live_root.exists(), "active server directory is preserved");
    assert!(process_alive(live_pid), "active server child is preserved");

    successful_tool(&live.call_tool("twin_stop", json!({"twin_id":live_twin["twin_id"]})));
    successful_tool(&sweeper.call_tool("twin_stop", json!({"twin_id":sweep_twin["twin_id"]})));
    assert!(live.finish("S09-live-server").status.success());
    assert!(sweeper.finish("S09-sweeper-server").status.success());
}

#[test]
fn s10_report_while_running_returns_not_ready() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    failed_tool(
        &server.call_tool("twin_report", json!({"twin_id":started["twin_id"]})),
        "E_REPORT_NOT_READY",
    );
    assert!(server.finish("S10").status.success());
}

#[test]
fn s11_unknown_twin_returns_not_found() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    failed_tool(
        &server.call_tool("twin_stop", json!({"twin_id":"tw_000000"})),
        "E_TWIN_NOT_FOUND",
    );
    assert!(server.finish("S11").status.success());
}

#[test]
fn s12_non_postgres_kind_is_refused_before_binary_or_spawn() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut options = default_launch(&workdir, "ok");
    options.twinning_bin = Some(temp.path().join("does-not-exist"));
    let mut server = TestServer::new(&temp, options);
    let error = failed_tool(
        &server.call_tool(
            "twin_start",
            json!({"kind":"mysql","schema_path":"schema.sql"}),
        ),
        "E_KIND_UNSUPPORTED",
    );
    assert_eq!("Choose kind postgres or rest", error["error"]["hint"]);
    let state_root = server.state_root();
    let finished = server.finish("S12");
    assert!(finished.status.success());
    assert!(!state_root.exists());
}

#[test]
fn s13_tools_list_matches_golden_with_sorted_closed_schemas() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let response = server.call("tools/list", None);
    assert_eq!(
        serde_json::from_str::<Value>(GOLDEN_TOOLS).unwrap(),
        response["result"]
    );
    let tools = response["result"]["tools"].as_array().unwrap();
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        tools
            .iter()
            .all(|tool| tool["inputSchema"]["additionalProperties"] == false)
    );
    assert!(server.finish("S13").status.success());
}

#[test]
fn s14_stdout_is_jsonrpc_only_and_secret_ddl_is_not_logged() {
    let temp = test_tempdir();
    let marker = "SECRET_MARKER_7f3a";
    let workdir = create_workdir(
        temp.path(),
        &format!("CREATE TABLE t (id INT); -- {marker}\n"),
    );
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let started = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    successful_tool(&server.call_tool("twin_stop", json!({"twin_id":started["twin_id"]})));
    let finished = server.finish("S14");
    assert!(finished.status.success());
    assert!(!finished.stdout.contains(marker));
    assert!(!finished.stderr.contains(marker));
    for line in finished.stdout.lines() {
        let _: Value = serde_json::from_str(line).expect("every stdout line is JSON-RPC JSON");
    }
    for line in finished.stderr.lines() {
        let timestamp = line.split_whitespace().next().expect("timestamp field");
        chrono::DateTime::parse_from_rfc3339(timestamp).expect("RFC3339 UTC timestamp");
        assert!(line.contains(" INFO ") || line.contains(" WARN ") || line.contains(" ERROR "));
    }
}

#[test]
fn s15_three_twins_remain_ready_on_distinct_loopback_ports() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let twins = (0..3)
        .map(|_| successful_tool(&server.call_tool("twin_start", start_args(&workdir))))
        .collect::<Vec<_>>();
    let mut ports = twins
        .iter()
        .map(|twin| twin["port"].as_u64().unwrap())
        .collect::<Vec<_>>();
    ports.sort_unstable();
    ports.dedup();
    assert_eq!(3, ports.len());
    for port in ports {
        assert!(TcpStream::connect(("127.0.0.1", port as u16)).is_ok());
    }
    assert_eq!(3, list_twins(&mut server).len());
    assert!(server.finish("S15").status.success());
}

#[test]
fn s16_listener_bind_retry_succeeds_and_repeated_refusal_is_reported() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut retry = TestServer::new(&temp, default_launch(&workdir, "addr_in_use_once"));
    let started = successful_tool(&retry.call_tool("twin_start", start_args(&workdir)));
    successful_tool(&retry.call_tool("twin_stop", json!({"twin_id":started["twin_id"]})));
    assert!(retry.finish("S16-retry").status.success());

    let mut repeated = TestServer::new(&temp, default_launch(&workdir, "always_bind_refuse"));
    let error = failed_tool(
        &repeated.call_tool("twin_start", start_args(&workdir)),
        "E_START_FAILED",
    );
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("exit code 2"));
    assert!(message.contains("E_RUNTIME_IO"));
    assert!(message.contains("stderr_tail"));
    assert!(repeated.finish("S16-repeated").status.success());
}

#[test]
fn s17_stop_requires_readable_report_but_accepts_exit_one_report() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut crash = TestServer::new(&temp, default_launch(&workdir, "crash_on_stop"));
    let twin = successful_tool(&crash.call_tool("twin_start", start_args(&workdir)));
    let error = failed_tool(
        &crash.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})),
        "E_REPORT_UNREADABLE",
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("fake crash on stop")
    );
    assert!(crash.finish("S17-crash").status.success());

    let mut exit_one = TestServer::new(&temp, default_launch(&workdir, "exit1_with_report"));
    let twin = successful_tool(&exit_one.call_tool("twin_start", start_args(&workdir)));
    let stopped =
        successful_tool(&exit_one.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})));
    assert_eq!(1, stopped["exit_code"]);
    assert!(exit_one.finish("S17-exit-one").status.success());
}

#[test]
fn s18_pre_spawn_errors_name_fields_have_hints_and_do_not_leak_schema() {
    let temp = test_tempdir();
    let workdir = create_workdir(
        temp.path(),
        "CREATE TABLE t (id INT); -- SECRET_SCHEMA_BODY",
    );
    let outside = temp.path().join("outside.sql");
    fs::write(&outside, "OUTSIDE_SECRET_BODY").unwrap();
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let missing = failed_tool(
        &server.call_tool(
            "twin_start",
            json!({"kind":"postgres","schema_path":"missing.sql"}),
        ),
        "E_PATH_NOT_FOUND",
    );
    assert!(
        missing["error"]["message"]
            .as_str()
            .unwrap()
            .contains("schema_path")
    );
    let outside_error = failed_tool(
        &server.call_tool(
            "twin_start",
            json!({"kind":"postgres","schema_path":outside}),
        ),
        "E_PATH_OUTSIDE_WORKDIR",
    );
    assert!(
        outside_error["error"]["hint"]
            .as_str()
            .unwrap()
            .contains("workdir")
    );
    let wrong = failed_tool(
        &server.call_tool("twin_start", json!({"kind":"postgres","schema_path":42})),
        "E_ARGUMENT_INVALID",
    );
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("schema_path")
    );
    let extra = failed_tool(
        &server.call_tool(
            "twin_start",
            json!({"kind":"postgres","schema_path":"schema.sql","unexpected":"x"}),
        ),
        "E_ARGUMENT_INVALID",
    );
    assert!(
        extra["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unexpected")
    );
    let oversized = workdir.join("oversized.sql");
    fs::write(&oversized, vec![b'x'; 5 * 1024 * 1024 + 1]).unwrap();
    let too_large = failed_tool(
        &server.call_tool(
            "twin_start",
            json!({"kind":"postgres","schema_path":"oversized.sql"}),
        ),
        "E_ARGUMENT_INVALID",
    );
    assert!(
        too_large["error"]["message"]
            .as_str()
            .unwrap()
            .contains("5 MiB")
    );
    let no_children = successful_tool(&server.call_tool("twin_list", json!({})));
    assert!(no_children["twins"].as_array().unwrap().is_empty());
    assert!(!server.raw_stdout.contains("SECRET_SCHEMA_BODY"));
    assert!(!server.raw_stdout.contains("OUTSIDE_SECRET_BODY"));
    assert!(server.finish("S18-path-errors").status.success());

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut unsafe_workdir = TestServer::new(
        &temp,
        LaunchOptions {
            current_dir: Some(PathBuf::from("/")),
            temp_dir: Some(temp.path().join("tmp")),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    failed_tool(
        &unsafe_workdir.call_tool(
            "twin_start",
            json!({"kind":"postgres","schema_path":workdir.join("schema.sql")}),
        ),
        "E_WORKDIR_UNSAFE",
    );
    assert!(unsafe_workdir.finish("S18-unsafe-workdir").status.success());

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let copied_server = temp.path().join("server-copy/twinning-mcp");
    fs::create_dir_all(copied_server.parent().unwrap()).unwrap();
    fs::copy(SERVER_BIN, &copied_server).unwrap();
    let bad_bin = temp.path().join("missing/twinning");
    let mut bad_twinning = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            server_bin: Some(copied_server),
            twinning_bin: Some(bad_bin),
            empty_path: true,
            ..LaunchOptions::default()
        },
    );
    failed_tool(
        &bad_twinning.call_tool("twin_start", start_args(&workdir)),
        "E_TWINNING_NOT_FOUND",
    );
    assert!(bad_twinning.finish("S18-bad-twinning-bin").status.success());

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let blocker = temp.path().join("not-a-directory");
    fs::write(&blocker, "x").unwrap();
    let mut state_failure = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(blocker),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    failed_tool(
        &state_failure.call_tool("twin_start", start_args(&workdir)),
        "E_INTERNAL",
    );
    assert!(state_failure.finish("S18-state-dir").status.success());
}

#[test]
fn s19_seed_contract_writes_private_contract_and_does_not_consume_cap() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            fake_mode: Some("ok"),
            max_twins: Some(1),
            ..LaunchOptions::default()
        },
    );
    let contract = successful_tool(&server.call_tool(
        "twin_seed_contract",
        json!({"kind":"postgres","schema_path":"schema.sql"}),
    ));
    assert_eq!(1, contract["table_count"]);
    let contract_path = PathBuf::from(contract["contract_path"].as_str().unwrap());
    let parent = contract_path.parent().unwrap();
    assert!(
        parent
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("contract_")
    );
    assert_eq!(
        0o700,
        fs::metadata(parent).unwrap().permissions().mode() & 0o777
    );
    assert!(contract_path.is_file());
    let second = successful_tool(&server.call_tool(
        "twin_seed_contract",
        json!({"kind":"postgres","schema_path":"schema.sql"}),
    ));
    assert_ne!(contract["contract_path"], second["contract_path"]);
    let twin = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    assert_eq!("postgres", twin["kind"]);
    assert!(server.finish("S19-success").status.success());

    let mut failure = TestServer::new(&temp, default_launch(&workdir, "contract_fail"));
    let error = failed_tool(
        &failure.call_tool(
            "twin_seed_contract",
            json!({"kind":"postgres","schema_path":"schema.sql"}),
        ),
        "E_SEED_CONTRACT_FAILED",
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("seed contract export failed")
    );
    assert!(failure.finish("S19-failure").status.success());
}

#[test]
fn s20_restricted_mode_refuses_optional_seed_before_spawn() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    fs::write(workdir.join("seed.jsonl"), "{}").unwrap();
    let mut options = default_launch(&workdir, "ok");
    options.restricted = true;
    let mut server = TestServer::new(&temp, options);
    let error = failed_tool(
        &server.call_tool(
            "twin_start",
            json!({"kind":"postgres","schema_path":"schema.sql","seed_path":"seed.jsonl"}),
        ),
        "E_ARGUMENT_INVALID",
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("seed_path")
    );
    assert!(!server.state_root().exists());
    assert!(server.finish("S20").status.success());
}

#[test]
fn s21_protocol_errors_stop_idempotence_and_doctor_degradation() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let unknown = server.call_tool("made_up_tool", json!({}));
    assert_eq!(Some(-32602), unknown["error"]["code"].as_i64());
    let array_args = server.call(
        "tools/call",
        Some(json!({"name":"twin_list","arguments":[]})),
    );
    assert_eq!(Some(-32602), array_args["error"]["code"].as_i64());
    assert!(server.finish("S21-protocol").status.success());

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut stopped_server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let stopped_twin =
        successful_tool(&stopped_server.call_tool("twin_start", start_args(&workdir)));
    let first_stop = successful_tool(
        &stopped_server.call_tool("twin_stop", json!({"twin_id":stopped_twin["twin_id"]})),
    );
    let second_stop = successful_tool(
        &stopped_server.call_tool("twin_stop", json!({"twin_id":stopped_twin["twin_id"]})),
    );
    assert_eq!(first_stop, second_stop);
    assert!(
        stopped_server
            .finish("S21-stopped-idempotent")
            .status
            .success()
    );

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut options = default_launch(&workdir, "ok");
    options
        .env
        .push(("TWINNING_MCP_TTL_SECONDS_TEST".into(), "1".into()));
    let mut expired_server = TestServer::new(&temp, options);
    let started = successful_tool(&expired_server.call_tool("twin_start", start_args(&workdir)));
    let twin_id = started["twin_id"].as_str().unwrap().to_owned();
    poll_until(
        scaled_deadline(Duration::from_secs(5)),
        Duration::from_millis(20),
        "S21 TTL expiry",
        || {
            list_twins(&mut expired_server)
                .into_iter()
                .find(|twin| twin["twin_id"] == twin_id && twin["status"] == "expired")
        },
    );
    let first_stop =
        successful_tool(&expired_server.call_tool("twin_stop", json!({"twin_id":twin_id})));
    let second_stop =
        successful_tool(&expired_server.call_tool("twin_stop", json!({"twin_id":twin_id})));
    assert_eq!(first_stop, second_stop);
    assert!(
        expired_server
            .finish("S21-expired-idempotent")
            .status
            .success()
    );

    let temp = test_tempdir();
    let copied_server = temp.path().join("server-copy/twinning-mcp");
    fs::create_dir_all(copied_server.parent().unwrap()).unwrap();
    fs::copy(SERVER_BIN, &copied_server).unwrap();
    let bad_bin = temp.path().join("missing/twinning");
    let mut doctor = TestServer::new(
        &temp,
        LaunchOptions {
            current_dir: Some(PathBuf::from("/")),
            temp_dir: Some(temp.path().join("doctor-tmp")),
            server_bin: Some(copied_server),
            twinning_bin: Some(bad_bin),
            empty_path: true,
            ..LaunchOptions::default()
        },
    );
    let doctor_result = successful_tool(&doctor.call_tool("twin_doctor", json!({})));
    assert!(doctor_result["twinning_bin"].is_null());
    assert!(doctor_result["twinning_version"].is_null());
    assert_eq!("unavailable", doctor_result["health"]);
    assert_eq!("/", doctor_result["workdir"]["path"]);
    assert_eq!(false, doctor_result["workdir"]["safe"]);
    assert!(doctor.finish("S21-doctor").status.success());
}

#[test]
fn rs01_start_list_stop_report_and_openapi_argv() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS01 local-ref spec body marker");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let mut arguments = rest_start_args(&spec);
    arguments["auth_mode"] = json!("bypass");
    arguments["server_variables"] = json!({"basePath":"v3"});
    let started = successful_tool(&server.call_tool("twin_start", arguments));
    let twin_id = started["twin_id"].as_str().unwrap();
    let pid = started["pid"].as_u64().unwrap() as u32;
    assert_eq!("rest", started["kind"]);
    assert!(
        started["base_url"]
            .as_str()
            .unwrap()
            .starts_with("http://127.0.0.1:")
    );
    assert!(started.get("connection_string").is_none());
    assert!(TcpStream::connect(("127.0.0.1", started["port"].as_u64().unwrap() as u16)).is_ok());
    let listed = list_twins(&mut server);
    assert_eq!(1, listed.len());
    assert_eq!("rest", listed[0]["kind"]);
    let state = server.state_dir(twin_id);
    let argv: Value = serde_json::from_slice(&fs::read(state.join("argv_summary.json")).unwrap())
        .expect("fake rest child records a redacted argv summary");
    assert_eq!("rest", argv["command"]);
    assert_eq!(true, argv["spec_present"]);
    assert_eq!("bypass", argv["auth_mode"]);
    assert_eq!(json!(["basePath"]), argv["server_variable_names"]);
    for option in [
        "run_enabled",
        "chaos_enabled",
        "canary_enabled",
        "strict_enabled",
        "routing_enabled",
        "base_prefix_enabled",
    ] {
        assert_eq!(false, argv[option], "REST MCP must not expose {option}");
    }

    let stopped = successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin_id})));
    assert_eq!("rest", stopped["kind"]);
    assert_eq!("stopped", stopped["status"]);
    assert_eq!("PASS", stopped["outcome"]);
    assert_eq!(7, stopped["request_count"]);
    assert_eq!(
        json!({"auth_refusal":1,"response_stub":2}),
        stopped["outcomes"]
    );
    assert_eq!(json!({"unauthorized":1}), stopped["refusals"]);
    assert_eq!(json!(["GET /items"]), stopped["endpoints_exercised"]);
    assert_eq!(json!(["POST /items"]), stopped["endpoints_not_exercised"]);
    assert!(stopped.get("trace_path").is_none());

    let report = successful_tool(&server.call_tool("twin_report", json!({"twin_id":twin_id})));
    assert_eq!("twinning.rest-report.v0", report["version"]);
    assert_eq!("PASS", report["outcome"]);
    assert_eq!(7, report["request_count"]);
    assert!(report.get("spec").is_none());
    assert!(report.get("session").is_none());
    let defaulted = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
    let defaulted_argv: Value = serde_json::from_slice(
        &fs::read(
            server
                .state_dir(defaulted["twin_id"].as_str().unwrap())
                .join("argv_summary.json"),
        )
        .unwrap(),
    )
    .expect("default-mode child argv summary is JSON");
    assert_eq!("shape", defaulted_argv["auth_mode"]);
    successful_tool(&server.call_tool("twin_stop", json!({"twin_id":defaulted["twin_id"]})));
    let finished = server.finish("RS01");
    assert!(finished.status.success());
    assert_process_gone(pid, "RS01 REST twin stop");
    assert_process_gone(
        defaulted["pid"].as_u64().unwrap() as u32,
        "RS01 REST default-mode twin stop",
    );
}

#[test]
fn rs02_kind_specific_fields_and_server_variables_are_validated() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS02 spec");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));

    for (arguments, field) in [
        (json!({"kind":"rest"}), "spec_path"),
        (
            json!({"kind":"rest","spec_path":"api.yaml","schema_path":"schema.sql"}),
            "schema_path",
        ),
        (
            json!({"kind":"rest","spec_path":"api.yaml","verify_path":"verify.json"}),
            "verify_path",
        ),
        (
            json!({"kind":"postgres","schema_path":"schema.sql","spec_path":"api.yaml"}),
            "spec_path",
        ),
        (
            json!({"kind":"postgres","schema_path":"schema.sql","auth_mode":"shape"}),
            "auth_mode",
        ),
        (json!({"kind":"postgres"}), "schema_path"),
    ] {
        let error = failed_tool(
            &server.call_tool("twin_start", arguments),
            "E_ARGUMENT_INVALID",
        );
        assert!(error["error"]["message"].as_str().unwrap().contains(field));
        assert!(error["error"]["hint"].as_str().unwrap().contains(field));
    }
    for arguments in [
        json!({"kind":"rest","spec_path":"api.yaml","auth_mode":"secret"}),
        json!({"kind":"rest","spec_path":"api.yaml","server_variables":[]}),
        json!({"kind":"rest","spec_path":"api.yaml","server_variables":{"basePath":7}}),
        json!({"kind":"rest","spec_path":"api.yaml","server_variables":{"bad=name":"v3"}}),
    ] {
        let error = failed_tool(
            &server.call_tool("twin_start", arguments),
            "E_ARGUMENT_INVALID",
        );
        assert!(
            error["error"]["message"]
                .as_str()
                .unwrap()
                .contains("auth_mode")
                || error["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("server_variables")
        );
    }
    assert!(!server.state_root().exists());
    assert!(server.finish("RS02").status.success());
    assert!(spec.is_file());
}

#[test]
fn rs03_remote_ref_is_refused_before_child_spawn_without_echoing_spec_text() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let secret = "RS03_PRIVATE_REMOTE_REF_91df";
    let spec = workdir.join("remote.yaml");
    fs::write(
        &spec,
        format!(
            "openapi: 3.0.3\ninfo:\n  title: remote ref test\n  version: '1.0'\npaths:\n  /items:\n    get:\n      responses:\n        '200':\n          description: item\n          content:\n            application/json:\n              schema:\n                $ref: 'https://example.invalid/{secret}#/Item'\n"
        ),
    )
    .unwrap();
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let error = failed_tool(
        &server.call_tool("twin_start", rest_start_args(&spec)),
        "E_ARGUMENT_INVALID",
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("spec_path")
    );
    assert!(error["error"]["hint"].as_str().unwrap().contains("local"));
    assert!(!serde_json::to_string(&error).unwrap().contains(secret));
    assert!(!server.raw_stdout.contains(secret));
    assert!(
        !server.state_root().exists(),
        "preflight refusal must not spawn a child"
    );
    assert!(server.finish("RS03").status.success());
}

#[test]
fn rs04_rest_report_versions_and_outcomes_are_checked() {
    for outcome in ["PASS", "FAIL", "VIOLATIONS", "REFUSAL"] {
        let temp = test_tempdir();
        let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
        let spec = write_rest_spec(&workdir, "api.yaml", "RS04 outcome");
        let mut options = default_launch(&workdir, "ok");
        options
            .env
            .push(("FAKE_TWINNING_REST_OUTCOME".into(), outcome.into()));
        let mut server = TestServer::new(&temp, options);
        let twin = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
        let stopped =
            successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})));
        assert_eq!(outcome, stopped["outcome"]);
        assert!(server.finish(&format!("RS04-{outcome}")).status.success());
        assert_process_gone(
            twin["pid"].as_u64().unwrap() as u32,
            "RS04 report child stop",
        );
    }

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS04 invalid version");
    let mut options = default_launch(&workdir, "ok");
    options
        .env
        .push(("FAKE_TWINNING_REST_BAD_VERSION".into(), "1".into()));
    let mut server = TestServer::new(&temp, options);
    let twin = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
    failed_tool(
        &server.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})),
        "E_REPORT_UNREADABLE",
    );
    assert!(server.finish("RS04-invalid-version").status.success());
    assert_process_gone(
        twin["pid"].as_u64().unwrap() as u32,
        "RS04 invalid report child stop",
    );

    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS04 invalid outcome");
    let mut options = default_launch(&workdir, "ok");
    options
        .env
        .push(("FAKE_TWINNING_REST_OUTCOME".into(), "UNKNOWN".into()));
    let mut server = TestServer::new(&temp, options);
    let twin = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
    failed_tool(
        &server.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})),
        "E_REPORT_UNREADABLE",
    );
    assert!(server.finish("RS04-invalid-outcome").status.success());
    assert_process_gone(
        twin["pid"].as_u64().unwrap() as u32,
        "RS04 invalid outcome child stop",
    );
}

#[test]
fn rs05_endpoint_summaries_are_capped_with_truncation_marker() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS05 large report");
    let mut options = default_launch(&workdir, "ok");
    options
        .env
        .push(("FAKE_TWINNING_REST_LARGE_REPORT".into(), "1".into()));
    let mut server = TestServer::new(&temp, options);
    let twin = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
    let stopped =
        successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})));
    assert_eq!(50, stopped["endpoints_exercised"].as_array().unwrap().len());
    assert_eq!(
        50,
        stopped["endpoints_not_exercised"].as_array().unwrap().len()
    );
    assert_eq!(true, stopped["truncated"]);
    assert_eq!(
        json!(["endpoints_exercised", "endpoints_not_exercised"]),
        stopped["truncated_fields"]
    );
    let report =
        successful_tool(&server.call_tool("twin_report", json!({"twin_id":twin["twin_id"]})));
    assert_eq!(50, report["endpoints_exercised"].as_array().unwrap().len());
    assert_eq!(true, report["truncated"]);
    assert!(server.finish("RS05").status.success());
    assert_process_gone(
        twin["pid"].as_u64().unwrap() as u32,
        "RS05 report child stop",
    );
}

#[test]
fn rs06_postgres_and_rest_share_the_live_twin_cap() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS06 shared cap");
    let mut server = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            fake_mode: Some("ok"),
            max_twins: Some(2),
            ..LaunchOptions::default()
        },
    );
    let postgres = successful_tool(&server.call_tool("twin_start", start_args(&workdir)));
    let rest = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
    assert_eq!("postgres", postgres["kind"]);
    assert_eq!("rest", rest["kind"]);
    failed_tool(
        &server.call_tool("twin_start", rest_start_args(&spec)),
        "E_TWIN_CAP_REACHED",
    );
    let listed = list_twins(&mut server);
    assert_eq!(2, listed.len());
    assert_eq!("postgres", listed[0]["kind"]);
    assert_eq!("rest", listed[1]["kind"]);
    let finished = server.finish("RS06");
    assert!(finished.status.success());
    assert_process_gone(
        postgres["pid"].as_u64().unwrap() as u32,
        "RS06 postgres shutdown",
    );
    assert_process_gone(rest["pid"].as_u64().unwrap() as u32, "RS06 rest shutdown");
}

#[test]
fn rs07_rest_ttl_expiry_stops_child_and_retains_report() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS07 TTL");
    let mut options = default_launch(&workdir, "ok");
    options
        .env
        .push(("TWINNING_MCP_TTL_SECONDS_TEST".into(), "1".into()));
    let mut server = TestServer::new(&temp, options);
    let twin = successful_tool(&server.call_tool("twin_start", rest_start_args(&spec)));
    let twin_id = twin["twin_id"].as_str().unwrap().to_owned();
    let expired = poll_until(
        scaled_deadline(Duration::from_secs(5)),
        Duration::from_millis(20),
        "RS07 REST TTL status expired",
        || {
            list_twins(&mut server)
                .into_iter()
                .find(|entry| entry["twin_id"] == twin_id && entry["status"] == "expired")
        },
    );
    assert_eq!("expired", expired["status"]);
    let stopped = successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin_id})));
    assert_eq!("expired", stopped["status"]);
    assert_eq!("PASS", stopped["outcome"]);
    let report = successful_tool(&server.call_tool("twin_report", json!({"twin_id":twin_id})));
    assert_eq!("twinning.rest-report.v0", report["version"]);
    assert!(server.finish("RS07").status.success());
    assert_process_gone(
        twin["pid"].as_u64().unwrap() as u32,
        "RS07 REST TTL child stop",
    );
}

#[test]
fn rs08_stale_sweep_removes_orphan_rest_child_and_preserves_live_child() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec = write_rest_spec(&workdir, "api.yaml", "RS08 stale sweep");
    let shared_tmp = temp.path().join("shared-tmp");
    fs::create_dir_all(&shared_tmp).unwrap();

    let mut crashed = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(shared_tmp.clone()),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    let orphan = successful_tool(&crashed.call_tool("twin_start", rest_start_args(&spec)));
    let orphan_pid = orphan["pid"].as_u64().unwrap() as u32;
    let crashed_root = crashed.state_root();

    let mut live = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(shared_tmp.clone()),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    let live_twin = successful_tool(&live.call_tool("twin_start", start_args(&workdir)));
    let live_pid = live_twin["pid"].as_u64().unwrap() as u32;
    let live_root = live.state_root();

    crashed.signal(libc::SIGKILL);
    let killed = crashed.finish("RS08-crashed-server");
    assert!(!killed.status.success());
    assert!(
        process_alive(orphan_pid),
        "RS08 crashed server should leave its child for sweep"
    );

    let mut sweeper = TestServer::new(
        &temp,
        LaunchOptions {
            workdir: Some(workdir.clone()),
            temp_dir: Some(shared_tmp),
            fake_mode: Some("ok"),
            ..LaunchOptions::default()
        },
    );
    let sweep_twin = successful_tool(&sweeper.call_tool("twin_start", rest_start_args(&spec)));
    assert_process_gone(orphan_pid, "RS08 stale orphan REST child");
    assert!(
        !crashed_root.exists(),
        "RS08 dead server directory is removed"
    );
    assert!(
        live_root.exists(),
        "RS08 active server directory is preserved"
    );
    assert!(
        process_alive(live_pid),
        "RS08 active Postgres child remains alive"
    );

    successful_tool(&live.call_tool("twin_stop", json!({"twin_id":live_twin["twin_id"]})));
    successful_tool(&sweeper.call_tool("twin_stop", json!({"twin_id":sweep_twin["twin_id"]})));
    assert!(live.finish("RS08-live-server").status.success());
    assert!(sweeper.finish("RS08-sweeper-server").status.success());
    assert_process_gone(live_pid, "RS08 live Postgres stop");
    assert_process_gone(
        sweep_twin["pid"].as_u64().unwrap() as u32,
        "RS08 sweeper REST stop",
    );
}

#[test]
fn rs09_openapi_spec_and_server_variable_secrets_never_enter_logs_or_report() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let spec_secret = "RS09_PRIVATE_SPEC_CONTENT_104a";
    let variable_secret = "RS09_PRIVATE_SERVER_VARIABLE_87cd";
    let spec = write_rest_spec(&workdir, "api.yaml", spec_secret);
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let mut arguments = rest_start_args(&spec);
    arguments["server_variables"] = json!({"basePath":variable_secret});
    let twin = successful_tool(&server.call_tool("twin_start", arguments));
    let stopped =
        successful_tool(&server.call_tool("twin_stop", json!({"twin_id":twin["twin_id"]})));
    let report =
        successful_tool(&server.call_tool("twin_report", json!({"twin_id":twin["twin_id"]})));
    let finished = server.finish("RS09");
    assert!(finished.status.success());
    for secret in [spec_secret, variable_secret] {
        assert!(
            !finished.stdout.contains(secret),
            "secret appeared in stdout: {secret}"
        );
        assert!(
            !finished.stderr.contains(secret),
            "secret appeared in stderr: {secret}"
        );
        assert!(!serde_json::to_string(&stopped).unwrap().contains(secret));
        assert!(!serde_json::to_string(&report).unwrap().contains(secret));
    }
    assert_process_gone(twin["pid"].as_u64().unwrap() as u32, "RS09 REST stop");
}

#[test]
fn rs10_doctor_lists_supported_kinds_and_seed_contract_refuses_rest() {
    let temp = test_tempdir();
    let workdir = create_workdir(temp.path(), "CREATE TABLE t (id INT);");
    let mut server = TestServer::new(&temp, default_launch(&workdir, "ok"));
    let doctor = successful_tool(&server.call_tool("twin_doctor", json!({})));
    assert_eq!(json!(["postgres", "rest"]), doctor["supported_kinds"]);
    let error = failed_tool(
        &server.call_tool("twin_seed_contract", json!({"kind":"rest"})),
        "E_KIND_UNSUPPORTED",
    );
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("twin_seed_contract")
    );
    assert!(
        error["error"]["hint"]
            .as_str()
            .unwrap()
            .contains("postgres")
    );
    assert!(server.finish("RS10").status.success());
}
