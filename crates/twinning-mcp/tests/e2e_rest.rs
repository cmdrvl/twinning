#![cfg(unix)]

pub mod common;

use std::{
    env, fs,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        Mutex,
        mpsc::{self, Receiver},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use common::{ProcessGroupDropGuard, scaled_deadline};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const MCP_BIN: &str = env!("CARGO_BIN_EXE_twinning-mcp");
const REST_SPEC: &str =
    include_str!("../../../tests/fixtures/rest/openfigi_v2_v3/response-stub-schema.yaml");
const API_KEY: &str = "r2-test-key";
const MCP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const CHILD_TIMEOUT: Duration = Duration::from_secs(20);

static TEST_SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn rr01_matching_post_returns_stubbed_figi_body() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR01") else {
        return;
    };
    let workdir = context.prepare_case("RR01");
    let mut session = context.session("RR01", &workdir);
    let twin = session.start_rest("RR01", &workdir);
    let response = context.post_mapping("RR01", &twin.base_url, "match", mapping_body(), true);

    assert_eq!(
        200, response.status,
        "RR01 HTTP response: {}",
        response.body
    );
    assert_eq!("BBG000B9XRY4", response.json[0]["data"][0]["figi"]);
    assert_eq!("AAPL", response.json[0]["data"][0]["ticker"]);
    let stopped = session.stop_twin("RR01", &twin.twin_id);
    assert_eq!(1, stopped["request_count"]);
    let report = session.report("RR01", &twin.twin_id);
    assert_eq!(1, report["request_count"]);
    session.finish_eof();
}

#[test]
fn rr02_missing_api_key_returns_401() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR02") else {
        return;
    };
    let workdir = context.prepare_case("RR02");
    let mut session = context.session("RR02", &workdir);
    let twin = session.start_rest("RR02", &workdir);
    let response =
        context.post_mapping("RR02", &twin.base_url, "missing-key", mapping_body(), false);

    assert_eq!(
        401, response.status,
        "RR02 HTTP response: {}",
        response.body
    );
    assert_eq!("unauthorized", response.json["code"]);
    let stopped = session.stop_twin("RR02", &twin.twin_id);
    assert_eq!(1, stopped["request_count"]);
    let report = session.report("RR02", &twin.twin_id);
    assert_eq!(1, report["outcomes"]["auth_refusal"]);
    session.finish_eof();
}

#[test]
fn rr03_object_body_returns_422_with_schema_path() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR03") else {
        return;
    };
    let workdir = context.prepare_case("RR03");
    let mut session = context.session("RR03", &workdir);
    let twin = session.start_rest("RR03", &workdir);
    let response = context.post_mapping(
        "RR03",
        &twin.base_url,
        "object-body",
        json!({"idType":"ID_CUSIP","idValue":"037833100"}),
        true,
    );

    assert_eq!(
        422, response.status,
        "RR03 HTTP response: {}",
        response.body
    );
    assert_eq!("schema_validation_failed", response.json["code"]);
    assert_eq!("$", response.json["path"]);
    let report = session.stop_twin("RR03", &twin.twin_id);
    assert_eq!(1, report["request_count"]);
    session.finish_eof();
}

#[test]
fn rr04_unknown_route_returns_404() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR04") else {
        return;
    };
    let workdir = context.prepare_case("RR04");
    let mut session = context.session("RR04", &workdir);
    let twin = session.start_rest("RR04", &workdir);
    let response = context.get("RR04", &twin.base_url, "unknown-route", "/v3/no-such-route");

    assert_eq!(
        404, response.status,
        "RR04 HTTP response: {}",
        response.body
    );
    assert_eq!("not_found", response.json["code"]);
    let stopped = session.stop_twin("RR04", &twin.twin_id);
    assert_eq!(1, stopped["request_count"]);
    session.finish_eof();
}

#[test]
fn rr05_report_counts_match_four_http_calls() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR05") else {
        return;
    };
    let workdir = context.prepare_case("RR05");
    let mut session = context.session("RR05", &workdir);
    let twin = session.start_rest("RR05", &workdir);

    let matching = context.post_mapping("RR05", &twin.base_url, "match", mapping_body(), true);
    let missing_key =
        context.post_mapping("RR05", &twin.base_url, "missing-key", mapping_body(), false);
    let invalid_body = context.post_mapping(
        "RR05",
        &twin.base_url,
        "object-body",
        json!({"idType":"ID_CUSIP","idValue":"037833100"}),
        true,
    );
    let unmatched = context.get("RR05", &twin.base_url, "unknown-route", "/v3/no-such-route");
    assert_eq!(200, matching.status);
    assert_eq!(401, missing_key.status);
    assert_eq!(422, invalid_body.status);
    assert_eq!(404, unmatched.status);

    let stopped = session.stop_twin("RR05", &twin.twin_id);
    assert_eq!(4, stopped["request_count"]);
    let report = session.report("RR05", &twin.twin_id);
    assert_eq!("twinning.rest-report.v0", report["version"]);
    assert_eq!("PASS", report["outcome"]);
    assert_eq!(4, report["request_count"]);
    assert_eq!(
        json!({
            "auth_refusal": 1,
            "schema_validation_refusal": 1,
            "response_stub": 1,
            "unmatched": 1
        }),
        report["outcomes"]
    );
    assert_eq!(json!([]), report["endpoints_not_exercised"]);
    event(
        &context.artifacts,
        "RR05",
        "report_assertions",
        &[
            ("request_count", report["request_count"].to_string()),
            ("outcome", report["outcome"].to_string()),
            ("outcomes", report["outcomes"].to_string()),
            (
                "endpoints_not_exercised",
                report["endpoints_not_exercised"].to_string(),
            ),
        ],
    );
    session.finish_eof();
}

#[test]
fn rr06_real_listener_is_loopback_only() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR06") else {
        return;
    };
    let Some((non_loopback, interface)) = non_loopback_ipv4().expect("inspect host addresses")
    else {
        common::skipped(
            "RR06",
            "host has no active non-loopback IPv4 address for the bind probe",
            &context.artifacts,
        );
        return;
    };
    let workdir = context.prepare_case("RR06");
    let mut session = context.session("RR06", &workdir);
    let twin = session.start_rest("RR06", &workdir);
    let probe = context.get_non_loopback("RR06", &non_loopback, twin.port);

    assert!(
        !probe.status.success(),
        "RR06 listener accepted a non-loopback request: {}",
        probe.stderr
    );
    assert!(
        probe
            .stderr
            .to_ascii_lowercase()
            .contains("failed to connect")
            || probe
                .stderr
                .to_ascii_lowercase()
                .contains("could not connect")
            || probe
                .stderr
                .to_ascii_lowercase()
                .contains("connection refused")
            || probe.stderr.to_ascii_lowercase().contains("timed out"),
        "RR06 expected a non-loopback connection failure: {}",
        tail(&probe.stderr, 2048)
    );
    event(
        &context.artifacts,
        "RR06",
        "non_loopback_probe",
        &[
            ("interface", interface),
            ("address", non_loopback.to_string()),
            ("port", twin.port.to_string()),
            ("exit_code", probe.status.code().unwrap_or(-1).to_string()),
            ("stderr_tail", tail(&probe.stderr, 2048)),
        ],
    );
    let stopped = session.stop_twin("RR06", &twin.twin_id);
    assert_eq!(0, stopped["request_count"]);
    session.finish_eof();
}

#[test]
fn rr07_remote_ref_is_refused_without_child_or_fetch() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR07") else {
        return;
    };
    let workdir = context.prepare_case("RR07");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local ref sentinel");
    listener
        .set_nonblocking(true)
        .expect("set ref sentinel nonblocking");
    let remote_url = format!(
        "http://127.0.0.1:{}/remote.yaml",
        listener.local_addr().expect("sentinel address").port()
    );
    let remote_spec = workdir.join("remote-ref.yaml");
    let content = format!(
        "openapi: 3.0.3\ninfo:\n  title: remote ref probe\n  version: '1'\npaths: {{}}\ncomponents:\n  schemas:\n    Remote:\n      $ref: '{remote_url}'\n"
    );
    fs::write(&remote_spec, content.as_bytes()).expect("write remote ref spec");
    event(
        &context.artifacts,
        "RR07",
        "remote_ref_spec_written",
        &[
            ("bytes", content.len().to_string()),
            ("sha256", sha256(content.as_bytes())),
        ],
    );

    let mut session = context.session("RR07", &workdir);
    let response = session.call_tool(
        "RR07",
        "twin_start",
        json!({"kind":"rest","spec_path":remote_spec}),
    );
    let error = failed_tool(&response, "E_ARGUMENT_INVALID");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("spec_path")
    );
    assert!(
        error["error"]["hint"]
            .as_str()
            .is_some_and(|hint| !hint.is_empty())
    );
    assert!(session.twins.is_empty(), "RR07 no child process may start");
    assert!(
        !session.state_root().exists()
            || fs::read_dir(session.state_root())
                .expect("read empty state root")
                .next()
                .is_none(),
        "RR07 a child state directory was created"
    );
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock),
        "RR07 the listener received a remote reference fetch"
    );
    event(
        &context.artifacts,
        "RR07",
        "remote_ref_preflight_refused",
        &[
            ("error_code", "E_ARGUMENT_INVALID".to_owned()),
            ("child_pid", "none".to_owned()),
            ("sentinel_connection", "none".to_owned()),
        ],
    );
    session.finish_eof();
}

#[test]
fn rr08_sigterm_and_stdin_eof_stop_rest_children_without_strays() {
    let _serial = test_serial();
    let Some(context) = TestContext::new("RR08") else {
        return;
    };
    let before = relevant_process_snapshot(&context);
    event(
        &context.artifacts,
        "RR08",
        "process_snapshot_before",
        &[("processes", json!(before).to_string())],
    );

    let sigterm_workdir = context.prepare_case("RR08-sigterm");
    let mut sigterm = context.session("RR08-sigterm", &sigterm_workdir);
    let sigterm_twin = sigterm.start_rest("RR08-sigterm", &sigterm_workdir);
    let sigterm_report = sigterm.twin_path(&sigterm_twin.twin_id).join("report.json");
    sigterm.finish_sigterm();
    assert_process_gone(
        &context.artifacts,
        "RR08-sigterm",
        sigterm_twin.pid,
        "REST child after SIGTERM",
    );
    let sigterm_report_value = read_rest_report(&sigterm_report, "RR08-sigterm");
    assert_eq!("PASS", sigterm_report_value["outcome"]);

    let eof_workdir = context.prepare_case("RR08-eof");
    let mut eof = context.session("RR08-eof", &eof_workdir);
    let eof_twin = eof.start_rest("RR08-eof", &eof_workdir);
    let eof_report = eof.twin_path(&eof_twin.twin_id).join("report.json");
    eof.finish_eof();
    assert_process_gone(
        &context.artifacts,
        "RR08-eof",
        eof_twin.pid,
        "REST child after stdin EOF",
    );
    let eof_report_value = read_rest_report(&eof_report, "RR08-eof");
    assert_eq!("PASS", eof_report_value["outcome"]);

    let after = relevant_process_snapshot(&context);
    event(
        &context.artifacts,
        "RR08",
        "process_snapshot_after",
        &[("processes", json!(after).to_string())],
    );
    assert_eq!(before, after, "RR08 left a twinning or MCP process running");
}

fn test_serial() -> std::sync::MutexGuard<'static, ()> {
    TEST_SERIAL
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

struct TestContext {
    artifacts: PathBuf,
    twinning: PathBuf,
    mcp: PathBuf,
    curl: PathBuf,
}

impl TestContext {
    fn new(test_id: &str) -> Option<Self> {
        let artifacts = common::artifacts_dir("twinning_mcp_e2e_rest").clone();
        let Some(curl) = find_executable("CURL_BIN", "curl") else {
            common::skipped(
                test_id,
                "curl executable was not found in PATH or CURL_BIN",
                &artifacts,
            );
            return None;
        };
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root resolves");
        let twinning = find_twinning_binary(&repo);
        let mcp = PathBuf::from(MCP_BIN)
            .canonicalize()
            .expect("Cargo-built twinning-mcp binary resolves");
        assert!(twinning.is_file(), "real twinning binary is missing");
        assert!(mcp.is_file(), "real twinning-mcp binary is missing");
        let context = Self {
            artifacts,
            twinning,
            mcp,
            curl,
        };
        context.record_curl_version(test_id);
        event(
            &context.artifacts,
            test_id,
            "real_binaries",
            &[
                ("twinning", context.twinning.display().to_string()),
                ("twinning_mcp", context.mcp.display().to_string()),
            ],
        );
        Some(context)
    }

    fn prepare_case(&self, test_id: &str) -> PathBuf {
        let workdir = self.artifacts.join(test_id).join("workdir");
        fs::create_dir_all(&workdir).expect("create REST workdir");
        let spec_path = workdir.join("response-stub-schema.yaml");
        fs::write(&spec_path, REST_SPEC.as_bytes()).expect("copy OpenFIGI fixture into workdir");
        event(
            &self.artifacts,
            test_id,
            "rest_spec_copied",
            &[
                ("path", spec_path.display().to_string()),
                ("bytes", REST_SPEC.len().to_string()),
                ("sha256", sha256(REST_SPEC.as_bytes())),
            ],
        );
        workdir
    }

    fn session(&self, test_id: &str, workdir: &Path) -> McpSession {
        let mut session = McpSession::spawn(self, test_id, workdir);
        session.initialize(test_id);
        session
    }

    fn record_curl_version(&self, test_id: &str) {
        let mut command = Command::new(&self.curl);
        command.arg("--version");
        let output = run_command(
            command,
            &self.artifacts,
            test_id,
            "curl_version",
            None,
            Duration::from_secs(10),
        );
        assert_success(&output, "curl --version");
        let version = output
            .stdout
            .lines()
            .next()
            .unwrap_or("<missing>")
            .to_owned();
        event(
            &self.artifacts,
            test_id,
            "curl_version",
            &[
                ("path", self.curl.display().to_string()),
                ("value", version),
            ],
        );
    }

    fn post_mapping(
        &self,
        test_id: &str,
        base_url: &str,
        name: &str,
        body: Value,
        include_key: bool,
    ) -> HttpResult {
        let request_path = self.write_request_body(test_id, name, &body);
        let url = format!("{base_url}/v3/mapping");
        let mut command = Command::new(&self.curl);
        command.args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--connect-timeout",
            "2",
            "--max-time",
            "10",
            "--request",
            "POST",
            "--header",
            "Content-Type: application/json",
        ]);
        if include_key {
            command
                .arg("--header")
                .arg(format!("X-OPENFIGI-APIKEY: {API_KEY}"));
        }
        command
            .arg("--data-binary")
            .arg(format!("@{}", request_path.display()))
            .arg("--output")
            .arg(
                self.artifacts
                    .join(test_id)
                    .join(format!("{name}.response.json")),
            )
            .arg("--write-out")
            .arg("\\n%{http_code}")
            .arg(&url);
        let display = format!(
            "curl --request POST --header Content-Type:application/json{} --data-binary @{} {}",
            if include_key {
                " --header X-OPENFIGI-APIKEY:[redacted]"
            } else {
                ""
            },
            request_path.display(),
            url
        );
        self.run_http_command(test_id, name, command, display)
    }

    fn get(&self, test_id: &str, base_url: &str, name: &str, path: &str) -> HttpResult {
        let url = format!("{base_url}{path}");
        let mut command = Command::new(&self.curl);
        command.args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--connect-timeout",
            "2",
            "--max-time",
            "10",
            "--request",
            "GET",
            "--output",
        ]);
        command
            .arg(
                self.artifacts
                    .join(test_id)
                    .join(format!("{name}.response.json")),
            )
            .arg("--write-out")
            .arg("\\n%{http_code}")
            .arg(&url);
        let display = format!("curl --request GET {url}");
        self.run_http_command(test_id, name, command, display)
    }

    fn get_non_loopback(&self, test_id: &str, address: &Ipv4Addr, port: u16) -> CapturedOutput {
        let url = format!("http://{address}:{port}/v3/mapping");
        let mut command = Command::new(&self.curl);
        command.args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--connect-timeout",
            "1",
            "--max-time",
            "2",
            "--output",
            "/dev/null",
            &url,
        ]);
        let display = format!("curl --connect-timeout 1 --max-time 2 {url}");
        run_command(
            command,
            &self.artifacts,
            test_id,
            "non_loopback_curl",
            Some(display),
            Duration::from_secs(5),
        )
    }

    fn write_request_body(&self, test_id: &str, name: &str, body: &Value) -> PathBuf {
        let bytes = serde_json::to_vec(body).expect("serialize request body");
        let path = self
            .artifacts
            .join(test_id)
            .join(format!("{name}.request.json"));
        fs::write(&path, &bytes).expect("write curl request body");
        event(
            &self.artifacts,
            test_id,
            "http_request_body",
            &[
                ("name", name.to_owned()),
                ("bytes", bytes.len().to_string()),
                ("sha256", sha256(&bytes)),
            ],
        );
        path
    }

    fn run_http_command(
        &self,
        test_id: &str,
        name: &str,
        command: Command,
        display: String,
    ) -> HttpResult {
        let output = run_command(
            command,
            &self.artifacts,
            test_id,
            "curl_http_request",
            Some(display.clone()),
            CHILD_TIMEOUT,
        );
        assert_success(&output, &display);
        let status = output
            .stdout
            .lines()
            .last()
            .unwrap_or_default()
            .parse::<u16>()
            .unwrap_or_else(|_| panic!("curl did not print an HTTP status: {}", output.stdout));
        let response_path = self
            .artifacts
            .join(test_id)
            .join(format!("{name}.response.json"));
        let bytes = fs::read(&response_path).expect("curl response body file exists");
        let body = String::from_utf8_lossy(&bytes).into_owned();
        let json = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("HTTP response is not JSON ({error}): {body}"));
        event(
            &self.artifacts,
            test_id,
            "http_response",
            &[
                ("name", name.to_owned()),
                ("command", display),
                ("status", status.to_string()),
                ("response_bytes", bytes.len().to_string()),
                ("response_sha256", sha256(&bytes)),
                ("stderr_tail", tail(&output.stderr, 2048)),
            ],
        );
        HttpResult { status, body, json }
    }
}

struct HttpResult {
    status: u16,
    body: String,
    json: Value,
}

#[derive(Debug)]
struct RestTwin {
    twin_id: String,
    pid: u32,
    port: u16,
    base_url: String,
}

struct McpSession {
    test_id: String,
    artifacts: PathBuf,
    session_root: PathBuf,
    temp_root: PathBuf,
    server_pid: u32,
    stdin: Option<std::process::ChildStdin>,
    responses: Receiver<String>,
    stdout_reader: Option<JoinHandle<String>>,
    stderr_reader: Option<JoinHandle<String>>,
    guard: Option<ProcessGroupDropGuard>,
    next_id: u64,
    twins: Vec<RestTwin>,
    stopped: Vec<String>,
    finished: bool,
}

impl McpSession {
    fn spawn(context: &TestContext, test_id: &str, workdir: &Path) -> Self {
        let session_root = context.artifacts.join(test_id);
        let temp_root = session_root.join("tmp");
        fs::create_dir_all(&temp_root).expect("create per-test TMPDIR");
        let mut command = Command::new(&context.mcp);
        command
            .args(["--workdir", workdir.to_str().expect("workdir UTF-8")])
            .args([
                "--twinning-bin",
                context.twinning.to_str().expect("binary path UTF-8"),
            ])
            .current_dir(workdir)
            .env("TMPDIR", &temp_root)
            .env_remove("TWINNING_BIN")
            .env_remove("TWINNING_MCP_WORKDIR")
            .env_remove("TWINNING_MCP_MAX_TWINS")
            .env_remove("TWINNING_MCP_RESTRICTED")
            .env_remove("FAKE_TWINNING_MODE")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let display = command_line(&command);
        command.process_group(0);
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("start real twinning-mcp ({display}): {error}"));
        let server_pid = child.id();
        event(
            &context.artifacts,
            test_id,
            "mcp_server_started",
            &[
                ("command", display),
                ("pid", server_pid.to_string()),
                (
                    "fake_mode",
                    "none; explicit real twinning binary".to_owned(),
                ),
                ("workdir", workdir.display().to_string()),
            ],
        );
        let stdout = child.stdout.take().expect("MCP stdout is piped");
        let stderr = child.stderr.take().expect("MCP stderr is piped");
        let (sender, responses) = mpsc::channel();
        let stdout_reader = thread::spawn(move || read_json_lines(stdout, sender));
        let stderr_reader = thread::spawn(move || read_all(stderr));
        let stdin = child.stdin.take();
        Self {
            test_id: test_id.to_owned(),
            artifacts: context.artifacts.clone(),
            session_root,
            temp_root,
            server_pid,
            stdin,
            responses,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            guard: Some(ProcessGroupDropGuard::new(child)),
            next_id: 1,
            twins: Vec::new(),
            stopped: Vec::new(),
            finished: false,
        }
    }

    fn initialize(&mut self, test_id: &str) {
        let response = self.call(
            test_id,
            "initialize",
            Some(json!({
                "protocolVersion":"2025-03-26",
                "capabilities":{},
                "clientInfo":{"name":"R2-REST-real-e2e","version":"1"}
            })),
        );
        assert_eq!("2025-03-26", response["result"]["protocolVersion"]);
        let notification = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        self.write_request(test_id, "notifications/initialized", &notification);
        let tools = self.call(test_id, "tools/list", None);
        let names = tools["result"]["tools"]
            .as_array()
            .expect("tools/list returns an array")
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();
        for required in ["twin_start", "twin_stop", "twin_report"] {
            assert!(
                names.contains(&required),
                "missing MCP tool {required}: {names:?}"
            );
        }
        event(
            &self.artifacts,
            test_id,
            "tools_list_verified",
            &[("tools", names.join(","))],
        );
    }

    fn call(&mut self, test_id: &str, method: &str, params: Option<Value>) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut request = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            request["params"] = params;
        }
        self.write_request(test_id, method, &request);
        let line = poll_until(
            &self.artifacts,
            test_id,
            scaled_deadline(MCP_RESPONSE_TIMEOUT),
            "MCP JSON-RPC response",
            || self.responses.try_recv().ok(),
        );
        let response: Value = serde_json::from_str(&line).expect("MCP stdout line is JSON");
        let error = response["error"]["code"]
            .as_i64()
            .map(|code| code.to_string())
            .or_else(|| tool_error_code(&response));
        let keys = response["result"]
            .as_object()
            .map(|object| object.keys().cloned().collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        event(
            &self.artifacts,
            test_id,
            "jsonrpc_response",
            &[
                ("id", response["id"].to_string()),
                ("error_code", error.unwrap_or_else(|| "-".to_owned())),
                ("result_keys", keys),
            ],
        );
        response
    }

    fn write_request(&mut self, test_id: &str, method: &str, request: &Value) {
        let rendered = serde_json::to_string(request).expect("serialize MCP request");
        event(
            &self.artifacts,
            test_id,
            "jsonrpc_request",
            &[
                ("id", request.get("id").unwrap_or(&Value::Null).to_string()),
                ("method", method.to_owned()),
            ],
        );
        let stdin = self.stdin.as_mut().expect("MCP stdin remains open");
        writeln!(stdin, "{rendered}").expect("write MCP request");
        stdin.flush().expect("flush MCP request");
    }

    fn call_tool(&mut self, test_id: &str, name: &str, arguments: Value) -> Value {
        self.call(
            test_id,
            "tools/call",
            Some(json!({"name":name,"arguments":arguments})),
        )
    }

    fn tool(&mut self, test_id: &str, name: &str, arguments: Value) -> Value {
        let response = self.call_tool(test_id, name, arguments);
        assert!(
            response.get("error").is_none(),
            "MCP protocol error: {response}"
        );
        assert_eq!(Some(false), response["result"]["isError"].as_bool());
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("successful MCP tool response is text JSON");
        serde_json::from_str(text).expect("MCP tool body is JSON")
    }

    fn start_rest(&mut self, test_id: &str, workdir: &Path) -> RestTwin {
        let spec_path = workdir.join("response-stub-schema.yaml");
        let started = self.tool(
            test_id,
            "twin_start",
            json!({
                "kind":"rest",
                "spec_path":spec_path,
                "server_variables":{"basePath":"v3"}
            }),
        );
        let twin_id = started["twin_id"].as_str().expect("twin id").to_owned();
        let port = started["port"].as_u64().expect("listener port") as u16;
        let pid = started["pid"].as_u64().expect("REST child pid") as u32;
        let base_url = started["base_url"]
            .as_str()
            .expect("REST base_url")
            .to_owned();
        assert_eq!(format!("http://127.0.0.1:{port}"), base_url);
        assert!(started.get("connection_string").is_none());

        let child_pid_path = self.twin_path(&twin_id).join("child.pid");
        poll_until(
            &self.artifacts,
            test_id,
            scaled_deadline(Duration::from_secs(5)),
            "REST child.pid artifact",
            || fs::read_to_string(&child_pid_path).ok(),
        );
        let file_pid = fs::read_to_string(&child_pid_path)
            .expect("REST child.pid is readable")
            .trim()
            .parse::<u32>()
            .expect("REST child.pid is numeric");
        assert_eq!(pid, file_pid);
        self.twins.push(RestTwin {
            twin_id: twin_id.clone(),
            pid,
            port,
            base_url: base_url.clone(),
        });
        self.assert_listener_ready(test_id, port);
        let argv = self.read_child_argv(test_id, pid);
        assert!(argv.contains("--host 127.0.0.1"), "REST argv: {argv}");
        assert!(
            argv.contains(&format!("--port {port}")),
            "REST argv: {argv}"
        );
        assert!(
            argv.contains("--server-variable basePath=v3"),
            "REST argv: {argv}"
        );
        event(
            &self.artifacts,
            test_id,
            "real_twinning_command",
            &[
                ("command", argv),
                ("pid", pid.to_string()),
                ("port", port.to_string()),
                ("base_url", base_url.clone()),
                ("workdir", workdir.display().to_string()),
            ],
        );
        RestTwin {
            twin_id,
            pid,
            port,
            base_url,
        }
    }

    fn assert_listener_ready(&self, test_id: &str, port: u16) {
        let started = Instant::now();
        poll_until(
            &self.artifacts,
            test_id,
            scaled_deadline(Duration::from_secs(10)),
            "real REST HTTP listener",
            || TcpStream::connect(("127.0.0.1", port)).ok(),
        );
        event(
            &self.artifacts,
            test_id,
            "http_listener_ready",
            &[
                ("host", "127.0.0.1".to_owned()),
                ("port", port.to_string()),
                ("elapsed_ms", started.elapsed().as_millis().to_string()),
            ],
        );
    }

    fn read_child_argv(&self, test_id: &str, pid: u32) -> String {
        let mut command = Command::new("ps");
        command.args(["-ww", "-p", &pid.to_string(), "-o", "command="]);
        let output = run_command(
            command,
            &self.artifacts,
            test_id,
            "real_twinning_process_argv",
            None,
            Duration::from_secs(10),
        );
        assert_success(&output, "ps real twinning child argv");
        output.stdout.trim().to_owned()
    }

    fn stop_twin(&mut self, test_id: &str, twin_id: &str) -> Value {
        let summary = self.tool(test_id, "twin_stop", json!({"twin_id":twin_id}));
        event(
            &self.artifacts,
            test_id,
            "rest_stop_summary",
            &[
                ("twin_id", twin_id.to_owned()),
                ("outcome", summary["outcome"].to_string()),
                ("exit_code", summary["exit_code"].to_string()),
                ("request_count", summary["request_count"].to_string()),
                ("outcomes", summary["outcomes"].to_string()),
                ("refusals", summary["refusals"].to_string()),
            ],
        );
        assert_eq!("stopped", summary["status"]);
        assert_eq!(0, summary["exit_code"]);
        self.stopped.push(twin_id.to_owned());
        summary
    }

    fn report(&mut self, test_id: &str, twin_id: &str) -> Value {
        let report = self.tool(test_id, "twin_report", json!({"twin_id":twin_id}));
        assert_eq!("twinning.rest-report.v0", report["version"]);
        event(
            &self.artifacts,
            test_id,
            "rest_report_summary",
            &[
                ("twin_id", twin_id.to_owned()),
                ("outcome", report["outcome"].to_string()),
                ("request_count", report["request_count"].to_string()),
                ("outcomes", report["outcomes"].to_string()),
                ("refusals", report["refusals"].to_string()),
                (
                    "endpoints_exercised",
                    report["endpoints_exercised"].to_string(),
                ),
                (
                    "endpoints_not_exercised",
                    report["endpoints_not_exercised"].to_string(),
                ),
            ],
        );
        report
    }

    fn twin_path(&self, twin_id: &str) -> PathBuf {
        self.state_root().join(twin_id)
    }

    fn state_root(&self) -> PathBuf {
        self.temp_root
            .join("twinning-mcp")
            .join(self.server_pid.to_string())
    }

    fn finish_eof(mut self) {
        event(
            &self.artifacts,
            &self.test_id,
            "shutdown_requested",
            &[("method", "stdin_eof".to_owned())],
        );
        self.stdin.take();
        self.finish_wait("stdin EOF");
    }

    fn finish_sigterm(mut self) {
        event(
            &self.artifacts,
            &self.test_id,
            "shutdown_requested",
            &[
                ("method", "SIGTERM".to_owned()),
                ("server_pid", self.server_pid.to_string()),
            ],
        );
        // SAFETY: the MCP process is this test's child and owns a separate process group.
        let result = unsafe { libc::kill(self.server_pid as libc::pid_t, libc::SIGTERM) };
        assert_eq!(0, result, "send SIGTERM to the real MCP server");
        self.stdin.take();
        self.finish_wait("SIGTERM");
    }

    fn finish_wait(&mut self, reason: &str) {
        let deadline = scaled_deadline(Duration::from_secs(20));
        let polled = poll_until(
            &self.artifacts,
            &self.test_id,
            deadline,
            "real twinning-mcp shutdown",
            || {
                let guard = self.guard.as_mut().expect("MCP process guard exists");
                match guard.child_mut().try_wait() {
                    Ok(Some(status)) => Some(Ok(status)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                }
            },
        )
        .unwrap_or_else(|error| panic!("wait for MCP after {reason}: {error}"));
        let mut child = self
            .guard
            .take()
            .expect("MCP process guard exists")
            .disarm();
        let status = child.wait().expect("wait for MCP child");
        assert_eq!(polled.code(), status.code());
        let stdout = self
            .stdout_reader
            .take()
            .expect("MCP stdout reader exists")
            .join()
            .expect("MCP stdout reader exits");
        let stderr = self
            .stderr_reader
            .take()
            .expect("MCP stderr reader exists")
            .join()
            .expect("MCP stderr reader exits");
        fs::write(self.session_root.join("stdout.json"), &stdout)
            .expect("write MCP stdout artifact");
        fs::write(self.session_root.join("stderr.log"), &stderr)
            .expect("write MCP stderr artifact");
        for line in stdout.lines() {
            serde_json::from_str::<Value>(line).expect("every MCP stdout line is JSON-RPC JSON");
        }
        assert_server_stderr_format(&stderr);
        self.copy_twin_artifacts();
        event(
            &self.artifacts,
            &self.test_id,
            "mcp_server_finished",
            &[
                ("pid", self.server_pid.to_string()),
                ("exit_code", status.code().unwrap_or(-1).to_string()),
                ("shutdown", reason.to_owned()),
                ("stdout_bytes", stdout.len().to_string()),
                ("stderr_tail", tail(&stderr, 2048)),
            ],
        );
        assert!(status.success(), "real twinning-mcp exited {status}");
        for twin in &self.twins {
            assert_process_gone(
                &self.artifacts,
                &self.test_id,
                twin.pid,
                "REST child after MCP shutdown",
            );
        }
        self.finished = true;
    }

    fn copy_twin_artifacts(&self) {
        for twin in &self.twins {
            let source = self.twin_path(&twin.twin_id);
            let destination = self
                .artifacts
                .join(&self.test_id)
                .join("twins")
                .join(&twin.twin_id);
            fs::create_dir_all(&destination).expect("create twin artifacts directory");
            for entry in fs::read_dir(&source)
                .unwrap_or_else(|error| panic!("read twin state {}: {error}", source.display()))
                .flatten()
            {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let target = destination.join(entry.file_name());
                fs::copy(&path, &target).expect("copy twin artifact");
                let bytes = fs::read(&target).expect("read copied twin artifact");
                event(
                    &self.artifacts,
                    &self.test_id,
                    "twin_artifact_copied",
                    &[
                        ("twin_id", twin.twin_id.clone()),
                        ("name", entry.file_name().to_string_lossy().into_owned()),
                        ("bytes", bytes.len().to_string()),
                        ("sha256", sha256(&bytes)),
                    ],
                );
            }
        }
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        kill_child_groups(&self.state_root());
        if let Some(guard) = &mut self.guard {
            let pid = guard.child_mut().id() as libc::pid_t;
            // SAFETY: this test launched the MCP server as a separate process-group leader.
            unsafe {
                let _ = libc::killpg(pid, libc::SIGKILL);
            }
        }
        self.stdin.take();
        if let Some(reader) = self.stdout_reader.take()
            && let Ok(stdout) = reader.join()
        {
            let _ = fs::write(self.session_root.join("stdout.json"), stdout);
        }
        if let Some(reader) = self.stderr_reader.take()
            && let Ok(stderr) = reader.join()
        {
            let _ = fs::write(self.session_root.join("stderr.log"), &stderr);
            event(
                &self.artifacts,
                &self.test_id,
                "failure_server_stderr_tail",
                &[("stderr_tail", tail(&stderr, 2048))],
            );
        }
        for twin in &self.twins {
            let path = self.twin_path(&twin.twin_id);
            for name in ["stdout.json", "stderr.log"] {
                if let Ok(bytes) = fs::read(path.join(name)) {
                    event(
                        &self.artifacts,
                        &self.test_id,
                        "failure_twin_output_tail",
                        &[
                            ("twin_id", twin.twin_id.clone()),
                            ("file", name.to_owned()),
                            ("tail", tail(&String::from_utf8_lossy(&bytes), 2048)),
                        ],
                    );
                }
            }
        }
    }
}

impl TestContext {
    fn get_process_snapshot(&self, test_id: &str) -> Vec<String> {
        let mut command = Command::new("ps");
        command.args(["-axo", "pid=,ppid=,command="]);
        let output = run_command(
            command,
            &self.artifacts,
            test_id,
            "process_snapshot",
            None,
            Duration::from_secs(10),
        );
        assert_success(&output, "ps process snapshot");
        output.stdout.lines().map(str::to_owned).collect()
    }

    fn post_is_relevant(&self, line: &str) -> bool {
        line.contains(self.mcp.to_str().unwrap_or_default())
            || (line.contains(self.twinning.to_str().unwrap_or_default())
                && line.contains(" rest "))
    }
}

fn relevant_process_snapshot(context: &TestContext) -> Vec<(u32, String)> {
    context
        .get_process_snapshot("RR08")
        .into_iter()
        .filter(|line| context.post_is_relevant(line))
        .filter_map(|line| {
            let pid = line.split_whitespace().next()?.parse::<u32>().ok()?;
            Some((pid, line))
        })
        .collect()
}

fn mapping_body() -> Value {
    json!([{"idType":"ID_CUSIP","idValue":"037833100"}])
}

fn failed_tool(response: &Value, expected_code: &str) -> Value {
    assert!(
        response.get("error").is_none(),
        "MCP protocol error: {response}"
    );
    assert_eq!(Some(true), response["result"]["isError"].as_bool());
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("tool error is text JSON");
    let body: Value = serde_json::from_str(text).expect("tool error JSON parses");
    assert_eq!(expected_code, body["error"]["code"]);
    body
}

fn read_rest_report(path: &Path, test_id: &str) -> Value {
    let bytes = fs::read(path).unwrap_or_else(|error| {
        panic!(
            "[{test_id}] REST report missing at {}: {error}",
            path.display()
        )
    });
    let report: Value = serde_json::from_slice(&bytes).expect("REST report JSON parses");
    assert_eq!("twinning.rest-report.v0", report["version"]);
    report
}

fn find_twinning_binary(repo: &Path) -> PathBuf {
    if let Some(path) = env::var_os("TWINNING_BIN") {
        let path = PathBuf::from(path);
        assert!(
            path.is_file(),
            "TWINNING_BIN is not a file: {}",
            path.display()
        );
        return path.canonicalize().expect("TWINNING_BIN resolves");
    }
    let target = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let target = if target.is_absolute() {
        target
    } else {
        repo.join(target)
    };
    let path = target.join("debug").join("twinning");
    assert!(
        path.is_file(),
        "real twinning binary is missing at {}; run cargo build -p twinning",
        path.display()
    );
    path.canonicalize().expect("built twinning binary resolves")
}

fn find_executable(env_name: &str, name: &str) -> Option<PathBuf> {
    if let Some(path) = env::var_os(env_name) {
        let path = PathBuf::from(path);
        return path.is_file().then_some(path);
    }
    env::var_os("PATH")
        .into_iter()
        .flat_map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(name))
        .find(|path| path.is_file())
}

fn non_loopback_ipv4() -> io::Result<Option<(Ipv4Addr, String)>> {
    let mut first: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs initializes `first` on success and the guard frees it below.
    if unsafe { libc::getifaddrs(&mut first) } != 0 {
        return Err(io::Error::last_os_error());
    }
    struct IfAddrsGuard(*mut libc::ifaddrs);
    impl Drop for IfAddrsGuard {
        fn drop(&mut self) {
            // SAFETY: this is the list returned by getifaddrs, freed exactly once.
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _guard = IfAddrsGuard(first);
    let mut current = first;
    while !current.is_null() {
        // SAFETY: the linked list remains live until IfAddrsGuard is dropped.
        let entry = unsafe { &*current };
        if !entry.ifa_addr.is_null()
            && entry.ifa_flags & libc::IFF_UP as u32 != 0
            && entry.ifa_flags & libc::IFF_LOOPBACK as u32 == 0
        {
            // SAFETY: the family check guarantees sockaddr_in below.
            let address = unsafe { &*entry.ifa_addr };
            if address.sa_family as i32 == libc::AF_INET {
                // SAFETY: AF_INET guarantees this sockaddr layout.
                let address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in) };
                let ip = Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes());
                if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast() {
                    // SAFETY: ifa_name is a NUL-terminated interface name.
                    let name = unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) }
                        .to_string_lossy()
                        .into_owned();
                    return Ok(Some((ip, name)));
                }
            }
        }
        current = entry.ifa_next;
    }
    Ok(None)
}

fn run_command(
    mut command: Command,
    artifacts: &Path,
    test_id: &str,
    label: &str,
    display: Option<String>,
    timeout: Duration,
) -> CapturedOutput {
    let command_line = display.unwrap_or_else(|| command_line(&command));
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.process_group(0);
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("[{test_id}] spawn {label} ({command_line}): {error}"));
    let pid = child.id();
    event(
        artifacts,
        test_id,
        "process_started",
        &[
            ("label", label.to_owned()),
            ("command", command_line.clone()),
            ("pid", pid.to_string()),
        ],
    );
    let stdout = child.stdout.take().expect("child stdout is piped");
    let stderr = child.stderr.take().expect("child stderr is piped");
    let stdout_reader = thread::spawn(move || read_all(stdout));
    let stderr_reader = thread::spawn(move || read_all(stderr));
    let mut guard = ProcessGroupDropGuard::new(child);
    let polled = poll_until(
        artifacts,
        test_id,
        scaled_deadline(timeout),
        &format!("{label} process exit"),
        || match guard.child_mut().try_wait() {
            Ok(Some(status)) => Some(Ok(status)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        },
    )
    .unwrap_or_else(|error| panic!("[{test_id}] wait for {label}: {error}"));
    let mut child = guard.disarm();
    let status = child.wait().expect("wait for completed child");
    assert_eq!(polled.code(), status.code());
    let stdout = stdout_reader.join().expect("child stdout reader exits");
    let stderr = stderr_reader.join().expect("child stderr reader exits");
    event(
        artifacts,
        test_id,
        "process_finished",
        &[
            ("label", label.to_owned()),
            ("pid", pid.to_string()),
            ("exit_code", status.code().unwrap_or(-1).to_string()),
            ("stdout_tail", tail(&stdout, 2048)),
            ("stderr_tail", tail(&stderr, 2048)),
        ],
    );
    CapturedOutput {
        pid,
        command_line,
        status,
        stdout,
        stderr,
    }
}

struct CapturedOutput {
    pid: u32,
    command_line: String,
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn assert_success(output: &CapturedOutput, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed: command={} pid={} exit={:?} stdout_tail={} stderr_tail={}",
        output.command_line,
        output.pid,
        output.status.code(),
        tail(&output.stdout, 2048),
        tail(&output.stderr, 2048)
    );
}

fn poll_until<T>(
    artifacts: &Path,
    test_id: &str,
    deadline: Instant,
    what: &str,
    mut observe: impl FnMut() -> Option<T>,
) -> T {
    let started = Instant::now();
    loop {
        if let Some(value) = observe() {
            event(
                artifacts,
                test_id,
                "poll_succeeded",
                &[
                    ("what", what.to_owned()),
                    ("elapsed_ms", started.elapsed().as_millis().to_string()),
                ],
            );
            return value;
        }
        if Instant::now() >= deadline {
            let elapsed = started.elapsed().as_millis().to_string();
            event(
                artifacts,
                test_id,
                "poll_timed_out",
                &[
                    ("what", what.to_owned()),
                    ("elapsed_ms", elapsed.clone()),
                    ("last", format!("not observed after {elapsed} ms")),
                ],
            );
            panic!("[{test_id}] poll timeout what={what:?} elapsed_ms={elapsed}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_process_gone(artifacts: &Path, test_id: &str, pid: u32, what: &str) {
    poll_until(
        artifacts,
        test_id,
        scaled_deadline(Duration::from_secs(10)),
        what,
        || (!process_alive(pid)).then_some(()),
    );
    assert!(!process_alive(pid), "{what}: pid {pid} remains alive");
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: signal 0 checks whether this process id still exists.
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn kill_child_groups(state_root: &Path) {
    if let Ok(entries) = fs::read_dir(state_root) {
        for entry in entries.flatten() {
            let path = entry.path().join("child.pid");
            if let Ok(pid) = fs::read_to_string(path)
                && let Ok(pid) = pid.trim().parse::<libc::pid_t>()
            {
                // SAFETY: the supervisor creates each twin as a process-group leader.
                unsafe {
                    let _ = libc::killpg(pid, libc::SIGKILL);
                }
            }
        }
    }
}

fn tool_error_code(response: &Value) -> Option<String> {
    response["result"]["content"][0]["text"]
        .as_str()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .and_then(|body| body["error"]["code"].as_str().map(str::to_owned))
}

fn read_json_lines(reader: impl Read, sender: mpsc::Sender<String>) -> String {
    let mut reader = BufReader::new(reader);
    let mut output = String::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                output.push_str(&line);
                if sender.send(line).is_err() {
                    break;
                }
            }
        }
    }
    output
}

fn read_all(mut reader: impl Read) -> String {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).expect("read child output");
    String::from_utf8_lossy(&bytes).into_owned()
}

fn command_line(command: &Command) -> String {
    std::iter::once(format!("{:?}", command.get_program()))
        .chain(command.get_args().map(|argument| format!("{argument:?}")))
        .collect::<Vec<_>>()
        .join(" ")
}

fn event(artifacts: &Path, test_id: &str, name: &str, fields: &[(&str, String)]) {
    let mut line = format!("[{test_id}] event={name}");
    for (key, value) in fields {
        line.push(' ');
        line.push_str(key);
        line.push('=');
        line.push_str(&format!("{value:?}"));
    }
    println!("{line}");
    let path = artifacts.join(format!("{test_id}.log"));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open per-case event log");
    writeln!(file, "{line}").expect("write per-case event log");
    file.flush().expect("flush per-case event log");
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("write digest string");
    }
    output
}

fn tail(text: &str, limit: usize) -> String {
    let start = text.len().saturating_sub(limit);
    text[start..].replace(['\n', '\r'], " | ")
}

fn assert_server_stderr_format(stderr: &str) {
    for line in stderr.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        assert!(fields.len() >= 4, "invalid MCP stderr event line: {line}");
        assert!(
            fields[0].ends_with('Z'),
            "stderr timestamp is not UTC: {line}"
        );
        assert!(
            matches!(fields[1], "INFO" | "WARN" | "ERROR"),
            "invalid MCP stderr level: {line}"
        );
    }
}
