#![cfg(unix)]

pub mod common;

use std::{
    env, fs,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, TcpStream},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use common::{ProcessGroupDropGuard, scaled_deadline};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const MCP_BIN: &str = env!("CARGO_BIN_EXE_twinning-mcp");
const MIGRATION_DEMO_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/demo/migration");
const SCHEMA: &str = "CREATE TABLE public.deals (\n    deal_id TEXT PRIMARY KEY,\n    deal_name TEXT NOT NULL,\n    amount NUMERIC CHECK (amount >= 0)\n);\n";
const MCP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const CHILD_TIMEOUT: Duration = Duration::from_secs(120);

#[test]
fn r01_real_start_stop_loopback_only() {
    let context = TestContext::new();
    let non_loopback = non_loopback_ipv4().expect("inspect host addresses");
    let mut session = context.session("R01");
    let workdir = prepare_case(&context.artifacts, "R01");
    let schema = write_schema(&context.artifacts, "R01", &workdir);
    let twin = session.start_twin("R01", &workdir, &schema, None);
    if let Some((address, interface)) = non_loopback {
        let Some(psql) = context.psql.as_deref() else {
            common::skipped(
                "R01-non-loopback",
                "psql executable was not found in PATH or TWINNING_PSQL_BIN",
                &context.artifacts,
            );
            let summary = session.stop_twin("R01", &twin.twin_id);
            assert_stop_ready(&summary, "R01");
            assert_eq!("READY", session.report("R01", &twin.twin_id)["outcome"]);
            session.finish();
            return;
        };
        context.record_psql_version("R01", psql);
        let started = std::time::Instant::now();
        let non_loopback_dsn = format!(
            "postgresql://u@{address}:{}/db?connect_timeout=1",
            twin.port
        );
        let result = run_psql(
            &context.artifacts,
            "R01",
            psql,
            &non_loopback_dsn,
            &["--command", "SELECT 1"],
        );
        assert!(
            !result.status.success(),
            "R01 non-loopback listener connected"
        );
        assert!(
            result
                .stderr
                .to_ascii_lowercase()
                .contains("could not connect")
                || result
                    .stderr
                    .to_ascii_lowercase()
                    .contains("connection refused")
                || result
                    .stderr
                    .to_ascii_lowercase()
                    .contains("operation timed out")
                || result
                    .stderr
                    .to_ascii_lowercase()
                    .contains("timeout expired"),
            "R01 expected a non-loopback connection failure, got: {}",
            tail(&result.stderr, 2048)
        );
        event(
            &context.artifacts,
            "R01",
            "non_loopback_probe",
            &[
                ("interface", interface),
                ("address", address.to_string()),
                ("port", twin.port.to_string()),
                ("exit_code", result.status.code().unwrap_or(-1).to_string()),
                ("stderr_tail", tail(&result.stderr, 2048)),
                ("elapsed_ms", started.elapsed().as_millis().to_string()),
            ],
        );
    } else {
        common::skipped(
            "R01-non-loopback",
            "host has no active non-loopback IPv4 address",
            &context.artifacts,
        );
    }
    let summary = session.stop_twin("R01", &twin.twin_id);
    assert_stop_ready(&summary, "R01");
    assert_eq!("READY", session.report("R01", &twin.twin_id)["outcome"]);
    session.finish();
}

#[test]
fn r02_psql_returned_connection_string() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "R02",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("R02", psql);
    let mut session = context.session("R02");
    let workdir = prepare_case(&context.artifacts, "R02");
    let schema = write_schema(&context.artifacts, "R02", &workdir);
    let twin = session.start_twin("R02", &workdir, &schema, None);
    let output = run_psql(
        &context.artifacts,
        "R02",
        psql,
        &twin.connection_string,
        &["--command", "BEGIN", "--command", "ROLLBACK"],
    );
    assert_success(&output, "R02 psql BEGIN/ROLLBACK");
    assert!(output.stdout.contains("BEGIN"));
    assert!(output.stdout.contains("ROLLBACK"));
    assert_stop_ready(&session.stop_twin("R02", &twin.twin_id), "R02");
    session.finish();
}

#[test]
fn r03_psql_default_sslmode() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "R03",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("R03", psql);
    let mut session = context.session("R03");
    let workdir = prepare_case(&context.artifacts, "R03");
    let schema = write_schema(&context.artifacts, "R03", &workdir);
    let twin = session.start_twin("R03", &workdir, &schema, None);
    let default_ssl_dsn = twin
        .connection_string
        .split('?')
        .next()
        .expect("connection URL has a scheme");
    assert!(!default_ssl_dsn.contains("sslmode="));
    let output = run_psql(
        &context.artifacts,
        "R03",
        psql,
        default_ssl_dsn,
        &["--command", "SELECT version()"],
    );
    assert_success(&output, "R03 psql default SSL mode");
    assert!(output.stdout.contains("PostgreSQL 16.0 (twinning)"));
    assert_stop_ready(&session.stop_twin("R03", &twin.twin_id), "R03");
    session.finish();
}

#[test]
fn r04_client_failure_isolation_and_disconnect() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "R04",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("R04", psql);
    let mut session = context.session("R04");
    let workdir = prepare_case(&context.artifacts, "R04");
    let schema = write_schema(&context.artifacts, "R04", &workdir);
    let twin = session.start_twin("R04", &workdir, &schema, None);
    let mut broken =
        TcpStream::connect(("127.0.0.1", twin.port)).expect("R04 bad client connects to live twin");
    broken
        .write_all(&[0_u8, 0, 0, 32, 0, 3])
        .expect("write partial startup packet");
    drop(broken);
    event(
        &context.artifacts,
        "R04",
        "bad_client_disconnected",
        &[
            ("port", twin.port.to_string()),
            ("bytes_sent", "6".to_owned()),
        ],
    );
    assert_tcp_ready(&context.artifacts, "R04", twin.port);
    let good = run_psql(
        &context.artifacts,
        "R04",
        psql,
        &twin.connection_string,
        &["--command", "SELECT version()"],
    );
    assert_success(&good, "R04 good psql after bad startup");
    let abandoned = run_psql(
        &context.artifacts,
        "R04",
        psql,
        &twin.connection_string,
        &["--command", "BEGIN"],
    );
    assert_success(&abandoned, "R04 BEGIN followed by psql disconnect");
    let second = run_psql(
        &context.artifacts,
        "R04",
        psql,
        &twin.connection_string,
        &["--command", "BEGIN", "--command", "ROLLBACK"],
    );
    assert_success(&second, "R04 second BEGIN after disconnect");
    let summary = session.stop_twin("R04", &twin.twin_id);
    assert_stop_ready(&summary, "R04");
    let trace: Value = serde_json::from_slice(
        &fs::read(session.twin_path(&twin.twin_id).join("trace.json")).expect("R04 trace exists"),
    )
    .expect("R04 trace parses");
    let sqlstates = trace_sqlstates(&trace);
    event(
        &context.artifacts,
        "R04",
        "trace_sqlstates",
        &[("values", serde_json::to_string(&sqlstates).unwrap())],
    );
    assert!(
        !sqlstates.iter().any(|sqlstate| sqlstate == "55P03"),
        "R04 writer lock remained held: {sqlstates:?}"
    );
    assert_eq!("READY", session.report("R04", &twin.twin_id)["outcome"]);
    session.finish();
}

#[test]
fn r05_psycopg2_constraints_and_report() {
    let context = TestContext::new();
    let Some(uv) = context.uv.as_deref() else {
        common::skipped(
            "R05",
            "approved uv executable was not found in PATH or UV_BIN",
            &context.artifacts,
        );
        return;
    };
    let mut session = context.session("R05");
    let workdir = prepare_case(&context.artifacts, "R05");
    let schema = write_schema(&context.artifacts, "R05", &workdir);
    let twin = session.start_twin("R05", &workdir, &schema, None);
    let cases = run_real_driver_cases(
        &context.artifacts,
        "R05",
        uv,
        &context.driver_script,
        &workdir,
        twin.port,
    );
    assert_driver_case(&cases, "psycopg2_valid_insert", Value::Null);
    assert_driver_case(&cases, "psycopg2_check_violation", json!("23514"));
    assert_driver_case(&cases, "psycopg2_duplicate_primary_key", json!("23505"));
    let summary = session.stop_twin("R05", &twin.twin_id);
    assert_stop_ready(&summary, "R05");
    assert_eq!(1, summary["constraints"]["check_violations"]);
    assert_eq!(1, summary["constraints"]["unique_violations"]);
    assert_eq!(2, summary["tables"]["public.deals"]["rows"]);
    let report = session.report("R05", &twin.twin_id);
    assert_eq!(1, report["constraints"]["check_violations"]);
    assert_eq!(1, report["constraints"]["unique_violations"]);
    session.finish();
}

#[test]
fn r06_sqlalchemy_core_insert_select() {
    let context = TestContext::new();
    let Some(uv) = context.uv.as_deref() else {
        common::skipped(
            "R06",
            "approved uv executable was not found in PATH or UV_BIN",
            &context.artifacts,
        );
        return;
    };
    let mut session = context.session("R06");
    let workdir = prepare_case(&context.artifacts, "R06");
    let schema = write_schema(&context.artifacts, "R06", &workdir);
    let twin = session.start_twin("R06", &workdir, &schema, None);
    let cases = run_real_driver_cases(
        &context.artifacts,
        "R06",
        uv,
        &context.driver_script,
        &workdir,
        twin.port,
    );
    assert_driver_case(&cases, "sqlalchemy_core_insert", Value::Null);
    let select = cases
        .iter()
        .find(|case| case["case"] == "sqlalchemy_core_select_by_pk")
        .expect("SQLAlchemy select case exists");
    assert_eq!(Value::Null, select["sqlstate"]);
    assert_eq!(json!([["sqlalchemy-valid-1", "Beta"]]), select["rows"]);
    let summary = session.stop_twin("R06", &twin.twin_id);
    assert_stop_ready(&summary, "R06");
    session.finish();
}

#[test]
fn r07_full_mcp_session_with_real_driver_violation() {
    let context = TestContext::new();
    let Some(uv) = context.uv.as_deref() else {
        common::skipped(
            "R07",
            "approved uv executable was not found in PATH or UV_BIN",
            &context.artifacts,
        );
        return;
    };
    let mut session = context.session("R07");
    let workdir = prepare_case(&context.artifacts, "R07");
    let schema = write_schema(&context.artifacts, "R07", &workdir);
    let twin = session.start_twin("R07", &workdir, &schema, None);
    let cases = run_real_driver_cases(
        &context.artifacts,
        "R07",
        uv,
        &context.driver_script,
        &workdir,
        twin.port,
    );
    assert_driver_case(&cases, "psycopg2_check_violation", json!("23514"));
    let summary = session.stop_twin("R07", &twin.twin_id);
    assert_stop_ready(&summary, "R07");
    assert_eq!(1, summary["constraints"]["check_violations"]);
    let report = session.report("R07", &twin.twin_id);
    assert_eq!("READY", report["outcome"]);
    assert_eq!(1, report["constraints"]["check_violations"]);
    event(
        &context.artifacts,
        "R07",
        "violation_visible_in_report",
        &[
            ("sqlstate", "23514".to_owned()),
            ("check_violations", "1".to_owned()),
        ],
    );
    session.finish();
}

#[test]
fn r08_seed_contract_materializes_rows() {
    let context = TestContext::new();
    let mut session = context.session("R08");
    let workdir = prepare_case(&context.artifacts, "R08");
    let schema = write_schema(&context.artifacts, "R08", &workdir);
    let contract = session.tool(
        "R08",
        "twin_seed_contract",
        json!({"kind":"postgres","schema_path":schema}),
    );
    assert_eq!(1, contract["table_count"]);
    let contract_path = PathBuf::from(
        contract["contract_path"]
            .as_str()
            .expect("contract path is text"),
    );
    let contract_bytes = fs::read(&contract_path).expect("generated seed contract is readable");
    let contract_text = String::from_utf8(contract_bytes.clone()).expect("seed contract is UTF-8");
    let contract_line = contract_text
        .lines()
        .next()
        .expect("contract has one target line");
    let target: Value = serde_json::from_str(contract_line).expect("contract target JSON");
    assert_eq!("twinning.seed-contract.v0", target["version"]);
    assert_eq!("postgres", target["twin"]);
    assert_eq!("public.deals", target["target"]);
    let filled_seed = workdir.join("seed-filled.jsonl");
    let data_row = json!({
        "version":"twinning.seed-data.v0",
        "kind":"row",
        "twin":target["twin"],
        "target_kind":target["target_kind"],
        "target":target["target"],
        "row":{"deal_id":"seeded-r08","deal_name":"Seeded from contract","amount":7}
    });
    let mut filled_text = contract_text;
    if !filled_text.ends_with('\n') {
        filled_text.push('\n');
    }
    filled_text.push_str(&serde_json::to_string(&data_row).expect("serialize seed row"));
    filled_text.push('\n');
    fs::write(&filled_seed, filled_text.as_bytes()).expect("write filled seed file");
    event(
        &context.artifacts,
        "R08",
        "seed_filled",
        &[
            ("contract_bytes", contract_bytes.len().to_string()),
            ("seed_bytes", filled_text.len().to_string()),
            ("seed_sha256", sha256(filled_text.as_bytes())),
        ],
    );
    let twin = session.start_twin("R08", &workdir, &schema, Some(&filled_seed));
    let summary = session.stop_twin("R08", &twin.twin_id);
    assert_stop_ready(&summary, "R08");
    assert_eq!(1, summary["tables"]["public.deals"]["rows"]);
    event(
        &context.artifacts,
        "R08",
        "seed_row_visible",
        &[(
            "rows",
            summary["tables"]["public.deals"]["rows"].to_string(),
        )],
    );
    session.finish();
}

struct TestContext {
    artifacts: PathBuf,
    twinning: PathBuf,
    psql: Option<PathBuf>,
    uv: Option<PathBuf>,
    driver_script: PathBuf,
}

impl TestContext {
    fn new() -> Self {
        let artifacts = common::artifacts_dir("twinning_mcp_e2e_real").clone();
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root resolves");
        let twinning = find_twinning_binary(&repo);
        let driver_script = repo.join("tests/compat/driver_cases.py");
        assert!(
            driver_script.is_file(),
            "M0c real-driver harness is missing"
        );
        Self {
            artifacts,
            twinning,
            psql: find_executable("TWINNING_PSQL_BIN", "psql"),
            uv: find_executable("UV_BIN", "uv"),
            driver_script,
        }
    }

    fn session(&self, test_id: &str) -> McpSession {
        let mut session = McpSession::spawn(&self.artifacts, test_id, &self.twinning);
        session.initialize_and_list(test_id);
        event(
            &self.artifacts,
            test_id,
            "twinning_binary",
            &[("path", self.twinning.display().to_string())],
        );
        session
    }

    fn record_psql_version(&self, test_id: &str, psql: &Path) {
        let mut command = Command::new(psql);
        command.arg("--version");
        let version = run_command(
            command,
            &self.artifacts,
            test_id,
            "psql_version",
            Duration::from_secs(10),
        );
        assert_success(&version, "psql --version");
        event(
            &self.artifacts,
            test_id,
            "psql_version",
            &[("value", version.stdout.trim().to_owned())],
        );
    }
}

#[derive(Debug)]
struct StartedTwin {
    twin_id: String,
    port: u16,
    connection_string: String,
}

struct TwinRecord {
    test_id: String,
    twin_id: String,
    pid: u32,
    port: u16,
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
    twins: Vec<TwinRecord>,
    stopped: Vec<String>,
    finished: bool,
}

impl McpSession {
    fn spawn(artifacts: &Path, session_name: &str, twinning: &Path) -> Self {
        let session_root = artifacts.join(session_name);
        let temp_root = session_root.join("tmp");
        fs::create_dir_all(&temp_root).expect("create MCP TMPDIR");
        let mut command = Command::new(MCP_BIN);
        command
            .args([
                "--workdir",
                session_root.to_str().expect("session path UTF-8"),
            ])
            .args([
                "--twinning-bin",
                twinning.to_str().expect("binary path UTF-8"),
            ])
            .current_dir(&session_root)
            .env("TMPDIR", &temp_root)
            .env_remove("TWINNING_BIN")
            .env_remove("TWINNING_MCP_WORKDIR")
            .env_remove("TWINNING_MCP_MAX_TWINS")
            .env_remove("TWINNING_MCP_RESTRICTED")
            .env_remove("FAKE_TWINNING_MODE")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let rendered_command = command_line(&command);
        command.process_group(0);
        let mut child = command.spawn().expect("start real twinning-mcp binary");
        let server_pid = child.id();
        event(
            artifacts,
            session_name,
            "mcp_server_started",
            &[
                ("command", rendered_command),
                ("pid", server_pid.to_string()),
                ("fake_mode", "none; explicit real --twinning-bin".to_owned()),
                ("tmpdir", temp_root.display().to_string()),
            ],
        );
        let stdout = child.stdout.take().expect("MCP stdout is piped");
        let stderr = child.stderr.take().expect("MCP stderr is piped");
        let (sender, responses) = mpsc::channel();
        let stdout_reader = thread::spawn(move || read_json_lines(stdout, sender));
        let stderr_reader = thread::spawn(move || read_all(stderr));
        let stdin = child.stdin.take();
        Self {
            test_id: session_name.to_owned(),
            artifacts: artifacts.to_path_buf(),
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

    fn initialize_and_list(&mut self, test_id: &str) {
        let response = self.call(
            test_id,
            "initialize",
            Some(json!({
                "protocolVersion":"2025-03-26",
                "capabilities":{},
                "clientInfo":{"name":"M5-real-e2e","version":"1"}
            })),
        );
        assert_eq!("2025-03-26", response["result"]["protocolVersion"]);
        self.notify_initialized(test_id);
        let tools = self.call(test_id, "tools/list", None);
        let names = tools["result"]["tools"]
            .as_array()
            .expect("tools/list returns an array")
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();
        assert!(names.contains(&"twin_start"));
        assert!(names.contains(&"twin_stop"));
        assert!(names.contains(&"twin_report"));
        assert!(names.contains(&"twin_pair_proof"));
        event(
            &self.artifacts,
            test_id,
            "tools_list_verified",
            &[("tools", names.join(","))],
        );
    }

    fn notify_initialized(&mut self, test_id: &str) {
        let request = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        let rendered = serde_json::to_string(&request).expect("serialize MCP notification");
        event(
            &self.artifacts,
            test_id,
            "jsonrpc_notification",
            &[("method", "notifications/initialized".to_owned())],
        );
        writeln!(
            self.stdin.as_mut().expect("MCP stdin remains open"),
            "{rendered}"
        )
        .expect("write initialized notification");
        self.stdin
            .as_mut()
            .expect("MCP stdin remains open")
            .flush()
            .expect("flush initialized notification");
    }

    fn call(&mut self, test_id: &str, method: &str, params: Option<Value>) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut request = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            request["params"] = params;
        }
        let rendered = serde_json::to_string(&request).expect("serialize MCP request");
        event(
            &self.artifacts,
            test_id,
            "jsonrpc_request",
            &[("id", id.to_string()), ("method", method.to_owned())],
        );
        writeln!(
            self.stdin.as_mut().expect("MCP stdin remains open"),
            "{rendered}"
        )
        .expect("write MCP request");
        self.stdin
            .as_mut()
            .expect("MCP stdin remains open")
            .flush()
            .expect("flush MCP request");
        let line = poll_case_until(
            &self.artifacts,
            test_id,
            scaled_deadline(MCP_RESPONSE_TIMEOUT),
            Duration::from_millis(20),
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
            .map(|result| result.keys().cloned().collect::<Vec<_>>().join(","))
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

    fn tool(&mut self, test_id: &str, name: &str, arguments: Value) -> Value {
        let response = self.call(
            test_id,
            "tools/call",
            Some(json!({"name":name,"arguments":arguments})),
        );
        assert!(
            response.get("error").is_none(),
            "tool protocol error: {response}"
        );
        assert_eq!(Some(false), response["result"]["isError"].as_bool());
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool content is JSON text");
        serde_json::from_str(text).expect("successful tool body is JSON")
    }

    fn start_twin(
        &mut self,
        test_id: &str,
        workdir: &Path,
        schema: &Path,
        seed: Option<&Path>,
    ) -> StartedTwin {
        self.start_twin_with_options(test_id, workdir, schema, seed, None, None)
    }

    fn start_twin_with_options(
        &mut self,
        test_id: &str,
        workdir: &Path,
        schema: &Path,
        seed: Option<&Path>,
        snapshot: Option<&Path>,
        declaration: Option<&Path>,
    ) -> StartedTwin {
        let mut arguments = json!({
            "kind":"postgres",
            "schema_path":schema.display().to_string()
        });
        if let Some(seed) = seed {
            arguments["seed_path"] = json!(seed.display().to_string());
        }
        if let Some(snapshot) = snapshot {
            arguments["snapshot_path"] = json!(snapshot.display().to_string());
        }
        if let Some(declaration) = declaration {
            arguments["declaration_path"] = json!(declaration.display().to_string());
        }
        let started = self.tool(test_id, "twin_start", arguments);
        let twin_id = started["twin_id"]
            .as_str()
            .expect("twin_start returns id")
            .to_owned();
        let port = started["port"].as_u64().expect("twin_start returns port") as u16;
        let pid = started["pid"].as_u64().expect("twin_start returns pid") as u32;
        let connection_string = started["connection_string"]
            .as_str()
            .expect("twin_start returns connection string")
            .to_owned();
        let child_pid = self.twin_path(&twin_id).join("child.pid");
        poll_case_until(
            &self.artifacts,
            test_id,
            scaled_deadline(Duration::from_secs(5)),
            Duration::from_millis(20),
            "MCP child.pid artifact",
            || fs::read_to_string(&child_pid).ok(),
        );
        let file_pid = fs::read_to_string(&child_pid)
            .expect("twin child pid file is readable")
            .trim()
            .parse::<u32>()
            .expect("twin child pid file is numeric");
        assert_eq!(pid, file_pid, "tool pid matches child.pid file");
        event(
            &self.artifacts,
            test_id,
            "twin_started",
            &[
                ("twin_id", twin_id.clone()),
                ("pid", pid.to_string()),
                ("port", port.to_string()),
                ("connection_string", connection_string.clone()),
                ("workdir", workdir.display().to_string()),
            ],
        );
        self.twins.push(TwinRecord {
            test_id: test_id.to_owned(),
            twin_id: twin_id.clone(),
            pid,
            port,
        });
        assert_tcp_ready(&self.artifacts, test_id, port);
        let command = assert_child_argv_is_loopback(&self.artifacts, test_id, pid, port);
        event(
            &self.artifacts,
            test_id,
            "twinning_command",
            &[
                ("command", command),
                ("port", port.to_string()),
                ("cwd", workdir.display().to_string()),
            ],
        );
        StartedTwin {
            twin_id,
            port,
            connection_string,
        }
    }

    fn stop_twin(&mut self, test_id: &str, twin_id: &str) -> Value {
        let summary = self.tool(test_id, "twin_stop", json!({"twin_id":twin_id}));
        event(
            &self.artifacts,
            test_id,
            "stop_summary",
            &[
                ("twin_id", twin_id.to_owned()),
                ("outcome", summary["outcome"].to_string()),
                ("exit_code", summary["exit_code"].to_string()),
                ("constraints", summary["constraints"].to_string()),
                ("request_count", summary["request_count"].to_string()),
            ],
        );
        self.stopped.push(twin_id.to_owned());
        summary
    }

    fn report(&mut self, test_id: &str, twin_id: &str) -> Value {
        let report = self.tool(test_id, "twin_report", json!({"twin_id":twin_id}));
        event(
            &self.artifacts,
            test_id,
            "twin_report",
            &[
                ("twin_id", twin_id.to_owned()),
                ("outcome", report["outcome"].to_string()),
                ("constraints", report["constraints"].to_string()),
                ("trace_summary", report["trace_summary"].to_string()),
            ],
        );
        report
    }

    fn twin_path(&self, twin_id: &str) -> PathBuf {
        self.temp_root
            .join("twinning-mcp")
            .join(self.server_pid.to_string())
            .join(twin_id)
    }

    fn finish(mut self) {
        self.stdin.take();
        let deadline = scaled_deadline(Duration::from_secs(15));
        poll_case_until(
            &self.artifacts,
            &self.test_id,
            deadline,
            Duration::from_millis(20),
            "real twinning-mcp exit after stdin EOF",
            || {
                let guard = self.guard.as_mut().expect("MCP process guard exists");
                match guard.child_mut().try_wait() {
                    Ok(Some(status)) => Some(Ok(status)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                }
            },
        )
        .expect("real twinning-mcp process wait succeeds");
        let mut child = self
            .guard
            .take()
            .expect("MCP process guard exists")
            .disarm();
        let status = child.wait().expect("wait for real twinning-mcp");
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
            .expect("save MCP stdout artifact");
        fs::write(self.session_root.join("stderr.log"), &stderr).expect("save MCP stderr artifact");
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
                ("stderr_tail", tail(&stderr, 2048)),
                ("stdout_bytes", stdout.len().to_string()),
            ],
        );
        assert!(status.success(), "real twinning-mcp exited {status}");
        self.finished = true;
    }

    fn copy_twin_artifacts(&self) {
        let state_root = self
            .temp_root
            .join("twinning-mcp")
            .join(self.server_pid.to_string());
        for twin in &self.twins {
            let source = state_root.join(&twin.twin_id);
            let destination = self
                .artifacts
                .join(&twin.test_id)
                .join("twins")
                .join(&twin.twin_id);
            fs::create_dir_all(&destination).expect("create twin artifact directory");
            for entry in fs::read_dir(&source)
                .unwrap_or_else(|error| panic!("read twin state {}: {error}", source.display()))
                .flatten()
            {
                let path = entry.path();
                if path.is_file() {
                    let target = destination.join(entry.file_name());
                    fs::copy(&path, &target).expect("copy twin artifact");
                    let bytes = fs::read(&target).expect("read copied artifact metadata");
                    event(
                        &self.artifacts,
                        &twin.test_id,
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
            event(
                &self.artifacts,
                &twin.test_id,
                "twin_process_stopped",
                &[
                    ("twin_id", twin.twin_id.clone()),
                    ("pid", twin.pid.to_string()),
                    ("port", twin.port.to_string()),
                    ("alive", process_alive(twin.pid).to_string()),
                    (
                        "was_stopped_by_tool",
                        self.stopped.contains(&twin.twin_id).to_string(),
                    ),
                ],
            );
            assert!(
                !process_alive(twin.pid),
                "twin child {} still exists after MCP session finished",
                twin.pid
            );
        }
    }

    fn capture_failure_artifacts(&mut self, state_root: &Path) {
        let stdout = self
            .stdout_reader
            .take()
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default();
        let stderr = self
            .stderr_reader
            .take()
            .and_then(|reader| reader.join().ok())
            .unwrap_or_default();
        let _ = fs::write(self.session_root.join("stdout.json"), &stdout);
        let _ = fs::write(self.session_root.join("stderr.log"), &stderr);
        event(
            &self.artifacts,
            &self.test_id,
            "failure_server_output",
            &[
                ("stdout_tail", tail(&stdout, 2048)),
                ("stderr_tail", tail(&stderr, 2048)),
            ],
        );

        for twin in &self.twins {
            let source = state_root.join(&twin.twin_id);
            let destination = self
                .artifacts
                .join(&twin.test_id)
                .join("twins")
                .join(&twin.twin_id);
            let _ = fs::create_dir_all(&destination);
            for name in [
                "report.json",
                "trace.json",
                "stdout.json",
                "stderr.log",
                "child.pid",
            ] {
                let path = source.join(name);
                let Ok(bytes) = fs::read(&path) else {
                    continue;
                };
                let _ = fs::write(destination.join(name), &bytes);
                if matches!(name, "stdout.json" | "stderr.log") {
                    let output = String::from_utf8_lossy(&bytes);
                    let field = if name == "stdout.json" {
                        "stdout_tail"
                    } else {
                        "stderr_tail"
                    };
                    event(
                        &self.artifacts,
                        &twin.test_id,
                        "failure_twin_output",
                        &[
                            ("twin_id", twin.twin_id.clone()),
                            ("file", name.to_owned()),
                            (field, tail(&output, 2048)),
                        ],
                    );
                }
            }
        }
    }
}

fn assert_child_argv_is_loopback(artifacts: &Path, test_id: &str, pid: u32, port: u16) -> String {
    let mut command = Command::new("ps");
    command.args(["-ww", "-p", &pid.to_string(), "-o", "command="]);
    let output = run_command(
        command,
        artifacts,
        test_id,
        "twinning_process_argv",
        Duration::from_secs(10),
    );
    assert_success(&output, "ps twinning child argv");
    let argv = output.stdout.trim();
    assert!(
        argv.contains("--host 127.0.0.1"),
        "real twinning child is not loopback-bound: {argv}"
    );
    assert!(
        argv.contains(&format!("--port {port}")),
        "real twinning child argv has the wrong port: {argv}"
    );
    event(
        artifacts,
        test_id,
        "twinning_child_argv_verified",
        &[("pid", pid.to_string()), ("argv", argv.to_owned())],
    );
    argv.to_owned()
}

impl Drop for McpSession {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let state_root = self
            .temp_root
            .join("twinning-mcp")
            .join(self.server_pid.to_string());
        kill_child_groups(&state_root);
        if let Some(guard) = &mut self.guard {
            let pid = guard.child_mut().id() as libc::pid_t;
            // SAFETY: the MCP server was started in a fresh process group owned by this test.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
        self.capture_failure_artifacts(&state_root);
    }
}

fn run_real_driver_cases(
    artifacts: &Path,
    test_id: &str,
    uv: &Path,
    script: &Path,
    workdir: &Path,
    port: u16,
) -> Vec<Value> {
    let scratch = artifacts.join(test_id).join("uv-scratch");
    fs::create_dir_all(&scratch).expect("create approved uv scratch directory");
    let mut command = Command::new(uv);
    command
        .args([
            "run",
            "--with",
            "psycopg2-binary",
            "--with",
            "sqlalchemy",
            "python",
        ])
        .arg(script)
        .args(["--port", &port.to_string()])
        .current_dir(&scratch)
        .env_remove("PGSSLMODE")
        .env_remove("PGGSSENCMODE")
        .env_remove("CONDA_DEFAULT_ENV")
        .env_remove("CONDA_EXE")
        .env_remove("CONDA_PREFIX")
        .env_remove("CONDA_PROMPT_MODIFIER")
        .env_remove("CONDA_PYTHON_EXE")
        .env_remove("CONDA_SHLVL");
    let command_text = command_line(&command);
    event(
        artifacts,
        test_id,
        "real_drivers_started",
        &[
            ("command", command_text.clone()),
            ("cwd", scratch.display().to_string()),
            ("port", port.to_string()),
            ("workdir", workdir.display().to_string()),
        ],
    );
    let output = run_command(
        command,
        artifacts,
        test_id,
        "real_driver_cases",
        CHILD_TIMEOUT,
    );
    event(
        artifacts,
        test_id,
        "real_drivers_finished",
        &[
            ("pid", output.pid.to_string()),
            ("exit_code", output.status.code().unwrap_or(-1).to_string()),
            ("stderr_tail", tail(&output.stderr, 2048)),
        ],
    );
    assert_success(&output, "approved uv real-driver harness");

    let mut versions = None;
    let mut cases = Vec::new();
    for line in output.stdout.lines() {
        if let Some(json) = line.strip_prefix("VERSIONS: ") {
            versions = Some(serde_json::from_str::<Value>(json).expect("driver versions JSON"));
        } else if line.starts_with('{') {
            let case: Value = serde_json::from_str(line).expect("driver case JSON");
            cases.push(case);
        } else if !line.trim().is_empty() {
            event(
                artifacts,
                test_id,
                "driver_stdout_line",
                &[("line", line.to_owned())],
            );
        }
    }
    let versions = versions.expect("driver harness printed versions");
    event(
        artifacts,
        test_id,
        "driver_versions",
        &[("value", versions.to_string())],
    );
    assert!(versions["psycopg2"].as_str().is_some());
    assert!(versions["sqlalchemy"].as_str().is_some());
    assert!(versions["uv"].as_str().is_some());
    assert_eq!(7, cases.len(), "all 7 M0c driver cases ran");

    let expected = [
        ("psycopg2_valid_insert", Value::Null),
        ("psycopg2_check_violation", json!("23514")),
        ("psycopg2_duplicate_primary_key", json!("23505")),
        ("psycopg2_select_by_pk", Value::Null),
        ("psycopg2_default_sslmode_select_by_pk", Value::Null),
        ("sqlalchemy_core_insert", Value::Null),
        ("sqlalchemy_core_select_by_pk", Value::Null),
    ];
    for (name, sqlstate) in expected {
        let case = cases
            .iter()
            .find(|case| case["case"] == name)
            .unwrap_or_else(|| panic!("missing real driver case {name}"));
        assert_eq!(sqlstate, case["sqlstate"], "driver case {name}: {case}");
        event(
            artifacts,
            test_id,
            "driver_case",
            &[
                ("case", name.to_owned()),
                ("sqlstate", case["sqlstate"].to_string()),
                ("rows", case["rows"].to_string()),
                ("row_count", case["row_count"].to_string()),
            ],
        );
    }
    for (name, expected_rows) in [
        (
            "psycopg2_select_by_pk",
            json!([["psycopg2-valid-1", "Alpha"]]),
        ),
        (
            "psycopg2_default_sslmode_select_by_pk",
            json!([["psycopg2-valid-1", "Alpha"]]),
        ),
        (
            "sqlalchemy_core_select_by_pk",
            json!([["sqlalchemy-valid-1", "Beta"]]),
        ),
    ] {
        let case = cases
            .iter()
            .find(|case| case["case"] == name)
            .expect("select driver case exists");
        assert_eq!(expected_rows, case["rows"], "driver case {name}");
    }
    cases
}

fn assert_driver_case(cases: &[Value], name: &str, sqlstate: Value) {
    let case = cases
        .iter()
        .find(|case| case["case"] == name)
        .unwrap_or_else(|| panic!("missing real driver case {name}"));
    assert_eq!(sqlstate, case["sqlstate"], "driver case {name}: {case}");
}

fn run_psql(
    artifacts: &Path,
    test_id: &str,
    psql: &Path,
    connection_string: &str,
    query_args: &[&str],
) -> CapturedOutput {
    let mut command = Command::new(psql);
    command
        .args(["--no-psqlrc", "--set=ON_ERROR_STOP=1"])
        .arg(connection_string)
        .args(query_args)
        .env_remove("PGSSLMODE")
        .env_remove("PGGSSENCMODE");
    let output = run_command(command, artifacts, test_id, "psql", Duration::from_secs(20));
    event(
        artifacts,
        test_id,
        "psql_finished",
        &[
            ("command", output.command_line.clone()),
            ("pid", output.pid.to_string()),
            ("exit_code", output.status.code().unwrap_or(-1).to_string()),
            ("stdout_tail", tail(&output.stdout, 2048)),
            ("stderr_tail", tail(&output.stderr, 2048)),
        ],
    );
    output
}

struct CapturedOutput {
    pid: u32,
    command_line: String,
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn run_command(
    mut command: Command,
    artifacts: &Path,
    test_id: &str,
    label: &str,
    timeout: Duration,
) -> CapturedOutput {
    let command_line = command_line(&command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.process_group(0);
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {label} ({command_line}): {error}"));
    let pid = child.id();
    event(
        artifacts,
        test_id,
        "child_spawned",
        &[
            ("label", label.to_owned()),
            ("command", command_line.clone()),
            ("pid", pid.to_string()),
        ],
    );
    let stdout = child.stdout.take().expect("child stdout piped");
    let stderr = child.stderr.take().expect("child stderr piped");
    let stdout_reader = thread::spawn(move || read_all(stdout));
    let stderr_reader = thread::spawn(move || read_all(stderr));
    let mut guard = ProcessGroupDropGuard::new(child);
    let polled_status = poll_case_until(
        artifacts,
        test_id,
        scaled_deadline(timeout),
        Duration::from_millis(20),
        &format!("{label} process exit"),
        || match guard.child_mut().try_wait() {
            Ok(Some(status)) => Some(Ok(status)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        },
    )
    .unwrap_or_else(|error| panic!("poll {label} process exit: {error}"));
    let mut child = guard.disarm();
    let status = child.wait().expect("wait for completed child");
    assert_eq!(
        polled_status.code(),
        status.code(),
        "polled exit status matches wait result"
    );
    let stdout = stdout_reader
        .join()
        .expect("child stdout reader thread exits");
    let stderr = stderr_reader
        .join()
        .expect("child stderr reader thread exits");
    event(
        artifacts,
        test_id,
        "child_finished",
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

fn assert_success(output: &CapturedOutput, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed: command={} exit={:?} stdout_tail={} stderr_tail={}",
        output.command_line,
        output.status.code(),
        tail(&output.stdout, 2048),
        tail(&output.stderr, 2048)
    );
}

fn prepare_case(artifacts: &Path, test_id: &str) -> PathBuf {
    let workdir = artifacts.join(test_id).join("workdir");
    fs::create_dir_all(&workdir).expect("create real twin workdir");
    event(
        artifacts,
        test_id,
        "workdir_created",
        &[("path", workdir.display().to_string())],
    );
    workdir
}

fn write_schema(artifacts: &Path, test_id: &str, workdir: &Path) -> PathBuf {
    let path = workdir.join("schema.sql");
    fs::write(&path, SCHEMA).expect("write real twin DDL");
    event(
        artifacts,
        test_id,
        "schema_written",
        &[
            ("path", path.display().to_string()),
            ("bytes", SCHEMA.len().to_string()),
            ("sha256", sha256(SCHEMA.as_bytes())),
        ],
    );
    path
}

fn assert_tcp_ready(artifacts: &Path, test_id: &str, port: u16) {
    let started = std::time::Instant::now();
    let stream = poll_case_until(
        artifacts,
        test_id,
        scaled_deadline(Duration::from_secs(10)),
        Duration::from_millis(50),
        "real twin TCP listener",
        || TcpStream::connect(("127.0.0.1", port)).ok(),
    );
    drop(stream);
    event(
        artifacts,
        test_id,
        "tcp_ready",
        &[
            ("host", "127.0.0.1".to_owned()),
            ("port", port.to_string()),
            ("elapsed_ms", started.elapsed().as_millis().to_string()),
        ],
    );
}

fn find_twinning_binary(repo: &Path) -> PathBuf {
    if let Some(path) = env::var_os("TWINNING_BIN") {
        let path = PathBuf::from(path);
        assert!(
            path.is_file(),
            "TWINNING_BIN is not a file: {}",
            path.display()
        );
        return path;
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
        "built twinning binary is missing at {}; run cargo build -p twinning",
        path.display()
    );
    path
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
    // SAFETY: getifaddrs initializes `first` on success; the guard below frees the list.
    let result = unsafe { libc::getifaddrs(&mut first) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    struct IfAddrsGuard(*mut libc::ifaddrs);
    impl Drop for IfAddrsGuard {
        fn drop(&mut self) {
            // SAFETY: this pointer is the successful getifaddrs result and is freed once.
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _guard = IfAddrsGuard(first);
    let mut current = first;
    while !current.is_null() {
        // SAFETY: the linked list remains valid until IfAddrsGuard is dropped.
        let entry = unsafe { &*current };
        if !entry.ifa_addr.is_null()
            && entry.ifa_flags & libc::IFF_UP as u32 != 0
            && entry.ifa_flags & libc::IFF_LOOPBACK as u32 == 0
        {
            // SAFETY: sa_family identifies the concrete sockaddr layout below.
            let address = unsafe { &*entry.ifa_addr };
            if address.sa_family as i32 == libc::AF_INET {
                // SAFETY: AF_INET guarantees a sockaddr_in address.
                let address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in) };
                let ip = Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes());
                if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast() {
                    // SAFETY: ifa_name is a NUL-terminated interface name in this OS API.
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

fn trace_sqlstates(trace: &Value) -> Vec<String> {
    trace["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| event["result"]["sqlstate"].as_str().map(str::to_owned))
        .collect()
}

fn assert_stop_ready(summary: &Value, test_id: &str) {
    assert_eq!("stopped", summary["status"], "{test_id} stop summary");
    assert_eq!("READY", summary["outcome"], "{test_id} stop summary");
    assert_eq!(0, summary["exit_code"], "{test_id} stop summary");
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

fn poll_case_until<T>(
    artifacts: &Path,
    test_id: &str,
    deadline: Instant,
    interval: Duration,
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
            let elapsed_ms = started.elapsed().as_millis().to_string();
            let last = format!("not observed after {elapsed_ms} ms");
            event(
                artifacts,
                test_id,
                "poll_timed_out",
                &[
                    ("what", what.to_owned()),
                    ("elapsed_ms", elapsed_ms.clone()),
                    ("last", last.clone()),
                ],
            );
            panic!("[{test_id}] poll timeout what={what:?} elapsed_ms={elapsed_ms} last={last}");
        }
        thread::sleep(interval);
    }
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
        write!(&mut output, "{byte:02x}").expect("write into digest string");
    }
    output
}

fn tail(text: &str, limit: usize) -> String {
    let start = text.len().saturating_sub(limit);
    text[start..].replace(['\n', '\r'], " | ")
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: signal zero checks process existence without sending a signal.
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
                // SAFETY: MCP writes child.pid for the process-group leader it owns.
                unsafe {
                    libc::killpg(pid, libc::SIGKILL);
                }
            }
        }
    }
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
        assert!(!fields[2].is_empty() && !fields[3].is_empty());
        for field in fields.iter().skip(4) {
            assert!(
                field.contains('='),
                "invalid stderr key/value field: {line}"
            );
        }
    }
}

#[test]
fn pr01_identical_live_twins_prove_all_non_skip_cases() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "PR01",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("PR01", psql);
    let mut session = context.session("PR01");
    let (workdir, schema, declaration, queries) =
        prepare_migration_case(&context.artifacts, "PR01");
    let left_path = workdir.join("left.snapshot");
    let right_path = workdir.join("right.snapshot");
    let left = session.start_twin_with_options(
        "PR01",
        &workdir,
        &schema,
        None,
        Some(&left_path),
        Some(&declaration),
    );
    let right = session.start_twin_with_options(
        "PR01",
        &workdir,
        &schema,
        None,
        Some(&right_path),
        Some(&declaration),
    );
    let sql = migration_seed_sql();
    run_migration_sql(&context, "PR01", psql, &left.connection_string, &sql);
    run_migration_sql(&context, "PR01", psql, &right.connection_string, &sql);
    assert_stop_ready(&session.stop_twin("PR01", &left.twin_id), "PR01-left");
    assert_stop_ready(&session.stop_twin("PR01", &right.twin_id), "PR01-right");
    assert!(left_path.is_file(), "twin_stop wrote left snapshot");
    assert!(right_path.is_file(), "twin_stop wrote right snapshot");
    let proof = session.tool(
        "PR01",
        "twin_pair_proof",
        json!({
            "left_snapshot_path":left_path,
            "right_snapshot_path":right_path,
            "queries_path":queries
        }),
    );
    assert_eq!("PASS", proof["outcome"]);
    assert_eq!(4, cases_with_verdict(&proof, "pass"));
    assert_eq!(1, cases_with_verdict(&proof, "skip"));
    assert_eq!("skip", proof_case(&proof, "join_shape_deferred")["verdict"]);
    record_proof_result(&context.artifacts, "PR01", &proof);
    session.finish();
}

#[test]
fn pr02_one_divergent_psql_row_fails_exactly_the_reading_cases() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "PR02",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("PR02", psql);
    let mut session = context.session("PR02");
    let (workdir, schema, declaration, queries) =
        prepare_migration_case(&context.artifacts, "PR02");
    let left_path = workdir.join("left.snapshot");
    let right_path = workdir.join("right.snapshot");
    let left = session.start_twin_with_options(
        "PR02",
        &workdir,
        &schema,
        None,
        Some(&left_path),
        Some(&declaration),
    );
    let right = session.start_twin_with_options(
        "PR02",
        &workdir,
        &schema,
        None,
        Some(&right_path),
        Some(&declaration),
    );
    run_migration_sql(
        &context,
        "PR02",
        psql,
        &left.connection_string,
        &migration_seed_sql(),
    );
    let mut diverged = migration_seed_sql();
    diverged[2] = "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-002', 'tenant-b', 'Beta migrated', 120)".to_owned();
    run_migration_sql(&context, "PR02", psql, &right.connection_string, &diverged);
    assert_stop_ready(&session.stop_twin("PR02", &left.twin_id), "PR02-left");
    assert_stop_ready(&session.stop_twin("PR02", &right.twin_id), "PR02-right");
    let proof = session.tool(
        "PR02",
        "twin_pair_proof",
        json!({
            "left_snapshot_path":left_path,
            "right_snapshot_path":right_path,
            "queries_path":queries
        }),
    );
    assert_eq!("FAIL", proof["outcome"]);
    let failing_ids = proof["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["verdict"] == "fail")
        .map(|case| case["case_id"].as_str().unwrap().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        std::collections::BTreeSet::from([
            "deal_lookup".to_owned(),
            "tenant_b_filtered_scan".to_owned()
        ]),
        failing_ids
    );
    assert_eq!("pass", proof_case(&proof, "tenant_b_deal_count")["verdict"]);
    assert_eq!(
        "pass",
        proof_case(&proof, "outside_subset_lookup")["verdict"]
    );
    assert_eq!("skip", proof_case(&proof, "join_shape_deferred")["verdict"]);
    record_proof_result(&context.artifacts, "PR02", &proof);
    session.finish();
}

#[test]
fn pr03_out_of_subset_refusal_has_real_snapshot_parity() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "PR03",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("PR03", psql);
    let mut session = context.session("PR03");
    let (workdir, schema, declaration, _) = prepare_migration_case(&context.artifacts, "PR03");
    let queries = workdir.join("refusal-query.json");
    fs::write(&queries, r#"{"queries":[{"id":"outside_subset_lookup","table":"public.audit_log","shape":"point_lookup","projection":["id"],"lookup_column":"id","lookup_value":{"text":"audit-001"}}]}"#)
        .expect("write outside-subset proof query");
    let left_path = workdir.join("left.snapshot");
    let right_path = workdir.join("right.snapshot");
    let left = session.start_twin_with_options(
        "PR03",
        &workdir,
        &schema,
        None,
        Some(&left_path),
        Some(&declaration),
    );
    let right = session.start_twin_with_options(
        "PR03",
        &workdir,
        &schema,
        None,
        Some(&right_path),
        Some(&declaration),
    );
    let sql = migration_seed_sql();
    run_migration_sql(&context, "PR03", psql, &left.connection_string, &sql);
    run_migration_sql(&context, "PR03", psql, &right.connection_string, &sql);
    assert_stop_ready(&session.stop_twin("PR03", &left.twin_id), "PR03-left");
    assert_stop_ready(&session.stop_twin("PR03", &right.twin_id), "PR03-right");
    let proof = session.tool(
        "PR03",
        "twin_pair_proof",
        json!({
            "left_snapshot_path":left_path,
            "right_snapshot_path":right_path,
            "queries_path":queries
        }),
    );
    assert_eq!("PASS", proof["outcome"]);
    let case = proof_case(&proof, "outside_subset_lookup");
    assert_eq!("pass", case["verdict"]);
    let report_path = PathBuf::from(
        proof["report_path"]
            .as_str()
            .expect("proof response report path"),
    );
    let full_report: Value =
        serde_json::from_slice(&fs::read(&report_path).expect("proof report file is readable"))
            .expect("full proof report parses");
    let full_case = proof_case(&full_report, "outside_subset_lookup");
    assert_eq!("42P01", full_case["left"]["result"]["sqlstate"]);
    assert_eq!(
        full_case["left"]["result"]["sqlstate"],
        full_case["right"]["result"]["sqlstate"]
    );
    record_proof_result(&context.artifacts, "PR03", &proof);
    session.finish();
}

#[test]
fn pr04_join_case_is_reported_as_skip() {
    let context = TestContext::new();
    let Some(psql) = context.psql.as_deref() else {
        common::skipped(
            "PR04",
            "psql executable was not found in PATH or TWINNING_PSQL_BIN",
            &context.artifacts,
        );
        return;
    };
    context.record_psql_version("PR04", psql);
    let mut session = context.session("PR04");
    let (workdir, schema, declaration, _) = prepare_migration_case(&context.artifacts, "PR04");
    let queries = workdir.join("join-query.json");
    fs::write(&queries, r#"{"queries":[{"id":"join_shape_deferred","table":"public.deals","shape":"join","projection":["deal_id"],"lookup_column":"tenant_id","lookup_value":{"text":"tenant-b"},"policy":"skip","reason_code":"join_replay_deferred","reason":"join is outside the declared subset"}]}"#)
        .expect("write join skip query");
    let left_path = workdir.join("left.snapshot");
    let right_path = workdir.join("right.snapshot");
    let left = session.start_twin_with_options(
        "PR04",
        &workdir,
        &schema,
        None,
        Some(&left_path),
        Some(&declaration),
    );
    let right = session.start_twin_with_options(
        "PR04",
        &workdir,
        &schema,
        None,
        Some(&right_path),
        Some(&declaration),
    );
    let sql = migration_seed_sql();
    run_migration_sql(&context, "PR04", psql, &left.connection_string, &sql);
    run_migration_sql(&context, "PR04", psql, &right.connection_string, &sql);
    assert_stop_ready(&session.stop_twin("PR04", &left.twin_id), "PR04-left");
    assert_stop_ready(&session.stop_twin("PR04", &right.twin_id), "PR04-right");
    let proof = session.tool(
        "PR04",
        "twin_pair_proof",
        json!({
            "left_snapshot_path":left_path,
            "right_snapshot_path":right_path,
            "queries_path":queries
        }),
    );
    assert_eq!("PASS", proof["outcome"]);
    assert_eq!("skip", proof_case(&proof, "join_shape_deferred")["verdict"]);
    record_proof_result(&context.artifacts, "PR04", &proof);
    session.finish();
}

fn prepare_migration_case(artifacts: &Path, test_id: &str) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let workdir = prepare_case(artifacts, test_id);
    let demo = Path::new(MIGRATION_DEMO_DIR);
    let schema = workdir.join("schema.sql");
    let declaration = workdir.join("declaration.json");
    let queries = workdir.join("queries.json");
    fs::copy(demo.join("schema.sql"), &schema).expect("copy migration schema");
    fs::copy(demo.join("declaration.json"), &declaration).expect("copy migration declaration");
    fs::copy(demo.join("queries.json"), &queries).expect("copy migration queries");
    (workdir, schema, declaration, queries)
}

fn migration_seed_sql() -> Vec<String> {
    vec![
        "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ('tenant-a', 'Tenant A')".to_owned(),
        "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ('tenant-b', 'Tenant B')".to_owned(),
        "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-002', 'tenant-b', 'Beta', 120)".to_owned(),
        "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-001', 'tenant-a', 'Alpha', 100)".to_owned(),
    ]
}

fn run_migration_sql(
    context: &TestContext,
    test_id: &str,
    psql: &Path,
    connection_string: &str,
    statements: &[String],
) {
    let mut command = Command::new(psql);
    command.arg(connection_string);
    for statement in statements {
        command.args(["--command", statement]);
    }
    let sql_text = statements.join("; ");
    let result = run_command(
        command,
        &context.artifacts,
        test_id,
        "migration_psql_seed",
        Duration::from_secs(30),
    );
    assert_success(&result, "migration proof psql seed");
    event(
        &context.artifacts,
        test_id,
        "real_psql_seed",
        &[
            ("sql", sql_text),
            ("exit_code", result.status.code().unwrap_or(-1).to_string()),
            ("stdout_tail", tail(&result.stdout, 2048)),
            ("stderr_tail", tail(&result.stderr, 2048)),
        ],
    );
}

fn proof_case<'a>(proof: &'a Value, case_id: &str) -> &'a Value {
    proof["cases"]
        .as_array()
        .expect("proof cases array")
        .iter()
        .find(|case| case["case_id"] == case_id)
        .unwrap_or_else(|| panic!("proof case {case_id} missing: {}", proof["cases"]))
}

fn cases_with_verdict(proof: &Value, verdict: &str) -> usize {
    proof["cases"]
        .as_array()
        .expect("proof cases array")
        .iter()
        .filter(|case| case["verdict"] == verdict)
        .count()
}

fn record_proof_result(artifacts: &Path, test_id: &str, proof: &Value) {
    let cases = proof["cases"].as_array().expect("proof cases array");
    event(
        artifacts,
        test_id,
        "twin_pair_proof_result",
        &[
            (
                "outcome",
                proof["outcome"].as_str().unwrap_or("<missing>").to_owned(),
            ),
            ("case_count", cases.len().to_string()),
            ("case_verdicts", serde_json::to_string(cases).unwrap()),
            (
                "report_path",
                proof["report_path"]
                    .as_str()
                    .unwrap_or("<missing>")
                    .to_owned(),
            ),
        ],
    );
}
