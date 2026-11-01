#![cfg(unix)]

use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::process::CommandExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
    time::Duration,
};

use serde_json::{Value, json};
use tempfile::tempdir;

const MCP_BIN: &str = env!("CARGO_BIN_EXE_twinning-mcp");
const JOIN_SQL: &str = "SELECT d.deal_id FROM public.deals AS d JOIN public.deals AS other ON d.deal_id = other.deal_id";
const SCHEMA: &str =
    "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY, amount NUMERIC CHECK (amount >= 0));\n";

#[test]
fn real_psql_join_refusal_is_available_only_in_opt_in_twin_report() {
    let root = repository_root();
    let twinning = twinning_binary(&root);
    let directory = tempdir().expect("create MCP test directory");
    let workdir = directory.path().join("workdir");
    fs::create_dir_all(&workdir).expect("create MCP work directory");
    let schema = workdir.join("schema.sql");
    fs::write(&schema, SCHEMA).expect("write Postgres schema");

    let mut mcp = McpClient::spawn(&workdir, &twinning);
    let initialized = mcp.call(
        "initialize",
        Some(json!({
            "protocolVersion":"2025-03-26",
            "capabilities":{},
            "clientInfo":{"name":"refused-statements-e2e","version":"1"}
        })),
    );
    assert_eq!("2025-03-26", initialized["result"]["protocolVersion"]);
    mcp.notify("notifications/initialized");

    let started = mcp.tool(
        "twin_start",
        json!({
            "kind":"postgres",
            "schema_path":schema.display().to_string(),
            "capture_refused_statements":true
        }),
    );
    let twin_id = started["twin_id"].as_str().expect("twin id").to_owned();
    assert!(!started.to_string().contains(JOIN_SQL));
    let connection = started["connection_string"]
        .as_str()
        .expect("Postgres connection string")
        .to_owned();

    let psql = env::var_os("TWINNING_PSQL_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("psql"));
    let output = Command::new(psql)
        .args([
            "--no-psqlrc",
            "--set=ON_ERROR_STOP=1",
            "--set=VERBOSITY=verbose",
        ])
        .arg(&connection)
        .args(["--command", JOIN_SQL])
        .env_remove("PGSSLMODE")
        .env_remove("PGGSSENCMODE")
        .output()
        .expect("run installed psql against the actual twin");
    assert!(
        !output.status.success(),
        "JOIN must be refused by the subset twin"
    );
    let psql_stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        psql_stderr.contains("0A000"),
        "JOIN refusal must report SQLSTATE 0A000"
    );

    let stopped = mcp.tool("twin_stop", json!({"twin_id":twin_id}));
    assert_eq!("stopped", stopped["status"]);
    assert!(!stopped.to_string().contains(JOIN_SQL));
    let report = mcp.tool("twin_report", json!({"twin_id":twin_id}));
    let statements = report["trace_summary"]["refused_statements"]
        .as_array()
        .expect("opt-in report exposes refused statements");
    assert!(
        statements.iter().any(|statement| {
            statement["sqlstate"] == "0A000"
                && statement["sample_text"]
                    .as_str()
                    .is_some_and(|text| text == JOIN_SQL)
        }),
        "twin_report must include the refused JOIN text"
    );

    mcp.finish();
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root resolves")
}

fn twinning_binary(root: &std::path::Path) -> PathBuf {
    if let Some(path) = env::var_os("TWINNING_BIN") {
        let path = PathBuf::from(path);
        assert!(path.is_file(), "TWINNING_BIN is missing");
        return path;
    }
    let target = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let target = if target.is_absolute() {
        target
    } else {
        root.join(target)
    };
    let path = target.join("debug").join("twinning");
    assert!(
        path.is_file(),
        "build the root twinning binary before MCP e2e"
    );
    path
}

struct McpClient {
    child: Option<Child>,
    stdin: Option<std::process::ChildStdin>,
    responses: Receiver<String>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<String>>,
    next_id: u64,
}

impl McpClient {
    fn spawn(workdir: &std::path::Path, twinning: &std::path::Path) -> Self {
        let mut command = Command::new(MCP_BIN);
        command
            .args(["--workdir", workdir.to_str().expect("UTF-8 workdir")])
            .args(["--twinning-bin", twinning.to_str().expect("UTF-8 binary")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.process_group(0);
        let mut child = command.spawn().expect("start real MCP server");
        let stdout = child.stdout.take().expect("MCP stdout pipe");
        let stderr = child.stderr.take().expect("MCP stderr pipe");
        let (sender, responses) = mpsc::channel();
        let stdout_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if sender.send(line).is_err() => break,
                    Ok(_) => {}
                }
            }
        });
        let stderr_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut text = String::new();
            let _ = reader.read_to_string(&mut text);
            text
        });
        let stdin = child.stdin.take();
        Self {
            child: Some(child),
            stdin,
            responses,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
            next_id: 1,
        }
    }

    fn notify(&mut self, method: &str) {
        self.send(json!({"jsonrpc":"2.0","method":method}));
    }

    fn call(&mut self, method: &str, params: Option<Value>) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut request = json!({"jsonrpc":"2.0","id":id,"method":method});
        if let Some(params) = params {
            request["params"] = params;
        }
        self.send(request);
        let line = self
            .responses
            .recv_timeout(Duration::from_secs(20))
            .expect("MCP response arrives");
        let response: Value = serde_json::from_str(&line).expect("MCP response JSON");
        assert!(response.get("error").is_none(), "MCP request succeeds");
        response
    }

    fn tool(&mut self, name: &str, arguments: Value) -> Value {
        let response = self.call(
            "tools/call",
            Some(json!({"name":name,"arguments":arguments})),
        );
        assert_eq!(false, response["result"]["isError"]);
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool response JSON text");
        serde_json::from_str(text).expect("tool body JSON")
    }

    fn send(&mut self, request: Value) {
        let input = self.stdin.as_mut().expect("MCP stdin remains open");
        serde_json::to_writer(&mut *input, &request).expect("serialize MCP request");
        input.write_all(b"\n").expect("write MCP line");
        input.flush().expect("flush MCP request");
    }

    fn finish(mut self) {
        self.stdin.take();
        let status = self
            .child
            .as_mut()
            .expect("MCP child exists")
            .wait()
            .expect("MCP server exits after EOF");
        assert!(status.success(), "MCP server exits cleanly");
        self.child.take();
        self.stdout_thread
            .take()
            .expect("MCP stdout thread")
            .join()
            .expect("MCP stdout thread exits");
        let stderr = self
            .stderr_thread
            .take()
            .expect("MCP stderr thread")
            .join()
            .expect("MCP stderr thread exits");
        assert!(
            !stderr.contains(JOIN_SQL),
            "refused SQL is not written to logs"
        );
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                // SAFETY: the child starts in a new process group owned by this test.
                unsafe { libc::killpg(child.id() as libc::pid_t, libc::SIGKILL) };
            }
            let _ = child.wait();
        }
    }
}
