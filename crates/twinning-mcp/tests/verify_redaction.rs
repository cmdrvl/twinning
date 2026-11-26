#![cfg(unix)]
//! Embedded verify through the MCP front end, with the real twin: it must not hand raw row
//! keys or cell values to the MCP client or leave them in its report file (bd-uk5d), and the
//! client must be able to read every way a served twin can end (bd-0yjb, bd-tgdr).

use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use serde_json::{Value, json};
use tempfile::tempdir;

const MCP_BIN: &str = env!("CARGO_BIN_EXE_twinning-mcp");
const ROW_KEY: &str = "ZQXCANARY-mcp-key-7714";
const ROW_CELL: &str = "ZQXCANARY-mcp-cell-7715";
/// Rule ids are not redacted. Seeing this one shows that the verify payload reached the surface.
const RULE_ID: &str = "ZQXCANARY_RULE_VISIBLE";

/// Startup, one simple-query INSERT, terminate.
const CHILD: &str = r#"
import socket, struct, sys

def read_until_ready(sock):
    data = b""
    def exact(size):
        out = b""
        while len(out) < size:
            chunk = sock.recv(size - len(out))
            if not chunk:
                raise SystemExit("connection closed")
            out += chunk
        return out
    while True:
        tag = exact(1)
        body = exact(struct.unpack("!I", exact(4))[0] - 4)
        if tag == b"E":
            raise SystemExit(str(body.replace(b"\x00", b" "), "utf-8", "replace"))
        if tag == b"Z":
            return

sock = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
body = struct.pack("!I", 196608) + b"user\x00postgres\x00database\x00postgres\x00\x00"
sock.sendall(struct.pack("!I", len(body) + 4) + body)
read_until_ready(sock)
sql = "INSERT INTO public.deals (deal_id, deal_name) VALUES ('%s', '%s')" % (sys.argv[2], sys.argv[3])
sock.sendall(b"Q" + struct.pack("!I", len(sql) + 5) + sql.encode() + b"\x00")
read_until_ready(sock)
sock.sendall(b"X" + struct.pack("!I", 4))
"#;

/// What one MCP session returned for a twin that was given one row and one verify rule.
struct Session {
    started: Value,
    stopped: Value,
    report: Value,
    report_file: String,
    stderr: String,
}

/// A server with a served twin that has committed one row and is waiting to be stopped.
struct Running {
    directory: tempfile::TempDir,
    mcp: Mcp,
    twin_id: String,
    started: Value,
}

fn start_twin_with_row(check: Option<Value>, snapshot: bool) -> Running {
    let directory = tempdir().expect("MCP test directory");
    let workdir = directory.path().join("workdir");
    fs::create_dir_all(&workdir).expect("work directory");
    fs::write(
        workdir.join("schema.sql"),
        "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY, deal_name TEXT);\n",
    )
    .expect("schema");
    let mut arguments = json!({"kind": "postgres",
        "schema_path": workdir.join("schema.sql").display().to_string()});
    if let Some(check) = check {
        fs::write(
            workdir.join("verify.json"),
            json!({
                "version": "verify.constraint.v1",
                "constraint_set_id": "mcp.redaction",
                "bindings": [{"name": "public.deals", "kind": "relation",
                              "key_fields": ["deal_id"]}],
                "rules": [{"id": RULE_ID, "severity": "error", "portability": "portable",
                           "check": check}]
            })
            .to_string(),
        )
        .expect("verify artifact");
        arguments["verify_path"] = json!(workdir.join("verify.json").display().to_string());
    }
    if snapshot {
        arguments["snapshot_path"] = json!(workdir.join("twin.snapshot").display().to_string());
    }

    let mut mcp = Mcp::spawn(&workdir);
    mcp.request(
        "initialize",
        json!({"protocolVersion": "2025-03-26", "capabilities": {},
               "clientInfo": {"name": "verify-redaction", "version": "1"}}),
    );
    mcp.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));

    let started = mcp.tool("twin_start", arguments);
    let twin_id = started["twin_id"].as_str().expect("twin id").to_owned();
    let connection = started["connection_string"].as_str().expect("connection");
    let port = connection
        .rsplit_once(':')
        .expect("port in connection string")
        .1
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    let child = Command::new("python3")
        .args(["-c", CHILD, &port, ROW_KEY, ROW_CELL])
        .output()
        .expect("run pgwire child");
    assert!(
        child.status.success(),
        "insert failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    Running {
        directory,
        mcp,
        twin_id,
        started,
    }
}

fn run_twin(check: Option<Value>) -> Session {
    let Running {
        directory: _directory,
        mut mcp,
        twin_id,
        started,
    } = start_twin_with_row(check, false);
    let stopped = mcp.tool("twin_stop", json!({"twin_id": twin_id}));
    let report = mcp.tool("twin_report", json!({"twin_id": twin_id}));
    let report_file = report_files(&stopped, &report);
    let stderr = mcp.finish();
    Session {
        started,
        stopped,
        report,
        report_file,
        stderr,
    }
}

/// bd-0yjb through MCP: a twin whose check was refused exits 2. The client must still get
/// the report with the refusal in it, not an unreadable-report error.
#[test]
fn mcp_returns_the_report_of_a_refused_check() {
    let session = run_twin(Some(json!({"op": "predicate", "binding": "public.deals",
        "expr": {"gt": [{"column": "deal_name"}, 5]}})));

    assert_eq!(session.stopped["outcome"], "REFUSAL", "{}", session.stopped);
    assert_eq!(session.stopped["exit_code"], 2);
    assert_eq!(session.report["outcome"], "REFUSAL");
    assert_eq!(session.report["verify"]["outcome"], "REFUSAL");
    assert_eq!(
        session.report["verify"]["refusal"]["detail"]["key"],
        json!({"deal_id": "redacted:ref:k1"})
    );
    // Control for the negative check: the key was committed, and the report is the payload.
    for text in [session.report.to_string(), session.report_file] {
        assert!(text.contains("E_BAD_EXPR"), "{text}");
        assert!(!text.contains(ROW_KEY), "{text}");
    }
}

/// bd-tgdr: every way a served twin can end, with the real binary. `twin_stop` and
/// `twin_report` must both give the report.
#[test]
fn mcp_reads_every_way_a_served_twin_can_end() {
    let not_null = json!({"op": "not_null", "binding": "public.deals", "columns": ["deal_id"]});
    let fails = json!({"op": "predicate", "binding": "public.deals",
        "expr": {"eq": [{"column": "deal_name"}, "Alpha"]}});
    let refuses = json!({"op": "predicate", "binding": "public.deals",
        "expr": {"gt": [{"column": "deal_name"}, 5]}});
    for (check, outcome, exit_code, verify) in [
        (None, "READY", 0, Value::Null),
        (Some(not_null), "PASS", 0, json!("PASS")),
        (Some(fails), "FAIL", 1, json!("FAIL")),
        (Some(refuses), "REFUSAL", 2, json!("REFUSAL")),
    ] {
        let session = run_twin(check);

        assert_eq!(session.stopped["outcome"], outcome, "{}", session.stopped);
        assert_eq!(session.stopped["exit_code"], exit_code, "{outcome}");
        assert_eq!(session.report["outcome"], outcome, "{}", session.report);
        assert_eq!(session.report["verify"]["outcome"], verify, "{outcome}");
    }
}

/// A twin that cannot write its output also exits 2 and prints `REFUSAL`, but wrote no
/// report: through MCP that stays an error, whatever its checks would have said.
#[test]
fn mcp_keeps_a_process_refusal_an_error() {
    let passes = json!({"op": "not_null", "binding": "public.deals", "columns": ["deal_id"]});
    let Running {
        directory,
        mut mcp,
        twin_id,
        ..
    } = start_twin_with_row(Some(passes), true);
    // The snapshot is written when the twin stops; a directory in its place stops that.
    fs::create_dir(directory.path().join("workdir/twin.snapshot")).expect("block snapshot");

    for (tool, message) in [
        ("twin_stop", "child exited with code 2"),
        ("twin_report", "report unreadable"),
    ] {
        let response = mcp.request(
            "tools/call",
            json!({"name": tool, "arguments": {"twin_id": twin_id}}),
        );
        assert_eq!(response["result"]["isError"], true, "{tool}: {response}");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool text");
        assert!(text.contains("E_REPORT_UNREADABLE"), "{tool}: {text}");
        assert!(text.contains(message), "{tool}: {text}");
    }
    mcp.finish();
}

#[test]
fn mcp_twin_with_verify_never_returns_row_data() {
    let Session {
        started,
        stopped,
        report,
        report_file,
        stderr,
    } = run_twin(Some(json!({"op": "predicate", "binding": "public.deals",
        "expr": {"eq": [{"column": "deal_name"}, "Alpha"]}})));

    assert_eq!(report["verify"]["outcome"], "FAIL", "{report}");
    assert_eq!(
        report["verify"]["results"][0]["affected"][0]["key"]["deal_id"],
        "redacted:ref:k1"
    );
    assert_eq!(
        report["verify"]["results"][0]["affected"][0]["value"],
        "redacted:ref:v1"
    );

    for (surface, text) in [
        ("twin_report response", report.to_string()),
        ("report file", report_file),
    ] {
        assert!(
            text.contains(RULE_ID),
            "{surface}: control did not fire: {text}"
        );
        for raw in [ROW_KEY, ROW_CELL] {
            assert!(!text.contains(raw), "{surface}: `{raw}` in {text}");
        }
    }
    // These carry no verify payload, so they have no control and this is only a negative check.
    for (surface, text) in [
        ("twin_start response", started.to_string()),
        ("twin_stop response", stopped.to_string()),
        ("server stderr", stderr),
    ] {
        for raw in [ROW_KEY, ROW_CELL] {
            assert!(!text.contains(raw), "{surface}: `{raw}` in {text}");
        }
    }
}

/// The report file the supervisor had the twin write, wherever it put it.
fn report_files(stopped: &Value, report: &Value) -> String {
    let path = [report, stopped]
        .iter()
        .find_map(|value| value["report_path"].as_str());
    assert!(path.is_some(), "no report_path in {report} or {stopped}");
    fs::read_to_string(path.expect("checked above")).expect("report file")
}

fn twinning_binary() -> PathBuf {
    if let Some(path) = env::var_os("TWINNING_BIN") {
        return PathBuf::from(path);
    }
    // Next to the MCP binary under test, so it is the same build.
    let path = Path::new(MCP_BIN).with_file_name("twinning");
    assert!(
        path.is_file(),
        "build the root twinning binary before MCP e2e"
    );
    path
}

struct Mcp {
    child: Child,
    responses: Receiver<String>,
    stderr: Option<thread::JoinHandle<String>>,
    next_id: u64,
}

impl Mcp {
    fn spawn(workdir: &Path) -> Self {
        let mut child = Command::new(MCP_BIN)
            .arg("--workdir")
            .arg(workdir)
            .arg("--twinning-bin")
            .arg(twinning_binary())
            // The server passes its environment on to the twins it starts.
            .env_remove("VERIFY_REDACTION_KEY")
            // Its own state root: servers that share one can race in the stale-state sweep.
            .env("TMPDIR", workdir.parent().expect("test directory"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start MCP server");
        let stdout = child.stdout.take().expect("stdout");
        let mut stderr = child.stderr.take().expect("stderr");
        let (sender, responses) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr = thread::spawn(move || {
            let mut text = String::new();
            let _ = stderr.read_to_string(&mut text);
            text
        });
        Self {
            child,
            responses,
            stderr: Some(stderr),
            next_id: 1,
        }
    }

    fn send(&mut self, message: Value) {
        let input = self.child.stdin.as_mut().expect("stdin open");
        writeln!(input, "{message}").expect("write");
        input.flush().expect("flush");
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let line = self
            .responses
            .recv_timeout(Duration::from_secs(20))
            .expect("MCP response");
        let response: Value = serde_json::from_str(&line).expect("response JSON");
        assert!(response.get("error").is_none(), "{response}");
        response
    }

    fn tool(&mut self, name: &str, arguments: Value) -> Value {
        let response = self.request("tools/call", json!({"name": name, "arguments": arguments}));
        assert_eq!(response["result"]["isError"], false, "{response}");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool text");
        serde_json::from_str(text).expect("tool JSON")
    }

    fn finish(mut self) -> String {
        drop(self.child.stdin.take());
        self.child.wait().expect("MCP server exits");
        self.stderr
            .take()
            .expect("stderr thread")
            .join()
            .expect("join")
    }
}

/// A failed assertion must not leave the server, or the twins it supervises, running.
impl Drop for Mcp {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}
