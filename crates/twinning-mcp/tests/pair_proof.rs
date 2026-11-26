#![cfg(unix)]
//! bd-m9v2: `twin_pair_proof` through MCP with the real twin. `proof twin-pair` exits 1 for a
//! FAIL proof, and the client must get that proof, not an internal error.
//!
//! Needs no psql: rows go in through a small pgwire client, so these cases always run. The
//! seed rows and inputs are those of `e2e_real`'s PR01 and PR02.

use std::{
    env, fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use serde_json::{Value, json};
use tempfile::tempdir;

const MCP_BIN: &str = env!("CARGO_BIN_EXE_twinning-mcp");
const DEMO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/demo/migration");

const SEED: [&str; 4] = [
    "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ('tenant-a', 'Tenant A')",
    "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ('tenant-b', 'Tenant B')",
    "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-002', 'tenant-b', 'Beta', 120)",
    "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-001', 'tenant-a', 'Alpha', 100)",
];
const DIVERGED_ROW: &str = "INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES ('deal-002', 'tenant-b', 'Beta migrated', 120)";

/// Startup, one simple query per argument, terminate. Any error response fails the run.
const PGWIRE_CLIENT: &str = r#"
import socket, struct, sys

def read_until_ready(sock):
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
for sql in sys.argv[2:]:
    sock.sendall(b"Q" + struct.pack("!I", len(sql.encode()) + 5) + sql.encode() + b"\x00")
    read_until_ready(sock)
sock.sendall(b"X" + struct.pack("!I", 4))
"#;

/// Two real twins on the demo schema, each given its rows, stopped so each wrote a snapshot.
struct Pair {
    _directory: tempfile::TempDir,
    mcp: Mcp,
    workdir: PathBuf,
}

fn snapshot_pair(left_rows: &[&str], right_rows: &[&str]) -> Pair {
    let directory = tempdir().expect("MCP test directory");
    let workdir = directory.path().join("workdir");
    fs::create_dir_all(&workdir).expect("work directory");
    for name in ["schema.sql", "declaration.json", "queries.json"] {
        fs::copy(Path::new(DEMO).join(name), workdir.join(name)).expect("copy demo input");
    }

    let mut mcp = Mcp::spawn(&workdir);
    mcp.request(
        "initialize",
        json!({"protocolVersion": "2025-03-26", "capabilities": {},
               "clientInfo": {"name": "pair-proof", "version": "1"}}),
    );
    mcp.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));

    for (side, rows) in [("left", left_rows), ("right", right_rows)] {
        let snapshot = workdir.join(format!("{side}.snapshot"));
        let started = mcp.tool(
            "twin_start",
            json!({"kind": "postgres",
                   "schema_path": workdir.join("schema.sql").display().to_string(),
                   "declaration_path": workdir.join("declaration.json").display().to_string(),
                   "snapshot_path": snapshot.display().to_string()}),
        );
        insert(&started, rows);
        let stopped = mcp.tool("twin_stop", json!({"twin_id": started["twin_id"]}));
        assert_eq!(stopped["outcome"], "READY", "{stopped}");
        assert!(snapshot.is_file(), "twin_stop wrote the {side} snapshot");
    }
    Pair {
        _directory: directory,
        mcp,
        workdir,
    }
}

fn insert(started: &Value, rows: &[&str]) {
    let connection = started["connection_string"].as_str().expect("connection");
    let port = connection
        .rsplit_once(':')
        .expect("port in connection string")
        .1
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    let client = Command::new("python3")
        .args(["-c", PGWIRE_CLIENT, &port])
        .args(rows)
        .output()
        .expect("run pgwire client");
    assert!(
        client.status.success(),
        "insert failed: {}",
        String::from_utf8_lossy(&client.stderr)
    );
}

impl Pair {
    fn prove(&mut self, queries: &str, report: &str) -> Value {
        let path = |name: &str| self.workdir.join(name).display().to_string();
        let arguments = json!({
            "left_snapshot_path": path("left.snapshot"),
            "right_snapshot_path": path("right.snapshot"),
            "queries_path": path(queries),
            "report_path": path(report),
        });
        self.mcp.request(
            "tools/call",
            json!({"name": "twin_pair_proof", "arguments": arguments}),
        )
    }
}

fn body(response: &Value, is_error: bool) -> Value {
    assert_eq!(response["result"]["isError"], is_error, "{response}");
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    serde_json::from_str(text).expect("tool JSON")
}

fn verdicts(proof: &Value) -> Vec<(String, String)> {
    let mut verdicts = proof["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .map(|case| {
            let text = |name: &str| case[name].as_str().expect("case text").to_owned();
            (text("case_id"), text("verdict"))
        })
        .collect::<Vec<_>>();
    verdicts.sort();
    verdicts
}

fn expected(verdicts: [(&str, &str); 5]) -> Vec<(String, String)> {
    verdicts
        .map(|(case, verdict)| (case.to_owned(), verdict.to_owned()))
        .to_vec()
}

/// PR02's scenario: one diverging row. Exactly the two cases that read it fail.
#[test]
fn mcp_returns_the_fail_proof_of_two_real_twins() {
    let mut diverged = SEED;
    diverged[2] = DIVERGED_ROW;
    let mut pair = snapshot_pair(&SEED, &diverged);

    let proof = body(&pair.prove("queries.json", "proof.json"), false);

    assert_eq!(proof["outcome"], "FAIL", "{proof}");
    assert_eq!(
        verdicts(&proof),
        expected([
            ("deal_lookup", "fail"),
            ("join_shape_deferred", "skip"),
            ("outside_subset_lookup", "pass"),
            ("tenant_b_deal_count", "pass"),
            ("tenant_b_filtered_scan", "fail"),
        ])
    );
    let written: Value = serde_json::from_slice(
        &fs::read(pair.workdir.join("proof.json")).expect("proof report file"),
    )
    .expect("proof report JSON");
    assert_eq!(written["version"], "twinning.twin-pair-proof.v0");
    assert_eq!(written["outcome"], "FAIL");
    pair.mcp.finish();
}

/// Guard, true before and after: identical twins give a PASS proof.
#[test]
fn mcp_returns_the_pass_proof_of_two_identical_real_twins() {
    let mut pair = snapshot_pair(&SEED, &SEED);

    let proof = body(&pair.prove("queries.json", "proof.json"), false);

    assert_eq!(proof["outcome"], "PASS", "{proof}");
    assert_eq!(
        verdicts(&proof),
        expected([
            ("deal_lookup", "pass"),
            ("join_shape_deferred", "skip"),
            ("outside_subset_lookup", "pass"),
            ("tenant_b_deal_count", "pass"),
            ("tenant_b_filtered_scan", "pass"),
        ])
    );
    pair.mcp.finish();
}

/// Guard, true before and after: a refusal from the real binary (exit 2, no report) stays an
/// error, even when a FAIL proof from an earlier call is already at the report path.
#[test]
fn mcp_keeps_a_real_proof_refusal_an_error() {
    let mut diverged = SEED;
    diverged[2] = DIVERGED_ROW;
    let mut pair = snapshot_pair(&SEED, &diverged);
    fs::write(pair.workdir.join("empty.json"), "{\"queries\": []}\n").expect("empty queries");
    fs::write(
        pair.workdir.join("stale.json"),
        json!({"version": "twinning.twin-pair-proof.v0", "outcome": "FAIL",
               "cases": [{"case_id": "stale", "verdict": "fail"}]})
        .to_string(),
    )
    .expect("stale report");

    let error = body(&pair.prove("empty.json", "stale.json"), true);

    assert_eq!(error["error"]["code"], "E_INTERNAL", "{error}");
    let message = error["error"]["message"].as_str().expect("message");
    assert!(message.contains("exited with 2"), "{message}");
    pair.mcp.finish();
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
    next_id: u64,
}

impl Mcp {
    fn spawn(workdir: &Path) -> Self {
        let mut child = Command::new(MCP_BIN)
            .arg("--workdir")
            .arg(workdir)
            .arg("--twinning-bin")
            .arg(twinning_binary())
            // Its own state root: servers that share one can race in the stale-state sweep.
            .env("TMPDIR", workdir.parent().expect("test directory"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start MCP server");
        let stdout = child.stdout.take().expect("stdout");
        let (sender, responses) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            responses,
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
        body(&response, false)
    }

    fn finish(&mut self) {
        drop(self.child.stdin.take());
        self.child.wait().expect("MCP server exits");
    }
}

/// A failed assertion must not leave the server, or the twins it supervises, running.
impl Drop for Mcp {
    fn drop(&mut self) {
        self.finish();
    }
}
