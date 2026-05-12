#![forbid(unsafe_code)]

use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
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
    assert_eq!(
        result.json["verify"]["results"][0]["affected"][0]["key"]["deal_id"],
        "deal-from-child"
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
    exit_code: i32,
    stdout: String,
    stderr: String,
    diagnostics: String,
    json: Value,
}

fn run_verify_case(deal_name_arg: &str) -> VerifyCaseResult {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let verify_path = write_verify_artifact(dir.path());
    let child_path = write_child(dir.path());
    let diagnostics_path = dir.path().join("client.err");
    let port = reserve_local_port();
    let run_command = format!(
        "python3 {} 127.0.0.1 {} {} {}",
        shell_quote(&child_path),
        port,
        shell_quote_str(deal_name_arg),
        shell_quote(&diagnostics_path),
    );

    let output = Command::new(twinning_bin())
        .arg("postgres")
        .arg("--schema")
        .arg(schema_path)
        .arg("--verify")
        .arg(verify_path)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--run")
        .arg(run_command)
        .arg("--json")
        .output()
        .expect("run twinning");
    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    let diagnostics = fs::read_to_string(diagnostics_path).unwrap_or_default();
    let json = serde_json::from_str(&stdout).expect("parse twinning json");

    VerifyCaseResult {
        exit_code: output.status.code().expect("process exit code"),
        stdout,
        stderr,
        diagnostics,
        json,
    }
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

fn write_verify_artifact(dir: &Path) -> PathBuf {
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
              "check": { "op": "not_null", "binding": "public.deals", "columns": ["deal_name"] }
            }
          ]
        }"#,
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
