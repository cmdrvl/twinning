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
fn run_once_failed_transaction_commit_preserves_empty_committed_artifacts() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let aborted_child = write_child(dir.path(), "aborted", ABORTED_TRANSACTION_CHILD);
    let noop_child = write_child(dir.path(), "noop", NOOP_CHILD);

    let aborted = run_case(dir.path(), "aborted", &schema_path, &aborted_child);
    let noop = run_case(dir.path(), "noop", &schema_path, &noop_child);

    assert!(
        aborted.status_success,
        "aborted transaction run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        aborted.stdout, aborted.stderr, aborted.diagnostics
    );
    assert!(
        noop.status_success,
        "noop run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        noop.stdout, noop.stderr, noop.diagnostics
    );
    assert!(
        aborted.stderr.is_empty(),
        "unexpected aborted stderr: {}",
        aborted.stderr
    );
    assert!(
        noop.stderr.is_empty(),
        "unexpected noop stderr: {}",
        noop.stderr
    );

    assert_eq!(aborted.report["mode"], "run_once");
    assert_eq!(aborted.report["run"]["exit_code"], 0);
    assert_eq!(aborted.report["tables"]["public.tenants"]["rows"], 0);
    assert_eq!(aborted.snapshot["mode"], "committed_state");
    assert_eq!(aborted.snapshot["table_rows"]["public.tenants"], 0);
    assert_eq!(
        aborted.snapshot["snapshot_hash"], noop.snapshot["snapshot_hash"],
        "aborted overlay writes must not change the committed-state snapshot hash"
    );
}

#[test]
fn run_once_query_trace_records_redacted_live_events_without_snapshot_drift() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let child = write_child(dir.path(), "trace-aborted", ABORTED_TRANSACTION_CHILD);

    let traced = run_case_with_query_trace(dir.path(), "trace-aborted", &schema_path, &child);
    let plain = run_case(dir.path(), "plain-aborted", &schema_path, &child);

    assert!(
        traced.status_success,
        "traced run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        traced.stdout, traced.stderr, traced.diagnostics
    );
    assert!(
        plain.status_success,
        "plain run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        plain.stdout, plain.stderr, plain.diagnostics
    );
    assert!(plain.query_trace.is_none());
    assert!(
        !dir.path().join("plain-aborted.query-trace.json").exists(),
        "default run_once behavior should not write a query trace"
    );
    assert_eq!(
        traced.snapshot["snapshot_hash"], plain.snapshot["snapshot_hash"],
        "query trace must not change committed-state snapshot hashing"
    );

    let trace = traced.query_trace.expect("query trace artifact");
    assert_query_trace_schema_surface(&trace);
    assert_eq!(trace["version"], "twinning.query-trace.v0");
    assert_eq!(trace["policy"]["sql_text"], "redacted_hash_only");
    assert_eq!(trace["policy"]["bind_values"], "redacted_hash_or_null");

    let events = trace["events"].as_array().expect("trace events");
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["protocol"], "simple_query");
    assert_eq!(events[0]["statement_kind"], "begin");
    assert_eq!(events[0]["transaction_before"], "idle");
    assert_eq!(events[0]["transaction_after"], "in_transaction");
    assert_eq!(events[0]["result"]["tag"], "begin");

    assert_eq!(events[1]["protocol"], "extended_query");
    assert_eq!(events[1]["statement_kind"], "insert");
    assert_eq!(events[1]["transaction_before"], "in_transaction");
    assert_eq!(events[1]["transaction_after"], "in_transaction");
    assert_eq!(events[1]["result"]["outcome"], "success");
    assert_eq!(events[1]["result"]["tag"], "insert");
    assert_eq!(events[1]["result"]["rows_affected"], 1);
    assert!(
        events[1]["operation_hash"]
            .as_str()
            .expect("operation hash")
            .starts_with("sha256:")
    );
    assert_eq!(events[1]["binds"]["count"], 2);
    assert_eq!(events[1]["binds"]["values"][0]["policy"], "sha256");

    assert_eq!(events[2]["protocol"], "extended_query");
    assert_eq!(events[2]["result"]["outcome"], "refusal");
    assert_eq!(events[2]["result"]["sqlstate"], "23502");
    assert_eq!(events[2]["transaction_after"], "failed_transaction");
    assert_eq!(events[2]["binds"]["values"][1]["policy"], "null");

    assert_eq!(events[3]["protocol"], "simple_query");
    assert_eq!(events[3]["statement_kind"], "commit");
    assert_eq!(events[3]["transaction_before"], "failed_transaction");
    assert_eq!(events[3]["transaction_after"], "idle");
    assert_eq!(events[3]["result"]["tag"], "rollback");

    let rendered = serde_json::to_string(&trace).expect("render trace");
    assert!(!rendered.contains("tenant-a"));
    assert!(!rendered.contains("Alpha"));
    assert!(!rendered.contains("INSERT INTO public.tenants"));
}

#[test]
fn run_once_autocommit_write_artifacts_include_only_successful_statements() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let child = write_child(dir.path(), "autocommit", AUTOCOMMIT_CHILD);

    let autocommit = run_case(dir.path(), "autocommit", &schema_path, &child);

    assert!(
        autocommit.status_success,
        "autocommit run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        autocommit.stdout, autocommit.stderr, autocommit.diagnostics
    );
    assert!(
        autocommit.stderr.is_empty(),
        "unexpected autocommit stderr: {}",
        autocommit.stderr
    );
    assert_eq!(autocommit.report["mode"], "run_once");
    assert_eq!(autocommit.report["run"]["exit_code"], 0);
    assert_eq!(autocommit.report["tables"]["public.tenants"]["rows"], 1);
    assert_eq!(autocommit.snapshot["mode"], "committed_state");
    assert_eq!(autocommit.snapshot["table_rows"]["public.tenants"], 1);
}

#[test]
fn run_once_update_delete_artifacts_reflect_only_committed_state() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let child = write_child(dir.path(), "update-delete", UPDATE_DELETE_CHILD);

    let result = run_case(dir.path(), "update-delete", &schema_path, &child);

    assert!(
        result.status_success,
        "update/delete run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        result.stdout, result.stderr, result.diagnostics
    );
    assert!(
        result.stderr.is_empty(),
        "unexpected update/delete stderr: {}",
        result.stderr
    );
    assert_eq!(result.report["mode"], "run_once");
    assert_eq!(result.report["run"]["exit_code"], 0);
    assert_eq!(result.report["tables"]["public.tenants"]["rows"], 0);
    assert_eq!(result.snapshot["mode"], "committed_state");
    assert_eq!(result.snapshot["table_rows"]["public.tenants"], 0);
}

#[test]
fn run_once_reordered_composite_on_conflict_updates_committed_artifacts() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_financials_schema(dir.path());
    let child = write_child(dir.path(), "composite-upsert", COMPOSITE_UPSERT_CHILD);

    let result = run_case(dir.path(), "composite-upsert", &schema_path, &child);

    assert!(
        result.status_success,
        "composite upsert run_once should exit cleanly: stdout={}; stderr={}; diagnostics={}",
        result.stdout, result.stderr, result.diagnostics
    );
    assert!(
        result.stderr.is_empty(),
        "unexpected composite upsert stderr: {}",
        result.stderr
    );
    assert_eq!(result.report["mode"], "run_once");
    assert_eq!(result.report["run"]["exit_code"], 0);
    assert_eq!(result.report["tables"]["public.financials"]["rows"], 1);
    assert_eq!(result.snapshot["mode"], "committed_state");
    assert_eq!(result.snapshot["table_rows"]["public.financials"], 1);
    assert_eq!(
        result.snapshot["relations"]["public.financials"][0]["noi"]["value"],
        125
    );
}

struct RunCase {
    status_success: bool,
    stdout: String,
    stderr: String,
    diagnostics: String,
    report: Value,
    snapshot: Value,
    query_trace: Option<Value>,
}

fn run_case(dir: &Path, label: &str, schema_path: &Path, child_path: &Path) -> RunCase {
    run_case_inner(dir, label, schema_path, child_path, false)
}

fn run_case_with_query_trace(
    dir: &Path,
    label: &str,
    schema_path: &Path,
    child_path: &Path,
) -> RunCase {
    run_case_inner(dir, label, schema_path, child_path, true)
}

fn run_case_inner(
    dir: &Path,
    label: &str,
    schema_path: &Path,
    child_path: &Path,
    query_trace: bool,
) -> RunCase {
    let diagnostics_path = dir.join(format!("{label}.client.err"));
    let report_path = dir.join(format!("{label}.report.json"));
    let snapshot_path = dir.join(format!("{label}.snapshot.twin"));
    let query_trace_path = dir.join(format!("{label}.query-trace.json"));
    let port = reserve_local_port();
    let run_command = format!(
        "python3 {} 127.0.0.1 {} {}",
        shell_quote(child_path),
        port,
        shell_quote(&diagnostics_path),
    );

    let mut command = Command::new(twinning_bin());
    command
        .arg("postgres")
        .arg("--schema")
        .arg(schema_path)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--run")
        .arg(run_command)
        .arg("--report")
        .arg(&report_path)
        .arg("--snapshot")
        .arg(&snapshot_path);
    if query_trace {
        command.arg("--query-trace").arg(&query_trace_path);
    }
    let output = command
        .arg("--json")
        .output()
        .unwrap_or_else(|error| panic!("run twinning {label}: {error}"));

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    let diagnostics = fs::read_to_string(diagnostics_path).unwrap_or_default();
    let report = serde_json::from_str(&stdout).expect("parse stdout report");
    let snapshot = serde_json::from_str(
        &fs::read_to_string(&snapshot_path).expect("read committed-state snapshot"),
    )
    .expect("parse committed-state snapshot");
    let query_trace = query_trace.then(|| {
        serde_json::from_str(
            &fs::read_to_string(&query_trace_path).expect("read query trace artifact"),
        )
        .expect("parse query trace artifact")
    });

    RunCase {
        status_success: output.status.success(),
        stdout,
        stderr,
        diagnostics,
        report,
        snapshot,
        query_trace,
    }
}

fn assert_query_trace_schema_surface(trace: &Value) {
    let schema: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/twinning.query-trace.v0.schema.json"
    )))
    .expect("parse query trace schema");
    for required in schema["required"].as_array().expect("top-level required") {
        let key = required.as_str().expect("required key");
        assert!(trace.get(key).is_some(), "trace should include `{key}`");
    }
    let event_required = schema["$defs"]["event"]["required"]
        .as_array()
        .expect("event required");
    for event in trace["events"].as_array().expect("events") {
        for required in event_required {
            let key = required.as_str().expect("event required key");
            assert!(event.get(key).is_some(), "event should include `{key}`");
        }
    }
}

fn write_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("schema.sql");
    fs::write(
        &schema_path,
        "CREATE TABLE public.tenants (tenant_id TEXT PRIMARY KEY, tenant_name TEXT NOT NULL);",
    )
    .expect("write schema");
    schema_path
}

fn write_financials_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("financials.sql");
    fs::write(
        &schema_path,
        r#"
        CREATE TABLE public.financials (
            property_id TEXT NOT NULL,
            period TEXT NOT NULL,
            noi INTEGER NOT NULL,
            CONSTRAINT financials_property_period_key UNIQUE (property_id, period)
        );
        "#,
    )
    .expect("write financials schema");
    schema_path
}

fn write_child(dir: &Path, label: &str, script: &str) -> PathBuf {
    let child_path = dir.join(format!("{label}.py"));
    fs::write(&child_path, script).expect("write child");
    child_path
}

fn reserve_local_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("reserve local port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

const NOOP_CHILD: &str = r#"
import socket
import struct
import sys

host = sys.argv[1]
port = int(sys.argv[2])
diagnostics = sys.argv[3]

def fail(message):
    with open(diagnostics, "w", encoding="utf-8") as handle:
        handle.write(message)
    raise SystemExit(1)

def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)

def read_frame():
    tag = sock.recv(1)
    if not tag:
        fail("connection closed")
    length = struct.unpack("!I", sock.recv(4))[0]
    body = b""
    while len(body) < length - 4:
        chunk = sock.recv(length - 4 - len(body))
        if not chunk:
            fail("connection closed mid-frame")
        body += chunk
    return tag, body

def read_until_ready(expected):
    frames = []
    while True:
        tag, body = read_frame()
        frames.append((tag, body))
        if tag == b"Z":
            if body != expected:
                fail(f"expected ReadyForQuery {expected!r}, got {body!r}")
            return frames

sock = socket.create_connection((host, port), timeout=5)
body = struct.pack("!I", 196608) + b"user\0postgres\0database\0postgres\0\0"
sock.sendall(struct.pack("!I", len(body) + 4) + body)
read_until_ready(b"I")
framed(b"X")
sock.close()
"#;

const ABORTED_TRANSACTION_CHILD: &str = r#"
import socket
import struct
import sys

host = sys.argv[1]
port = int(sys.argv[2])
diagnostics = sys.argv[3]
TEXT_OID = 25

def fail(message):
    with open(diagnostics, "w", encoding="utf-8") as handle:
        handle.write(message)
    raise SystemExit(1)

def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)

def read_exact(length):
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            fail("connection closed mid-frame")
        body += chunk
    return body

def read_frame():
    tag = sock.recv(1)
    if not tag:
        fail("connection closed")
    length = struct.unpack("!I", read_exact(4))[0]
    return tag, read_exact(length - 4)

def read_until_ready(expected):
    frames = []
    while True:
        frame = read_frame()
        frames.append(frame)
        if frame[0] == b"Z":
            if frame[1] != expected:
                fail(f"expected ReadyForQuery {expected!r}, got {frame[1]!r}")
            return frames

def command_tag(frame):
    tag, body = frame
    if tag != b"C":
        fail(f"expected CommandComplete, got {tag!r}")
    return body[:-1].decode("utf-8")

def error_sqlstate(frame):
    tag, body = frame
    if tag != b"E":
        fail(f"expected ErrorResponse, got {tag!r}")
    offset = 0
    while offset < len(body):
        field = body[offset]
        offset += 1
        if field == 0:
            break
        end = body.index(b"\0", offset)
        value = body[offset:end].decode("utf-8")
        offset = end + 1
        if field == ord("C"):
            return value
    fail("missing SQLSTATE")

def query(sql, expected_ready, expected_command):
    framed(b"Q", sql.encode("utf-8") + b"\0")
    frames = read_until_ready(expected_ready)
    observed = command_tag(frames[0])
    if observed != expected_command:
        fail(f"expected command {expected_command!r}, got {observed!r}")

def parse_insert():
    body = (
        b"insert_tenant\0"
        + b"INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ($1, $2)\0"
        + struct.pack("!hII", 2, TEXT_OID, TEXT_OID)
    )
    framed(b"P", body)
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"1":
        fail(f"expected ParseComplete, got {frames[0][0]!r}")

def bind_execute(portal, tenant_id, tenant_name, expected_ready, expected_sqlstate=None):
    params = [tenant_id, tenant_name]
    body = portal.encode("utf-8") + b"\0" + b"insert_tenant\0" + struct.pack("!h", 0)
    body += struct.pack("!h", len(params))
    for value in params:
        if value is None:
            body += struct.pack("!i", -1)
        else:
            encoded = value.encode("utf-8")
            body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\0" + struct.pack("!I", 0))
    framed(b"S")
    frames = read_until_ready(expected_ready)
    if frames[0][0] != b"2":
        fail(f"expected BindComplete, got {frames[0][0]!r}")
    if expected_sqlstate is None:
        observed = command_tag(frames[1])
        if observed != "INSERT 0 1":
            fail(f"expected INSERT command, got {observed!r}")
    else:
        observed = error_sqlstate(frames[1])
        if observed != expected_sqlstate:
            fail(f"expected SQLSTATE {expected_sqlstate}, got {observed}")

sock = socket.create_connection((host, port), timeout=5)
startup = struct.pack("!I", 196608) + b"user\0postgres\0database\0postgres\0\0"
sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
read_until_ready(b"I")
parse_insert()
query("BEGIN", b"T", "BEGIN")
bind_execute("valid_tenant", "tenant-a", "Alpha", b"T")
bind_execute("invalid_tenant", "tenant-b", None, b"E", "23502")
query("COMMIT", b"I", "ROLLBACK")
framed(b"X")
sock.close()
"#;

const AUTOCOMMIT_CHILD: &str = r#"
import socket
import struct
import sys

host = sys.argv[1]
port = int(sys.argv[2])
diagnostics = sys.argv[3]
TEXT_OID = 25

def fail(message):
    with open(diagnostics, "w", encoding="utf-8") as handle:
        handle.write(message)
    raise SystemExit(1)

def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)

def read_exact(length):
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            fail("connection closed mid-frame")
        body += chunk
    return body

def read_frame():
    tag = sock.recv(1)
    if not tag:
        fail("connection closed")
    length = struct.unpack("!I", read_exact(4))[0]
    return tag, read_exact(length - 4)

def read_until_ready(expected):
    frames = []
    while True:
        frame = read_frame()
        frames.append(frame)
        if frame[0] == b"Z":
            if frame[1] != expected:
                fail(f"expected ReadyForQuery {expected!r}, got {frame[1]!r}")
            return frames

def command_tag(frame):
    tag, body = frame
    if tag != b"C":
        fail(f"expected CommandComplete, got {tag!r}")
    return body[:-1].decode("utf-8")

def error_sqlstate(frame):
    tag, body = frame
    if tag != b"E":
        fail(f"expected ErrorResponse, got {tag!r}")
    offset = 0
    while offset < len(body):
        field = body[offset]
        offset += 1
        if field == 0:
            break
        end = body.index(b"\0", offset)
        value = body[offset:end].decode("utf-8")
        offset = end + 1
        if field == ord("C"):
            return value
    fail("missing SQLSTATE")

def parse_insert():
    body = (
        b"insert_tenant\0"
        + b"INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ($1, $2)\0"
        + struct.pack("!hII", 2, TEXT_OID, TEXT_OID)
    )
    framed(b"P", body)
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"1":
        fail(f"expected ParseComplete, got {frames[0][0]!r}")

def bind_execute(portal, tenant_id, tenant_name, expected_sqlstate=None):
    params = [tenant_id, tenant_name]
    body = portal.encode("utf-8") + b"\0" + b"insert_tenant\0" + struct.pack("!h", 0)
    body += struct.pack("!h", len(params))
    for value in params:
        if value is None:
            body += struct.pack("!i", -1)
        else:
            encoded = value.encode("utf-8")
            body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\0" + struct.pack("!I", 0))
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"2":
        fail(f"expected BindComplete, got {frames[0][0]!r}")
    if expected_sqlstate is None:
        observed = command_tag(frames[1])
        if observed != "INSERT 0 1":
            fail(f"expected INSERT command, got {observed!r}")
    else:
        observed = error_sqlstate(frames[1])
        if observed != expected_sqlstate:
            fail(f"expected SQLSTATE {expected_sqlstate}, got {observed}")

sock = socket.create_connection((host, port), timeout=5)
startup = struct.pack("!I", 196608) + b"user\0postgres\0database\0postgres\0\0"
sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
read_until_ready(b"I")
parse_insert()
bind_execute("valid_tenant", "tenant-a", "Alpha")
bind_execute("invalid_tenant", "tenant-b", None, "23502")
framed(b"X")
sock.close()
"#;

const UPDATE_DELETE_CHILD: &str = r#"
import socket
import struct
import sys

host = sys.argv[1]
port = int(sys.argv[2])
diagnostics = sys.argv[3]
TEXT_OID = 25

def fail(message):
    with open(diagnostics, "w", encoding="utf-8") as handle:
        handle.write(message)
    raise SystemExit(1)

def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)

def read_exact(length):
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            fail("connection closed mid-frame")
        body += chunk
    return body

def read_frame():
    tag = sock.recv(1)
    if not tag:
        fail("connection closed")
    length = struct.unpack("!I", read_exact(4))[0]
    return tag, read_exact(length - 4)

def read_until_ready(expected):
    frames = []
    while True:
        frame = read_frame()
        frames.append(frame)
        if frame[0] == b"Z":
            if frame[1] != expected:
                fail(f"expected ReadyForQuery {expected!r}, got {frame[1]!r}")
            return frames

def command_tag(frame):
    tag, body = frame
    if tag != b"C":
        fail(f"expected CommandComplete, got {tag!r}")
    return body[:-1].decode("utf-8")

def error_sqlstate(frame):
    tag, body = frame
    if tag != b"E":
        fail(f"expected ErrorResponse, got {tag!r}")
    offset = 0
    while offset < len(body):
        field = body[offset]
        offset += 1
        if field == 0:
            break
        end = body.index(b"\0", offset)
        value = body[offset:end].decode("utf-8")
        offset = end + 1
        if field == ord("C"):
            return value
    fail("missing SQLSTATE")

def parse(name, sql, oids):
    body = name.encode("utf-8") + b"\0" + sql.encode("utf-8") + b"\0"
    body += struct.pack("!h", len(oids))
    for oid in oids:
        body += struct.pack("!I", oid)
    framed(b"P", body)
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"1":
        fail(f"expected ParseComplete for {name}, got {frames[0][0]!r}")

def bind_execute(statement, portal, params, expected_command=None, expected_sqlstate=None):
    body = portal.encode("utf-8") + b"\0" + statement.encode("utf-8") + b"\0"
    body += struct.pack("!h", 0)
    body += struct.pack("!h", len(params))
    for value in params:
        if value is None:
            body += struct.pack("!i", -1)
        else:
            encoded = value.encode("utf-8")
            body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\0" + struct.pack("!I", 0))
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"2":
        fail(f"expected BindComplete, got {frames[0][0]!r}")
    if expected_sqlstate is not None:
        observed = error_sqlstate(frames[1])
        if observed != expected_sqlstate:
            fail(f"expected SQLSTATE {expected_sqlstate}, got {observed}")
    else:
        observed = command_tag(frames[1])
        if observed != expected_command:
            fail(f"expected command {expected_command!r}, got {observed!r}")

sock = socket.create_connection((host, port), timeout=5)
startup = struct.pack("!I", 196608) + b"user\0postgres\0database\0postgres\0\0"
sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
read_until_ready(b"I")
parse("insert_tenant", "INSERT INTO public.tenants (tenant_id, tenant_name) VALUES ($1, $2)", [TEXT_OID, TEXT_OID])
parse("update_tenant", "UPDATE public.tenants SET tenant_name = $2 WHERE tenant_id = $1", [TEXT_OID, TEXT_OID])
parse("delete_tenant", "DELETE FROM public.tenants WHERE tenant_id = $1", [TEXT_OID])
bind_execute("insert_tenant", "insert_tenant_portal", ["tenant-a", "Alpha"], expected_command="INSERT 0 1")
bind_execute("update_tenant", "update_tenant_portal", ["tenant-a", "Beta"], expected_command="UPDATE 1")
bind_execute("update_tenant", "bad_update_tenant_portal", ["tenant-a", None], expected_sqlstate="23502")
bind_execute("delete_tenant", "delete_tenant_portal", ["tenant-a"], expected_command="DELETE 1")
framed(b"X")
sock.close()
"#;

const COMPOSITE_UPSERT_CHILD: &str = r#"
import socket
import struct
import sys

host = sys.argv[1]
port = int(sys.argv[2])
diagnostics = sys.argv[3]
TEXT_OID = 25
INT4_OID = 23

def fail(message):
    with open(diagnostics, "w", encoding="utf-8") as handle:
        handle.write(message)
    raise SystemExit(1)

def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)

def read_exact(length):
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            fail("connection closed mid-frame")
        body += chunk
    return body

def read_frame():
    tag = sock.recv(1)
    if not tag:
        fail("connection closed")
    length = struct.unpack("!I", read_exact(4))[0]
    return tag, read_exact(length - 4)

def read_until_ready(expected):
    frames = []
    while True:
        frame = read_frame()
        frames.append(frame)
        if frame[0] == b"Z":
            if frame[1] != expected:
                fail(f"expected ReadyForQuery {expected!r}, got {frame[1]!r}")
            return frames

def command_tag(frame):
    tag, body = frame
    if tag != b"C":
        fail(f"expected CommandComplete, got {tag!r}")
    return body[:-1].decode("utf-8")

def parse_upsert():
    body = (
        b"upsert_financials\0"
        + b"INSERT INTO public.financials (property_id, period, noi) VALUES ($1, $2, $3) ON CONFLICT (period, property_id) DO UPDATE SET noi = EXCLUDED.noi\0"
        + struct.pack("!hIII", 3, TEXT_OID, TEXT_OID, INT4_OID)
    )
    framed(b"P", body)
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"1":
        fail(f"expected ParseComplete, got {frames[0][0]!r}")

def bind_execute(portal, property_id, period, noi):
    params = [property_id, period, str(noi)]
    body = portal.encode("utf-8") + b"\0" + b"upsert_financials\0" + struct.pack("!h", 0)
    body += struct.pack("!h", len(params))
    for value in params:
        encoded = value.encode("utf-8")
        body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\0" + struct.pack("!I", 0))
    framed(b"S")
    frames = read_until_ready(b"I")
    if frames[0][0] != b"2":
        fail(f"expected BindComplete, got {frames[0][0]!r}")
    observed = command_tag(frames[1])
    if observed != "INSERT 0 1":
        fail(f"expected INSERT command, got {observed!r}")

sock = socket.create_connection((host, port), timeout=5)
startup = struct.pack("!I", 196608) + b"user\0postgres\0database\0postgres\0\0"
sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
read_until_ready(b"I")
parse_upsert()
bind_execute("insert_financials", "property-1", "2026-01", 100)
bind_execute("update_financials", "property-1", "2026-01", 125)
framed(b"X")
sock.close()
"#;
