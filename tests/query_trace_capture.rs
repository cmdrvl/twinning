#![forbid(unsafe_code)]
#![cfg(feature = "postgres")]

use std::{
    fs,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::tempdir;

const SECRET_BIND_VALUE: &str = "r3_bind_value_must_not_be_recorded_7f91";
const SCHEMA: &str = "CREATE TABLE public.deals (\n    deal_id TEXT PRIMARY KEY,\n    deal_name TEXT NOT NULL,\n    amount NUMERIC CHECK (amount >= 0)\n);\n";
const JOIN_SQL: &str = "SELECT d.deal_id FROM public.deals AS d JOIN public.deals AS other ON d.deal_id = other.deal_id";
const INSERT_SQL: &str =
    "INSERT INTO public.deals (deal_id, deal_name, amount) VALUES ($1, $2, $3)";

#[test]
fn refused_sql_capture_is_opt_in_and_never_records_accepted_sql_or_bind_values() {
    let captured = run_case(true);
    assert_eq!("twinning.query-trace.v1", captured.trace["version"]);
    assert_eq!("refused_text", captured.trace["policy"]["sql_text"]);
    assert_eq!(
        "redacted_hash_or_null",
        captured.trace["policy"]["bind_values"]
    );

    let events = captured.trace["events"].as_array().expect("trace events");
    let accepted = events
        .iter()
        .find(|event| event["sql_hash"].is_string() && event["result"]["outcome"] == "success")
        .expect("accepted SELECT is traced");
    assert!(accepted.get("sql_text").is_none());

    for (needle, sqlstate) in [("JOIN", "0A000"), ("SHOW ALL", "0A000"), ("$1", "23514")] {
        let event = events
            .iter()
            .find(|event| {
                event["sql_text"]
                    .as_str()
                    .is_some_and(|sql| sql.to_ascii_uppercase().contains(needle))
            })
            .unwrap_or_else(|| panic!("a refused {needle} statement is captured"));
        assert_eq!("refusal", event["result"]["outcome"]);
        assert_eq!(sqlstate, event["result"]["sqlstate"]);
        assert!(event["result"]["refusal_code"].as_str().is_some());
    }

    let captured_text = captured.trace.to_string();
    assert!(
        !captured_text.contains(SECRET_BIND_VALUE),
        "bind value leaked into trace"
    );
    assert!(!captured.stdout.contains(SECRET_BIND_VALUE));
    assert!(!captured.stderr.contains(SECRET_BIND_VALUE));
    for sql in [JOIN_SQL, "SHOW ALL", INSERT_SQL] {
        assert!(
            !captured.stdout.contains(sql),
            "refused SQL leaked to stdout"
        );
        assert!(
            !captured.stderr.contains(sql),
            "refused SQL leaked to stderr"
        );
    }

    let redacted = run_case(false);
    assert_eq!("twinning.query-trace.v0", redacted.trace["version"]);
    assert_eq!("redacted_hash_only", redacted.trace["policy"]["sql_text"]);
    assert_eq!(
        "redacted_hash_or_null",
        redacted.trace["policy"]["bind_values"]
    );
    assert!(
        redacted.trace["events"]
            .as_array()
            .expect("trace events")
            .iter()
            .all(|event| event.get("sql_text").is_none())
    );
    assert!(!redacted.trace.to_string().contains(SECRET_BIND_VALUE));
    assert!(!redacted.stdout.contains(SECRET_BIND_VALUE));
    assert!(!redacted.stderr.contains(SECRET_BIND_VALUE));
    for sql in [JOIN_SQL, "SHOW ALL", INSERT_SQL] {
        assert!(
            !redacted.stdout.contains(sql),
            "refused SQL leaked to stdout"
        );
        assert!(
            !redacted.stderr.contains(sql),
            "refused SQL leaked to stderr"
        );
    }
}

struct TraceRun {
    trace: Value,
    stdout: String,
    stderr: String,
}

fn run_case(capture_refused_statements: bool) -> TraceRun {
    let directory = tempdir().expect("test work directory");
    let schema_path = directory.path().join("schema.sql");
    let report_path = directory.path().join("report.json");
    let trace_path = directory.path().join("trace.json");
    fs::write(&schema_path, SCHEMA).expect("write schema");
    let port = free_port();

    let mut command = Command::new(env!("CARGO_BIN_EXE_twinning"));
    command
        .arg("postgres")
        .arg("--schema")
        .arg(&schema_path)
        .args(["--serve", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .arg("--report")
        .arg(&report_path)
        .arg("--query-trace")
        .arg(&trace_path);
    if capture_refused_statements {
        command.arg("--capture-refused-statements");
    }
    command
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().expect("spawn twinning");
    let mut child = ChildGuard::new(child);
    let mut client = connect_when_ready(port, &mut child);
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set pgwire read timeout");

    write_startup_packet(&mut client);
    let startup = read_until_ready(&mut client).expect("Postgres startup frames");
    assert_eq!(Some(b'I'), ready_status(&startup));

    write_query(&mut client, "SELECT version()");
    let accepted = read_until_ready(&mut client).expect("accepted query frames");
    assert_eq!(Some(b'I'), ready_status(&accepted));

    write_query(&mut client, JOIN_SQL);
    let join_frames = read_until_ready(&mut client).expect("join refusal frames");
    assert_eq!(Some("0A000"), sqlstate(&join_frames).as_deref());

    write_query(&mut client, "SHOW ALL");
    let show_frames = read_until_ready(&mut client).expect("SHOW ALL refusal frames");
    assert_eq!(Some("0A000"), sqlstate(&show_frames).as_deref());

    write_parse(&mut client, "r3_bind", INSERT_SQL, &[25, 25, 1700]);
    write_sync(&mut client);
    let parse_frames = read_until_ready(&mut client).expect("prepared statement frames");
    assert!(
        parse_frames
            .iter()
            .any(|frame| frame.first() == Some(&b'1'))
    );

    write_bind(
        &mut client,
        "r3_portal",
        "r3_bind",
        &[Some(SECRET_BIND_VALUE), Some("refused row"), Some("-1")],
    );
    write_execute(&mut client, "r3_portal");
    write_sync(&mut client);
    let bind_frames = read_until_ready(&mut client).expect("bound insert refusal frames");
    assert_eq!(Some("23514"), sqlstate(&bind_frames).as_deref());

    write_terminate(&mut client);
    drop(client);
    signal_terminate(child.child_mut().id());
    let output = child.finish();
    assert!(
        output.status.success(),
        "twinning should stop cleanly: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let trace_bytes = fs::read(&trace_path).expect("trace artifact exists");
    let trace: Value = serde_json::from_slice(&trace_bytes).expect("trace JSON");
    TraceRun {
        trace,
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("child guard owns process")
    }

    fn finish(&mut self) -> Output {
        self.0
            .take()
            .expect("child guard owns process")
            .wait_with_output()
            .expect("wait for twinning")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

fn connect_when_ready(port: u16, child: &mut ChildGuard) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.child_mut().try_wait().expect("poll twinning") {
            let output = child.finish();
            panic!(
                "twinning exited before pgwire readiness: status={status}; stdout={}; stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if let Ok(mut addresses) = ("127.0.0.1", port).to_socket_addrs()
            && let Some(address) = addresses.next()
            && let Ok(stream) = TcpStream::connect_timeout(&address, Duration::from_millis(50))
        {
            return stream;
        }
        assert!(
            Instant::now() < deadline,
            "pgwire listener readiness timeout"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("reserve ephemeral port")
        .port()
}

fn signal_terminate(pid: u32) {
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM to twinning");
    assert!(status.success(), "SIGTERM command failed");
}

fn write_startup_packet(stream: &mut TcpStream) {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608_i32.to_be_bytes());
    for (name, value) in [("user", "postgres"), ("database", "postgres")] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = Vec::new();
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);
    stream.write_all(&packet).expect("write startup packet");
}

fn write_query(stream: &mut TcpStream, sql: &str) {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    write_message(stream, b'Q', &body);
}

fn write_parse(stream: &mut TcpStream, name: &str, sql: &str, oids: &[u32]) {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    body.extend_from_slice(&(oids.len() as i16).to_be_bytes());
    for oid in oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    write_message(stream, b'P', &body);
}

fn write_bind(stream: &mut TcpStream, portal: &str, statement: &str, params: &[Option<&str>]) {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i16.to_be_bytes());
    body.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        if let Some(param) = param {
            body.extend_from_slice(&(param.len() as i32).to_be_bytes());
            body.extend_from_slice(param.as_bytes());
        } else {
            body.extend_from_slice(&(-1_i32).to_be_bytes());
        }
    }
    body.extend_from_slice(&0_i16.to_be_bytes());
    write_message(stream, b'B', &body);
}

fn write_execute(stream: &mut TcpStream, portal: &str) {
    let mut body = portal.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(&0_i32.to_be_bytes());
    write_message(stream, b'E', &body);
}

fn write_sync(stream: &mut TcpStream) {
    write_message(stream, b'S', &[]);
}

fn write_terminate(stream: &mut TcpStream) {
    write_message(stream, b'X', &[]);
}

fn write_message(stream: &mut TcpStream, tag: u8, body: &[u8]) {
    stream.write_all(&[tag]).expect("write message tag");
    stream
        .write_all(&((body.len() as i32 + 4).to_be_bytes()))
        .expect("write message length");
    stream.write_all(body).expect("write message body");
}

fn read_until_ready(stream: &mut TcpStream) -> std::io::Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    loop {
        let mut header = [0_u8; 5];
        stream.read_exact(&mut header)?;
        let payload_length = u32::from_be_bytes(header[1..5].try_into().expect("length field"));
        let mut frame = header.to_vec();
        frame.resize(1 + payload_length as usize, 0);
        stream.read_exact(&mut frame[5..])?;
        let ready = header[0] == b'Z';
        frames.push(frame);
        if ready {
            return Ok(frames);
        }
    }
}

fn ready_status(frames: &[Vec<u8>]) -> Option<u8> {
    frames
        .iter()
        .find(|frame| frame.first() == Some(&b'Z'))
        .and_then(|frame| frame.get(5).copied())
}

fn sqlstate(frames: &[Vec<u8>]) -> Option<String> {
    let frame = frames.iter().find(|frame| frame.first() == Some(&b'E'))?;
    let mut cursor = 5;
    while cursor < frame.len() {
        let field = *frame.get(cursor)?;
        cursor += 1;
        if field == 0 {
            break;
        }
        let end = frame[cursor..].iter().position(|byte| *byte == 0)? + cursor;
        if field == b'C' {
            return String::from_utf8(frame[cursor..end].to_vec()).ok();
        }
        cursor = end + 1;
    }
    None
}
