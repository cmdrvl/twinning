#![forbid(unsafe_code)]
#![cfg(all(feature = "postgres", unix))]

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

const TEST_ID: &str = "M0E-SIMPLE-WRITES";
const SCHEMA: &str = "CREATE TABLE public.deals (\n    deal_id TEXT PRIMARY KEY,\n    deal_name TEXT NOT NULL,\n    amount NUMERIC CHECK (amount >= 0)\n);\n";

#[derive(Debug)]
struct QueryObservation {
    sqlstate: Option<String>,
    rows: Vec<Vec<Option<String>>>,
    command_tag: Option<String>,
    ready_status: u8,
    bytes_sent: usize,
    bytes_received: usize,
}

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
        self.child.as_mut().expect("twinning child")
    }

    fn id(&mut self) -> u32 {
        self.child_mut().id()
    }

    fn finish(mut self, timeout: Duration) -> ChildOutput {
        let mut child = self.child.take().expect("twinning child");
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll twinning child") {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().expect("kill twinning after shutdown timeout");
                let status = child.wait().expect("wait for killed twinning process");
                panic!("twinning did not exit after SIGTERM: {status}");
            }
            thread::sleep(Duration::from_millis(20));
        };
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .expect("twinning stdout pipe")
            .read_to_string(&mut stdout)
            .expect("read twinning stdout");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("twinning stderr pipe")
            .read_to_string(&mut stderr)
            .expect("read twinning stderr");
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
            if child.try_wait().expect("check twinning child").is_none() {
                child.kill().expect("kill twinning during test cleanup");
            }
            let _ = child.wait();
        }
    }
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
                .expect("open M0e test log"),
        }
    }

    fn line(&mut self, line: String) {
        println!("{line}");
        writeln!(self.file, "{line}").expect("write M0e test log");
        self.file.flush().expect("flush M0e test log");
    }
}

#[test]
fn r05_r06_simple_query_writes_share_kernel_and_report_constraints() {
    let artifacts = artifacts_dir();
    fs::create_dir_all(&artifacts).expect("create artifact directory");
    println!("ARTIFACTS: {}", artifacts.display());
    let mut log = ArtifactLog::new(&artifacts.join("M0E.log"));

    let schema_path = artifacts.join("schema.sql");
    let report_path = artifacts.join("report.json");
    let snapshot_path = artifacts.join("snapshot.twin");
    let trace_path = artifacts.join("trace.json");
    fs::write(&schema_path, SCHEMA).expect("write test schema");

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

    let mut client = connect_when_ready(port);
    let insert = query(
        &mut client,
        "INSERT INTO public.deals (deal_id, deal_name) VALUES ('simple-1', 'Alpha')",
    );
    log_observation(&mut log, "valid_insert", &insert);
    let select = query(
        &mut client,
        "SELECT deal_id, deal_name FROM public.deals WHERE deal_id = 'simple-1'",
    );
    log_observation(&mut log, "select_by_pk", &select);
    let duplicate = query(
        &mut client,
        "INSERT INTO public.deals (deal_id, deal_name) VALUES ('simple-1', 'Duplicate')",
    );
    log_observation(&mut log, "duplicate_primary_key", &duplicate);
    let check = query(
        &mut client,
        "INSERT INTO public.deals (deal_id, deal_name, amount) VALUES ('bad-check', 'Negative', -1)",
    );
    log_observation(&mut log, "check_violation", &check);
    terminate_connection(client);

    let mut transaction_client = connect_when_ready(port);
    let begin = query(&mut transaction_client, "BEGIN");
    log_observation(&mut log, "transaction_begin", &begin);
    let transaction_insert = query(
        &mut transaction_client,
        "INSERT INTO public.deals (deal_id, deal_name) VALUES ('rolled-back', 'Transient')",
    );
    log_observation(&mut log, "transaction_insert", &transaction_insert);
    let rollback = query(&mut transaction_client, "ROLLBACK");
    log_observation(&mut log, "transaction_rollback", &rollback);
    terminate_connection(transaction_client);

    let mut writer_probe = connect_when_ready(port);
    let writer_begin = query(&mut writer_probe, "BEGIN");
    log_observation(&mut log, "writer_released_begin", &writer_begin);
    let writer_rollback = query(&mut writer_probe, "ROLLBACK");
    log_observation(&mut log, "writer_released_rollback", &writer_rollback);
    terminate_connection(writer_probe);

    let mut refusal_client = connect_when_ready(port);
    let hstore_probe = query(
        &mut refusal_client,
        "SELECT t.oid, typarray\nFROM pg_type t JOIN pg_namespace ns\n    ON typnamespace = ns.oid\nWHERE typname = 'hstore';\n",
    );
    log_observation(&mut log, "sqlalchemy_hstore_probe", &hstore_probe);
    let after_hstore_probe = query(&mut refusal_client, "SELECT version()");
    log_observation(&mut log, "query_after_hstore_probe", &after_hstore_probe);
    let unsupported = query(&mut refusal_client, "SHOW ALL");
    log_observation(&mut log, "unsupported_show_all", &unsupported);
    let after_refusal = query(&mut refusal_client, "SELECT version()");
    log_observation(&mut log, "query_after_refusal", &after_refusal);
    terminate_connection(refusal_client);

    send_sigterm(server_pid);
    let server_output = server.finish(Duration::from_secs(15));
    log.line(format!(
        "[{TEST_ID}] event=server_finished pid={server_pid} port={port} exit_code={:?} stdout_tail={:?} stderr_tail={:?}",
        server_output.status.code(),
        tail(&server_output.stdout, 2048),
        tail(&server_output.stderr, 2048),
    ));

    assert!(report_path.exists(), "interactive report was not written");
    assert!(
        trace_path.exists(),
        "interactive query trace was not written"
    );
    let report: Value = serde_json::from_slice(&fs::read(&report_path).expect("read report"))
        .expect("parse report JSON");
    let trace: Value = serde_json::from_slice(&fs::read(&trace_path).expect("read trace"))
        .expect("parse query trace JSON");
    let events = trace["events"].as_array().expect("trace event list");
    let sqlstates = events
        .iter()
        .filter_map(|event| event["result"]["sqlstate"].as_str())
        .collect::<Vec<_>>();
    log.line(format!(
        "[{TEST_ID}] event=report_summary outcome={} rows={} constraints={} request_count={} trace_sqlstates={sqlstates:?}",
        report["outcome"],
        report["tables"]["public.deals"]["rows"],
        report["constraints"],
        events.len(),
    ));

    assert!(
        server_output.status.success(),
        "interactive server exit={:?}; stdout_tail={} stderr_tail={}",
        server_output.status.code(),
        tail(&server_output.stdout, 2048),
        tail(&server_output.stderr, 2048)
    );
    assert_eq!(insert.sqlstate, None, "simple-query insert must succeed");
    assert_eq!(insert.command_tag.as_deref(), Some("INSERT 0 1"));
    assert_eq!(select.sqlstate, None, "simple-query SELECT must succeed");
    assert_eq!(
        select.rows,
        vec![vec![Some("simple-1".into()), Some("Alpha".into())]]
    );
    assert_eq!(duplicate.sqlstate.as_deref(), Some("23505"));
    assert_eq!(check.sqlstate.as_deref(), Some("23514"));
    assert_eq!(begin.ready_status, b'T');
    assert_eq!(transaction_insert.sqlstate, None);
    assert_eq!(rollback.command_tag.as_deref(), Some("ROLLBACK"));
    assert_eq!(
        writer_begin.sqlstate, None,
        "rollback should release writer"
    );
    assert_eq!(writer_rollback.command_tag.as_deref(), Some("ROLLBACK"));
    assert_eq!(
        hstore_probe.sqlstate, None,
        "hstore absence probe should report no hstore rows"
    );
    assert!(hstore_probe.rows.is_empty());
    assert_eq!(after_hstore_probe.sqlstate, None);
    assert_eq!(unsupported.sqlstate.as_deref(), Some("0A000"));
    assert_eq!(
        after_refusal.sqlstate, None,
        "connection should survive refusal"
    );
    assert_eq!(
        after_refusal.rows,
        vec![vec![Some("PostgreSQL 16.0 (twinning)".into())]]
    );
    assert_eq!(report["outcome"], "READY");
    assert_eq!(report["tables"]["public.deals"]["rows"], 1);
    assert_eq!(report["constraints"]["check_violations"], 1);
    assert_eq!(report["constraints"]["unique_violations"], 1);
    assert!(sqlstates.contains(&"23514") && sqlstates.contains(&"23505"));
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
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve free local port");
    let port = listener.local_addr().expect("read local port").port();
    drop(listener);
    port
}

fn wait_until_listener_bound(port: u16, child: &mut Child, log: &mut ArtifactLog) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().expect("poll server startup") {
            panic!("twinning exited before listening: {status}");
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
            Err(error) => panic!("probe port {port}: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "listener did not bind port {port}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn connect_when_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("set client read timeout");
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .expect("set client write timeout");
                write_startup_packet(&mut stream);
                let startup = read_until_ready(&mut stream).expect("read startup response");
                assert!(startup.iter().any(|frame| frame.first() == Some(&b'R')));
                assert_eq!(ready_status(startup.last().expect("startup ready")), b'I');
                return stream;
            }
            Err(error) => {
                assert!(Instant::now() < deadline, "connect to {port}: {error}");
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn write_startup_packet(stream: &mut TcpStream) -> usize {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608_u32.to_be_bytes());
    for (name, value) in [("user", "postgres"), ("database", "postgres")] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = Vec::with_capacity(body.len() + 4);
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);
    stream.write_all(&packet).expect("write startup packet");
    packet.len()
}

fn query(stream: &mut TcpStream, sql: &str) -> QueryObservation {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let mut packet = Vec::with_capacity(body.len() + 5);
    packet.push(b'Q');
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);
    stream
        .write_all(&packet)
        .expect("write simple Query message");
    stream.flush().expect("flush simple Query message");
    let frames = read_until_ready(stream).expect("read simple Query response");
    let sqlstate = frames
        .iter()
        .find(|frame| frame.first() == Some(&b'E'))
        .and_then(|frame| error_field(frame, b'C'));
    let rows = frames
        .iter()
        .filter(|frame| frame.first() == Some(&b'D'))
        .map(|frame| decode_data_row(frame))
        .collect();
    let command_tag = frames
        .iter()
        .find(|frame| frame.first() == Some(&b'C'))
        .map(|frame| decode_command_tag(frame));
    QueryObservation {
        sqlstate,
        rows,
        command_tag,
        ready_status: ready_status(frames.last().expect("ReadyForQuery frame")),
        bytes_sent: packet.len(),
        bytes_received: frames.iter().map(Vec::len).sum(),
    }
}

fn terminate_connection(mut stream: TcpStream) {
    let packet = [b'X', 0, 0, 0, 4];
    stream.write_all(&packet).expect("write Terminate message");
}

fn read_until_ready(stream: &mut TcpStream) -> std::io::Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    loop {
        let mut header = [0_u8; 5];
        stream.read_exact(&mut header)?;
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        if length < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid backend frame length {length}"),
            ));
        }
        let mut frame = header.to_vec();
        frame.resize(1 + length as usize, 0);
        stream.read_exact(&mut frame[5..])?;
        let ready = header[0] == b'Z';
        frames.push(frame);
        if ready {
            return Ok(frames);
        }
    }
}

fn ready_status(frame: &[u8]) -> u8 {
    assert_eq!(frame[0], b'Z');
    frame[5]
}

fn decode_data_row(frame: &[u8]) -> Vec<Option<String>> {
    let body = &frame[5..];
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]);
        cursor += 4;
        if length < 0 {
            values.push(None);
        } else {
            let length = length as usize;
            values.push(Some(
                String::from_utf8(body[cursor..cursor + length].to_vec())
                    .expect("decode data row value"),
            ));
            cursor += length;
        }
    }
    values
}

fn decode_command_tag(frame: &[u8]) -> String {
    assert_eq!(frame[0], b'C');
    String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("decode command tag")
}

fn error_field(frame: &[u8], wanted: u8) -> Option<String> {
    assert_eq!(frame[0], b'E');
    let body = &frame[5..];
    let mut cursor = 0;
    while body[cursor] != 0 {
        let code = body[cursor];
        cursor += 1;
        let length = body[cursor..].iter().position(|byte| *byte == 0)?;
        let value = String::from_utf8(body[cursor..cursor + length].to_vec()).ok()?;
        cursor += length + 1;
        if code == wanted {
            return Some(value);
        }
    }
    None
}

fn log_observation(log: &mut ArtifactLog, case: &str, observation: &QueryObservation) {
    log.line(format!(
        "[{TEST_ID}] event=query case={case} sqlstate={:?} rows={:?} command_tag={:?} ready_status={} bytes_sent={} bytes_received={}",
        observation.sqlstate,
        observation.rows,
        observation.command_tag,
        observation.ready_status as char,
        observation.bytes_sent,
        observation.bytes_received,
    ));
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
