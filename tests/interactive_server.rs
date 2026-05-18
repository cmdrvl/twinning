#![forbid(unsafe_code)]
#![cfg(feature = "postgres")]

#[cfg(unix)]
mod unix {
    use std::{
        fs,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::{Path, PathBuf},
        process::{Child, Command, ExitStatus, Stdio},
        thread,
        time::{Duration, Instant},
    };

    use serde_json::Value;
    use tempfile::tempdir;

    fn twinning_bin() -> PathBuf {
        PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
    }

    #[test]
    fn interactive_serve_accepts_clients_until_signal_and_emits_final_artifacts() {
        let dir = tempdir().expect("tempdir");
        let schema_path = write_schema(dir.path());
        let report_path = dir.path().join("interactive.json");
        let snapshot_path = dir.path().join("interactive.twin");
        let port = reserve_local_port();

        let mut child = Command::new(twinning_bin())
            .args([
                "postgres",
                "--schema",
                schema_path.to_str().expect("schema path"),
                "--serve",
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--report",
                report_path.to_str().expect("report path"),
                "--snapshot",
                snapshot_path.to_str().expect("snapshot path"),
                "--json",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn twinning --serve");

        let mut client = connect_when_ready(port, &mut child);
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");

        write_startup_packet(&mut client);
        let startup_frames = read_until_ready(&mut client).expect("startup frames");
        assert!(startup_frames.iter().any(|frame| frame[0] == b'R'));
        assert_eq!(
            decode_ready_status(startup_frames.last().expect("startup ready")),
            b'I'
        );

        write_query_message(&mut client, "VACUUM");
        let refusal_frames = read_until_ready(&mut client).expect("refusal frames");
        assert_eq!(
            decode_error_sqlstate(refusal_frames.first().expect("error frame"))
                .expect("SQLSTATE field"),
            "0A000"
        );
        assert_eq!(
            decode_ready_status(refusal_frames.last().expect("ready frame")),
            b'I'
        );

        write_terminate_message(&mut client);
        drop(client);

        send_sigterm(child.id());
        let output = collect_child_output(child, Duration::from_secs(5));
        assert!(
            output.status.success(),
            "interactive server should exit cleanly: stdout={}; stderr={}; status={}",
            output.stdout,
            output.stderr,
            output.status
        );
        assert!(
            output.stderr.is_empty(),
            "interactive server should not emit stderr: {}",
            output.stderr
        );

        let stdout_json: Value = serde_json::from_str(&output.stdout).expect("stdout json");
        assert_eq!(stdout_json["version"], "twinning.v0");
        assert_eq!(stdout_json["mode"], "interactive");
        assert_eq!(stdout_json["outcome"], "READY");
        assert_eq!(stdout_json["port"], port);
        assert!(stdout_json.get("run").is_none());
        assert_eq!(
            stdout_json["snapshot"]["written_to"],
            snapshot_path.display().to_string()
        );

        let report_json: Value =
            serde_json::from_slice(&fs::read(&report_path).expect("read report"))
                .expect("report json");
        assert_eq!(report_json["mode"], "interactive");
        assert_eq!(report_json["outcome"], "READY");

        let snapshot_json: Value =
            serde_json::from_slice(&fs::read(&snapshot_path).expect("read snapshot"))
                .expect("snapshot json");
        assert_eq!(snapshot_json["mode"], "committed_state");
        assert_eq!(snapshot_json["table_rows"]["public.deals"], 0);
    }

    struct ChildOutput {
        status: ExitStatus,
        stdout: String,
        stderr: String,
    }

    fn write_schema(dir: &Path) -> PathBuf {
        let schema_path = dir.join("schema.sql");
        fs::write(
            &schema_path,
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT NOT NULL
            );
            "#,
        )
        .expect("write schema");
        schema_path
    }

    fn reserve_local_port() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local port");
        let port = listener.local_addr().expect("listener addr").port();
        drop(listener);
        port
    }

    fn connect_when_ready(port: u16, child: &mut Child) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => return stream,
                Err(error) => {
                    if let Some(status) = child.try_wait().expect("check server status") {
                        panic!("interactive server exited before accepting clients: {status}");
                    }
                    assert!(
                        Instant::now() < deadline,
                        "interactive server did not accept connections: {error}"
                    );
                    thread::sleep(Duration::from_millis(25));
                }
            }
        }
    }

    fn send_sigterm(pid: u32) {
        let status = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(status.success(), "kill -TERM should succeed: {status}");
    }

    fn collect_child_output(mut child: Child, timeout: Duration) -> ChildOutput {
        let mut stdout = child.stdout.take().expect("child stdout pipe");
        let mut stderr = child.stderr.take().expect("child stderr pipe");
        let status = wait_for_child(&mut child, timeout);

        let mut stdout_text = String::new();
        stdout
            .read_to_string(&mut stdout_text)
            .expect("read child stdout");
        let mut stderr_text = String::new();
        stderr
            .read_to_string(&mut stderr_text)
            .expect("read child stderr");

        ChildOutput {
            status,
            stdout: stdout_text,
            stderr: stderr_text,
        }
    }

    fn wait_for_child(child: &mut Child, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("wait for child") {
                return status;
            }
            if Instant::now() >= deadline {
                child.kill().expect("kill timed-out child");
                let status = child.wait().expect("wait for killed child");
                panic!("interactive server did not shut down after SIGTERM: {status}");
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn write_startup_packet(stream: &mut TcpStream) {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608_u32.to_be_bytes());
        for (name, value) in [("user", "postgres"), ("database", "postgres")] {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);

        let mut frame = Vec::new();
        frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(&body);
        stream.write_all(&frame).expect("write startup");
        stream.flush().expect("flush startup");
    }

    fn write_query_message(stream: &mut TcpStream, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        write_framed_message(stream, b'Q', &body);
    }

    fn write_terminate_message(stream: &mut TcpStream) {
        write_framed_message(stream, b'X', &[]);
    }

    fn write_framed_message(stream: &mut TcpStream, tag: u8, body: &[u8]) {
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(tag);
        frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(body);
        stream.write_all(&frame).expect("write message");
        stream.flush().expect("flush message");
    }

    fn read_until_ready(stream: &mut TcpStream) -> std::io::Result<Vec<Vec<u8>>> {
        let mut frames = Vec::new();
        loop {
            let frame = read_backend_frame(stream)?;
            let done = frame[0] == b'Z';
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    fn read_backend_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut tag = [0_u8; 1];
        stream.read_exact(&mut tag)?;
        let mut length_bytes = [0_u8; 4];
        stream.read_exact(&mut length_bytes)?;
        let length = i32::from_be_bytes(length_bytes);
        let mut body = vec![0_u8; length as usize - 4];
        stream.read_exact(&mut body)?;

        let mut frame = Vec::with_capacity(length as usize + 1);
        frame.push(tag[0]);
        frame.extend_from_slice(&length_bytes);
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    fn decode_ready_status(frame: &[u8]) -> u8 {
        assert_eq!(frame[0], b'Z');
        frame[5]
    }

    fn decode_error_sqlstate(frame: &[u8]) -> Result<String, &'static str> {
        assert_eq!(frame[0], b'E');
        let mut cursor = 5;
        while cursor < frame.len() {
            let field = frame[cursor];
            cursor += 1;
            if field == 0 {
                break;
            }
            let end = frame[cursor..]
                .iter()
                .position(|byte| *byte == 0)
                .ok_or("unterminated error field")?
                + cursor;
            if field == b'C' {
                return Ok(String::from_utf8(frame[cursor..end].to_vec()).expect("sqlstate utf-8"));
            }
            cursor = end + 1;
        }

        Err("missing SQLSTATE field")
    }
}
