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
        std::env::var_os("TWINNING_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_twinning")))
    }

    #[test]
    fn interactive_serve_accepts_clients_until_signal_and_emits_final_artifacts() {
        let dir = tempdir().expect("tempdir");
        let schema_path = write_schema(dir.path());
        let report_path = dir.path().join("interactive.json");
        let snapshot_path = dir.path().join("interactive.twin");
        let port = reserve_local_port();

        let mut child = spawn_locked(
            Command::new(twinning_bin())
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
                .stderr(Stdio::piped()),
        )
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

    #[test]
    fn m0b_half_startup_disconnect_does_not_fail_shutdown_or_skip_artifacts() {
        let dir = tempdir().expect("tempdir");
        let schema_path = write_schema(dir.path());
        let report_path = dir.path().join("m0b-half-report.json");
        let trace_path = dir.path().join("m0b-half-trace.json");
        let port = reserve_local_port();
        let (mut child, command_line) =
            spawn_interactive_server(&schema_path, &report_path, &trace_path, port);
        let pid = child.id();
        let mut bytes_sent = 0usize;
        let mut bytes_received = 0usize;
        println!("[M0B-HALF] event=started command={command_line:?} pid={pid} port={port}");

        let mut broken_client = connect_when_ready(port, child.child_mut());
        broken_client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set half-startup read timeout");
        let partial_startup = [0_u8, 0, 0, 32, 0, 3];
        broken_client
            .write_all(&partial_startup)
            .expect("write partial startup packet");
        bytes_sent += partial_startup.len();
        drop(broken_client);

        let mut good_client = connect_when_ready(port, child.child_mut());
        good_client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set good-client read timeout");
        bytes_sent += write_startup_packet(&mut good_client);
        let startup_frames = read_until_ready(&mut good_client).expect("good client startup");
        bytes_received += frame_bytes(&startup_frames);
        let startup_ready = startup_frames
            .last()
            .is_some_and(|frame| frame.first() == Some(&b'Z'));

        bytes_sent += write_terminate_message(&mut good_client);
        bytes_received += read_server_close(&mut good_client).expect("good connection close");
        drop(good_client);

        send_sigterm(pid);
        let output = child.finish(Duration::from_secs(5));
        let exit_code = output.status.code();
        println!(
            "[M0B-HALF] event=finished command={command_line:?} pid={pid} port={port} bytes_sent={bytes_sent} bytes_received={bytes_received} exit_code={exit_code:?} stdout_tail={:?} stderr_tail={:?}",
            tail(&output.stdout, 2048),
            tail(&output.stderr, 2048)
        );

        assert!(
            output.status.success(),
            "M0b server should exit cleanly after a failed connection: exit={exit_code:?} stdout={} stderr={}",
            tail(&output.stdout, 2048),
            tail(&output.stderr, 2048)
        );
        assert!(startup_ready, "good client should reach ReadyForQuery");
        assert_eq!(
            output.stderr.lines().count(),
            1,
            "failed connection should produce one stderr line: {}",
            output.stderr
        );
        assert!(
            output.stderr.contains("connection_error"),
            "stderr should identify the isolated connection error: {}",
            output.stderr
        );
        assert!(report_path.exists(), "final report should be written");
        assert!(trace_path.exists(), "final query trace should be written");

        let report_json: Value =
            serde_json::from_slice(&fs::read(&report_path).expect("read report"))
                .expect("report json");
        assert_eq!(report_json["outcome"], "READY");
        let trace_json: Value = serde_json::from_slice(&fs::read(&trace_path).expect("read trace"))
            .expect("trace json");
        assert_eq!(trace_json["version"], "twinning.query-trace.v0");
    }

    #[test]
    fn m0b_psql_begin_rollback_then_sigterm_emits_artifacts() {
        let dir = tempdir().expect("tempdir");
        let schema_path = PathBuf::from("tests/fixtures/bootstrap_cli/schema.sql");
        let report_path = dir.path().join("m0b-psql-report.json");
        let trace_path = dir.path().join("m0b-psql-trace.json");
        let port = reserve_local_port();
        let (mut server, server_command) =
            spawn_interactive_server(&schema_path, &report_path, &trace_path, port);
        let server_pid = server.id();

        // Observe the listening socket without creating a client connection.
        wait_until_listener_bound(port, server.child_mut());

        let psql_args = [
            format!("postgresql://u@127.0.0.1:{port}/db"),
            "-v".to_owned(),
            "ON_ERROR_STOP=1".to_owned(),
            "-c".to_owned(),
            "BEGIN".to_owned(),
            "-c".to_owned(),
            "ROLLBACK".to_owned(),
        ];
        let psql_command = format!("psql {:?}", psql_args);
        let psql = spawn_locked(
            Command::new("psql")
                .args(&psql_args)
                .env_remove("PGSSLMODE")
                .env_remove("PGGSSENCMODE")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )
        .expect("spawn psql (PostgreSQL default SSL mode)");
        let mut psql = ProcessGuard::new(psql);
        let psql_pid = psql.id();
        println!(
            "[M0B-PSQL] event=started server_command={server_command:?} server_pid={server_pid} psql_command={psql_command:?} psql_pid={psql_pid} port={port} readiness=listener_bind probe_bytes_sent=0 probe_bytes_received=0 sslmode=libpq_default"
        );

        let psql_status = psql.wait_with_timeout(Duration::from_secs(15));
        // Signal the twin as soon as psql exits, before reading or logging its output.
        send_sigterm(server_pid);
        let (psql_stdout, psql_stderr) = psql.read_output();
        let output = server.finish(Duration::from_secs(5));
        let exit_code = output.status.code();
        println!(
            "[M0B-PSQL] event=finished server_command={server_command:?} server_pid={server_pid} psql_command={psql_command:?} psql_pid={psql_pid} port={port} psql_exit_code={:?} psql_stdout_tail={:?} psql_stderr_tail={:?} server_exit_code={exit_code:?} server_stdout_tail={:?} server_stderr_tail={:?}",
            psql_status.code(),
            tail(&psql_stdout, 2048),
            tail(&psql_stderr, 2048),
            tail(&output.stdout, 2048),
            tail(&output.stderr, 2048)
        );

        assert!(
            psql_status.success(),
            "psql -c begin -c rollback should succeed: exit={:?} stdout={} stderr={}",
            psql_status.code(),
            psql_stdout,
            psql_stderr
        );
        assert!(
            output.status.success(),
            "server should exit cleanly immediately after psql disconnect: exit={exit_code:?} stdout={} stderr={}",
            tail(&output.stdout, 2048),
            tail(&output.stderr, 2048)
        );
        assert!(report_path.exists(), "final report should be written");
        assert!(trace_path.exists(), "final query trace should be written");
        let report_json: Value =
            serde_json::from_slice(&fs::read(&report_path).expect("read report"))
                .expect("report json");
        assert_eq!(report_json["outcome"], "READY");
    }

    #[test]
    fn m0b_disconnect_after_begin_releases_writer_for_next_client() {
        let dir = tempdir().expect("tempdir");
        let schema_path = write_schema(dir.path());
        let report_path = dir.path().join("m0b-writer-report.json");
        let trace_path = dir.path().join("m0b-writer-trace.json");
        let port = reserve_local_port();
        let (mut child, command_line) =
            spawn_interactive_server(&schema_path, &report_path, &trace_path, port);
        let pid = child.id();
        let mut bytes_sent = 0usize;
        let mut bytes_received = 0usize;
        println!("[M0B-WRITER] event=started command={command_line:?} pid={pid} port={port}");

        let mut first_client = connect_when_ready(port, child.child_mut());
        first_client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set first-client read timeout");
        bytes_sent += write_startup_packet(&mut first_client);
        let first_startup = read_until_ready(&mut first_client).expect("first startup");
        bytes_received += frame_bytes(&first_startup);
        bytes_sent += write_query_message(&mut first_client, "BEGIN");
        let first_begin = read_until_ready(&mut first_client).expect("first BEGIN");
        bytes_received += frame_bytes(&first_begin);
        let first_begin_ready = first_begin
            .last()
            .is_some_and(|frame| decode_ready_status(frame) == b'T');
        bytes_sent += write_terminate_message(&mut first_client);
        bytes_received += read_server_close(&mut first_client).expect("first connection close");
        drop(first_client);

        let mut second_client = connect_when_ready(port, child.child_mut());
        second_client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set second-client read timeout");
        bytes_sent += write_startup_packet(&mut second_client);
        let second_startup = read_until_ready(&mut second_client).expect("second startup");
        bytes_received += frame_bytes(&second_startup);
        bytes_sent += write_query_message(&mut second_client, "BEGIN");
        let second_begin = read_until_ready(&mut second_client).expect("second BEGIN");
        bytes_received += frame_bytes(&second_begin);
        let second_begin_tag = second_begin.first().map(|frame| frame[0]);
        let second_begin_sqlstate = second_begin
            .first()
            .filter(|frame| frame.first() == Some(&b'E'))
            .and_then(|frame| decode_error_sqlstate(frame).ok());
        let second_begin_ready = second_begin.last().map(|frame| decode_ready_status(frame));

        bytes_sent += write_terminate_message(&mut second_client);
        bytes_received += read_server_close(&mut second_client).expect("second connection close");
        drop(second_client);
        send_sigterm(pid);
        let output = child.finish(Duration::from_secs(5));
        let exit_code = output.status.code();
        println!(
            "[M0B-WRITER] event=finished command={command_line:?} pid={pid} port={port} bytes_sent={bytes_sent} bytes_received={bytes_received} first_begin_ready={first_begin_ready} second_begin_tag={second_begin_tag:?} second_begin_sqlstate={second_begin_sqlstate:?} second_begin_ready={second_begin_ready:?} exit_code={exit_code:?} stdout_tail={:?} stderr_tail={:?}",
            tail(&output.stdout, 2048),
            tail(&output.stderr, 2048)
        );

        assert!(
            first_begin_ready,
            "first client should enter a transaction before disconnecting"
        );
        assert_eq!(second_begin_tag, Some(b'C'));
        assert_eq!(second_begin_sqlstate, None);
        assert_eq!(second_begin_ready, Some(b'T'));
        assert!(output.status.success(), "server exit {exit_code:?}");
        assert!(report_path.exists(), "final report should be written");
        assert!(trace_path.exists(), "final query trace should be written");
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
            self.child.as_mut().expect("server child")
        }

        fn id(&mut self) -> u32 {
            self.child_mut().id()
        }

        fn finish(mut self, timeout: Duration) -> ChildOutput {
            collect_child_output(self.child.take().expect("server child"), timeout)
        }

        fn wait_with_timeout(&mut self, timeout: Duration) -> ExitStatus {
            wait_for_child(self.child_mut(), timeout)
        }

        fn read_output(&mut self) -> (String, String) {
            let child = self.child_mut();
            let mut stdout = child.stdout.take().expect("child stdout pipe");
            let mut stderr = child.stderr.take().expect("child stderr pipe");
            let mut stdout_text = String::new();
            stdout
                .read_to_string(&mut stdout_text)
                .expect("read child stdout");
            let mut stderr_text = String::new();
            stderr
                .read_to_string(&mut stderr_text)
                .expect("read child stderr");
            (stdout_text, stderr_text)
        }
    }

    impl Drop for ProcessGuard {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let status_at_drop = child.try_wait().expect("check server status");
                if status_at_drop.is_none() {
                    child.kill().expect("kill server during cleanup");
                }
                let _ = child.wait();
                if thread::panicking() {
                    // Surface what the server saw when a test fails, so a flaky
                    // failure is diagnosable from the test log alone.
                    let mut stdout_text = String::new();
                    if let Some(mut stdout) = child.stdout.take() {
                        let _ = stdout.read_to_string(&mut stdout_text);
                    }
                    let mut stderr_text = String::new();
                    if let Some(mut stderr) = child.stderr.take() {
                        let _ = stderr.read_to_string(&mut stderr_text);
                    }
                    eprintln!(
                        "[ProcessGuard] test failed; server status_at_drop={status_at_drop:?} stdout={stdout_text:?} stderr={stderr_text:?}"
                    );
                }
            }
        }
    }

    fn spawn_interactive_server(
        schema_path: &Path,
        report_path: &Path,
        trace_path: &Path,
        port: u16,
    ) -> (ProcessGuard, String) {
        let args = [
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
            "--query-trace".to_owned(),
            trace_path.display().to_string(),
            "--json".to_owned(),
        ];
        let command_line = format!("{:?} {:?}", twinning_bin(), args);
        let child = spawn_locked(
            Command::new(twinning_bin())
                .args(&args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )
        .expect("spawn twinning --serve");
        (ProcessGuard::new(child), command_line)
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

    /// Serializes `fork`/`exec` against the window in which `reserve_local_port`
    /// holds its listener open.
    ///
    /// The tests in this file run in parallel threads of one process. A socket is
    /// not close-on-exec until after it is created on some platforms (macOS), so a
    /// child forked by another test thread in that window inherits the reservation
    /// listener and keeps the port bound after the reserving test has dropped it.
    /// The server this test then starts fails with `listener_bind: Address already
    /// in use` and exits 2, while the test's client connects to the orphaned
    /// listener and is reset when its holder exits. Every spawn takes this lock.
    static FORK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn fork_guard() -> std::sync::MutexGuard<'static, ()> {
        FORK_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn spawn_locked(command: &mut Command) -> std::io::Result<Child> {
        let _guard = fork_guard();
        command.spawn()
    }

    fn reserve_local_port() -> u16 {
        let _guard = fork_guard();
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

    fn wait_until_listener_bound(port: u16, child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => drop(listener),
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => return,
                Err(error) => assert_eq!(
                    error.kind(),
                    std::io::ErrorKind::AddrInUse,
                    "listener bind probe failed for port {port}: {error}"
                ),
            }
            let status = child.try_wait().expect("check server status");
            assert!(
                status.is_none(),
                "interactive server exited before binding port {port}: {status:?}"
            );
            assert!(
                Instant::now() < deadline,
                "interactive server did not bind port {port} before timeout"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn send_sigterm(pid: u32) {
        let status = spawn_locked(Command::new("kill").args(["-TERM", &pid.to_string()]))
            .expect("spawn kill")
            .wait()
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

        let mut frame = Vec::new();
        frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(&body);
        stream.write_all(&frame).expect("write startup");
        stream.flush().expect("flush startup");
        frame.len()
    }

    fn write_query_message(stream: &mut TcpStream, sql: &str) -> usize {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        write_framed_message(stream, b'Q', &body)
    }

    fn write_terminate_message(stream: &mut TcpStream) -> usize {
        write_framed_message(stream, b'X', &[])
    }

    fn write_framed_message(stream: &mut TcpStream, tag: u8, body: &[u8]) -> usize {
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(tag);
        frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(body);
        stream.write_all(&frame).expect("write message");
        stream.flush().expect("flush message");
        frame.len()
    }

    fn frame_bytes(frames: &[Vec<u8>]) -> usize {
        frames.iter().map(Vec::len).sum()
    }

    fn read_server_close(stream: &mut TcpStream) -> std::io::Result<usize> {
        let mut bytes = [0_u8; 256];
        let mut total = 0;
        loop {
            match stream.read(&mut bytes) {
                Ok(0) => return Ok(total),
                Ok(count) => total += count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn tail(text: &str, max_bytes: usize) -> String {
        let start = text.len().saturating_sub(max_bytes);
        let start = text
            .char_indices()
            .find(|(index, _)| *index >= start)
            .map_or(0, |(index, _)| index);
        text[start..].to_owned()
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
