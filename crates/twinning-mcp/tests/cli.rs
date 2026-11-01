use std::{
    env, fs,
    io::Write,
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn run_cli(
    test_id: &str,
    args: &[&str],
    input: &[u8],
    assume_tty: bool,
    no_color: Option<bool>,
) -> (Output, Duration) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_twinning-mcp"));
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if assume_tty {
        command.env("TWINNING_MCP_ASSUME_TTY", "1");
    } else {
        command.env_remove("TWINNING_MCP_ASSUME_TTY");
    }
    match no_color {
        Some(true) => {
            command.env("NO_COLOR", "1");
        }
        Some(false) => {
            command.env_remove("NO_COLOR");
        }
        None => {}
    }

    let started = Instant::now();
    let mut child = command.spawn().expect("twinning-mcp starts");
    let pid = child.id();
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(input)
        .expect("test input writes");
    let output = child.wait_with_output().expect("twinning-mcp exits");
    let elapsed = started.elapsed();
    record_cli_run(test_id, args, input, pid, elapsed, &output);
    (output, elapsed)
}

fn artifacts_dir() -> &'static PathBuf {
    static ARTIFACTS: OnceLock<PathBuf> = OnceLock::new();
    ARTIFACTS.get_or_init(|| {
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock follows Unix epoch")
            .as_millis();
        let path = env::temp_dir()
            .join("twinning-mcp-tests")
            .join(format!("cli-{}-{unix_ms}", std::process::id()));
        fs::create_dir_all(&path).expect("test artifacts directory is created");
        println!("ARTIFACTS: {}", path.display());
        path
    })
}

fn record_cli_run(
    test_id: &str,
    args: &[&str],
    input: &[u8],
    pid: u32,
    elapsed: Duration,
    output: &Output,
) {
    let directory = artifacts_dir();
    fs::write(directory.join(format!("{test_id}.stdout")), &output.stdout)
        .expect("CLI stdout artifact writes");
    fs::write(directory.join(format!("{test_id}.stderr")), &output.stderr)
        .expect("CLI stderr artifact writes");
    let command = std::iter::once("twinning-mcp")
        .chain(args.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    let log = format!(
        "[{test_id}] command={command}\n[{test_id}] pid={pid} input_bytes={} elapsed_ms={} exit_code={:?}\n[{test_id}] stdout_bytes={} stderr_bytes={}\n",
        input.len(),
        elapsed.as_millis(),
        output.status.code(),
        output.stdout.len(),
        output.stderr.len()
    );
    fs::write(directory.join(format!("{test_id}.log")), &log).expect("CLI test log writes");
    print!("{log}");
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout contains one JSON value")
}

fn golden(relative_path: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(relative_path);
    let contents = std::fs::read_to_string(path).expect("checked-in golden exists");
    serde_json::from_str(&contents).expect("golden is valid JSON")
}

#[test]
fn u16_bare_terminal_prints_usage_and_pipe_serves_initialize() {
    let (terminal, _) = run_cli("U16-terminal", &[], b"", true, None);
    assert!(terminal.status.success());
    let usage = String::from_utf8(terminal.stdout).expect("usage is UTF-8");
    assert!(usage.contains("next: twinning-mcp setup"));
    assert!(usage.contains("next: twinning-mcp doctor"));

    let request = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}"#;
    let mut request_line = request.to_vec();
    request_line.push(b'\n');
    let (pipe, elapsed) = run_cli("U16-pipe", &[], &request_line, false, None);
    assert!(pipe.status.success());
    let response = stdout_json(&pipe);
    assert_eq!(jsonrpc_id(&response), 1);
    assert_eq!("2025-03-26", response["result"]["protocolVersion"]);
    assert!(
        elapsed < Duration::from_millis(500),
        "initialize round trip took {} ms",
        elapsed.as_millis()
    );
    eprintln!("[U16] initialize_round_trip_ms={}", elapsed.as_millis());
}

#[test]
fn u17_capabilities_match_the_golden_and_live_tool_list_names() {
    let (output, _) = run_cli("U17", &["capabilities", "--json"], b"", false, None);
    assert!(output.status.success());
    let actual = stdout_json(&output);
    let mut expected = golden("capabilities.json");
    expected["version"] = env!("CARGO_PKG_VERSION").into();
    assert_eq!(expected, actual);
    assert_eq!(env!("CARGO_PKG_VERSION"), actual["version"]);

    let tools = golden("tools_list.json");
    let names = tools["tools"]
        .as_array()
        .expect("golden tool list is an array")
        .iter()
        .map(|tool| tool["name"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        actual["tools"]
            .as_array()
            .expect("capability tools array")
            .clone()
    );
    assert!(output.stdout.ends_with(b"\n"));
}

#[test]
fn u18_cli_typos_show_clap_corrections_and_unknowns_show_next() {
    let cases: &[(&[&str], &str)] = &[
        (&["setpu"], "twinning-mcp setup"),
        (&["docter"], "twinning-mcp doctor"),
        (&["setup", "--dryrun"], "twinning-mcp setup --dry-run"),
        (&["--workdr", "X"], "twinning-mcp --workdir X"),
    ];
    for (index, (args, expected)) in cases.iter().enumerate() {
        let (output, _) = run_cli(&format!("U18-{}", index + 1), args, b"", false, None);
        assert_eq!(Some(1), output.status.code(), "args: {args:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(
            stderr.contains(expected),
            "args: {args:?}; stderr: {stderr}"
        );
        assert!(
            stderr
                .lines()
                .last()
                .is_some_and(|line| line.starts_with("next: "))
        );
    }

    let (unknown, _) = run_cli("U18-unknown", &["--zzzzzzzzzzzz"], b"", false, None);
    assert_eq!(Some(1), unknown.status.code());
    let stderr = String::from_utf8(unknown.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("next: twinning-mcp --help"));
}

#[test]
fn u19_cli_and_server_never_emit_ansi_with_no_color_set_or_unset() {
    let initialize = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n";
    let cases: &[(&[&str], &[u8], bool)] = &[
        (&[], initialize, false),
        (&["--help"], b"", false),
        (&["capabilities"], b"", false),
        (&["setpu"], b"", false),
        (&[], b"", true),
    ];
    let mut case_number = 0;
    for no_color in [None, Some(true)] {
        for (args, input, assume_tty) in cases {
            case_number += 1;
            let (output, _) = run_cli(
                &format!("U19-{case_number}"),
                args,
                input,
                *assume_tty,
                no_color,
            );
            assert!(
                !output.stdout.contains(&0x1b),
                "stdout contains ESC for {args:?}"
            );
            assert!(
                !output.stderr.contains(&0x1b),
                "stderr contains ESC for {args:?}"
            );
        }
    }
}

fn jsonrpc_id(value: &Value) -> &Value {
    &value["id"]
}
