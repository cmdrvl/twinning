use std::{
    env, fs,
    io::Write,
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

fn serve(test_id: &str, input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_twinning-mcp"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("twinning-mcp starts");
    let pid = child.id();
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(input)
        .expect("session writes");
    let output = child.wait_with_output().expect("server exits at EOF");
    record_session(test_id, input, pid, &output);
    output
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
            .join(format!("stub_integration-{}-{unix_ms}", std::process::id()));
        fs::create_dir_all(&path).expect("test artifacts directory is created");
        println!("ARTIFACTS: {}", path.display());
        path
    })
}

fn record_session(test_id: &str, input: &[u8], pid: u32, output: &Output) {
    let directory = artifacts_dir();
    fs::write(
        directory.join(format!("{test_id}.stdout.json")),
        &output.stdout,
    )
    .expect("server stdout artifact writes");
    fs::write(
        directory.join(format!("{test_id}.stderr.log")),
        &output.stderr,
    )
    .expect("server stderr artifact writes");

    let mut log = format!("[{test_id}] command=twinning-mcp pid={pid}\n");
    for line in input
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        match serde_json::from_slice::<Value>(line) {
            Ok(request) => log.push_str(&format!(
                "[{test_id}] request id={} method={}\n",
                request.get("id").unwrap_or(&Value::Null),
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("<invalid>")
            )),
            Err(error) => log.push_str(&format!("[{test_id}] request_parse_error={error}\n")),
        }
    }
    for line in output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        match serde_json::from_slice::<Value>(line) {
            Ok(response) => {
                let result_keys = response["result"]
                    .as_object()
                    .map(|result| result.keys().cloned().collect::<Vec<_>>().join(","))
                    .unwrap_or_default();
                log.push_str(&format!(
                    "[{test_id}] response id={} error_code={} result_keys={}\n",
                    response.get("id").unwrap_or(&Value::Null),
                    response["error"]["code"],
                    result_keys
                ));
            }
            Err(error) => log.push_str(&format!("[{test_id}] response_parse_error={error}\n")),
        }
    }
    fs::write(directory.join(format!("{test_id}.log")), &log).expect("session log writes");
    print!("{log}");
}

fn golden_tools() -> Value {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/tools_list.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("tools golden exists"))
        .expect("tools golden is valid JSON")
}

#[test]
fn s13_tools_list_matches_sorted_golden_with_closed_schemas() {
    let output = serve(
        "S13",
        b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n",
    );
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let response: Value = serde_json::from_slice(&output.stdout).expect("JSON-RPC response");
    let actual = &response["result"];
    assert_eq!(golden_tools(), *actual);

    let tools = actual["tools"].as_array().expect("tools array");
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect::<Vec<_>>();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        tools
            .iter()
            .all(|tool| { tool["inputSchema"]["additionalProperties"] == Value::Bool(false) })
    );
}

#[test]
fn s14_initialize_tools_list_is_a_jsonrpc_only_session() {
    let input = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\"}}\n",
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
    );
    let output = serve("S14", input.as_bytes());
    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let lines = output
        .stdout
        .split(|byte| *byte == b'\n')
        .collect::<Vec<_>>();
    assert_eq!(3, lines.len());
    assert!(lines[2].is_empty());
    let initialize: Value = serde_json::from_slice(lines[0]).expect("initialize response");
    let tools: Value = serde_json::from_slice(lines[1]).expect("tools/list response");
    assert_eq!(json!(1), initialize["id"]);
    assert_eq!("twinning-mcp", initialize["result"]["serverInfo"]["name"]);
    assert_eq!(json!(2), tools["id"]);
    assert_eq!(golden_tools(), tools["result"]);
    for line in lines.iter().filter(|line| !line.is_empty()) {
        let _: Value = serde_json::from_slice(line).expect("every stdout line is JSON-RPC JSON");
    }
}
