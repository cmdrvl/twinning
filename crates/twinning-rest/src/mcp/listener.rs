//! Axum HTTP listener for the MCP JSON-RPC twin.

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::Response,
    routing::post,
};
use serde_json::{Value as JsonValue, json};
#[cfg(unix)]
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use tokio::{net::TcpListener, runtime::Builder as TokioRuntimeBuilder};

use crate::{
    auth::{AuthExtract, RestAuthMode, check_auth},
    refusal::{self, RefusalEnvelope, RefusalResult},
    runtime::Execution,
};

use super::{
    catalog::{McpCatalog, load_mcp_catalog_from_manifest, load_mcp_catalog_from_server},
    dispatcher::{
        JSONRPC_INVALID_REQUEST, JSONRPC_PARSE_ERROR, JSONRPC_SERVER_ERROR, JsonRpcError,
        JsonRpcRequest, JsonRpcResponse, ToolExecutability, classify_tool_executability, dispatch,
    },
    report::{McpProtocolVersionLog, McpReport, McpRequestLog, McpSessionLog, McpToolCallOutcome},
    version::negotiate_initialize_protocol,
};

const MAX_REQUEST_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConfig {
    pub source: McpCatalogInput,
    pub host: String,
    pub port: u16,
    pub auth_mode: RestAuthMode,
    pub stdio: bool,
    pub run_command: Option<String>,
    pub report_path: Option<PathBuf>,
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpCatalogInput {
    LiveServer { command: String },
    Manifest { path: PathBuf },
}

struct McpSharedState {
    catalog: Arc<McpCatalog>,
    auth_mode: RestAuthMode,
    session_log: Arc<Mutex<McpSessionLog>>,
}

struct RunningMcpServer {
    addr: SocketAddr,
    shutdown_tx: mpsc::Sender<()>,
    handle: thread::JoinHandle<Result<(), String>>,
    state: Arc<McpSharedState>,
}

pub fn run(config: McpConfig) -> Result<Execution, Box<dyn std::error::Error>> {
    let json_mode = config.json;
    let rendered = match run_inner(config) {
        Ok(execution) => execution,
        Err(refusal) => Execution {
            exit_code: 2,
            stdout: refusal.render(json_mode)?,
        },
    };

    Ok(rendered)
}

fn run_inner(config: McpConfig) -> RefusalResult<Execution> {
    let Some(run_command) = config.run_command.clone() else {
        let server = start_server(&config)?;
        wait_for_shutdown_signal()?;
        let state = shutdown_and_join(server)?;
        write_mcp_report_if_requested(&config, &state)?;
        return Ok(Execution {
            exit_code: 0,
            stdout: String::from("MCP twin stopped.\n"),
        });
    };

    let server = start_server(&config)?;
    let base_url = format!("http://{}", server.addr);
    let child = run_child_with_base_url(&run_command, &base_url)?;
    let state = shutdown_and_join(server)?;
    write_mcp_report_if_requested(&config, &state)?;

    let mut stdout = format!(
        "MCP twin child command completed: exit_code={:?} signal={:?} timed_out=false\n",
        child.code(),
        exit_signal(&child)
    );
    if config.json {
        stdout = serde_json::to_string_pretty(&json!({
            "version": "twinning.mcp-run.v0",
            "outcome": "PASS",
            "child": {
                "command": run_command,
                "exit_code": child.code(),
                "signal": exit_signal(&child),
                "timed_out": false
            }
        }))
        .map(|mut rendered| {
            rendered.push('\n');
            rendered
        })
        .map_err(|error| Box::new(refusal::runtime_io("mcp_run_render", error.to_string())))?;
    }

    Ok(Execution {
        exit_code: 0,
        stdout,
    })
}

pub fn start_test_server(config: McpConfig) -> (SocketAddr, mpsc::Sender<()>) {
    let server = start_server(&config).expect("MCP test server should start");
    let addr = server.addr;
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        let _ = shutdown_rx.recv();
        match shutdown_and_join(server) {
            Ok(state) => {
                if let Err(error) = write_mcp_report_if_requested(&config, &state) {
                    eprintln!("failed to write MCP test report: {error:?}");
                }
            }
            Err(error) => eprintln!("failed to shut down MCP test server: {error:?}"),
        }
    });
    (addr, shutdown_tx)
}

fn start_server(config: &McpConfig) -> RefusalResult<RunningMcpServer> {
    let state = build_shared_state(config)?;
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let (addr_tx, addr_rx) = mpsc::channel::<Result<SocketAddr, String>>();
    let host = config.host.clone();
    let port = config.port;
    let thread_state = Arc::clone(&state);

    let handle = thread::spawn(move || {
        serve_on_current_thread(host, port, thread_state, addr_tx, shutdown_rx)
    });

    let addr = addr_rx
        .recv()
        .map_err(|error| Box::new(refusal::runtime_io("mcp_listener_ready", error.to_string())))?
        .map_err(|error| Box::new(refusal::runtime_io("mcp_listener_bind", error)))?;

    if !config.json {
        eprintln!(
            "MCP twin ready - {} tools ({} stubbable, {} unsupported), {} resources, {} prompts",
            state.catalog.tools.len(),
            state
                .catalog
                .tools
                .iter()
                .filter(|tool| classify_tool_executability(tool) == ToolExecutability::Stubbable)
                .count(),
            state
                .catalog
                .tools
                .iter()
                .filter(
                    |tool| classify_tool_executability(tool) == ToolExecutability::UnsupportedShape
                )
                .count(),
            state.catalog.resources.len(),
            state.catalog.prompts.len()
        );
        eprintln!("Listening on http://{addr}");
    }

    Ok(RunningMcpServer {
        addr,
        shutdown_tx,
        handle,
        state,
    })
}

fn build_shared_state(config: &McpConfig) -> RefusalResult<Arc<McpSharedState>> {
    let catalog = match &config.source {
        McpCatalogInput::LiveServer { command } => load_mcp_catalog_from_server(command)?,
        McpCatalogInput::Manifest { path } => load_mcp_catalog_from_manifest(path)?,
    };

    Ok(Arc::new(McpSharedState {
        catalog: Arc::new(catalog),
        auth_mode: config.auth_mode,
        session_log: Arc::new(Mutex::new(McpSessionLog::default())),
    }))
}

fn serve_on_current_thread(
    host: String,
    port: u16,
    state: Arc<McpSharedState>,
    addr_tx: mpsc::Sender<Result<SocketAddr, String>>,
    shutdown_rx: mpsc::Receiver<()>,
) -> Result<(), String> {
    let runtime = TokioRuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;

    runtime.block_on(async move {
        let listener = TcpListener::bind((host.as_str(), port))
            .await
            .map_err(|error| error.to_string())?;
        let addr = listener.local_addr().map_err(|error| error.to_string())?;
        addr_tx.send(Ok(addr)).map_err(|error| error.to_string())?;

        let app = Router::new()
            .route("/", post(handle_jsonrpc))
            .route("/message", post(handle_jsonrpc))
            .route("/mcp", post(handle_jsonrpc))
            .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
            .with_state(state);

        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_server_shutdown(shutdown_rx))
            .await
            .map_err(|error| error.to_string())
    })
}

async fn wait_for_server_shutdown(shutdown_rx: mpsc::Receiver<()>) {
    loop {
        if shutdown_rx.try_recv().is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn handle_jsonrpc(
    State(state): State<Arc<McpSharedState>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let outcome = handle_jsonrpc_inner(&state, &uri, &headers, body);
    record_request(&state, outcome.log);
    outcome.response
}

struct McpDispatchOutcome {
    response: Response,
    log: McpRequestLog,
}

fn handle_jsonrpc_inner(
    state: &McpSharedState,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> McpDispatchOutcome {
    if body.len() > MAX_REQUEST_BYTES {
        return error_outcome(
            StatusCode::PAYLOAD_TOO_LARGE,
            None,
            None,
            JSONRPC_INVALID_REQUEST,
            "request_too_large",
            Some(json!({ "max_bytes": MAX_REQUEST_BYTES })),
        );
    }

    if state.auth_mode == RestAuthMode::Shape {
        match check_auth(
            &state.catalog.required_auth_schemes,
            &state.catalog.security_schemes,
            headers,
            uri.query().unwrap_or_default(),
        ) {
            AuthExtract::Present => {}
            auth_refusal => {
                return error_outcome(
                    StatusCode::UNAUTHORIZED,
                    None,
                    None,
                    JSONRPC_INVALID_REQUEST,
                    "unauthorized",
                    Some(auth_error_data(auth_refusal)),
                );
            }
        }
    }

    let request = match serde_json::from_slice::<JsonRpcRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return error_outcome(
                StatusCode::BAD_REQUEST,
                None,
                None,
                JSONRPC_PARSE_ERROR,
                "parse error",
                Some(json!({ "detail": error.to_string() })),
            );
        }
    };
    let method = request.method.clone();
    let tool_call = request_tool_call_outcome(&request, &state.catalog);
    let protocol_version = request_protocol_version_log(&request);
    let response = dispatch(request, &state.catalog);
    let refusal = response
        .as_ref()
        .is_some_and(|response| response.error.is_some());
    let log = McpRequestLog {
        method: Some(method),
        refusal,
        tool_call,
        protocol_version,
    };

    let Some(response) = response else {
        return McpDispatchOutcome {
            response: Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("static MCP notification response"),
            log,
        };
    };

    match serde_json::to_string(&response) {
        Ok(rendered) => McpDispatchOutcome {
            response: json_http_response(StatusCode::OK, rendered),
            log,
        },
        Err(error) => error_outcome(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(log.method.clone().unwrap_or_else(|| "unknown".to_owned())),
            log.tool_call,
            JSONRPC_SERVER_ERROR,
            "server error",
            Some(json!({ "detail": error.to_string() })),
        ),
    }
}

fn request_tool_call_outcome(
    request: &JsonRpcRequest,
    catalog: &McpCatalog,
) -> Option<McpToolCallOutcome> {
    if request.method != "tools/call" {
        return None;
    }
    let name = request
        .params
        .as_ref()
        .and_then(|params| params.get("name"))
        .and_then(JsonValue::as_str)?;
    let tool = catalog.tools.iter().find(|tool| tool.name == name)?;
    match classify_tool_executability(tool) {
        ToolExecutability::Stubbable => Some(McpToolCallOutcome::Stubbable),
        ToolExecutability::UnsupportedShape => Some(McpToolCallOutcome::Unsupported),
    }
}

fn request_protocol_version_log(request: &JsonRpcRequest) -> Option<McpProtocolVersionLog> {
    if request.method != "initialize" {
        return None;
    }
    let negotiated = negotiate_initialize_protocol(request.params.as_ref());
    Some(McpProtocolVersionLog {
        requested: negotiated.requested,
        negotiated: negotiated.negotiated,
        supported: negotiated.supported,
    })
}

fn error_outcome(
    status: StatusCode,
    method: Option<String>,
    tool_call: Option<McpToolCallOutcome>,
    code: i64,
    message: impl Into<String>,
    data: Option<JsonValue>,
) -> McpDispatchOutcome {
    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id: JsonValue::Null,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
            data,
        }),
    };
    let rendered = serde_json::to_string(&response).expect("MCP JSON-RPC error serializes");
    McpDispatchOutcome {
        response: json_http_response(status, rendered),
        log: McpRequestLog {
            method,
            refusal: true,
            tool_call,
            protocol_version: None,
        },
    }
}

fn auth_error_data(auth: AuthExtract) -> JsonValue {
    match auth {
        AuthExtract::Present => json!({}),
        AuthExtract::Missing {
            scheme, location, ..
        } => json!({
            "kind": "missing_auth",
            "scheme": scheme,
            "location": location.to_string()
        }),
        AuthExtract::Malformed { scheme, detail } => json!({
            "kind": "malformed_auth",
            "scheme": scheme,
            "detail": detail
        }),
    }
}

fn json_http_response(status: StatusCode, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("static MCP JSON response")
}

fn record_request(state: &McpSharedState, request: McpRequestLog) {
    if let Ok(mut log) = state.session_log.lock() {
        log.record(request);
    }
}

fn shutdown_and_join(server: RunningMcpServer) -> RefusalResult<Arc<McpSharedState>> {
    let _ = server.shutdown_tx.send(());
    match server.handle.join() {
        Ok(Ok(())) => Ok(server.state),
        Ok(Err(error)) => Err(Box::new(refusal::runtime_io("mcp_listener", error))),
        Err(_) => Err(Box::new(refusal::runtime_io(
            "mcp_listener",
            "MCP listener thread panicked",
        ))),
    }
}

fn write_mcp_report_if_requested(config: &McpConfig, state: &McpSharedState) -> RefusalResult<()> {
    let Some(path) = &config.report_path else {
        return Ok(());
    };

    let log = state
        .session_log
        .lock()
        .map_err(|_| {
            Box::new(refusal::runtime_io(
                "mcp_report",
                "session log lock poisoned",
            ))
        })?
        .clone();
    let report = McpReport::new(&state.catalog, log.summary());
    write_json_report(path, &report)
}

fn write_json_report(path: &Path, report: &McpReport) -> RefusalResult<()> {
    let rendered = report
        .render_json()
        .map_err(|error| Box::new(refusal::runtime_io("mcp_report_render", error.to_string())))?;
    fs::write(path, rendered).map_err(|error| Box::new(refusal::io_write(path, &error)))
}

#[cfg(unix)]
fn wait_for_shutdown_signal() -> RefusalResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])
        .map_err(|error| Box::new(refusal::runtime_io("mcp_signal", error.to_string())))?;
    let _ = signals.forever().next();
    Ok(())
}

#[cfg(not(unix))]
fn wait_for_shutdown_signal() -> RefusalResult<()> {
    std::thread::park();
    Ok(())
}

fn run_child_with_base_url(
    command: &str,
    base_url: &str,
) -> Result<std::process::ExitStatus, Box<RefusalEnvelope>> {
    shell_command(command)
        .env("MCP_BASE_URL", base_url)
        .env("TWIN_BASE_URL", base_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| Box::new(refusal::runtime_io("mcp_run_child", error.to_string())))
}

fn shell_command(command: &str) -> Command {
    #[cfg(unix)]
    {
        let mut child_command = Command::new("sh");
        child_command.arg("-c").arg(command);
        child_command
    }

    #[cfg(windows)]
    {
        let mut child_command = Command::new("cmd");
        child_command.arg("/C").arg(command);
        child_command
    }
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        net::TcpStream,
    };

    use serde_json::{Value as JsonValue, json};
    use tempfile::tempdir;

    use crate::{
        auth::RestAuthMode,
        mcp::listener::{McpCatalogInput, McpConfig, start_test_server},
    };

    #[test]
    fn http_listener_dispatches_jsonrpc_and_writes_report() {
        let dir = tempdir().expect("tempdir");
        let manifest_path = dir.path().join("manifest.json");
        let report_path = dir.path().join("mcp-report.json");
        fs::write(&manifest_path, manifest()).expect("write manifest");
        let (addr, shutdown_tx) = start_test_server(McpConfig {
            source: McpCatalogInput::Manifest {
                path: manifest_path.clone(),
            },
            host: "127.0.0.1".to_owned(),
            port: 0,
            auth_mode: RestAuthMode::Bypass,
            stdio: false,
            run_command: None,
            report_path: Some(report_path.clone()),
            json: false,
        });

        let response = request(
            addr,
            "/",
            &json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}).to_string(),
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains(r#""tools""#), "{response}");

        shutdown_tx.send(()).expect("shutdown");
        std::thread::sleep(std::time::Duration::from_millis(80));
        let report: JsonValue =
            serde_json::from_str(&fs::read_to_string(report_path).expect("report should exist"))
                .expect("report JSON");
        assert_eq!("twinning.mcp-report.v0", report["version"]);
        assert_eq!(1, report["session"]["request_count"]);
        assert_eq!(2, report["catalog"]["tool_count"]);
        assert_eq!(1, report["catalog"]["tools_stubbable"]);
        assert_eq!(1, report["catalog"]["tools_unsupported"]);
    }

    fn request(addr: std::net::SocketAddr, path: &str, body: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .expect("read timeout");
        let raw = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(raw.as_bytes()).expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    fn manifest() -> String {
        serde_json::to_string_pretty(&json!({
            "serverInfo": { "name": "demo-mcp", "version": "1.0.0" },
            "tools": [
                {
                    "name": "lookup",
                    "description": "Lookup",
                    "inputSchema": { "type": "object" },
                    "outputSchema": { "type": "object", "properties": { "value": { "type": "string" } } }
                },
                {
                    "name": "write_note",
                    "description": "Write",
                    "inputSchema": { "type": "object" }
                }
            ],
            "resources": [],
            "prompts": []
        }))
        .expect("manifest JSON")
    }
}
