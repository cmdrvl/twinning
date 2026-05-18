//! Axum listener for the Snowflake HTTP wire protocol session skeleton.

use std::{
    collections::HashMap,
    fs,
    io::Read,
    net::SocketAddr,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, Uri, header},
    routing::{get, post},
};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
#[cfg(unix)]
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use tokio::{net::TcpListener, runtime::Builder as TokioRuntimeBuilder};
use twinning_kernel::{
    refusal::{self, RefusalEnvelope, RefusalResult},
    runtime::Execution,
};
use uuid::Uuid;

use crate::{
    catalog::SnowflakeCatalog,
    config::SnowflakeConfig,
    materialize::apply_source_materialization,
    query::{query_request, query_result},
    report::NEXT_STEP,
    session::{SnowflakeSession, SnowflakeSharedState},
};

const MAX_REQUEST_BYTES: usize = 1_048_576;

type JsonResponse = (StatusCode, Json<JsonValue>);

struct RunningSnowflakeServer {
    addr: SocketAddr,
    shutdown_tx: mpsc::Sender<()>,
    handle: thread::JoinHandle<Result<(), String>>,
    state: Arc<SnowflakeSharedState>,
}

pub struct EmbeddedSnowflakeServer {
    server: Option<RunningSnowflakeServer>,
    report_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnowflakeServerSession {
    pub addr: SocketAddr,
    pub active_session_count: usize,
    pub cached_result_count: usize,
}

#[derive(Debug, Deserialize, Default)]
struct LoginRequestData {
    #[serde(rename = "SESSION_PARAMETERS", default)]
    session_parameters: JsonValue,
    #[serde(rename = "ACCOUNT_NAME", default)]
    account_name: String,
    #[serde(rename = "LOGIN_NAME", default)]
    login_name: String,
}

#[derive(Debug, Deserialize, Default)]
struct LoginRequest {
    #[serde(default)]
    data: LoginRequestData,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct TokenRequest {
    old_session_token: Option<String>,
    request_type: Option<String>,
}

pub fn run(config: SnowflakeConfig) -> Result<Execution, Box<dyn std::error::Error>> {
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

fn run_inner(config: SnowflakeConfig) -> RefusalResult<Execution> {
    let json_mode = config.json;
    let Some(run_command) = config.run_command.clone() else {
        let server = start_server(&config)?;
        eprintln!(
            "Snowflake twin ready - {} tables, {} rows materialized",
            server.state.catalog.table_count(),
            server.state.catalog.row_count()
        );
        eprintln!("account: fakesnow  host: {}", server.addr);
        eprintln!(
            "Connect: snowflake.connector.connect(account='fakesnow', host='{}', port={}, protocol='http', user='x', password='x')",
            server.addr.ip(),
            server.addr.port()
        );
        eprintln!("{NEXT_STEP}");
        wait_for_shutdown_signal()?;
        let state = shutdown_and_join(server)?;
        write_report_if_requested(&config, &state)?;
        return Ok(Execution {
            exit_code: 0,
            stdout: String::from("Snowflake twin stopped.\n"),
        });
    };

    let server = start_server(&config)?;
    let base_url = format!("http://{}", server.addr);
    let child = run_child_with_base_url(&run_command, &base_url)?;
    let state = shutdown_and_join(server)?;
    write_report_if_requested(&config, &state)?;

    let mut stdout = format!(
        "Snowflake twin child command completed: exit_code={:?} signal={:?} timed_out=false\n",
        child.code(),
        exit_signal(&child)
    );
    if json_mode {
        stdout = serde_json::to_string_pretty(&json!({
            "version": "twinning.snowflake-run.v0",
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
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "snowflake_run_render",
                error.to_string(),
            ))
        })?;
    }

    Ok(Execution {
        exit_code: 0,
        stdout,
    })
}

pub fn start_embedded_server(config: SnowflakeConfig) -> RefusalResult<EmbeddedSnowflakeServer> {
    let report_path = config.report_path.clone();
    Ok(EmbeddedSnowflakeServer {
        server: Some(start_server(&config)?),
        report_path,
    })
}

impl EmbeddedSnowflakeServer {
    pub fn addr(&self) -> SocketAddr {
        self.server
            .as_ref()
            .expect("embedded Snowflake server is still running")
            .addr
    }

    pub fn shutdown(mut self) -> RefusalResult<SnowflakeServerSession> {
        let server = self
            .server
            .take()
            .expect("embedded Snowflake server should only shut down once");
        let addr = server.addr;
        let state = shutdown_and_join(server)?;
        write_report_if_requested_path(self.report_path.as_ref(), &state)?;
        snowflake_server_session(addr, &state)
    }
}

impl Drop for EmbeddedSnowflakeServer {
    fn drop(&mut self) {
        let Some(server) = self.server.take() else {
            return;
        };
        let _ = server.shutdown_tx.send(());
        let _ = server.handle.join();
    }
}

fn start_server(config: &SnowflakeConfig) -> RefusalResult<RunningSnowflakeServer> {
    let mut catalog = SnowflakeCatalog::from_schema_path(config.schema_path.as_deref())?;
    if let Some(source_url) = &config.materialize_source_url {
        apply_source_materialization(&mut catalog, source_url, config.max_rows_per_table)?;
    }
    let state = Arc::new(SnowflakeSharedState::new(catalog));
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
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "snowflake_listener_ready",
                error.to_string(),
            ))
        })?
        .map_err(|error| Box::new(refusal::runtime_io("snowflake_listener_bind", error)))?;

    Ok(RunningSnowflakeServer {
        addr,
        shutdown_tx,
        handle,
        state,
    })
}

fn serve_on_current_thread(
    host: String,
    port: u16,
    state: Arc<SnowflakeSharedState>,
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
            .route("/session/v1/login-request", post(login_request))
            .route("/session", post(session_request))
            .route("/session/token-request", post(token_request))
            .route("/session/heartbeat", post(heartbeat_request))
            .route("/queries/v1/abort-request", post(abort_request))
            .route("/queries/v1/query-request", post(query_request))
            .route("/queries/:query_id/result", get(query_result))
            .route("/monitoring/queries/:sfqid", get(monitoring_query))
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

async fn login_request(
    State(state): State<Arc<SnowflakeSharedState>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> JsonResponse {
    state.record_request();
    let decoded = match decode_body(&headers, body) {
        Ok(decoded) => decoded,
        Err(response) => {
            state.record_error();
            return response;
        }
    };
    let request = parse_login_request(&decoded);
    let token = opaque_token();
    let master_token = opaque_token();
    let session_id = positive_session_id();
    let database_name = query
        .get("databaseName")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "TWINDB".to_owned());
    let schema_name = query
        .get("schemaName")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "PUBLIC".to_owned());
    let warehouse_name = query
        .get("warehouse")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "TWIN_WH".to_owned());
    let role_name = query
        .get("roleName")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "SYSADMIN".to_owned());
    let display_user_name = if request.data.login_name.is_empty() {
        "USER".to_owned()
    } else {
        request.data.login_name.to_ascii_uppercase()
    };
    let _account_name = request.data.account_name.as_str();

    let session = SnowflakeSession {
        token: token.clone(),
        master_token: master_token.clone(),
        session_id,
        database_name: database_name.clone(),
        schema_name: schema_name.clone(),
        warehouse_name: warehouse_name.clone(),
        role_name: role_name.clone(),
        created_at: Instant::now(),
    };

    let mut sessions = match state.sessions.lock() {
        Ok(sessions) => sessions,
        Err(_) => {
            state.record_error();
            return server_error("session store lock poisoned");
        }
    };
    sessions.insert(token.clone(), session);

    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "token": token,
                "validityInSeconds": 3600,
                "masterToken": master_token,
                "masterValidityInSeconds": 14400,
                "sessionId": session_id,
                "displayUserName": display_user_name,
                "serverVersion": "8.40.2",
                "firstLogin": false,
                "healthCheckInterval": 3600,
                "parameters": session_parameters(request.data.session_parameters),
                "sessionInfo": {
                    "databaseName": database_name,
                    "schemaName": schema_name,
                    "warehouseName": warehouse_name,
                    "roleName": role_name
                }
            },
            "code": JsonValue::Null,
            "message": JsonValue::Null,
            "success": true
        })),
    )
}

async fn session_request(
    State(state): State<Arc<SnowflakeSharedState>>,
    uri: Uri,
    headers: HeaderMap,
) -> JsonResponse {
    state.record_request();
    let Some(token) = extract_token(&headers).map(str::to_owned) else {
        state.record_error();
        return missing_token_response();
    };

    let mut sessions = match state.sessions.lock() {
        Ok(sessions) => sessions,
        Err(_) => {
            state.record_error();
            return server_error("session store lock poisoned");
        }
    };
    if !sessions.contains_key(&token) {
        state.record_error();
        return expired_token_response();
    }
    if query_has_delete_true(uri.query()) {
        sessions.remove(&token);
    }

    success_null()
}

async fn token_request(
    State(state): State<Arc<SnowflakeSharedState>>,
    headers: HeaderMap,
    body: Bytes,
) -> JsonResponse {
    state.record_request();
    let Some(master_token) = extract_token(&headers).map(str::to_owned) else {
        state.record_error();
        return missing_token_response();
    };
    let decoded = match decode_body(&headers, body) {
        Ok(decoded) => decoded,
        Err(response) => {
            state.record_error();
            return response;
        }
    };
    let request = parse_token_request(&decoded);
    if request.request_type.as_deref() != Some("RENEW") {
        state.record_error();
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "data": JsonValue::Null,
                "code": "390100",
                "message": "Unsupported token request",
                "success": false
            })),
        );
    }

    let mut sessions = match state.sessions.lock() {
        Ok(sessions) => sessions,
        Err(_) => {
            state.record_error();
            return server_error("session store lock poisoned");
        }
    };
    let Some((old_token, mut session)) = find_session_by_master_token(
        &sessions,
        &master_token,
        request.old_session_token.as_deref(),
    ) else {
        state.record_error();
        return expired_token_response();
    };

    let new_token = opaque_token();
    sessions.remove(&old_token);
    session.token = new_token.clone();
    let session_id = session.session_id;
    let master_token = session.master_token.clone();
    sessions.insert(new_token.clone(), session);

    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "sessionToken": new_token,
                "validityInSecondsST": 3600,
                "masterToken": master_token,
                "validityInSecondsMT": 14400,
                "sessionId": session_id
            },
            "code": JsonValue::Null,
            "message": JsonValue::Null,
            "success": true
        })),
    )
}

async fn heartbeat_request(
    State(state): State<Arc<SnowflakeSharedState>>,
    headers: HeaderMap,
) -> JsonResponse {
    state.record_request();
    match require_session(&state, &headers) {
        Ok(_) => (StatusCode::OK, Json(json!({ "success": true }))),
        Err(response) => {
            state.record_error();
            response
        }
    }
}

async fn abort_request(State(state): State<Arc<SnowflakeSharedState>>) -> JsonResponse {
    state.record_request();
    (StatusCode::OK, Json(json!({ "success": true })))
}

async fn monitoring_query(
    State(state): State<Arc<SnowflakeSharedState>>,
    Path(sfqid): Path<String>,
    headers: HeaderMap,
) -> JsonResponse {
    state.record_request();
    if let Err(response) = require_session(&state, &headers) {
        state.record_error();
        return response;
    }

    let results = match state.results_cache.lock() {
        Ok(results) => results,
        Err(_) => {
            state.record_error();
            return server_error("results cache lock poisoned");
        }
    };
    let queries = results
        .get(&sfqid)
        .map(|result| vec![json!({ "status": result.status })])
        .unwrap_or_default();

    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "queries": queries
            },
            "success": true
        })),
    )
}

pub fn extract_token(headers: &HeaderMap) -> Option<&str> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    auth.strip_prefix("Snowflake Token=\"")?.strip_suffix('"')
}

fn decode_body(headers: &HeaderMap, body: Bytes) -> Result<Vec<u8>, JsonResponse> {
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("gzip"))
        .unwrap_or(false);

    if !gzip {
        return Ok(body.to_vec());
    }

    let mut decoder = GzDecoder::new(body.as_ref());
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "data": JsonValue::Null,
                "code": "390100",
                "message": format!("Invalid gzip request body: {error}"),
                "success": false
            })),
        )
    })?;
    Ok(decompressed)
}

fn parse_login_request(body: &[u8]) -> LoginRequest {
    if body.is_empty() {
        return LoginRequest::default();
    }
    serde_json::from_slice(body).unwrap_or_default()
}

fn parse_token_request(body: &[u8]) -> TokenRequest {
    if body.is_empty() {
        return TokenRequest::default();
    }
    serde_json::from_slice(body).unwrap_or_default()
}

fn require_session(
    state: &SnowflakeSharedState,
    headers: &HeaderMap,
) -> Result<SnowflakeSession, JsonResponse> {
    let Some(token) = extract_token(headers) else {
        return Err(missing_token_response());
    };
    let sessions = state
        .sessions
        .lock()
        .map_err(|_| server_error("session store lock poisoned"))?;
    sessions
        .get(token)
        .cloned()
        .ok_or_else(expired_token_response)
}

fn find_session_by_master_token(
    sessions: &HashMap<String, SnowflakeSession>,
    master_token: &str,
    old_session_token: Option<&str>,
) -> Option<(String, SnowflakeSession)> {
    if let Some(old_session_token) = old_session_token
        && let Some(session) = sessions.get(old_session_token)
        && session.master_token == master_token
    {
        return Some((old_session_token.to_owned(), session.clone()));
    }

    sessions
        .iter()
        .find(|(_, session)| session.master_token == master_token)
        .map(|(token, session)| (token.clone(), session.clone()))
}

fn query_has_delete_true(query: Option<&str>) -> bool {
    query
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .any(|(key, value)| key == "delete" && value.eq_ignore_ascii_case("true"))
}

fn session_parameters(_raw: JsonValue) -> Vec<JsonValue> {
    vec![
        json!({"name": "AUTOCOMMIT", "value": true}),
        json!({"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600}),
        json!({"name": "TIMEZONE", "value": "UTC"}),
        json!({"name": "TIMESTAMP_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"}),
        json!({"name": "TIMESTAMP_NTZ_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF9"}),
        json!({"name": "TIMESTAMP_LTZ_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"}),
        json!({"name": "TIMESTAMP_TZ_OUTPUT_FORMAT", "value": ""}),
        json!({"name": "DATE_OUTPUT_FORMAT", "value": "YYYY-MM-DD"}),
        json!({"name": "TIME_OUTPUT_FORMAT", "value": "HH24:MI:SS"}),
        json!({"name": "BINARY_OUTPUT_FORMAT", "value": "HEX"}),
        json!({"name": "CLIENT_TELEMETRY_ENABLED", "value": true}),
        json!({"name": "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT", "value": "arrow"}),
    ]
}

fn missing_token_response() -> JsonResponse {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390101",
            "message": "No token provided",
            "success": false
        })),
    )
}

fn expired_token_response() -> JsonResponse {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390104",
            "message": "Token expired",
            "success": false
        })),
    )
}

fn success_null() -> JsonResponse {
    (
        StatusCode::OK,
        Json(json!({
            "data": JsonValue::Null,
            "code": JsonValue::Null,
            "message": JsonValue::Null,
            "success": true
        })),
    )
}

fn server_error(message: &str) -> JsonResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390100",
            "message": message,
            "success": false
        })),
    )
}

fn opaque_token() -> String {
    Uuid::new_v4().to_string().replace('-', "")
}

fn positive_session_id() -> i64 {
    let candidate = (Uuid::new_v4().as_u128() % i64::MAX as u128) as i64;
    candidate.max(1)
}

fn snowflake_server_session(
    addr: SocketAddr,
    state: &SnowflakeSharedState,
) -> RefusalResult<SnowflakeServerSession> {
    let active_session_count = state
        .sessions
        .lock()
        .map_err(|_| {
            Box::new(refusal::runtime_io(
                "snowflake_session_store",
                "session store lock poisoned",
            ))
        })?
        .len();
    let cached_result_count = state
        .results_cache
        .lock()
        .map_err(|_| {
            Box::new(refusal::runtime_io(
                "snowflake_results_cache",
                "results cache lock poisoned",
            ))
        })?
        .len();

    Ok(SnowflakeServerSession {
        addr,
        active_session_count,
        cached_result_count,
    })
}

fn shutdown_and_join(server: RunningSnowflakeServer) -> RefusalResult<Arc<SnowflakeSharedState>> {
    let state = Arc::clone(&server.state);
    let _ = server.shutdown_tx.send(());
    server
        .handle
        .join()
        .map_err(|_| {
            Box::new(refusal::runtime_io(
                "snowflake_listener_join",
                "thread panicked",
            ))
        })?
        .map_err(|error| Box::new(refusal::runtime_io("snowflake_listener_join", error)))?;
    Ok(state)
}

fn write_report_if_requested(
    config: &SnowflakeConfig,
    state: &SnowflakeSharedState,
) -> RefusalResult<()> {
    write_report_if_requested_path(config.report_path.as_ref(), state)
}

fn write_report_if_requested_path(
    path: Option<&PathBuf>,
    state: &SnowflakeSharedState,
) -> RefusalResult<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let rendered = serde_json::to_string_pretty(&state.report_value()).map_err(|error| {
        Box::new(refusal::runtime_io(
            "snowflake_report_render",
            error.to_string(),
        ))
    })?;
    fs::write(path, format!("{rendered}\n"))
        .map_err(|error| Box::new(refusal::io_write(path, &error)))
}

#[cfg(unix)]
fn wait_for_shutdown_signal() -> RefusalResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])
        .map_err(|error| Box::new(refusal::runtime_io("snowflake_signal", error.to_string())))?;
    signals.forever().next();
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
        .env("SNOWFLAKE_TWIN_URL", base_url)
        .env("TWIN_BASE_URL", base_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "snowflake_run_child",
                error.to_string(),
            ))
        })
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
    use std::io::Write;

    use axum::http::{HeaderMap, HeaderValue, header};
    use flate2::{Compression, write::GzEncoder};
    use serde_json::{Value as JsonValue, json};

    use crate::{
        SnowflakeConfig,
        config::SnowflakeConfigParts,
        listener::{extract_token, start_embedded_server},
    };

    #[test]
    fn token_extraction_matches_snowflake_authorization_shape() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Snowflake Token=\"abc123\""),
        );

        assert_eq!(Some("abc123"), extract_token(&headers));
    }

    #[test]
    fn login_accepts_gzip_body_and_returns_required_session_fields() {
        let server = start_embedded_server(test_config()).expect("server");
        let response = ureq::post(&url(server.addr(), "/session/v1/login-request"))
            .set("content-encoding", "gzip")
            .send_bytes(&gzip_json(&login_body()))
            .expect("login response");
        let body: JsonValue = response.into_json().expect("login json");

        assert_eq!(true, body["success"]);
        assert!(body["data"]["token"].as_str().is_some());
        assert!(body["data"]["masterToken"].as_str().is_some());
        assert!(body["data"]["sessionId"].as_i64().unwrap_or_default() > 0);
        assert_eq!("8.40.2", body["data"]["serverVersion"]);
        assert_parameter(&body, "AUTOCOMMIT");
        assert_parameter(&body, "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT");
        assert_eq!("TWINDB", body["data"]["sessionInfo"]["databaseName"]);

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn login_accepts_plain_json_body() {
        let server = start_embedded_server(test_config()).expect("server");
        let response = ureq::post(&url(server.addr(), "/session/v1/login-request"))
            .send_json(login_body())
            .expect("login response");
        let body: JsonValue = response.into_json().expect("login json");

        assert_eq!(true, body["success"]);
        assert!(body["data"]["token"].as_str().is_some());

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn query_requires_authorization_and_rejects_unknown_tokens() {
        let server = start_embedded_server(test_config()).expect("server");

        let missing = ureq::post(&url(server.addr(), "/queries/v1/query-request"))
            .send_json(json!({ "sqlText": "select 1" }))
            .expect_err("missing auth should fail");
        assert_snowflake_error(missing, 401, "390101");

        let unknown = ureq::post(&url(server.addr(), "/queries/v1/query-request"))
            .set("authorization", "Snowflake Token=\"missing\"")
            .send_json(json!({ "sqlText": "select 1" }))
            .expect_err("unknown auth should fail");
        assert_snowflake_error(unknown, 401, "390104");

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn query_request_routes_show_tables_and_uses_uppercase_data_key() {
        let server = start_embedded_server(test_config_with_schema(
            "CREATE TABLE deals (id int, name varchar);",
        ))
        .expect("server");
        let auth = login_auth(&server);

        let body: JsonValue = ureq::post(&url(
            server.addr(),
            "/queries/v1/query-request?requestId=ignored",
        ))
        .set("authorization", &auth)
        .send_json(json!({ "sqlText": "show tables" }))
        .expect("query response")
        .into_json()
        .expect("query json");

        assert_eq!(true, body["success"]);
        assert!(body.get("Data").is_some());
        assert!(body.get("data").is_none());
        assert_eq!(1, body["Data"]["total"]);
        assert_eq!("name", body["Data"]["rowtype"][1]["name"]);
        assert!(
            !body["Data"]["rowsetBase64"]
                .as_str()
                .expect("rowsetBase64")
                .is_empty()
        );

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn query_request_updates_use_context_and_current_database() {
        let server = start_embedded_server(test_config()).expect("server");
        let auth = login_auth(&server);

        let use_body: JsonValue = ureq::post(&url(server.addr(), "/queries/v1/query-request"))
            .set("authorization", &auth)
            .send_json(json!({ "sqlText": "use database analytics" }))
            .expect("use response")
            .into_json()
            .expect("use json");
        assert_eq!("ANALYTICS", use_body["Data"]["finalDatabaseName"]);
        assert_eq!("", use_body["Data"]["rowsetBase64"]);

        let current: JsonValue = ureq::post(&url(server.addr(), "/queries/v1/query-request"))
            .set("authorization", &auth)
            .send_json(json!({ "sqlText": "select current_database()" }))
            .expect("current response")
            .into_json()
            .expect("current json");
        assert_eq!("ANALYTICS", current["Data"]["finalDatabaseName"]);
        assert_eq!("CURRENT_DATABASE()", current["Data"]["rowtype"][0]["name"]);

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn query_request_unknown_table_returns_snowflake_error_envelope() {
        let server = start_embedded_server(test_config()).expect("server");
        let auth = login_auth(&server);

        let body: JsonValue = ureq::post(&url(server.addr(), "/queries/v1/query-request"))
            .set("authorization", &auth)
            .send_json(json!({ "sqlText": "select * from missing_table" }))
            .expect("query response")
            .into_json()
            .expect("query json");

        assert_eq!(false, body["success"]);
        assert_eq!("002003", body["code"]);
        assert_eq!("42S02", body["data"]["sqlState"]);
        assert_eq!(
            "Object 'MISSING_TABLE' does not exist or not authorized.",
            body["message"]
        );

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn async_query_result_can_be_retrieved_from_cache() {
        let server = start_embedded_server(test_config()).expect("server");
        let auth = login_auth(&server);

        let accepted: JsonValue = ureq::post(&url(server.addr(), "/queries/v1/query-request"))
            .set("authorization", &auth)
            .send_json(json!({
                "sqlText": "select current_version()",
                "asyncExec": true
            }))
            .expect("async accepted")
            .into_json()
            .expect("async json");
        assert_eq!(true, accepted["success"]);
        assert_eq!("333334", accepted["code"]);
        let result_url = accepted["data"]["getResultUrl"]
            .as_str()
            .expect("result URL");

        let result: JsonValue = ureq::get(&url(server.addr(), result_url))
            .set("authorization", &auth)
            .call()
            .expect("cached result")
            .into_json()
            .expect("cached json");
        assert_eq!(true, result["success"]);
        assert_eq!("CURRENT_VERSION()", result["Data"]["rowtype"][0]["name"]);

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn logout_removes_session_and_subsequent_request_is_unauthorized() {
        let server = start_embedded_server(test_config()).expect("server");
        let login: JsonValue = ureq::post(&url(server.addr(), "/session/v1/login-request"))
            .send_json(login_body())
            .expect("login response")
            .into_json()
            .expect("login json");
        let token = login["data"]["token"].as_str().expect("token");
        let auth = format!("Snowflake Token=\"{token}\"");

        let logout: JsonValue = ureq::post(&url(
            server.addr(),
            "/session?delete=true&requestId=ignored",
        ))
        .set("authorization", &auth)
        .send_json(json!({}))
        .expect("logout response")
        .into_json()
        .expect("logout json");
        assert_eq!(true, logout["success"]);

        let heartbeat = ureq::post(&url(server.addr(), "/session/heartbeat"))
            .set("authorization", &auth)
            .send_json(json!({}))
            .expect_err("old session should be gone");
        assert_snowflake_error(heartbeat, 401, "390104");

        server.shutdown().expect("shutdown");
    }

    #[test]
    fn token_renewal_uses_master_token_and_replaces_session_token() {
        let server = start_embedded_server(test_config()).expect("server");
        let login: JsonValue = ureq::post(&url(server.addr(), "/session/v1/login-request"))
            .send_json(login_body())
            .expect("login response")
            .into_json()
            .expect("login json");
        let token = login["data"]["token"].as_str().expect("token");
        let master_token = login["data"]["masterToken"].as_str().expect("master token");

        let renewed: JsonValue = ureq::post(&url(server.addr(), "/session/token-request"))
            .set(
                "authorization",
                &format!("Snowflake Token=\"{master_token}\""),
            )
            .send_json(json!({
                "oldSessionToken": token,
                "requestType": "RENEW"
            }))
            .expect("renewal response")
            .into_json()
            .expect("renewal json");
        let new_token = renewed["data"]["sessionToken"].as_str().expect("new token");
        assert_ne!(token, new_token);

        let old_heartbeat = ureq::post(&url(server.addr(), "/session/heartbeat"))
            .set("authorization", &format!("Snowflake Token=\"{token}\""))
            .send_json(json!({}))
            .expect_err("old token should be expired");
        assert_snowflake_error(old_heartbeat, 401, "390104");

        let new_heartbeat: JsonValue = ureq::post(&url(server.addr(), "/session/heartbeat"))
            .set("authorization", &format!("Snowflake Token=\"{new_token}\""))
            .send_json(json!({}))
            .expect("new token should work")
            .into_json()
            .expect("heartbeat json");
        assert_eq!(true, new_heartbeat["success"]);

        server.shutdown().expect("shutdown");
    }

    fn test_config() -> SnowflakeConfig {
        SnowflakeConfig::from_parts(
            SnowflakeConfigParts {
                schema_path: None,
                host: "127.0.0.1".to_owned(),
                port: 0,
                run_command: None,
                serve: true,
                report_path: None,
                materialize_source_url: None,
                max_rows_per_table: 100_000,
            },
            false,
        )
    }

    fn test_config_with_schema(schema: &str) -> SnowflakeConfig {
        let path =
            std::env::temp_dir().join(format!("twinning-snowflake-{}.sql", uuid::Uuid::new_v4()));
        std::fs::write(&path, schema).expect("write test schema");

        SnowflakeConfig::from_parts(
            SnowflakeConfigParts {
                schema_path: Some(path),
                host: "127.0.0.1".to_owned(),
                port: 0,
                run_command: None,
                serve: true,
                report_path: None,
                materialize_source_url: None,
                max_rows_per_table: 100_000,
            },
            false,
        )
    }

    fn login_body() -> JsonValue {
        json!({
            "data": {
                "ACCOUNT_NAME": "acct",
                "LOGIN_NAME": "user",
                "SESSION_PARAMETERS": {}
            }
        })
    }

    fn gzip_json(value: &JsonValue) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        write!(
            encoder,
            "{}",
            serde_json::to_string(value).expect("render json")
        )
        .expect("write gzip");
        encoder.finish().expect("finish gzip")
    }

    fn url(addr: std::net::SocketAddr, path: &str) -> String {
        format!("http://{addr}{path}")
    }

    fn login_auth(server: &crate::listener::EmbeddedSnowflakeServer) -> String {
        let login: JsonValue = ureq::post(&url(server.addr(), "/session/v1/login-request"))
            .send_json(login_body())
            .expect("login response")
            .into_json()
            .expect("login json");
        let token = login["data"]["token"].as_str().expect("token");
        format!("Snowflake Token=\"{token}\"")
    }

    fn assert_parameter(body: &JsonValue, name: &str) {
        let parameters = body["data"]["parameters"].as_array().expect("parameters");
        assert!(
            parameters
                .iter()
                .any(|parameter| parameter["name"].as_str() == Some(name)),
            "missing parameter {name}"
        );
    }

    fn assert_snowflake_error(error: ureq::Error, status: u16, code: &str) {
        let ureq::Error::Status(actual_status, response) = error else {
            panic!("expected HTTP status error");
        };
        assert_eq!(status, actual_status);
        let body: JsonValue = response.into_json().expect("error json");
        assert_eq!(code, body["code"]);
        assert_eq!(false, body["success"]);
    }
}
