//! Axum listener for the OpenAPI-spec-driven REST protocol adapter.

use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method as HttpMethod, StatusCode, Uri, header},
    response::Response,
};
use serde_json::json;
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use tokio::{net::TcpListener, runtime::Builder as TokioRuntimeBuilder};

use crate::{
    backend::BaseSnapshotBackend,
    config::{ChaosConfig, RestConfig},
    ir::ReadShape,
    kernel::{
        mutation::execute_mutation,
        read::{execute_read, execute_scan_all},
        storage::TableStorage,
    },
    refusal::{self, RefusalEnvelope, RefusalResult},
    report::{RestReport, RestSpecReport},
    result::KernelResult,
    runtime::Execution,
};

use super::{
    auth::{AuthExtract, RestAuthMode, check_auth},
    canary::RestCanaryManifest,
    encode::{encode, encode_rest_refusal},
    normalize::{IrOp, NormalizeRequest, RestRefusal, normalize_request},
    policy::{RoutingConfig, RoutingPolicy, resolve_routing_config},
    report::RoutingReport,
    routes::{Method, PathPattern, PathSegment, RouteTable, build_route_registry, match_route},
    seed::seed_from_spec,
    session::RestSessionIds,
    session_log::{RestRequestLog, RestSessionLog, RestSessionSummary, constraint_violation_kind},
    spec::{RestCatalog, SecurityScheme, load_rest_catalog},
    topology::build_spec_topology,
    xext::resolve_auth_mode,
};

const MAX_REQUEST_BYTES: usize = 1_048_576;
const CHAOS_TIMEOUT_DURATION: Duration = Duration::from_secs(30);

struct RestSharedState {
    catalog: Arc<RestCatalog>,
    routes: Arc<RouteTable>,
    backend: Arc<Mutex<BaseSnapshotBackend>>,
    session_log: Arc<Mutex<RestSessionLog>>,
    session_ids: RestSessionIds,
    auth_mode: RestAuthMode,
    chaos: Option<ChaosConfig>,
    chaos_rng: AtomicU64,
}

struct RunningRestServer {
    addr: SocketAddr,
    shutdown_tx: mpsc::Sender<()>,
    handle: thread::JoinHandle<Result<(), String>>,
    state: Arc<RestSharedState>,
}

pub struct EmbeddedRestServer {
    server: Option<RunningRestServer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestServerSession {
    pub addr: SocketAddr,
    pub log: RestSessionLog,
    pub summary: RestSessionSummary,
}

struct DispatchOutcome {
    response: Response,
    route: String,
    refusal: Option<String>,
    constraint_violation: Option<String>,
}

pub fn run(config: RestConfig) -> Result<Execution, Box<dyn std::error::Error>> {
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

fn run_inner(config: RestConfig) -> RefusalResult<Execution> {
    let json_mode = config.json;
    let Some(run_command) = config.run_command.clone() else {
        let server = start_server(&config)?;
        wait_for_shutdown_signal()?;
        let state = shutdown_and_join(server)?;
        write_rest_report_if_requested(&config, &state)?;
        return Ok(Execution {
            exit_code: 0,
            stdout: String::from("REST twin stopped.\n"),
        });
    };

    let server = start_server(&config)?;
    let base_url = format!("http://{}", server.addr);
    let child = run_child_with_base_url(&run_command, &base_url)?;
    let state = shutdown_and_join(server)?;
    write_rest_report_if_requested(&config, &state)?;

    let mut stdout = format!(
        "REST twin child command completed: exit_code={:?} signal={:?} timed_out=false\n",
        child.code(),
        exit_signal(&child)
    );
    if json_mode {
        stdout = serde_json::to_string_pretty(&json!({
            "version": "twinning.rest-run.v0",
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
        .map_err(|error| Box::new(refusal::runtime_io("rest_run_render", error.to_string())))?;
    }

    Ok(Execution {
        exit_code: 0,
        stdout,
    })
}

pub fn start_test_server(config: RestConfig) -> (SocketAddr, mpsc::Sender<()>) {
    let server = start_server(&config).expect("REST test server should start");
    let addr = server.addr;
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        let _ = shutdown_rx.recv();
        match shutdown_and_join(server) {
            Ok(state) => {
                if let Err(error) = write_rest_report_if_requested(&config, &state) {
                    eprintln!("failed to write REST test report: {error:?}");
                }
            }
            Err(error) => eprintln!("failed to shut down REST test server: {error:?}"),
        }
    });
    (addr, shutdown_tx)
}

pub fn start_embedded_server(config: RestConfig) -> RefusalResult<EmbeddedRestServer> {
    Ok(EmbeddedRestServer {
        server: Some(start_server(&config)?),
    })
}

impl EmbeddedRestServer {
    pub fn addr(&self) -> SocketAddr {
        self.server
            .as_ref()
            .expect("embedded REST server is still running")
            .addr
    }

    pub fn shutdown(mut self) -> RefusalResult<RestServerSession> {
        let server = self
            .server
            .take()
            .expect("embedded REST server should only shut down once");
        let addr = server.addr;
        let state = shutdown_and_join(server)?;
        rest_server_session(addr, &state)
    }
}

impl Drop for EmbeddedRestServer {
    fn drop(&mut self) {
        let Some(server) = self.server.take() else {
            return;
        };
        let _ = server.shutdown_tx.send(());
        let _ = server.handle.join();
    }
}

fn start_server(config: &RestConfig) -> RefusalResult<RunningRestServer> {
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
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "rest_listener_ready",
                error.to_string(),
            ))
        })?
        .map_err(|error| Box::new(refusal::runtime_io("rest_listener_bind", error)))?;

    Ok(RunningRestServer {
        addr,
        shutdown_tx,
        handle,
        state,
    })
}

fn rest_server_session(
    addr: SocketAddr,
    state: &RestSharedState,
) -> RefusalResult<RestServerSession> {
    let log = state
        .session_log
        .lock()
        .map_err(|_| {
            Box::new(refusal::runtime_io(
                "rest_session_log",
                "session log lock poisoned",
            ))
        })?
        .clone();
    let summary = log.summary(&declared_endpoints(&state.routes));
    Ok(RestServerSession { addr, log, summary })
}

fn build_shared_state(config: &RestConfig) -> RefusalResult<Arc<RestSharedState>> {
    let catalog = load_rest_catalog(&config.spec_path)?;
    if config.strict {
        eprintln!("--strict response validation is not yet implemented");
    }
    let auth_mode = resolve_auth_mode(config.auth_mode, catalog.x_twinning.as_ref());
    if !catalog.security_schemes.is_empty() {
        let schemes = catalog
            .security_schemes
            .iter()
            .map(|scheme| scheme.name.as_str())
            .collect::<Vec<_>>();
        eprintln!(
            "Security schemes declared in spec: {:?}. REST auth mode: {}.",
            schemes, auth_mode
        );
    }
    if let Some(chaos) = config.chaos {
        eprintln!("REST chaos mode active: {chaos}");
    }

    let routing = effective_routing_config(config, &catalog);
    let topology = build_spec_topology(&catalog, &config.routing);
    let registry = build_route_registry(&catalog, &topology, &routing);
    RoutingReport::from_registry(&registry, &catalog).log_at_startup();
    for warning in catalog
        .warnings
        .iter()
        .map(|warning| warning.message.as_str())
        .chain(registry.warnings.iter().map(String::as_str))
    {
        eprintln!("{warning}");
    }

    let mut backend = initialize_backend(&catalog)?;
    let seed_result = seed_from_spec(&catalog, &mut backend)
        .map_err(|error| Box::new(refusal::runtime_io("rest_seed", error.to_string())))?;
    backend.promote_overlay_to_base();
    for warning in &seed_result.warnings {
        eprintln!("{}", warning.message);
    }

    Ok(Arc::new(RestSharedState {
        catalog: Arc::new(catalog),
        routes: Arc::new(registry.routes),
        backend: Arc::new(Mutex::new(backend)),
        session_log: Arc::new(Mutex::new(RestSessionLog::default())),
        session_ids: RestSessionIds::new(),
        auth_mode,
        chaos: config.chaos,
        chaos_rng: AtomicU64::new(initial_chaos_seed()),
    }))
}

fn effective_routing_config(config: &RestConfig, catalog: &RestCatalog) -> RoutingConfig {
    let cli_policy =
        (config.routing.policy != RoutingPolicy::Auto).then_some(config.routing.policy);
    resolve_routing_config(
        cli_policy,
        config.routing.base_prefix.clone(),
        catalog.x_twinning.as_ref(),
    )
}

fn initialize_backend(catalog: &RestCatalog) -> RefusalResult<BaseSnapshotBackend> {
    let mut tables = Vec::new();
    for table in &catalog.catalog.tables {
        tables.push(TableStorage::new(table).map_err(|error| {
            Box::new(refusal::runtime_io("rest_storage_init", error.to_string()))
        })?);
    }

    BaseSnapshotBackend::new(tables)
        .map_err(|error| Box::new(refusal::runtime_io("rest_backend_init", error.to_string())))
}

fn serve_on_current_thread(
    host: String,
    port: u16,
    state: Arc<RestSharedState>,
    addr_tx: mpsc::Sender<Result<SocketAddr, String>>,
    shutdown_rx: mpsc::Receiver<()>,
) -> Result<(), String> {
    let runtime = TokioRuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;

    // Option A from bd-12o.4: a dedicated std thread hosts a single-thread
    // Tokio runtime. The shared std::sync::Mutex intentionally serializes
    // reads and writes for the v0 REST twin.
    runtime.block_on(async move {
        let listener = TcpListener::bind((host.as_str(), port))
            .await
            .map_err(|error| error.to_string())?;
        let addr = listener.local_addr().map_err(|error| error.to_string())?;
        addr_tx.send(Ok(addr)).map_err(|error| error.to_string())?;

        let app = Router::new()
            .fallback(dispatch)
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

async fn dispatch(
    State(state): State<Arc<RestSharedState>>,
    method: HttpMethod,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let outcome = dispatch_inner(
        Arc::clone(&state),
        method.clone(),
        uri.clone(),
        headers,
        body,
    );
    let duration_ms = started.elapsed().as_millis() as u64;
    let status = outcome.response.status();
    record_request(
        &state,
        RestRequestLog {
            method: method.as_str().to_owned(),
            path: uri.path().to_owned(),
            route: outcome.route,
            status: status.as_u16(),
            duration_ms,
            constraint_violation: outcome.constraint_violation,
            refusal: outcome
                .refusal
                .or_else(|| (status == StatusCode::NOT_FOUND).then(|| String::from("not_found"))),
        },
    );
    eprintln!(
        "{} {} -> {} ({}ms)",
        method,
        uri.path(),
        status.as_u16(),
        duration_ms
    );
    outcome.response
}

fn dispatch_inner(
    state: Arc<RestSharedState>,
    http_method: HttpMethod,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> DispatchOutcome {
    if body.len() > MAX_REQUEST_BYTES {
        return DispatchOutcome::rest_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "unmatched",
            "request_too_large",
            json!({ "code": "request_too_large", "max_bytes": MAX_REQUEST_BYTES }),
        );
    }

    let Some(method) = rest_method(&http_method) else {
        return DispatchOutcome::rest_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "unmatched",
            "method_not_allowed",
            json!({ "code": "method_not_allowed", "method": http_method.as_str() }),
        );
    };

    let Some(matched) = match_route(&state.routes, method, uri.path()) else {
        let allowed_methods = allowed_methods_for_path(&state.routes, uri.path());
        if !allowed_methods.is_empty() {
            return DispatchOutcome::method_not_allowed(&http_method, allowed_methods);
        }
        return DispatchOutcome::rest_error(
            StatusCode::NOT_FOUND,
            "unmatched",
            "not_found",
            json!({ "code": "not_found", "detail": "route not found" }),
        );
    };
    let route = path_pattern_string(matched.pattern);

    if state.auth_mode == RestAuthMode::Shape {
        match check_auth(
            &matched.entry.required_auth_schemes,
            &state.catalog.security_schemes,
            &headers,
            uri.query().unwrap_or_default(),
        ) {
            AuthExtract::Present => {}
            auth_refusal => {
                return DispatchOutcome::auth_error(
                    &route,
                    auth_refusal,
                    &matched.entry.required_auth_schemes,
                    &state.catalog.security_schemes,
                );
            }
        }
    }

    if let Some(outcome) = maybe_inject_chaos(&state, &route) {
        return outcome;
    }

    let session_id = state.session_ids.next_session_id();
    let normalized = normalize_request(
        &state.catalog,
        matched.entry,
        NormalizeRequest {
            method: matched.method,
            path_params: &matched.path_params,
            session_id: &session_id,
            uri: &uri,
            headers: &headers,
            body: body.as_ref(),
        },
    );

    let op = match normalized {
        Ok(op) => op,
        Err(refusal) => {
            let code = rest_refusal_code(&refusal).to_owned();
            let constraint_violation = constraint_violation_kind(&code).map(str::to_owned);
            return DispatchOutcome {
                response: encode_rest_refusal(refusal, &session_id),
                route,
                refusal: Some(code),
                constraint_violation,
            };
        }
    };

    let mut backend = match state.backend.lock() {
        Ok(backend) => backend,
        Err(_) => {
            return DispatchOutcome::rest_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &route,
                "kernel_error",
                json!({ "code": "kernel_error", "detail": "REST backend lock is poisoned" }),
            );
        }
    };

    let result = match op {
        IrOp::Read(read) if read.shape == ReadShape::FilteredScan && read.predicate.is_none() => {
            execute_scan_all(
                &state.catalog.catalog,
                &*backend,
                &read.table,
                &read.projection,
                read.limit,
            )
        }
        IrOp::Read(read) => execute_read(&state.catalog.catalog, &*backend, &read),
        IrOp::Mutation(mutation) => {
            let result = execute_mutation(&state.catalog.catalog, &mut *backend, &mutation);
            if matches!(result, KernelResult::Mutation(_)) {
                backend.promote_overlay_to_base();
            }
            result
        }
    };

    let (refusal, constraint_violation) = kernel_result_observation(&result);
    DispatchOutcome {
        response: encode(result, matched.entry, &session_id),
        route,
        refusal,
        constraint_violation,
    }
}

impl DispatchOutcome {
    fn rest_error(status: StatusCode, route: &str, refusal: &str, body: serde_json::Value) -> Self {
        Self {
            response: json_response(status, body),
            route: route.to_owned(),
            refusal: Some(refusal.to_owned()),
            constraint_violation: constraint_violation_kind(refusal).map(str::to_owned),
        }
    }

    fn method_not_allowed(http_method: &HttpMethod, allowed_methods: Vec<Method>) -> Self {
        let allowed = allowed_methods
            .iter()
            .map(|method| method.as_str())
            .collect::<Vec<_>>();
        let allow_header = allowed.join(", ");
        Self {
            response: json_response_with_allow(
                StatusCode::METHOD_NOT_ALLOWED,
                json!({
                    "code": "method_not_allowed",
                    "method": http_method.as_str(),
                    "allowed": allowed,
                }),
                &allow_header,
            ),
            route: String::from("unmatched"),
            refusal: Some(String::from("method_not_allowed")),
            constraint_violation: constraint_violation_kind("method_not_allowed")
                .map(str::to_owned),
        }
    }

    fn auth_error(
        route: &str,
        auth: AuthExtract,
        required: &[String],
        schemes: &[SecurityScheme],
    ) -> Self {
        let (body, challenge) = match auth {
            AuthExtract::Present => unreachable!("present auth is not an auth error"),
            AuthExtract::Missing {
                scheme, location, ..
            } => (
                json!({
                    "code": "unauthorized",
                    "detail": format!("Authorization required: {scheme} ({location})"),
                }),
                www_authenticate_challenge(required, schemes),
            ),
            AuthExtract::Malformed { detail, .. } => (
                json!({
                    "code": "unauthorized",
                    "detail": detail,
                }),
                None,
            ),
        };

        Self {
            response: json_response_with_www_authenticate(
                StatusCode::UNAUTHORIZED,
                body,
                challenge.as_deref(),
            ),
            route: route.to_owned(),
            refusal: Some(String::from("unauthorized")),
            constraint_violation: None,
        }
    }

    fn rate_limited(route: &str) -> Self {
        Self {
            response: json_response_with_retry_after(
                StatusCode::TOO_MANY_REQUESTS,
                json!({
                    "code": "rate_limited",
                    "detail": "chaos mode injected rate limit"
                }),
                "1",
            ),
            route: route.to_owned(),
            refusal: Some(String::from("rate_limited")),
            constraint_violation: None,
        }
    }

    fn server_error(route: &str) -> Self {
        Self {
            response: json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "code": "internal_error",
                    "detail": "chaos mode injected server error"
                }),
            ),
            route: route.to_owned(),
            refusal: Some(String::from("internal_error")),
            constraint_violation: None,
        }
    }

    fn timeout(route: &str) -> Self {
        thread::sleep(CHAOS_TIMEOUT_DURATION);
        Self {
            response: Response::builder()
                .status(StatusCode::REQUEST_TIMEOUT)
                .body(Body::empty())
                .expect("static REST timeout response"),
            route: route.to_owned(),
            refusal: Some(String::from("timeout")),
            constraint_violation: None,
        }
    }
}

fn maybe_inject_chaos(state: &RestSharedState, route: &str) -> Option<DispatchOutcome> {
    let chaos = state.chaos?;
    if !chaos.is_enabled() {
        return None;
    }

    if chance(state, chaos.rate_limit_per_million) {
        return Some(DispatchOutcome::rate_limited(route));
    }
    if chance(state, chaos.server_error_per_million) {
        return Some(DispatchOutcome::server_error(route));
    }
    if chance(state, chaos.timeout_per_million) {
        return Some(DispatchOutcome::timeout(route));
    }

    None
}

fn chance(state: &RestSharedState, per_million: u32) -> bool {
    per_million > 0 && next_chaos_draw(state) < per_million.min(ChaosConfig::SCALE)
}

fn next_chaos_draw(state: &RestSharedState) -> u32 {
    let value = state
        .chaos_rng
        .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    splitmix64(value) as u32 % ChaosConfig::SCALE
}

fn splitmix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn initial_chaos_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0xA5A5_5A5A_D3C1_B0A7)
}

fn www_authenticate_challenge(required: &[String], schemes: &[SecurityScheme]) -> Option<String> {
    required.iter().find_map(|required_scheme| {
        schemes
            .iter()
            .find(|scheme| scheme.name == *required_scheme)
            .and_then(security_scheme_challenge)
    })
}

fn security_scheme_challenge(scheme: &SecurityScheme) -> Option<String> {
    let kind = scheme.kind.as_deref().unwrap_or_default();
    if kind.eq_ignore_ascii_case("oauth2") || kind.eq_ignore_ascii_case("openIdConnect") {
        return Some(String::from("Bearer realm=\"twinning\""));
    }

    if !kind.eq_ignore_ascii_case("http") {
        return None;
    }

    let auth_scheme = scheme.scheme.as_deref().unwrap_or_default();
    if auth_scheme.eq_ignore_ascii_case("bearer") {
        Some(String::from("Bearer realm=\"twinning\""))
    } else if auth_scheme.eq_ignore_ascii_case("basic") {
        Some(String::from("Basic realm=\"twinning\""))
    } else {
        None
    }
}

fn allowed_methods_for_path(routes: &RouteTable, request_path: &str) -> Vec<Method> {
    let mut allowed = BTreeSet::new();
    for (method, pattern, _) in routes {
        if pattern.captures(request_path).is_some() {
            allowed.insert(*method);
        }
    }

    [
        Method::Get,
        Method::Head,
        Method::Post,
        Method::Put,
        Method::Patch,
        Method::Delete,
    ]
    .into_iter()
    .filter(|method| allowed.contains(method))
    .collect()
}

fn record_request(state: &RestSharedState, request: RestRequestLog) {
    if let Ok(mut log) = state.session_log.lock() {
        log.record(request);
    }
}

fn kernel_result_observation(result: &KernelResult) -> (Option<String>, Option<String>) {
    match result {
        KernelResult::Refusal(refusal) => {
            let code = refusal.code.clone();
            let constraint = constraint_violation_kind(&code).map(str::to_owned);
            (Some(code), constraint)
        }
        _ => (None, None),
    }
}

fn rest_refusal_code(refusal: &RestRefusal) -> &'static str {
    match refusal {
        RestRefusal::MissingRequiredField { .. } => "missing_required_field",
        RestRefusal::UnknownField { .. } => "unknown_field",
        RestRefusal::TypeMismatch { .. } => "type_mismatch",
        RestRefusal::UnsupportedShape { .. } => "unsupported_shape",
        RestRefusal::UndeclaredQueryParam { .. } => "undeclared_query_param",
        RestRefusal::UnsupportedMediaType { .. } => "unsupported_media_type",
        RestRefusal::InvalidJson { .. } => "invalid_json",
    }
}

fn path_pattern_string(pattern: &PathPattern) -> String {
    if pattern.segments.is_empty() {
        return String::from("/");
    }

    let mut path = String::new();
    for segment in &pattern.segments {
        path.push('/');
        match segment {
            PathSegment::Literal(value) => path.push_str(value),
            PathSegment::Param(value) => {
                path.push('{');
                path.push_str(value);
                path.push('}');
            }
            PathSegment::Template {
                prefix,
                name,
                suffix,
            } => {
                path.push_str(prefix);
                path.push('{');
                path.push_str(name);
                path.push('}');
                path.push_str(suffix);
            }
        }
    }
    path
}

fn rest_method(method: &HttpMethod) -> Option<Method> {
    if method == HttpMethod::GET {
        Some(Method::Get)
    } else if method == HttpMethod::HEAD {
        Some(Method::Head)
    } else if method == HttpMethod::POST {
        Some(Method::Post)
    } else if method == HttpMethod::PUT {
        Some(Method::Put)
    } else if method == HttpMethod::PATCH {
        Some(Method::Patch)
    } else if method == HttpMethod::DELETE {
        Some(Method::Delete)
    } else {
        None
    }
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_string(&body).expect("REST error body serializes"),
        ))
        .expect("static REST response")
}

fn json_response_with_www_authenticate(
    status: StatusCode,
    body: serde_json::Value,
    challenge: Option<&str>,
) -> Response {
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(challenge) = challenge {
        builder = builder.header(header::WWW_AUTHENTICATE, challenge);
    }

    builder
        .body(Body::from(
            serde_json::to_string(&body).expect("REST error body serializes"),
        ))
        .expect("static REST response")
}

fn json_response_with_allow(
    status: StatusCode,
    body: serde_json::Value,
    allow_header: &str,
) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ALLOW, allow_header)
        .body(Body::from(
            serde_json::to_string(&body).expect("REST error body serializes"),
        ))
        .expect("static REST response")
}

fn json_response_with_retry_after(
    status: StatusCode,
    body: serde_json::Value,
    retry_after: &str,
) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::RETRY_AFTER, retry_after)
        .body(Body::from(
            serde_json::to_string(&body).expect("REST error body serializes"),
        ))
        .expect("static REST response")
}

fn wait_for_shutdown_signal() -> RefusalResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])
        .map_err(|error| Box::new(refusal::runtime_io("rest_signal", error.to_string())))?;
    let _ = signals.forever().next();
    Ok(())
}

fn shutdown_and_join(server: RunningRestServer) -> RefusalResult<Arc<RestSharedState>> {
    let _ = server.shutdown_tx.send(());
    match server.handle.join() {
        Ok(Ok(())) => Ok(server.state),
        Ok(Err(error)) => Err(Box::new(refusal::runtime_io("rest_listener", error))),
        Err(_) => Err(Box::new(refusal::runtime_io(
            "rest_listener",
            "REST listener thread panicked",
        ))),
    }
}

fn write_rest_report_if_requested(
    config: &RestConfig,
    state: &RestSharedState,
) -> RefusalResult<()> {
    let Some(path) = &config.report_path else {
        return Ok(());
    };

    let log = state
        .session_log
        .lock()
        .map_err(|_| {
            Box::new(refusal::runtime_io(
                "rest_report",
                "session log lock poisoned",
            ))
        })?
        .clone();
    let declared = declared_endpoints(&state.routes);
    let session = log.summary(&declared);
    let canary = match &config.canary_path {
        Some(path) => Some(
            RestCanaryManifest::load(path)
                .map_err(|error| Box::new(refusal::runtime_io("rest_canary", error.to_string())))?
                .evaluate(&log),
        ),
        None => None,
    };
    let report = RestReport::new(
        RestSpecReport {
            source: config.spec_path.display().to_string(),
            hash: state.catalog.spec_hash.clone(),
            resource_count: state.catalog.resources.len(),
            route_count: state.routes.len(),
            security_schemes_bypassed: security_schemes_bypassed(state),
        },
        session,
        canary,
    );
    write_json_report(path, &report)
}

fn security_schemes_bypassed(state: &RestSharedState) -> Vec<String> {
    if state.auth_mode != RestAuthMode::Bypass {
        return Vec::new();
    }

    state
        .catalog
        .security_schemes
        .iter()
        .map(|scheme| scheme.name.clone())
        .collect()
}

fn write_json_report(path: &Path, report: &RestReport) -> RefusalResult<()> {
    let rendered = report
        .render_json()
        .map_err(|error| Box::new(refusal::runtime_io("rest_report_render", error.to_string())))?;
    fs::write(path, rendered).map_err(|error| Box::new(refusal::io_write(path, &error)))
}

fn declared_endpoints(routes: &RouteTable) -> Vec<String> {
    routes
        .iter()
        .map(|(method, pattern, _)| format!("{} {}", method.as_str(), path_pattern_string(pattern)))
        .collect()
}

fn run_child_with_base_url(
    command: &str,
    base_url: &str,
) -> Result<std::process::ExitStatus, Box<RefusalEnvelope>> {
    shell_command(command)
        .env("TWIN_BASE_URL", base_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| Box::new(refusal::runtime_io("rest_run_child", error.to_string())))
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
        net::{SocketAddr, TcpStream},
        time::Duration,
    };

    use serde_json::json;
    use tempfile::tempdir;

    use crate::protocol::rest::{auth::RestAuthMode, policy::RoutingPolicy, routes::RouteKind};

    use super::*;

    fn test_config(spec: std::path::PathBuf) -> RestConfig {
        RestConfig {
            spec_path: spec,
            host: String::from("127.0.0.1"),
            port: 0,
            run_command: None,
            serve: true,
            serve_defaulted: false,
            report_path: None,
            canary_path: None,
            strict: false,
            routing: crate::protocol::rest::policy::RoutingConfig::default(),
            auth_mode: None,
            chaos: None,
            json: false,
        }
    }

    fn write_spec() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("tempdir");
        let spec_path = dir.path().join("api.yaml");
        fs::write(
            &spec_path,
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      additionalProperties: false
      properties:
        id: { type: integer }
        name: { type: string }
paths:
  /files:
    get:
      parameters:
        - name: limit
          in: query
          schema: { type: integer }
      responses: {}
    post:
      responses: {}
  /files/{id}:
    head:
      responses: {}
    get:
      responses: {}
"#,
        )
        .expect("write spec");
        (dir, spec_path)
    }

    fn write_prefixed_x_twinning_spec() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("tempdir");
        let spec_path = dir.path().join("prefixed-api.yaml");
        fs::write(
            &spec_path,
            r##"
openapi: 3.0.3
x-twinning:
  routing: prefix-scoped
  base-prefix: /__admin
components:
  schemas:
    Mapping:
      type: object
      required: [id, name]
      additionalProperties: false
      properties:
        id: { type: string }
        name: { type: string }
paths:
  /__admin/mappings:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Mapping"
  /__admin/mappings/{id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Mapping"
"##,
        )
        .expect("write prefixed spec");
        (dir, spec_path)
    }

    fn write_bearer_auth_spec() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("tempdir");
        let spec_path = dir.path().join("auth-api.yaml");
        fs::write(
            &spec_path,
            r##"
openapi: 3.0.3
security:
  - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
  schemas:
    File:
      type: object
      properties:
        id: { type: integer }
        name: { type: string }
paths:
  /files:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/File"
"##,
        )
        .expect("write auth spec");
        (dir, spec_path)
    }

    fn write_bearer_auth_bypass_x_twinning_spec() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().expect("tempdir");
        let spec_path = dir.path().join("auth-bypass-api.yaml");
        fs::write(
            &spec_path,
            r##"
openapi: 3.0.3
x-twinning:
  auth:
    mode: bypass
security:
  - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
  schemas:
    File:
      type: object
      properties:
        id: { type: integer }
        name: { type: string }
paths:
  /files:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/File"
"##,
        )
        .expect("write auth bypass spec");
        (dir, spec_path)
    }

    fn request(addr: SocketAddr, raw: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        stream.write_all(raw.as_bytes()).expect("write request");

        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    #[test]
    fn auth_required_route_returns_401_until_bearer_is_present() {
        let (_dir, spec_path) = write_bearer_auth_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        let missing = request(
            addr,
            &format!("GET /files HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert!(missing.starts_with("HTTP/1.1 401"), "{missing}");
        assert!(
            missing
                .to_ascii_lowercase()
                .contains("www-authenticate: bearer realm=\"twinning\""),
            "{missing}"
        );
        assert!(missing.contains(r#""code":"unauthorized""#), "{missing}");
        assert!(
            missing.contains("Authorization required: bearerAuth (header)"),
            "{missing}"
        );

        let malformed = request(
            addr,
            &format!(
                "GET /files HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Token nope\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(malformed.starts_with("HTTP/1.1 401"), "{malformed}");
        assert!(
            malformed.contains("must use Bearer credentials"),
            "{malformed}"
        );

        let not_found = request(
            addr,
            &format!("GET /missing HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert!(not_found.starts_with("HTTP/1.1 404"), "{not_found}");

        let present = request(
            addr,
            &format!(
                "GET /files HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer anything\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(present.starts_with("HTTP/1.1 200"), "{present}");

        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn auth_mode_bypass_skips_required_auth_check_from_cli() {
        let (_dir, spec_path) = write_bearer_auth_spec();
        let mut config = test_config(spec_path);
        config.auth_mode = Some(RestAuthMode::Bypass);
        let (addr, shutdown_tx) = start_test_server(config);

        let response = request(
            addr,
            &format!("GET /files HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn x_twinning_auth_mode_bypass_skips_required_auth_check() {
        let (_dir, spec_path) = write_bearer_auth_bypass_x_twinning_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        let response = request(
            addr,
            &format!("GET /files HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn cli_auth_mode_overrides_x_twinning_auth_mode() {
        let (_dir, spec_path) = write_bearer_auth_bypass_x_twinning_spec();
        let mut config = test_config(spec_path);
        config.auth_mode = Some(RestAuthMode::Shape);
        let state = build_shared_state(&config).expect("build rest state");

        assert_eq!(state.auth_mode, RestAuthMode::Shape);
    }

    #[test]
    fn report_security_schemes_bypassed_reflects_resolved_auth_mode() {
        let (_dir, spec_path) = write_bearer_auth_spec();
        let report_dir = tempdir().expect("report tempdir");

        let shape_report_path = report_dir.path().join("shape-report.json");
        let mut shape_config = test_config(spec_path.clone());
        shape_config.auth_mode = Some(RestAuthMode::Shape);
        shape_config.report_path = Some(shape_report_path.clone());
        let shape_state = build_shared_state(&shape_config).expect("build shape state");
        write_rest_report_if_requested(&shape_config, &shape_state).expect("write shape report");
        let shape_report: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&shape_report_path).expect("read shape report"),
        )
        .expect("shape report JSON");
        assert_eq!(shape_report["spec"]["security_schemes_bypassed"], json!([]));

        let bypass_report_path = report_dir.path().join("bypass-report.json");
        let mut bypass_config = test_config(spec_path);
        bypass_config.auth_mode = Some(RestAuthMode::Bypass);
        bypass_config.report_path = Some(bypass_report_path.clone());
        let bypass_state = build_shared_state(&bypass_config).expect("build bypass state");
        write_rest_report_if_requested(&bypass_config, &bypass_state).expect("write bypass report");
        let bypass_report: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&bypass_report_path).expect("read bypass report"),
        )
        .expect("bypass report JSON");
        assert_eq!(
            bypass_report["spec"]["security_schemes_bypassed"],
            json!(["bearerAuth"])
        );
    }

    #[test]
    fn test_server_accepts_mutation_and_next_read_observes_committed_state() {
        let (_dir, spec_path) = write_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        let body = r#"{"id":1,"name":"foo.txt"}"#;
        let post = request(
            addr,
            &format!(
                "POST /files HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        );
        assert!(post.starts_with("HTTP/1.1 201"), "{post}");

        let get = request(
            addr,
            &format!("GET /files/1 HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert!(get.starts_with("HTTP/1.1 200"), "{get}");
        assert!(get.contains(r#""id":1"#), "{get}");
        assert!(get.contains(r#""name":"foo.txt""#), "{get}");

        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn startup_uses_x_twinning_routing_for_prefixed_routes() {
        let (_dir, spec_path) = write_prefixed_x_twinning_spec();
        let config = test_config(spec_path);
        let state = build_shared_state(&config).expect("build rest state");
        let route = state
            .routes
            .iter()
            .find(|(method, pattern, _)| {
                *method == Method::Get && pattern == &PathPattern::parse("/__admin/mappings")
            })
            .map(|(_, _, entry)| entry)
            .expect("prefixed route should exist");

        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::PrefixScoped));
        assert_eq!(route.effective_resource_name.as_deref(), Some("mappings"));
    }

    #[test]
    fn unmatched_path_returns_json_not_found() {
        let (_dir, spec_path) = write_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        let response = request(
            addr,
            &format!("GET /missing HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );

        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        assert!(response.contains(r#""code":"not_found""#), "{response}");

        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn declared_path_with_unsupported_method_returns_method_not_allowed() {
        let (_dir, spec_path) = write_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        let response = request(
            addr,
            &format!("DELETE /files HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );

        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert!(
            response.to_ascii_lowercase().contains("allow: get, post"),
            "{response}"
        );
        assert!(
            response.contains(r#""code":"method_not_allowed""#),
            "{response}"
        );
        assert!(response.contains(r#""method":"DELETE""#), "{response}");

        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn declared_head_operation_returns_route_refusal_not_method_mismatch() {
        let (_dir, spec_path) = write_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        let response = request(
            addr,
            &format!("HEAD /files/1 HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );

        assert!(response.starts_with("HTTP/1.1 501"), "{response}");
        assert!(
            !response.contains(r#""code":"method_not_allowed""#),
            "{response}"
        );

        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn test_server_applies_read_many_limit_and_refuses_invalid_limit() {
        let (_dir, spec_path) = write_spec();
        let (addr, shutdown_tx) = start_test_server(test_config(spec_path));

        for (id, name) in [(1, "foo.txt"), (2, "bar.txt")] {
            let body = format!(r#"{{"id":{id},"name":"{name}"}}"#);
            let post = request(
                addr,
                &format!(
                    "POST /files HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ),
            );
            assert!(post.starts_with("HTTP/1.1 201"), "{post}");
        }

        let limited = request(
            addr,
            &format!("GET /files?limit=1 HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert!(limited.starts_with("HTTP/1.1 200"), "{limited}");
        let limited_body = limited.split("\r\n\r\n").nth(1).expect("response body");
        let limited_json: serde_json::Value =
            serde_json::from_str(limited_body).expect("limited body json");
        assert_eq!(limited_json.as_array().expect("array").len(), 1);

        let unlimited = request(
            addr,
            &format!("GET /files HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert!(unlimited.starts_with("HTTP/1.1 200"), "{unlimited}");
        let unlimited_body = unlimited.split("\r\n\r\n").nth(1).expect("response body");
        let unlimited_json: serde_json::Value =
            serde_json::from_str(unlimited_body).expect("unlimited body json");
        assert_eq!(unlimited_json.as_array().expect("array").len(), 2);

        let invalid = request(
            addr,
            &format!("GET /files?limit=abc HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert!(invalid.starts_with("HTTP/1.1 400"), "{invalid}");
        assert!(invalid.contains(r#""code":"type_mismatch""#), "{invalid}");

        shutdown_tx.send(()).expect("shutdown");
    }

    #[test]
    fn report_writer_emits_rest_report_and_canary_results() {
        let (dir, spec_path) = write_spec();
        let report_path = dir.path().join("rest-report.json");
        let canary_path = dir.path().join("rest-canary.json");
        fs::write(
            &canary_path,
            r#"{
              "assertions": [
                { "kind": "endpoint_called", "method": "POST", "path": "/files" },
                { "kind": "status_returned", "method": "POST", "path": "/files", "status": 201 },
                { "kind": "refusal_issued", "code": "not_found" }
              ]
            }"#,
        )
        .expect("write canary");

        let mut config = test_config(spec_path);
        config.report_path = Some(report_path.clone());
        config.canary_path = Some(canary_path);
        let state = build_shared_state(&config).expect("build rest state");
        state
            .session_log
            .lock()
            .expect("session log")
            .record(RestRequestLog {
                method: String::from("POST"),
                path: String::from("/files"),
                route: String::from("/files"),
                status: 201,
                duration_ms: 4,
                constraint_violation: None,
                refusal: None,
            });

        write_rest_report_if_requested(&config, &state).expect("write report");

        let rendered = fs::read_to_string(&report_path).expect("read report");
        let report: serde_json::Value = serde_json::from_str(&rendered).expect("parse report");
        assert_eq!(report["version"], "twinning.rest-report.v0");
        assert_eq!(report["outcome"], "FAIL");
        assert_eq!(report["session"]["request_count"], 1);
        assert_eq!(
            report["session"]["endpoints_exercised"],
            serde_json::json!(["POST /files"])
        );
        assert_eq!(report["canary"]["total"], 3);
        assert_eq!(report["canary"]["passed"], 2);
        assert_eq!(report["canary"]["failed"], 1);
        assert_eq!(
            report["canary"]["failures"][0]["assertion"],
            "refusal_issued"
        );
    }
}
