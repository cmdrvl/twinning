//! Owns twin child processes, readiness, stop, TTL expiry and server shutdown.

use std::collections::{HashMap, HashSet};
use std::fs::{self, DirBuilder, File};
use std::io::{Read, Seek, SeekFrom};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::argv::TwinArgv;
use crate::errors::ToolError;
use crate::log::{self, Level};
use crate::registry::{Registry, TwinKind, TwinRecord, TwinStatus};
use crate::stale;

const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub twinning_bin: PathBuf,
    /// Usually `<temp_dir>/twinning-mcp/<server_pid>`.
    pub state_root: PathBuf,
    pub max_twins: usize,
    pub ready_timeout: Option<Duration>,
    pub stop_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct StartRequest {
    pub workdir: PathBuf,
    pub schema: PathBuf,
    pub verify: Option<PathBuf>,
    pub seed: Option<PathBuf>,
    pub restore: Option<PathBuf>,
    pub ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct TwinStartRequest {
    pub workdir: PathBuf,
    pub kind: TwinKind,
    pub input: PathBuf,
    pub verify: Option<PathBuf>,
    pub seed: Option<PathBuf>,
    pub restore: Option<PathBuf>,
    pub snapshot: Option<PathBuf>,
    pub declaration: Option<PathBuf>,
    pub auth_mode: Option<String>,
    pub server_variables: Vec<(String, String)>,
    pub capture_refused_statements: bool,
    pub ttl: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StopResult {
    pub twin_id: String,
    pub kind: TwinKind,
    pub status: TwinStatus,
    pub exit_code: i32,
    pub report_path: PathBuf,
    pub trace_path: PathBuf,
    pub report: Value,
}

#[derive(Debug)]
struct Shared {
    twinning_bin: PathBuf,
    state_root: PathBuf,
    registry: Mutex<Registry>,
    children: Mutex<HashMap<String, Child>>,
    completed: Mutex<HashMap<String, StopResult>>,
    operation: Mutex<()>,
    ready_timeout: Duration,
    stop_timeout: Duration,
}

#[derive(Debug)]
pub struct Supervisor {
    shared: Arc<Shared>,
}

impl Supervisor {
    pub fn new(config: SupervisorConfig) -> Result<Self, ToolError> {
        let ready_timeout = env_duration("TWINNING_MCP_READY_TIMEOUT_MS", config.ready_timeout)
            .unwrap_or(DEFAULT_READY_TIMEOUT);
        let stop_timeout = env_duration("TWINNING_MCP_STOP_TIMEOUT_MS", config.stop_timeout)
            .unwrap_or(DEFAULT_STOP_TIMEOUT);
        let mut builder = DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(&config.state_root)
            .map_err(|error| io_error("state directory", error))?;
        fs::set_permissions(&config.state_root, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error("state directory permissions", error))?;
        if let Some(parent) = config.state_root.parent() {
            stale::sweep_stale(parent, std::process::id())
                .map_err(|error| io_error("stale state sweep", error))?;
        }
        Ok(Self {
            shared: Arc::new(Shared {
                twinning_bin: config.twinning_bin,
                state_root: config.state_root,
                registry: Mutex::new(Registry::new(config.max_twins)),
                children: Mutex::new(HashMap::new()),
                completed: Mutex::new(HashMap::new()),
                operation: Mutex::new(()),
                ready_timeout,
                stop_timeout,
            }),
        })
    }

    pub fn start(&self, request: StartRequest) -> Result<TwinRecord, ToolError> {
        self.start_typed(TwinStartRequest {
            workdir: request.workdir,
            kind: TwinKind::Postgres,
            input: request.schema,
            verify: request.verify,
            seed: request.seed,
            restore: request.restore,
            snapshot: None,
            declaration: None,
            auth_mode: None,
            server_variables: Vec::new(),
            capture_refused_statements: false,
            ttl: request.ttl,
        })
    }

    pub fn start_typed(&self, request: TwinStartRequest) -> Result<TwinRecord, ToolError> {
        let _operation = self
            .shared
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let ttl = test_ttl_override(request.ttl);
        let first_port = free_port()?;
        let mut record = self
            .shared
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reserve(&self.shared.state_root, first_port, ttl, request.kind)?;
        if let Err(error) = create_private_dir(&record.state_dir) {
            self.mark_failed(&record.id);
            return Err(error);
        }
        let argv = TwinArgv {
            kind: request.kind,
            input: request.input,
            verify: request.verify,
            seed: request.seed,
            restore: request.restore,
            snapshot: request.snapshot,
            declaration: request.declaration,
            auth_mode: request.auth_mode,
            server_variables: request.server_variables,
            capture_refused_statements: request.capture_refused_statements,
            report: record.report_path.clone(),
            trace: record.trace_path.clone(),
            port: first_port,
        };

        for attempt in 0..3 {
            let port = if attempt == 0 {
                first_port
            } else {
                match free_port() {
                    Ok(port) => port,
                    Err(error) => {
                        self.mark_failed(&record.id);
                        return Err(error);
                    }
                }
            };
            record.port = port;
            if let Some(stored) = self
                .shared
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_mut(&record.id)
            {
                stored.port = port;
            }
            let stdout = match File::create(&record.stdout_path) {
                Ok(file) => file,
                Err(error) => {
                    self.mark_failed(&record.id);
                    return Err(io_error("child stdout", error));
                }
            };
            let stderr = match File::create(&record.stderr_path) {
                Ok(file) => file,
                Err(error) => {
                    self.mark_failed(&record.id);
                    return Err(io_error("child stderr", error));
                }
            };
            let mut command = Command::new(&self.shared.twinning_bin);
            let mut attempt_argv = argv.clone();
            attempt_argv.port = port;
            command
                .args(attempt_argv.build())
                .current_dir(&request.workdir)
                .stdin(Stdio::null())
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr));
            command.process_group(0);
            let started = Instant::now();
            let child = command.spawn().map_err(|error| {
                self.mark_failed(&record.id);
                ToolError {
                    code: "E_START_FAILED",
                    message: format!("twinning child: failed to spawn: {error}"),
                    hint: "Check the twinning binary and run twinning-mcp doctor",
                }
            })?;
            let pid = child.id();
            self.shared
                .children
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .insert(record.id.clone(), child);
            if let Some(stored) = self
                .shared
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_mut(&record.id)
            {
                stored.child_pid = Some(pid);
            }
            if let Err(error) = fs::write(record.state_dir.join("child.pid"), format!("{pid}\n")) {
                self.kill_child_group(pid, libc::SIGKILL);
                self.reap_child(&record.id);
                self.mark_failed(&record.id);
                return Err(io_error("child pid file", error));
            }
            log::emit(
                Level::Info,
                Some(&record.id),
                "child_spawned",
                &[("pid", pid.to_string()), ("port", port.to_string())],
            );
            match await_readiness(&self.shared, &record.id, port) {
                Ok(()) => {
                    if let Some(stored) = self
                        .shared
                        .registry
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .get_mut(&record.id)
                    {
                        stored.status = TwinStatus::Running;
                        record = stored.clone();
                    }
                    log::emit(
                        Level::Info,
                        Some(&record.id),
                        "ready",
                        &[("port", port.to_string())],
                    );
                    spawn_ttl_timer(Arc::downgrade(&self.shared), record.id.clone(), ttl);
                    return Ok(record);
                }
                Err(ReadinessFailure::Exited(status))
                    if attempt < 2
                        && started.elapsed() <= Duration::from_secs(1)
                        && is_address_in_use_refusal(&record.stdout_path) =>
                {
                    log::emit(
                        Level::Warn,
                        Some(&record.id),
                        "listener_bind_retry",
                        &[("exit_code", exit_code(status).to_string())],
                    );
                    self.reap_child(&record.id);
                }
                Err(ReadinessFailure::Exited(status)) => {
                    self.reap_child(&record.id);
                    self.mark_failed(&record.id);
                    return Err(ToolError {
                        code: "E_START_FAILED",
                        message: format!(
                            "twinning child: exited before readiness with exit code {}",
                            exit_code(status)
                        ),
                        hint: "Read the twin's stdout.json and try again",
                    });
                }
                Err(ReadinessFailure::Timeout) => {
                    self.kill_child_group(pid, libc::SIGKILL);
                    self.reap_child(&record.id);
                    self.mark_failed(&record.id);
                    return Err(ToolError {
                        code: "E_READY_TIMEOUT",
                        message: format!(
                            "twinning child: listener did not become ready on port {port}"
                        ),
                        hint: "Check the twin's stdout.json and stderr.log, then retry",
                    });
                }
            }
        }
        self.mark_failed(&record.id);
        Err(ToolError {
            code: "E_START_FAILED",
            message: "twinning child: listener bind failed after three port attempts".into(),
            hint: "Check the twin's stdout.json and try again",
        })
    }

    pub fn stop(&self, twin_id: &str) -> Result<StopResult, ToolError> {
        let _operation = self
            .shared
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        stop_locked(&self.shared, twin_id, false)
    }

    /// Send SIGTERM to every twin first, then kill remaining groups after five seconds.
    pub fn shutdown_all(&self) {
        let _operation = self
            .shared
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        log::emit(Level::Info, None, "shutdown_all", &[]);
        let ids: Vec<String> = self
            .shared
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records()
            .iter()
            .filter(|record| record.status.is_live())
            .map(|record| record.id.clone())
            .collect();
        let pids: Vec<(String, u32)> = {
            let mut registry = self
                .shared
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            ids.iter()
                .filter_map(|id| {
                    let record = registry.get_mut(id)?;
                    record.status = TwinStatus::Stopping;
                    record.child_pid.map(|pid| (id.clone(), pid))
                })
                .collect()
        };
        for (_, pid) in &pids {
            self.kill_child_group(*pid, libc::SIGTERM);
        }
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            if all_children_exited(&self.shared, &ids) || Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        for (id, pid) in &pids {
            if !child_exited(&self.shared, id) {
                self.kill_child_group(*pid, libc::SIGKILL);
            }
        }
        for id in ids {
            if let Some(result) = stop_after_shutdown(&self.shared, &id) {
                cache_completed(&self.shared, result);
            }
        }
    }

    pub fn records(&self) -> Vec<TwinRecord> {
        self.shared
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records()
            .to_vec()
    }

    fn kill_child_group(&self, pid: u32, signal: i32) {
        // SAFETY: children are created in a fresh process group with pgid == child pid.
        let result = unsafe { libc::killpg(pid as libc::pid_t, signal) };
        if result != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            log::emit(
                Level::Warn,
                None,
                "signal_failed",
                &[("pid", pid.to_string())],
            );
        }
    }

    fn reap_child(&self, twin_id: &str) {
        if let Some(mut child) = self
            .shared
            .children
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(twin_id)
            && let Ok(status) = child.wait()
        {
            log::emit(
                Level::Info,
                Some(twin_id),
                "child_exited",
                &[("exit_code", exit_code(status).to_string())],
            );
        }
    }

    fn mark_failed(&self, twin_id: &str) {
        self.shared
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mark_stopped(twin_id, false);
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == 1 {
            self.shutdown_all();
        }
    }
}

enum ReadinessFailure {
    Exited(ExitStatus),
    Timeout,
}

fn await_readiness(shared: &Shared, twin_id: &str, port: u16) -> Result<(), ReadinessFailure> {
    let deadline = Instant::now() + shared.ready_timeout;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    loop {
        let status = shared
            .children
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(twin_id)
            .and_then(|child| child.try_wait().ok().flatten());
        if let Some(status) = status {
            return Err(ReadinessFailure::Exited(status));
        }
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            let status = shared
                .children
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_mut(twin_id)
                .and_then(|child| child.try_wait().ok().flatten());
            if let Some(status) = status {
                return Err(ReadinessFailure::Exited(status));
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ReadinessFailure::Timeout);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn stop_locked(shared: &Shared, twin_id: &str, expired: bool) -> Result<StopResult, ToolError> {
    if let Some(result) = shared
        .completed
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(twin_id)
        .cloned()
    {
        return Ok(result);
    }
    let record = {
        let mut registry = shared
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = registry.get_mut(twin_id) else {
            return Err(twin_not_found(twin_id));
        };
        if !record.status.is_live() {
            return Err(ToolError {
                code: "E_REPORT_UNREADABLE",
                message: format!("twin {twin_id}: stop result was not retained"),
                hint: "Run twin_start to create a new twin",
            });
        }
        record.status = TwinStatus::Stopping;
        record.clone()
    };
    let pid = record.child_pid.ok_or_else(|| ToolError {
        code: "E_INTERNAL",
        message: format!("twin {twin_id}: child pid is missing"),
        hint: "Run twinning-mcp doctor",
    })?;
    log::emit(Level::Info, Some(twin_id), "stop_requested", &[]);
    // SAFETY: child process groups are created by this supervisor.
    let _ = unsafe { libc::killpg(pid as libc::pid_t, libc::SIGTERM) };
    let deadline = Instant::now() + shared.stop_timeout;
    let status = loop {
        let status = shared
            .children
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(twin_id)
            .and_then(|child| child.try_wait().ok().flatten());
        if let Some(status) = status {
            break status;
        }
        if Instant::now() >= deadline {
            // SAFETY: child process groups are created by this supervisor.
            let _ = unsafe { libc::killpg(pid as libc::pid_t, libc::SIGKILL) };
            reap_shared(shared, twin_id);
            shared
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .mark_stopped(twin_id, expired);
            return Err(ToolError {
                code: "E_STOP_TIMEOUT",
                message: format!("twin {twin_id}: process did not stop before the deadline"),
                hint: "The twin was killed; start a new twin and inspect stderr.log",
            });
        }
        thread::sleep(POLL_INTERVAL);
    };
    reap_shared(shared, twin_id);
    let final_status = if expired {
        TwinStatus::Expired
    } else {
        TwinStatus::Stopped
    };
    shared
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .mark_stopped(twin_id, expired);
    let report = read_report(
        &record.report_path,
        &record.stderr_path,
        exit_code(status),
        twin_id,
        record.kind,
    )?;
    let result = StopResult {
        twin_id: twin_id.to_owned(),
        kind: record.kind,
        status: final_status,
        exit_code: exit_code(status),
        report_path: record.report_path,
        trace_path: record.trace_path,
        report,
    };
    cache_completed(shared, result.clone());
    Ok(result)
}

fn cache_completed(shared: &Shared, result: StopResult) {
    let retained: HashSet<String> = shared
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .records()
        .iter()
        .map(|record| record.id.clone())
        .collect();
    let mut completed = shared
        .completed
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    completed.retain(|twin_id, _| retained.contains(twin_id));
    completed.insert(result.twin_id.clone(), result);
}

fn spawn_ttl_timer(shared: std::sync::Weak<Shared>, twin_id: String, ttl: Duration) {
    thread::spawn(move || {
        let deadline = Instant::now() + ttl;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(remaining.min(Duration::from_millis(100)));
            if shared.strong_count() == 0 {
                return;
            }
        }
        let Some(shared) = shared.upgrade() else {
            return;
        };
        log::emit(Level::Info, Some(&twin_id), "ttl_expired", &[]);
        let _ = stop_with_shared(shared, &twin_id, true);
    });
}

fn stop_with_shared(
    shared: Arc<Shared>,
    twin_id: &str,
    expired: bool,
) -> Result<StopResult, ToolError> {
    let _operation = shared
        .operation
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    stop_locked(&shared, twin_id, expired)
}

fn read_report(
    path: &Path,
    stderr_path: &Path,
    exit: i32,
    twin_id: &str,
    kind: TwinKind,
) -> Result<Value, ToolError> {
    if exit != 0 && exit != 1 {
        return Err(report_error(
            twin_id,
            format!("child exited with code {exit}"),
            path,
            stderr_path,
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| report_error(twin_id, error.to_string(), path, stderr_path))?;
    let report: Value = serde_json::from_slice(&bytes)
        .map_err(|error| report_error(twin_id, error.to_string(), path, stderr_path))?;
    let expected_version = match kind {
        TwinKind::Postgres => "twinning.v0",
        TwinKind::Rest => "twinning.rest-report.v0",
    };
    let outcome = report["outcome"].as_str();
    let valid_outcome = match kind {
        TwinKind::Postgres => matches!(outcome, Some("READY" | "VIOLATIONS" | "FAIL")),
        TwinKind::Rest => matches!(outcome, Some("PASS" | "FAIL" | "VIOLATIONS" | "REFUSAL")),
    };
    let session = &report["session"];
    let valid_rest_session = !matches!(kind, TwinKind::Rest)
        || (session["request_count"].as_u64().is_some()
            && session["endpoints_exercised"]
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_string))
            && session["endpoints_not_exercised"]
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_string))
            && valid_rest_count_map(session.get("outcomes"), true)
            && valid_rest_count_map(session.get("response_stubs"), true)
            && valid_rest_count_map(session.get("constraint_violations"), false)
            && valid_rest_count_map(session.get("refusals"), false));
    if report["version"] != expected_version || !valid_outcome || !valid_rest_session {
        return Err(report_error(
            twin_id,
            format!("expected a readable {expected_version} report"),
            path,
            stderr_path,
        ));
    }
    Ok(report)
}

fn valid_rest_count_map(value: Option<&Value>, optional: bool) -> bool {
    match value {
        Some(Value::Object(object)) => object.values().all(|count| count.as_u64().is_some()),
        None => optional,
        _ => false,
    }
}

fn is_address_in_use_refusal(path: &Path) -> bool {
    let Ok(bytes) = fs::read(path) else {
        return false;
    };
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    value["version"] == "twinning.v0"
        && value["outcome"] == "REFUSAL"
        && value["refusal"]["detail"]["stage"] == "listener_bind"
        && value["refusal"]["detail"]["error"]
            .as_str()
            .is_some_and(|error| {
                error
                    .to_ascii_lowercase()
                    .contains("address already in use")
            })
}

fn free_port() -> Result<u16, ToolError> {
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(|error| io_error("free loopback port", error))
}

fn create_private_dir(path: &Path) -> Result<(), ToolError> {
    let mut builder = DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|error| io_error("twin state directory", error))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| io_error("twin state directory permissions", error))
}

fn env_duration(name: &str, configured: Option<Duration>) -> Option<Duration> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .or(configured)
}

fn test_ttl_override(configured: Duration) -> Duration {
    std::env::var("TWINNING_MCP_TTL_SECONDS_TEST")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(configured)
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(128 + status.signal().unwrap_or(0))
}

fn report_error(twin_id: &str, detail: String, path: &Path, stderr_path: &Path) -> ToolError {
    ToolError {
        code: "E_REPORT_UNREADABLE",
        message: format!(
            "twin {twin_id}: report unreadable at {}: {detail}; stderr_tail={}",
            path.display(),
            read_stderr_tail(stderr_path)
        ),
        hint: "Read stderr.log and start a new twin",
    }
}

fn read_stderr_tail(path: &Path) -> String {
    const TAIL_BYTES: u64 = 2048;
    let Ok(mut file) = File::open(path) else {
        return "<unavailable>".into();
    };
    let Ok(length) = file.metadata().map(|metadata| metadata.len()) else {
        return "<unavailable>".into();
    };
    if file
        .seek(SeekFrom::Start(length.saturating_sub(TAIL_BYTES)))
        .is_err()
    {
        return "<unavailable>".into();
    }
    let mut bytes = Vec::with_capacity(TAIL_BYTES as usize);
    if file.read_to_end(&mut bytes).is_err() {
        return "<unavailable>".into();
    }
    let tail = String::from_utf8_lossy(&bytes)
        .lines()
        .collect::<Vec<_>>()
        .join(" | ");
    let tail: String = tail
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if tail.is_empty() {
        "<empty>".into()
    } else {
        tail
    }
}

fn twin_not_found(twin_id: &str) -> ToolError {
    ToolError {
        code: "E_TWIN_NOT_FOUND",
        message: format!("twin {twin_id}: not found"),
        hint: "Call twin_list for retained twin ids",
    }
}

fn io_error(what: &str, error: std::io::Error) -> ToolError {
    ToolError {
        code: "E_INTERNAL",
        message: format!("{what}: {error}"),
        hint: "Run twinning-mcp doctor",
    }
}

fn child_exited(shared: &Shared, twin_id: &str) -> bool {
    shared
        .children
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get_mut(twin_id)
        .is_none_or(|child| child.try_wait().ok().flatten().is_some())
}

fn all_children_exited(shared: &Shared, ids: &[String]) -> bool {
    ids.iter().all(|id| child_exited(shared, id))
}

fn stop_after_shutdown(shared: &Shared, twin_id: &str) -> Option<StopResult> {
    let record = shared
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(twin_id)
        .cloned()?;
    let status = reap_shared(shared, twin_id)?;
    shared
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .mark_stopped(twin_id, false);
    let report = read_report(
        &record.report_path,
        &record.stderr_path,
        exit_code(status),
        twin_id,
        record.kind,
    )
    .ok()?;
    Some(StopResult {
        twin_id: twin_id.to_owned(),
        kind: record.kind,
        status: TwinStatus::Stopped,
        exit_code: exit_code(status),
        report_path: record.report_path,
        trace_path: record.trace_path,
        report,
    })
}

fn reap_shared(shared: &Shared, twin_id: &str) -> Option<ExitStatus> {
    let mut child = shared
        .children
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(twin_id)?;
    let status = child.wait().ok();
    if let Some(status) = status {
        log::emit(
            Level::Info,
            Some(twin_id),
            "child_exited",
            &[("exit_code", exit_code(status).to_string())],
        );
    }
    status
}
