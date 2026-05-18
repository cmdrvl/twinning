use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    process::{Command, ExitStatus, Stdio},
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    cli::PortArgs,
    config::RestConfig,
    protocol::rest::{
        listener::{RestServerSession, start_embedded_server},
        policy::RoutingConfig,
        session_log::{RestRequestLog, RestSessionLog, RestSessionSummary},
    },
    refusal::{self, RefusalEnvelope, RefusalResult},
    runtime::Execution,
};

pub const PORT_PROOF_VERSION: &str = "twinning.port-proof.v0";
const SNAPSHOT_UNIMPLEMENTED_MESSAGE: &str = "twinning port snapshot restore is not implemented in v0. Run without snapshot flags to use Mode A (empty starting state).";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortProof {
    pub version: String,
    pub state_mode: PortStateMode,
    pub source_spec: String,
    pub target_spec: String,
    pub shared_snapshot: Option<String>,
    pub verdict: PortVerdict,
    pub missing_operations: Vec<String>,
    pub missing_response_handling: Vec<String>,
    pub missing_error_paths: Vec<String>,
    pub source_session: RestSessionSummary,
    pub target_session: RestSessionSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortStateMode {
    Independent,
    SharedSnapshot,
    SeededIndependent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PortVerdict {
    Equivalent,
    Partial,
    Incomplete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortDiff {
    pub verdict: PortVerdict,
    pub missing_operations: Vec<String>,
    pub missing_response_handling: Vec<String>,
    pub missing_error_paths: Vec<String>,
}

pub fn execute(args: &PortArgs, json_mode: bool) -> Result<Execution, Box<dyn std::error::Error>> {
    let execution = match execute_inner(args, json_mode) {
        Ok(execution) => execution,
        Err(refusal) => Execution {
            exit_code: 2,
            stdout: refusal.render(json_mode)?,
        },
    };

    Ok(execution)
}

fn execute_inner(args: &PortArgs, json_mode: bool) -> RefusalResult<Execution> {
    validate_snapshot_flags(args)?;

    let proof = run_port_proof(args)?;
    if let Some(report_path) = &args.report {
        write_report(report_path, &proof)?;
    }

    let stdout = if json_mode {
        render_json(&proof)?
    } else {
        render_human(&proof)
    };

    Ok(Execution {
        exit_code: 0,
        stdout,
    })
}

fn run_port_proof(args: &PortArgs) -> RefusalResult<PortProof> {
    let source = start_port_endpoint("source", &args.from_spec, args.from_port)?;
    let target = match start_port_endpoint("target", &args.to_spec, args.to_port) {
        Ok(target) => target,
        Err(error) => {
            let _ = source.shutdown();
            return Err(error);
        }
    };

    let source_url = format!("http://{}", source.addr());
    let target_url = format!("http://{}", target.addr());
    let child_result = run_client_command(&args.client_cmd, &source_url, &target_url);
    let source_session = source.shutdown();
    let target_session = target.shutdown();

    let child_status = child_result?;
    let source_session = source_session?;
    let target_session = target_session?;
    Ok(build_port_proof(
        args,
        &source_session,
        &target_session,
        child_status,
    ))
}

fn build_port_proof(
    args: &PortArgs,
    source: &RestServerSession,
    target: &RestServerSession,
    child_status: ExitStatus,
) -> PortProof {
    let mut diff = diff_sessions(&source.log, &target.log);
    if !child_status.success() {
        diff.verdict = PortVerdict::Incomplete;
    }

    PortProof {
        version: PORT_PROOF_VERSION.to_owned(),
        state_mode: PortStateMode::Independent,
        source_spec: args.from_spec.display().to_string(),
        target_spec: args.to_spec.display().to_string(),
        shared_snapshot: None,
        verdict: diff.verdict,
        missing_operations: diff.missing_operations,
        missing_response_handling: diff.missing_response_handling,
        missing_error_paths: diff.missing_error_paths,
        source_session: source.summary.clone(),
        target_session: target.summary.clone(),
    }
}

pub fn diff_sessions(source: &RestSessionLog, target: &RestSessionLog) -> PortDiff {
    let source_operations = operation_set(source);
    let target_operations = operation_set(target);
    let missing_operations = source_operations
        .difference(&target_operations)
        .cloned()
        .collect::<Vec<_>>();

    let source_statuses = status_set(source);
    let target_statuses = status_set(target);
    let missing_response_handling = source_statuses
        .difference(&target_statuses)
        .filter(|(operation, _)| target_operations.contains(operation))
        .map(|(operation, status)| format!("{operation} -> {status}"))
        .collect::<Vec<_>>();

    let source_errors = error_set(source);
    let target_errors = error_set(target);
    let missing_error_paths = source_errors
        .difference(&target_errors)
        .filter(|(operation, _)| target_operations.contains(operation))
        .map(|(operation, refusal)| format!("{operation} refusal {refusal}"))
        .collect::<Vec<_>>();

    let verdict = if source_operations.is_empty() || !missing_operations.is_empty() {
        PortVerdict::Incomplete
    } else if !missing_response_handling.is_empty() || !missing_error_paths.is_empty() {
        PortVerdict::Partial
    } else {
        PortVerdict::Equivalent
    };

    PortDiff {
        verdict,
        missing_operations,
        missing_response_handling,
        missing_error_paths,
    }
}

fn operation_set(log: &RestSessionLog) -> BTreeSet<String> {
    log.requests.iter().map(operation_key).collect()
}

fn status_set(log: &RestSessionLog) -> BTreeSet<(String, u16)> {
    log.requests
        .iter()
        .map(|request| (operation_key(request), request.status))
        .collect()
}

fn error_set(log: &RestSessionLog) -> BTreeSet<(String, String)> {
    log.requests
        .iter()
        .filter_map(|request| {
            request
                .refusal
                .as_ref()
                .map(|refusal| (operation_key(request), refusal.clone()))
        })
        .collect()
}

fn operation_key(request: &RestRequestLog) -> String {
    format!("{} {}", request.method, request.route)
}

fn start_port_endpoint(
    role: &str,
    spec_path: &Path,
    port: Option<u16>,
) -> RefusalResult<crate::protocol::rest::listener::EmbeddedRestServer> {
    start_embedded_server(RestConfig {
        spec_path: spec_path.to_path_buf(),
        host: String::from("127.0.0.1"),
        port: port.unwrap_or(0),
        run_command: None,
        serve: true,
        serve_defaulted: false,
        report_path: None,
        canary_path: None,
        strict: false,
        routing: RoutingConfig::default(),
        auth_mode: None,
        chaos: None,
        json: false,
    })
    .map_err(|error| Box::new(port_boot_refusal(role, &error)))
}

fn run_client_command(
    command: &str,
    source_url: &str,
    target_url: &str,
) -> Result<ExitStatus, Box<RefusalEnvelope>> {
    shell_command(command)
        .env("TWIN_FROM_URL", source_url)
        .env("TWIN_TO_URL", target_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| Box::new(refusal::runtime_io("port_client", error.to_string())))
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

fn validate_snapshot_flags(args: &PortArgs) -> RefusalResult<()> {
    let snapshot_flags = [
        ("shared_snapshot", args.shared_snapshot.as_ref()),
        ("from_snapshot", args.from_snapshot.as_ref()),
        ("to_snapshot", args.to_snapshot.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|path| (name, path.display().to_string())))
    .collect::<Vec<_>>();

    if snapshot_flags.is_empty() {
        return Ok(());
    }

    Err(Box::new(RefusalEnvelope::new(
        "E_PORT_SNAPSHOT_RESTORE_UNIMPLEMENTED",
        SNAPSHOT_UNIMPLEMENTED_MESSAGE,
        json!({
            "state_mode": if args.shared_snapshot.is_some() {
                "shared_snapshot"
            } else {
                "seeded_independent"
            },
            "snapshot_flags": snapshot_flags,
        }),
        Some(String::from(
            "twinning port --from-spec source.yaml --to-spec target.yaml --client-cmd 'python client.py' --json",
        )),
    )))
}

fn port_boot_refusal(role: &str, source: &RefusalEnvelope) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_PORT_TWIN_BOOT",
        format!("Failed to start {role} REST twin for port proof."),
        json!({
            "role": role,
            "source_refusal": source
                .render(true)
                .unwrap_or_else(|_| format!("{source:?}")),
        }),
        Some(String::from(
            "twinning port --from-spec source.yaml --to-spec target.yaml --client-cmd 'python client.py' --json",
        )),
    )
}

fn write_report(path: &Path, report: &PortProof) -> RefusalResult<()> {
    let rendered = render_json(report)?;
    fs::write(path, rendered).map_err(|error| Box::new(refusal::io_write(path, &error)))
}

fn render_json(report: &PortProof) -> RefusalResult<String> {
    let mut rendered = serde_json::to_string_pretty(report)
        .map_err(|error| Box::new(refusal::runtime_io("port_report_render", error.to_string())))?;
    rendered.push('\n');
    Ok(rendered)
}

fn render_human(report: &PortProof) -> String {
    let mut lines = vec![
        format!("twinning port {:?}", report.verdict).to_ascii_lowercase(),
        format!("source: {}", report.source_spec),
        format!("target: {}", report.target_spec),
        format!(
            "requests: source={} target={}",
            report.source_session.request_count, report.target_session.request_count
        ),
    ];
    if !report.missing_operations.is_empty() {
        lines.push(format!(
            "missing operations: {}",
            report.missing_operations.join(", ")
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{PortVerdict, diff_sessions};
    use crate::protocol::rest::session_log::{RestRequestLog, RestSessionLog};

    fn request(method: &str, route: &str, status: u16, refusal: Option<&str>) -> RestRequestLog {
        RestRequestLog {
            method: method.to_owned(),
            path: route.replace("{id}", "1"),
            route: route.to_owned(),
            status,
            duration_ms: 1,
            constraint_violation: None,
            refusal: refusal.map(str::to_owned),
        }
    }

    #[test]
    fn equivalent_verdict_when_sessions_match() {
        let source = RestSessionLog {
            requests: vec![
                request("POST", "/files", 201, None),
                request("GET", "/files/{id}", 200, None),
            ],
        };
        let target = source.clone();

        let proof = diff_sessions(&source, &target);

        assert_eq!(proof.verdict, PortVerdict::Equivalent);
        assert!(proof.missing_operations.is_empty());
        assert!(proof.missing_response_handling.is_empty());
        assert!(proof.missing_error_paths.is_empty());
    }

    #[test]
    fn incomplete_verdict_when_target_missing_operations() {
        let source = RestSessionLog {
            requests: vec![
                request("POST", "/files", 201, None),
                request("DELETE", "/files/{id}", 204, None),
            ],
        };
        let target = RestSessionLog {
            requests: vec![request("POST", "/files", 201, None)],
        };

        let proof = diff_sessions(&source, &target);

        assert_eq!(proof.verdict, PortVerdict::Incomplete);
        assert!(
            proof
                .missing_operations
                .contains(&String::from("DELETE /files/{id}"))
        );
    }

    #[test]
    fn partial_verdict_when_status_or_error_path_differs() {
        let source = RestSessionLog {
            requests: vec![request("GET", "/files/{id}", 404, Some("not_found"))],
        };
        let target = RestSessionLog {
            requests: vec![request("GET", "/files/{id}", 200, None)],
        };

        let proof = diff_sessions(&source, &target);

        assert_eq!(proof.verdict, PortVerdict::Partial);
        assert_eq!(
            proof.missing_response_handling,
            vec![String::from("GET /files/{id} -> 404")]
        );
        assert_eq!(
            proof.missing_error_paths,
            vec![String::from("GET /files/{id} refusal not_found")]
        );
    }
}
